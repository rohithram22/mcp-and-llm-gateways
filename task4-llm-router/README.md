# Task 4: Rate-Limiting & Model Fallback Router

An OpenAI-compatible LLM gateway with a **token-aware sliding-window rate limiter** (50,000 tokens per minute per tenant API key, stored in **on-disk SQLite**) and **automatic failover** from a primary to a secondary model provider when the primary returns **429** or takes longer than **3000 ms**. Every error uses one standardized, sanitized payload.

```
client ──Bearer key──▶ gateway.py
                        1. authenticate tenant          (tenants table, hashed keys)
                        2. reserve worst-case tokens    (token_usage table, BEGIN IMMEDIATE)
                        3. route ──▶ primary   (deadline 3000 ms)
                                 └─▶ secondary (on 429 / timeout / 5xx / unreachable)
                        4. settle to reported usage, or release if nobody answered
```

## Run it

Requires Python 3.10 or newer. From the repository root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r task4-llm-router/requirements.txt

python -m pytest task4-llm-router -v     # 59 tests; starts its own servers and temp databases
```

To run it by hand, use four terminals:

```bash
MOCK_NAME=primary   MOCK_PORT=9201 python task4-llm-router/mock_provider.py
MOCK_NAME=secondary MOCK_PORT=9202 python task4-llm-router/mock_provider.py
python task4-llm-router/gateway.py                                   # :8200, creates task4-router.db

curl -s localhost:8200/v1/chat/completions -H "Authorization: Bearer sk-acme-123" \
  -d '{"max_tokens":20,"messages":[{"role":"user","content":"hello"}]}'
```

Make the primary misbehave without restarting it, then watch `"model"` in the reply switch to `secondary-model`:

```bash
curl -s -X POST localhost:9201/control -d '{"mode":"rate429"}'           # or slow / error500 / stall / ok
curl -s -X POST localhost:9201/control -d '{"mode":"slow","delay":5}'    # fails over after 3 s
```

Two demo tenants are created at startup (`SEED_DEMO_TENANTS=0` disables this): `acme` / `sk-acme-123` with 50,000 tokens per minute, and `tiny` / `sk-tiny-123` with 2,000 tokens per minute (small enough to hit by hand).

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ROUTER_DB` | `task4-router.db` | SQLite file (WAL mode) |
| `PRIMARY_URL` / `SECONDARY_URL` | `:9201` / `:9202` | Provider endpoints |
| `PRIMARY_DEADLINE_MS` | `3000` | Primary deadline before failover |
| `SECONDARY_DEADLINE_MS` | `30000` | Secondary deadline (no further fallback) |
| `STREAM_IDLE_TIMEOUT` | `30` | Longest silence between chunks once a stream has started, in seconds |
| `GATEWAY_PORT` | `8200` | Listening port |

## Files

| File | Purpose |
|---|---|
| `gateway.py` | Endpoint, request lifecycle, standardized errors, streaming relay with usage capture |
| `limiter.py` | `RateLimiter`: `reserve` / `settle` / `release`, eviction, `Retry-After`, async wrappers |
| `router.py` | Primary → secondary routing with a single-outcome deadline |
| `db.py` | Schema (`tenants`, `token_usage`, indexes), hashed API keys, connections |
| `tokens.py` | Request pricing: prompt estimate + `max_tokens` |
| `mock_provider.py` | OpenAI-style provider with runtime-switchable failure modes and `/stats` |
| `tests/` | Limiter, router, and end-to-end tests |

## Design

### Token-aware sliding window

The limit is in **tokens**, not requests, and the window **slides**: at any moment, a tenant's usage is the sum of its charges in the **last 60 seconds**. A fixed per-minute window would let a tenant spend 50,000 at 0:59 and another 50,000 at 1:01; a sliding window can't be gamed that way.

It's implemented as a **sliding log** in SQLite: one `token_usage` row per charge (`tenant_id`, `ts`, `tokens`), indexed on `(tenant_id, ts)` so the window sum is an index range scan.

**Reserve → settle / release.** A reply's real length is unknown until it arrives, so each request is admitted in two phases:

| Phase | When | Effect on its row |
|---|---|---|
| `reserve` | before calling a model | inserted with the **worst case**: prompt estimate + `max_tokens` |
| `settle` | a model answered | updated to the provider's reported `usage.total_tokens` |
| `release` | no model answered | deleted: full refund |

Reserving the worst case **up front** is what makes concurrency safe: in-flight requests are already counted, so a burst can't be admitted on a budget that only looks free because nothing has finished yet. Settling keeps the row's **original timestamp**, so tokens expire 60 s after the request started, and slow requests can't shift their cost into a later window.

**Eviction.** The window sum only counts rows with `ts > now − 60`, so expired rows stop counting immediately (correctness). Each reservation also deletes rows older than the window (`DELETE ... WHERE ts <= cutoff`, indexed on `ts`), so the table stays bounded: each row is deleted exactly once.

**`Retry-After`.** When a request doesn't fit, the limiter walks the tenant's rows oldest-first to find when enough tokens will have slid out of the window, and returns that exact number of seconds. A request bigger than the tenant's whole budget is rejected with 400 `request_too_large` instead of 429, since waiting would never help.

### Concurrency

**Check-then-act race.** "Sum the window, then insert if it fits" is unsafe if two requests interleave: both see room and both insert. `reserve` runs the eviction, sum, and insert inside **`BEGIN IMMEDIATE`**, which takes SQLite's write lock before reading. Concurrent reservations queue at the lock and each sees every earlier hold. Because the lock belongs to the database file, this holds across **multiple gateway processes**, not just within one.

**Measured:** 50 simultaneous 4,000-token reservations against 50,000 admit **exactly 12** (48,000), both from 50 threads and from 4 processes sharing the file. Without the transaction, the same burst admitted 17–23 (68,000–92,000 tokens).

**Non-blocking.** `sqlite3` is blocking, so the async wrappers (`areserve`, `asettle`, `arelease`) run each call on a worker thread via `asyncio.to_thread`. A 5 ms heartbeat measured an 80–120 ms worst stall when calling SQLite directly, versus under 10 ms through the worker threads.

**Cancellation.** If a request is cancelled (the client disconnects) while its reservation is being written, the worker thread can't be stopped and may still commit. `areserve` shields the work and attaches a callback that **releases the orphaned reservation** as soon as the thread finishes. `asettle` and `arelease` are shielded so a refund always completes.

**Every admitted request is settled or released on every path**, including failures and cancellation during routing.

### Failover and the timeout race

| Primary outcome | Failover? |
|---|---|
| **429** | Yes (required) |
| **No response within 3000 ms** | Yes (required) |
| 500 / 502 / 503 / 504, connection error | Yes: the provider is unavailable either way |
| Other 4xx | **No**: the request is malformed and the backup would reject it too |

"Response" means the complete reply for non-streaming requests, and the **first chunk** for streaming ones. Once any text has reached the client, failing over would splice two different models' answers together, so a stream that stalls **after** it has started ends with an in-band error event instead.

**Exactly one outcome at the deadline.** Each attempt runs as its own task, awaited with `asyncio.wait(..., timeout=deadline)`. What `asyncio.wait` reports at the deadline is final: either the reply is done (use it) or it isn't (cancel the call and fail over). A reply that completes in the instant after the decision is **closed and ignored** (`_discard`), so a request is never answered twice and no connection is leaked. Cancelling the call closes the connection, which tells the provider to stop generating. The mock's `/stats` confirms timed-out requests are `abandoned`, not `completed`. If the gateway itself is cancelled while waiting, the provider call is cancelled too.

This was chosen over `asyncio.wait_for` / `asyncio.timeout`, which cancel the *waiting* code itself, so that the decision point at the deadline is explicit and unambiguous.

**Only one clock per phase.** The httpx client has no read timeout: before a reply starts, the router's deadline alone decides; after it starts, the gateway's per-chunk `STREAM_IDLE_TIMEOUT` does. (During development, an httpx read timeout shorter than the deadline fired first and misclassified a slow primary as unreachable.)

**Billing.** The tenant is charged **once**: the settled usage of the model that answered. The primary's abandoned work isn't charged to the tenant, since failover is the gateway's decision. Its cost to the operator is limited to what it did before the deadline, because the connection is closed at that moment.

### Standardized errors

Every error, whether from authentication, validation, rate limiting, routing, an unknown path, or an unexpected exception, has one shape, plus an `X-Request-ID` header (also present on successful responses):

```json
{"error": {"type": "upstream_overloaded", "message": "All model providers are rate limited. Retry shortly.", "request_id": "req_3a77f4661cbf44ce"}}
```

| Situation | Status | `type` |
|---|---|---|
| Missing / unknown API key | 401 | `authentication_error` |
| Invalid JSON, unpriceable request | 400 | `invalid_request` |
| Request exceeds the whole budget | 400 | `request_too_large` |
| Over the sliding-window budget | 429 + `Retry-After` | `rate_limit_exceeded` |
| Every provider returned 429 | 503 + `Retry-After` | `upstream_overloaded` |
| Provider rejected the request (4xx) | 400 | `upstream_rejected` |
| Timeouts | 504 | `upstream_timeout` |
| 5xx / unreachable | 502 | `upstream_unavailable` |
| Stream broke after it started | in-band SSE event | `upstream_interrupted` |
| Unexpected gateway exception | 500 | `internal_error` |

Upstream bodies, provider names and URLs, hostnames, and stack traces are **never** returned. The full attempt history (`primary:rate_limited(4ms) → secondary:rate_limited(3ms)`) is logged under the same `request_id` for operators.

### Storage

- **On-disk SQLite in WAL mode:** readers don't block the writer, and state survives restarts. A second gateway process on the same file sees the first one's in-flight holds (tested).
- **API keys are stored as SHA-256 hashes**, never in plain text.
- `CHECK` constraints reject negative token counts and non-positive limits at the database level.
- One short-lived connection per operation, which is cheap for SQLite and safe across worker threads.

## Tests

`python -m pytest task4-llm-router -v` runs 59 tests at three levels:

- **Limiter** (temporary database, fake clock): exact limit boundary, separate tenant budgets, `Retry-After` correctness, the fixed-window exploit being impossible, the exact 60 s boundary, eviction and bounded table size, reconciliation keeping the original timestamp, request pricing, **concurrent bursts from threads and from separate processes**, event-loop responsiveness, and refunding cancelled reservations.
- **Router** (in-memory fake transport that counts open connections): failover rules, the deadline on both sides, **40 replies within ±4 ms of the deadline each answered exactly once with zero leaked connections**, a reply completing after the decision being closed, time-to-first-chunk for streams, and cancellation propagation.
- **End to end** (real gateway, real mocks, on-disk SQLite): settlement to reported usage, every error in the standard shape, a concurrent burst admitting exactly what fits, state shared across a second gateway process, failover on 429/500/timeout with the primary abandoned and the tenant charged once, sanitized 503/502 with full refunds, streaming settlement, failover only before the first chunk, in-band errors for stalls, and no tracebacks in the gateway log.

The suite was **mutation-tested**: removing the transaction, the orphan refund, eviction, the late-reply close, or cancellation propagation each makes at least one test fail.

## Trade-offs and extensions

- **Worst-case reservations can briefly block a tenant** while requests are in flight, until they settle. Lower `max_tokens` reduces this. It's the price of never overspending under concurrency.
- **A broken stream keeps its worst-case charge**, because the final `usage` report never arrives. That can overcharge, but it never undercounts.
- **The token estimate** is about 4 characters per token. A production gateway would use the model's tokenizer; reconciliation corrects the difference either way.
- **Scaling out:** one SQLite file serializes reservations at its write lock, which is fine for one host and a few processes. Across many hosts, the same algorithm maps onto Redis (a sorted set per tenant with an atomic Lua script) or a database row lock.
- **Possible additions:** a circuit breaker so a failing primary is skipped for a cooldown instead of costing 3 s per request; per-tenant model routing; honouring the provider's `Retry-After`; and hedged requests (start the secondary early, before the primary's deadline) for latency-critical tenants.

## Assumptions

- The tenant identifier is the API key in `Authorization: Bearer`, and limits are per tenant.
- Tokens are counted as prompt + completion (`usage.total_tokens`).
- Failover on 5xx and connection errors is in addition to the required 429 and timeout cases.