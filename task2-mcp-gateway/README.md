# Task 2: MCP Security Gateway

An HTTP reverse proxy that sits between an AI agent and a downstream MCP server. It authenticates every request with a JWT bearer token, inspects the JSON-RPC payload, and blocks `admin_*` tool calls from non-admin users with `-32001 Unauthorized Tool Call`, **without the downstream server ever seeing the request**. Everything else is forwarded transparently, including MCP sessions and Server-Sent Event streams.

```
AI agent ──HTTP──▶  gateway.py  ──HTTP──▶  mock_server.py
  (Bearer JWT)      :8000/mcp              :9000/mcp
                    1. authenticate        (official MCP SDK,
                    2. inspect + authorize   Streamable HTTP)
                    3. forward / stream
```

## Run it

Requires Python 3.10 or newer. From the repository root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r task2-mcp-gateway/requirements.txt

python -m pytest task2-mcp-gateway -v      # 56 tests; starts its own servers
```

To run it by hand, use three terminals:

```bash
python task2-mcp-gateway/mock_server.py            # downstream on :9000 (MOCK_SSE=1 for SSE replies)
python task2-mcp-gateway/gateway.py                # gateway on :8000
python task2-mcp-gateway/demo_client.py 2>/dev/null
```

`demo_client.py` uses the official MCP client through the gateway, once as a viewer and once as an admin:

```
=== bob (viewer) ===
get_weather -> {'city': 'Paris', 'forecast': 'sunny', 'temp_c': 21}
admin_reset_key -> blocked by gateway: code=-32001 message='Unauthorized Tool Call'

=== alice (admin) ===
admin_reset_key -> {'user': 'bob', 'new_key': 'sk-NEW-0000', 'status': 'rotated'}
```

To mint tokens for curl: `python task2-mcp-gateway/make_token.py <viewer|admin> <user> [ttl_seconds]`.

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `GATEWAY_JWT_SECRET` | dev-only value | HS256 signing secret. **Must** be set in any real deployment |
| `DOWNSTREAM_URL` | `http://127.0.0.1:9000/mcp` | The MCP server being protected |
| `UPSTREAM_READ_TIMEOUT` | `60` | Seconds of downstream silence before returning 504 |
| `GATEWAY_PORT` / `MOCK_PORT` | `8000` / `9000` | Listening ports |

## Files

| File | Purpose |
|---|---|
| `gateway.py` | The HTTP layer: the `/mcp` route, header allowlists, streaming forwarder, and 401/502/504 responses |
| `auth.py` | Authentication. Verifies the bearer JWT and returns a `Principal(subject, role)` |
| `policy.py` | Authorization. `check_request()` decides, for one request body, whether to forward it or answer it with an error |
| `mock_server.py` | Downstream MCP server (official SDK) with `get_weather`, `list_users`, `admin_reset_key`, `admin_delete_user` |
| `make_token.py` | Command-line helper that mints test tokens |
| `demo_client.py` | The official MCP client run through the gateway as a viewer and as an admin |
| `tests/` | End-to-end tests against real processes, plus unit tests of the policy |

## Request lifecycle

Every request to `/mcp` goes through three stages, in this order:

**1. Authenticate (`auth.py`)**, before the body is read. The `Authorization` header must be `Bearer <JWT>`. The token must have a valid HS256 signature, must not be expired, and must carry `sub` and `role` claims, with `role` being `admin` or `viewer`. Any failure returns **HTTP 401** with `WWW-Authenticate: Bearer` and one generic message. The specific reason is logged but never returned, so the response gives no hints about which check failed.

**2. Authorize (`policy.py`)**, POST bodies only. The body is parsed and checked step by step. Every case that isn't explicitly understood and allowed ends in a rejection:

| Check | Result if it fails |
|---|---|
| Body larger than 1 MB | HTTP 413, `-32600` |
| Not valid JSON / invalid UTF-8 | HTTP 400, `-32700` Parse error |
| Duplicate keys in any object | HTTP 400, `-32600` |
| JSON array (batch) | HTTP 400, `-32600` |
| Not an object, or missing/non-string `method` | HTTP 400, `-32600` |
| Client JSON-RPC *response* (`result`/`error`, no `method`) | **forwarded**. It cannot invoke a tool |
| Any method other than `tools/call` | **forwarded** (`initialize`, `tools/list`, notifications...) |
| `tools/call` without a readable string `params.name` | HTTP 200, `-32602` |
| `params.name` is an admin tool and role is not `admin` | HTTP 200, **`-32001` Unauthorized Tool Call** |
| Otherwise | **forwarded** |

Gateway-generated errors echo the request's `id` (only when it is a valid JSON-RPC id type), so the client can match the reply to its request.

**3. Forward (`gateway.py`).** The original bytes are sent downstream unchanged, and the reply is **streamed** back as it arrives.

## Design decisions

**JWT over a static token map.** The role is carried in a signed token, so the gateway needs no user database and the role cannot be altered without invalidating the signature. `algorithms=["HS256"]` is pinned in `jwt.decode`, which blocks `alg: none` and algorithm-swap attacks.

**Admin tool detection is normalized.** `is_admin_tool()` applies Unicode NFKC normalization, `strip()`, and `casefold()` before checking the `admin_` prefix, so `Admin_reset_key`, `" admin_reset_key"`, and the full-width `ａdmin_reset_key` are all treated as admin tools. The gateway can't know how the downstream compares names, so it errs on the side of blocking.

**Batches are rejected, not inspected item by item.** MCP 2025-06-18 removed JSON-RPC batching, so legitimate clients never send them, and a batch is the classic way to smuggle a forbidden call past a gateway that only inspects single objects. As defense in depth, a batch would also fail the next check (body must be an object).

**Duplicate keys are rejected.** The gateway forwards the original bytes. If a body contained `"name": "get_weather", "name": "admin_reset_key"`, the gateway's parser and the downstream's parser could pick different values (a *parser differential*), and the gateway would authorize one call while the server executed another.

**Header allowlists in both directions.** Only `content-type`, `accept`, `mcp-session-id`, `mcp-protocol-version`, and `last-event-id` are forwarded downstream. The client's **`Authorization` header is never forwarded**, so user credentials stay at the gateway. Only `content-type`, `mcp-session-id`, and `cache-control` are relayed back.

**Authorization is per request, not per session.** The role is re-derived from the token on every request. A session ID is a conversation identifier, not a credential: a viewer presenting an admin's session ID is still blocked.

**HTTP status of denials.** A well-formed but forbidden call returns HTTP 200 with a JSON-RPC error, the same way a downstream server reports JSON-RPC errors, because MCP clients may treat non-2xx statuses as transport failures. Malformed bodies return 400, since the HTTP request itself is invalid.

**Streaming passthrough.** The forwarder uses `httpx` with `stream=True` and a `StreamingResponse` over `aiter_raw()`, so SSE events are relayed as they arrive and the gateway never holds a whole reply in memory. `GET` streams (server-initiated messages) have no read timeout, since they may legitimately sit idle.

**Upstream failures.** If the downstream can't be reached, the gateway returns HTTP 502; if it doesn't answer within the read timeout, HTTP 504. Both use JSON-RPC `-32603` with a generic message and the original request `id`. Exception details, hostnames, and ports are logged, never returned.

## Tests

`python -m pytest task2-mcp-gateway -v` starts real processes on free ports: two mock MCP servers (JSON and SSE), a test-only echo server, and four gateway instances, including one pointed at a dead port.

Requests that should be blocked carry a unique marker, and the test asserts that the marker **never appears in the downstream server's log**. This proves "without calling the downstream server" directly, rather than inferring it from the response.

| Area | What is verified |
|---|---|
| Authentication | 401 for missing, garbage, expired, wrong-secret, `alg: none`, unknown-role, and tampered-payload tokens; identical 401 message for every cause |
| Authorization | `-32001` for six spellings of admin tools; request `id` preserved; admin allowed; viewer cannot use an admin's session |
| Hardening | 12 malformed bodies plus oversized bodies, each with the exact status and code |
| Proxying | `Authorization` not forwarded while `Mcp-Session-Id` is; 502 and 504 without leaking internals |
| Streaming | First SSE event relayed in under 0.4 s while the downstream takes 1.5 s to finish |
| Compatibility | The official MCP client works through the gateway in both JSON and SSE modes |
| Stability | No gateway logged a traceback during the run |

During development, the streaming test and the traceback check caught a real bug: a timeout misconfiguration that made every `GET` stream fail with a 500, while the demo output still looked correct.

## Production notes

- **Asymmetric signing (RS256/ES256) with a JWKS endpoint**, so the gateway holds only a public key and cannot mint tokens.
- **Bind sessions to their creator**: record which subject opened each `Mcp-Session-Id` and reject other subjects using it.
- **Optionally filter `tools/list` per role**, so viewers aren't shown admin tools at all. The task specifies transparent forwarding, so this was not done.
- **Policy from configuration** (role → allowed tool patterns) rather than a hardcoded prefix, plus audit logging of denials and rate limiting per subject.
- **TLS termination** in front of the gateway: bearer tokens must only travel over HTTPS.
- Read the body with a streaming size limit, rather than reading it fully and then checking its length.

## Assumptions

- Roles are `admin` and `viewer`. Any other role in a validly signed token is rejected.
- "Starts with `admin_`" is applied after normalization (case, whitespace, Unicode compatibility forms).
- The downstream speaks MCP Streamable HTTP at a single endpoint.