# Task 3: LLM Gateway Streaming Guardrail (PII Redaction)

An OpenAI-compatible LLM gateway that forwards chat completion requests to a provider and streams the reply back, replacing **emails, SSNs/ITINs, and payment card numbers** with `[REDACTED]` **while the text is streaming**. The response is never accumulated in memory, and the gateway adds roughly one chunk of latency.

```
client ──POST /v1/chat/completions──▶ gateway.py ──▶ LLM provider (mock_llm.py)
       ◀──────── SSE, redacted ─────── │  ◀── SSE, raw ──
                                       ├─ SSEParser       bytes  → complete events
                                       ├─ StreamRedactor  deltas → safe text (+ small held tail)
                                       └─ re-emit events in the same OpenAI format
```

## Run it

Requires Python 3.10 or newer. From the repository root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r task3-pii-stream-guardrail/requirements.txt

python -m pytest task3-pii-stream-guardrail -v     # 335 tests; starts its own servers
```

To run it by hand, use three terminals:

```bash
python task3-pii-stream-guardrail/mock_llm.py                   # provider on :9100
python task3-pii-stream-guardrail/gateway.py                    # gateway on :8100
python task3-pii-stream-guardrail/stream_client.py "mixed"      # watch it stream, with TTFT
```

```
Customer profile: email [REDACTED], SSN [REDACTED], card [REDACTED]. Order number 1234 5678 9012 3456 is not a card.
TTFT: 99 ms   total: 912 ms
```

The mock picks its reply from a keyword in the prompt: `ssn`, `email`, `card`, `mixed`, `clean`, plus the failure modes `fail500`, `rate429`, `cutoff`, `garbage`, and `stall`. Replies are sent in fixed 5-character chunks (`mock_chunk_size` overrides this), so every PII value is split across chunks.

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LLM_URL` | `http://127.0.0.1:9100/v1/chat/completions` | Provider endpoint (any OpenAI-compatible API) |
| `UPSTREAM_READ_TIMEOUT` | `60` | Longest silence allowed between two chunks from the provider, in seconds |
| `GATEWAY_PORT` / `MOCK_LLM_PORT` | `8100` / `9100` | Listening ports |
| `MOCK_CHUNK_DELAY` | `0.03` | Seconds between mock chunks |

## Files

| File | Purpose |
|---|---|
| `gateway.py` | The endpoint, upstream forwarding, per-choice redaction of the stream, and error handling |
| `redactor.py` | `StreamRedactor`: `feed(text) → safe text`, `flush() → the rest`. The rolling buffer |
| `patterns.py` | PII detection on complete strings: email, SSN/ITIN, Luhn-checked cards |
| `sse.py` | Incremental Server-Sent Events parser (bytes → complete event payloads) |
| `mock_llm.py` | OpenAI-style mock provider with PII-heavy replies and failure modes |
| `stream_client.py` | Command-line client that prints the stream as it arrives and reports TTFT |
| `tests/` | Unit and property tests, end-to-end tests, and a scriptable `fake_provider.py` |

## How it works

### Two layers of reassembly

1. **Bytes → events (`SSEParser`).** Network chunks don't respect event boundaries. The parser buffers until a blank line completes an event, and uses an incremental UTF-8 decoder so a multi-byte character split across chunks is never corrupted.
2. **Text → safe text (`StreamRedactor`).** Even when every event arrives whole, the *text* inside is split: `"123-4"`, `"5-678"`, `"9, an"`. The redactor handles splits at this level.

Each layer holds only an incomplete tail; neither ever waits for the full response.

### The rolling buffer

Text that has been sent can't be taken back, so the redactor **only releases text that can no longer become part of a match**. On every `feed()`:

1. **Find the ambiguous tail** (`hold_point`): a trailing word of email characters (`noon` could become `noon@example.com`), or a trailing run of digits, spaces, and dashes (`123-4` could become an SSN, `4111 1111` a card).
2. **Run the patterns over the buffer.** Matches before the tail are replaced with `[REDACTED]`; matches inside the tail are ignored, because they might still grow (`123-45-6789` followed by `0` is not an SSN).
3. **Never cut through a match.** If a match straddles the cut, the whole match is held.
4. **Release** everything before the cut, and keep the tail.

```
in '123-4'  -> out ''               held '123-4'
in '5-678'  -> out ''               held '123-45-678'
in '9, an'  -> out '[REDACTED], '   held 'an'
```

Two details keep this exactly equivalent to redacting the whole text:

- **Fence context.** The patterns use lookbehind ("not preceded by a digit"). After text is released it's gone from the buffer, so the last released character is prepended when scanning, so a match can't wrongly start in the middle of a number that was partly sent already.
- **Capped tails, cut at group boundaries.** A tail is held for at most 254 characters (the RFC limit for an email) or 40 characters (the longest card with separators), so memory is bounded. When capping, the cut never lands inside a digit group of 19 digits or fewer, since such a group could belong to a card.

### Patterns

| Kind | Rule |
|---|---|
| Email | `local@domain.tld`; the local part may contain `._%+-`; at least a 2-letter TLD |
| SSN / ITIN | `ddd-dd-dddd`, not inside a longer digit/dash run. Never-issued numbers (area `000`/`666`, group `00`, serial `0000`) are skipped. Area `9xx` (ITINs, which are also tax IDs) **is** redacted |
| Card | 13–19 digits made of whole groups separated by single spaces or dashes, **and Luhn-valid**. Cards are searched *inside* each run of digit groups, so `Room 4 4111 1111 1111 1111` is still caught |

Lookalikes are deliberately left alone: phone numbers, `123-45-67890`, the Luhn-failing `1234 5678 9012 3456`, `user@localhost`.

### Gateway behaviour

- **One redactor per choice.** With `n > 1`, providers interleave several replies in one stream; sharing a buffer would mix them or let a value split by the other choice's text leak.
- **Events with no releasable text are skipped**, so the client receives whole words instead of the provider's fragments.
- **Held text is flushed before the closing event.** When a choice reports `finish_reason`, its held text is decided and sent as an extra event *before* the closing one (a safety net also flushes at `[DONE]`).
- **Non-streaming requests** (`stream: false`) are redacted with the same patterns.
- **Counts only in logs**: `choice 0 finished: 3 value(s) redacted, peak buffer 23 chars`. Values are never logged.

### Failure handling

| Situation | Response |
|---|---|
| Invalid JSON request body | HTTP 400 |
| Provider unreachable / no response in time | HTTP 502 / 504 |
| Provider 429 | HTTP 429, generic message |
| Provider other 4xx / 5xx | Same 4xx / HTTP 502, generic message |
| Malformed event mid-stream | Dropped (it can't be redacted, so it's never forwarded); the stream continues |
| Connection breaks or provider stalls mid-stream | Final in-band `{"error": ...}` event (the 200 status is already sent) |
| Client disconnects | Provider connection closed immediately |

**Provider error bodies are never passed through.** They can contain internal hostnames and stack traces, or echo the prompt (and the PII in it). They're logged by size only.

**On an abnormal end, held text is discarded, not flushed.** If the stream breaks right after `123-4`, flushing would release half of a real SSN. A normal end (`finish_reason` or `[DONE]`) flushes; a broken stream discards and logs `discarded N held character(s)`.

## Performance

| Measure | Result |
|---|---|
| TTFT overhead | At most about one chunk. The redactor waits for the character after the first word (~30 ms with the mock); tested to be under 100 ms against the provider called directly |
| Total latency overhead | Tested under 100 ms (`ssn` prompt: 588 ms through the gateway vs 600 ms direct) |
| Memory | Bounded by the held tail, not the response: a 70,000-character stream never buffers more than 100 characters, and the gateway reports a peak of under 100 for a 3,000-chunk reply |
| CPU | Each `feed()` scans only the small buffer; the card search does bounded work per digit group (≤ 19-digit windows), so it stays linear even on adversarial input |

## Tests

`python -m pytest task3-pii-stream-guardrail -v` runs 335 tests.

**Correctness of streaming (the core guarantee):** for every text in a tricky corpus and **every chunk size from 1 to 30**, the streamed output must equal `redact(whole_text)`. A further 3,000 random texts are split at random points, and a one-character-at-a-time test asserts that no PII fragment (`123-45`, `4111 1111`, `ada@`) is ever released before it is decided.

During development, randomized testing found two real leaks that the example tests had missed: a card hidden by an adjacent number (`Room 4 4111 ...`), and a cut landing inside a digit group in long numeric runs. Both are fixed and covered by tests. After the fixes, 140,000 fuzz cases (adversarial and realistic) show zero differences between streaming and whole-text redaction.

**End to end (real servers):** exact redaction for every reply at several chunk sizes; the OpenAI event format and flush ordering; two interleaved choices; measured TTFT and total overhead against the provider directly; first text arriving long before the reply ends; bounded gateway memory on a 3,000-chunk reply; every failure mode, including that a broken stream never leaks the held `123-4`; client disconnect closing the provider stream; and no tracebacks in any gateway log.

## Trade-offs and extensions

- **One-word lag.** Any trailing word could be the start of an email, so output runs at most one word behind the provider. That's the minimum delay a correct streaming redactor can have for unbounded-prefix patterns.
- **False positives over false negatives.** A guardrail that leaks is worse than one that over-redacts: ITINs are redacted, and the redactor holds back conservatively.
- **Patterns are regex-based** and cover the three requested types. Natural extensions are phone numbers (format-dependent and prone to false positives), IBANs, API keys, and a named-entity model (e.g. Presidio) for names and addresses.
- **Request-side protection.** This guardrail covers responses. A production gateway would also redact or **tokenize** PII in prompts before they reach the provider (reversible pseudonymization, `<EMAIL_1>` → original for authorized viewers).
- **SSNs without dashes** (`123456789`) are not matched, to avoid flagging every 9-digit number.

## Assumptions

- The provider speaks the OpenAI chat completions format (`choices[].delta.content` for streams, `choices[].message.content` otherwise), with SSE ending in `data: [DONE]`.
- Text is redacted per choice; tool-call arguments and other non-`content` fields are passed through unchanged.