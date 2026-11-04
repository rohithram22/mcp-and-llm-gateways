# AI Field Developer / Solutions Engineer Assessment

Four Python implementations covering MCP servers, MCP and LLM gateways, streaming guardrails, and resilient model routing. Each task is self-contained in its own folder, with its own README, requirements, mock services, and tests.

| Task | Folder | What it is | Tests |
|---|---|---|---|
| 1 | [`task1-mcp-server`](task1-mcp-server/) | MCP server over **stdio** with two tools, strict **Pydantic** validation, standard JSON-RPC errors, and stdout reserved for JSON-RPC | 38 |
| 2 | [`task2-mcp-gateway`](task2-mcp-gateway/) | MCP security **gateway proxy**: JWT bearer auth, `admin_*` tools blocked for non-admins with `-32001` before reaching the server, fail-closed JSON-RPC inspection | 56 |
| 3 | [`task3-pii-stream-guardrail`](task3-pii-stream-guardrail/) | LLM gateway that **redacts emails, SSNs, and card numbers while streaming**, with a bounded rolling buffer and about one chunk of added latency | 335 |
| 4 | [`task4-llm-router`](task4-llm-router/) | LLM router with a **token-aware sliding-window rate limiter on SQLite** and **3000 ms / 429 failover** to a backup provider, with sanitized standard errors | 59 |

## Quick start

Requires **Python 3.10 or newer**.

```bash
git clone <this repository> && cd ai-assessment
python3 -m venv .venv && source .venv/bin/activate
pip install -r task1-mcp-server/requirements.txt -r task2-mcp-gateway/requirements.txt \
            -r task3-pii-stream-guardrail/requirements.txt -r task4-llm-router/requirements.txt

python -m pytest          # all 488 tests; each suite starts its own mock services
```

Run a single task's tests with `python -m pytest <task-folder> -v`. Use `python -m pytest` rather than a bare `pytest`, so the tests run under the virtual environment's Python. Each task's README explains how to run it by hand.

No API keys or external services are needed: every task ships with mock servers that reproduce the behaviour under test, including failures (429s, timeouts, broken streams, malformed payloads).

## Common principles

The same few ideas run through all four tasks:

- **Fail closed.** Anything the system can't fully understand is rejected rather than passed through: unparseable JSON-RPC, batches and duplicate keys (Task 2), unparseable stream events and partial values held when a stream breaks (Task 3), requests that can't be priced (Task 4).
- **Specific logs, generic responses.** Clients receive standard error codes and messages; stack traces, upstream bodies, hostnames, and the reasons behind authentication failures go only to stderr or the logs, correlated by request ID where relevant.
- **Standard protocol semantics.** JSON-RPC error codes (`-32602`, `-32603`, `-32700`, `-32600`, and the required `-32001`), MCP's `isError` for business failures, OpenAI-compatible request, stream, and error formats, and HTTP status codes chosen for what the client should do next (401, 429 with `Retry-After`, 502, 504).
- **Prove the claims.** Each requirement is backed by a test, often at the wire level against real processes. For example: stdout parsed line by line as JSON-RPC (Task 1), the downstream server's own log searched to show a blocked call never arrived (Task 2), streaming output compared with whole-text redaction at every chunk size from 1 to 30 (Task 3), and concurrent bursts from multiple processes against one SQLite file (Task 4). Randomized and mutation testing were used to check that the tests catch real bugs, and several real defects found that way are documented in the task READMEs.

## Repository layout

```
task1-mcp-server/            server.py, schemas.py, data.py, demo_client.py, tests/
task2-mcp-gateway/           gateway.py, auth.py, policy.py, mock_server.py, make_token.py, demo_client.py, tests/
task3-pii-stream-guardrail/  gateway.py, redactor.py, patterns.py, sse.py, mock_llm.py, stream_client.py, tests/
task4-llm-router/            gateway.py, limiter.py, router.py, db.py, tokens.py, mock_provider.py, tests/
```