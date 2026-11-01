# Task 1: Customer Support MCP Server

A Python MCP server, built on the official `mcp` SDK (v2), that exposes two tools over the **stdio** transport, with strict Pydantic input validation and standard JSON-RPC error mapping.

| Tool | Inputs |
|---|---|
| `get_customer_record` | `customer_id`: `CUST-` followed by exactly 5 digits |
| `trigger_refund` | `customer_id`; `amount`: positive number, at most 2 decimal places; `reason`: at least 10 characters |

## Run it

Requires Python 3.10 or newer. From the repository root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r task1-mcp-server/requirements.txt

python -m pytest task1-mcp-server -v                 # 38 end-to-end tests over real stdio
python task1-mcp-server/demo_client.py 2>/dev/null   # calls the server with the official MCP client
```

To explore it interactively in the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector python task1-mcp-server/server.py
```

## Files

| File | Purpose |
|---|---|
| `schemas.py` | Pydantic input models. The single source of truth for both validation and the `inputSchema` advertised in `tools/list` |
| `data.py` | In-memory mock customer database, plus the business exceptions `CustomerNotFound` and `RefundRejected` |
| `server.py` | stderr-only logging, the `tools/list` and `tools/call` handlers, and the stdio entry point |
| `demo_client.py` | Exercises every response type through the official MCP client |
| `tests/test_server.py` | Starts the server as a subprocess and asserts on the raw JSON-RPC wire traffic |
| `tests/stray_print_server.py` | The server with a deliberate `print()` injected, used to prove stdout stays clean |

## Design

### Error mapping

Every failure is sorted into one of three categories, each with a distinct response:

| Situation | Response | Rationale |
|---|---|---|
| Arguments fail validation, or the tool name is unknown | JSON-RPC error **-32602** Invalid params, with per-field details in `error.data.errors` | The request itself is malformed |
| Valid request, but a business rule rejects it (customer not found, refund exceeds balance) | A normal result with **`isError: true`** and a readable message | The protocol exchange succeeded and the action failed. The LLM sees the message and can recover |
| Unexpected exception | JSON-RPC error **-32603** Internal error, with a generic message | The full traceback goes to stderr only, so no paths, hosts, or stack details reach the client |

All validation failures are reported in a single response, so a caller can fix everything in one retry. The error payload contains field names and messages only. It never echoes the raw input back.

### Validation

One Pydantic model per tool is used both to validate arguments and to generate the advertised JSON Schema (`model_json_schema()`), so the contract the LLM sees can never drift from the contract that is enforced.

| Rule | Rejects |
|---|---|
| `strict=True` | Silent type coercion: `"50"` or `true` as an amount, `12345` as a customer ID |
| `extra="forbid"` | Unknown fields such as `"approved_by": "me"` (advertised as `additionalProperties: false`) |
| Anchored pattern `^CUST-\d{5}$` | `CUST-1234`, `CUST-123456`, `cust-12345`, `CUST-ABCDE`, leading or trailing whitespace, trailing newline |
| `gt=0`, `le=1_000_000`, `allow_inf_nan=False`, 2-decimal check | `0`, negatives, `10.999`, implausibly large amounts, `NaN` and `Infinity` |
| `min_length=10` plus a whitespace-stripping validator, `max_length=500` | `"too short"`, a reason made only of spaces, space-padded short reasons, oversized input |

### STDIO isolation

stdout is reserved for JSON-RPC. This is enforced in three layers:

1. **All logging goes to stderr.** `logging.basicConfig(stream=sys.stderr)` is configured before anything else runs, and the server code contains no `print()` calls.
2. **The SDK's stdout diversion.** While serving, MCP SDK v2 points file descriptor 1 at stderr and writes protocol messages through a private duplicate of the real stdout, so a stray `print()` inside a handler is diverted away from the wire.
3. **A flush before the stream is restored.** When stdout is a pipe, Python buffers `print()` output. Without intervention, the buffer is flushed at process exit, *after* the SDK has restored the real stdout, and stray output reaches the client. `server.main()` flushes `sys.stdout` in a `finally` block while the diversion is still active.

The tests verify this end to end. `test_stdout_is_pure_jsonrpc_and_logs_go_to_stderr` parses every stdout line as JSON-RPC and confirms the log lines appeared on stderr. `test_stray_print_cannot_corrupt_stdout` runs a copy of the server with a `print()` injected into a tool handler and asserts that the wire stays clean. That test caught the buffering gap described in layer 3.

## Notes and trade-offs

- **Low-level `Server` instead of the `@mcp.tool()` decorator.** The decorator API validates arguments implicitly and chooses its own error format. The low-level API keeps validation and error-code mapping explicit, which is what this task evaluates.
- **Validation errors as -32602 vs `isError` results.** Recent MCP guidance suggests reporting input-validation failures as tool results with `isError: true`, so the model can self-correct. This task asks for JSON-RPC error codes, so -32602 is used. Switching would be a one-line change in `call_tool`.
- **Production extensions:** an idempotency key on `trigger_refund` so a retried call cannot refund twice, authorization (see Task 2's gateway), a transactional datastore, and an audit log of issued refunds.

## Assumptions

- `XXXXX` in `CUST-XXXXX` means exactly five digits.
- Amounts are in a single currency with cent precision. The 1,000,000 ceiling is a sanity limit.
- Mock data lives in memory and resets each time the server starts.
