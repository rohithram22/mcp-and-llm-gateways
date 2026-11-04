"""LLM Gateway: per-tenant token rate limiting + primary/secondary model failover.

POST /v1/chat/completions   (OpenAI-style; Authorization: Bearer <tenant API key>)

Request lifecycle:
  1. authenticate the tenant by API key
  2. price the request (prompt estimate + max_tokens) and RESERVE it in the sliding window
  3. route: primary under a 3 s deadline, failing over to the secondary
  4. SETTLE the reservation to the real usage reported by whichever model answered
     (or RELEASE it if none did)

Every error uses one payload shape and carries a request_id; details go to the log only.

Run:  python task4-llm-router/gateway.py      -> http://127.0.0.1:8200
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

import db
import router
from limiter import RateLimiter
from tokens import InvalidRequest, reservation_for

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("llm-gateway")

# Longest silence allowed between two chunks of a reply that has already started.
STREAM_IDLE_TIMEOUT = float(os.environ.get("STREAM_IDLE_TIMEOUT", "30"))

# Demo tenants, so the gateway is usable out of the box. Real keys would be provisioned separately.
DEMO_TENANTS = [("acme", "sk-acme-123", 50_000), ("tiny", "sk-tiny-123", 2_000)]

limiter = RateLimiter()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    if os.environ.get("SEED_DEMO_TENANTS", "1") == "1":
        for tenant_id, key, tpm in DEMO_TENANTS:
            db.add_tenant(tenant_id, key, tpm)
    # No read timeout here: how long to wait for a provider is decided by the router's
    # deadline (before the reply starts) and by STREAM_IDLE_TIMEOUT (after it starts).
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=None, write=10.0, pool=5.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="LLM Router Gateway", lifespan=lifespan)


# ----------------------------------------------------------------------------- errors
def gateway_error(status: int, error_type: str, message: str, request_id: str,
                  retry_after: int | None = None) -> JSONResponse:
    """The ONE shape every error takes. Never contains upstream bodies, URLs or tracebacks."""
    headers = {"X-Request-ID": request_id}
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return JSONResponse(
        status_code=status,
        content={"error": {"type": error_type, "message": message, "request_id": request_id}},
        headers=headers,
    )


def new_request_id() -> str:
    return f"req_{uuid.uuid4().hex[:16]}"


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    # Unknown paths, wrong methods: same error shape as everything else.
    return gateway_error(exc.status_code, "not_found" if exc.status_code == 404 else "invalid_request",
                         "No such endpoint or method.", new_request_id())


@app.exception_handler(Exception)
async def unexpected_error_handler(request: Request, exc: Exception):
    # A bug: log the full traceback against a request id, send the client nothing internal.
    request_id = new_request_id()
    log.exception("unhandled error request_id=%s", request_id)
    return gateway_error(500, "internal_error", "Internal gateway error.", request_id)


def failure_response(result: router.RouteResult, request_id: str) -> JSONResponse:
    """No model answered: turn the attempt history into ONE sanitised error."""
    outcomes = {a.outcome for a in result.attempts}
    if outcomes == {"rate_limited"}:
        return gateway_error(503, "upstream_overloaded",
                             "All model providers are rate limited. Retry shortly.", request_id, retry_after=5)
    if result.attempts and result.attempts[-1].outcome == "rejected":
        return gateway_error(400, "upstream_rejected",
                             "The model provider rejected the request.", request_id)
    if "timeout" in outcomes and outcomes <= {"timeout", "rate_limited"}:
        return gateway_error(504, "upstream_timeout", "The model providers did not respond in time.", request_id)
    return gateway_error(502, "upstream_unavailable", "The model providers are unavailable.", request_id)


# ----------------------------------------------------------------------------- usage
def usage_tokens(event_or_body: dict) -> int | None:
    usage = event_or_body.get("usage")
    if isinstance(usage, dict) and isinstance(usage.get("total_tokens"), int):
        return usage["total_tokens"]
    return None


async def relay_stream(result: router.RouteResult, reservation_id: int, request_id: str, provider: str):
    """Pass the stream through unchanged while watching for the final `usage` report."""
    response = result.response
    buffer = b""
    actual: int | None = None
    completed = False

    def scan(chunk: bytes) -> None:
        nonlocal buffer, actual, completed
        buffer += chunk
        while b"\n\n" in buffer:
            raw_event, buffer = buffer.split(b"\n\n", 1)
            for line in raw_event.split(b"\n"):
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                if data == b"[DONE]":
                    completed = True
                    continue
                try:
                    tokens = usage_tokens(json.loads(data))
                except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                    continue
                if tokens is not None:
                    actual = tokens

    def interrupted(message: str) -> bytes:
        # The stream already started, so no failover (the client has text from this model)
        # and no status change (200 is sent). Report it in-band, in the standard shape.
        error = {"error": {"type": "upstream_interrupted", "message": message, "request_id": request_id}}
        return f"data: {json.dumps(error)}\n\n".encode()

    try:
        scan(result.first_chunk)
        yield result.first_chunk
        while True:
            try:
                chunk = await asyncio.wait_for(anext(result.chunks), STREAM_IDLE_TIMEOUT)
            except StopAsyncIteration:
                break
            except TimeoutError:
                log.error("stream idle for %.0fs request_id=%s provider=%s", STREAM_IDLE_TIMEOUT, request_id, provider)
                yield interrupted("The model stopped responding mid-reply.")
                break
            except httpx.HTTPError as exc:
                log.error("stream broke request_id=%s provider=%s: %r", request_id, provider, exc)
                yield interrupted("The model connection was interrupted mid-reply.")
                break
            scan(chunk)
            yield chunk
    finally:
        await response.aclose()
        if actual is not None:
            await limiter.asettle(reservation_id, actual)
        # No usage report (stream broke or client left): the true cost is unknown, so the
        # worst-case reservation stays charged. That can overcharge, but never undercounts.
        log.info("stream finished request_id=%s provider=%s completed=%s tokens=%s",
                 request_id, provider, completed, actual if actual is not None else "unknown(kept reservation)")


# ----------------------------------------------------------------------------- endpoint
@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    request_id = new_request_id()

    # 1. Who is calling?
    scheme, _, api_key = request.headers.get("authorization", "").partition(" ")
    tenant = await _find_tenant(api_key.strip()) if scheme.lower() == "bearer" and api_key.strip() else None
    if tenant is None:
        return gateway_error(401, "authentication_error", "Missing or invalid API key.", request_id)
    tenant_id, tokens_per_minute = tenant

    # 2. What will it cost at most?
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise InvalidRequest("body must be a JSON object")
        cost = reservation_for(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return gateway_error(400, "invalid_request", "Request body must be valid JSON.", request_id)
    except InvalidRequest as exc:
        return gateway_error(400, "invalid_request", f"Invalid request: {exc}.", request_id)

    # 3. Can the tenant afford it right now?
    decision = await limiter.areserve(tenant_id, tokens_per_minute, cost)
    if not decision.allowed:
        if decision.retry_after is None:
            return gateway_error(400, "request_too_large",
                                 f"This request needs up to {cost} tokens, more than the limit of "
                                 f"{tokens_per_minute} tokens per minute. Lower max_tokens.", request_id)
        log.info("rate limited request_id=%s tenant=%s used=%d cost=%d", request_id, tenant_id, decision.used, cost)
        return gateway_error(429, "rate_limit_exceeded",
                             f"Token rate limit of {tokens_per_minute} per minute exceeded. "
                             f"Retry after {decision.retry_after} s.", request_id, retry_after=decision.retry_after)

    # 4. Route to a model. From here on, the reservation must be settled or released on every path.
    stream = bool(payload.get("stream"))
    try:
        result = await router.route(request.app.state.http, payload, stream)
    except BaseException:
        await limiter.arelease(decision.reservation_id)
        raise

    if not result.ok:
        await limiter.arelease(decision.reservation_id)      # nobody answered: full refund
        log.warning("no provider answered request_id=%s attempts=%s", request_id,
                    [(a.provider, a.outcome, a.status, a.elapsed_ms) for a in result.attempts])
        return failure_response(result, request_id)

    log.info("routed request_id=%s tenant=%s provider=%s attempts=%s", request_id, tenant_id, result.provider,
             [(a.provider, a.outcome, a.elapsed_ms) for a in result.attempts])

    if stream:
        return StreamingResponse(
            relay_stream(result, decision.reservation_id, request_id, result.provider),
            media_type="text/event-stream", headers={"X-Request-ID": request_id},
        )

    try:
        body = json.loads(result.body)
        actual = usage_tokens(body)
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        await limiter.arelease(decision.reservation_id)
        log.error("invalid JSON from provider=%s request_id=%s", result.provider, request_id)
        return gateway_error(502, "upstream_invalid_response", "The model returned an invalid response.", request_id)

    if actual is not None:
        await limiter.asettle(decision.reservation_id, actual)  # reconcile: charge what was really used
    return Response(content=result.body, media_type="application/json", headers={"X-Request-ID": request_id})


async def _find_tenant(api_key: str):
    return await asyncio.to_thread(db.find_tenant, api_key)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("GATEWAY_PORT", "8200")), log_level="warning")