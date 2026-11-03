"""LLM Gateway with a streaming PII guardrail.

POST /v1/chat/completions (OpenAI-style). Forwards to the LLM provider and streams the
reply back, redacting emails, SSNs and card numbers on the way through.

Run:  python task3-pii-stream-guardrail/gateway.py      -> http://127.0.0.1:8100
"""

import json
import logging
import os
import sys
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from patterns import redact
from redactor import StreamRedactor
from sse import SSEParser, format_event

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("llm-gateway")

LLM_URL = os.environ.get("LLM_URL", "http://127.0.0.1:9100/v1/chat/completions")

# read = the longest silence allowed between two chunks of the provider's reply.
UPSTREAM_TIMEOUT = httpx.Timeout(
    connect=5.0, read=float(os.environ.get("UPSTREAM_READ_TIMEOUT", "60")), write=30.0, pool=5.0
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    yield
    await app.state.http.aclose()


app = FastAPI(title="LLM Gateway", lifespan=lifespan)


# ----------------------------------------------------------------------------- errors
def gateway_error(status: int, message: str, error_type: str) -> JSONResponse:
    """Error before streaming started: a normal HTTP error with an OpenAI-style body."""
    return JSONResponse(status_code=status, content={"error": {"message": message, "type": error_type}})


def stream_error_event(message: str) -> bytes:
    """Error after streaming started. The HTTP status (200) is already sent and cannot
    change, so the failure is reported in-band as a final SSE event."""
    return format_event(json.dumps({"error": {"message": message, "type": "upstream_error"}}))


# ----------------------------------------------------------------------------- streaming
async def relay_stream(upstream: httpx.Response):
    """Read the provider's SSE stream, redact each delta, and yield new SSE events.

    One StreamRedactor per choice: with n > 1 the provider interleaves several
    independent replies in one stream, and their text must never be mixed.
    """
    parser = SSEParser()
    redactors: dict[int, StreamRedactor] = {}
    template: dict = {}       # last event's metadata (id, model...), reused for flush events
    completed = False         # did the provider finish properly?

    def flush_choice(index: int) -> bytes | None:
        """Normal end of a choice: decide on held text and release it as one more event."""
        redactor = redactors.pop(index, None)
        if redactor is None:
            return None
        text = redactor.flush()
        log.info("choice %d finished: %d value(s) redacted, peak buffer %d chars",
                 index, redactor.redactions, redactor.max_buffered)
        if not text:
            return None
        event = {**template, "choices": [{"index": index, "delta": {"content": text}, "finish_reason": None}]}
        return format_event(json.dumps(event))

    try:
        async for chunk in upstream.aiter_bytes():
            for data in parser.feed(chunk):
                if data == "[DONE]":
                    for index in list(redactors):              # provider ended without finish_reason
                        if (event := flush_choice(index)) is not None:
                            yield event
                    completed = True
                    yield format_event("[DONE]")
                    continue

                try:
                    event = json.loads(data)
                except json.JSONDecodeError:
                    # We cannot redact what we cannot parse, so it is never forwarded.
                    log.warning("dropped malformed event from provider (%d bytes)", len(data))
                    continue

                template = {k: v for k, v in event.items() if k != "choices"}
                has_output = False
                for choice in event.get("choices", []):
                    index = choice.get("index", 0)
                    delta = choice.get("delta") or {}
                    if isinstance(delta.get("content"), str) and delta["content"]:
                        delta["content"] = redactors.setdefault(index, StreamRedactor()).feed(delta["content"])
                        if not delta["content"]:
                            del delta["content"]               # everything held back: nothing to send yet
                    if choice.get("finish_reason") is not None:
                        # Release this choice's held text BEFORE the event that closes it.
                        if (flushed := flush_choice(index)) is not None:
                            yield flushed
                    if delta or choice.get("finish_reason") is not None:
                        has_output = True
                if has_output or not event.get("choices"):
                    yield format_event(json.dumps(event))

    except httpx.ReadTimeout:
        log.error("provider went silent mid-stream (read timeout)")
        yield stream_error_event("The upstream model stopped responding.")
    except httpx.HTTPError as exc:
        log.error("provider stream broke: %r", exc)
        yield stream_error_event("The upstream model connection was interrupted.")
    finally:
        # Runs on success, on error, and when the CLIENT disconnects (the generator is
        # closed): always release the provider connection so it is not left streaming to nobody.
        await upstream.aclose()

    if not completed and redactors:
        # The stream ended abnormally. Held text may be the first half of a real SSN
        # ("123-4"); flushing it would leak a partial value, so it is discarded (fail closed).
        held = sum(len(r._buffer) for r in redactors.values())
        log.warning("stream incomplete: discarded %d held character(s) instead of releasing them", held)


# ----------------------------------------------------------------------------- endpoint
@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return gateway_error(400, "Request body must be valid JSON.", "invalid_request_error")
    if not isinstance(payload, dict):
        return gateway_error(400, "Request body must be a JSON object.", "invalid_request_error")

    client: httpx.AsyncClient = request.app.state.http
    streaming = bool(payload.get("stream"))
    log.info("completion request model=%s stream=%s", payload.get("model"), streaming)

    try:
        upstream = await client.send(client.build_request("POST", LLM_URL, json=payload), stream=True)
    except httpx.TimeoutException:
        log.error("provider timed out before responding")
        return gateway_error(504, "The upstream model timed out.", "upstream_timeout")
    except httpx.HTTPError as exc:
        log.error("provider unreachable: %r", exc)
        return gateway_error(502, "The upstream model is unavailable.", "upstream_unavailable")

    if upstream.status_code != 200:
        # Provider error bodies can contain internal details, or even echo the prompt
        # (and the PII in it), so they are logged by size only and never passed on.
        body = await upstream.aread()
        await upstream.aclose()
        log.error("provider returned HTTP %d (%d bytes)", upstream.status_code, len(body))
        if upstream.status_code == 429:
            return gateway_error(429, "The upstream model is rate limited. Retry later.", "rate_limited")
        if 400 <= upstream.status_code < 500:
            return gateway_error(upstream.status_code, "The upstream model rejected the request.", "upstream_rejected")
        return gateway_error(502, "The upstream model returned an error.", "upstream_error")

    if not streaming:
        try:
            body = json.loads(await upstream.aread())
        except (httpx.HTTPError, json.JSONDecodeError):
            return gateway_error(502, "The upstream model returned an invalid response.", "upstream_error")
        finally:
            await upstream.aclose()
        for choice in body.get("choices", []):
            message = choice.get("message") or {}
            if isinstance(message.get("content"), str):
                message["content"] = redact(message["content"])
        return JSONResponse(body)

    return StreamingResponse(relay_stream(upstream), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("GATEWAY_PORT", "8100")), log_level="warning")