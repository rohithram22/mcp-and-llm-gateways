"""LLM Gateway with a streaming guardrail.

POST /v1/chat/completions (OpenAI-style). Forwards to the LLM provider and streams the
reply back, rewriting each delta on the way through.

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
from starlette.background import BackgroundTask

from patterns import redact
from redactor import StreamRedactor
from sse import SSEParser, format_event

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("llm-gateway")

LLM_URL = os.environ.get("LLM_URL", "http://127.0.0.1:9100/v1/chat/completions")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=60.0, write=30.0, pool=5.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="LLM Gateway", lifespan=lifespan)


async def relay_stream(upstream: httpx.Response):
    """Read the provider's SSE stream, redact each delta, and yield new SSE events.

    One StreamRedactor per choice: with n > 1 the provider interleaves several
    independent replies in one stream, and their text must never be mixed.
    """
    parser = SSEParser()
    redactors: dict[int, StreamRedactor] = {}
    template: dict | None = None  # last event seen, used as the shape for flush events

    def flush_event(index: int, text: str) -> bytes:
        event = {**template, "choices": [{"index": index, "delta": {"content": text}, "finish_reason": None}]}
        return format_event(json.dumps(event))

    def flush_choice(index: int):
        redactor = redactors.pop(index, None)
        if redactor is None:
            return None
        text = redactor.flush()
        log.info("choice %d finished: %d value(s) redacted, peak buffer %d chars",
                 index, redactor.redactions, redactor.max_buffered)
        return flush_event(index, text) if text else None

    async for chunk in upstream.aiter_bytes():
        for data in parser.feed(chunk):
            if data == "[DONE]":
                for index in list(redactors):              # provider ended without finish_reason
                    if (event := flush_choice(index)) is not None:
                        yield event
                yield format_event("[DONE]")
                continue

            event = json.loads(data)
            template = {k: v for k, v in event.items() if k != "choices"}
            has_output = False
            for choice in event.get("choices", []):
                index = choice.get("index", 0)
                delta = choice.get("delta", {})
                if delta.get("content"):
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

    for index in list(redactors):                          # stream cut off mid-way: still flush
        if (event := flush_choice(index)) is not None:
            yield event


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    payload = await request.json()
    client: httpx.AsyncClient = request.app.state.http
    log.info("completion request model=%s stream=%s", payload.get("model"), bool(payload.get("stream")))

    if not payload.get("stream"):
        upstream = await client.post(LLM_URL, json=payload)
        body = upstream.json()
        for choice in body.get("choices", []):
            message = choice.get("message", {})
            if message.get("content"):
                message["content"] = redact(message["content"])
        return JSONResponse(body, status_code=upstream.status_code)

    upstream = await client.send(client.build_request("POST", LLM_URL, json=payload), stream=True)
    return StreamingResponse(
        relay_stream(upstream),
        media_type="text/event-stream",
        background=BackgroundTask(upstream.aclose),
    )


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("GATEWAY_PORT", "8100")), log_level="warning")