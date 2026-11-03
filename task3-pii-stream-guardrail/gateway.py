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


def transform_text(text: str) -> str:
    """Placeholder for the guardrail. Step 2: pass text through unchanged."""
    return text


async def relay_stream(upstream: httpx.Response):
    """Read the provider's SSE stream, rewrite each delta, and yield new SSE events."""
    parser = SSEParser()
    async for chunk in upstream.aiter_bytes():          # raw network chunks, any size
        for data in parser.feed(chunk):                  # complete events only
            if data == "[DONE]":
                yield format_event("[DONE]")
                continue
            event = json.loads(data)
            for choice in event.get("choices", []):
                delta = choice.get("delta", {})
                if delta.get("content"):
                    delta["content"] = transform_text(delta["content"])
            yield format_event(json.dumps(event))


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
                message["content"] = transform_text(message["content"])
        return JSONResponse(body, status_code=upstream.status_code)

    upstream = await client.send(client.build_request("POST", LLM_URL, json=payload), stream=True)
    return StreamingResponse(
        relay_stream(upstream),
        media_type="text/event-stream",
        background=BackgroundTask(upstream.aclose),
    )


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("GATEWAY_PORT", "8100")), log_level="warning")