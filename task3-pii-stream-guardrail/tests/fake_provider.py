"""Test-only LLM provider that streams exactly the events a test asks for.

POST /v1/chat/completions with a "script" field:
    {"stream": true, "script": ["<data payload>", ...], "delay": 0.01}
streams each payload as one SSE event. "forever": true streams a slow endless story
(for disconnect tests). GET /active reports how many streams are still running, so a
test can check the gateway really hung up on us.
"""

import asyncio
import json
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()
ACTIVE = {"streams": 0}


@app.get("/active")
async def active():
    return ACTIVE


@app.post("/v1/chat/completions")
async def completions(request: Request):
    payload = await request.json()
    delay = float(payload.get("delay", 0.01))

    async def stream():
        ACTIVE["streams"] += 1
        try:
            if payload.get("forever"):
                n = 0
                while True:
                    await asyncio.sleep(0.05)
                    event = {"id": "x", "choices": [{"index": 0, "delta": {"content": f"word{n} "}, "finish_reason": None}]}
                    yield f"data: {json.dumps(event)}\n\n".encode()
                    n += 1
            for data in payload.get("script", []):
                await asyncio.sleep(delay)
                yield f"data: {data}\n\n".encode()
        finally:
            ACTIVE["streams"] -= 1

    return StreamingResponse(stream(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ["FAKE_PORT"]), log_level="warning")