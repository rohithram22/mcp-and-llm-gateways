"""Mock LLM provider (OpenAI-style) whose behaviour can be switched at runtime.

Run it twice, as the primary and the backup:
    MOCK_NAME=primary   MOCK_PORT=9201 python task4-llm-router/mock_provider.py
    MOCK_NAME=secondary MOCK_PORT=9202 python task4-llm-router/mock_provider.py

Change its behaviour (for the next requests) without restarting:
    curl -X POST localhost:9201/control -d '{"mode": "slow", "delay": 5}'

Modes:
    ok          answer normally
    slow        wait `delay` seconds before answering (to trigger the gateway's 3 s timeout)
    rate429     HTTP 429 Too Many Requests, with Retry-After
    error500    HTTP 500 whose body leaks internals (the gateway must not pass it on)
    stall       streaming only: send a few words, then go silent for `delay` seconds

GET /stats reports how many requests arrived, how many finished, and how many were
abandoned by the caller mid-wait, which proves the gateway really cancels timed-out calls.
"""

import asyncio
import json
import math
import os
import time
import uuid

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

NAME = os.environ.get("MOCK_NAME", "primary")
app = FastAPI(title=f"Mock provider ({NAME})")

state = {"mode": "ok", "delay": 5.0}
stats = {"received": 0, "completed": 0, "abandoned": 0}


def count_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))  # same rough rule real tokenizers average out to


@app.post("/control")
async def control(request: Request):
    state.update(await request.json())
    return state


@app.get("/stats")
async def get_stats():
    return stats


@app.post("/stats/reset")
async def reset_stats():
    stats.update(received=0, completed=0, abandoned=0)
    return stats


async def wait_unless_abandoned(request: Request, seconds: float) -> bool:
    """Sleep, but notice if the caller hangs up. Returns False if it did."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await request.is_disconnected():
            stats["abandoned"] += 1
            return False
        await asyncio.sleep(0.05)
    return True


@app.post("/v1/chat/completions")
async def completions(request: Request):
    stats["received"] += 1
    payload = await request.json()
    mode = state["mode"]

    if mode == "rate429":
        return JSONResponse({"error": {"message": f"Rate limit reached for {NAME}-org-42"}}, status_code=429,
                            headers={"Retry-After": "20"})
    if mode == "error500":
        return JSONResponse({"error": f"Traceback (most recent call last): File '/srv/{NAME}/infer.py', line 512"},
                            status_code=500)
    if mode == "slow" and not await wait_unless_abandoned(request, state["delay"]):
        return JSONResponse({}, status_code=499)  # nobody is listening any more

    prompt = " ".join(str(m.get("content", "")) for m in payload.get("messages", []))
    words = f"Hello from the {NAME} model. You said: {prompt[:60]}".split(" ")
    max_tokens = int(payload.get("max_tokens", 256))
    words = words[:max_tokens]
    reply = " ".join(words)
    usage = {
        "prompt_tokens": count_tokens(prompt),
        "completion_tokens": count_tokens(reply),
    }
    usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
    base = {"id": f"chatcmpl-{uuid.uuid4().hex[:10]}", "created": int(time.time()), "model": f"{NAME}-model"}

    if not payload.get("stream"):
        stats["completed"] += 1
        return JSONResponse({**base, "object": "chat.completion", "usage": usage, "choices": [
            {"index": 0, "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}]})

    async def stream():
        for i, word in enumerate(words):
            if mode == "stall" and i == 3 and not await wait_unless_abandoned(request, state["delay"]):
                return
            await asyncio.sleep(0.02)
            chunk = {**base, "object": "chat.completion.chunk",
                     "choices": [{"index": 0, "delta": {"content": word + " "}, "finish_reason": None}]}
            yield f"data: {json.dumps(chunk)}\n\n".encode()
        final = {**base, "object": "chat.completion.chunk", "usage": usage,
                 "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
        yield f"data: {json.dumps(final)}\n\n".encode()
        yield b"data: [DONE]\n\n"
        stats["completed"] += 1

    return StreamingResponse(stream(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("MOCK_PORT", "9201")), log_level="warning")