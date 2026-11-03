"""Mock LLM provider with an OpenAI-style chat completions API.

POST /v1/chat/completions  {"model": "...", "messages": [...], "stream": true}

Replies are canned and full of PII, and streamed in tiny chunks so that sensitive
values are split across chunk boundaries, which is the hard case for the gateway.

Run:  python task3-pii-stream-guardrail/mock_llm.py      -> http://127.0.0.1:9100
"""

import asyncio
import json
import os
import time
import uuid

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

CHUNK_DELAY = float(os.environ.get("MOCK_CHUNK_DELAY", "0.03"))  # seconds between chunks

# The reply is chosen by a keyword in the last user message.
REPLIES = {
    "ssn": "Sure. The SSN on file is 123-45-6789, and the backup record shows 987-65-4321.",
    "email": "You can reach Ada at ada.lovelace@example.com or the team at support@acme.io anytime.",
    "card": "The card ending in 1111 is 4111 1111 1111 1111. An older one, 3782-822463-10005, was cancelled.",
    "mixed": (
        "Customer profile: email jane.doe@example.org, SSN 078-05-1120, "
        "card 5555 5555 5555 4444. Order number 1234 5678 9012 3456 is not a card."
    ),
    "clean": "The weather in Paris is sunny with a light breeze. Nothing sensitive here at all.",
}
DEFAULT_REPLY = REPLIES["mixed"]

app = FastAPI(title="Mock LLM provider")


def pick_reply(messages: list) -> str:
    last = next((m.get("content", "") for m in reversed(messages) if m.get("role") == "user"), "")
    for keyword, reply in REPLIES.items():
        if keyword in str(last).lower():
            return reply
    return DEFAULT_REPLY


def split_into_chunks(text: str, size: int) -> list[str]:
    """Fixed-size pieces: crude, but guarantees PII gets cut mid-value."""
    return [text[i : i + size] for i in range(0, len(text), size)]


def sse(data: str) -> bytes:
    return f"data: {data}\n\n".encode()


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    payload = await request.json()
    model = payload.get("model", "mock-1")
    reply = pick_reply(payload.get("messages", []))
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
    created = int(time.time())

    if not payload.get("stream"):
        return JSONResponse({
            "id": completion_id, "object": "chat.completion", "created": created, "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
        })

    chunk_size = int(payload.get("mock_chunk_size", 5))  # test hook; real providers ignore unknown fields

    async def stream():
        def event(delta: dict, finish_reason=None) -> bytes:
            return sse(json.dumps({
                "id": completion_id, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
            }))

        yield event({"role": "assistant", "content": ""})
        for piece in split_into_chunks(reply, chunk_size):
            await asyncio.sleep(CHUNK_DELAY)
            yield event({"content": piece})
        yield event({}, finish_reason="stop")
        yield sse("[DONE]")

    return StreamingResponse(stream(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("MOCK_LLM_PORT", "9100")), log_level="warning")