"""Test-only downstream that reports what the gateway sent it, and can misbehave on purpose.

  POST body containing "__sleep__"  -> waits 5 s before answering (to trigger the gateway's timeout)
  GET                               -> an SSE stream of 3 events, 0.5 s apart (to prove streaming)
  any other POST                    -> JSON-RPC result listing the headers it received
"""

import asyncio
import json
import os

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route


async def mcp(request: Request):
    if request.method == "GET":
        async def events():
            for i in range(3):
                yield f"event: message\ndata: {json.dumps({'n': i})}\n\n".encode()
                await asyncio.sleep(0.5)
        return StreamingResponse(events(), media_type="text/event-stream")

    body = await request.body()
    if b"__sleep__" in body:
        await asyncio.sleep(5)
    message = json.loads(body)
    return JSONResponse({"jsonrpc": "2.0", "id": message.get("id"), "result": {"received_headers": dict(request.headers)}})


app = Starlette(routes=[Route("/mcp", mcp, methods=["GET", "POST"])])

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ["ECHO_PORT"]), log_level="warning")