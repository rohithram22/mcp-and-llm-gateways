"""MCP Security Gateway: a reverse proxy in front of a downstream MCP server.

Run:  python task2-mcp-gateway/gateway.py      -> http://127.0.0.1:8000/mcp
"""

import logging
import os
import sys
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from auth import AuthError, authenticate
from policy import check_request, request_id_or_none

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mcp-gateway")

DOWNSTREAM_URL = os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp")

# Only these headers are passed between client and downstream. Everything else
# (Host, cookies, and above all the client's Authorization token) stays at the gateway.
FORWARD_REQUEST_HEADERS = {"content-type", "accept", "mcp-session-id", "mcp-protocol-version", "last-event-id"}
FORWARD_RESPONSE_HEADERS = {"content-type", "mcp-session-id", "cache-control"}

# connect: how long to wait for the downstream to accept a connection.
# read:    max silence between chunks of the reply. Plenty for a tool call.
UPSTREAM_TIMEOUT = httpx.Timeout(
    connect=5.0, read=float(os.environ.get("UPSTREAM_READ_TIMEOUT", "60")), write=30.0, pool=5.0
)
# GET opens a long-lived stream for server-initiated messages, which may sit silent for
# minutes by design, so it gets no read timeout.
STREAM_TIMEOUT = httpx.Timeout(connect=5.0, read=None, write=30.0, pool=5.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One shared HTTP client for the whole app: it reuses connections to the downstream.
    app.state.http = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    yield
    await app.state.http.aclose()


app = FastAPI(title="MCP Security Gateway", lifespan=lifespan)


def upstream_error(status: int, body: bytes, message: str) -> JSONResponse:
    """The downstream could not be reached or did not answer in time. No internal detail leaks."""
    return JSONResponse(
        status_code=status,
        content={"jsonrpc": "2.0", "id": request_id_or_none(body), "error": {"code": -32603, "message": message}},
    )


async def forward(request: Request, body: bytes) -> Response:
    """Send the request downstream and stream the reply back as it arrives.

    Streaming (rather than reading the whole reply first) matters because MCP servers may
    answer with Server-Sent Events: a long-lived stream of messages. Buffering it would
    delay every event until the stream ended, or forever for a GET stream.
    """
    client: httpx.AsyncClient = request.app.state.http
    headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD_REQUEST_HEADERS}
    timeout = STREAM_TIMEOUT if request.method == "GET" else UPSTREAM_TIMEOUT
    upstream_request = client.build_request(request.method, DOWNSTREAM_URL, content=body, headers=headers, timeout=timeout)

    try:
        upstream = await client.send(upstream_request, stream=True)
    except httpx.TimeoutException:
        log.error("downstream timed out: %s %s", request.method, DOWNSTREAM_URL)
        return upstream_error(504, body, "Upstream MCP server timed out")
    except httpx.HTTPError as exc:
        log.error("downstream unreachable: %s %s: %r", request.method, DOWNSTREAM_URL, exc)
        return upstream_error(502, body, "Upstream MCP server unavailable")

    reply_headers = {k: v for k, v in upstream.headers.items() if k.lower() in FORWARD_RESPONSE_HEADERS}
    return StreamingResponse(
        upstream.aiter_raw(),               # pass bytes through as they arrive, unmodified
        status_code=upstream.status_code,
        headers=reply_headers,
        background=BackgroundTask(upstream.aclose),  # release the connection when the client is done
    )


def unauthorized() -> JSONResponse:
    """HTTP 401. Deliberately vague: the exact reason is logged, not sent to the caller."""
    return JSONResponse(
        status_code=401,
        content={"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": "Unauthorized: missing, invalid or expired bearer token"}},
        headers={"WWW-Authenticate": 'Bearer realm="mcp-gateway"'},
    )


@app.api_route("/mcp", methods=["POST", "GET", "DELETE"])
async def mcp_endpoint(request: Request) -> Response:
    # 1. Authenticate FIRST, before reading or parsing the body.
    try:
        principal = authenticate(request.headers.get("authorization"))
    except AuthError as exc:
        log.warning("401 %s /mcp: %s", request.method, exc)
        return unauthorized()

    body = await request.body()
    log.info("%s /mcp user=%s role=%s (%d bytes)", request.method, principal.subject, principal.role, len(body))

    # 2. Authorize: inspect the JSON-RPC message (POST bodies only; GET/DELETE have none).
    if request.method == "POST":
        rejection = check_request(principal, body)
        if rejection is not None:
            log.warning(
                "rejected %s for user=%s role=%s: %s",
                rejection.payload["error"]["code"], principal.subject, principal.role, rejection.reason,
            )
            # Answered here: the downstream server never sees this request.
            return JSONResponse(status_code=rejection.http_status, content=rejection.payload)

    # 3. Allowed: forward unchanged.
    return await forward(request, body)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")