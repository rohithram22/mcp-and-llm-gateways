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
from fastapi.responses import JSONResponse

from auth import AuthError, authenticate
from policy import check_request

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mcp-gateway")

DOWNSTREAM_URL = os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp")

# Only these headers are passed between client and downstream. Everything else
# (Host, cookies, and above all the client's Authorization token) stays at the gateway.
FORWARD_REQUEST_HEADERS = {"content-type", "accept", "mcp-session-id", "mcp-protocol-version", "last-event-id"}
FORWARD_RESPONSE_HEADERS = {"content-type", "mcp-session-id"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One shared HTTP client for the whole app: it reuses connections to the downstream.
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="MCP Security Gateway", lifespan=lifespan)


async def forward(request: Request, body: bytes) -> Response:
    """Send the request to the downstream server and relay its reply unchanged."""
    headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD_REQUEST_HEADERS}
    upstream = await request.app.state.http.request(request.method, DOWNSTREAM_URL, content=body, headers=headers)
    reply_headers = {k: v for k, v in upstream.headers.items() if k.lower() in FORWARD_RESPONSE_HEADERS}
    return Response(content=upstream.content, status_code=upstream.status_code, headers=reply_headers)


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