"""Mock downstream MCP server (the thing the gateway protects).

Speaks real MCP over Streamable HTTP using the official SDK.
Run:  python task2-mcp-gateway/mock_server.py      -> http://127.0.0.1:9000/mcp
"""

import json
import logging
import os
import sys

import mcp.types as types
import uvicorn
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import MCPError

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mock-mcp")

TOOLS = [
    types.Tool(
        name="get_weather",
        description="Get the current weather for a city.",
        inputSchema={"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
    ),
    types.Tool(
        name="list_users",
        description="List user accounts.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="admin_reset_key",
        description="ADMIN ONLY: rotate a user's API key.",
        inputSchema={"type": "object", "properties": {"user": {"type": "string"}}, "required": ["user"]},
    ),
    types.Tool(
        name="admin_delete_user",
        description="ADMIN ONLY: delete a user account.",
        inputSchema={"type": "object", "properties": {"user": {"type": "string"}}, "required": ["user"]},
    ),
]


async def list_tools(ctx, params) -> types.ListToolsResult:
    log.info("tools/list")
    return types.ListToolsResult(tools=TOOLS)


async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    # This log line is how we prove a blocked call never reached the server.
    log.info("tools/call name=%s arguments=%s", params.name, params.arguments)
    args = params.arguments or {}

    if params.name == "get_weather":
        payload = {"city": args.get("city"), "forecast": "sunny", "temp_c": 21}
    elif params.name == "list_users":
        payload = {"users": ["alice", "bob"]}
    elif params.name == "admin_reset_key":
        payload = {"user": args.get("user"), "new_key": "sk-NEW-0000", "status": "rotated"}
    elif params.name == "admin_delete_user":
        payload = {"user": args.get("user"), "status": "deleted"}
    else:
        raise MCPError(code=types.INVALID_PARAMS, message=f"Unknown tool: {params.name}")

    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(payload))],
        structuredContent=payload,
    )


server = Server("mock-downstream-mcp", version="1.0.0", on_list_tools=list_tools, on_call_tool=call_tool)
# MOCK_SSE=1 makes the server answer with Server-Sent Event streams instead of plain JSON,
# which lets us check that the gateway passes streams through untouched.
app = server.streamable_http_app(json_response=os.environ.get("MOCK_SSE") != "1")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("MOCK_PORT", "9000")), log_level="warning")