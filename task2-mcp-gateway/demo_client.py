"""Use the official MCP client through the gateway, once as a viewer and once as an admin.

Needs mock_server.py and gateway.py running. Run:  python task2-mcp-gateway/demo_client.py
"""

import anyio
from mcp.client import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.exceptions import MCPError

from auth import issue_token

GATEWAY_URL = "http://127.0.0.1:8000/mcp"


async def run_as(role: str, user: str) -> None:
    print(f"\n=== {user} ({role}) ===")
    http = create_mcp_http_client(headers={"Authorization": f"Bearer {issue_token(user, role)}"})
    async with http, streamable_http_client(GATEWAY_URL, http_client=http) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            print("tools/list ->", [t.name for t in tools.tools])

            result = await session.call_tool("get_weather", {"city": "Paris"})
            print("get_weather ->", result.structured_content)

            try:
                result = await session.call_tool("admin_reset_key", {"user": "bob"})
                print("admin_reset_key ->", result.structured_content)
            except MCPError as exc:
                print(f"admin_reset_key -> blocked by gateway: code={exc.code} message={exc.message!r}")


async def main() -> None:
    await run_as("viewer", "bob")
    await run_as("admin", "alice")


if __name__ == "__main__":
    anyio.run(main)