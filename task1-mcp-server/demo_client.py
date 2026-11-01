"""Call the server with the official MCP client, the way a real AI agent would.

Run from anywhere:  python task1-mcp-server/demo_client.py
"""

import sys
from pathlib import Path

import anyio
from mcp.client import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError

SERVER = Path(__file__).resolve().parent / "server.py"


async def main() -> None:
    params = StdioServerParameters(command=sys.executable, args=[str(SERVER)])
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()

        tools = await session.list_tools()
        print("Tools:", [t.name for t in tools.tools])

        result = await session.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
        print("\nget_customer_record ->", result.structured_content)

        result = await session.call_tool(
            "trigger_refund", {"customer_id": "CUST-00001", "amount": 25.0, "reason": "Arrived damaged"}
        )
        print("\ntrigger_refund ->", result.structured_content)

        result = await session.call_tool(
            "trigger_refund", {"customer_id": "CUST-00003", "amount": 5.0, "reason": "Goodwill gesture"}
        )
        print(f"\nRefund over balance -> isError={result.is_error}:", result.structured_content)

        try:
            await session.call_tool("trigger_refund", {"customer_id": "CUST-1", "amount": -5, "reason": "no"})
        except MCPError as exc:
            print(f"\nInvalid call rejected -> code={exc.code} message={exc.message!r}")
            for err in exc.data["errors"]:
                print(f"   {err['field']}: {err['message']}")


if __name__ == "__main__":
    anyio.run(main)
