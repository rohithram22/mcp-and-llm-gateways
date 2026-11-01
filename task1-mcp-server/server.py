import anyio
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server


async def list_tools(ctx, params) -> types.ListToolsResult:
    return types.ListToolsResult(
        tools=[
            types.Tool(
                name="say_hello",
                description="Says hello to someone.",
                inputSchema={
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                },
            )
        ]
    )


async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    name = params.arguments["name"]
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=f"Hello, {name}!")]
    )


server = Server("hello-server", version="0.1.0", on_list_tools=list_tools, on_call_tool=call_tool)


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)