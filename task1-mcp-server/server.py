import logging
import sys

import anyio
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from pydantic import ValidationError

from schemas import TOOLS

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("customer-mcp")


async def list_tools(ctx, params) -> types.ListToolsResult:
    log.info("tools/list requested")
    return types.ListToolsResult(
        tools=[
            types.Tool(
                name=name,
                description=description,
                inputSchema=model.model_json_schema(),
            )
            for name, (model, description) in TOOLS.items()
        ]
    )


async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    log.info("tools/call name=%s", params.name)

    # 1. Is this a tool we actually have?
    if params.name not in TOOLS:
        raise MCPError(code=types.INVALID_PARAMS, message=f"Unknown tool: {params.name}")

    # 2. Are the arguments valid?
    model, _ = TOOLS[params.name]
    try:
        args = model.model_validate(params.arguments or {})
    except ValidationError as exc:
        errors = [
            {"field": ".".join(str(part) for part in err["loc"]), "message": err["msg"]}
            for err in exc.errors()
        ]
        log.warning("invalid arguments for %s: %s", params.name, errors)
        raise MCPError(
            code=types.INVALID_PARAMS,
            message=f"Invalid arguments for tool '{params.name}'",
            data={"errors": errors},
        ) from None

    # 3. Placeholder: the real tool logic arrives in Step 6.
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=f"Arguments OK: {args.model_dump()}")]
    )


server = Server("customer-mcp", version="0.1.0", on_list_tools=list_tools, on_call_tool=call_tool)


async def main():
    log.info("server starting on stdio")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)