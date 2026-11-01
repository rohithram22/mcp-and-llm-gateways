import json
import logging
import sys

import anyio
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from pydantic import ValidationError

import data
from schemas import TOOLS

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("customer-mcp")

def tool_result(payload: dict, is_error: bool = False) -> types.CallToolResult:
    """Wrap a dict as a tool result: readable text for the LLM, plus the raw structured data."""
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(payload, indent=2))],
        structuredContent=payload,
        isError=is_error,
    )

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

    # 3. Run the tool.
    try:
        if params.name == "get_customer_record":
            return tool_result(data.get_customer(args.customer_id))

        if params.name == "trigger_refund":
            refund = data.create_refund(args.customer_id, args.amount, args.reason)
            log.info("refund %s issued: %s %.2f", refund["refund_id"], args.customer_id, args.amount)
            return tool_result(refund)

    # Valid request, but the action failed   -> normal result with isError=true
    except (data.CustomerNotFound, data.RefundRejected) as exc:
        log.info("tool %s failed: %s", params.name, exc)
        return tool_result({"error": str(exc)}, is_error=True)

    # A bug in our code                      -> JSON-RPC error -32603, details only in the log
    except Exception:
        log.exception("unexpected error in tool %s", params.name)
        raise MCPError(code=types.INTERNAL_ERROR, message="Internal server error") from None

    # Only reachable if a tool is in TOOLS but has no branch above.
    log.error("tool %s is listed but not implemented", params.name)
    raise MCPError(code=types.INTERNAL_ERROR, message="Internal server error")


server = Server("customer-mcp", version="1.0.0", on_list_tools=list_tools, on_call_tool=call_tool)


async def main():
    log.info("server starting on stdio")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)