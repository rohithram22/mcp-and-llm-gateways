"""Authorization: decide whether a verified caller may send a given JSON-RPC message."""

from auth import Principal

ADMIN_TOOL_PREFIX = "admin_"
UNAUTHORIZED_TOOL_CALL = -32001


def is_admin_tool(tool_name: str) -> bool:
    # casefold() + strip(): "Admin_reset_key" or " admin_reset_key" must not slip through
    # just because the downstream server might normalise names differently than we do.
    return tool_name.strip().casefold().startswith(ADMIN_TOOL_PREFIX)


def check_message(principal: Principal, message: dict) -> dict | None:
    """Return a JSON-RPC error response if the message is not allowed, or None if it may be forwarded."""
    if message.get("method") != "tools/call":
        return None  # tools/list, initialize, notifications, ...: forwarded transparently

    params = message.get("params") or {}
    tool_name = params.get("name")
    if isinstance(tool_name, str) and is_admin_tool(tool_name) and principal.role != "admin":
        return {
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "error": {
                "code": UNAUTHORIZED_TOOL_CALL,
                "message": "Unauthorized Tool Call",
                "data": {"required_role": "admin"},
            },
        }
    return None