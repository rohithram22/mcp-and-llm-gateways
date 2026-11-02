"""Authorization: decide whether a verified caller may send a given request body.

Every path that is not explicitly understood and allowed ends in a rejection (fail closed).
"""

import json
import unicodedata
from dataclasses import dataclass

from auth import Principal

ADMIN_TOOL_PREFIX = "admin_"

# JSON-RPC error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
INVALID_PARAMS = -32602
UNAUTHORIZED_TOOL_CALL = -32001  # server-defined range, required by the task

MAX_BODY_BYTES = 1_000_000


@dataclass(frozen=True)
class Rejection:
    """A request the gateway answers itself instead of forwarding."""

    http_status: int
    payload: dict  # the JSON-RPC error response sent to the client
    reason: str    # detail for our logs only


class DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    """json.loads hook. With duplicate keys, our parser and the downstream's could each pick a
    different value, so the gateway would authorize one thing while the server runs another."""
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise DuplicateKeyError("duplicate key in JSON object")
    return dict(pairs)


def is_admin_tool(tool_name: str) -> bool:
    # NFKC + casefold + strip: "Admin_reset_key", " admin_reset_key" and look-alike Unicode
    # forms must not slip through just because the downstream server normalises names differently.
    normalized = unicodedata.normalize("NFKC", tool_name).strip().casefold()
    return normalized.startswith(ADMIN_TOOL_PREFIX)


def _safe_id(message) -> str | int | None:
    """The request id to echo back. Only valid JSON-RPC id types are reflected."""
    if isinstance(message, dict):
        msg_id = message.get("id")
        if isinstance(msg_id, (str, int)) and not isinstance(msg_id, bool):
            return msg_id
    return None


def _reject(http_status: int, msg_id, code: int, message: str, reason: str, data: dict | None = None) -> Rejection:
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return Rejection(http_status, {"jsonrpc": "2.0", "id": msg_id, "error": error}, reason)


def check_request(principal: Principal, body: bytes) -> Rejection | None:
    """Return a Rejection if this POST body must not reach the downstream server, else None."""
    # 1. Size and syntax
    if len(body) > MAX_BODY_BYTES:
        return _reject(413, None, INVALID_REQUEST, "Request too large", f"body of {len(body)} bytes")
    try:
        message = json.loads(body, object_pairs_hook=_reject_duplicate_keys)
    except DuplicateKeyError:
        return _reject(400, None, INVALID_REQUEST, "Invalid Request: duplicate keys", "JSON object with duplicate keys")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _reject(400, None, PARSE_ERROR, "Parse error", "body is not valid JSON")

    # 2. Batches: MCP (2025-06-18) does not use JSON-RPC batching, and a batch is the classic
    #    way to smuggle a forbidden call past a gateway that only inspects single objects.
    if isinstance(message, list):
        return _reject(400, None, INVALID_REQUEST, "Invalid Request: batch requests are not supported", "JSON array body")

    if not isinstance(message, dict):
        return _reject(400, None, INVALID_REQUEST, "Invalid Request", "body is not a JSON object")

    # 3. A client's *response* to a server-initiated request (e.g. sampling) has no method.
    #    It cannot invoke a tool, so it is forwarded as-is.
    if "method" not in message and ("result" in message or "error" in message):
        return None

    # 4. Otherwise it must be a request or notification with a string method
    if not isinstance(message.get("method"), str):
        return _reject(400, _safe_id(message), INVALID_REQUEST, "Invalid Request", "missing or non-string method")

    # 5. Only tools/call is subject to the admin rule; everything else is forwarded transparently
    if message["method"] != "tools/call":
        return None

    # 6. A tools/call we cannot read is a tools/call we cannot authorize
    params = message.get("params")
    tool_name = params.get("name") if isinstance(params, dict) else None
    if not isinstance(tool_name, str) or not tool_name.strip():
        return _reject(
            200, _safe_id(message), INVALID_PARAMS, "Invalid params: tools/call requires a string params.name",
            "tools/call without a readable tool name",
        )

    # 7. The admin rule
    if is_admin_tool(tool_name) and principal.role != "admin":
        return _reject(
            200, _safe_id(message), UNAUTHORIZED_TOOL_CALL, "Unauthorized Tool Call",
            f"role={principal.role} called admin tool {tool_name!r}", data={"required_role": "admin"},
        )

    return None