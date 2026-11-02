"""Fast unit tests for the pure decision logic, no servers involved."""

import json

import pytest

from auth import AuthError, Principal, authenticate, issue_token
from policy import check_request, is_admin_tool

VIEWER = Principal("bob", "viewer")
ADMIN = Principal("alice", "admin")


def body(obj) -> bytes:
    return json.dumps(obj).encode()


@pytest.mark.parametrize("name,expected", [
    ("admin_reset_key", True),
    ("Admin_Reset_Key", True),
    (" admin_x", True),
    ("ａdmin_x", True),       # full-width look-alike
    ("get_weather", False),
    ("administrator", False),     # no underscore: not the admin_ prefix
    ("my_admin_tool", False),     # prefix, not substring
])
def test_is_admin_tool(name, expected):
    assert is_admin_tool(name) is expected


def test_non_tool_methods_are_forwarded():
    for method in ("initialize", "tools/list", "notifications/initialized", "resources/list"):
        assert check_request(VIEWER, body({"jsonrpc": "2.0", "id": 1, "method": method})) is None


def test_client_responses_are_forwarded():
    # A client answering a server-initiated request: no method, cannot invoke a tool.
    assert check_request(VIEWER, body({"jsonrpc": "2.0", "id": 5, "result": {}})) is None
    assert check_request(VIEWER, body({"jsonrpc": "2.0", "id": 5, "error": {"code": 1, "message": "x"}})) is None


def test_admin_rule():
    call = body({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "admin_reset_key"}})
    rejection = check_request(VIEWER, call)
    assert rejection.payload["error"]["code"] == -32001 and rejection.payload["id"] == 3
    assert check_request(ADMIN, call) is None


def test_invalid_utf8_is_a_parse_error():
    assert check_request(VIEWER, b"\xff\xfe").payload["error"]["code"] == -32700


def test_boolean_id_is_not_reflected():
    call = body({"jsonrpc": "2.0", "id": True, "method": "tools/call", "params": {"name": "admin_x"}})
    assert check_request(VIEWER, call).payload["id"] is None


def test_authenticate_accepts_lowercase_scheme_and_returns_principal():
    principal = authenticate(f"bearer {issue_token('carol', 'admin')}")
    assert principal == Principal("carol", "admin")


def test_authenticate_requires_claims():
    import jwt
    from auth import JWT_SECRET

    no_role = jwt.encode({"sub": "bob", "exp": 9_999_999_999}, JWT_SECRET, algorithm="HS256")
    with pytest.raises(AuthError):
        authenticate(f"Bearer {no_role}")