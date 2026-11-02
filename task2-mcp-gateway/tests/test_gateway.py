"""End-to-end tests: real gateway and real downstream servers, talking real HTTP.

Run:  python -m pytest task2-mcp-gateway -v
"""

import json
import time
import uuid

import anyio
import jwt
import pytest
from mcp.client import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.exceptions import MCPError

from auth import JWT_SECRET, issue_token

ACCEPT = "application/json, text/event-stream"
INIT = {
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "pytest", "version": "0"}},
}


# ----------------------------------------------------------------------------- helpers
def token(role: str, user: str = "bob") -> str:
    return issue_token(user, role)


def post(http, url: str, body, bearer: str | None = None, session_id: str | None = None):
    headers = {"Content-Type": "application/json", "Accept": ACCEPT}
    if bearer is not None:
        headers["Authorization"] = f"Bearer {bearer}"
    if session_id is not None:
        headers["Mcp-Session-Id"] = session_id
    content = body if isinstance(body, (bytes, str)) else json.dumps(body)
    return http.post(url, content=content, headers=headers)


def open_session(http, url: str, bearer: str) -> str:
    response = post(http, url, INIT, bearer)
    assert response.status_code == 200, response.text
    return response.headers["mcp-session-id"]


def tool_call(name: str, probe: str, msg_id=7) -> dict:
    """A tools/call carrying a unique marker, so we can search the downstream's log for it."""
    return {"jsonrpc": "2.0", "id": msg_id, "method": "tools/call", "params": {"name": name, "arguments": {"user": probe}}}


def probe() -> str:
    return f"probe-{uuid.uuid4().hex}"


# ------------------------------------------------------------------ authentication: 401
def _tampered_viewer_token() -> str:
    """A real viewer token with the payload swapped to role=admin and the original signature kept."""
    header, _, signature = token("viewer").split(".")
    forged = jwt.encode({"sub": "bob", "role": "admin", "exp": int(time.time()) + 600}, "forger-secret-forger-secret-forger-secret", algorithm="HS256")
    return f"{header}.{forged.split('.')[1]}.{signature}"


BAD_CREDENTIALS = {
    "missing": None,
    "garbage": "not.a.jwt",
    "expired": issue_token("bob", "viewer", ttl_seconds=-10),
    "wrong_secret": issue_token("bob", "admin", secret="attacker-secret-attacker-secret-attacker"),
    "alg_none": jwt.encode({"sub": "bob", "role": "admin", "exp": int(time.time()) + 600}, None, algorithm="none"),
    "unknown_role": token("superuser"),
    "tampered_payload": _tampered_viewer_token(),
}


@pytest.mark.parametrize("case", BAD_CREDENTIALS)
def test_bad_credentials_get_401_and_never_reach_downstream(servers, http, case):
    marker = probe()
    response = post(http, servers["gateway"].url, tool_call("get_weather", marker), BAD_CREDENTIALS[case])
    assert response.status_code == 401
    assert response.headers["www-authenticate"].startswith("Bearer")
    assert response.json()["error"]["message"].startswith("Unauthorized")
    assert marker not in servers["mock"].log()


def test_non_bearer_scheme_is_rejected(servers, http):
    response = http.post(servers["gateway"].url, content=json.dumps(INIT),
                         headers={"Authorization": "Basic Ym9iOnB3", "Content-Type": "application/json"})
    assert response.status_code == 401


def test_401_message_is_identical_whatever_the_cause(servers, http):
    messages = {post(http, servers["gateway"].url, INIT, BAD_CREDENTIALS[c]).text for c in BAD_CREDENTIALS}
    assert len(messages) == 1  # no hints for an attacker about which check failed


# ------------------------------------------------------------------ authorization: -32001
@pytest.fixture
def viewer(servers, http):
    bearer = token("viewer", "bob")
    return bearer, open_session(http, servers["gateway"].url, bearer)


@pytest.fixture
def admin(servers, http):
    bearer = token("admin", "alice")
    return bearer, open_session(http, servers["gateway"].url, bearer)


def test_viewer_tools_list_is_forwarded(servers, http, viewer):
    response = post(http, servers["gateway"].url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, *viewer)
    names = [t["name"] for t in response.json()["result"]["tools"]]
    assert "admin_reset_key" in names  # tools/list is passed through untouched


def test_viewer_normal_tool_is_forwarded(servers, http, viewer):
    marker = probe()
    response = post(http, servers["gateway"].url, tool_call("list_users", marker), *viewer)
    assert "result" in response.json()
    assert marker in servers["mock"].log()


@pytest.mark.parametrize("name", [
    "admin_reset_key", "admin_delete_user", "Admin_reset_key", "ADMIN_RESET_KEY", "  admin_reset_key",
    "ａdmin_reset_key",  # full-width "ａ" look-alike
])
def test_viewer_admin_tool_blocked_with_32001(servers, http, viewer, name):
    marker = probe()
    response = post(http, servers["gateway"].url, tool_call(name, marker, msg_id=41), *viewer)
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == 41
    assert body["error"]["code"] == -32001
    assert body["error"]["message"] == "Unauthorized Tool Call"
    assert marker not in servers["mock"].log()  # the downstream never saw it


def test_string_request_id_is_preserved(servers, http, viewer):
    response = post(http, servers["gateway"].url, tool_call("admin_reset_key", probe(), msg_id="req-abc"), *viewer)
    assert response.json()["id"] == "req-abc"


def test_admin_can_call_admin_tool(servers, http, admin):
    marker = probe()
    response = post(http, servers["gateway"].url, tool_call("admin_reset_key", marker), *admin)
    assert response.json()["result"]["structuredContent"]["status"] == "rotated"
    assert marker in servers["mock"].log()


def test_viewer_cannot_borrow_an_admin_session(servers, http, viewer, admin):
    viewer_token, _ = viewer
    _, admin_session = admin
    marker = probe()
    response = post(http, servers["gateway"].url, tool_call("admin_reset_key", marker), viewer_token, admin_session)
    assert response.json()["error"]["code"] == -32001  # role comes from the token, not the session
    assert marker not in servers["mock"].log()


# ------------------------------------------------------------------ hardening: malformed bodies
MALFORMED = [
    ("empty body", b"", 400, -32700),
    ("invalid json", b'{"jsonrpc":', 400, -32700),
    ("batch smuggling an admin call", b'[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"user":"MARK"}}}]', 400, -32600),
    ("empty batch", b"[]", 400, -32600),
    ("json string", b'"hello"', 400, -32600),
    ("missing method", b'{"jsonrpc":"2.0","id":1}', 400, -32600),
    ("non-string method", b'{"jsonrpc":"2.0","id":1,"method":5}', 400, -32600),
    ("duplicate keys", b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_weather","name":"admin_reset_key","arguments":{"user":"MARK"}}}', 400, -32600),
    ("params is a list", b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":["admin_reset_key"]}', 200, -32602),
    ("name is a number", b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":123}}', 200, -32602),
    ("name missing", b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}', 200, -32602),
    ("name blank", b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"   "}}', 200, -32602),
]


@pytest.mark.parametrize("label,body,status,code", MALFORMED, ids=[m[0] for m in MALFORMED])
def test_malformed_bodies_are_rejected_at_the_gateway(servers, http, viewer, label, body, status, code):
    marker = probe()
    response = post(http, servers["gateway"].url, body.replace(b"MARK", marker.encode()), *viewer)
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert marker not in servers["mock"].log()


def test_oversized_body_rejected(servers, http, viewer):
    big = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "pad": "x" * 1_100_000}
    assert post(http, servers["gateway"].url, big, *viewer).status_code == 413


def test_unsafe_ids_are_not_reflected(servers, http, viewer):
    body = {"jsonrpc": "2.0", "id": {"evil": 1}, "method": "tools/call", "params": {"name": "admin_reset_key"}}
    assert post(http, servers["gateway"].url, body, *viewer).json()["id"] is None


# ------------------------------------------------------------------ proxying behaviour
def test_authorization_header_is_not_forwarded(servers, http):
    response = post(http, servers["gateway_echo"].url, {"jsonrpc": "2.0", "id": 1, "method": "ping"}, token("viewer"), "sess-1")
    received = response.json()["result"]["received_headers"]
    assert "authorization" not in received        # the user's credential stays at the gateway
    assert received["mcp-session-id"] == "sess-1"  # but MCP's own headers pass through


def test_get_stream_is_relayed_as_it_arrives(servers, http):
    """The echo server sends 3 SSE events 0.5 s apart. If the gateway buffered the whole reply,
    the first event would only arrive after ~1.5 s."""
    headers = {"Authorization": f"Bearer {token('viewer')}", "Accept": "text/event-stream"}
    start = time.monotonic()
    with http.stream("GET", servers["gateway_echo"].url, headers=headers) as response:
        assert response.headers["content-type"].startswith("text/event-stream")
        chunks = response.iter_raw()
        first = next(chunks)
        first_at = time.monotonic() - start
        rest = b"".join(chunks)
    assert b'"n": 0' in first
    assert first_at < 0.4
    assert b'"n": 2' in first + rest


def test_downstream_down_gives_502_with_request_id(servers, http):
    response = post(http, servers["gateway_dead"].url, {"jsonrpc": "2.0", "id": 42, "method": "tools/list"}, token("viewer"))
    assert response.status_code == 502
    assert response.json() == {"jsonrpc": "2.0", "id": 42, "error": {"code": -32603, "message": "Upstream MCP server unavailable"}}


def test_downstream_hang_gives_504(servers, http):
    body = {"jsonrpc": "2.0", "id": 43, "method": "tools/list", "params": {"x": "__sleep__"}}
    start = time.monotonic()
    response = post(http, servers["gateway_echo"].url, body, token("viewer"))
    assert response.status_code == 504
    assert response.json()["error"]["message"] == "Upstream MCP server timed out"
    assert time.monotonic() - start < 3  # gave up after the 1 s read timeout, not the 5 s stall


def test_upstream_errors_do_not_leak_internals(servers, http):
    text = post(http, servers["gateway_dead"].url, {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, token("viewer")).text
    assert "127.0.0.1" not in text and "ConnectError" not in text


# ------------------------------------------------------------------ official MCP client
async def _run_client(url: str, role: str, user: str) -> tuple[list[str], dict | int]:
    http = create_mcp_http_client(headers={"Authorization": f"Bearer {token(role, user)}"})
    async with http, streamable_http_client(url, http_client=http) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = [t.name for t in (await session.list_tools()).tools]
            try:
                result = await session.call_tool("admin_reset_key", {"user": "bob"})
                return tools, result.structured_content
            except MCPError as exc:
                return tools, exc.code


@pytest.mark.parametrize("gateway", ["gateway", "gateway_sse"])
def test_official_mcp_client_through_gateway(servers, gateway):
    url = servers[gateway].url
    tools, outcome = anyio.run(_run_client, url, "viewer", "bob")
    assert "get_weather" in tools and outcome == -32001
    tools, outcome = anyio.run(_run_client, url, "admin", "alice")
    assert outcome["status"] == "rotated"


# ------------------------------------------------------------------ runs last (file order)
def test_gateways_never_logged_a_traceback(servers):
    for name in ("gateway", "gateway_sse", "gateway_echo", "gateway_dead"):
        assert "Traceback" not in servers[name].log(), f"{name} logged a traceback"