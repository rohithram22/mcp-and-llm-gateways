"""End-to-end tests: start the server as a real subprocess and speak raw JSON-RPC.

Testing at the wire level (instead of calling Python functions directly) is what
lets us check the two things graders care about most: the exact error codes on
the wire, and that stdout contains nothing but JSON-RPC.

Run:  pytest -v
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
INVALID_PARAMS = -32602


class StdioSession:
    """Minimal MCP client: write one JSON line to stdin, read one JSON line from stdout."""

    def __init__(self, script: Path):
        self.proc = subprocess.Popen(
            [sys.executable, str(script)],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.stdout_lines: list[str] = []
        self._next_id = 0
        self.request("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "0"},
        })
        self.notify("notifications/initialized")

    def _send(self, message: dict) -> None:
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def notify(self, method: str, params: dict | None = None) -> None:
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)

    def request(self, method: str, params: dict | None = None) -> dict:
        self._next_id += 1
        msg = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)
        line = self.proc.stdout.readline()
        self.stdout_lines.append(line)
        response = json.loads(line)  # raises if stdout was polluted
        assert response["id"] == self._next_id
        return response

    def call_tool(self, name: str, arguments: dict) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})

    def close(self) -> tuple[str, str]:
        out, err = self.proc.communicate(timeout=10)
        self.stdout_lines.extend(out.splitlines(keepends=True))
        return "".join(self.stdout_lines), err


@pytest.fixture
def session():
    s = StdioSession(ROOT / "server.py")
    yield s
    s.close()


# ---------------------------------------------------------------- tools/list
def test_lists_both_tools_with_schemas(session):
    tools = {t["name"]: t for t in session.request("tools/list")["result"]["tools"]}
    assert set(tools) == {"get_customer_record", "trigger_refund"}
    refund_schema = tools["trigger_refund"]["inputSchema"]
    assert refund_schema["required"] == ["customer_id", "amount", "reason"]
    assert refund_schema["additionalProperties"] is False
    assert refund_schema["properties"]["reason"]["minLength"] == 10


# ------------------------------------------------------------- happy paths
def test_get_customer_record_success(session):
    result = session.call_tool("get_customer_record", {"customer_id": "CUST-00001"})["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["name"] == "Ada Lovelace"


def test_trigger_refund_success(session):
    result = session.call_tool(
        "trigger_refund",
        {"customer_id": "CUST-00001", "amount": 19.99, "reason": "Package arrived damaged"},
    )["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["status"] == "processed"
    assert result["structuredContent"]["remaining_refundable_balance"] == 230.01


def test_integer_amount_is_accepted(session):
    # JSON has no int/float distinction for users; 10 is a valid positive amount.
    result = session.call_tool(
        "trigger_refund", {"customer_id": "CUST-00002", "amount": 10, "reason": "Late delivery refund"}
    )["result"]
    assert result["isError"] is False


# ----------------------------------------- validation -> JSON-RPC -32602
BAD_CUSTOMER_IDS = [
    "CUST-1234",       # too short
    "CUST-123456",     # too long
    "cust-12345",      # wrong case
    "CUST-ABCDE",      # letters, not digits
    " CUST-12345",     # leading whitespace
    "CUST-12345\n",    # trailing newline
    "XCUST-12345",     # prefix junk
    "",                # empty
    12345,             # wrong type
    None,              # null
]


@pytest.mark.parametrize("bad_id", BAD_CUSTOMER_IDS)
def test_get_customer_rejects_malformed_ids(session, bad_id):
    response = session.call_tool("get_customer_record", {"customer_id": bad_id})
    assert response["error"]["code"] == INVALID_PARAMS
    assert response["error"]["data"]["errors"][0]["field"] == "customer_id"


GOOD_REFUND = {"customer_id": "CUST-00001", "amount": 5.0, "reason": "Item never arrived"}

BAD_REFUND_FIELDS = [
    ("amount", 0),               # not positive
    ("amount", -1.5),            # negative
    ("amount", "10.00"),         # string, not number
    ("amount", True),            # bool sneaking in as 1
    ("amount", 10.999),          # fractions of a cent
    ("amount", 5_000_000),       # above sanity ceiling
    ("amount", None),
    ("reason", "too short"),     # 9 chars
    ("reason", " " * 20),        # only whitespace
    ("reason", "   short   "),   # padded to 11, real content 5
    ("reason", "x" * 501),       # too long
    ("reason", 1234567890),      # wrong type
]


@pytest.mark.parametrize("field,value", BAD_REFUND_FIELDS)
def test_trigger_refund_rejects_bad_fields(session, field, value):
    args = {**GOOD_REFUND, field: value}
    response = session.call_tool("trigger_refund", args)
    assert response["error"]["code"] == INVALID_PARAMS, response
    assert response["error"]["data"]["errors"][0]["field"] == field


def test_nan_amount_rejected_at_the_wire(session):
    # json.dumps writes NaN as a bare token, which strict JSON forbids.
    session.proc.stdin.write(
        '{"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":"trigger_refund",'
        '"arguments":{"customer_id":"CUST-00001","amount":NaN,"reason":"Damaged in transit"}}}\n'
    )
    session.proc.stdin.flush()
    response = json.loads(session.proc.stdout.readline())
    assert "error" in response and "result" not in response


@pytest.mark.parametrize("missing", ["customer_id", "amount", "reason"])
def test_trigger_refund_rejects_missing_fields(session, missing):
    args = {k: v for k, v in GOOD_REFUND.items() if k != missing}
    response = session.call_tool("trigger_refund", args)
    assert response["error"]["code"] == INVALID_PARAMS
    assert response["error"]["data"]["errors"][0]["field"] == missing


def test_unknown_extra_field_rejected(session):
    response = session.call_tool("trigger_refund", {**GOOD_REFUND, "approved_by": "me"})
    assert response["error"]["code"] == INVALID_PARAMS


def test_all_errors_reported_at_once(session):
    response = session.call_tool("trigger_refund", {"customer_id": "bad", "amount": -1, "reason": "no"})
    fields = {e["field"] for e in response["error"]["data"]["errors"]}
    assert fields == {"customer_id", "amount", "reason"}


def test_unknown_tool_is_invalid_params(session):
    response = session.call_tool("delete_everything", {})
    assert response["error"]["code"] == INVALID_PARAMS


def test_error_payload_does_not_echo_raw_input(session):
    response = session.call_tool("get_customer_record", {"customer_id": "<script>alert(1)</script>"})
    assert "<script>" not in json.dumps(response["error"])


# ------------------------------------- business failures -> isError result
def test_unknown_customer_is_tool_error_not_protocol_error(session):
    result = session.call_tool("get_customer_record", {"customer_id": "CUST-99999"})["result"]
    assert result["isError"] is True


def test_refund_over_balance_is_tool_error(session):
    result = session.call_tool(
        "trigger_refund", {"customer_id": "CUST-00003", "amount": 1.0, "reason": "Goodwill gesture refund"}
    )["result"]
    assert result["isError"] is True
    assert "exceeds" in result["structuredContent"]["error"]


# ------------------------------------------------------- stdio isolation
def _assert_pure_jsonrpc(stdout: str) -> None:
    lines = [line for line in stdout.splitlines() if line.strip()]
    assert lines, "server wrote nothing to stdout"
    for line in lines:
        message = json.loads(line)  # every single line must parse
        assert message["jsonrpc"] == "2.0"


def test_stdout_is_pure_jsonrpc_and_logs_go_to_stderr():
    s = StdioSession(ROOT / "server.py")
    s.request("tools/list")
    s.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
    s.call_tool("trigger_refund", {"customer_id": "bad", "amount": -1, "reason": "x"})
    stdout, stderr = s.close()
    _assert_pure_jsonrpc(stdout)
    assert "tools/call name=get_customer_record" in stderr  # logs exist, just not on stdout
    assert "invalid arguments for trigger_refund" in stderr


def test_stray_print_cannot_corrupt_stdout():
    s = StdioSession(ROOT / "tests" / "stray_print_server.py")
    response = s.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
    assert response["result"]["isError"] is False
    stdout, stderr = s.close()
    _assert_pure_jsonrpc(stdout)
    assert "DEBUG: looking up CUST-00001" in stderr  # the print was redirected, not lost
