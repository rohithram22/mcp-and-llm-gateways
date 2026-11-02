"""Shared fixtures: start real servers as subprocesses on free ports.

Process layout for the whole test session:

    gateway        -> mock MCP server (official SDK, JSON replies)
    gateway_sse    -> mock MCP server (official SDK, SSE replies)
    gateway_echo   -> echo server     (reports received headers, can stall, streams on GET)
    gateway_dead   -> a port nobody listens on

Every process writes its stderr to a log file, so tests can assert on what the
downstream actually received and that the gateway never logged a traceback.
"""

import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # so tests can import auth / policy directly


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"nothing listening on port {port} after {timeout}s")


@dataclass
class Proc:
    url: str
    log_path: Path
    process: subprocess.Popen

    def log(self) -> str:
        return self.log_path.read_text()


def start(script: Path, env_extra: dict, port_var: str, log_path: Path) -> Proc:
    port = free_port()
    env = {**os.environ, **env_extra, port_var: str(port), "PYTHONUNBUFFERED": "1"}
    log_file = open(log_path, "w")
    process = subprocess.Popen([sys.executable, str(script)], cwd=ROOT, env=env, stdout=log_file, stderr=log_file)
    wait_for_port(port)
    return Proc(url=f"http://127.0.0.1:{port}/mcp", log_path=log_path, process=process)


@pytest.fixture(scope="session")
def servers(tmp_path_factory):
    logs = tmp_path_factory.mktemp("logs")
    procs: dict[str, Proc] = {}
    try:
        procs["mock"] = start(ROOT / "mock_server.py", {}, "MOCK_PORT", logs / "mock.log")
        procs["mock_sse"] = start(ROOT / "mock_server.py", {"MOCK_SSE": "1"}, "MOCK_PORT", logs / "mock_sse.log")
        procs["echo"] = start(ROOT / "tests" / "echo_server.py", {}, "ECHO_PORT", logs / "echo.log")

        def gateway(name: str, downstream: str, **env) -> None:
            procs[name] = start(
                ROOT / "gateway.py", {"DOWNSTREAM_URL": downstream, **env}, "GATEWAY_PORT", logs / f"{name}.log"
            )

        gateway("gateway", procs["mock"].url)
        gateway("gateway_sse", procs["mock_sse"].url)
        gateway("gateway_echo", procs["echo"].url, UPSTREAM_READ_TIMEOUT="1")
        gateway("gateway_dead", f"http://127.0.0.1:{free_port()}/mcp")
        yield procs
    finally:
        for proc in procs.values():
            proc.process.terminate()
        for proc in procs.values():
            proc.process.wait(timeout=10)


@pytest.fixture
def http():
    with httpx.Client(timeout=10.0) as client:
        yield client