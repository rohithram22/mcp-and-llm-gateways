"""Shared fixtures for Task 4.

Unit tests get a fresh temporary SQLite file each. End-to-end tests share one set of
real processes (two mock providers and the gateway) started once per session.
"""

import os
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import db  # noqa: E402


class FakeClock:
    """Time that only moves when a test says so."""

    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "router.db")
    db.init_db(path)
    return path


@pytest.fixture
def clock():
    return FakeClock()


# ----------------------------------------------------------------------------- processes
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
    raise RuntimeError(f"nothing listening on port {port}")


@dataclass
class Proc:
    port: int
    log_path: Path
    process: subprocess.Popen

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def log(self) -> str:
        return self.log_path.read_text()


def start(script: Path, port_var: str, log_path: Path, **env) -> Proc:
    port = free_port()
    full_env = {**os.environ, **env, port_var: str(port), "PYTHONUNBUFFERED": "1"}
    log_file = open(log_path, "w")
    process = subprocess.Popen([sys.executable, str(script)], cwd=ROOT, env=full_env, stdout=log_file, stderr=log_file)
    wait_for_port(port)
    return Proc(port, log_path, process)


@dataclass
class Stack:
    primary: Proc
    secondary: Proc
    gateway: Proc
    db_path: str
    logs: Path

    @property
    def url(self) -> str:
        return f"{self.gateway.base}/v1/chat/completions"

    def tenant(self, limit: int = 50_000) -> tuple[str, str]:
        """A brand-new tenant per test, so tests never share a budget. Returns (tenant_id, key)."""
        tenant_id = f"t-{uuid.uuid4().hex[:8]}"
        key = f"sk-{uuid.uuid4().hex}"
        db.add_tenant(tenant_id, key, limit, path=self.db_path)
        return tenant_id, key


@pytest.fixture(scope="session")
def stack(tmp_path_factory):
    logs = tmp_path_factory.mktemp("logs")
    db_path = str(logs / "gateway.db")
    procs = []
    try:
        primary = start(ROOT / "mock_provider.py", "MOCK_PORT", logs / "primary.log", MOCK_NAME="primary")
        procs.append(primary)
        secondary = start(ROOT / "mock_provider.py", "MOCK_PORT", logs / "secondary.log", MOCK_NAME="secondary")
        procs.append(secondary)
        gateway = start(
            ROOT / "gateway.py", "GATEWAY_PORT", logs / "gateway.log",
            ROUTER_DB=db_path,
            PRIMARY_URL=f"{primary.base}/v1/chat/completions",
            SECONDARY_URL=f"{secondary.base}/v1/chat/completions",
            STREAM_IDLE_TIMEOUT="2",
        )
        procs.append(gateway)
        yield Stack(primary, secondary, gateway, db_path, logs)
    finally:
        for proc in procs:
            proc.process.terminate()
        for proc in procs:
            proc.process.wait(timeout=10)


@pytest.fixture
def providers(stack):
    """Reset both mocks to healthy, with fresh stats, before each end-to-end test."""
    with httpx.Client(timeout=5) as client:
        for proc in (stack.primary, stack.secondary):
            client.post(f"{proc.base}/control", json={"mode": "ok", "delay": 5.0})
            client.post(f"{proc.base}/stats/reset")

        def control(which: str, mode: str, delay: float = 5.0) -> None:
            proc = stack.primary if which == "primary" else stack.secondary
            client.post(f"{proc.base}/control", json={"mode": mode, "delay": delay})

        def stats(which: str) -> dict:
            proc = stack.primary if which == "primary" else stack.secondary
            return client.get(f"{proc.base}/stats").json()

        control.stats = stats
        yield control