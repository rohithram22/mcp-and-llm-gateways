"""Shared fixtures: real servers started as subprocesses on free ports.

    gateway       -> mock_llm.py      (canned PII replies and failure modes)
    gateway_fast  -> mock_llm.py      with a 1 s read timeout (for the stall test)
    gateway_fake  -> fake_provider.py (streams whatever events a test scripts)
    gateway_dead  -> a port nobody listens on
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
sys.path.insert(0, str(ROOT))  # so tests can import patterns / redactor / sse directly


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
    base: str
    log_path: Path
    process: subprocess.Popen

    @property
    def url(self) -> str:
        return f"{self.base}/v1/chat/completions"

    def log(self) -> str:
        return self.log_path.read_text()


def start(script: Path, port_var: str, log_path: Path, **env) -> Proc:
    port = free_port()
    full_env = {**os.environ, **env, port_var: str(port), "PYTHONUNBUFFERED": "1"}
    log_file = open(log_path, "w")
    process = subprocess.Popen([sys.executable, str(script)], cwd=ROOT, env=full_env, stdout=log_file, stderr=log_file)
    wait_for_port(port)
    return Proc(base=f"http://127.0.0.1:{port}", log_path=log_path, process=process)


@pytest.fixture(scope="session")
def servers(tmp_path_factory):
    logs = tmp_path_factory.mktemp("logs")
    procs: dict[str, Proc] = {}
    try:
        procs["llm"] = start(ROOT / "mock_llm.py", "MOCK_LLM_PORT", logs / "llm.log")
        procs["fake"] = start(ROOT / "tests" / "fake_provider.py", "FAKE_PORT", logs / "fake.log")
        gw = ROOT / "gateway.py"
        procs["gateway"] = start(gw, "GATEWAY_PORT", logs / "gateway.log", LLM_URL=procs["llm"].url)
        procs["gateway_fast"] = start(gw, "GATEWAY_PORT", logs / "gateway_fast.log",
                                      LLM_URL=procs["llm"].url, UPSTREAM_READ_TIMEOUT="1")
        procs["gateway_fake"] = start(gw, "GATEWAY_PORT", logs / "gateway_fake.log", LLM_URL=procs["fake"].url)
        procs["gateway_dead"] = start(gw, "GATEWAY_PORT", logs / "gateway_dead.log",
                                      LLM_URL=f"http://127.0.0.1:{free_port()}/v1/chat/completions")
        yield procs
    finally:
        for proc in procs.values():
            proc.process.terminate()
        for proc in procs.values():
            proc.process.wait(timeout=10)


@pytest.fixture
def http():
    with httpx.Client(timeout=30.0) as client:
        yield client