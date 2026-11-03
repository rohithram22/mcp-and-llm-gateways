"""End-to-end tests: real gateway, real (mock) providers, real HTTP streaming.

Run:  python -m pytest task3-pii-stream-guardrail -v
"""

import json
import re
import time

import pytest

from patterns import redact
from sse import SSEParser

REPLIES = {
    "ssn": "Sure. The SSN on file is 123-45-6789, and the backup record shows 987-65-4321.",
    "email": "You can reach Ada at ada.lovelace@example.com or the team at support@acme.io anytime.",
    "card": "The card ending in 1111 is 4111 1111 1111 1111. An older one, 3782-822463-10005, was cancelled.",
    "mixed": (
        "Customer profile: email jane.doe@example.org, SSN 078-05-1120, "
        "card 5555 5555 5555 4444. Order number 1234 5678 9012 3456 is not a card."
    ),
    "clean": "The weather in Paris is sunny with a light breeze. Nothing sensitive here at all.",
}


# ----------------------------------------------------------------------------- helpers
class StreamResult:
    def __init__(self):
        self.events: list = []          # parsed events, "[DONE]" kept as a string
        self.text: dict[int, str] = {}  # text per choice index
        self.ttft: float | None = None
        self.total: float = 0.0
        self.status: int = 0

    @property
    def errors(self):
        return [e for e in self.events if isinstance(e, dict) and "error" in e]


def run_stream(http, url: str, body: dict) -> StreamResult:
    result, parser, start = StreamResult(), SSEParser(), time.perf_counter()
    with http.stream("POST", url, json={"model": "m", "stream": True, **body}) as response:
        result.status = response.status_code
        for chunk in response.iter_bytes():
            for data in parser.feed(chunk):
                event = data if data == "[DONE]" else json.loads(data)
                result.events.append(event)
                if isinstance(event, dict):
                    for choice in event.get("choices", []):
                        content = choice.get("delta", {}).get("content")
                        if content:
                            result.ttft = result.ttft or time.perf_counter() - start
                            result.text[choice["index"]] = result.text.get(choice["index"], "") + content
    result.total = time.perf_counter() - start
    return result


def ask(prompt: str, **extra) -> dict:
    return {"messages": [{"role": "user", "content": prompt}], **extra}


def chunk_event(content: str, index: int = 0, finish=None) -> str:
    delta = {"content": content} if content is not None else {}
    return json.dumps({"id": "c1", "model": "fake", "choices": [{"index": index, "delta": delta, "finish_reason": finish}]})


# ----------------------------------------------------------------------------- redaction
@pytest.mark.parametrize("prompt", list(REPLIES))
@pytest.mark.parametrize("chunk_size", [1, 3, 5, 8])
def test_stream_is_redacted_exactly(servers, http, prompt, chunk_size):
    result = run_stream(http, servers["gateway"].url, ask(prompt, mock_chunk_size=chunk_size))
    assert result.text[0] == redact(REPLIES[prompt])
    assert result.events[-1] == "[DONE]"


def test_clean_text_is_unchanged(servers, http):
    assert run_stream(http, servers["gateway"].url, ask("clean")).text[0] == REPLIES["clean"]


def test_non_streaming_reply_is_redacted(servers, http):
    body = http.post(servers["gateway"].url, json={"model": "m", **ask("mixed")}).json()
    assert body["choices"][0]["message"]["content"] == redact(REPLIES["mixed"])


def test_no_pii_digits_ever_reach_the_client(servers, http):
    result = run_stream(http, servers["gateway"].url, ask("mixed", mock_chunk_size=1))
    raw = json.dumps(result.events)
    for secret in ("078-05", "5555 5555", "jane.doe@"):
        assert secret not in raw


def test_events_keep_the_openai_shape(servers, http):
    result = run_stream(http, servers["gateway"].url, ask("ssn", mock_chunk_size=4))
    events = [e for e in result.events if e != "[DONE]"]
    assert all(e["object"] == "chat.completion.chunk" and e["model"] == "m" for e in events)
    # held text is flushed in its own event BEFORE the event carrying finish_reason
    assert events[-1]["choices"][0]["finish_reason"] == "stop"
    assert events[-2]["choices"][0]["delta"]["content"] == "[REDACTED]."


def test_choices_are_redacted_independently(servers, http):
    """Two interleaved choices: SSNs split across their chunks must not leak or mix."""
    script = [
        chunk_event("A: 123-4", 0), chunk_event("B: 987-6", 1),
        chunk_event("5-6789 end", 0), chunk_event("5-4321 end", 1),
        chunk_event(None, 0, "stop"), chunk_event(None, 1, "stop"), "[DONE]",
    ]
    result = run_stream(http, servers["gateway_fake"].url, {"script": script})
    assert result.text == {0: "A: [REDACTED] end", 1: "B: [REDACTED] end"}


def test_held_text_is_flushed_when_provider_skips_finish_reason(servers, http):
    script = [chunk_event("card 4111 1111 "), chunk_event("1111 1111"), "[DONE]"]
    result = run_stream(http, servers["gateway_fake"].url, {"script": script})
    assert result.text[0] == "card [REDACTED]"


# ----------------------------------------------------------------------------- latency & memory
def test_ttft_and_total_latency_overhead_is_small(servers, http):
    direct = min((run_stream(http, servers["llm"].url, ask("clean")) for _ in range(3)), key=lambda r: r.total)
    via = min((run_stream(http, servers["gateway"].url, ask("clean")) for _ in range(3)), key=lambda r: r.total)
    assert via.ttft - direct.ttft < 0.1      # at most about one chunk (30 ms) of holdback
    assert via.total - direct.total < 0.1


def test_response_is_streamed_not_buffered(servers, http):
    result = run_stream(http, servers["gateway"].url, ask("mixed"))
    assert result.ttft < result.total / 4    # first text long before the reply finishes


def test_gateway_memory_stays_bounded_on_long_replies(servers, http):
    words = ["Card", "4111 1111 1111 1111", "and", "mail", "ada@example.com", "and", "SSN", "123-45-6789", "ok."]
    script = [chunk_event(f"{words[i % len(words)]} ") for i in range(3000)] + [chunk_event(None, 0, "stop"), "[DONE]"]
    result = run_stream(http, servers["gateway_fake"].url, {"script": script, "delay": 0})
    assert "4111" not in result.text[0] and "ada@" not in result.text[0]
    peak = int(re.findall(r"peak buffer (\d+) chars", servers["gateway_fake"].log())[-1])
    assert peak < 100


# ----------------------------------------------------------------------------- failures
def test_provider_500_is_sanitised(servers, http):
    response = http.post(servers["gateway"].url, json={"model": "m", "stream": True, **ask("fail500")})
    assert response.status_code == 502
    assert "llm-node-7" not in response.text and "model.py" not in response.text


def test_provider_429_is_passed_as_429(servers, http):
    response = http.post(servers["gateway"].url, json={"model": "m", **ask("rate429")})
    assert response.status_code == 429
    assert "org-123" not in response.text


def test_broken_stream_never_releases_held_partial_pii(servers, http):
    result = run_stream(http, servers["gateway"].url, ask("cutoff"))
    assert result.text[0] == "Sure. The SSN on file is "
    assert "123" not in json.dumps(result.events)            # the held "123-4" was discarded
    assert result.errors and result.events[-1] != "[DONE]"


def test_malformed_provider_event_is_dropped_and_stream_continues(servers, http):
    result = run_stream(http, servers["gateway"].url, ask("garbage"))
    assert result.text[0] == redact(REPLIES["ssn"])
    assert "not json" not in json.dumps(result.events)


def test_silent_provider_times_out_mid_stream(servers, http):
    result = run_stream(http, servers["gateway_fast"].url, ask("stall"))
    assert result.errors and result.total < 3


def test_provider_down_gives_502(servers, http):
    response = http.post(servers["gateway_dead"].url, json={"model": "m", **ask("ssn")})
    assert response.status_code == 502
    assert "127.0.0.1" not in response.text


def test_invalid_request_body_gives_400(servers, http):
    response = http.post(servers["gateway"].url, content=b"{oops", headers={"Content-Type": "application/json"})
    assert response.status_code == 400


def test_client_disconnect_closes_the_provider_stream(servers, http):
    with http.stream("POST", servers["gateway_fake"].url, json={"model": "m", "stream": True, "forever": True}) as r:
        for _ in r.iter_bytes():
            break                                            # read one chunk, then hang up
    time.sleep(0.5)
    assert http.get(f"{servers['fake'].base}/active").json()["streams"] == 0


# ----------------------------------------------------------------------------- runs last (file order)
def test_gateways_never_logged_a_traceback(servers):
    for name in ("gateway", "gateway_fast", "gateway_fake", "gateway_dead"):
        assert "Traceback" not in servers[name].log(), f"{name} logged a traceback"