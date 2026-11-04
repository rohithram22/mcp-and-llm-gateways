"""End-to-end: the real gateway, two real mock providers, an on-disk SQLite file.

Run:  python -m pytest task4-llm-router -v
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from limiter import RateLimiter

LEAKS = ("Traceback", "org-42", "infer.py", "/srv/", "127.0.0.1", "primary", "secondary")


def body(max_tokens=20, stream=False, content="hello"):
    return {"model": "x", "max_tokens": max_tokens, "stream": stream,
            "messages": [{"role": "user", "content": content}]}


def post(stack, key, payload, timeout=40.0):
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    return httpx.post(stack.url, json=payload, headers=headers, timeout=timeout)


def assert_standard_error(response, status, error_type):
    assert response.status_code == status
    error = response.json()["error"]
    assert error["type"] == error_type
    assert error["request_id"].startswith("req_")
    assert response.headers["x-request-id"] == error["request_id"]
    assert set(response.json()) == {"error"} and set(error) == {"type", "message", "request_id"}


def usage(stack, tenant_id):
    return RateLimiter(path=stack.db_path).usage(tenant_id)


# ----------------------------------------------------------------------------- requests
def test_success_is_settled_to_reported_usage(stack, providers):
    tenant, key = stack.tenant()
    response = post(stack, key, body(max_tokens=200))
    assert response.status_code == 200 and response.headers["x-request-id"].startswith("req_")
    reported = response.json()["usage"]["total_tokens"]
    assert usage(stack, tenant) == reported          # not the ~205-token worst-case reservation


@pytest.mark.parametrize("key", [None, "sk-unknown"])
def test_missing_or_unknown_key_is_401(stack, providers, key):
    assert_standard_error(post(stack, key, body()), 401, "authentication_error")


def test_invalid_json_is_400(stack, providers):
    _, key = stack.tenant()
    response = httpx.post(stack.url, content=b"{oops", headers={"Authorization": f"Bearer {key}"})
    assert_standard_error(response, 400, "invalid_request")


def test_unpriceable_request_is_400_and_costs_nothing(stack, providers):
    tenant, key = stack.tenant()
    assert_standard_error(post(stack, key, body(max_tokens=-1)), 400, "invalid_request")
    assert usage(stack, tenant) == 0 and providers.stats("primary")["received"] == 0


def test_request_bigger_than_the_whole_budget_is_400_not_429(stack, providers):
    _, key = stack.tenant(limit=1_000)
    response = post(stack, key, body(max_tokens=5_000))
    assert_standard_error(response, 400, "request_too_large")
    assert "retry-after" not in response.headers


def test_unknown_path_uses_the_standard_shape(stack, providers):
    assert_standard_error(httpx.get(f"{stack.gateway.base}/nope"), 404, "not_found")


# ----------------------------------------------------------------------------- rate limiting
def test_concurrent_burst_admits_exactly_what_fits(stack, providers):
    """8 simultaneous requests reserving ~605 tokens each against a 2,000 budget: exactly 3 fit."""
    tenant, key = stack.tenant(limit=2_000)
    providers("primary", "slow", 1.0)                 # keep them in flight together
    with ThreadPoolExecutor(8) as pool:
        responses = list(pool.map(lambda _: post(stack, key, body(max_tokens=600, content="hi")), range(8)))
    codes = sorted(r.status_code for r in responses)
    assert codes == [200] * 3 + [429] * 5
    for r in responses:
        if r.status_code == 429:
            assert_standard_error(r, 429, "rate_limit_exceeded")
            assert int(r.headers["retry-after"]) >= 1
    # reconciliation: what is still counted is the real usage of the three answered requests
    assert usage(stack, tenant) == sum(r.json()["usage"]["total_tokens"] for r in responses if r.status_code == 200)


def test_limit_survives_a_gateway_restart(stack, providers, tmp_path):
    """State is on disk: a second gateway process sharing the file sees the same budget."""
    from conftest import ROOT, start
    _, key = stack.tenant(limit=2_000)
    providers("primary", "slow", 1.5)
    with ThreadPoolExecutor(3) as pool:                     # fill the budget with in-flight holds
        first = [pool.submit(post, stack, key, body(max_tokens=600, content="hi")) for _ in range(3)]
        time.sleep(0.5)
        other = start(ROOT / "gateway.py", "GATEWAY_PORT", tmp_path / "gw2.log", ROUTER_DB=stack.db_path,
                      PRIMARY_URL=f"{stack.primary.base}/v1/chat/completions",
                      SECONDARY_URL=f"{stack.secondary.base}/v1/chat/completions", SEED_DEMO_TENANTS="0")
        try:
            response = httpx.post(f"{other.base}/v1/chat/completions", json=body(max_tokens=600, content="hi"),
                                  headers={"Authorization": f"Bearer {key}"}, timeout=10)
            assert response.status_code == 429
        finally:
            other.process.terminate()
            other.process.wait(timeout=10)
        assert all(f.result().status_code == 200 for f in first)


# ----------------------------------------------------------------------------- failover
@pytest.mark.parametrize("mode", ["rate429", "error500"])
def test_primary_failure_fails_over_to_secondary(stack, providers, mode):
    _, key = stack.tenant()
    providers("primary", mode)
    response = post(stack, key, body())
    assert response.status_code == 200 and response.json()["model"] == "secondary-model"


def test_primary_timeout_fails_over_after_3000ms_and_is_abandoned(stack, providers):
    tenant, key = stack.tenant()
    providers("primary", "slow", 6.0)
    start = time.perf_counter()
    response = post(stack, key, body())
    elapsed = time.perf_counter() - start
    assert response.json()["model"] == "secondary-model"
    assert 3.0 <= elapsed < 3.5
    time.sleep(0.3)
    assert providers.stats("primary")["abandoned"] == 1     # the gateway hung up on the slow primary
    assert usage(stack, tenant) == response.json()["usage"]["total_tokens"]  # charged once, for the answer


def test_both_rate_limited_is_a_sanitised_503(stack, providers):
    tenant, key = stack.tenant()
    providers("primary", "rate429")
    providers("secondary", "rate429")
    response = post(stack, key, body())
    assert_standard_error(response, 503, "upstream_overloaded")
    assert "retry-after" in response.headers
    assert not any(leak in response.text for leak in LEAKS)
    assert usage(stack, tenant) == 0                        # nobody answered: full refund


def test_both_failing_with_500_is_a_sanitised_502(stack, providers):
    tenant, key = stack.tenant()
    providers("primary", "error500")
    providers("secondary", "error500")
    response = post(stack, key, body())
    assert_standard_error(response, 502, "upstream_unavailable")
    assert not any(leak in response.text for leak in LEAKS)
    assert usage(stack, tenant) == 0


# ----------------------------------------------------------------------------- streaming
def read_stream(stack, key, payload):
    events = []
    with httpx.stream("POST", stack.url, json=payload, headers={"Authorization": f"Bearer {key}"}, timeout=40) as r:
        assert r.status_code == 200
        buffer = b""
        for chunk in r.iter_bytes():
            buffer += chunk
            while b"\n\n" in buffer:
                raw, buffer = buffer.split(b"\n\n", 1)
                data = raw.decode().removeprefix("data: ")
                events.append(data if data == "[DONE]" else json.loads(data))
    return events


def test_stream_is_relayed_and_settled_from_final_usage(stack, providers):
    tenant, key = stack.tenant()
    events = read_stream(stack, key, body(max_tokens=200, stream=True))
    assert events[-1] == "[DONE]"
    reported = next(e["usage"]["total_tokens"] for e in events if isinstance(e, dict) and "usage" in e)
    time.sleep(0.2)
    assert usage(stack, tenant) == reported


def test_stream_fails_over_only_before_the_first_chunk(stack, providers):
    _, key = stack.tenant()
    providers("primary", "slow", 6.0)
    events = read_stream(stack, key, body(stream=True))
    models = {e["model"] for e in events if isinstance(e, dict) and "model" in e}
    assert models == {"secondary-model"}                    # one model only, never a mix


def test_stall_after_streaming_started_is_an_in_band_error_not_a_failover(stack, providers):
    tenant, key = stack.tenant()
    providers("primary", "stall", 6.0)
    events = read_stream(stack, key, body(max_tokens=20, stream=True))
    assert {e["model"] for e in events if isinstance(e, dict) and "model" in e} == {"primary-model"}
    error = events[-1]["error"]
    assert error["type"] == "upstream_interrupted" and error["request_id"].startswith("req_")
    assert providers.stats("secondary")["received"] == 0
    time.sleep(0.2)
    assert usage(stack, tenant) > 0                         # true cost unknown: worst case stays charged


# ----------------------------------------------------------------------------- runs last (file order)
def test_gateway_never_logged_a_traceback(stack):
    assert "Traceback" not in stack.gateway.log()