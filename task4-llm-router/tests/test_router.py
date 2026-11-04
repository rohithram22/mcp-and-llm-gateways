"""Router: failover rules, the deadline, and the timeout race. Uses an in-memory fake
transport instead of servers, so timings are exact and every connection can be counted."""

import asyncio
import json
import random

import httpx
import pytest

import router
from router import Provider

DEADLINE = 0.3   # a short deadline keeps these tests fast; the logic is identical at 3 s


class TrackedStream(httpx.AsyncByteStream):
    """A response body that records whether it was closed, so tests can catch leaks."""

    open_count = 0

    def __init__(self, chunks, gap: float = 0.0):
        self.chunks, self.gap = chunks, gap
        TrackedStream.open_count += 1
        self.closed = False

    async def __aiter__(self):
        for i, chunk in enumerate(self.chunks):
            if i and self.gap:
                await asyncio.sleep(self.gap)
            yield chunk

    async def aclose(self):
        if not self.closed:
            self.closed = True
            TrackedStream.open_count -= 1


class FakeProvider:
    """Scripted behaviour per provider: status, delay before replying, or a connection error."""

    def __init__(self, status=200, delay=0.0, error=None, stream_gap=0.0):
        self.status, self.delay, self.error, self.stream_gap = status, delay, error, stream_gap
        self.calls = 0
        self.cancelled = 0

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        if self.error:
            raise self.error
        body = json.dumps({"model": request.url.host, "usage": {"total_tokens": 7}}).encode()
        chunks = [b"data: first\n\n", b"data: second\n\n"] if self.status == 200 else [body]
        return httpx.Response(self.status, stream=TrackedStream(chunks if self.status == 200 else [body],
                                                                self.stream_gap))


@pytest.fixture
def setup(monkeypatch):
    TrackedStream.open_count = 0
    fakes = {"primary": FakeProvider(), "secondary": FakeProvider()}

    async def dispatch(request: httpx.Request):
        return await fakes[request.url.host].handle(request)

    monkeypatch.setattr(router, "PROVIDERS", (
        Provider("primary", "http://primary/v1/chat/completions", DEADLINE),
        Provider("secondary", "http://secondary/v1/chat/completions", DEADLINE),
    ))
    client = httpx.AsyncClient(transport=httpx.MockTransport(dispatch))
    return fakes, client


def run_route(client, stream=False):
    async def go():
        result = await router.route(client, {"messages": []}, stream)
        if result.response is not None:
            await result.response.aclose()
        return result
    return asyncio.run(go())


def outcomes(result):
    return [(a.provider, a.outcome) for a in result.attempts]


# ----------------------------------------------------------------------------- failover rules
def test_healthy_primary_answers(setup):
    fakes, client = setup
    result = run_route(client)
    assert result.provider == "primary" and fakes["secondary"].calls == 0


@pytest.mark.parametrize("status,outcome", [(429, "rate_limited"), (500, "upstream_error"), (503, "upstream_error")])
def test_failover_on_429_and_5xx(setup, status, outcome):
    fakes, client = setup
    fakes["primary"].status = status
    result = run_route(client)
    assert result.provider == "secondary"
    assert outcomes(result) == [("primary", outcome), ("secondary", "ok")]


def test_failover_when_primary_unreachable(setup):
    fakes, client = setup
    fakes["primary"].error = httpx.ConnectError("refused")
    assert run_route(client).provider == "secondary"


@pytest.mark.parametrize("status", [400, 401, 422])
def test_no_failover_on_client_errors(setup, status):
    """A malformed request would fail on the backup too: don't spend a second call on it."""
    fakes, client = setup
    fakes["primary"].status = status
    result = run_route(client)
    assert not result.ok and fakes["secondary"].calls == 0
    assert outcomes(result) == [("primary", "rejected")]


def test_both_failing_reports_every_attempt(setup):
    fakes, client = setup
    fakes["primary"].status = fakes["secondary"].status = 429
    result = run_route(client)
    assert not result.ok
    assert outcomes(result) == [("primary", "rate_limited"), ("secondary", "rate_limited")]


# ----------------------------------------------------------------------------- the deadline
def test_slow_primary_times_out_at_the_deadline(setup):
    fakes, client = setup
    fakes["primary"].delay = 5
    result = run_route(client)
    assert result.provider == "secondary"
    assert DEADLINE * 1000 <= result.attempts[0].elapsed_ms < DEADLINE * 1000 + 100
    assert fakes["primary"].cancelled == 1            # the slow call was really stopped


def test_reply_just_before_the_deadline_is_used(setup):
    fakes, client = setup
    fakes["primary"].delay = DEADLINE - 0.1
    assert run_route(client).provider == "primary"


def test_reply_just_after_the_deadline_is_not(setup):
    fakes, client = setup
    fakes["primary"].delay = DEADLINE + 0.1
    assert run_route(client).provider == "secondary"


def test_timeout_race_has_exactly_one_outcome_and_leaks_nothing(setup):
    """Replies landing within a few ms either side of the deadline, 40 times. Each request
    must be answered by exactly one provider, and no connection may be left open."""
    fakes, client = setup
    rng = random.Random(7)
    seen = set()
    for _ in range(40):
        fakes["primary"].delay = DEADLINE + rng.uniform(-0.004, 0.004)
        result = run_route(client)
        assert result.ok
        assert sum(a.outcome == "ok" for a in result.attempts) == 1
        seen.add(result.provider)
    assert TrackedStream.open_count == 0
    assert seen == {"primary", "secondary"}           # both sides of the boundary were exercised


# ----------------------------------------------------------------------------- streaming
def test_stream_deadline_is_time_to_first_chunk(setup):
    """A primary that starts streaming in time keeps the request, even if it is slow afterwards."""
    fakes, client = setup
    fakes["primary"].stream_gap = 1.0                 # first chunk at once, second much later
    result = run_route(client, stream=True)
    assert result.provider == "primary" and result.first_chunk == b"data: first\n\n"
    assert fakes["secondary"].calls == 0


def test_stream_with_no_first_chunk_in_time_fails_over(setup):
    fakes, client = setup
    fakes["primary"].delay = 5
    result = run_route(client, stream=True)
    assert result.provider == "secondary"


def test_successful_stream_hands_over_the_same_iterator(setup):
    fakes, client = setup

    async def go():
        result = await router.route(client, {"messages": []}, True)
        rest = [chunk async for chunk in result.chunks]
        await result.response.aclose()
        return result.first_chunk, rest

    first, rest = asyncio.run(go())
    assert first == b"data: first\n\n" and rest == [b"data: second\n\n"]


# ----------------------------------------------------------------------------- cancellation
def test_cancelling_the_route_cancels_the_provider_call(setup):
    """The client leaves while the gateway waits for the primary: the provider call must stop too."""
    fakes, client = setup
    fakes["primary"].delay = 5

    async def go():
        task = asyncio.create_task(router.route(client, {"messages": []}, False))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)
        # Checked INSIDE the loop: asyncio.run would cancel leftover tasks on exit and hide a leak.
        return fakes["primary"].cancelled

    assert asyncio.run(go()) == 1 and fakes["secondary"].calls == 0


def test_a_reply_that_arrives_after_giving_up_is_closed(setup):
    """The race at the deadline: the call finishes in the instant between 'too late' and
    cancel(). _discard must still close that reply's connection."""
    fakes, client = setup

    async def go():
        reply_body = TrackedStream([b"late"])
        response = httpx.Response(200, stream=reply_body)
        finished = asyncio.get_running_loop().create_future()
        finished.set_result(router._Reply(200, response))   # already completed: cancel() cannot stop it
        await router._discard(finished)
        return reply_body.closed

    assert asyncio.run(go()) is True