"""Model router: try the primary provider under a deadline, fail over to the secondary.

Failover happens when the primary returns 429 or does not respond within 3000 ms.
It also happens on 5xx and connection errors (the provider is unavailable either way);
it does NOT happen on other 4xx errors, because a malformed request would fail on the
backup too.

"Responds" means:
  * non-streaming: the complete reply arrived within the deadline
  * streaming: the first chunk of the reply arrived within the deadline. After that the
    stream belongs to the client: text already sent cannot be replaced by another model.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field

import httpx

log = logging.getLogger("llm-router")

PRIMARY_URL = os.environ.get("PRIMARY_URL", "http://127.0.0.1:9201/v1/chat/completions")
SECONDARY_URL = os.environ.get("SECONDARY_URL", "http://127.0.0.1:9202/v1/chat/completions")
PRIMARY_DEADLINE = float(os.environ.get("PRIMARY_DEADLINE_MS", "3000")) / 1000
SECONDARY_DEADLINE = float(os.environ.get("SECONDARY_DEADLINE_MS", "30000")) / 1000

FAILOVER_STATUSES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Provider:
    name: str
    url: str
    deadline: float  # seconds


PROVIDERS = (
    Provider("primary", PRIMARY_URL, PRIMARY_DEADLINE),
    Provider("secondary", SECONDARY_URL, SECONDARY_DEADLINE),
)


@dataclass
class Attempt:
    """What happened when one provider was tried. Kept for logs, never sent to clients."""
    provider: str
    outcome: str                 # "ok" | "timeout" | "rate_limited" | "upstream_error" | "unreachable" | "rejected"
    status: int | None = None
    elapsed_ms: int = 0
    retry_after: str | None = None


@dataclass
class RouteResult:
    provider: str | None = None              # who answered (None if nobody did)
    status: int | None = None
    body: bytes | None = None                # non-streaming: the complete reply
    first_chunk: bytes | None = None         # streaming: already-received first chunk...
    response: httpx.Response | None = None   # ...and the open response to read the rest from
    attempts: list[Attempt] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.provider is not None


@dataclass
class _Reply:
    status: int
    response: httpx.Response
    body: bytes | None = None
    first_chunk: bytes | None = None


async def _fetch(client: httpx.AsyncClient, provider: Provider, payload: dict, stream: bool) -> _Reply:
    """Talk to one provider until it has 'responded' (see module docstring). No deadline here."""
    response = await client.send(client.build_request("POST", provider.url, json=payload), stream=True)
    try:
        if response.status_code != 200:
            await response.aread()   # drained so the connection can be reused; the body is never forwarded
            await response.aclose()
            return _Reply(response.status_code, response)
        if stream:
            first = await anext(response.aiter_raw(), b"")
            return _Reply(200, response, first_chunk=first)
        body = await response.aread()
        await response.aclose()
        return _Reply(200, response, body=body)
    except BaseException:
        await response.aclose()
        raise


async def _discard(task: "asyncio.Task[_Reply]") -> None:
    """Cancel an attempt we gave up on, and close its connection even if it won the race."""
    task.cancel()
    try:
        late = await task                 # it may have finished in the instant before cancel()
    except BaseException:
        return                            # cancelled or failed: httpx already closed the connection
    await late.response.aclose()          # finished too late: we already chose to fail over


async def _try(client: httpx.AsyncClient, provider: Provider, payload: dict, stream: bool) -> tuple[Attempt, RouteResult | None]:
    """One attempt at one provider, with a hard deadline and exactly one outcome."""
    loop = asyncio.get_running_loop()
    started = loop.time()

    def attempt(outcome: str, status: int | None = None, retry_after: str | None = None) -> Attempt:
        return Attempt(provider.name, outcome, status, int((loop.time() - started) * 1000), retry_after)

    # The attempt runs as its own task, and we wait for it with a timeout. Whatever
    # asyncio.wait reports at the deadline is final: either the reply is in `done` (use it)
    # or it is not (cancel it and fail over). A reply arriving a microsecond later is
    # closed and ignored, so a request can never be answered or billed twice.
    task = asyncio.create_task(_fetch(client, provider, payload, stream))
    try:
        done, _ = await asyncio.wait({task}, timeout=provider.deadline)
    except asyncio.CancelledError:
        await asyncio.shield(_discard(task))   # WE were cancelled (client left): stop the provider too
        raise

    if task not in done:
        await _discard(task)
        return attempt("timeout"), None

    try:
        reply = task.result()
    except httpx.HTTPError as exc:
        log.warning("%s unreachable: %r", provider.name, exc)
        return attempt("unreachable"), None

    if reply.status != 200:
        outcome = ("rate_limited" if reply.status == 429
                   else "upstream_error" if reply.status in FAILOVER_STATUSES
                   else "rejected")
        return attempt(outcome, reply.status, reply.response.headers.get("retry-after")), None

    result = RouteResult(provider.name, 200, body=reply.body, first_chunk=reply.first_chunk,
                         response=reply.response if stream else None)
    return attempt("ok", 200), result


async def route(client: httpx.AsyncClient, payload: dict, stream: bool) -> RouteResult:
    """Try each provider in order until one answers or a non-retryable error occurs."""
    attempts: list[Attempt] = []
    for provider in PROVIDERS:
        attempt, result = await _try(client, provider, payload, stream)
        attempts.append(attempt)
        log.info("attempt provider=%s outcome=%s status=%s elapsed_ms=%d",
                 attempt.provider, attempt.outcome, attempt.status, attempt.elapsed_ms)
        if result is not None:
            result.attempts = attempts
            return result
        if attempt.outcome == "rejected":
            break                     # the request itself is bad: the backup would reject it too
    return RouteResult(attempts=attempts)