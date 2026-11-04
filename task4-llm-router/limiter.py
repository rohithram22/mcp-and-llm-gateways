"""Token-aware sliding-window rate limiter, stored in SQLite.

A tenant may use at most `limit` tokens in any 60-second window. Each admitted request
RESERVES its worst-case cost up front (one row in token_usage); once the provider reports
real usage, the reservation is SETTLED to the actual number, or RELEASED if no model
answered. Rows older than the window no longer count and are deleted (eviction).
"""

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Callable

import db

WINDOW_SECONDS = 60.0


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reservation_id: int | None   # the usage row to settle later (allowed only)
    used: int                    # tokens counted in the window, before this request
    limit: int
    retry_after: int | None      # whole seconds until the request would fit; None = it never can


class RateLimiter:
    def __init__(self, path: str | None = None, window: float = WINDOW_SECONDS,
                 clock: Callable[[], float] = time.time):
        self.path = path
        self.window = window
        self.clock = clock  # injectable, so tests can move time forward without sleeping

    def reserve(self, tenant_id: str, limit: int, tokens: int) -> Decision:
        """Admit the request and record its reservation, or refuse it. All in one transaction."""
        if tokens > limit:
            # Bigger than the whole budget: waiting will never help.
            return Decision(False, None, 0, limit, None)

        with db.connect(self.path) as conn:
            # BEGIN IMMEDIATE takes SQLite's write lock NOW, before reading. Any other
            # request (thread or process) trying to reserve waits here until we commit,
            # so check-then-insert below cannot be interleaved with another one.
            conn.execute("BEGIN IMMEDIATE")
            try:
                now = self.clock()
                cutoff = now - self.window

                # Eviction: rows older than the window will never count again.
                conn.execute("DELETE FROM token_usage WHERE ts <= ?", (cutoff,))

                used = conn.execute(
                    "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant_id = ? AND ts > ?",
                    (tenant_id, cutoff),
                ).fetchone()[0]

                if used + tokens <= limit:
                    cursor = conn.execute(
                        "INSERT INTO token_usage (tenant_id, ts, tokens) VALUES (?, ?, ?)", (tenant_id, now, tokens)
                    )
                    conn.execute("COMMIT")
                    return Decision(True, cursor.lastrowid, used, limit, 0)

                retry_after = self._retry_after(conn, tenant_id, cutoff, now, need=used + tokens - limit)
                conn.execute("COMMIT")
                return Decision(False, None, used, limit, retry_after)
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def _retry_after(self, conn, tenant_id: str, cutoff: float, now: float, need: int) -> int:
        """Seconds until enough of the oldest rows slide out of the window to free `need` tokens."""
        freed = 0
        for ts, tokens in conn.execute(
            "SELECT ts, tokens FROM token_usage WHERE tenant_id = ? AND ts > ? ORDER BY ts", (tenant_id, cutoff)
        ):
            freed += tokens
            if freed >= need:
                return max(1, math.ceil(ts + self.window - now))
        return max(1, math.ceil(self.window))  # unreachable when tokens <= limit; safe fallback

    def settle(self, reservation_id: int, actual_tokens: int) -> None:
        """Replace the worst-case reservation with what the provider really used."""
        with db.connect(self.path) as conn:
            conn.execute("UPDATE token_usage SET tokens = ? WHERE id = ?", (max(0, actual_tokens), reservation_id))

    def release(self, reservation_id: int) -> None:
        """No model produced anything: refund the whole reservation."""
        with db.connect(self.path) as conn:
            conn.execute("DELETE FROM token_usage WHERE id = ?", (reservation_id,))

    def usage(self, tenant_id: str) -> int:
        """Tokens currently counted in the tenant's window (for monitoring and tests)."""
        with db.connect(self.path) as conn:
            return conn.execute(
                "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant_id = ? AND ts > ?",
                (tenant_id, self.clock() - self.window),
            ).fetchone()[0]

    # ------------------------------------------------------------------ async API
    # sqlite3 blocks: waiting for the write lock or the disk would freeze the event loop
    # and every other request with it. These wrappers run each call on a worker thread.

    async def areserve(self, tenant_id: str, limit: int, tokens: int) -> Decision:
        work = asyncio.ensure_future(asyncio.to_thread(self.reserve, tenant_id, limit, tokens))
        try:
            # shield: if WE are cancelled (e.g. the client hung up), the worker thread cannot
            # be stopped and may still commit a reservation. Don't lose track of it...
            return await asyncio.shield(work)
        except asyncio.CancelledError:
            # ...refund it as soon as the thread finishes, so no tokens stay held by a
            # request that no longer exists.
            work.add_done_callback(self._release_orphan)
            raise

    def _release_orphan(self, work: "asyncio.Future[Decision]") -> None:
        if work.cancelled() or work.exception() is not None:
            return
        decision = work.result()
        if decision.allowed:
            asyncio.ensure_future(asyncio.to_thread(self.release, decision.reservation_id))

    async def asettle(self, reservation_id: int, actual_tokens: int) -> None:
        await asyncio.shield(asyncio.to_thread(self.settle, reservation_id, actual_tokens))

    async def arelease(self, reservation_id: int) -> None:
        await asyncio.shield(asyncio.to_thread(self.release, reservation_id))