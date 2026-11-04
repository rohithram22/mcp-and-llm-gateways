"""Rate limiter: window accuracy, eviction, reconciliation, and concurrency. No servers."""

import asyncio
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import db
from limiter import RateLimiter
from tokens import InvalidRequest, reservation_for

LIMIT = 50_000


@pytest.fixture
def limiter(db_path, clock):
    db.add_tenant("acme", "sk-acme", LIMIT, path=db_path)
    db.add_tenant("globex", "sk-globex", LIMIT, path=db_path)
    return RateLimiter(path=db_path, clock=clock)


def rows(db_path) -> int:
    with db.connect(db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0]


# ----------------------------------------------------------------------------- admission
def test_allows_up_to_exactly_the_limit(limiter):
    assert limiter.reserve("acme", LIMIT, 30_000).allowed
    assert limiter.reserve("acme", LIMIT, 20_000).allowed       # exactly 50,000 is allowed
    assert not limiter.reserve("acme", LIMIT, 1).allowed        # one more token is not


def test_request_larger_than_the_whole_budget_can_never_fit(limiter):
    decision = limiter.reserve("acme", LIMIT, LIMIT + 1)
    assert not decision.allowed and decision.retry_after is None


def test_tenants_have_separate_budgets(limiter):
    assert limiter.reserve("acme", LIMIT, LIMIT).allowed
    assert limiter.reserve("globex", LIMIT, LIMIT).allowed      # acme being full does not affect globex


def test_retry_after_is_when_enough_old_tokens_expire(limiter, clock):
    limiter.reserve("acme", LIMIT, 30_000)                      # t=0
    clock.advance(20)
    limiter.reserve("acme", LIMIT, 15_000)                      # t=20
    clock.advance(10)                                           # t=30: 45,000 used
    decision = limiter.reserve("acme", LIMIT, 8_000)            # 3,000 too many
    assert not decision.allowed and decision.retry_after == 30  # the t=0 row expires at t=60
    clock.advance(30)
    assert limiter.reserve("acme", LIMIT, 8_000).allowed        # and then it really fits


# ----------------------------------------------------------------------------- sliding window
def test_window_slides_rather_than_resetting(limiter, clock):
    """A fixed per-minute window lets 2x the limit through around a minute boundary. A sliding one must not."""
    clock.advance(59)
    assert limiter.reserve("acme", LIMIT, LIMIT).allowed        # spend everything at 0:59
    clock.advance(2)                                            # 1:01: a fixed window would have reset here
    assert not limiter.reserve("acme", LIMIT, 1).allowed
    clock.advance(58)                                           # 1:59: the 0:59 tokens are exactly 60 s old
    assert limiter.reserve("acme", LIMIT, LIMIT).allowed


def test_window_boundary_is_exact(limiter, clock):
    limiter.reserve("acme", LIMIT, 10_000)
    clock.advance(59.999)
    assert limiter.usage("acme") == 10_000                      # still inside the window
    clock.advance(0.001)
    assert limiter.usage("acme") == 0                           # exactly 60 s old: no longer counts


# ----------------------------------------------------------------------------- eviction
def test_expired_rows_are_deleted_and_recent_ones_kept(limiter, clock, db_path):
    for _ in range(5):
        limiter.reserve("acme", LIMIT, 100)
    clock.advance(30)
    limiter.reserve("globex", LIMIT, 100)
    clock.advance(31)                                           # acme rows 61 s old, globex 31 s old
    limiter.reserve("acme", LIMIT, 100)                         # any reservation triggers eviction
    assert rows(db_path) == 2                                   # globex's row and the new acme row
    assert limiter.usage("globex") == 100


def test_table_stays_small_under_steady_traffic(limiter, clock, db_path):
    for _ in range(600):                                        # 10 minutes, one request per second
        limiter.reserve("acme", LIMIT, 10)
        clock.advance(1)
    assert rows(db_path) <= 61


# ----------------------------------------------------------------------------- reconciliation
def test_settle_replaces_the_worst_case_with_real_usage(limiter):
    big = limiter.reserve("acme", LIMIT, 45_000)
    assert not limiter.reserve("acme", LIMIT, 10_000).allowed
    limiter.settle(big.reservation_id, 1_200)                   # the reply was short
    assert limiter.usage("acme") == 1_200
    assert limiter.reserve("acme", LIMIT, 10_000).allowed


def test_settle_keeps_the_original_timestamp(limiter, clock):
    reservation = limiter.reserve("acme", LIMIT, 5_000)
    clock.advance(50)
    limiter.settle(reservation.reservation_id, 4_000)           # settled late...
    clock.advance(10)
    assert limiter.usage("acme") == 0                           # ...but still expires 60 s after it was reserved


def test_release_is_a_full_refund(limiter):
    reservation = limiter.reserve("acme", LIMIT, 20_000)
    limiter.release(reservation.reservation_id)
    assert limiter.usage("acme") == 0


# ----------------------------------------------------------------------------- token pricing
def test_reservation_is_prompt_estimate_plus_max_tokens():
    payload = {"messages": [{"role": "user", "content": "x" * 400}], "max_tokens": 300}
    assert reservation_for(payload) == 100 + 4 + 300


@pytest.mark.parametrize("payload", [
    {}, {"messages": []}, {"messages": "hi"},
    {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 0},
    {"messages": [{"role": "user", "content": "hi"}], "max_tokens": -5},
    {"messages": [{"role": "user", "content": "hi"}], "max_tokens": "lots"},
    {"messages": [{"role": "user", "content": "hi"}], "max_tokens": True},
])
def test_unpriceable_requests_are_rejected(payload):
    with pytest.raises(InvalidRequest):
        reservation_for(payload)


# ----------------------------------------------------------------------------- concurrency
def test_simultaneous_requests_never_overspend(db_path):
    """50 threads reserve 4,000 tokens at once. Exactly 12 fit in 50,000, never 13."""
    db.add_tenant("acme", "k", LIMIT, path=db_path)
    limiter = RateLimiter(path=db_path)
    with ThreadPoolExecutor(max_workers=50) as pool:
        decisions = list(pool.map(lambda _: limiter.reserve("acme", LIMIT, 4_000), range(50)))
    assert sum(d.allowed for d in decisions) == 12
    assert limiter.usage("acme") == 48_000


def test_separate_processes_sharing_the_file_never_overspend(db_path, tmp_path):
    """The lock is SQLite's, so it also holds across gateway processes."""
    db.add_tenant("acme", "k", LIMIT, path=db_path)
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import sys; sys.path.insert(0, sys.argv[1])\n"
        "from limiter import RateLimiter\n"
        "from concurrent.futures import ThreadPoolExecutor\n"
        "lim = RateLimiter(path=sys.argv[2])\n"
        "with ThreadPoolExecutor(25) as p:\n"
        "    print(sum(d.allowed for d in p.map(lambda _: lim.reserve('acme', 50000, 4000), range(25))))\n"
    )
    root = str(Path(__file__).resolve().parent.parent)
    procs = [subprocess.Popen([sys.executable, str(worker), root, db_path], stdout=subprocess.PIPE, text=True)
             for _ in range(4)]
    admitted = sum(int(p.communicate(timeout=60)[0]) for p in procs)
    assert admitted == 12
    assert RateLimiter(path=db_path).usage("acme") == 48_000


def test_async_reservations_do_not_block_the_event_loop(db_path):
    db.add_tenant("acme", "k", LIMIT, path=db_path)
    limiter = RateLimiter(path=db_path)

    async def run():
        gaps, last, done = [], time.perf_counter(), False

        async def heartbeat():
            nonlocal last
            while not done:
                await asyncio.sleep(0.005)
                gaps.append(time.perf_counter() - last)
                last = time.perf_counter()

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.02)
        await asyncio.gather(*[limiter.areserve("acme", LIMIT, 1) for _ in range(100)])
        done = True
        await beat
        return max(gaps)

    assert asyncio.run(run()) < 0.05


def test_cancelled_reservation_is_refunded(db_path):
    """The client leaves while its reservation is being written: the orphaned hold must be released."""
    db.add_tenant("acme", "k", LIMIT, path=db_path)
    limiter = RateLimiter(path=db_path)

    async def run():
        task = asyncio.create_task(limiter.areserve("acme", LIMIT, 10_000))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(50):
            await asyncio.sleep(0.02)
            if limiter.usage("acme") == 0:
                return 0
        return limiter.usage("acme")

    assert asyncio.run(run()) == 0