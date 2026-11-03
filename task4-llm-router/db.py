"""On-disk SQLite storage for tenants and the token-usage log.

Every call opens its own short-lived connection. SQLite connections are cheap, and
this keeps them safe to use from the worker threads that asyncio.to_thread runs on.
"""

import hashlib
import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.environ.get("ROUTER_DB", "task4-router.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    id                 TEXT PRIMARY KEY,
    key_hash           TEXT NOT NULL UNIQUE,      -- SHA-256 of the API key; the key itself is never stored
    tokens_per_minute  INTEGER NOT NULL CHECK (tokens_per_minute > 0)
);

-- The sliding-window log: one row per charge. A tenant's usage "right now" is the
-- sum of its rows from the last 60 seconds.
CREATE TABLE IF NOT EXISTS token_usage (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id),
    ts         REAL NOT NULL,                     -- unix time (seconds) the tokens were charged
    tokens     INTEGER NOT NULL CHECK (tokens >= 0)
);

-- Makes "this tenant's rows newer than X" a fast index range scan instead of a full table scan.
CREATE INDEX IF NOT EXISTS idx_usage_tenant_ts ON token_usage (tenant_id, ts);

-- Makes "delete every row older than X" (eviction) fast too.
CREATE INDEX IF NOT EXISTS idx_usage_ts ON token_usage (ts);
"""


def hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


@contextmanager
def connect(path: str | None = None):
    conn = sqlite3.connect(path or DB_PATH, timeout=10.0, isolation_level=None)  # we manage transactions
    try:
        conn.execute("PRAGMA busy_timeout = 10000")   # wait up to 10 s for a lock instead of failing
        yield conn
    finally:
        conn.close()


def init_db(path: str | None = None) -> None:
    with connect(path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")     # readers don't block the writer, and vice versa
        conn.executescript(SCHEMA)


def add_tenant(tenant_id: str, api_key: str, tokens_per_minute: int = 50_000, path: str | None = None) -> None:
    with connect(path) as conn:
        conn.execute(
            "INSERT INTO tenants (id, key_hash, tokens_per_minute) VALUES (?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET key_hash = excluded.key_hash, tokens_per_minute = excluded.tokens_per_minute",
            (tenant_id, hash_key(api_key), tokens_per_minute),
        )


def find_tenant(api_key: str, path: str | None = None) -> tuple[str, int] | None:
    """(tenant_id, tokens_per_minute) for an API key, or None if the key is unknown."""
    with connect(path) as conn:
        row = conn.execute(
            "SELECT id, tokens_per_minute FROM tenants WHERE key_hash = ?", (hash_key(api_key),)
        ).fetchone()
    return (row[0], row[1]) if row else None