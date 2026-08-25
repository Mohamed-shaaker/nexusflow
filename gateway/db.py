"""
NexusFlow — gateway/db.py
=========================
Async PostgreSQL helper built on top of asyncpg's connection pool.

Lifecycle
---------
Call ``init_pool()`` once during application startup (FastAPI lifespan) and
``close_pool()`` during shutdown.  All other functions borrow a connection from
the pool transparently — no manual acquire/release needed.

State machine
-------------
    pending  →  processing  →  completed
                            ↘  failed
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg

log = logging.getLogger("nexusflow.db")

# ---------------------------------------------------------------------------
# Configuration — read from environment with sensible defaults
# ---------------------------------------------------------------------------
_PG_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
_PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
_PG_USER     = os.getenv("POSTGRES_USER",     "nexus")
_PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nexuspassword")
_PG_DB       = os.getenv("POSTGRES_DB",       "nexusflow")

_DSN = (
    f"postgresql://{_PG_USER}:{_PG_PASSWORD}"
    f"@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
)

# ---------------------------------------------------------------------------
# Connection pool (module-level singleton)
# ---------------------------------------------------------------------------
# asyncpg pools are safe to use from multiple coroutines concurrently; each
# call acquires a connection from the pool, uses it, and returns it.
_pool: asyncpg.Pool | None = None


async def init_pool(min_size: int = 2, max_size: int = 10) -> None:
    """
    Open the asyncpg connection pool.  Call once at application startup.

    ``min_size`` connections are created eagerly; additional connections are
    opened on demand up to ``max_size``.
    """
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=_DSN,
        min_size=min_size,
        max_size=max_size,
    )
    log.info(
        "PostgreSQL pool ready  host=%s db=%s  min=%d max=%d",
        _PG_HOST, _PG_DB, min_size, max_size,
    )


async def close_pool() -> None:
    """
    Gracefully close all connections in the pool.  Call on application shutdown.
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("PostgreSQL pool closed.")


def _pool_or_raise() -> asyncpg.Pool:
    """Return the pool, raising RuntimeError if it has not been initialised."""
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialised. "
            "Ensure init_pool() was called during application startup."
        )
    return _pool


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


async def create_task(
    task_id: str,
    name: str,
    payload: dict[str, Any],
    created_at: str,
) -> None:
    """
    Insert a new task row with status='pending'.

    Parameters
    ----------
    task_id:    UUID string assigned by the gateway.
    name:       Human-readable task name.
    payload:    Arbitrary JSON submitted by the caller.
    created_at: ISO-8601 UTC timestamp string from the gateway.
    """
    # asyncpg requires the JSONB value to be a JSON *string*, not a dict.
    payload_json = json.dumps(payload)
    ts = datetime.fromisoformat(created_at)

    async with _pool_or_raise().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tasks (task_id, name, status, payload, created_at, updated_at)
            VALUES ($1, $2, 'pending', $3::jsonb, $4, $4)
            """,
            task_id,
            name,
            payload_json,
            ts,
        )
    log.debug("Inserted task  task_id=%s  status=pending", task_id)


async def update_task_status(
    task_id: str,
    status: str,
    result: dict[str, Any] | None = None,
) -> None:
    """
    Update the status (and optional result) of an existing task row.

    Parameters
    ----------
    task_id: UUID of the task to update.
    status:  New status string, e.g. 'processing', 'completed', 'failed'.
    result:  Optional result payload to persist alongside the status change.
    """
    now = datetime.now(tz=timezone.utc)
    result_json = json.dumps(result) if result is not None else None

    async with _pool_or_raise().acquire() as conn:
        await conn.execute(
            """
            UPDATE tasks
               SET status     = $2,
                   result     = COALESCE($3::jsonb, result),
                   updated_at = $4
             WHERE task_id = $1
            """,
            task_id,
            status,
            result_json,
            now,
        )
    log.debug("Updated task  task_id=%s  status=%s", task_id, status)


async def fetch_task(task_id: str) -> dict[str, Any] | None:
    """
    Fetch a single task row by its primary key.

    Returns a plain dict (JSON-serialisable) or ``None`` if not found.
    asyncpg returns JSONB columns as Python dicts/lists automatically.
    """
    async with _pool_or_raise().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT task_id, name, status, payload, result,
                   created_at, updated_at
              FROM tasks
             WHERE task_id = $1
            """,
            task_id,
        )

    if row is None:
        return None

    return {
        "task_id":    row["task_id"],
        "name":       row["name"],
        "status":     row["status"],
        "payload":    row["payload"],   # already a dict — asyncpg deserialises JSONB
        "result":     row["result"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }
