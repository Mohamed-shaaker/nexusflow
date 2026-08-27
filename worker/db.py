"""
NexusFlow — worker/db.py
========================
Async PostgreSQL helper for the worker process.

The worker is an asyncio-based process, so it uses the same asyncpg pool
pattern as the gateway.  The pool is initialised once in ``main()`` and
shared across all coroutine calls.

State machine handled here:
    pending     →  processing  (when XREADGROUP delivers the message)
    processing  →  completed   (when XACK is called after success)
    processing  →  pending     (transient retry: retry_count < max_retries)
    processing  →  failed      (retries exhausted: retry_count >= max_retries)
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
_pool: asyncpg.Pool | None = None


async def init_pool(min_size: int = 1, max_size: int = 5) -> None:
    """
    Open the asyncpg connection pool.  Call once at worker startup.

    The worker processes tasks sequentially so a small pool (1–5) is enough.
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
    """Gracefully close all pool connections.  Call on worker shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("PostgreSQL pool closed.")


def _pool_or_raise() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialised. "
            "Ensure init_pool() was awaited before processing tasks."
        )
    return _pool


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


async def fetch_task_retry_info(task_id: str) -> dict[str, Any]:
    """
    Fetch the current retry counters for a task row.

    Returns a dict with keys ``retry_count`` and ``max_retries``.
    Raises ``LookupError`` if the task_id is not found.
    """
    async with _pool_or_raise().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT retry_count, max_retries FROM tasks WHERE task_id = $1",
            task_id,
        )
    if row is None:
        raise LookupError(f"Task not found in database: task_id={task_id!r}")
    return {"retry_count": row["retry_count"], "max_retries": row["max_retries"]}


async def update_task_status(
    task_id: str,
    status: str,
    result: dict[str, Any] | None = None,
) -> None:
    """
    Update the status (and optional result) of a task row.

    Parameters
    ----------
    task_id: The task's UUID string (stored in the Redis message field).
    status:  Target status: 'processing', 'completed', or 'failed'.
    result:  Optional result payload; only written for completed/failed.
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


async def update_task_for_retry(task_id: str, error_message: str) -> None:
    """
    Atomically increment ``retry_count``, reset status to ``'pending'``,
    and record the latest ``error_message``.

    Called when a task fails but still has retries remaining.
    """
    now = datetime.now(tz=timezone.utc)
    async with _pool_or_raise().acquire() as conn:
        await conn.execute(
            """
            UPDATE tasks
               SET status        = 'pending',
                   retry_count   = retry_count + 1,
                   error_message = $2,
                   updated_at    = $3
             WHERE task_id = $1
            """,
            task_id,
            error_message,
            now,
        )
    log.debug("Queued retry  task_id=%s  error=%s", task_id, error_message)


async def update_task_as_failed(task_id: str, error_message: str) -> None:
    """
    Mark a task as permanently ``'failed'`` and record the ``error_message``.

    Called when ``retry_count >= max_retries`` — no further retries will occur.
    """
    now = datetime.now(tz=timezone.utc)
    async with _pool_or_raise().acquire() as conn:
        await conn.execute(
            """
            UPDATE tasks
               SET status        = 'failed',
                   error_message = $2,
                   updated_at    = $3
             WHERE task_id = $1
            """,
            task_id,
            error_message,
            now,
        )
    log.debug("Marked failed  task_id=%s  error=%s", task_id, error_message)

