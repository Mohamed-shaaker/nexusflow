"""
NexusFlow - Background Worker
==============================
Consumes tasks from a Redis Stream and processes them.

The worker uses the Redis Streams "consumer group" model:
  - Multiple worker replicas can run simultaneously without duplicating work.
  - XACK removes a message from the Pending Entries List (PEL) once processing
    succeeds or permanently fails (retries exhausted).
  - On a transient failure with retries remaining, the original PEL message is
    XACK'd and a *new* message is re-published to the stream, ensuring a clean
    re-delivery without blocking the PEL with stale entries.

PostgreSQL state transitions (via db.py):
  XREADGROUP delivers message       →  status: 'processing'
  Processing succeeds + XACK        →  status: 'completed'
  Failure + retry_count < max_retries:
      DB status reset to 'pending', retry_count incremented, XACK original
      message, re-publish new message to stream
  Failure + retry_count >= max_retries:
      status: 'failed', XACK original message (drops from PEL permanently)

Environment variables (with defaults):
  REDIS_HOST        Host of the Redis instance        (default: localhost)
  REDIS_PORT        Port of the Redis instance        (default: 6379)
  STREAM_NAME       Name of the Redis stream          (default: tasks)
  CONSUMER_GROUP    Consumer group name               (default: workers)
  CONSUMER_NAME     Unique name for this worker       (default: worker-1)
  BLOCK_MS          XREADGROUP block timeout in ms    (default: 2000)
  REQUEUE_DELAY_MS  Delay before re-queuing a retry   (default: 1000)
  POSTGRES_HOST     PostgreSQL host                   (default: localhost)
  POSTGRES_PORT     PostgreSQL port                   (default: 5432)
  POSTGRES_USER     PostgreSQL user                   (default: nexus)
  POSTGRES_PASSWORD PostgreSQL password               (default: nexuspassword)
  POSTGRES_DB       PostgreSQL database               (default: nexusflow)
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time

import redis

import db

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Use a structured format so log shippers (Fluent Bit, Loki, CloudWatch)
# can parse fields without extra config.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("nexusflow.worker")

# ---------------------------------------------------------------------------
# Configuration — read from environment, fall back to safe defaults
# ---------------------------------------------------------------------------
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME     = os.getenv("STREAM_NAME", "tasks")
CONSUMER_GROUP  = os.getenv("CONSUMER_GROUP", "workers")
CONSUMER_NAME   = os.getenv("CONSUMER_NAME", "worker-1")
BLOCK_MS        = int(os.getenv("BLOCK_MS", "2000"))       # ms to block on XREADGROUP
REQUEUE_DELAY_S = int(os.getenv("REQUEUE_DELAY_MS", "1000")) / 1000.0  # convert to seconds

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
# The main loop checks this flag. When SIGTERM or SIGINT arrives
# (e.g., kubectl delete pod / Ctrl-C), the worker finishes its current
# task before exiting cleanly — no orphaned messages left in the PEL.
_shutdown = False


def _handle_signal(signum: int, _frame) -> None:
    global _shutdown
    log.info("Received signal %s — finishing current task then shutting down.", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------

def connect_redis() -> redis.Redis:
    """
    Create and return a Redis client, retrying until the server is reachable.
    This makes the worker resilient to Redis starting slightly after the worker
    (common in docker-compose / Kubernetes pod start ordering).
    """
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    for attempt in range(1, 11):  # up to 10 attempts
        try:
            client.ping()
            log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)
            return client
        except redis.ConnectionError:
            wait = 2 ** attempt  # exponential back-off
            log.warning(
                "Redis not ready (attempt %d/10). Retrying in %ds...", attempt, wait
            )
            time.sleep(wait)
    log.error("Could not connect to Redis after 10 attempts. Exiting.")
    sys.exit(1)


def ensure_consumer_group(client: redis.Redis) -> None:
    """
    Create the consumer group if it does not already exist.
    MKSTREAM creates the stream itself if it is absent.
    '0' means new members start from the very beginning of the stream.
    """
    try:
        client.xgroup_create(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
        log.info(
            "Consumer group '%s' created on stream '%s'.", CONSUMER_GROUP, STREAM_NAME
        )
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            # Group already exists — nothing to do.
            log.info("Consumer group '%s' already exists. Joining.", CONSUMER_GROUP)
        else:
            raise


# ---------------------------------------------------------------------------
# Task processing
# ---------------------------------------------------------------------------

async def process_task(task_id: str, fields: dict) -> dict:
    """
    Core business logic placeholder.

    Replace the body of this function with real work:
    image resizing, sending an email, calling a downstream API, etc.
    The function should raise an exception on failure so the caller
    can apply the retry/failure policy.

    Returns a result dict that is persisted in PostgreSQL on success.

    Simulated failure
    -----------------
    If the task ``name`` field equals ``"fail_task"``, a ``ValueError`` is
    raised on every attempt.  Use this to exercise the retry path locally
    without needing a real downstream service to misbehave.
    """
    task_name = fields.get("name", "unknown")
    log.info("Processing task_id=%s  name=%s", task_id, task_name)

    # ── Simulated failure for local testing ───────────────────────────────────
    # Remove or guard with an env flag before deploying to production.
    if task_name == "fail_task":
        raise ValueError("Simulated failure for testing")

    # Simulate work (replace with actual async I/O or CPU-bound work via executor)
    await asyncio.sleep(0.1)

    log.info("Task completed  task_id=%s", task_id)
    return {"processed": True, "name": task_name}


# ---------------------------------------------------------------------------
# Main async loop
# ---------------------------------------------------------------------------

async def run(client: redis.Redis) -> None:
    """
    Continuously read from the Redis stream and dispatch tasks.

    XREADGROUP ">" means: give me only NEW messages not yet delivered to
    any consumer in my group. After a restart we first drain any messages
    that were pending (delivered but not ACKed) by using id "0".
    """
    log.info(
        "Worker '%s' starting — listening on stream '%s' (group: %s)",
        CONSUMER_NAME, STREAM_NAME, CONSUMER_GROUP,
    )

    # Initialise the PostgreSQL connection pool before entering the loop.
    await db.init_pool()

    try:
        while not _shutdown:
            try:
                # XREADGROUP with ">" delivers new, undelivered messages.
                # BLOCK_MS causes the call to block (park the thread) until a
                # message arrives or the timeout expires — avoids busy-waiting.
                response = client.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={STREAM_NAME: ">"},
                    count=1,
                    block=BLOCK_MS,
                )

                if not response:
                    # Timeout expired, no messages — loop and check _shutdown flag.
                    continue

                # response shape: [(stream_name, [(msg_id, {field: value, ...})])]
                for _stream, messages in response:
                    for msg_id, fields in messages:
                        # Extract the application-level task_id from the stream
                        # message fields (distinct from Redis's own msg_id).
                        task_id: str = fields.get("task_id", msg_id)

                        # ── 1. Fetch retry state & mark as processing ─────────
                        retry_info = await db.fetch_task_retry_info(task_id)
                        retry_count = retry_info["retry_count"]
                        max_retries = retry_info["max_retries"]

                        log.info(
                            "Picked up task_id=%s  attempt=%d/%d",
                            task_id, retry_count + 1, max_retries + 1,
                        )
                        await db.update_task_status(task_id, "processing")

                        try:
                            result = await process_task(task_id, fields)

                            # ── 2a. Success: ACK + mark completed ─────────────
                            client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                            await db.update_task_status(
                                task_id, "completed", result=result
                            )
                            log.info("task_id=%s completed successfully.", task_id)

                        except Exception as exc:  # noqa: BLE001
                            error_str = str(exc)
                            log.error(
                                "task_id=%s failed (attempt %d/%d): %s",
                                task_id, retry_count + 1, max_retries + 1,
                                error_str, exc_info=True,
                            )

                            if retry_count < max_retries:
                                # ── 2b. Transient failure — schedule a retry ───
                                # 1. Persist the incremented retry_count and reset
                                #    status to 'pending' so the task is visible as
                                #    "waiting to retry" in the UI/API.
                                await db.update_task_for_retry(task_id, error_str)

                                # 2. Brief back-off before re-queuing so we don't
                                #    hammer a struggling downstream service.
                                await asyncio.sleep(REQUEUE_DELAY_S)

                                # 3. Re-publish the original message fields as a
                                #    brand-new stream entry so any consumer in the
                                #    group can pick it up cleanly.
                                client.xadd(STREAM_NAME, fields)
                                log.warning(
                                    "task_id=%s re-queued for retry %d/%d.",
                                    task_id, retry_count + 1, max_retries,
                                )

                                # 4. ACK the original message to remove it from the
                                #    PEL — the new entry is now the authoritative one.
                                client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)

                            else:
                                # ── 2c. Permanent failure — retries exhausted ──
                                log.error(
                                    "task_id=%s permanently failed after %d/%d attempts. "
                                    "error_message=%s",
                                    task_id, retry_count + 1, max_retries + 1, error_str,
                                )
                                await db.update_task_as_failed(task_id, error_str)

                                # ACK so the dead message is cleared from the PEL
                                # and does not block stream progress.
                                client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)

            except redis.ConnectionError as exc:
                log.error("Lost Redis connection: %s — retrying in 5s.", exc)
                await asyncio.sleep(5)

    finally:
        # Always close the PG pool, even if the loop exits due to an exception.
        await db.close_pool()

    log.info("Shutdown flag set. Worker '%s' exiting cleanly.", CONSUMER_NAME)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    r = connect_redis()
    ensure_consumer_group(r)
    asyncio.run(run(r))
