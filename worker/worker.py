"""
NexusFlow - Background Worker
==============================
Consumes tasks from a Redis Stream and processes them.

The worker uses the Redis Streams "consumer group" model:
  - Multiple worker replicas can run simultaneously without duplicating work.
  - Unacknowledged messages are automatically re-delivered on restart (at-least-once delivery).
  - XACK removes a message from the Pending Entries List (PEL) once processing succeeds.

Environment variables (with defaults):
  REDIS_HOST        Host of the Redis instance        (default: localhost)
  REDIS_PORT        Port of the Redis instance        (default: 6379)
  STREAM_NAME       Name of the Redis stream          (default: tasks)
  CONSUMER_GROUP    Consumer group name               (default: workers)
  CONSUMER_NAME     Unique name for this worker       (default: worker-1)
  BLOCK_MS          XREADGROUP block timeout in ms    (default: 2000)
"""

import json
import logging
import os
import signal
import sys
import time

import redis

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
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME    = os.getenv("STREAM_NAME", "tasks")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")
CONSUMER_NAME  = os.getenv("CONSUMER_NAME", "worker-1")
BLOCK_MS       = int(os.getenv("BLOCK_MS", "2000"))   # ms to block on XREADGROUP

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

def process_task(task_id: str, fields: dict) -> None:
    """
    Core business logic placeholder.

    Replace the body of this function with real work:
    image resizing, sending an email, calling a downstream API, etc.
    The function should raise an exception on failure so the message
    is NOT acknowledged and will be re-delivered.
    """
    log.info("Processing task_id=%s  name=%s", task_id, fields.get("name", "unknown"))

    # Simulate work
    time.sleep(0.1)

    log.info("Task completed  task_id=%s", task_id)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(client: redis.Redis) -> None:
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
                    try:
                        process_task(msg_id, fields)
                        # ACK removes the message from the Pending Entries List.
                        client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                    except Exception as exc:  # noqa: BLE001
                        # Log the error but do NOT ACK — the message stays in
                        # the PEL and will be reclaimed after a timeout.
                        log.error(
                            "Failed to process task_id=%s: %s", msg_id, exc, exc_info=True
                        )

        except redis.ConnectionError as exc:
            log.error("Lost Redis connection: %s — retrying in 5s.", exc)
            time.sleep(5)

    log.info("Shutdown flag set. Worker '%s' exiting cleanly.", CONSUMER_NAME)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    r = connect_redis()
    ensure_consumer_group(r)
    run(r)
