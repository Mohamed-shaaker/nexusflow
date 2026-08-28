"""
NexusFlow - API Gateway
=======================
Entry point for all inbound HTTP traffic. Responsible for:
  - Health reporting
  - Accepting task submissions, persisting them in PostgreSQL (status='pending'),
    and publishing them to a Redis Stream.
  - Serving task state via GET /tasks/{task_id}.

Run locally:
    uvicorn main:app --reload --port 8000
"""

import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import redis
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import db

log = logging.getLogger("nexusflow.gateway")

# ---------------------------------------------------------------------------
# Application lifespan — manages async resource lifecycle
# ---------------------------------------------------------------------------
# FastAPI's lifespan context manager replaces the deprecated @app.on_event
# hooks.  Code before ``yield`` runs at startup; code after runs at shutdown.


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    await db.init_pool()
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────────
    await db.close_pool()


# ---------------------------------------------------------------------------
# Application instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NexusFlow Gateway",
    description="API Gateway for the NexusFlow event-driven microservices platform.",
    version="0.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------
# host="redis" resolves via Docker / Kubernetes service-name DNS.
# decode_responses=True means all values come back as str, not bytes.
# The client is module-level so the connection pool is shared across requests.
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class TaskRequest(BaseModel):
    """Payload accepted by POST /tasks."""

    name: str = Field(..., min_length=1, max_length=128, description="Human-readable task name.")
    payload: dict = Field(default_factory=dict, description="Arbitrary task data.")
    idempotency_key: Optional[str] = Field(
        None,
        max_length=255,
        description=(
            "Optional deduplication key. If a task with this key already exists, "
            "the original task is returned instead of creating a new one. "
            "The Idempotency-Key HTTP header takes precedence over this field."
        ),
    )


class TaskResponse(BaseModel):
    """Confirmation returned after a task is accepted or deduplicated."""

    task_id: str = Field(..., description="Unique identifier assigned to the task.")
    name: str
    status: str = "accepted"
    accepted_at: str = Field(..., description="ISO-8601 timestamp of acceptance.")
    idempotency_key: Optional[str] = Field(
        None,
        description="Echo of the idempotency key used for this request, if any.",
    )


class TaskStatusResponse(BaseModel):
    """Full task details returned by GET /tasks/{task_id}."""

    task_id: str
    name: str
    status: str
    payload: dict | None = None
    result: dict | None = None
    # Retry telemetry — lets callers see failure details without DB access
    retry_count: int = Field(0, description="Number of times this task has been retried.")
    max_retries: int = Field(3, description="Maximum retry attempts allowed for this task.")
    error_message: str | None = Field(None, description="Last error recorded for this task, if any.")
    created_at: str
    updated_at: str


class HealthResponse(BaseModel):
    """Shape of the /health response."""

    service: str = "gateway"
    status: str = "ok"
    timestamp: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Health check",
    tags=["Observability"],
)
def health_check() -> HealthResponse:
    """
    Returns the current health status of the gateway.

    Kubernetes liveness and readiness probes should point here.
    """
    return HealthResponse(timestamp=_utc_now())


@app.post(
    "/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={status.HTTP_200_OK: {"model": TaskResponse, "description": "Duplicate — existing task returned."}},
    summary="Submit a task",
    tags=["Tasks"],
)
async def create_task(
    task: TaskRequest,
    idempotency_key_header: Optional[str] = Header(
        None,
        alias="Idempotency-Key",
        description="Deduplication key supplied as an HTTP header (takes precedence over the body field).",
    ),
) -> TaskResponse:
    """
    Accepts a task payload, persists it in PostgreSQL with status='pending',
    then publishes it to the Redis Stream 'tasks'.

    **Idempotency**

    An optional deduplication key may be supplied in two ways (header wins):

    - HTTP header: ``Idempotency-Key: <key>``
    - JSON body field: ``"idempotency_key": "<key>"``

    When a key is provided:

    - **First request** — the task is created normally and the key is stored.
      Returns **202 Accepted**.
    - **Duplicate request** (same key) — the original task record is returned
      unchanged.  Returns **200 OK**.  No new task is inserted, no Redis
      message is published.

    When no key is provided, every POST creates a new task as before.
    """
    # ── Resolve the effective idempotency key ─────────────────────────────────
    # Header takes precedence over the body field so that HTTP-level middleware
    # (load balancers, API gateways) can inject or strip the key independently
    # of the request body.
    effective_key: str | None = idempotency_key_header or task.idempotency_key

    # ── Deduplication check ───────────────────────────────────────────────────
    if effective_key is not None:
        existing = await db.fetch_task_by_idempotency_key(effective_key)
        if existing is not None:
            log.info(
                "Idempotent hit  idempotency_key=%s  task_id=%s",
                effective_key, existing["task_id"],
            )
            # Return a Response object directly so we can override the status
            # code to 200 OK without changing the endpoint's default 202.
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=TaskResponse(
                    task_id=existing["task_id"],
                    name=existing["name"],
                    status=existing["status"],
                    accepted_at=existing["accepted_at"],
                    idempotency_key=effective_key,
                ).model_dump(),
            )

    # ── New task — normal insertion path ──────────────────────────────────────
    task_id = str(uuid.uuid4())
    accepted_at = _utc_now()

    # 1. Persist in PostgreSQL first so the task is visible immediately via
    #    GET /tasks/{task_id} even before the worker picks it up.
    await db.create_task(
        task_id=task_id,
        name=task.name,
        payload=task.payload,
        created_at=accepted_at,
        idempotency_key=effective_key,
    )

    # 2. Publish the task to the Redis Stream.
    # xadd() appends a new entry to the stream and returns the auto-generated
    # stream message ID (e.g. "1693000000000-0") — we don't need it here.
    # payload is serialised to a JSON string because Redis Stream field values
    # must be scalars (str/bytes/int), not nested dicts.
    redis_client.xadd(
        "tasks",
        {
            "task_id":     task_id,
            "name":        task.name,
            "payload":     json.dumps(task.payload),
            "accepted_at": accepted_at,
        },
    )

    return TaskResponse(
        task_id=task_id,
        name=task.name,
        accepted_at=accepted_at,
        idempotency_key=effective_key,
    )


@app.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    status_code=status.HTTP_200_OK,
    summary="Get task status",
    tags=["Tasks"],
)
async def get_task(task_id: str) -> TaskStatusResponse:
    """
    Query PostgreSQL for the current state of a previously submitted task.

    Returns the full task record including current status, payload, and result
    (if the worker has already completed or failed the task).

    Raises HTTP 404 if no task with that ID exists.
    """
    row = await db.fetch_task(task_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found.",
        )
    return TaskStatusResponse(**row)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()
