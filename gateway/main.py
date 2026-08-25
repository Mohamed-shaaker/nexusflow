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
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

import db

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


class TaskResponse(BaseModel):
    """Confirmation returned after a task is accepted."""

    task_id: str = Field(..., description="Unique identifier assigned to the task.")
    name: str
    status: str = "accepted"
    accepted_at: str = Field(..., description="ISO-8601 timestamp of acceptance.")


class TaskStatusResponse(BaseModel):
    """Full task details returned by GET /tasks/{task_id}."""

    task_id: str
    name: str
    status: str
    payload: dict | None = None
    result: dict | None = None
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
    summary="Submit a task",
    tags=["Tasks"],
)
async def create_task(task: TaskRequest) -> TaskResponse:
    """
    Accepts a task payload, persists it in PostgreSQL with status='pending',
    then publishes it to the Redis Stream 'tasks'.

    The worker service consumes from this stream via a consumer group.
    HTTP 202 Accepted signals that the task has been queued, not yet executed.
    """
    task_id = str(uuid.uuid4())
    accepted_at = _utc_now()

    # 1. Persist in PostgreSQL first so the task is visible immediately via
    #    GET /tasks/{task_id} even before the worker picks it up.
    await db.create_task(
        task_id=task_id,
        name=task.name,
        payload=task.payload,
        created_at=accepted_at,
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
