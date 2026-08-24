"""
NexusFlow - API Gateway
=======================
Entry point for all inbound HTTP traffic. Responsible for:
  - Health reporting
  - Accepting task submissions and publishing them to a Redis Stream.

Run locally:
    uvicorn main:app --reload --port 8000
"""

import json
import uuid
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, status
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Application instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NexusFlow Gateway",
    description="API Gateway for the NexusFlow event-driven microservices platform.",
    version="0.1.0",
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
def create_task(task: TaskRequest) -> TaskResponse:
    """
    Accepts a task payload and publishes it to the Redis Stream 'tasks'.

    The worker service consumes from this stream via a consumer group.
    HTTP 202 Accepted signals that the task has been queued, not yet executed.
    """
    task_id = str(uuid.uuid4())
    accepted_at = _utc_now()

    # Publish the task to the Redis Stream.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()
