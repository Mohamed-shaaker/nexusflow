# =============================================================================
# NexusFlow — worker/Dockerfile
# =============================================================================
# Multi-stage build:
#   builder  → installs pip dependencies into an isolated prefix
#   runtime  → lean python:3.12-slim image with only packages + source
#
# Why COPY . . instead of per-file copies:
#   Explicit per-file COPY lines must be updated every time a new module is
#   added. COPY . . picks up worker.py, db.py, and any future helpers in one
#   instruction. Unwanted files (cache, .env, .git) are excluded by
#   .dockerignore in the build context root, so the image stays clean.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1 — builder
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS builder

# Prevent .pyc files being written during the install step.
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /install

# Copy the manifest first so Docker caches the pip layer independently of
# application source — changing worker.py or db.py won't trigger a re-install.
COPY requirements.txt .

# Install into a relocatable prefix.
# --no-cache-dir  → omit pip's HTTP cache (saves image space)
# --prefix        → easy to COPY cleanly into the runtime stage
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --no-cache-dir --prefix=/install/packages -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2 — runtime
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE  → no .pyc files
# PYTHONUNBUFFERED         → stdout / stderr flush immediately; critical for
#                            kubectl logs / CloudWatch / Datadog log streaming
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Default runtime configuration — all values are overridden by docker-compose
# environment: blocks or Kubernetes ConfigMaps / Secrets.
ENV REDIS_HOST=localhost \
    REDIS_PORT=6379 \
    STREAM_NAME=tasks \
    CONSUMER_GROUP=workers \
    CONSUMER_NAME=worker-1 \
    BLOCK_MS=2000

# Non-root user — required by Kubernetes Pod Security Standards (restricted).
RUN addgroup --system appgroup \
 && adduser --system --ingroup appgroup appuser

WORKDIR /app

# ── Dependencies ──────────────────────────────────────────────────────────────
COPY --from=builder /install/packages /usr/local

# ── Application source ────────────────────────────────────────────────────────
# COPY . . sends the entire build context (worker/) into /app.
# .dockerignore at the repo root filters out __pycache__, .env, .git, etc.
# worker.py, db.py, and any future modules are included automatically.
COPY . .

# Drop privileges before the process starts.
USER appuser

# ── Startup command ───────────────────────────────────────────────────────────
# Exec form delivers SIGTERM directly to the Python process — this is what
# triggers the graceful shutdown path in worker.py (_handle_signal / _shutdown).
# -u  → unbuffered output (belt-and-suspenders alongside PYTHONUNBUFFERED)
CMD ["python", "-u", "worker.py"]
