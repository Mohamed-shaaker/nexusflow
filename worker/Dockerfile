# =============================================================================
# NexusFlow — Worker Microservice
# =============================================================================
# Multi-stage build:
#   builder  → installs Python dependencies into an isolated prefix
#   runtime  → lean final image with no pip, no build cache
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: builder
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS builder

# Suppress .pyc file generation during the install step.
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /install

# Copy only the manifest — Docker caches this layer until requirements change.
COPY requirements.txt .

# Install into a relocatable prefix so we can COPY it cleanly in stage 2.
# --no-cache-dir keeps this layer as small as possible.
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --no-cache-dir --prefix=/install/packages -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE  → no .pyc files
# PYTHONUNBUFFERED         → stdout/stderr flush immediately (critical for
#                            kubectl logs / CloudWatch log streaming)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- Default runtime configuration (override with env vars or ConfigMap) ----
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

# Pull in only the installed packages — no pip, no wheel cache.
COPY --from=builder /install/packages /usr/local

# Copy application source last (cheapest cache layer to invalidate).
COPY worker.py .

# Drop privileges before the process starts.
USER appuser

# The worker is a long-running process, not an HTTP server — no EXPOSE needed.
# CMD uses exec form so SIGTERM is delivered directly to the Python process,
# enabling the graceful shutdown logic in worker.py.
CMD ["python", "-u", "worker.py"]
