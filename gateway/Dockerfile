# =============================================================================
# NexusFlow — Gateway Microservice
# =============================================================================
# Multi-stage build keeps the final image lean:
#   builder  → installs dependencies into an isolated prefix
#   runtime  → copies only the installed packages, no build tooling
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: builder
# Install Python dependencies in an isolated directory so nothing from the
# build environment bleeds into the final image.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS builder

# Keeps Python from writing .pyc bytecode files to disk (not useful in a
# container where the filesystem is ephemeral).
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /install

# Copy only the dependency manifest first.
# Docker layer-caches this step independently of the app code — re-running
# the build after editing main.py will reuse this cached layer.
COPY requirements.txt .

# Install packages into /install/packages so they are easy to copy later.
# --no-cache-dir   → skip pip's download cache (reduces image size)
# --upgrade        → ensure pip itself is current before installing
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --no-cache-dir --prefix=/install/packages -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: runtime
# Minimal final image — no pip, no build tools, no cache.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE  → no .pyc files cluttering the container FS
# PYTHONUNBUFFERED         → stdout/stderr are sent straight to the terminal
#                            (essential for Docker / Kubernetes log streaming)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Run as a non-root user — a container security best practice.
# Kubernetes Pod Security Standards (restricted) require this.
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Pull in the pre-built packages from the builder stage.
COPY --from=builder /install/packages /usr/local

# Copy application source last so editing main.py only invalidates this layer.
COPY main.py .

# Switch to the non-root user before the process starts.
USER appuser

# Document which port the service listens on.
# (docker run -p 8000:8000 or Kubernetes containerPort declaration maps this.)
EXPOSE 8000

# -------------------------------------------------------------------------
# Default startup command
# --host 0.0.0.0   → bind to all interfaces (required inside a container)
# --port 8000      → must match EXPOSE above
# --workers 1      → single worker; scale horizontally via Kubernetes replicas
# --no-access-log  → remove per-request noise; use a sidecar/ingress for logs
# -------------------------------------------------------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
