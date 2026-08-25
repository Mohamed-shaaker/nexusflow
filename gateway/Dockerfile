# =============================================================================
# NexusFlow — gateway/Dockerfile
# =============================================================================
# Multi-stage build:
#   builder  → installs pip dependencies into an isolated prefix; no artefacts
#              from the build environment (gcc, pip cache) leak into the image.
#   runtime  → lean python:3.12-slim image; only the installed packages and
#              the application source are present.
#
# Why COPY . . instead of per-file copies:
#   Explicit per-file COPY lines must be updated every time a new module is
#   added. COPY . . picks up main.py, db.py, and any future helpers in one
#   instruction. Unwanted files (cache, .env, .git) are excluded by
#   .dockerignore in the build context root, so the image stays clean.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1 — builder
# Install Python dependencies into /install/packages so they can be copied
# cleanly into the runtime stage without dragging pip or wheel caches along.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS builder

# Prevent .pyc files being written during the install step.
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /install

# Copy the dependency manifest first.
# Docker caches this layer independently of application code: re-builds after
# editing main.py or db.py reuse the cached pip layer and finish in seconds.
COPY requirements.txt .

# Upgrade pip, then install everything into an isolated prefix.
# --no-cache-dir  → omit pip's HTTP cache (saves ~50–100 MB per build)
# --prefix        → relocatable install root, easy to COPY in the next stage
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --no-cache-dir --prefix=/install/packages -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2 — runtime
# Minimal final image: no pip, no build tools, no wheel cache.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE  → no .pyc files cluttering the container filesystem
# PYTHONUNBUFFERED         → stdout / stderr reach Docker logs immediately;
#                            essential for `docker compose logs` and Kubernetes
#                            log aggregators (Loki, CloudWatch, Datadog).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Run as a non-root user.
# Required by Kubernetes Pod Security Standards (restricted profile) and a
# general container security best practice.
RUN addgroup --system appgroup \
 && adduser --system --ingroup appgroup appuser

WORKDIR /app

# ── Dependencies ──────────────────────────────────────────────────────────────
# Pull in the pre-built packages from the builder stage.
# Copying into /usr/local merges with Python's default sys.path — no PYTHONPATH
# manipulation required.
COPY --from=builder /install/packages /usr/local

# ── Application source ────────────────────────────────────────────────────────
# COPY . . sends the entire build context (gateway/) into /app.
# .dockerignore at the repo root filters out __pycache__, .env, .git, etc.
# This means main.py, db.py, and any future modules are all included
# automatically without having to update this Dockerfile.
COPY . .

# Drop privileges before starting the process.
USER appuser

# Document the listening port.
# Map it with -p 8000:8000 (docker run) or containerPort (Kubernetes).
EXPOSE 8000

# ── Startup command ───────────────────────────────────────────────────────────
# Exec form ensures SIGTERM is delivered directly to uvicorn (not via a shell),
# enabling graceful shutdown of in-flight requests.
#
# --host 0.0.0.0   bind to all interfaces (required inside a container)
# --port 8000      must match EXPOSE above
# --workers 1      scale horizontally via Kubernetes replicas instead
# --no-access-log  suppress per-request noise; use an ingress/sidecar for logs
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
