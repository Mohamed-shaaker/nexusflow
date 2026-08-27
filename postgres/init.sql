-- =============================================================================
-- NexusFlow — postgres/init.sql
-- =============================================================================
-- Executed once by the PostgreSQL Docker entrypoint on first container start.
-- Creates the schema required for task state persistence.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- tasks table
-- ---------------------------------------------------------------------------
-- task_id       — client-generated UUID, used as the primary key so the gateway
--                 can reference it before the worker ever touches it.
-- status        — state-machine column:
--                   pending → processing → completed
--                   processing → pending  (transient retry, retry_count incremented)
--                   processing → failed   (retry_count >= max_retries)
-- payload       — arbitrary JSON input submitted by the caller (JSONB for indexing)
-- result        — arbitrary JSON output written by the worker (nullable)
-- retry_count   — number of processing attempts made so far (default 0)
-- max_retries   — maximum number of retry attempts allowed before marking failed
-- error_message — last exception message captured on failure (nullable)
-- created_at    — set once at INSERT time (UTC)
-- updated_at    — updated on every state transition
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    task_id       VARCHAR(64)  PRIMARY KEY,
    name          VARCHAR(128) NOT NULL,
    status        VARCHAR(32)  NOT NULL DEFAULT 'pending',
    payload       JSONB,
    result        JSONB,
    retry_count   INTEGER      NOT NULL DEFAULT 0,
    max_retries   INTEGER      NOT NULL DEFAULT 3,
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Index on status so queries like "give me all pending tasks" stay fast
-- even when the table grows to millions of rows.
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
