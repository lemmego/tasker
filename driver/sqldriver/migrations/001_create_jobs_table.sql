CREATE TABLE IF NOT EXISTS tasker_jobs (
    id          BIGSERIAL PRIMARY KEY,
    uuid        TEXT NOT NULL UNIQUE,
    queue       TEXT NOT NULL DEFAULT 'default',
    kind        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    state       TEXT NOT NULL DEFAULT 'available',
    priority    INTEGER NOT NULL DEFAULT 0,
    attempt     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    attempted_by TEXT[] NOT NULL DEFAULT '{}',
    attempted_at TIMESTAMPTZ,
    errors      JSONB NOT NULL DEFAULT '[]',
    output      BYTEA,
    tags        TEXT[] NOT NULL DEFAULT '{}',
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at  TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ,
    node_id     TEXT,
    batch_id    TEXT,
    timeout     BIGINT NOT NULL DEFAULT 0,
    metadata    JSONB NOT NULL DEFAULT '{}',
    unique_key  TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_queue_state
    ON tasker_jobs (queue, state, scheduled_at, priority DESC, id);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_state
    ON tasker_jobs (state);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_kind
    ON tasker_jobs (kind);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_batch_id
    ON tasker_jobs (batch_id);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_node_id
    ON tasker_jobs (node_id);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_scheduled_at
    ON tasker_jobs (scheduled_at);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_tags
    ON tasker_jobs USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_tasker_jobs_unique_key
    ON tasker_jobs (unique_key) WHERE unique_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS tasker_nodes (
    node_id     TEXT PRIMARY KEY,
    host        TEXT NOT NULL,
    port        INTEGER NOT NULL DEFAULT 0,
    queues      TEXT[] NOT NULL DEFAULT '{}',
    workers     INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'active',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version     TEXT NOT NULL DEFAULT '1.0.0'
);

CREATE TABLE IF NOT EXISTS tasker_failed_jobs (
    id          BIGSERIAL PRIMARY KEY,
    uuid        TEXT NOT NULL,
    queue       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    attempt     INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    errors      JSONB NOT NULL,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tags        TEXT[] NOT NULL DEFAULT '{}'
);
