-- Phase 2 additions

-- Dead Letter Queue: events that failed to publish to Kafka
CREATE TABLE IF NOT EXISTS dlq_events (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      VARCHAR(36)  NOT NULL,          -- references cdc_events.event_id
    connector     VARCHAR(255) NOT NULL,
    error_message TEXT         NOT NULL,
    retry_count   INTEGER      NOT NULL DEFAULT 0,
    last_attempt  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved      BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_events_unresolved
    ON dlq_events (id) WHERE resolved = FALSE;

-- Schema Registry: tracks the column layout of each captured table over time
CREATE TABLE IF NOT EXISTS schema_versions (
    id           BIGSERIAL    PRIMARY KEY,
    connector_id VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255) NOT NULL,
    version      INTEGER      NOT NULL DEFAULT 1,
    columns      JSONB        NOT NULL,   -- ordered list of {name, type}
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (connector_id, table_name, version)
);

CREATE INDEX IF NOT EXISTS idx_schema_versions_lookup
    ON schema_versions (connector_id, table_name, version DESC);

-- Replay Jobs: tracks replay requests and their progress
CREATE TABLE IF NOT EXISTS replay_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_id       VARCHAR(36)  NOT NULL UNIQUE,
    connector_id VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255),            -- NULL = all tables for this connector
    from_time    TIMESTAMPTZ  NOT NULL,
    to_time      TIMESTAMPTZ  NOT NULL,
    dest_topic   VARCHAR(255) NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending, running, done, failed
    events_total INTEGER      NOT NULL DEFAULT 0,
    events_sent  INTEGER      NOT NULL DEFAULT 0,
    error_msg    TEXT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
