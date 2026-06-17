-- FluxCDC Phase 1 schema

-- Event store: source of truth for all captured CDC events
CREATE TABLE IF NOT EXISTS cdc_events (
    id             BIGSERIAL    PRIMARY KEY,
    event_id       VARCHAR(36)  NOT NULL UNIQUE,
    connector      VARCHAR(255) NOT NULL,
    database_name  VARCHAR(255) NOT NULL,
    table_name     VARCHAR(255) NOT NULL,
    operation      VARCHAR(20)  NOT NULL,  -- INSERT, UPDATE, DELETE, UPSERT
    before_data    JSONB,
    after_data     JSONB,
    schema_version INTEGER      NOT NULL DEFAULT 1,
    event_timestamp TIMESTAMPTZ NOT NULL,
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Index for the relay service: quickly find unpublished events in insert order
CREATE INDEX IF NOT EXISTS idx_cdc_events_unpublished
    ON cdc_events (id) WHERE published = FALSE;

-- Index for filtering by connector
CREATE INDEX IF NOT EXISTS idx_cdc_events_connector
    ON cdc_events (connector);

-- Connector offsets: tracks the last watermark checkpoint per connector
CREATE TABLE IF NOT EXISTS connector_offsets (
    id           SERIAL       PRIMARY KEY,
    connector_id VARCHAR(255) NOT NULL UNIQUE,
    last_offset  TIMESTAMPTZ  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
