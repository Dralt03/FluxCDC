package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq" // Postgres driver

	"github.com/fluxcdc/fluxcdc/internal/event"
)

// initSQL creates all tables and indices needed by FluxCDC.
// Using IF NOT EXISTS makes this safe to run on every startup.
const initSQL = `
CREATE TABLE IF NOT EXISTS cdc_events (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        VARCHAR(36)  NOT NULL UNIQUE,
    connector       VARCHAR(255) NOT NULL,
    database_name   VARCHAR(255) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    operation       VARCHAR(20)  NOT NULL,
    before_data     JSONB,
    after_data      JSONB,
    schema_version  INTEGER      NOT NULL DEFAULT 1,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    published       BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cdc_events_unpublished
    ON cdc_events (id) WHERE published = FALSE;

CREATE INDEX IF NOT EXISTS idx_cdc_events_connector
    ON cdc_events (connector);

CREATE TABLE IF NOT EXISTS connector_offsets (
    id           SERIAL       PRIMARY KEY,
    connector_id VARCHAR(255) NOT NULL UNIQUE,
    last_offset  TIMESTAMPTZ  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Phase 2: Dead Letter Queue
CREATE TABLE IF NOT EXISTS dlq_events (
    id            BIGSERIAL    PRIMARY KEY,
    event_id      VARCHAR(36)  NOT NULL,
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

-- Phase 2: Schema Registry
CREATE TABLE IF NOT EXISTS schema_versions (
    id           BIGSERIAL    PRIMARY KEY,
    connector_id VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255) NOT NULL,
    version      INTEGER      NOT NULL DEFAULT 1,
    columns      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (connector_id, table_name, version)
);

CREATE INDEX IF NOT EXISTS idx_schema_versions_lookup
    ON schema_versions (connector_id, table_name, version DESC);

-- Phase 2: Replay Jobs
CREATE TABLE IF NOT EXISTS replay_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_id       VARCHAR(36)  NOT NULL UNIQUE,
    connector_id VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255),
    from_time    TIMESTAMPTZ  NOT NULL,
    to_time      TIMESTAMPTZ  NOT NULL,
    dest_topic   VARCHAR(255) NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
    events_total INTEGER      NOT NULL DEFAULT 0,
    events_sent  INTEGER      NOT NULL DEFAULT 0,
    error_msg    TEXT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
`

// Store is a Postgres-backed event store.
type Store struct {
	db *sql.DB
}

// New opens a Postgres connection and returns a Store.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping verifies the database connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Migrate creates all required tables and indices if they don't already exist.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, initSQL)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

// SaveEvent persists a CDC event. Duplicate event_ids are silently ignored.
func (s *Store) SaveEvent(ctx context.Context, e *event.Event) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cdc_events
		    (event_id, connector, database_name, table_name, operation,
		     before_data, after_data, schema_version, event_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (event_id) DO NOTHING
	`,
		e.EventID, e.Connector, e.Database, e.Table, string(e.Operation),
		nullableJSON(e.Before), nullableJSON(e.After),
		e.SchemaVersion, e.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("store: save event %s: %w", e.EventID, err)
	}
	return nil
}

// GetUnpublishedEvents fetches up to limit events that have not yet been
// sent to Kafka, in insertion order.
func (s *Store) GetUnpublishedEvents(ctx context.Context, limit int) ([]*event.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, connector, database_name, table_name, operation,
		       before_data, after_data, schema_version, event_timestamp
		FROM   cdc_events
		WHERE  published = FALSE
		ORDER  BY id ASC
		LIMIT  $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: get unpublished: %w", err)
	}
	defer rows.Close()

	var events []*event.Event
	for rows.Next() {
		var e event.Event
		var beforeData, afterData []byte
		if err := rows.Scan(
			&e.EventID, &e.Connector, &e.Database, &e.Table, &e.Operation,
			&beforeData, &afterData, &e.SchemaVersion, &e.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("store: scan event: %w", err)
		}
		e.Before = beforeData
		e.After = afterData
		events = append(events, &e)
	}
	return events, rows.Err()
}

// MarkPublished marks a batch of events as successfully published to Kafka.
func (s *Store) MarkPublished(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	// Build parameterized IN clause without external dependencies.
	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs)+1)
	args[0] = time.Now().UTC()
	for i, id := range eventIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}
	query := fmt.Sprintf(
		"UPDATE cdc_events SET published = TRUE, published_at = $1 WHERE event_id IN (%s)",
		strings.Join(placeholders, ","),
	)
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// GetOffset retrieves the last saved watermark for a connector.
// Returns time.Time{} (zero) if no offset has been saved yet.
func (s *Store) GetOffset(ctx context.Context, connectorID string) (time.Time, error) {
	var offset time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT last_offset FROM connector_offsets WHERE connector_id = $1`,
		connectorID,
	).Scan(&offset)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("store: get offset %s: %w", connectorID, err)
	}
	return offset, nil
}

// SaveOffset upserts the watermark checkpoint for a connector.
func (s *Store) SaveOffset(ctx context.Context, connectorID string, offset time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO connector_offsets (connector_id, last_offset)
		VALUES ($1, $2)
		ON CONFLICT (connector_id)
		DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = NOW()
	`, connectorID, offset)
	if err != nil {
		return fmt.Errorf("store: save offset %s: %w", connectorID, err)
	}
	return nil
}

// ── DLQ ──────────────────────────────────────────────────────────────────────

// SaveDLQEvent records an event that failed to publish to Kafka.
func (s *Store) SaveDLQEvent(ctx context.Context, eventID, connector, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dlq_events (event_id, connector, error_message)
		VALUES ($1, $2, $3)
	`, eventID, connector, errMsg)
	if err != nil {
		return fmt.Errorf("store: save dlq event %s: %w", eventID, err)
	}
	return nil
}

// ── Schema Registry ───────────────────────────────────────────────────────────

// SchemaColumn describes a single column in a table's schema.
type SchemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SaveSchema saves a new schema version for a connector+table.
// The version number is auto-incremented (max existing + 1).
func (s *Store) SaveSchema(ctx context.Context, connectorID, tableName string, cols []SchemaColumn) (int, error) {
	colsJSON, err := json.Marshal(cols)
	if err != nil {
		return 0, fmt.Errorf("store: marshal schema: %w", err)
	}

	var version int
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO schema_versions (connector_id, table_name, version, columns)
		VALUES (
		    $1, $2,
		    COALESCE((SELECT MAX(version) FROM schema_versions WHERE connector_id=$1 AND table_name=$2), 0) + 1,
		    $3
		)
		RETURNING version
	`, connectorID, tableName, colsJSON).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("store: save schema: %w", err)
	}
	return version, nil
}

// GetLatestSchema returns the most recent schema version for a connector+table.
// Returns version=0 and nil columns if no schema has been recorded yet.
func (s *Store) GetLatestSchema(ctx context.Context, connectorID, tableName string) (int, []SchemaColumn, error) {
	var version int
	var colsJSON []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT version, columns
		FROM   schema_versions
		WHERE  connector_id = $1 AND table_name = $2
		ORDER  BY version DESC
		LIMIT  1
	`, connectorID, tableName).Scan(&version, &colsJSON)
	if err == sql.ErrNoRows {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("store: get schema: %w", err)
	}
	var cols []SchemaColumn
	if err := json.Unmarshal(colsJSON, &cols); err != nil {
		return 0, nil, fmt.Errorf("store: unmarshal schema: %w", err)
	}
	return version, cols, nil
}

// ── Replay Jobs ───────────────────────────────────────────────────────────────

// ReplayJob represents a replay request.
type ReplayJob struct {
	JobID       string
	ConnectorID string
	TableName   string // empty = all tables
	FromTime    time.Time
	ToTime      time.Time
	DestTopic   string
	Status      string // pending, running, done, failed
	EventsTotal int
	EventsSent  int
}

// CreateReplayJob inserts a new replay job with status=pending.
func (s *Store) CreateReplayJob(ctx context.Context, j *ReplayJob) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO replay_jobs
		    (job_id, connector_id, table_name, from_time, to_time, dest_topic)
		VALUES ($1, $2, NULLIF($3,''), $4, $5, $6)
	`, j.JobID, j.ConnectorID, j.TableName, j.FromTime, j.ToTime, j.DestTopic)
	if err != nil {
		return fmt.Errorf("store: create replay job: %w", err)
	}
	return nil
}

// UpdateReplayJob updates status and progress counters for a running job.
func (s *Store) UpdateReplayJob(ctx context.Context, jobID, status string, total, sent int, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE replay_jobs SET
		    status       = $2,
		    events_total = $3,
		    events_sent  = $4,
		    error_msg    = NULLIF($5, ''),
		    started_at   = COALESCE(started_at, CASE WHEN $2='running' THEN NOW() END),
		    finished_at  = CASE WHEN $2 IN ('done','failed') THEN NOW() END
		WHERE job_id = $1
	`, jobID, status, total, sent, errMsg)
	return err
}

// GetEventsForReplay fetches events for a replay job, ordered by event_timestamp.
func (s *Store) GetEventsForReplay(ctx context.Context, connectorID, tableName string, from, to time.Time) ([]*event.Event, error) {
	query := `
		SELECT event_id, connector, database_name, table_name, operation,
		       before_data, after_data, schema_version, event_timestamp
		FROM   cdc_events
		WHERE  connector = $1
		  AND  event_timestamp >= $2
		  AND  event_timestamp <= $3
	`
	args := []interface{}{connectorID, from, to}
	if tableName != "" {
		query += " AND table_name = $4"
		args = append(args, tableName)
	}
	query += " ORDER BY event_timestamp ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: get events for replay: %w", err)
	}
	defer rows.Close()

	var events []*event.Event
	for rows.Next() {
		var e event.Event
		var beforeData, afterData []byte
		if err := rows.Scan(
			&e.EventID, &e.Connector, &e.Database, &e.Table, &e.Operation,
			&beforeData, &afterData, &e.SchemaVersion, &e.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("store: scan replay event: %w", err)
		}
		e.Before = beforeData
		e.After = afterData
		events = append(events, &e)
	}
	return events, rows.Err()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Close closes the underlying database connection pool.
func (s *Store) Close() error {
	return s.db.Close()
}

// nullableJSON returns nil for an empty/null JSON blob so the database
// stores a proper SQL NULL rather than the string "null".
func nullableJSON(data []byte) interface{} {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	return data
}
