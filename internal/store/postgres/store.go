package postgres

import (
	"context"
	"database/sql"
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
