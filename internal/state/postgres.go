package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"fluxcdc/internal/event"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS cdc_events (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        VARCHAR(36)  NOT NULL UNIQUE,
    connector       VARCHAR(255) NOT NULL,
    database_name   VARCHAR(255) NOT NULL,
    schema_name     VARCHAR(255) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    operation       VARCHAR(20)  NOT NULL,
    before_data     JSONB,
    after_data      JSONB,
    schema_version  INTEGER      NOT NULL DEFAULT 1,
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingest_timestamp TIMESTAMPTZ NOT NULL,
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
    last_offset  VARCHAR(255) NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
`

type Store struct {
	db *sql.DB
}

func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to execute migrations: %w", err)
	}
	return nil
}

func (s *Store) SaveEvent(ctx context.Context, e *event.Event) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cdc_events (
			event_id, connector, database_name, schema_name, table_name,
			operation, before_data, after_data, schema_version,
			source_timestamp, ingest_timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (event_id) DO NOTHING
	`,
		e.EventID, e.Source.Connector, e.Source.Database, e.Source.Schema, e.Source.Table,
		string(e.Operation), nullableJSON(e.Before), nullableJSON(e.After), e.SchemaVersion,
		e.Source.Timestamp, e.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to save event to state store: %w", err)
	}
	return nil
}

func (s *Store) GetOffset(ctx context.Context, connectorID string) (string, error) {
	var lastOffset string
	err := s.db.QueryRowContext(ctx, `
		SELECT last_offset FROM connector_offsets WHERE connector_id = $1
	`, connectorID).Scan(&lastOffset)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to fetch offset: %w", err)
	}
	return lastOffset, nil
}

func (s *Store) SaveOffset(ctx context.Context, connectorID string, lastOffset string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO connector_offsets (connector_id, last_offset, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (connector_id)
		DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = NOW()
	`, connectorID, lastOffset)
	if err != nil {
		return fmt.Errorf("failed to save offset: %w", err)
	}
	return nil
}

func (s *Store) GetUnpublishedEvents(ctx context.Context, limit int) ([]*event.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, connector, database_name, schema_name, table_name,
		       operation, before_data, after_data, schema_version,
		       source_timestamp, ingest_timestamp
		FROM cdc_events
		WHERE published = FALSE
		ORDER BY id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch unpublished events: %w", err)
	}
	defer rows.Close()

	var events []*event.Event
	for rows.Next() {
		var e event.Event
		var beforeBytes, afterBytes []byte
		if err := rows.Scan(
			&e.EventID, &e.Source.Connector, &e.Source.Database, &e.Source.Schema, &e.Source.Table,
			&e.Operation, &beforeBytes, &afterBytes, &e.SchemaVersion,
			&e.Source.Timestamp, &e.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}
		e.Before = beforeBytes
		e.After = afterBytes
		events = append(events, &e)
	}
	return events, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs)+1)
	args[0] = time.Now().UTC()

	for i, id := range eventIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		UPDATE cdc_events
		SET published = TRUE, published_at = $1
		WHERE event_id IN (%s)
	`, strings.Join(placeholders, ","))

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark events published: %w", err)
	}
	return nil
}

func nullableJSON(data []byte) interface{} {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	return data
}
