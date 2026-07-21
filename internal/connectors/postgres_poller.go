package connectors

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"

	"fluxcdc/internal/config"
	"fluxcdc/internal/event"
	"fluxcdc/internal/state"
)

type PostgresPoller struct {
	cfg        config.Config
	store      *state.Store
	db         *sql.DB
	connectorID string
}

func NewPostgresPoller(cfg config.Config, store *state.Store) (*PostgresPoller, error) {
	db, err := sql.Open("postgres", cfg.Database.SourceConn)
	if err != nil {
		return nil, fmt.Errorf("failed to open source database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping source database: %w", err)
	}

	return &PostgresPoller{
		cfg:         cfg,
		store:       store,
		db:          db,
		connectorID: cfg.Pipeline.Source.Connector,
	}, nil
}

func (p *PostgresPoller) Start(ctx context.Context) (<-chan *event.Event, <-chan error, error) {
	out := make(chan *event.Event, 100)
	errs := make(chan error, 10)

	go p.pollLoop(ctx, out, errs)

	return out, errs, nil
}

func (p *PostgresPoller) Close() error {
	return p.db.Close()
}

func (p *PostgresPoller) pollLoop(ctx context.Context, out chan<- *event.Event, errs chan<- error) {
	defer close(out)
	defer close(errs)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, table := range p.cfg.Pipeline.Source.Tables {
				if err := p.pollTable(ctx, table, out); err != nil {
					select {
					case errs <- err:
					default:
						log.Printf("Error queue full. Dropped error: %v", err)
					}
				}
			}
		}
	}
}

func (p *PostgresPoller) pollTable(ctx context.Context, table string, out chan<- *event.Event) error {
	offsetKey := fmt.Sprintf("%s:%s", p.connectorID, table)
	lastOffsetStr, err := p.store.GetOffset(ctx, offsetKey)
	if err != nil {
		return fmt.Errorf("failed to fetch last offset for %s: %w", table, err)
	}

	lastOffset := time.Time{}
	if lastOffsetStr != "" {
		parsedTime, err := time.Parse(time.RFC3339Nano, lastOffsetStr)
		if err == nil {
			lastOffset = parsedTime
		}
	}

	// Read rows with watermark > lastOffset
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE updated_at > $1 ORDER BY updated_at ASC LIMIT 100",
		table,
	)
	rows, err := p.db.QueryContext(ctx, query, lastOffset)
	if err != nil {
		return fmt.Errorf("query error on table %s: %w", table, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to read columns for %s: %w", table, err)
	}

	var maxUpdatedAt time.Time
	hasRows := false

	for rows.Next() {
		hasRows = true
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("row scan error on table %s: %w", table, err)
		}

		rowMap := make(map[string]interface{})
		var rowUpdatedAt time.Time

		for i, col := range columns {
			val := values[i]
			rowMap[col] = val

			if col == "updated_at" {
				if t, ok := val.(time.Time); ok {
					rowUpdatedAt = t
				}
			}
		}

		if rowUpdatedAt.After(maxUpdatedAt) {
			maxUpdatedAt = rowUpdatedAt
		}

		afterJSON, err := json.Marshal(rowMap)
		if err != nil {
			return fmt.Errorf("failed to marshal row map: %w", err)
		}

		evt := &event.Event{
			EventID: generateUUID(),
			Source: event.SourceMetadata{
				Connector: p.connectorID,
				Database:  "source_db", // Generic database name or pull from config
				Schema:    "public",    // Default postgres schema
				Table:     table,
				Timestamp: rowUpdatedAt,
			},
			Operation:     event.OperationUpsert,
			After:         afterJSON,
			SchemaVersion: 1,
			Timestamp:     time.Now().UTC(),
		}

		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if hasRows && !maxUpdatedAt.IsZero() {
		err = p.store.SaveOffset(ctx, offsetKey, maxUpdatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("failed to save offset: %w", err)
		}
	}

	return nil
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
