package poll

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // Postgres driver

	"github.com/fluxcdc/fluxcdc/internal/config"
	"github.com/fluxcdc/fluxcdc/internal/event"
)

// Poller implements the generic polling connector.
//
// It connects to a source Postgres database and periodically queries
// rows where the watermark column is greater than the last checkpoint,
// emitting each row as a CDC event.
type Poller struct {
	cfg        config.ConnectorConfig
	db         *sql.DB
	checkpoint time.Time
}

// New creates a new Poller. checkpoint is the last persisted offset;
// pass time.Time{} to start from the beginning of time.
func New(cfg config.ConnectorConfig, checkpoint time.Time) *Poller {
	return &Poller{
		cfg:        cfg,
		checkpoint: checkpoint,
	}
}

func (p *Poller) ID() string { return p.cfg.ID }

// Checkpoint returns the current watermark position.
func (p *Poller) Checkpoint() time.Time { return p.checkpoint }

// Connect opens a connection to the source database.
func (p *Poller) Connect(ctx context.Context) error {
	db, err := sql.Open("postgres", p.cfg.SourceDSN)
	if err != nil {
		return fmt.Errorf("poller %s: open: %w", p.cfg.ID, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("poller %s: ping: %w", p.cfg.ID, err)
	}
	p.db = db
	log.Printf("[connector:%s] connected to source database", p.cfg.ID)
	return nil
}

// Poll queries new rows from the source table and returns them as CDC events.
// It advances the internal checkpoint to the maximum watermark seen in this batch.
func (p *Poller) Poll(ctx context.Context) ([]*event.Event, error) {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC LIMIT $2`,
		p.cfg.Table, p.cfg.WatermarkColumn, p.cfg.WatermarkColumn,
	)

	rows, err := p.db.QueryContext(ctx, query, p.checkpoint, p.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("poller %s: query: %w", p.cfg.ID, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("poller %s: columns: %w", p.cfg.ID, err)
	}

	var events []*event.Event
	var maxWatermark time.Time

	for rows.Next() {
		// Scan all columns into generic interface values.
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("poller %s: scan: %w", p.cfg.ID, err)
		}

		// Build a column-name → value map for JSON serialization.
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}

		// Advance watermark.
		if wv, ok := row[p.cfg.WatermarkColumn]; ok {
			if t, ok := wv.(time.Time); ok && t.After(maxWatermark) {
				maxWatermark = t
			}
		}

		after, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("poller %s: marshal row: %w", p.cfg.ID, err)
		}

		e := &event.Event{
			EventID:       newUUID(),
			Connector:     p.cfg.ID,
			Database:      p.cfg.Database,
			Table:         p.cfg.Table,
			Operation:     event.OperationUpsert,
			After:         after,
			SchemaVersion: 1,
			Timestamp:     time.Now().UTC(),
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("poller %s: rows: %w", p.cfg.ID, err)
	}

	// Advance checkpoint only if we actually read rows.
	if !maxWatermark.IsZero() {
		p.checkpoint = maxWatermark
		log.Printf("[connector:%s] polled %d events, checkpoint → %s",
			p.cfg.ID, len(events), p.checkpoint.Format(time.RFC3339))
	}

	return events, nil
}

// Close releases the database connection.
func (p *Poller) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// newUUID generates a random UUID v4 using the standard library only.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
