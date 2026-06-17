package event

import (
	"encoding/json"
	"time"
)

// Operation represents the type of database change.
type Operation string

const (
	OperationInsert Operation = "INSERT"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
	// OperationUpsert is used by the polling connector when INSERT vs UPDATE
	// cannot be determined without additional state tracking.
	OperationUpsert Operation = "UPSERT"
)

// Event is the canonical CDC event model. Every captured database change
// is represented as an immutable Event.
type Event struct {
	EventID       string          `json:"event_id"`
	Connector     string          `json:"connector"`
	Database      string          `json:"database"`
	Table         string          `json:"table"`
	Operation     Operation       `json:"operation"`
	Before        json.RawMessage `json:"before,omitempty"` // Row state before the change
	After         json.RawMessage `json:"after,omitempty"`  // Row state after the change
	SchemaVersion int             `json:"schema_version"`
	Timestamp     time.Time       `json:"timestamp"`
}
