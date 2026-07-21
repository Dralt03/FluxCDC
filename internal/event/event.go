package event

import (
	"encoding/json"
	"time"
)

type Operation string

const (
	OperationInsert Operation = "INSERT"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
	OperationUpsert Operation = "UPSERT"
)

type SourceMetadata struct {
	Connector string `json:"connector"`
	Database  string    `json:"database"`
	Schema    string    `json:"schema"`
	Table     string    `json:"table"`
	Timestamp time.Time `json:"ts"`
}

// Event represents a canonical CDC event payload.
type Event struct {
	EventID       string          `json:"event_id"`
	Source        SourceMetadata  `json:"source"`
	Operation     Operation       `json:"operation"`
	Before        json.RawMessage `json:"before,omitempty"`
	After         json.RawMessage `json:"after,omitempty"`
	SchemaVersion int             `json:"schema_version"`
	Timestamp     time.Time       `json:"timestamp"`
}
