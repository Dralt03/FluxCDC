package connector

import (
	"context"

	"github.com/fluxcdc/fluxcdc/internal/event"
)

// Connector is the interface that all source connectors must implement.
// Phase 1 implements only the "poll" connector type.
type Connector interface {
	// ID returns the unique connector identifier from config.
	ID() string

	// Connect establishes the connection to the source database.
	Connect(ctx context.Context) error

	// Poll fetches new events from the source since the last checkpoint.
	// Returns an empty slice (not an error) when there are no new events.
	Poll(ctx context.Context) ([]*event.Event, error)

	// Close releases all resources held by the connector.
	Close() error
}
