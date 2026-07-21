package connectors

import (
	"context"

	"fluxcdc/internal/event"
)

type Connector interface {
	// Start begins capturing changes and sending them to the returned channel.
	Start(ctx context.Context) (<-chan *event.Event, <-chan error, error)
	
	// Close releases any database or connection resources.
	Close() error
}
