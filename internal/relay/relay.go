package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/fluxcdc/fluxcdc/internal/store/postgres"
)

// Relay reads unpublished events from the event store and publishes them to Kafka.
// After a successful publish, events are marked as published in the store.
type Relay struct {
	store     *postgres.Store
	writer    *kafka.Writer
	batchSize int
}

// New creates a Relay connected to the given store and Kafka topic.
func New(store *postgres.Store, brokers []string, topic string) *Relay {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 100 * time.Millisecond,
		// Allow Kafka topic to be auto-created on first write.
		AllowAutoTopicCreation: true,
	}
	return &Relay{
		store:     store,
		writer:    writer,
		batchSize: 50,
	}
}

// Flush fetches one batch of unpublished events, publishes them to Kafka,
// and marks them as published in the store.
// Returns nil immediately if there are no pending events.
func (r *Relay) Flush(ctx context.Context) error {
	events, err := r.store.GetUnpublishedEvents(ctx, r.batchSize)
	if err != nil {
		return fmt.Errorf("relay: fetch events: %w", err)
	}
	if len(events) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, 0, len(events))
	ids := make([]string, 0, len(events))

	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			log.Printf("[relay] skipping event %s: marshal error: %v", e.EventID, err)
			continue
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(e.EventID),
			Value: data,
		})
		ids = append(ids, e.EventID)
	}

	if len(msgs) == 0 {
		return nil
	}

	if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("relay: write to kafka: %w", err)
	}

	if err := r.store.MarkPublished(ctx, ids); err != nil {
		// Not fatal: events will be re-published on next flush (at-least-once delivery).
		log.Printf("[relay] warning: failed to mark %d events published: %v", len(ids), err)
	}

	log.Printf("[relay] published %d events to kafka", len(ids))
	return nil
}

// Close closes the underlying Kafka writer.
func (r *Relay) Close() error {
	return r.writer.Close()
}
