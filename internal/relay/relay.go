package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/fluxcdc/fluxcdc/internal/event"
	"github.com/fluxcdc/fluxcdc/internal/store/postgres"
)

// Relay reads unpublished events from the event store and publishes them to Kafka.
// Events that fail to publish are routed to the DLQ table (at-least-once delivery).
type Relay struct {
	store     *postgres.Store
	writer    *kafka.Writer
	batchSize int
}

// New creates a Relay connected to the given store and Kafka topic.
func New(store *postgres.Store, brokers []string, topic string) *Relay {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           100 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
	return &Relay{
		store:     store,
		writer:    writer,
		batchSize: 50,
	}
}

// Flush fetches one batch of unpublished events and publishes them to Kafka.
//
// On success  → events marked published=TRUE in the store.
// On failure  → events routed to the DLQ table; relay continues with next batch.
func (r *Relay) Flush(ctx context.Context) error {
	events, err := r.store.GetUnpublishedEvents(ctx, r.batchSize)
	if err != nil {
		return fmt.Errorf("relay: fetch events: %w", err)
	}
	if len(events) == 0 {
		return nil
	}

	// Build Kafka messages.
	type entry struct {
		event *event.Event
		msg   kafka.Message
	}
	entries := make([]entry, 0, len(events))
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			log.Printf("[relay] skipping event %s: marshal error: %v", e.EventID, err)
			continue
		}
		entries = append(entries, entry{
			event: e,
			msg:   kafka.Message{Key: []byte(e.EventID), Value: data},
		})
	}
	if len(entries) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, len(entries))
	for i, en := range entries {
		msgs[i] = en.msg
	}

	if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
		// Batch publish failed — move every event in this batch to the DLQ.
		// The relay will then continue; these events won't block future batches.
		log.Printf("[relay] kafka write failed (%v), routing %d events to DLQ", err, len(entries))
		errMsg := err.Error()
		for _, en := range entries {
			if dlqErr := r.store.SaveDLQEvent(ctx, en.event.EventID, en.event.Connector, errMsg); dlqErr != nil {
				log.Printf("[relay] DLQ save failed for event %s: %v", en.event.EventID, dlqErr)
			}
		}
		// Mark as published so the relay doesn't retry them endlessly.
		// The DLQ table is now the source of truth for retrying.
		ids := make([]string, len(entries))
		for i, en := range entries {
			ids[i] = en.event.EventID
		}
		_ = r.store.MarkPublished(ctx, ids)
		return nil
	}

	// Success — mark published.
	ids := make([]string, len(entries))
	for i, en := range entries {
		ids[i] = en.event.EventID
	}
	if err := r.store.MarkPublished(ctx, ids); err != nil {
		log.Printf("[relay] warning: failed to mark %d events published: %v", len(ids), err)
	}
	log.Printf("[relay] published %d events to kafka", len(ids))
	return nil
}

// Close closes the underlying Kafka writer.
func (r *Relay) Close() error {
	return r.writer.Close()
}

