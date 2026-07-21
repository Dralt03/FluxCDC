package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"fluxcdc/internal/event"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Publish(ctx context.Context, e *event.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event for kafka: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.EventID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, len(events))
	for i, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal event for kafka: %w", err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(e.EventID),
			Value: payload,
		}
	}

	err := p.writer.WriteMessages(ctx, msgs...)
	if err != nil {
		return fmt.Errorf("failed to write messages to kafka: %w", err)
	}

	return nil
}
