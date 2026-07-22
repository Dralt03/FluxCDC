package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"fluxcdc/internal/config"
	"fluxcdc/internal/connectors"
	"fluxcdc/internal/data"
	"fluxcdc/internal/state"
)

func main() {
	configPath := flag.String("config", "config/config.yml", "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	log.Println("Configuration loaded successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 2. Initialize State Store
	store, err := state.NewStore(cfg.Database.StateConn)
	if err != nil {
		log.Fatalf("Error initializing state store: %v", err)
	}
	defer store.Close()

	log.Println("Running database migrations on state store...")
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
	log.Println("State store initialized and migrated")

	// 3. Initialize Kafka Producer
	producer := data.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer producer.Close()
	log.Printf("Kafka producer initialized targeting topic %s\n", cfg.Kafka.Topic)

	// 4. Initialize Source Connector
	var poller connectors.Connector
	if cfg.Pipeline.Source.Connector == "postgres" {
		poller, err = connectors.NewPostgresPoller(*cfg, store)
		if err != nil {
			log.Fatalf("Failed to create postgres poller: %v", err)
		}
	} else {
		log.Fatalf("Unsupported source connector: %s", cfg.Pipeline.Source.Connector)
	}
	defer poller.Close()

	eventChan, errChan, err := poller.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start poller: %v", err)
	}
	log.Println("Postgres poller started capturing events")

	var wg sync.WaitGroup

	// 5. Ingestion Loop (Receive from Poller -> Save to State Store)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errChan:
				if ok && err != nil {
					log.Printf("Poller error occurred: %v", err)
				}
			case evt, ok := <-eventChan:
				if !ok {
					return
				}
				log.Printf("Polled event: ID=%s, Table=%s, Op=%s", evt.EventID, evt.Source.Table, evt.Operation)
				if err := store.SaveEvent(ctx, evt); err != nil {
					log.Printf("Failed to save event to state store: %v", err)
				}
			}
		}
	}()

	// 6. Relay Loop (Fetch unpublished from State Store -> Publish to Kafka -> Mark published)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				events, err := store.GetUnpublishedEvents(ctx, 50)
				if err != nil {
					log.Printf("Relay: failed to get unpublished events: %v", err)
					continue
				}
				if len(events) == 0 {
					continue
				}

				log.Printf("Relay: sending batch of %d events to Kafka...", len(events))
				if err := producer.PublishBatch(ctx, events); err != nil {
					log.Printf("Relay: failed to publish batch to Kafka: %v", err)
					continue
				}

				eventIDs := make([]string, len(events))
				for i, e := range events {
					eventIDs[i] = e.EventID
				}

				if err := store.MarkPublished(ctx, eventIDs); err != nil {
					log.Printf("Relay: failed to mark events as published in state store: %v", err)
				} else {
					log.Printf("Relay: successfully marked %d events as published", len(events))
				}
			}
		}
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received shutdown signal (%s). Cleaning up...", sig)
	cancel()

	wg.Wait()
	log.Println("FluxCDC exited gracefully")
}