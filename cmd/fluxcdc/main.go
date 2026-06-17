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

	"github.com/fluxcdc/fluxcdc/internal/config"
	"github.com/fluxcdc/fluxcdc/internal/connector/poll"
	"github.com/fluxcdc/fluxcdc/internal/relay"
	pgstore "github.com/fluxcdc/fluxcdc/internal/store/postgres"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// ── Load config ──────────────────────────────────────────────────────────
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Event store ───────────────────────────────────────────────────────────
	store, err := pgstore.New(cfg.EventStore.DSN)
	if err != nil {
		log.Fatalf("failed to connect to event store: %v", err)
	}
	defer store.Close()

	if err := store.Ping(ctx); err != nil {
		log.Fatalf("event store not reachable: %v", err)
	}
	log.Println("[store] connected to postgres event store")

	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migration failed: %v", err)
	}
	log.Println("[store] schema ready")

	// ── Relay service ─────────────────────────────────────────────────────────
	rel := relay.New(store, cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer rel.Close()
	log.Printf("[relay] targeting kafka topic %q on %v", cfg.Kafka.Topic, cfg.Kafka.Brokers)

	var wg sync.WaitGroup

	// ── Start relay loop ──────────────────────────────────────────────────────
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := rel.Flush(ctx); err != nil {
					log.Printf("[relay] flush error: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── Start connector loops ─────────────────────────────────────────────────
	for _, connCfg := range cfg.Connectors {
		if connCfg.Type != "poll" {
			log.Printf("[connector:%s] type %q not supported in Phase 1, skipping", connCfg.ID, connCfg.Type)
			continue
		}

		// Load saved checkpoint (zero time = capture from the beginning).
		checkpoint, err := store.GetOffset(ctx, connCfg.ID)
		if err != nil {
			log.Fatalf("[connector:%s] failed to load offset: %v", connCfg.ID, err)
		}

		poller := poll.New(connCfg, checkpoint)
		if err := poller.Connect(ctx); err != nil {
			log.Fatalf("[connector:%s] connect failed: %v", connCfg.ID, err)
		}

		interval := connCfg.GetPollInterval()
		log.Printf("[connector:%s] starting poll loop (interval=%s, table=%s, watermark=%s)",
			connCfg.ID, interval, connCfg.Table, connCfg.WatermarkColumn)

		wg.Add(1)
		go func(p *poll.Poller, interval time.Duration) {
			defer wg.Done()
			defer p.Close()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					events, err := p.Poll(ctx)
					if err != nil {
						log.Printf("[connector:%s] poll error: %v", p.ID(), err)
						continue
					}
					if len(events) == 0 {
						continue
					}

					// Persist events to the store.
					saved := 0
					for _, e := range events {
						if err := store.SaveEvent(ctx, e); err != nil {
							log.Printf("[connector:%s] save event error: %v", p.ID(), err)
							continue
						}
						saved++
					}

					// Persist the advanced checkpoint.
					if err := store.SaveOffset(ctx, p.ID(), p.Checkpoint()); err != nil {
						log.Printf("[connector:%s] save offset error: %v", p.ID(), err)
					}

					log.Printf("[connector:%s] saved %d/%d events to store", p.ID(), saved, len(events))

				case <-ctx.Done():
					return
				}
			}
		}(poller, interval)
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal %s, shutting down...", sig)

	cancel()
	wg.Wait()
	log.Println("shutdown complete")
}
