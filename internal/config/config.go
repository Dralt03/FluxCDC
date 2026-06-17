package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for FluxCDC.
type Config struct {
	EventStore EventStoreConfig  `yaml:"event_store"`
	Kafka      KafkaConfig       `yaml:"kafka"`
	Connectors []ConnectorConfig `yaml:"connectors"`
}

// EventStoreConfig holds connection details for the Postgres event store.
type EventStoreConfig struct {
	DSN string `yaml:"dsn"`
}

// KafkaConfig holds Kafka broker and topic settings.
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

// ConnectorConfig defines a single CDC connector.
type ConnectorConfig struct {
	ID              string `yaml:"id"`
	Type            string `yaml:"type"`             // "poll" (Phase 1 only)
	SourceDSN       string `yaml:"source_dsn"`       // Connection string for the source DB
	Database        string `yaml:"database"`         // Logical database name for event metadata
	Table           string `yaml:"table"`            // Source table to capture
	WatermarkColumn string `yaml:"watermark_column"` // e.g. "updated_at"
	PrimaryKey      string `yaml:"primary_key"`      // e.g. "id"
	PollInterval    string `yaml:"poll_interval"`    // e.g. "5s", "1m"
	BatchSize       int    `yaml:"batch_size"`       // Max rows per poll cycle
}

// GetPollInterval parses the PollInterval string and returns a time.Duration.
// Defaults to 5 seconds if unset or unparseable.
func (c ConnectorConfig) GetPollInterval() time.Duration {
	if c.PollInterval == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(c.PollInterval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

// Load reads and parses a YAML config file at the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	// Apply defaults
	for i := range cfg.Connectors {
		if cfg.Connectors[i].BatchSize <= 0 {
			cfg.Connectors[i].BatchSize = 100
		}
	}

	return &cfg, nil
}
