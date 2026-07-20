package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Database DBConfig       `yaml:"database"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type DBConfig struct {
	SourceConn string `yaml:"source_conn"`
	StateConn  string `yaml:"state_conn"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type PipelineConfig struct {
	Source SourceConfig `yaml:"source"`
	Sink   []string     `yaml:"sink"`
}

type SourceConfig struct {
	Connector string   `yaml:"connector"`
	Tables    []string `yaml:"tables"`
	Filters   []string `yaml:"filters"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}
