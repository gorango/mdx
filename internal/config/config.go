package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Exchanges ExchangesConfig `yaml:"exchanges"`
	Flusher   FlusherConfig   `yaml:"flusher"`
}

type ExchangesConfig struct {
	Binance     ExchangeConfig `yaml:"binance"`
	Bybit       ExchangeConfig `yaml:"bybit"`
	Hyperliquid ExchangeConfig `yaml:"hyperliquid"`
}

type ExchangeConfig struct {
	Enabled bool     `yaml:"enabled"`
	Symbols []string `yaml:"symbols"`
	Testnet bool     `yaml:"testnet"`
	APIKey  string   `yaml:"api_key"`
	Secret  string   `yaml:"secret"`
}

type FlusherConfig struct {
	IntervalSeconds int `yaml:"interval_seconds"`
	MaxBatchSize    int `yaml:"max_batch_size"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Flusher.IntervalSeconds == 0 {
		cfg.Flusher.IntervalSeconds = 30
	}
	if cfg.Flusher.MaxBatchSize == 0 {
		cfg.Flusher.MaxBatchSize = 1000
	}
	return &cfg, nil
}

func LoadExchanges(path string) (*ExchangesConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg struct {
		Exchanges ExchangesConfig `yaml:"exchanges"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg.Exchanges, nil
}
