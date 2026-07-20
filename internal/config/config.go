package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

var includeRe = regexp.MustCompile(`(?m)^\s*!include\s+(\S+)\s*`)

func resolveIncludes(path string, data []byte) ([]byte, error) {
	dir := filepath.Dir(path)
	return includeRe.ReplaceAllFunc(data, func(match []byte) []byte {
		parts := includeRe.FindSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		incPath := string(parts[1])
		if !filepath.IsAbs(incPath) {
			incPath = filepath.Join(dir, incPath)
		}
		incData, err := os.ReadFile(incPath)
		if err != nil {
			return match
		}

		indent := match[:len(match)-len(bytes.TrimLeft(match, " \t"))]
		indentCopy := make([]byte, len(indent))
		copy(indentCopy, indent)
		lines := bytes.Split(incData, []byte("\n"))
		for i, line := range lines {
			if len(bytes.TrimSpace(line)) > 0 {
				lines[i] = append(indentCopy, line...)
			}
		}
		return bytes.Join(lines, []byte("\n"))
	}), nil
}

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
	data, err = resolveIncludes(path, data)
	if err != nil {
		return nil, fmt.Errorf("resolve includes: %w", err)
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

	data, err = resolveIncludes(path, data)
	if err != nil {
		return nil, fmt.Errorf("resolve includes: %w", err)
	}

	var cfg struct {
		Exchanges ExchangesConfig `yaml:"exchanges"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg.Exchanges, nil
}
