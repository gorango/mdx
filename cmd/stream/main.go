package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"gorango/exchanges/internal/streamer"

	"gopkg.in/yaml.v3"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	symbolsPath := flag.String("symbols", "../config/symbols.yaml", "Path to symbols file")
	flag.Parse()

	var symbols []string
	if *symbolsPath != "" {
		data, err := os.ReadFile(*symbolsPath)
		if err != nil {
			fmt.Printf("Failed to read symbols file: %v\n", err)
			os.Exit(1)
		}
		if err := yaml.Unmarshal(data, &symbols); err != nil {
			fmt.Printf("Failed to parse symbols file: %v\n", err)
			os.Exit(1)
		}
	}

	s, err := streamer.New(streamer.Options{
		ConfigPath: *configPath,
		NatsURL:    *natsURL,
		Symbols:    symbols,
		Logger:     slog.Default(),
	})
	if err != nil {
		fmt.Printf("Failed to initialize streamer: %v\n", err)
		os.Exit(1)
	}

	if err := s.Start(); err != nil {
		fmt.Printf("Streamer error: %v\n", err)
		os.Exit(1)
	}
}
