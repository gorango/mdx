package main

import (
	"flag"
	"fmt"
	"gorango/mdx/internal/streamer"
	"log/slog"
	"os"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	backfillOB := flag.Bool("backfill-ob", false, "Enable hourly ob-hydrate backfill for latest 2 hours (overwrite)")
	flag.Parse()

	s, err := streamer.New(streamer.Options{
		ConfigPath: *configPath,
		NatsURL:    *natsURL,
		Logger:     slog.Default(),
		BackfillOB: *backfillOB,
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
