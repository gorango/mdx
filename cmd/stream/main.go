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
	backfillOB := flag.Bool("backfill-ob", false, "Enable hourly cryptoHFT ob-hydrate backfill of the previous two hours (overwrite: rebuild from cryptoHFT + settled Binance funding; each hour swept twice so a delayed tail is still captured)")
	netflow := flag.Bool("netflow", false, "Enable on-chain exchange netflow refresh (BigQuery -> flow_bars) on a 6h cadence")
	netflowScript := flag.String("netflow-script", "scripts/fetch-netflow.py", "Path to fetch-netflow.py")
	flag.Parse()

	s, err := streamer.New(streamer.Options{
		ConfigPath:    *configPath,
		NatsURL:       *natsURL,
		Symbols:       symbols,
		Logger:        slog.Default(),
		BackfillOB:    *backfillOB,
		Netflow:       *netflow,
		NetflowScript: *netflowScript,
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
