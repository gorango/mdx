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
	backfillOB := flag.Bool("backfill-ob", true, "Hourly cryptoHFT ob-hydrate backfill at :25 UTC (overwrite: rebuild prior two hours from cryptoHFT + settled Binance funding once parquets are available; each hour swept twice so a delayed tail is still captured) [default on, pass -backfill-ob=false to disable]")
	netflow := flag.Bool("netflow", true, "On-chain exchange netflow refresh (BigQuery -> flow_bars) every 6h at :30 UTC [default on, pass -netflow=false to disable]")
	netflowScript := flag.String("netflow-script", "scripts/fetch_netflow.py", "Path to fetch_netflow.py")
	flag.Parse()

	s, err := streamer.New(streamer.Options{
		ConfigPath:    *configPath,
		NatsURL:       *natsURL,
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
