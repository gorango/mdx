package main

import (
	"flag"
	"fmt"
	"gorango/mdx/internal/streamer"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	symbolsPath := flag.String("symbols", "../config/symbols.yaml", "Path to symbols file")
	backfillOB := flag.Bool("backfill-ob", true, "Hourly cryptoHFT ob-hydrate backfill at :25 UTC (overwrite: rebuild prior two hours from cryptoHFT + settled Binance funding once parquets are available; each hour swept twice so a delayed tail is still captured) [default on, pass -backfill-ob=false to disable]")
	netflow := flag.Bool("netflow", true, "On-chain exchange netflow refresh (BigQuery -> flow_bars) every 6h at :30 UTC [default on, pass -netflow=false to disable]")
	netflowScript := flag.String("netflow-script", "scripts/fetch_netflow.py", "Path to fetch_netflow.py")
	fred := flag.Bool("fred", true, "FRED macro refresh (St. Louis Fed -> fred_observations) daily at 23:30 UTC, after the 18:00 ET EOD update [default on, pass -fred=false to disable]")
	fredScript := flag.String("fred-script", "scripts/fetch_fred.py", "Path to fetch_fred.py")
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
		ConfigPath:    *configPath,
		NatsURL:       *natsURL,
		Symbols:       symbols,
		Logger:        slog.Default(),
		BackfillOB:    *backfillOB,
		Netflow:       *netflow,
		NetflowScript: *netflowScript,
		Fred:          *fred,
		FredScript:    *fredScript,
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
