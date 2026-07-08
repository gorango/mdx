package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/internal/db"
	"gorango/exchanges/internal/orderbook/api"

	"github.com/joho/godotenv"
)

func main() {
	help := flag.Bool("h", false, "Show help")
	helpLong := flag.Bool("help", false, "Show help")
	start := flag.String("start", "", "Start date YYYY-MM-DD (optional, default: all)")
	end := flag.String("end", "", "End date YYYY-MM-DD (optional, default: all)")
	batch := flag.Int("batch", 5000, "Batch size per UPDATE")
	flag.Parse()

	if *help || *helpLong {
		fmt.Println("Backfill funding_rate and funding_rate_change for orderbook_bars.")
		fmt.Println("Usage: go run cmd/ob-backfill-funding/main.go [options]")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if err := godotenv.Load(); err != nil {
		_ = err
	}

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Fatalf("Failed to connect to database: PG_URL env var not set")
	}
	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		log.Fatal("CRYPTO_HFT_DATA environment variable is required")
	}

	database, err := db.New(pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	var startTime, endTime *time.Time
	if *start != "" {
		t, err := time.Parse("2006-01-02", *start)
		if err != nil {
			log.Fatalf("Invalid start date: %v", err)
		}
		startTime = &t
	}
	if *end != "" {
		t, err := time.Parse("2006-01-02", *end)
		if err != nil {
			log.Fatalf("Invalid end date: %v", err)
		}
		endTime = &t
	}

	fmt.Println("Starting backfill...")
	bc := api.NewBinanceClient()

	syms, err := database.GetDistinctSymbols(ctx, "binance_futures")
	if err != nil {
		log.Fatalf("Failed to get distinct symbols: %v", err)
	}
	fmt.Printf("Found %d symbols to process\n", len(syms))

	var fetchEnd time.Time
	if endTime != nil {
		fetchEnd = endTime.Add(8 * time.Hour)
	} else {
		fetchEnd = time.Now().Add(8 * time.Hour)
	}
	var fetchStart time.Time
	if startTime != nil {
		fetchStart = startTime.Add(-8 * time.Hour)
	} else {
		fetchStart = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	var grandTotalUpdated, grandTotalErrors int
	for _, sym := range syms {
		fmt.Printf("\nProcessing %s...\n", sym)
		exchangeSym := symbols.CanonicalToExchange(sym, "binance_futures")
		fundingPoints, err := bc.FetchFundingHistory(exchangeSym, fetchStart.UnixMilli(), fetchEnd.UnixMilli())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to fetch funding history for %s: %v\n", sym, err)
			grandTotalErrors++
			continue
		}
		fmt.Printf("  Fetched %d funding points\n", len(fundingPoints))

		updated, errs := database.BackfillFundingHistory(ctx, sym, fundingPoints, startTime, endTime, *batch)
		grandTotalUpdated += updated
		grandTotalErrors += errs
	}

	fmt.Printf("\nBackfill complete: %d rows updated, %d errors\n", grandTotalUpdated, grandTotalErrors)
}
