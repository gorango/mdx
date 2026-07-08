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
	"gorango/exchanges/internal/orderbook/pipeline"

	"github.com/joho/godotenv"
)

func parseDateTime(s string) (time.Time, error) {
	// Try YYYY-MM-DDTHH:MM format first
	if t, err := time.ParseInLocation("2006-01-02T15:04", s, time.UTC); err == nil {
		return t, nil
	}
	// Try YYYY-MM-DDTHH format
	if t, err := time.ParseInLocation("2006-01-02T15", s, time.UTC); err == nil {
		return t, nil
	}
	// Try YYYY-MM-DD format
	if t, err := time.ParseInLocation("2006-01-02", s, time.UTC); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date format: %s (expected YYYY-MM-DD, YYYY-MM-DDTHH, or YYYY-MM-DDTHH:MM)", s)
}

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// It's okay if .env doesn't exist
		_ = err
	}

	var (
		symbol    = flag.String("symbol", "", "Trading symbol (e.g., BTC/USDT:PERP)")
		start     = flag.String("start", "", "Start datetime YYYY-MM-DD, YYYY-MM-DDTHH, or YYYY-MM-DDTHH:MM")
		end       = flag.String("end", "", "End datetime YYYY-MM-DD, YYYY-MM-DDTHH, or YYYY-MM-DDTHH:MM")
		exchange  = flag.String("exchange", "binance_futures", "Exchange name")
		resume    = flag.Bool("resume", false, "Skip already processed hours")
		dryRun    = flag.Bool("dry-run", false, "Skip DB insert, write bars to JSON files")
		outputDir = flag.String("output-dir", "./hydrate-output", "Output directory for dry-run JSON files")
	)

	flag.Parse()

	// Validate required flags
	if *symbol == "" || *start == "" || *end == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --symbol <symbol> --start <YYYY-MM-DD[THH:MM]> --end <YYYY-MM-DD[THH:MM]> [options]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  --symbol BTC/USDT:PERP  (futures)\n")
		fmt.Fprintf(os.Stderr, "  --symbol BTC/USDT:SPOT  (spot)\n")
		fmt.Fprintf(os.Stderr, "  --symbol BTC/USDT:PERP --start 2026-04-14 --end 2026-04-14  (full day)\n")
		fmt.Fprintf(os.Stderr, "  --symbol BTC/USDT:PERP --start 2026-04-14T10 --end 2026-04-14T12  (hours)\n")
		fmt.Fprintf(os.Stderr, "  --symbol BTC/USDT:PERP --start 2026-04-14T10:18 --end 2026-04-14T10:56  (sub-hour)\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	canonicalSymbol := symbols.NormalizeCanonical(*symbol)

	// Get API key from environment
	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		log.Fatal("CRYPTO_HFT_DATA environment variable is required")
	}

	// Get database connection
	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Fatalf("Failed to connect to database: PG_URL env var not set")
	}

	// Parse dates - support both YYYY-MM-DD and YYYY-MM-DDTHH:MM formats
	startDate, err := parseDateTime(*start)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}

	endDate, err := parseDateTime(*end)
	if err != nil {
		log.Fatalf("Invalid end date: %v", err)
	}

	if endDate.Before(startDate) {
		log.Fatal("End date cannot be before start date")
	}

	// Connect to database
	database, err := db.New(pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	// Create coordinator config
	config := pipeline.Config{
		Symbol:     canonicalSymbol,
		StartDate:  startDate,
		EndDate:    endDate,
		Exchange:   *exchange,
		APIKey:     apiKey,
		ResumeMode: *resume,
		DryRun:     *dryRun,
		OutputDir:  *outputDir,
	}

	// Create and run coordinator
	coordinator := pipeline.NewCoordinator(config, database)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Run(ctx); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	log.Println("Pipeline completed successfully")
}
