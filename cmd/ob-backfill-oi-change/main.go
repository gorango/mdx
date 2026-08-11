package main

import (
	"context"
	"flag"
	"fmt"
	"gorango/mdx/internal/db"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	help := flag.Bool("h", false, "Show help")
	helpLong := flag.Bool("help", false, "Show help")
	symbol := flag.String("symbol", "", "Symbol to backfill (optional, default: all)")
	start := flag.String("start", "", "Start date YYYY-MM-DD (optional, default: all)")
	end := flag.String("end", "", "End date YYYY-MM-DD (optional, default: all)")
	batch := flag.Int("batch", 50000, "Batch size per UPDATE")
	dryRun := flag.Bool("dry-run", false, "Count rows without updating")
	flag.Parse()

	if *help || *helpLong {
		fmt.Println("Backfill open_interest_change for orderbook_bars.")
		fmt.Println("Usage: go run cmd/ob-backfill-oi-change/main.go [options]")
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

	database, err := db.New(pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	var startTime, endTime *time.Time
	var symbolPtr *string
	if *symbol != "" {
		symbolPtr = symbol
	}
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

	if *dryRun {
		var count int
		err := database.Pool().QueryRow(ctx, `
			SELECT COUNT(*) FROM orderbook_bars
			WHERE open_interest IS NOT NULL
			AND open_interest_change IS NULL
			AND ($1::text IS NULL OR symbol = $1)
			AND ($2::timestamptz IS NULL OR timestamp >= $2)
			AND ($3::timestamptz IS NULL OR timestamp < $3)
		`, symbolPtr, startTime, endTime).Scan(&count)
		if err != nil {
			log.Fatalf("Count query failed: %v", err)
		}
		fmt.Printf("Dry run: %d rows need open_interest_change backfilled\n", count)
		return
	}

	fmt.Println("Starting open_interest_change backfill...")
	totalUpdated, totalErrors := database.BackfillOpenInterestChange(ctx, symbolPtr, startTime, endTime, *batch)
	fmt.Printf("Backfill complete: %d rows updated, %d errors\n", totalUpdated, totalErrors)
}
