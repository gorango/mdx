package main

import (
	"context"
	"flag"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/internal/db"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

const HELP = `
Prune data from database

Usage: go run cmd/prune/main.go [options]

Options:
  --type <type>         Data type to prune: orderbook, price, or both (required)
  --symbol <symbol>     Trading symbol (e.g., BTC/USDT:PERP)
  --exchange <exchange> Exchange name (default: binance_futures)
  --start <date>        Start date YYYY-MM-DD (optional)
  --end <date>          End date YYYY-MM-DD (optional)
  -h, --help            Show this help message

If no dates are specified, all data for the symbol will be pruned.
`

func parseDate(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %s (expected YYYY-MM-DD)", s)
	}
	return &t, nil
}

type pruneType string

const (
	pruneTypeOrderbook pruneType = "orderbook"
	pruneTypePrice     pruneType = "price"
	pruneTypeBoth      pruneType = "both"
)

func (p pruneType) IsValid() bool {
	switch p {
	case pruneTypeOrderbook, pruneTypePrice, pruneTypeBoth:
		return true
	}
	return false
}

func main() {
	help := flag.Bool("h", false, "Show help")
	helpLong := flag.Bool("help", false, "Show help")

	dataType := flag.String("type", "", "Data type: orderbook, price, or both (required)")
	symbol := flag.String("symbol", "", "Trading symbol (e.g., BTC/USDT:PERP)")
	exchange := flag.String("exchange", "binance_futures", "Exchange name")
	start := flag.String("start", "", "Start date YYYY-MM-DD (optional)")
	end := flag.String("end", "", "End date YYYY-MM-DD (optional)")

	flag.Parse()

	if *help || *helpLong {
		fmt.Print(HELP)
		os.Exit(0)
	}

	if *dataType == "" {
		fmt.Fprintf(os.Stderr, "Error: --type is required (orderbook, price, or both)\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s --type <type> --symbol <symbol> [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	pt := pruneType(*dataType)
	if !pt.IsValid() {
		fmt.Fprintf(os.Stderr, "Error: --type must be one of: orderbook, price, both\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s --type <type> --symbol <symbol> [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *symbol == "" {
		fmt.Fprintf(os.Stderr, "Error: --symbol is required\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s --type <type> --symbol <symbol> [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if err := godotenv.Load(); err != nil {
		_ = err
	}

	canonicalSymbol := symbols.NormalizeCanonical(*symbol)

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Fatalf("Failed to connect to database: PG_URL env var not set")
	}

	startDate, err := parseDate(*start)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}

	endDate, err := parseDate(*end)
	if err != nil {
		log.Fatalf("Invalid end date: %v", err)
	}

	if startDate != nil && endDate != nil && endDate.Before(*startDate) {
		log.Fatal("End date cannot be before start date")
	}

	database, err := db.New(pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	pruneLabel := string(pt)
	if startDate == nil && endDate == nil {
		fmt.Printf("Pruning ALL %s data for %s on %s...\n", pruneLabel, canonicalSymbol, *exchange)
	} else if startDate == nil {
		fmt.Printf("Pruning %s data for %s on %s before %s...\n", pruneLabel, canonicalSymbol, *exchange, endDate.Format("2006-01-02"))
	} else if endDate == nil {
		fmt.Printf("Pruning %s data for %s on %s from %s onwards...\n", pruneLabel, canonicalSymbol, *exchange, startDate.Format("2006-01-02"))
	} else {
		fmt.Printf("Pruning %s data for %s on %s from %s to %s...\n", pruneLabel, canonicalSymbol, *exchange, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	}

	switch pt {
	case pruneTypeOrderbook:
		barsDeleted, err := database.DeleteOrderbookBars(ctx, *exchange, canonicalSymbol, startDate, endDate)
		if err != nil {
			log.Fatalf("Failed to delete orderbook bars: %v", err)
		}
		fmt.Printf("Deleted %d orderbook bars\n", barsDeleted)

	case pruneTypePrice:
		barsDeleted, err := database.DeletePriceBars(ctx, *exchange, canonicalSymbol, startDate, endDate)
		if err != nil {
			log.Fatalf("Failed to delete price bars: %v", err)
		}
		fmt.Printf("Deleted %d price bars\n", barsDeleted)

	case pruneTypeBoth:
		obDeleted, err := database.DeleteOrderbookBars(ctx, *exchange, canonicalSymbol, startDate, endDate)
		if err != nil {
			log.Fatalf("Failed to delete orderbook bars: %v", err)
		}
		fmt.Printf("Deleted %d orderbook bars\n", obDeleted)

		priceDeleted, err := database.DeletePriceBars(ctx, *exchange, canonicalSymbol, startDate, endDate)
		if err != nil {
			log.Fatalf("Failed to delete price bars: %v", err)
		}
		fmt.Printf("Deleted %d price bars\n", priceDeleted)
	}

	fmt.Println("Prune completed successfully")
}
