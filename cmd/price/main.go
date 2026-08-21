package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gorango/mdx/domain/symbols"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/internal/cache"
	"gorango/mdx/internal/config"
	"gorango/mdx/internal/db"
	"gorango/mdx/internal/rest"

	"github.com/joho/godotenv"
)

func main() {
	symbol := flag.String("symbol", "BTC/USDT:PERP", "Trading symbol")
	timeframeFlag := flag.String("timeframe", "1m", "Timeframe (1m, 5m, 1h, 4h, 1d, etc.) - note: data is always fetched at 1m")
	startDate := flag.String("start", "", "Start date (RFC3339, defaults to 24h ago)")
	endDate := flag.String("end", "", "End date (RFC3339, defaults to now)")
	configPath := flag.String("config", "config.yaml", "Path to config file")
	exchange := flag.String("exchange", "binance", "Exchange ID (binance, bybit)")
	showStats := flag.Bool("stats", false, "Show cache statistics")
	project := flag.String("project", "", "Project bars to higher timeframe (e.g., 1h, 1d)")
	count := flag.Int("count", 0, "Limit number of bars returned")
	overwrite := flag.Bool("overwrite", false, "Re-fetch and overwrite existing bars")
	flag.Parse()

	ctx := context.Background()

	_ = godotenv.Load()

	_, err := config.LoadExchanges(*configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	connString := os.Getenv("PG_URL")
	if connString == "" {
		fmt.Printf("Failed to connect to database: PG_URL env var not set\n")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	dbConn, err := db.New(connString)
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	restClient := createRESTClient(*exchange)
	priceCache := cache.NewPriceCache(*exchange, dbConn, restClient, logger)
	priceCache.SetOverwrite(*overwrite)

	if *showStats {
		stats := priceCache.GetMemoryStats()
		fmt.Printf("Memory cache entries: %d\n", stats.Len)
		return
	}

	var start, end time.Time
	if *startDate != "" {
		start, err = parseDate(*startDate)
		if err != nil {
			fmt.Printf("Invalid start date: %v\n", err)
			os.Exit(1)
		}
	}
	if *endDate != "" {
		end, err = parseDate(*endDate)
		if err != nil {
			fmt.Printf("Invalid end date: %v\n", err)
			os.Exit(1)
		}
	}

	canonical := symbols.NormalizeCanonical(*symbol)

	bars, getErr := priceCache.GetHistory(ctx, canonical, timeframe.MustParse(*timeframeFlag), start, end)
	if getErr != nil {
		fmt.Printf("Failed to fetch history: %v\n", getErr)
		os.Exit(1)
	}

	requestedTf := timeframe.MustParse(*timeframeFlag)
	isProjected := *project != "" && requestedTf.Ms < timeframe.MustParse(*project).Ms

	if isProjected {
		fmt.Printf("Fetched %d %s bars (projected from 1m) for %s %s (%s)\n",
			len(bars), requestedTf.ID, *exchange, *symbol, formatTimeRange(start, end))
	} else {
		fmt.Printf("Fetched %d %s bars for %s %s (%s)\n",
			len(bars), requestedTf.ID, *exchange, *symbol, formatTimeRange(start, end))
	}

	if *count > 0 && len(bars) > *count {
		bars = bars[len(bars)-*count:]
	}

	if *project != "" {
		targetTf := timeframe.MustParse(*project)
		if targetTf.Ms > requestedTf.Ms {
			fmt.Printf("\nProjecting to %s...\n", *project)
			resampled := priceCache.ResampleBars(bars, targetTf)
			fmt.Printf("Resampled to %d bars\n", len(resampled))
			for _, bar := range resampled {
				fmt.Printf("  %s O:%.4f H:%.4f L:%.4f C:%.4f V:%.2f\n",
					bar.Time.Format(time.RFC3339), bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
			}
			return
		}
	}

	for _, bar := range bars {
		fmt.Printf("  %s O:%.4f H:%.4f L:%.4f C:%.4f V:%.2f\n",
			bar.Time.Format(time.RFC3339), bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
	}
}

func createRESTClient(exchange string) rest.Client {
	switch exchange {
	case "binance":
		return rest.NewBinance(rest.Config{})
	case "bybit":
		return rest.NewBybit(rest.Config{})
	default:
		return nil
	}
}

func formatTimeRange(start, end time.Time) string {
	if start.IsZero() && end.IsZero() {
		return "all time"
	}
	if start.IsZero() {
		return "start to " + end.Format("2006-01-02")
	}
	if end.IsZero() {
		return start.Format("2006-01-02") + " to now"
	}
	return start.Format("2006-01-02") + " to " + end.Format("2006-01-02")
}

func parseDate(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02T15",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse date: %s", s)
}
