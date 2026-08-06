package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/db"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type ComparisonResult struct {
	Timestamp   int64
	Fields      map[string]FieldDiff
	MatchStatus string // "exact", "near", "missing_db", "missing_hydrate", "extra_db"
}

type FieldDiff struct {
	HydrateValue string
	DBValue      string
	Diff         float64
	WithinTol    bool
}

type Summary struct {
	TotalHydrate int
	TotalDB      int
	ExactMatches int
	NearMatches  int
	MissingInDB  int
	ExtraInDB    int
	FieldStats   map[string]FieldSummary
}

type FieldSummary struct {
	Compared  int
	WithinTol int
	MaxDiff   float64
	AvgDiff   float64
}

var tolerances = map[string]float64{
	"vwap":                 0.01,   // 1% (trade-level differences between sources)
	"trade_count":          5,      // trade counts differ between data sources
	"buy_volume":           0.05,   // 5% (trade classification differs between sources)
	"sell_volume":          0.05,   // 5% (trade classification differs between sources)
	"avg_spread":           0.01,   // 1% BPS tolerance
	"spread_std_dev":       0.05,   // 5% tolerance
	"depth_imbalance":      0.05,   // 5% tolerance (cold-start and source differences)
	"depth_ratio":          0.05,   // 5% tolerance (cold-start and source differences)
	"open_interest":        0.003,  // 0.3% proportional (5s live vs 5min hydrate sampling)
	"open_interest_change": 0.003,  // 0.3% proportional
	"funding_rate":         0.0001, // exact for funding
	"funding_rate_change":  0.0001, // exact for funding
	"liq_long_vol":         0.001,  // 0.1%
	"liq_short_vol":        0.001,  // 0.1%
	"liq_covered":          0,      // exact match expected
}

var proportionalTolerance = map[string]bool{
	"open_interest":        true,
	"open_interest_change": true,
}

var fieldSets = map[string][]string{
	"microstructure": {"avg_spread", "spread_std_dev", "depth_imbalance", "depth_ratio"},
	"trades":         {"vwap", "trade_count", "buy_volume", "sell_volume"},
	"full": {
		"vwap", "trade_count", "buy_volume", "sell_volume",
		"avg_spread", "spread_std_dev", "depth_imbalance", "depth_ratio",
		"open_interest", "open_interest_change", "funding_rate", "funding_rate_change",
		"liq_long_vol", "liq_short_vol", "liq_covered",
	},
}

func parseDateTime(s string) (time.Time, error) {
	if t, err := time.ParseInLocation("2006-01-02T15:04", s, time.UTC); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", s, time.UTC); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date format: %s", s)
}

func loadHydrateBars(inputDir, exchange, symbol string, start, end time.Time) (map[int64]types.OrderbookBar, error) {
	bars := make(map[int64]types.OrderbookBar)

	dir := filepath.Join(inputDir, exchange, symbol)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read input dir: %w", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		// Parse date from filename: YYYY-MM-DD_HH.json
		parts := strings.TrimSuffix(entry.Name(), ".json")
		dateParts := strings.Split(parts, "_")
		if len(dateParts) != 2 {
			continue
		}

		fileTime, err := time.ParseInLocation("2006-01-02T15:04:05", dateParts[0]+"T"+dateParts[1]+":00:00", time.UTC)
		if err != nil {
			continue
		}

		if fileTime.Before(start) || fileTime.After(end) {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", entry.Name(), err)
		}

		var hourBars []types.OrderbookBar
		if err := json.Unmarshal(data, &hourBars); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", entry.Name(), err)
		}

		for _, bar := range hourBars {
			barTime := time.UnixMilli(bar.Timestamp)
			if barTime.Before(start) || !barTime.Before(end) {
				continue
			}
			bars[bar.Timestamp] = bar
		}
	}

	return bars, nil
}

func loadDBBars(database *db.DB, exchange, symbol string, start, end time.Time) (map[int64]types.OrderbookBar, error) {
	ctx := context.Background()

	bars, err := database.QueryOrderbookBars(ctx, exchange, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("query bars: %w", err)
	}

	result := make(map[int64]types.OrderbookBar)
	for _, bar := range bars {
		result[bar.Timestamp] = bar
	}

	return result, nil
}

func compareBars(hydrateBars, dbBars map[int64]types.OrderbookBar, fields []string) ([]ComparisonResult, Summary) {
	var results []ComparisonResult
	summary := Summary{
		TotalHydrate: len(hydrateBars),
		TotalDB:      len(dbBars),
		FieldStats:   make(map[string]FieldSummary),
	}

	allTimestamps := make(map[int64]bool)
	for ts := range hydrateBars {
		allTimestamps[ts] = true
	}
	for ts := range dbBars {
		allTimestamps[ts] = true
	}

	timestamps := make([]int64, 0, len(allTimestamps))
	for ts := range allTimestamps {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	for _, ts := range timestamps {
		hBar, hOk := hydrateBars[ts]
		dBar, dOk := dbBars[ts]

		result := ComparisonResult{
			Timestamp: ts,
			Fields:    make(map[string]FieldDiff),
		}

		if !hOk {
			result.MatchStatus = "extra_db"
			summary.ExtraInDB++
			results = append(results, result)
			continue
		}

		if !dOk {
			result.MatchStatus = "missing_db"
			summary.MissingInDB++
			results = append(results, result)
			continue
		}

		// Compare fields
		exactMatch := true
		nearMatch := false

		for _, field := range fields {
			switch field {
			case "vwap":
				compareFloatField(&result, &summary, field, hBar.VWAP, dBar.VWAP, &exactMatch, &nearMatch)
			case "trade_count":
				compareIntField(&result, &summary, field, int64(hBar.TradeCount), int64(dBar.TradeCount), &exactMatch, &nearMatch)
			case "buy_volume":
				compareFloatField(&result, &summary, field, hBar.BuyVolume, dBar.BuyVolume, &exactMatch, &nearMatch)
			case "sell_volume":
				compareFloatField(&result, &summary, field, hBar.SellVolume, dBar.SellVolume, &exactMatch, &nearMatch)
			case "avg_spread":
				compareFloatField(&result, &summary, field, hBar.AvgSpread, dBar.AvgSpread, &exactMatch, &nearMatch)
			case "spread_std_dev":
				compareFloatField(&result, &summary, field, hBar.SpreadStdDev, dBar.SpreadStdDev, &exactMatch, &nearMatch)
			case "depth_imbalance":
				compareFloatField(&result, &summary, field, hBar.DepthImbalance, dBar.DepthImbalance, &exactMatch, &nearMatch)
			case "depth_ratio":
				compareFloatField(&result, &summary, field, hBar.DepthRatio, dBar.DepthRatio, &exactMatch, &nearMatch)
			case "open_interest":
				comparePtrFloatField(&result, &summary, field, hBar.OpenInterest, dBar.OpenInterest, &exactMatch, &nearMatch)
			case "open_interest_change":
				comparePtrFloatField(&result, &summary, field, hBar.OpenInterestChange, dBar.OpenInterestChange, &exactMatch, &nearMatch)
			case "funding_rate":
				comparePtrFloatField(&result, &summary, field, hBar.FundingRate, dBar.FundingRate, &exactMatch, &nearMatch)
			case "funding_rate_change":
				comparePtrFloatField(&result, &summary, field, hBar.FundingRateChange, dBar.FundingRateChange, &exactMatch, &nearMatch)
			case "liq_long_vol":
				comparePtrFloatField(&result, &summary, field, hBar.LiqLongVol, dBar.LiqLongVol, &exactMatch, &nearMatch)
			case "liq_short_vol":
				comparePtrFloatField(&result, &summary, field, hBar.LiqShortVol, dBar.LiqShortVol, &exactMatch, &nearMatch)
			case "liq_covered":
				compareIntField(&result, &summary, field, int64(hBar.LiqCovered), int64(dBar.LiqCovered), &exactMatch, &nearMatch)
			}
		}

		if exactMatch {
			result.MatchStatus = "exact"
			summary.ExactMatches++
		} else if nearMatch {
			result.MatchStatus = "near"
			summary.NearMatches++
		} else {
			result.MatchStatus = "diff"
			summary.NearMatches++ // count as near match even if outside tolerance
		}

		results = append(results, result)
	}

	return results, summary
}

func compareFloatField(result *ComparisonResult, summary *Summary, name string, hVal, dVal float64, exactMatch, nearMatch *bool) {
	tol := tolerances[name]
	diff := math.Abs(hVal - dVal)

	if diff > 0 {
		*exactMatch = false
	}

	withinTol := diff <= tol
	if proportionalTolerance[name] {
		denom := math.Max(math.Abs(hVal), math.Abs(dVal))
		if denom < 1 {
			denom = 1
		}
		withinTol = diff/denom <= tol
	}
	if !withinTol {
		*nearMatch = true
	}

	updateFieldStats(summary, name, diff, withinTol)

	result.Fields[name] = FieldDiff{
		HydrateValue: fmt.Sprintf("%.6f", hVal),
		DBValue:      fmt.Sprintf("%.6f", dVal),
		Diff:         diff,
		WithinTol:    withinTol,
	}
}

func compareIntField(result *ComparisonResult, summary *Summary, name string, hVal, dVal int64, exactMatch, nearMatch *bool) {
	diff := math.Abs(float64(hVal - dVal))
	tol := tolerances[name]

	if diff > 0 {
		*exactMatch = false
	}

	withinTol := diff <= tol
	if !withinTol {
		*nearMatch = true
	}

	updateFieldStats(summary, name, diff, withinTol)

	result.Fields[name] = FieldDiff{
		HydrateValue: fmt.Sprintf("%d", hVal),
		DBValue:      fmt.Sprintf("%d", dVal),
		Diff:         diff,
		WithinTol:    withinTol,
	}
}

func comparePtrFloatField(result *ComparisonResult, summary *Summary, name string, hVal, dVal *float64, exactMatch, nearMatch *bool) {
	if hVal == nil && dVal == nil {
		result.Fields[name] = FieldDiff{
			HydrateValue: "nil",
			DBValue:      "nil",
			Diff:         0,
			WithinTol:    true,
		}
		updateFieldStats(summary, name, 0, true)
		return
	}

	if hVal == nil || dVal == nil {
		*exactMatch = false
		*nearMatch = true
		hStr := "nil"
		dStr := "nil"
		var hF, dF float64
		if hVal != nil {
			hStr = fmt.Sprintf("%.6f", *hVal)
			hF = *hVal
		}
		if dVal != nil {
			dStr = fmt.Sprintf("%.6f", *dVal)
			dF = *dVal
		}
		diff := math.Abs(hF - dF)
		updateFieldStats(summary, name, diff, false)
		result.Fields[name] = FieldDiff{
			HydrateValue: hStr,
			DBValue:      dStr,
			Diff:         diff,
			WithinTol:    false,
		}
		return
	}

	compareFloatField(result, summary, name, *hVal, *dVal, exactMatch, nearMatch)
}

func updateFieldStats(summary *Summary, name string, diff float64, withinTol bool) {
	fs, ok := summary.FieldStats[name]
	if !ok {
		fs = FieldSummary{}
	}

	fs.Compared++
	if withinTol {
		fs.WithinTol++
	}
	if diff > fs.MaxDiff {
		fs.MaxDiff = diff
	}
	// Running average
	fs.AvgDiff = (fs.AvgDiff*float64(fs.Compared-1) + diff) / float64(fs.Compared)

	summary.FieldStats[name] = fs
}

func printSummary(summary Summary, fields []string) {
	fmt.Println("\n=== COMPARISON SUMMARY ===")
	fmt.Printf("Hydrate bars: %d\n", summary.TotalHydrate)
	fmt.Printf("DB bars:      %d\n", summary.TotalDB)
	fmt.Printf("Exact matches: %d (%.1f%%)\n", summary.ExactMatches, pct(summary.ExactMatches, summary.TotalHydrate))
	fmt.Printf("Near matches:  %d (%.1f%%)\n", summary.NearMatches, pct(summary.NearMatches, summary.TotalHydrate))
	fmt.Printf("Missing in DB: %d (%.1f%%)\n", summary.MissingInDB, pct(summary.MissingInDB, summary.TotalHydrate))
	fmt.Printf("Extra in DB:   %d\n", summary.ExtraInDB)

	fmt.Println("\n=== FIELD-LEVEL STATS ===")
	for _, field := range fields {
		fs, ok := summary.FieldStats[field]
		if !ok || fs.Compared == 0 {
			continue
		}
		tol := tolerances[field]
		pct := float64(fs.WithinTol) / float64(fs.Compared) * 100
		fmt.Printf("%-25s tol=%-8v compared=%-5d within_tol=%-5d (%5.1f%%) max_diff=%.6f avg_diff=%.6f\n",
			field, tol, fs.Compared, fs.WithinTol, pct, fs.MaxDiff, fs.AvgDiff)
	}
}

func pct(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

func printDiffs(results []ComparisonResult, maxDiffs int) {
	fmt.Println("\n=== SAMPLE DIFFERENCES ===")
	count := 0
	for _, r := range results {
		if r.MatchStatus == "exact" {
			continue
		}
		if count >= maxDiffs {
			break
		}

		ts := time.UnixMilli(r.Timestamp).UTC().Format("2006-01-02 15:04:05")
		fmt.Printf("\n--- %s (%s) ---\n", ts, r.MatchStatus)

		for name, diff := range r.Fields {
			if !diff.WithinTol {
				fmt.Printf("  %-25s hydrate=%-20s db=%-20s diff=%.6f\n",
					name, diff.HydrateValue, diff.DBValue, diff.Diff)
			}
		}
		count++
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		_ = err
	}

	inputDir := os.Getenv("HYDRATE_INPUT_DIR")
	if inputDir == "" {
		inputDir = "./hydrate-output"
	}

	symbol := os.Getenv("HYDRATE_SYMBOL")
	if symbol == "" {
		symbol = "BTC/USDT:PERP"
	}

	exchange := os.Getenv("HYDRATE_EXCHANGE")
	if exchange == "" {
		exchange = "binance"
	}

	// Hydrate output uses different exchange naming (e.g., binance_futures vs binance)
	hydrateExchange := os.Getenv("HYDRATE_INPUT_EXCHANGE")
	if hydrateExchange == "" {
		// Default mapping: binance -> binance_futures
		if exchange == "binance" {
			hydrateExchange = "binance_futures"
		} else {
			hydrateExchange = exchange
		}
	}

	startStr := os.Getenv("HYDRATE_START")
	endStr := os.Getenv("HYDRATE_END")

	if startStr == "" || endStr == "" {
		log.Fatal("HYDRATE_START and HYDRATE_END environment variables required")
	}

	start, err := parseDateTime(startStr)
	if err != nil {
		log.Fatalf("Invalid start date: %v", err)
	}

	end, err := parseDateTime(endStr)
	if err != nil {
		log.Fatalf("Invalid end date: %v", err)
	}

	maxDiffs := 100
	if s := os.Getenv("HYDRATE_MAX_DIFFS"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &maxDiffs)
	}

	fmt.Printf("Comparing ob-hydrate vs stream data\n")
	fmt.Printf("Symbol:   %s\n", symbol)
	fmt.Printf("Exchange: %s (hydrate: %s)\n", exchange, hydrateExchange)
	fmt.Printf("Range:    %s to %s\n", start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	fmt.Printf("Input:    %s\n", inputDir)
	fieldSetName := os.Getenv("HYDRATE_FIELD_SET")
	if fieldSetName == "" {
		fieldSetName = "full"
	}
	fields, ok := fieldSets[fieldSetName]
	if !ok {
		log.Fatalf("Invalid HYDRATE_FIELD_SET %q (expected microstructure, trades, or full)", fieldSetName)
	}
	fmt.Printf("Fields:   %s\n", fieldSetName)

	// Load hydrate bars
	hydrateBars, err := loadHydrateBars(inputDir, hydrateExchange, symbol, start, end)
	if err != nil {
		log.Fatalf("Failed to load hydrate bars: %v", err)
	}
	fmt.Printf("Loaded %d hydrate bars from JSON\n", len(hydrateBars))

	// Connect to DB
	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		log.Fatal("PG_URL environment variable required")
	}

	database, err := db.New(pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	// Load DB bars
	dbBars, err := loadDBBars(database, exchange, symbol, start, end)
	if err != nil {
		log.Fatalf("Failed to load DB bars: %v", err)
	}
	fmt.Printf("Loaded %d bars from DB\n", len(dbBars))

	// Compare
	results, summary := compareBars(hydrateBars, dbBars, fields)

	// Print results
	printSummary(summary, fields)
	printDiffs(results, maxDiffs)
}
