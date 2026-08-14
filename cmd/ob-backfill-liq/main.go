package main

// Re-derive ONLY the liquidation columns (liq_long_vol, liq_short_vol,
// liq_covered) in orderbook_bars across a date range × all symbols, from the
// cryptohftdata liquidations parquets.
//
//   - parquet available  → aggregate the forceOrder events into 1m bars
//     (BUY → liq_short_vol, SELL → liq_long_vol, raw quantity; parity with
//     the hydration aggregator) and write liq_covered = 1 with the volumes.
//   - parquet 404 (vendor gap) → liq_covered = 0, volumes NULL ("unknown"),
//     matching the hydration's no-source encoding.
//
// Every other column is preserved.  This is the backfill companion to the
// aggregator change that emits NULL (not 0) when the liquidation source is
// missing, and fixes the historical rows that were previously stamped with
// zeroed / stream-derived liquidation values.
//
// RESUME: completed (symbol, hour) pairs are recorded in the
// liq_backfill_progress table, so a restarted run skips what is already done.
// Drop that table (or pass --reset) to force a full re-run.
//
// PROGRESS: per-symbol progress prints periodically, plus a global heartbeat
// (every 30s) with % complete and ETA.
//
// Usage:
//     go run ./cmd/ob-backfill-liq [--start 2025-07-01] [--end 2026-08-01] [--workers 8]
//     go run ./cmd/ob-backfill-liq --symbols "TRX/USDT:PERP,DOGE/USDT:PERP" --start 2026-08-01
//
// Requires PG_URL and CRYPTO_HFT_DATA env vars.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"

	"gorango/mdx/domain/symbols"
	"gorango/mdx/internal/db"
	"gorango/mdx/internal/orderbook/aggregator"
	"gorango/mdx/internal/orderbook/api"
	"gorango/mdx/internal/orderbook/parquet"
)

const (
	exchange   = "binance_futures"
	dbExchange = "binance"
	maxWorkers = 16
	// Marker flush + progress cadence: process in ~48-hour chunks per symbol.
	markerBatch = 48
	// Per-symbol progress log every N processed hours (~30-60s at ~0.5s/hour).
	progressEvery = 96
	// Global heartbeat interval.
	heartbeatEvery = 30 * time.Second
)

type symStats struct {
	total       int
	processed   int
	skipped     int
	withSource  int
	noSource    int
	errors      int
	barsWritten int64
}

func loadSymbols(flagSymbols string) ([]string, error) {
	if flagSymbols != "" {
		var out []string
		for _, s := range strings.Split(flagSymbols, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out, nil
	}
	path := "../config/symbols.yaml"
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s (or pass --symbols): %w", path, err)
	}
	var syms []string
	if err := yaml.Unmarshal(data, &syms); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return syms, nil
}

// backfillSymbol re-derives the liquidation columns for one symbol across all
// hours, skipping hours already recorded as done.  Hours are processed
// sequentially (one parquet download each); the aggregator reuses the
// hydration's liquidation aggregation semantics.
func backfillSymbol(
	ctx context.Context,
	database *db.DB,
	apiKey, sym string,
	hours []time.Time,
	done map[int64]bool,
	processedCount, barsCount, errsCount *atomic.Int64,
) symStats {
	var st symStats
	st.total = len(hours)
	client := api.NewCryptoHFTClient(apiKey)
	agg := aggregator.New()
	exchangeSym := symbols.CanonicalToExchange(sym, exchange)

	markerBuf := make([]int64, 0, markerBatch)
	flushMarkers := func() {
		if len(markerBuf) == 0 {
			return
		}
		if err := database.MarkLiqProgress(ctx, sym, markerBuf); err == nil {
			markerBuf = markerBuf[:0]
		}
	}

	for _, h := range hours {
		hourTs := h.UnixMilli()
		if done[hourTs] {
			st.skipped++
			continue
		}

		date := h.Format("2006-01-02")
		hourStr := fmt.Sprintf("%02d", h.Hour())
		hourStartMs := hourTs
		hourEndMs := hourStartMs + 3600000

		download, err := client.DownloadParquet(exchange, exchangeSym, date, hourStr, "liquidations")
		if err != nil {
			if api.IsNotAvailable(err) {
				// Vendor gap → "no liquidation data": NULL volumes, liq_covered=0.
				start := h.Truncate(time.Hour)
				end := start.Add(time.Hour)
				if _, derr := database.SetLiqUnknownRange(ctx, dbExchange, sym, start, end); derr != nil {
					st.errors++
					errsCount.Add(1)
					fmt.Fprintf(os.Stderr, "[%s] %s %s liq-range update err: %v\n", sym, date, hourStr, derr)
					continue
				}
				st.noSource++
			} else {
				st.errors++
				errsCount.Add(1)
				fmt.Fprintf(os.Stderr, "[%s] %s %s download err: %v\n", sym, date, hourStr, err)
				continue
			}
		} else {
			reader, perr := parquet.Open(download.FilePath)
			if perr != nil {
				_ = download.Cleanup()
				st.errors++
				errsCount.Add(1)
				fmt.Fprintf(os.Stderr, "[%s] %s %s parquet open err: %v\n", sym, date, hourStr, perr)
				continue
			}
			serr := reader.StreamLiquidations(func(liq parquet.Liquidation) error {
				if liq.TradeTime >= hourStartMs && liq.TradeTime < hourEndMs {
					agg.ProcessLiquidation(aggregator.Liquidation{
						Timestamp: liq.TradeTime,
						Quantity:  liq.LastFilledQuantity,
						Side:      liq.Side,
					})
				}
				return nil
			})
			_ = reader.Close()
			_ = download.Cleanup()
			if serr != nil {
				st.errors++
				errsCount.Add(1)
				fmt.Fprintf(os.Stderr, "[%s] %s %s stream err: %v\n", sym, date, hourStr, serr)
				continue
			}

			// A covered hour marks EVERY 1m bar liq_covered=1 — the source was
			// available, even where no liquidation occurred (volumes 0).  The
			// aggregator's Finalize only emits minutes that had events, so expand
			// to the full 60-bar grid and fill the aggregated volumes into the
			// minutes that had liquidations.
			liqByClose := map[int64][2]float64{}
			for _, b := range agg.Finalize(true) {
				liqByClose[b.Timestamp] = [2]float64{*b.LiqLongVol, *b.LiqShortVol}
			}
			ts := make([]int64, 0, 60)
			liqLong := make([]float64, 0, 60)
			liqShort := make([]float64, 0, 60)
			for m := 0; m < 60; m++ {
				closeTs := hourStartMs + (int64(m)+1)*60000
				v := liqByClose[closeTs]
				ts = append(ts, closeTs)
				liqLong = append(liqLong, v[0])
				liqShort = append(liqShort, v[1])
			}
			n, derr := database.SetLiqBars(ctx, dbExchange, sym, ts, liqLong, liqShort)
			if derr != nil {
				st.errors++
				errsCount.Add(1)
				fmt.Fprintf(os.Stderr, "[%s] %s %s liq-bars update err: %v\n", sym, date, hourStr, derr)
				continue
			}
			st.barsWritten += n
			st.withSource++
		}

		// Processed → record for resume + progress.
		markerBuf = append(markerBuf, hourTs)
		st.processed++
		processedCount.Add(1)
		if len(markerBuf) >= markerBatch {
			flushMarkers()
		}
		if st.processed > 0 && st.processed%progressEvery == 0 {
			fmt.Printf("[%s] %d/%d (%.0f%%) source=%d noSource=%d bars=%d errs=%d\n",
				sym, st.processed, st.total, float64(st.processed)/float64(st.total)*100,
				st.withSource, st.noSource, st.barsWritten, st.errors)
		}
	}
	flushMarkers()
	return st
}

func main() {
	_ = godotenv.Load()

	startStr := flag.String("start", "2025-07-01", "start date YYYY-MM-DD")
	endStr := flag.String("end", "2026-08-01", "end date YYYY-MM-DD (inclusive)")
	workers := flag.Int("workers", 8, "parallel symbol workers")
	symbolsFlag := flag.String("symbols", "", "comma-separated symbols (default: config/symbols.yaml)")
	reset := flag.Bool("reset", false, "clear resume-progress markers and re-process everything")
	flag.Parse()

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		fmt.Fprintln(os.Stderr, "PG_URL env var not set")
		os.Exit(1)
	}
	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "CRYPTO_HFT_DATA env var not set")
		os.Exit(1)
	}

	start, err := time.Parse("2006-01-02", *startStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --start %q: %v\n", *startStr, err)
		os.Exit(1)
	}
	end, err := time.Parse("2006-01-02", *endStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --end %q: %v\n", *endStr, err)
		os.Exit(1)
	}
	end = end.Add(24 * time.Hour).Truncate(24 * time.Hour) // include the end day
	syms, err := loadSymbols(*symbolsFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *workers < 1 || *workers > maxWorkers {
		*workers = 8
	}

	database, err := db.New(pgURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect db: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	ctx := context.Background()
	if err := database.InitLiqProgress(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "init progress table: %v\n", err)
		os.Exit(1)
	}
	if *reset {
		if err := database.ResetLiqProgress(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "reset progress: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Cleared resume-progress markers (full re-run)")
	}

	hours := aggregator.GenerateHours(start, end)
	totalHours := int64(len(syms)) * int64(len(hours))
	fmt.Printf("Backfilling liquidations for %d symbols x %d hours (%s -> %s), %d workers\n",
		len(syms), len(hours), start.Format("2006-01-02"), end.Format("2006-01-02"), *workers)

	var processedCount, barsCount, errsCount atomic.Int64
	begin := time.Now()

	// Global heartbeat: % complete + ETA.
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(heartbeatEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				p := processedCount.Load()
				pct := float64(p) / float64(totalHours) * 100
				eta := "n/a"
				if p > 0 {
					remain := time.Duration(float64(time.Since(begin)) / float64(p) * float64(totalHours-p))
					eta = remain.Round(time.Minute).String()
				}
				fmt.Printf("[progress] %.1f%% (%d/%d hours) bars=%d errs=%d elapsed=%s eta=%s\n",
					pct, p, totalHours, barsCount.Load(), errsCount.Load(),
					time.Since(begin).Round(time.Second), eta)
			}
		}
	}()

	sem := make(chan struct{}, *workers)
	var mu sync.Mutex
	var totals symStats
	var wg sync.WaitGroup
	for _, sym := range syms {
		wg.Add(1)
		sem <- struct{}{}
		go func(s string) {
			defer wg.Done()
			defer func() { <-sem }()
			doneSet, err := database.LiqProgressDone(ctx, s)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] load progress err: %v\n", s, err)
				return
			}
			st := backfillSymbol(ctx, database, apiKey, s, hours, doneSet, &processedCount, &barsCount, &errsCount)
			mu.Lock()
			totals.total += st.total
			totals.processed += st.processed
			totals.skipped += st.skipped
			totals.withSource += st.withSource
			totals.noSource += st.noSource
			totals.errors += st.errors
			totals.barsWritten += st.barsWritten
			mu.Unlock()
			fmt.Printf("[%s] done: %d processed, %d skipped, source=%d noSource=%d bars=%d errs=%d\n",
				s, st.processed, st.skipped, st.withSource, st.noSource, st.barsWritten, st.errors)
		}(sym)
	}
	wg.Wait()
	close(done)

	fmt.Printf("\nDone in %.0fs: %d symbols, %d hours (%d processed, %d skipped, %d with source, %d no-source), %d bars updated, %d errors\n",
		time.Since(begin).Seconds(), len(syms), totals.total, totals.processed, totals.skipped,
		totals.withSource, totals.noSource, totals.barsWritten, totals.errors)
}
