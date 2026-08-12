package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"gorango/mdx/domain/symbols"
	"gorango/mdx/internal/db"
	"gorango/mdx/internal/orderbook/aggregator"
	"gorango/mdx/internal/orderbook/api"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Config contains the pipeline configuration
type Config struct {
	Symbol          string
	StartDate       time.Time
	EndDate         time.Time
	Exchange        string
	APIKey          string
	ResumeMode      bool
	Overwrite       bool
	DryRun          bool
	OutputDir       string
	FundingCacheDir string
}

// Coordinator manages the hydration pipeline
type Coordinator struct {
	config Config
	db     *db.DB
	logger *slog.Logger
}

// NewCoordinator creates a new coordinator
func NewCoordinator(config Config, database *db.DB) *Coordinator {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	return &Coordinator{
		config: config,
		db:     database,
		logger: logger,
	}
}

// Run executes the hydration pipeline.
// Hours are processed sequentially per symbol because the orderbook treap
// must accumulate delta state across consecutive hours. Parallel workers
// would each carry their own treap, producing incorrect spread/depth data
// at hour boundaries.
func (c *Coordinator) Run(ctx context.Context) error {
	c.logger.Info("Fetching funding history",
		"symbol", c.config.Symbol,
	)

	var fundingPoints []api.FundingPoint

	// Check funding cache first
	if c.config.FundingCacheDir != "" {
		cachePath := filepath.Join(c.config.FundingCacheDir, c.config.Symbol+".json")
		if data, err := os.ReadFile(cachePath); err == nil {
			if err := json.Unmarshal(data, &fundingPoints); err == nil && len(fundingPoints) > 0 {
				c.logger.Info("Funding history loaded from cache",
					"symbol", c.config.Symbol,
					"points", len(fundingPoints),
				)
				goto afterFunding
			}
		}
	}

	{
		bc := api.NewBinanceClient()
		var err error
		fundingPoints, err = bc.FetchFundingHistory(
			symbols.CanonicalToExchange(c.config.Symbol, c.config.Exchange),
			c.config.StartDate.Add(-8*time.Hour).UnixMilli(),
			c.config.EndDate.Add(8*time.Hour).UnixMilli(),
		)
		if err != nil {
			return fmt.Errorf("fetch funding history: %w", err)
		}
		c.logger.Info("Fetched funding history",
			"symbol", c.config.Symbol,
			"points", len(fundingPoints),
		)

		// Write to cache
		if c.config.FundingCacheDir != "" {
			cachePath := filepath.Join(c.config.FundingCacheDir, c.config.Symbol+".json")
			if data, err := json.Marshal(fundingPoints); err == nil {
				_ = os.MkdirAll(c.config.FundingCacheDir, 0755)
				_ = os.WriteFile(cachePath, data, 0644)
			}
		}
	}

afterFunding:
	hours := aggregator.GenerateHours(c.config.StartDate, c.config.EndDate)
	c.logger.Info("Starting pipeline",
		"symbol", c.config.Symbol,
		"start", c.config.StartDate.Format("2006-01-02"),
		"end", c.config.EndDate.Format("2006-01-02"),
		"hours", len(hours),
	)

	// Skip to earliest data in DB to avoid probing the API for pre-listing hours
	if minTs, _, err := c.db.GetOrderbookBarRange(ctx, symbols.MapExchangeToDB(c.config.Exchange), c.config.Symbol); err == nil && minTs != nil {
		cut := minTs.Truncate(time.Hour)
		skipped := 0
		for len(hours) > 0 && hours[0].Before(cut) {
			hours = hours[1:]
			skipped++
		}
		if skipped > 0 {
			c.logger.Info("Skipped to first DB data",
				"symbol", c.config.Symbol,
				"skipped_hours", skipped,
				"first_db_hour", cut.Format("2006-01-02T15:04"),
			)
		}
	}

	if c.config.ResumeMode {
		hours = c.filterExisting(ctx, hours)
		c.logger.Info("Resume mode: filtered hours",
			"symbol", c.config.Symbol,
			"remaining", len(hours),
		)
	}

	if len(hours) == 0 {
		c.logger.Info("No hours to process",
			"symbol", c.config.Symbol,
		)
		return nil
	}

	processor := NewHourProcessor(c.config.APIKey, c.config.Symbol, c.config.Exchange, c.db, fundingPoints, c.config.Overwrite)
	processor.SetDryRun(c.config.DryRun, c.config.OutputDir)

	totalBars := 0
	processed := 0
	failed := 0
	skipped := 0
	firstData := time.Time{}
	lastData := time.Time{}
	hadAny404 := false
	hadExistingSkip := false

	for i := 0; i < len(hours); i++ {
		hour := hours[i]
		date := hour.Format("2006-01-02")
		hourNum := hour.Hour()

		result, err := processor.Process(ctx, date, hourNum)
		processed++

		if err != nil {
			failed++
			c.logger.Error("Hour failed",
				"symbol", c.config.Symbol,
				"date", date,
				"hour", hourNum,
				"error", err,
			)
		} else if result.Skipped {
			skipped++
			if result.DataNotAvailable {
				hadAny404 = true
				if totalBars == 0 {
					nextDay := hour.Add(24 * time.Hour).Truncate(24 * time.Hour)
					skipCount := 0
					for i+1 < len(hours) && hours[i+1].Before(nextDay) {
						i++
						skipCount++
					}
					skipped += skipCount
					processed += skipCount
					c.logger.Warn("Skipping ahead — no data yet",
						"symbol", c.config.Symbol,
						"skipped", skipped,
						"at", date,
						"skip_day", true,
					)
				}
			} else {
				hadExistingSkip = true
				c.logger.Debug("Hour skipped (already exists)",
					"symbol", c.config.Symbol,
					"date", date,
					"hour", hourNum,
				)
			}
		} else {
			if totalBars == 0 {
				firstData = hour
				c.logger.Info("First data found",
					"symbol", c.config.Symbol,
					"at", firstData.Format("2006-01-02"),
				)
			}
			lastData = hour
			totalBars += result.BarsSaved
			if processed%10 == 0 {
				c.logger.Info("Progress",
					"symbol", c.config.Symbol,
					"processed", fmt.Sprintf("%d/%d", processed, len(hours)),
					"bars", result.BarsSaved,
					"total", totalBars,
				)
			}
		}
	}

	firstStr := "never"
	lastStr := "never"
	if totalBars > 0 {
		firstStr = firstData.Format("2006-01-02")
		lastStr = lastData.Format("2006-01-02")
	}

	c.logger.Info("Pipeline complete",
		"symbol", c.config.Symbol,
		"processed", processed,
		"failed", failed,
		"skipped", skipped,
		"total_bars", totalBars,
		"first_data", firstStr,
		"last_data", lastStr,
	)
	if totalBars == 0 && hadAny404 && !hadExistingSkip {
		c.logger.Warn("No data found on API for symbol",
			"symbol", c.config.Symbol,
		)
	}

	return nil
}

func (c *Coordinator) filterExisting(ctx context.Context, hours []time.Time) []time.Time {
	var filtered []time.Time
	dbExchange := symbols.MapExchangeToDB(c.config.Exchange)
	for _, hour := range hours {
		exists, err := c.db.HourExists(ctx, dbExchange, c.config.Symbol, hour)
		if err != nil {
			c.logger.Warn("Failed to check hour exists",
				"symbol", c.config.Symbol,
				"hour", hour,
				"error", err,
			)
			filtered = append(filtered, hour)
		} else if !exists {
			filtered = append(filtered, hour)
		}
	}
	return filtered
}
