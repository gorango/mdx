package cache

import (
	"context"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/db"
	"gorango/exchanges/internal/rest"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	MaxBackfillDays      = 30
	MaxBarsPerRequest    = 1000
	DefaultCacheBackfill = 24 * time.Hour
)

type PriceCache struct {
	exchangeID   string
	db           PriceDB
	memoryCache  *MemoryCache
	restClient   rest.Client
	logger       *slog.Logger
	requestGroup singleflight.Group
	overwrite    bool
}

func (c *PriceCache) SetOverwrite(v bool) {
	c.overwrite = v
}

var (
	globalCache     *PriceCache
	globalCacheOnce sync.Once
)

func NewPriceCache(exchangeID string, dbConn PriceDB, restClient rest.Client, logger *slog.Logger) *PriceCache {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cache, _ := NewMemoryCache(500)
	return &PriceCache{
		exchangeID:  exchangeID,
		db:          dbConn,
		memoryCache: cache,
		restClient:  restClient,
		logger:      logger,
	}
}

func CreateGlobalPriceCache(exchangeID, connString string) (*PriceCache, error) {
	var err error
	globalCacheOnce.Do(func() {
		dbConn, e := db.New(connString)
		if e != nil {
			err = e
			return
		}
		var rc rest.Client
		switch exchangeID {
		case "binance":
			rc = rest.NewBinance(rest.Config{})
		case "bybit":
			rc = rest.NewBybit(rest.Config{})
		}
		globalCache = NewPriceCache(exchangeID, dbConn, rc, nil)
	})
	return globalCache, err
}

func GetGlobalPriceCache() *PriceCache {
	return globalCache
}

func (c *PriceCache) GetHistory(ctx context.Context, symbol string, targetTf timeframe.Timeframe, start, end time.Time) ([]types.Bar, error) {
	effectiveStart, effectiveEnd := c.normalizeTimeRange(start, end)
	if effectiveStart.After(effectiveEnd) {
		return nil, fmt.Errorf("start must be before end")
	}

	canonical := symbols.NormalizeCanonical(symbol)
	c.logger.Debug("get history", "symbol", canonical, "tf", targetTf.ID, "start", effectiveStart, "end", effectiveEnd)

	if targetTf.Ms >= 24*60*60*1000 {
		return c.getHistoryHigherTimeframe(ctx, canonical, targetTf, effectiveStart, effectiveEnd)
	}

	if targetTf.ID == timeframe.TF1m.ID {
		return c.getHistory1m(ctx, canonical, effectiveStart, effectiveEnd)
	}

	if bars, ok := c.tryMemoryCache(canonical, targetTf.ID, effectiveStart, effectiveEnd); ok && len(bars) > 0 {
		return bars, nil
	}

	if c.db != nil && !c.overwrite {
		bars, err := c.db.QueryPriceBarsGrouped(ctx, c.exchangeID, canonical, effectiveStart, effectiveEnd, targetTf)
		if err == nil && len(bars) > 0 {
			tfMinutes := targetTf.Ms / (60 * 1000)
			if isDataComplete(bars, effectiveStart, effectiveEnd, tfMinutes) {
				c.logger.Debug("using DB grouped query", "symbol", canonical, "tf", targetTf.ID, "bars", len(bars))
				return bars, nil
			}
			c.logger.Debug("DB data incomplete, backfilling", "symbol", canonical, "tf", targetTf.ID, "bars", len(bars))
		}
		if err != nil {
			c.logger.Warn("DB grouped query failed, falling back to 1m fetch", "symbol", canonical, "err", err)
		}
	}

	baseBars, err := c.get1mBaseData(ctx, canonical, effectiveStart, effectiveEnd)
	if err != nil {
		return nil, err
	}

	projected := ResampleBars(baseBars, targetTf)
	return projected, nil
}

// isDataComplete checks that bars cover the full [start, end) range without
// significant gaps.  Requires end-coverage within 1 min, start-coverage within
// 5 min, and bar count ≥ 95 % of the expected count for the given resolution.
func isDataComplete(bars []types.Bar, start, end time.Time, tfMinutes int64) bool {
	if len(bars) == 0 {
		return false
	}
	barDur := time.Duration(tfMinutes) * time.Minute
	if len(bars) >= 2 {
		if d := bars[1].Time.Sub(bars[0].Time); d > 0 {
			barDur = d
		}
	}
	if bars[len(bars)-1].Time.Before(end.Add(-barDur)) {
		return false
	}
	if bars[0].Time.Add(-barDur).After(start.Add(5 * time.Minute)) {
		return false
	}
	totalMinutes := int64(end.Sub(start).Minutes())
	if totalMinutes <= 0 {
		return true
	}
	expected := int(totalMinutes / tfMinutes)
	// allow 5 % tolerance for natural gaps (exchange quirks, partial days)
	return expected <= 0 || len(bars) >= int(float64(expected)*0.95)
}

func (c *PriceCache) getHistory1m(ctx context.Context, canonical string, effectiveStart, effectiveEnd time.Time) ([]types.Bar, error) {
	chunks := ChunkDateRange(effectiveStart, effectiveEnd)
	today := time.Now().UTC().Format("2006-01-02")

	var cachedBars []types.Bar
	allChunksHit := true

	for _, chunk := range chunks {
		if chunk == today {
			allChunksHit = false
			break
		}
		chunkKey := ChunkCacheKey{
			Exchange:  c.exchangeID,
			Symbol:    canonical,
			Timeframe: timeframe.TF1m.ID,
			Date:      chunk,
		}
		if bars, ok := c.memoryCache.GetChunk(chunkKey); ok && len(bars) > 0 {
			c.logger.Debug("memory cache hit", "symbol", canonical, "tf", "1m", "chunk", chunk)
			cachedBars = append(cachedBars, bars...)
		} else {
			allChunksHit = false
			break
		}
	}

	if allChunksHit && len(cachedBars) > 0 {
		return filterBarsByTime(cachedBars, effectiveStart, effectiveEnd), nil
	}

	if c.db != nil && !c.overwrite {
		bars, err := c.db.QueryPriceBarsGrouped(ctx, c.exchangeID, canonical, effectiveStart, effectiveEnd, timeframe.TF1m)
		if err == nil && len(bars) > 0 {
			if isDataComplete(bars, effectiveStart, effectiveEnd, 1) {
				c.logger.Debug("using DB grouped query for 1m", "symbol", canonical, "bars", len(bars))
				return bars, nil
			}
			c.logger.Debug("DB data incomplete, backfilling", "symbol", canonical, "bars", len(bars))
		}
		if err != nil {
			c.logger.Warn("DB grouped query failed for 1m, falling back to REST", "symbol", canonical, "err", err)
		}
	}

	baseBars, err := c.get1mBaseData(ctx, canonical, effectiveStart, effectiveEnd)
	if err != nil {
		return nil, err
	}

	if len(chunks) == 1 && chunks[0] != today {
		chunkKey := ChunkCacheKey{
			Exchange:  c.exchangeID,
			Symbol:    canonical,
			Timeframe: timeframe.TF1m.ID,
			Date:      chunks[0],
		}
		c.memoryCache.SetChunk(chunkKey, baseBars)
	}

	return baseBars, nil
}

func (c *PriceCache) tryMemoryCache(canonical, tfID string, effectiveStart, effectiveEnd time.Time) ([]types.Bar, bool) {
	chunks := ChunkDateRange(effectiveStart, effectiveEnd)
	today := time.Now().UTC().Format("2006-01-02")
	var cachedBars []types.Bar
	allChunksHit := true

	for _, chunk := range chunks {
		if chunk == today {
			allChunksHit = false
			break
		}
		chunkKey := ChunkCacheKey{
			Exchange:  c.exchangeID,
			Symbol:    canonical,
			Timeframe: tfID,
			Date:      chunk,
		}
		if bars, ok := c.memoryCache.GetChunk(chunkKey); ok && len(bars) > 0 {
			c.logger.Debug("memory cache hit", "symbol", canonical, "tf", tfID, "chunk", chunk)
			cachedBars = append(cachedBars, bars...)
		} else {
			allChunksHit = false
			break
		}
	}

	if allChunksHit && len(cachedBars) > 0 {
		return filterBarsByTime(cachedBars, effectiveStart, effectiveEnd), true
	}
	return nil, false
}

func (c *PriceCache) getHistoryHigherTimeframe(ctx context.Context, symbol string, targetTf timeframe.Timeframe, start, end time.Time) ([]types.Bar, error) {
	cacheKey := CacheKey{
		Exchange:  c.exchangeID,
		Symbol:    symbol,
		Timeframe: targetTf.ID,
		StartMs:   start.UnixMilli(),
		EndMs:     end.UnixMilli(),
	}

	if bars, ok := c.memoryCache.GetHigherTfCache(cacheKey); ok && len(bars) > 0 {
		c.logger.Debug("memory cache hit (higher tf)", "symbol", symbol, "tf", targetTf.ID)
		return bars, nil
	}

	baseBars, err := c.get1mBaseData(ctx, symbol, start, end)
	if err != nil {
		return nil, err
	}

	projected := ResampleBars(baseBars, targetTf)

	c.memoryCache.SetHigherTfCache(cacheKey, projected)

	return projected, nil
}

func (c *PriceCache) GetHistoryWithTimeframe(ctx context.Context, symbol string, tfDef timeframe.Timeframe, start, end time.Time) ([]types.Bar, error) {
	return c.GetHistory(ctx, symbol, tfDef, start, end)
}

func (c *PriceCache) normalizeTimeRange(start, end time.Time) (time.Time, time.Time) {
	effectiveEnd := end
	if effectiveEnd.IsZero() {
		effectiveEnd = time.Now()
	}
	effectiveStart := start
	if effectiveStart.IsZero() {
		effectiveStart = effectiveEnd.Add(-DefaultCacheBackfill)
	}
	// Callers pass open-time ranges; shift to close time so
	// downstream code works with close time natively.
	return effectiveStart.UTC().Add(time.Minute), effectiveEnd.UTC().Add(time.Minute)
}

func (c *PriceCache) get1mBaseData(ctx context.Context, symbol string, start, end time.Time) ([]types.Bar, error) {
	flightKey := fmt.Sprintf("%s:%s:%d:%d", c.exchangeID, symbol, start.UnixMilli(), end.UnixMilli())

	val, err, _ := c.requestGroup.Do(flightKey, func() (interface{}, error) {
		return c.fetch1mData(ctx, symbol, start, end)
	})

	if err != nil {
		return nil, err
	}
	return val.([]types.Bar), nil
}

func (c *PriceCache) fetch1mData(ctx context.Context, symbol string, start, end time.Time) ([]types.Bar, error) {
	if start.After(end) {
		return nil, nil
	}

	var existingBars []types.Bar
	var ranges []struct{ Start, End time.Time }

	if c.overwrite {
		// Overwrite mode: re-fetch the full range, skip existing data
		ranges = []struct{ Start, End time.Time }{{Start: start, End: end}}
	} else {
		if c.db != nil {
			var dbErr error
			existingBars, dbErr = c.db.QueryPriceBars(ctx, c.exchangeID, symbol, start, end)
			if dbErr != nil {
				c.logger.Warn("db query error", "symbol", symbol, "err", dbErr)
			}
		}
		SortBars(existingBars)
		existingBars = filterBarsByTime(existingBars, start, end)
		ranges = findMissingDayRanges(getDaysWithData(existingBars), start, end)
	}

	if c.restClient != nil {
		for _, r := range ranges {
			fetchEnd := end
			if r.End.Before(end) {
				fetchEnd = r.End
			}
			monthStart := r.Start.UTC().Truncate(24 * time.Hour)
			for !monthStart.After(fetchEnd) {
				year, month, _ := monthStart.Date()
				bars, err := c.restClient.DownloadMonthlyZip(ctx, symbol, year, int(month))
				if err == nil && len(bars) > 0 {
					fetched := filterBarsByTime(bars, r.Start, fetchEnd)
					if len(fetched) > 0 {
						if c.db != nil {
							if c.overwrite {
								c.deletePriceBarsForRange(ctx, symbol, fetched[0].Time, fetched[len(fetched)-1].Time.Add(time.Minute))
							}
							if insertErr := c.db.InsertPriceBars(ctx, c.exchangeID, symbol, fetched); insertErr != nil {
								c.logger.Error("db insert error", "symbol", symbol, "err", insertErr)
							}
						}
						existingBars = append(existingBars, fetched...)
						SortBars(existingBars)
						existingBars = DeduplicateBars(existingBars)
					}
				} else {
					fetched, err := c.backfillConcurrently(ctx, symbol, monthStart, monthStart.AddDate(0, 1, 0))
					if err != nil {
						c.logger.Error("backfill error", "symbol", symbol, "err", err)
						break
					}
					if len(fetched) > 0 {
						if c.db != nil {
							if c.overwrite {
								c.deletePriceBarsForRange(ctx, symbol, fetched[0].Time, fetched[len(fetched)-1].Time.Add(time.Minute))
							}
							if insertErr := c.db.InsertPriceBars(ctx, c.exchangeID, symbol, fetched); insertErr != nil {
								c.logger.Error("db insert error", "symbol", symbol, "err", insertErr)
							}
						}
						existingBars = append(existingBars, fetched...)
						SortBars(existingBars)
						existingBars = DeduplicateBars(existingBars)
					}
				}
				monthStart = monthStart.AddDate(0, 1, 0)
			}
		}
	}

	return filterBarsByTime(existingBars, start, end), nil
}

func (c *PriceCache) deletePriceBarsForRange(ctx context.Context, symbol string, rangeStart, rangeEnd time.Time) {
	if d, ok := c.db.(*db.DB); ok {
		s := rangeStart
		e := rangeEnd
		if _, err := d.DeletePriceBars(ctx, c.exchangeID, symbol, &s, &e); err != nil {
			c.logger.Error("delete price bars", "symbol", symbol, "err", err)
		}
	}
}

func (c *PriceCache) backfillConcurrently(ctx context.Context, symbol string, gapStart, gapEnd time.Time) ([]types.Bar, error) {
	return c.backfillGap(ctx, symbol, gapStart, gapEnd)
}

func (c *PriceCache) backfillGap(ctx context.Context, symbol string, gapStart, gapEnd time.Time) ([]types.Bar, error) {
	var allFetched []types.Bar
	fetchStart := gapStart

	for fetchStart.Before(gapEnd) {
		resp, err := c.backfillWithRetry(ctx, symbol, fetchStart, MaxBarsPerRequest)
		if err != nil {
			if len(allFetched) > 0 {
				break
			}
			return nil, err
		}
		if len(resp) == 0 {
			break
		}

		if c.db != nil {
			if insertErr := c.db.InsertPriceBars(ctx, c.exchangeID, symbol, resp); insertErr != nil {
				c.logger.Error("db insert error", "symbol", symbol, "err", insertErr)
			}
		}

		allFetched = append(allFetched, resp...)

		lastBar := resp[len(resp)-1]
		if !lastBar.Time.Before(gapEnd) {
			break
		}
		if !lastBar.Time.After(fetchStart) || len(resp) < MaxBarsPerRequest {
			break
		}
		fetchStart = lastBar.Time
		time.Sleep(50 * time.Millisecond)
	}

	return allFetched, nil
}

func (c *PriceCache) backfillWithRetry(ctx context.Context, symbol string, since time.Time, limit int) ([]types.Bar, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * time.Second):
			}
		}

		resp, err := c.restClient.FetchOHLCV(ctx, symbol, timeframe.TF1m.ID, since.UnixMilli(), limit)
		if err == nil {
			return resp, nil
		}

		if isRateLimitError(err) {
			lastErr = err
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("rate limited after retries: %w", lastErr)
}

func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "429") || strings.Contains(errStr, "status code 429") ||
		strings.Contains(errStr, "rate limit") || strings.Contains(errStr, "Too Many Requests")
}

func (c *PriceCache) Invalidate(symbol, tf string) {
	canonical := symbols.NormalizeCanonical(symbol)
	c.memoryCache.Invalidate(c.exchangeID, canonical, tf)
}

func (c *PriceCache) GetMemoryStats() MemoryCacheStats {
	return c.memoryCache.Stats()
}

func (c *PriceCache) ResampleBars(bars []types.Bar, targetTf timeframe.Timeframe) []types.Bar {
	return ResampleBars(bars, targetTf)
}

func getDaysWithData(bars []types.Bar) map[string]bool {
	days := make(map[string]bool)
	for _, b := range bars {
		days[b.Time.UTC().Add(-time.Minute).Format("2006-01-02")] = true
	}
	return days
}

func findMissingDayRanges(daysWithData map[string]bool, start, end time.Time) []struct{ Start, End time.Time } {
	startDay := start.UTC().Truncate(24 * time.Hour)
	endDay := end.UTC().Truncate(24 * time.Hour)
	var ranges []struct{ Start, End time.Time }
	var rangeStart time.Time
	current := startDay
	for !current.After(endDay) {
		dayStr := current.Format("2006-01-02")
		if !daysWithData[dayStr] {
			if rangeStart.IsZero() {
				rangeStart = current
			}
		} else {
			if !rangeStart.IsZero() {
				ranges = append(ranges, struct{ Start, End time.Time }{rangeStart, current.Add(24 * time.Hour)})
				rangeStart = time.Time{}
			}
		}
		current = current.Add(24 * time.Hour)
	}
	if !rangeStart.IsZero() {
		ranges = append(ranges, struct{ Start, End time.Time }{rangeStart, endDay.Add(24 * time.Hour)})
	}
	return ranges
}

func filterBarsByTime(bars []types.Bar, start, end time.Time) []types.Bar {
	filtered := make([]types.Bar, 0, len(bars))
	for _, b := range bars {
		if !b.Time.Before(start) && b.Time.Before(end) {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

func (c *PriceCache) BackfillGapForTest(ctx context.Context, symbol string, gapStart, gapEnd time.Time) ([]types.Bar, error) {
	return c.backfillGap(ctx, symbol, gapStart, gapEnd)
}

func (c *PriceCache) BackfillConcurrentlyForTest(ctx context.Context, symbol string, gapStart, gapEnd time.Time) ([]types.Bar, error) {
	return c.backfillConcurrently(ctx, symbol, gapStart, gapEnd)
}
