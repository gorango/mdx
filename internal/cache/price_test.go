package cache

import (
	"context"
	"fmt"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"gorango/mdx/internal/rest"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewPriceCacheNilLogger(t *testing.T) {
	cache := NewPriceCache("binance", nil, nil, nil)
	assert.NotNil(t, cache)
	assert.NotNil(t, cache.logger)
}

func TestNewPriceCacheWithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))
	cache := NewPriceCache("binance", nil, nil, logger)
	assert.NotNil(t, cache)
	assert.Equal(t, logger, cache.logger)
}

func TestNewPriceCacheWithRestClient(t *testing.T) {
	mockClient := &rest.MockRESTClient{}
	cache := NewPriceCache("binance", nil, mockClient, nil)
	assert.NotNil(t, cache)
	assert.Equal(t, mockClient, cache.restClient)
}

func TestPriceCacheGetHistoryInvalidRange(t *testing.T) {
	cache := NewPriceCache("binance", nil, nil, nil)

	end := time.Now().Add(-2 * time.Hour)
	start := time.Now()

	_, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, start, end)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start must be before end")
}

func TestPriceCacheGetHistoryDefaultRange(t *testing.T) {
	var called bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			called = true
			return []types.Bar{
				{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, time.Time{}, time.Time{})
	assert.NoError(t, err)
	assert.True(t, called)
	assert.NotNil(t, bars)
}

func TestPriceCacheGetHistoryMemoryCacheHit(t *testing.T) {
	now := time.Now().Add(-24 * time.Hour).Truncate(time.Hour).UTC()
	startTime := now
	endTime := now.Add(30 * time.Minute)

	cache := NewPriceCache("binance", nil, nil, nil)

	key := ChunkCacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Date:      startTime.Format("2006-01-02"),
	}
	cachedBars := []types.Bar{{Time: now, Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000}}
	cache.memoryCache.SetChunk(key, cachedBars)

	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			t.Fatal("REST client should not be called on cache hit")
			return nil, nil
		},
	}
	cache.restClient = mockClient

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, startTime, endTime)
	assert.NoError(t, err)
	assert.NotNil(t, bars)
}

func TestPriceCacheInvalidate(t *testing.T) {
	now := time.Now()

	cache := NewPriceCache("binance", nil, nil, nil)

	key := ChunkCacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Date:      now.Format("2006-01-02"),
	}
	cachedBars := []types.Bar{{Time: now, Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000}}
	cache.memoryCache.SetChunk(key, cachedBars)

	cache.Invalidate("BTC/USDT", "1m")

	stats := cache.GetMemoryStats()
	assert.Equal(t, 0, stats.Len)
}

func TestPriceCacheGetMemoryStats(t *testing.T) {
	cache := NewPriceCache("binance", nil, nil, nil)

	key := ChunkCacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Date:      time.Now().Format("2006-01-02"),
	}
	cachedBars := []types.Bar{{Time: time.Now()}}
	cache.memoryCache.SetChunk(key, cachedBars)

	stats := cache.GetMemoryStats()
	assert.Equal(t, 1, stats.Len)
}

func TestPriceCacheGetHistoryWithGaps(t *testing.T) {
	now := time.Now()
	startTime := now.Add(-10 * time.Minute)
	endTime := now

	var fetchCount int
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			fetchCount++
			return []types.Bar{
				{Time: time.UnixMilli(since), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
				{Time: time.UnixMilli(since).Add(time.Minute), Open: 103, High: 106, Low: 102, Close: 104, Volume: 1100},
			}, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, startTime, endTime)
	assert.NoError(t, err)
	assert.Greater(t, fetchCount, 0)
	assert.NotNil(t, bars)
}

func TestPriceCacheGetHistoryNoGapsReturnsExisting(t *testing.T) {
	now := time.Now().Add(-24 * time.Hour).Truncate(time.Hour).UTC()
	startTime := now
	endTime := now.Add(10 * time.Minute)

	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			t.Fatal("REST client should not be called when DB has complete data")
			return nil, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	key := ChunkCacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Date:      startTime.Format("2006-01-02"),
	}
	existingBars := []types.Bar{
		{Time: startTime.Add(time.Minute), Open: 100, High: 101, Low: 99, Close: 100, Volume: 1000},
		{Time: startTime.Add(2 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100, Volume: 1000},
	}
	cache.memoryCache.SetChunk(key, existingBars)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, startTime, endTime)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(bars))
}

func TestBackfillGap(t *testing.T) {
	now := time.Now()
	startTime := now.Add(-10 * time.Minute)
	endTime := now

	var fetchedRanges []struct {
		start time.Time
		end   time.Time
	}

	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			fetchedRanges = append(fetchedRanges, struct {
				start time.Time
				end   time.Time
			}{
				start: time.UnixMilli(since),
				end:   time.UnixMilli(since).Add(time.Duration(limit) * time.Minute),
			})
			return []types.Bar{
				{Time: time.UnixMilli(since), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
				{Time: time.UnixMilli(since).Add(time.Minute), Open: 103, High: 106, Low: 102, Close: 104, Volume: 1100},
			}, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	fetched, err := cache.BackfillGapForTest(context.Background(), "BTC/USDT", startTime, endTime)
	assert.NoError(t, err)
	assert.Greater(t, len(fetched), 0)
}

func TestBackfillConcurrentlyFetchesMultipleChunks(t *testing.T) {
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(time.Duration(MaxBarsPerRequest*2) * time.Minute)

	var mu sync.Mutex
	var callCount int

	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			mu.Lock()
			callCount++
			mu.Unlock()

			bars := make([]types.Bar, 0, limit)
			for i := 0; i < limit; i++ {
				openTime := time.UnixMilli(since).Add(time.Duration(i) * time.Minute)
				closeTime := openTime.Add(time.Minute)
				if openTime.Before(endTime) {
					bars = append(bars, types.Bar{
						Time:   closeTime,
						Open:   100,
						High:   105,
						Low:    99,
						Close:  103,
						Volume: 1000,
					})
				}
			}
			return bars, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	fetched, err := cache.BackfillConcurrentlyForTest(context.Background(), "BTC/USDT", startTime, endTime)
	assert.NoError(t, err)

	mu.Lock()
	calls := callCount
	mu.Unlock()

	assert.Equal(t, 2, calls, "should make 2 concurrent page calls")
	assert.Equal(t, MaxBarsPerRequest*2, len(fetched), "should return all bars for 2 pages")
}

func TestFilterBarsByTime_EndBoundary(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
		{Time: time.Date(2026, 3, 1, 0, 1, 0, 0, time.UTC), Open: 103, High: 106, Low: 102, Close: 104, Volume: 1100},
		{Time: time.Date(2026, 3, 1, 0, 2, 0, 0, time.UTC), Open: 104, High: 107, Low: 103, Close: 105, Volume: 1200},
	}
	// filterBarsByTime expects close-time ranges; [0:00,0:01) open → [0:01,0:02) close
	start := time.Date(2026, 3, 1, 0, 1, 0, 0, time.UTC)
	end := time.Date(2026, 3, 1, 0, 2, 0, 0, time.UTC)

	filtered := filterBarsByTime(bars, start, end)

	assert.Equal(t, 1, len(filtered), "bar at close=0:01 (covering [0:00,0:01)) should be included, bars at close=0:00 and 0:02 excluded")
	assert.Equal(t, time.Date(2026, 3, 1, 0, 1, 0, 0, time.UTC), filtered[0].Time)
}

func TestGetHistory_BackfillsFromQueryStart(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC)

	var fetched bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			fetched = true
			sinceTime := time.UnixMilli(since)
			bars := make([]types.Bar, 0, limit)
			for i := 0; i < limit; i++ {
				ts := sinceTime.Add(time.Duration(i) * time.Minute)
				if !ts.Before(endTime) {
					break
				}
				bars = append(bars, types.Bar{
					Time:   ts,
					Open:   100,
					High:   105,
					Low:    99,
					Close:  103,
					Volume: 1000,
				})
			}
			return bars, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1h, startTime, endTime)
	assert.NoError(t, err)
	assert.True(t, fetched, "REST client should be called since no DB data exists")
	assert.GreaterOrEqual(t, len(bars), 1, "should return projected 1h bars from fetched 1m data")
}

func TestGetHistory_ProjectsFetched1mBarsToRequestedTf(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

	var callCount int
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			callCount++
			bars := make([]types.Bar, 60)
			for i := 0; i < 60; i++ {
				closeTime := time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC)
				if closeTime.Before(endTime) || closeTime.Equal(endTime) {
					bars[i] = types.Bar{
						Time:   closeTime,
						Open:   float64(100 + i),
						High:   float64(101 + i),
						Low:    float64(99 + i),
						Close:  float64(100.5 + float64(i)),
						Volume: float64(1000 + i),
					}
				}
			}
			return bars, nil
		},
	}

	mockDB := &mockPriceDB{
		queryPriceBarsFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
			return []types.Bar{}, nil
		},
		insertPriceBarsFunc: func(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
			return nil
		},
	}

	cache := NewPriceCache("binance", mockDB, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1h, startTime, endTime)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
	assert.Equal(t, 1, len(bars), "60 1m bars should project to exactly 1 hourly bar")
	assert.Equal(t, time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), bars[0].Time)
}

func TestGetHistory_BackfillWithGaps(t *testing.T) {
	queryStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	queryEnd := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)

	var restCalled bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			restCalled = true
			sinceTime := time.UnixMilli(since)
			bars := make([]types.Bar, 0, limit)
			for i := 0; i < limit; i++ {
				ts := sinceTime.Add(time.Duration(i) * time.Minute)
				if !ts.Before(queryEnd) {
					break
				}
				bars = append(bars, types.Bar{
					Time:   ts,
					Open:   100,
					High:   105,
					Low:    99,
					Close:  103,
					Volume: 1000,
				})
			}
			return bars, nil
		},
	}

	mockDB := &mockPriceDB{
		queryPriceBarsFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
			return []types.Bar{
				{Time: time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
		queryPriceBarsGroupedFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
			return []types.Bar{}, nil
		},
		insertPriceBarsFunc: func(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
			return nil
		},
	}

	cache := NewPriceCache("binance", mockDB, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1h, queryStart, queryEnd)
	assert.NoError(t, err)
	assert.True(t, restCalled, "REST client must be called to fill gap when DB grouped returns empty")

	barTimes := make([]time.Time, len(bars))
	for i, b := range bars {
		barTimes[i] = b.Time
	}
	sort.Slice(barTimes, func(i, j int) bool { return barTimes[i].Before(barTimes[j]) })

	assert.Greater(t, len(barTimes), 1, "should return bars from both DB and REST")
}

func TestGetHistory_BackfillEvenIfLastDBBarAfterQueryEnd(t *testing.T) {
	queryStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	queryEnd := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)

	var restCalled bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			restCalled = true
			return []types.Bar{}, nil
		},
	}

	mockDB := &mockPriceDB{
		queryPriceBarsFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
			return []types.Bar{
				{Time: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
		queryPriceBarsGroupedFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
			return []types.Bar{}, nil
		},
		insertPriceBarsFunc: func(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
			return nil
		},
	}

	cache := NewPriceCache("binance", mockDB, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1h, queryStart, queryEnd)
	assert.NoError(t, err)
	assert.True(t, restCalled, "REST client SHOULD be called when DB grouped returns empty")
	assert.Equal(t, 0, len(bars), "no bars should be returned when REST returns empty")
}

type mockPriceDB struct {
	queryPriceBarsFunc        func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error)
	queryPriceBarsGroupedFunc func(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error)
	insertPriceBarsFunc       func(ctx context.Context, exchange, symbol string, bars []types.Bar) error
}

func (m *mockPriceDB) QueryPriceBars(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
	return m.queryPriceBarsFunc(ctx, exchange, symbol, start, end)
}

func (m *mockPriceDB) QueryPriceBarsGrouped(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
	if m.queryPriceBarsGroupedFunc != nil {
		return m.queryPriceBarsGroupedFunc(ctx, exchange, symbol, start, end, tf)
	}
	return m.queryPriceBarsFunc(ctx, exchange, symbol, start, end)
}

func (m *mockPriceDB) InsertPriceBars(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
	return m.insertPriceBarsFunc(ctx, exchange, symbol, bars)
}

func TestBackfillWithRetryOnRateLimit(t *testing.T) {
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(10 * time.Minute)

	attempt := 0
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			attempt++
			if attempt == 1 {
				return nil, fmt.Errorf("binance API error: status code 429")
			}
			return []types.Bar{
				{Time: startTime, Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
	}

	cache := NewPriceCache("binance", nil, mockClient, nil)

	fetched, err := cache.BackfillGapForTest(context.Background(), "BTC/USDT", startTime, endTime)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fetched))
	assert.Equal(t, 2, attempt, "should retry once after rate limit")
}

func TestGetHistory_UsesDBGroupedWhenAvailable(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

	var groupedCalled bool
	mockDB := &mockPriceDB{
		queryPriceBarsFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
			return []types.Bar{}, nil
		},
		queryPriceBarsGroupedFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
			groupedCalled = true
			return []types.Bar{
				{Time: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 5000},
			}, nil
		},
		insertPriceBarsFunc: func(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
			return nil
		},
	}

	var restCalled bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			restCalled = true
			return []types.Bar{}, nil
		},
	}

	cache := NewPriceCache("binance", mockDB, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1h, startTime, endTime)
	assert.NoError(t, err)
	assert.True(t, groupedCalled, "DB grouped query should be called")
	assert.False(t, restCalled, "REST client should NOT be called when DB returns data")
	assert.Equal(t, 1, len(bars), "should return 1 hourly bar from DB grouped query (close=1:00 covering [0:00,1:00))")
	assert.Equal(t, time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), bars[0].Time)
}

func TestGetHistory_1m_UsesDBGroupedWhenAvailable(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)

	var groupedCalled bool
	mockDB := &mockPriceDB{
		queryPriceBarsFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
			return []types.Bar{}, nil
		},
		queryPriceBarsGroupedFunc: func(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
			groupedCalled = true
			bars := make([]types.Bar, 30)
			for i := 0; i < 30; i++ {
				bars[i] = types.Bar{
					Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
					Open:   100,
					High:   105,
					Low:    99,
					Close:  103,
					Volume: 1000,
				}
			}
			return bars, nil
		},
		insertPriceBarsFunc: func(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
			return nil
		},
	}

	var restCalled bool
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			restCalled = true
			return []types.Bar{}, nil
		},
	}

	cache := NewPriceCache("binance", mockDB, mockClient, nil)

	bars, err := cache.GetHistory(context.Background(), "BTC/USDT", timeframe.TF1m, startTime, endTime)
	assert.NoError(t, err)
	assert.True(t, groupedCalled, "DB grouped query should be called for 1m")
	assert.False(t, restCalled, "REST client should NOT be called when DB returns data")
	assert.Equal(t, 30, len(bars), "should return 30 1m bars from DB grouped query")
}
