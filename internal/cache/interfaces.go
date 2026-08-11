package cache

import (
	"context"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"time"
)

type BarFetcher interface {
	FetchOHLCV(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error)
}

type PriceDB interface {
	QueryPriceBars(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error)
	QueryPriceBarsGrouped(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error)
	InsertPriceBars(ctx context.Context, exchange, symbol string, bars []types.Bar) error
}

type MarketDataCache interface {
	GetHistory(ctx context.Context, symbol string, tf timeframe.Timeframe, start, end time.Time) ([]types.Bar, error)
	Invalidate(symbol, tf string)
	GetMemoryStats() MemoryCacheStats
}

type PriceCacheInterface interface {
	MarketDataCache
	ResampleBars(bars []types.Bar, targetTf timeframe.Timeframe) []types.Bar
}
