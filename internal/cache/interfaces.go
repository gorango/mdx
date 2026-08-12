package cache

import (
	"context"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"time"
)

type PriceDB interface {
	QueryPriceBars(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error)
	QueryPriceBarsGrouped(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error)
	InsertPriceBars(ctx context.Context, exchange, symbol string, bars []types.Bar) error
}
