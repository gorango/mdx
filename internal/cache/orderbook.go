package cache

import (
	"context"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/db"
	"time"
)

type OrderbookCache struct {
	exchangeID string
	db         *db.DB
	timeframe  timeframe.Timeframe
}

func NewOrderbookCache(exchangeID string, dbConn *db.DB) *OrderbookCache {
	return &OrderbookCache{
		exchangeID: exchangeID,
		db:         dbConn,
		timeframe:  timeframe.TF1m,
	}
}

func (c *OrderbookCache) GetBars(ctx context.Context, symbol string, start, end time.Time) ([]types.OrderbookBar, error) {
	if c.db == nil {
		return nil, nil
	}
	canonical := symbols.NormalizeCanonical(symbol)
	return c.db.QueryOrderbookBars(ctx, c.exchangeID, canonical, start, end)
}

func (c *OrderbookCache) ResampleBars(bars []types.OrderbookBar, targetTf timeframe.Timeframe) []types.OrderbookBar {
	if targetTf.Ms <= c.timeframe.Ms {
		return bars
	}
	multiplier := targetTf.Ms / c.timeframe.Ms
	if multiplier <= 1 {
		return bars
	}

	resampled := make([]types.OrderbookBar, 0, (len(bars)+int(multiplier)-1)/int(multiplier))
	for i := 0; i < len(bars); i += int(multiplier) {
		end := i + int(multiplier)
		if end > len(bars) {
			end = len(bars)
		}
		chunk := bars[i:end]
		resampled = append(resampled, c.aggregateBars(chunk))
	}
	return resampled
}

func (c *OrderbookCache) aggregateBars(chunk []types.OrderbookBar) types.OrderbookBar {
	if len(chunk) == 0 {
		return types.OrderbookBar{}
	}

	first := chunk[0]
	last := chunk[len(chunk)-1]

	var (
		totalBuyVol     float64
		totalSellVol    float64
		totalTradeCount int
		sumVwapVol      float64
		totalSpread     float64
		totalDepthImb   float64
		totalDepthRatio float64
		totalLiqLong    float64
		totalLiqShort   float64
		maxLiqCovered   int
		count           float64
	)

	for _, b := range chunk {
		vol := b.BuyVolume + b.SellVolume
		totalBuyVol += b.BuyVolume
		totalSellVol += b.SellVolume
		totalTradeCount += b.TradeCount
		sumVwapVol += b.VWAP * vol
		totalSpread += b.AvgSpread
		totalDepthImb += b.DepthImbalance
		totalDepthRatio += b.DepthRatio
		if b.LiqLongVol != nil {
			totalLiqLong += *b.LiqLongVol
		}
		if b.LiqShortVol != nil {
			totalLiqShort += *b.LiqShortVol
		}
		if b.LiqCovered > maxLiqCovered {
			maxLiqCovered = b.LiqCovered
		}
		count++
	}

	totalVol := totalBuyVol + totalSellVol
	finalVwap := last.VWAP
	if totalVol > 0 {
		finalVwap = sumVwapVol / totalVol
	}

	liqLongPtr, liqShortPtr := new(float64), new(float64)
	*liqLongPtr, *liqShortPtr = totalLiqLong, totalLiqShort

	var oiChange, frChange float64
	if last.OpenInterest != nil && first.OpenInterest != nil {
		oiChange = *last.OpenInterest - *first.OpenInterest
	}
	if last.FundingRate != nil && first.FundingRate != nil {
		frChange = *last.FundingRate - *first.FundingRate
	}
	oiPtr, frPtr, oiChangePtr, frChangePtr := last.OpenInterest, last.FundingRate, &oiChange, &frChange

	return types.OrderbookBar{
		Timestamp:          last.Timestamp,
		VWAP:               finalVwap,
		TradeCount:         totalTradeCount,
		BuyVolume:          totalBuyVol,
		SellVolume:         totalSellVol,
		AvgSpread:          totalSpread / count,
		SpreadStdDev:       last.SpreadStdDev,
		DepthImbalance:     totalDepthImb / count,
		DepthRatio:         totalDepthRatio / count,
		OpenInterest:       oiPtr,
		OpenInterestChange: oiChangePtr,
		FundingRate:        frPtr,
		FundingRateChange:  frChangePtr,
		LiqLongVol:         liqLongPtr,
		LiqShortVol:        liqShortPtr,
		LiqCovered:         maxLiqCovered,
	}
}
