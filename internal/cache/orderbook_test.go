package cache

import (
	"context"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResampleBars1mTo5m(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	bars := make([]types.OrderbookBar, 5)
	for i := range bars {
		bars[i] = types.OrderbookBar{
			Timestamp:      int64(i * 60000),
			VWAP:           100.0 + float64(i),
			TradeCount:     i + 1,
			BuyVolume:      float64((i + 1) * 10),
			SellVolume:     float64((i + 1) * 8),
			AvgSpread:      0.5,
			DepthImbalance: 0.1,
			DepthRatio:     0.5,
		}
	}

	result := cache.ResampleBars(bars, timeframe.TF5m)

	assert.Len(t, result, 1)
	assert.Equal(t, int64(4*60000), result[0].Timestamp)
	assert.Equal(t, 15, result[0].TradeCount)
	assert.Equal(t, float64(150), result[0].BuyVolume)
	assert.Equal(t, float64(120), result[0].SellVolume)
	assert.InDelta(t, 102.67, result[0].VWAP, 0.01)
}

func TestResampleBarsAlready1m(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	bars := []types.OrderbookBar{
		{Timestamp: 60000, VWAP: 100},
	}

	result := cache.ResampleBars(bars, timeframe.TF1m)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(60000), result[0].Timestamp)
}

func TestResampleBarsPartialGroup(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	bars := make([]types.OrderbookBar, 3)
	for i := range bars {
		bars[i] = types.OrderbookBar{
			Timestamp:      int64(i * 60000),
			VWAP:           100.0,
			TradeCount:     i + 1,
			BuyVolume:      10.0,
			SellVolume:     8.0,
			AvgSpread:      0.5,
			DepthImbalance: 0.1,
			DepthRatio:     0.5,
		}
	}

	result := cache.ResampleBars(bars, timeframe.TF5m)
	assert.Len(t, result, 1)
}

func TestResampleBarsEmpty(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	result := cache.ResampleBars([]types.OrderbookBar{}, timeframe.TF5m)
	assert.Len(t, result, 0)
}

func TestResampleBarsLowerTarget(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	bars := []types.OrderbookBar{
		{Timestamp: 60000, VWAP: 100},
	}

	result := cache.ResampleBars(bars, timeframe.TF1m)
	assert.Len(t, result, 1)
}

func TestAggregateBars(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	chunk := []types.OrderbookBar{
		{
			Timestamp:      60000,
			VWAP:           100.0,
			TradeCount:     10,
			BuyVolume:      100.0,
			SellVolume:     80.0,
			AvgSpread:      0.0005,
			SpreadStdDev:   0.1,
			DepthImbalance: 0.2,
			DepthRatio:     0.4,
		},
		{
			Timestamp:      120000,
			VWAP:           101.0,
			TradeCount:     5,
			BuyVolume:      50.0,
			SellVolume:     40.0,
			AvgSpread:      0.0006,
			SpreadStdDev:   0.1,
			DepthImbalance: 0.3,
			DepthRatio:     0.5,
		},
	}

	result := cache.aggregateBars(chunk)
	assert.Equal(t, int64(120000), result.Timestamp)
	assert.Equal(t, 15, result.TradeCount)
	assert.Equal(t, 150.0, result.BuyVolume)
	assert.Equal(t, 120.0, result.SellVolume)
	assert.InDelta(t, 0.00055, result.AvgSpread, 0.00001)
	assert.Equal(t, 0.25, result.DepthImbalance)
	assert.Equal(t, 0.45, result.DepthRatio)
	assert.InDelta(t, (100.0*180.0+101.0*90.0)/270.0, result.VWAP, 0.01)
}

func TestAggregateBarsEmpty(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	result := cache.aggregateBars([]types.OrderbookBar{})
	assert.Equal(t, types.OrderbookBar{}, result)
}

func TestAggregateBarsLiquidationsSummed(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	liqLong1, liqLong2 := 100.0, 200.0
	liqShort1, liqShort2 := 50.0, 150.0

	chunk := []types.OrderbookBar{
		{
			Timestamp:   60000,
			VWAP:        100.0,
			TradeCount:  10,
			BuyVolume:   100.0,
			SellVolume:  80.0,
			LiqLongVol:  &liqLong1,
			LiqShortVol: &liqShort1,
			LiqCovered:  5,
		},
		{
			Timestamp:   120000,
			VWAP:        101.0,
			TradeCount:  5,
			BuyVolume:   50.0,
			SellVolume:  40.0,
			LiqLongVol:  &liqLong2,
			LiqShortVol: &liqShort2,
			LiqCovered:  10,
		},
	}

	result := cache.aggregateBars(chunk)
	assert.NotNil(t, result.LiqLongVol)
	assert.NotNil(t, result.LiqShortVol)
	assert.Equal(t, 300.0, *result.LiqLongVol)
	assert.Equal(t, 200.0, *result.LiqShortVol)
	assert.Equal(t, 10, result.LiqCovered)
}

func TestAggregateBarsOpenInterestChange(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		timeframe:  timeframe.TF1m,
	}

	oi1, oi2 := 1000000.0, 1100000.0
	fr1, fr2 := 0.0001, 0.00015

	chunk := []types.OrderbookBar{
		{
			Timestamp:    60000,
			VWAP:         100.0,
			BuyVolume:    100.0,
			SellVolume:   80.0,
			OpenInterest: &oi1,
			FundingRate:  &fr1,
		},
		{
			Timestamp:    120000,
			VWAP:         101.0,
			BuyVolume:    50.0,
			SellVolume:   40.0,
			OpenInterest: &oi2,
			FundingRate:  &fr2,
		},
	}

	result := cache.aggregateBars(chunk)
	assert.NotNil(t, result.OpenInterestChange)
	assert.NotNil(t, result.FundingRateChange)
	assert.Equal(t, 100000.0, *result.OpenInterestChange)
	assert.InDelta(t, 0.00005, *result.FundingRateChange, 0.000001)
}

func TestNewOrderbookCache(t *testing.T) {
	cache := NewOrderbookCache("binance", nil)
	assert.NotNil(t, cache)
	assert.Equal(t, "binance", cache.exchangeID)
	assert.Equal(t, timeframe.TF1m, cache.timeframe)
}

func TestGetBarsNilDB(t *testing.T) {
	cache := &OrderbookCache{
		exchangeID: "binance_futures",
		db:         nil,
	}

	result, err := cache.GetBars(context.TODO(), "BTC/USDT:PERP", time.Now().Add(-time.Hour), time.Now())
	assert.NoError(t, err)
	assert.Nil(t, result)
}

var _ = time.Now
