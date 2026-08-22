package aggregator_test

// GOLDEN CROSS-PARITY (footprint scalars, migration 0006):
// An identical trade sequence fed through the batch aggregator
// (internal/orderbook/aggregator, vendor-parquet hydration path) and the live
// streaming aggregator (internal/orderbook/aggregator/streaming) MUST produce
// byte-identical footprint scalars per minute. Both paths share the
// levelhist package; this test fails loudly if that ever stops being true.
//
// The sequence deliberately includes degenerate rows (zero quantity, non-
// finite price): both aggregators count them in the scalar accumulators while
// levelhist skips them — the important property is that BOTH PATHS DO THE
// SAME THING, whatever the vendor stream contains.

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gorango/mdx/domain/types"
	aggregator "gorango/mdx/internal/orderbook/aggregator"
	streaming "gorango/mdx/internal/orderbook/aggregator/streaming"
)

type gTrade struct {
	ts         int64 // ms epoch, minute-bucketed like production feeds
	price      float64
	qty        float64
	buyerMaker bool
}

func goldenSequence() []gTrade {
	return []gTrade{
		{60000, 100.0, 2.0, false},
		{60001, 101.5, 1.0, true},
		{60002, 101.5, 4.0, true},      // POC level (total 5 at 101.5)
		{60003, 99.75, 0.25, false},    // low extreme
		{60004, 0.0, 0, false},         // degenerate: zero qty — skipped by levelhist
		{60005, math.NaN(), 1.0, true}, // degenerate: NaN price — skipped by levelhist
		{60500, 102.0, 3.0, false},
		// Second minute, one-sided selling (buy side must come out NULL):
		{120000, 101.0, 1.5, true},
		{121000, 100.5, 2.5, true},
	}
}

func TestGoldenLevelParityBatchVsStreaming(t *testing.T) {
	seq := goldenSequence()

	// Batch path.
	batch := aggregator.New()
	for _, tr := range seq {
		batch.ProcessTrade(aggregator.Trade{
			Timestamp:    tr.ts,
			Price:        tr.price,
			Quantity:     tr.qty,
			IsBuyerMaker: tr.buyerMaker,
			TradeCount:   1,
		})
	}
	batchBars := batch.Finalize(true)

	// Live path.
	live := streaming.New("TEST")
	for _, tr := range seq {
		require.NoError(t, live.ProcessEvent(types.Event{
			Type:      types.EventTypeTrade,
			Symbol:    "TEST",
			Timestamp: tr.ts,
			Data: types.Trade{
				Price:        tr.price,
				Quantity:     tr.qty,
				IsBuyerMaker: tr.buyerMaker,
				TradeCount:   1,
			},
		}))
	}
	liveBars := live.Finalize(true, math.MaxInt64)

	toMap := func(bars []types.OrderbookBar) map[int64]types.OrderbookBar {
		m := make(map[int64]types.OrderbookBar, len(bars))
		for _, b := range bars {
			m[b.Timestamp] = b
		}
		return m
	}
	bm := toMap(batchBars)
	lm := toMap(liveBars)
	require.Len(t, bm, len(lm))

	sameOpt := func(name string, a, b *float64) {
		if a == nil || b == nil {
			assert.Nil(t, a, name)
			assert.Nil(t, b, name)
			assert.Equal(t, a == nil, b == nil, "%s: NULL-ness must match", name)
			return
		}
		assert.True(t, *a == *b, "%s: %v vs %v", name, *a, *b)
	}

	for ts, bb := range bm {
		lb, ok := lm[ts]
		require.True(t, ok, "live missing bar %d", ts)

		// Legacy scalars stay in lockstep too.
		assert.Equal(t, bb.TradeCount, lb.TradeCount, "ts=%d", ts)
		assert.True(t, bb.BuyVolume == lb.BuyVolume && bb.SellVolume == lb.SellVolume, "ts=%d", ts)

		// Footprint consistency group.
		sameOpt("trade_open", bb.TradeOpen, lb.TradeOpen)
		sameOpt("trade_high", bb.TradeHigh, lb.TradeHigh)
		sameOpt("trade_low", bb.TradeLow, lb.TradeLow)
		sameOpt("trade_close", bb.TradeClose, lb.TradeClose)
		sameOpt("buy_vwap", bb.BuyVWAP, lb.BuyVWAP)
		sameOpt("sell_vwap", bb.SellVWAP, lb.SellVWAP)
		sameOpt("poc_price", bb.POCPrice, lb.POCPrice)
		sameOpt("poc_volume_share", bb.POCVolumeShare, lb.POCVolumeShare)
		sameOpt("buy_poc_price", bb.BuyPOCPrice, lb.BuyPOCPrice)
		sameOpt("sell_poc_price", bb.SellPOCPrice, lb.SellPOCPrice)
		sameOpt("trade_price_std", bb.TradePriceStd, lb.TradePriceStd)
		sameOpt("hi_band_buy_vol", bb.HiBandBuyVol, lb.HiBandBuyVol)
		sameOpt("hi_band_sell_vol", bb.HiBandSellVol, lb.HiBandSellVol)
		sameOpt("lo_band_buy_vol", bb.LoBandBuyVol, lb.LoBandBuyVol)
		sameOpt("lo_band_sell_vol", bb.LoBandSellVol, lb.LoBandSellVol)
	}

	// Sanity on the shared fixture itself (bars carry close time):
	// minute-1 POC is the 101.5 sell cluster; minute 2 is sell-only so the
	// buy-side fields must be NULL.
	first := bm[120_000]
	require.NotNil(t, first.POCPrice)
	assert.InDelta(t, 101.5, *first.POCPrice, 1e-12)
	second := bm[180_000]
	require.NotNil(t, second.SellVWAP)
	assert.Nil(t, second.BuyVWAP)
	assert.Nil(t, second.BuyPOCPrice)
}
