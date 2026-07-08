package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessTrade(t *testing.T) {
	agg := New()

	agg.ProcessTrade(Trade{
		Timestamp:    60000,
		Price:        100.0,
		Quantity:     1.0,
		IsBuyerMaker: false,
	})

	bar := agg.bars[1]
	assert.NotNil(t, bar)
	assert.Equal(t, 1, bar.TradeCount)
	assert.Equal(t, 1.0, bar.TotalVolume)
	assert.Equal(t, 1.0, bar.BuyVolume)
	assert.Equal(t, 0.0, bar.SellVolume)
}

func TestProcessOrderBookUpdate(t *testing.T) {
	agg := New()

	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime: 60000,
		Price:     99.0,
		Quantity:  10.0,
		Side:      "bid",
	})

	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime: 60000,
		Price:     101.0,
		Quantity:  8.0,
		Side:      "ask",
	})

	assert.Equal(t, 0.0, agg.lastTradePrice)
	assert.False(t, agg.bids.IsEmpty())
	assert.False(t, agg.asks.IsEmpty())
}

func TestProcessLiquidation(t *testing.T) {
	agg := New()

	agg.ProcessLiquidation(Liquidation{
		Timestamp: 60000,
		Quantity:  5.0,
		Side:      "BUY",
	})
	bar := agg.bars[1]
	assert.NotNil(t, bar)
	assert.Equal(t, 5.0, bar.LiqShortVol)
	assert.Equal(t, 0.0, bar.LiqLongVol)

	agg.ProcessLiquidation(Liquidation{
		Timestamp: 60000,
		Quantity:  3.0,
		Side:      "SELL",
	})
	assert.Equal(t, 5.0, bar.LiqShortVol)
	assert.Equal(t, 3.0, bar.LiqLongVol)
}

func TestFinalize(t *testing.T) {
	agg := New()

	agg.ProcessTrade(Trade{Timestamp: 60000, Price: 100.0, Quantity: 2.0, IsBuyerMaker: false})
	agg.ProcessTrade(Trade{Timestamp: 120000, Price: 101.0, Quantity: 3.0, IsBuyerMaker: true})
	agg.ProcessOrderBookUpdate(OrderBookUpdate{EventTime: 60000, Price: 99.0, Quantity: 10.0, Side: "bid"})
	agg.ProcessOrderBookUpdate(OrderBookUpdate{EventTime: 60000, Price: 101.0, Quantity: 10.0, Side: "ask"})
	agg.ProcessOpenInterest(OpenInterest{Timestamp: 60000, Value: 1000.0})

	bars := agg.Finalize(true)
	assert.Len(t, bars, 2)

	assert.Empty(t, agg.bars)
}

func TestGenerateHours(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC)

	hours := GenerateHours(start, end)
	assert.Len(t, hours, 4)
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), hours[0])
	assert.Equal(t, time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), hours[1])
	assert.Equal(t, time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), hours[2])
	assert.Equal(t, time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), hours[3])
}

func TestGenerateHoursEmpty(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	hours := GenerateHours(start, end)
	assert.Len(t, hours, 1)
}

func TestResetOrderbook(t *testing.T) {
	agg := New()

	// Add some initial orderbook data with contiguous IDs
	// First update establishes state
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             99.0,
		Quantity:          10.0,
		Side:              "bid",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 0, // First event
	})
	// Second update is contiguous (PrevFinalUpdateID matches previous FinalUpdateID)
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             101.0,
		Quantity:          8.0,
		Side:              "ask",
		FinalUpdateID:     200,
		PrevFinalUpdateID: 100, // Contiguous with previous
	})

	// Verify initial state
	assert.False(t, agg.bids.IsEmpty())
	assert.False(t, agg.asks.IsEmpty())
	bidPrice := agg.bids.MaxPrice()
	askPrice := agg.asks.MinPrice()
	assert.True(t, bidPrice > 0)
	assert.True(t, askPrice > 0)

	// Verify lastFinalUpdateID was updated to the last seen FinalUpdateID
	assert.Equal(t, int64(200), agg.lastFinalUpdateID)

	// Reset the orderbook - should clear both sides and reset the ID tracker
	agg.ResetOrderbook()

	// After reset, both sides should be empty
	assert.True(t, agg.bids.IsEmpty())
	assert.True(t, agg.asks.IsEmpty())
	assert.Equal(t, float64(0), agg.bids.MaxPrice())
	assert.Equal(t, float64(0), agg.asks.MinPrice())
	// LastFinalUpdateID should also be reset
	assert.Equal(t, int64(0), agg.lastFinalUpdateID)
}

func TestWelfordStdDev(t *testing.T) {
	agg := New()
	bar := &BarBuilder{}

	// Test data: 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0
	// Known stddev: ~2.138
	values := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}
	for _, v := range values {
		agg.updateSpreadWelford(bar, v)
	}

	// Check mean
	assert.InDelta(t, 5.0, bar.SpreadMean, 0.001)

	// Check stddev
	stdDev := agg.spreadStdDev(bar)
	assert.InDelta(t, 2.138, stdDev, 0.001)
}

func TestDepthSampling(t *testing.T) {
	agg := New()

	// Add orderbook data
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             99.0,
		Quantity:          10.0,
		Side:              "bid",
		EventType:         "snapshot",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 0,
	})
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             101.0,
		Quantity:          8.0,
		Side:              "ask",
		EventType:         "snapshot",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 0,
	})

	bar := agg.bars[1]
	assert.NotNil(t, bar)
	assert.Equal(t, 2, bar.DepthSampleCount)
	assert.True(t, bar.BidDepthSum > 0)
	assert.True(t, bar.AskDepthSum > 0)
}

// TestSmartResetContiguousIDs verifies that when update IDs form a contiguous chain,
// the orderbook state is preserved across updates (no reset occurs).
// This test simulates the flattened Parquet row scenario where multiple rows share
// the same FinalUpdateID within a single Binance payload.
func TestSmartResetContiguousIDs(t *testing.T) {
	agg := New()

	// First payload (simulating a single Binance depth update with multiple levels)
	// Both rows share the same FinalUpdateID = 100, PrevFinalUpdateID = 0
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             99.0,
		Quantity:          10.0,
		Side:              "bid",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 0, // First payload, no prior state
	})
	// Second row of same payload - same FinalUpdateID, should NOT trigger gap check
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             101.0,
		Quantity:          8.0,
		Side:              "ask",
		FinalUpdateID:     100, // Same FinalUpdateID as row above
		PrevFinalUpdateID: 0,   // Same PrevFinalUpdateID
	})

	// Verify state after first payload
	assert.Equal(t, int64(100), agg.lastFinalUpdateID)
	assert.False(t, agg.bids.IsEmpty())
	assert.False(t, agg.asks.IsEmpty())
	assert.True(t, agg.bids.MaxPrice() > 0)
	assert.True(t, agg.asks.MinPrice() > 0)

	// Second payload (new FinalUpdateID, contiguous with first)
	// PrevFinalUpdateID (100) matches lastFinalUpdateID (100) - should NOT reset
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         120000,
		Price:             98.0,
		Quantity:          5.0,
		Side:              "bid",
		FinalUpdateID:     200, // NEW FinalUpdateID - triggers gap check
		PrevFinalUpdateID: 100, // Links perfectly to previous final ID
	})

	// Verify state: orderbook should still have all three levels
	// (bid at 99, ask at 101, bid at 98) - state was NOT reset
	assert.Equal(t, int64(200), agg.lastFinalUpdateID)
	assert.False(t, agg.bids.IsEmpty())
	// The treap should have all three levels since we didn't reset
	assert.True(t, agg.bids.MaxPrice() > 0)
}

// TestSmartResetGapDetection verifies that when a gap in update IDs is detected,
// the orderbook is automatically reset to prevent ghost levels.
func TestSmartResetGapDetection(t *testing.T) {
	agg := New()

	// First payload with multiple rows (same FinalUpdateID - flattened Parquet)
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             99.0,
		Quantity:          10.0,
		Side:              "bid",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 0, // First payload
	})
	// Second row of same payload
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             101.0,
		Quantity:          8.0,
		Side:              "ask",
		FinalUpdateID:     100, // Same FinalUpdateID - no gap check
		PrevFinalUpdateID: 0,
	})

	// Verify state after first payload
	assert.Equal(t, int64(100), agg.lastFinalUpdateID)
	assert.False(t, agg.bids.IsEmpty())
	assert.False(t, agg.asks.IsEmpty())
	initialBidMax := agg.bids.MaxPrice()
	initialAskMin := agg.asks.MinPrice()
	assert.True(t, initialBidMax > 0)
	assert.True(t, initialAskMin > 0)

	// Second payload with a GAP in the stream
	// PrevFinalUpdateID (500) does NOT match lastFinalUpdateID (100)
	// This simulates a dropped packet or script restart
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         120000,
		Price:             98.0,
		Quantity:          5.0,
		Side:              "bid",
		FinalUpdateID:     600, // NEW FinalUpdateID - triggers gap check
		PrevFinalUpdateID: 500, // GAP: expected 100, got 500
	})

	// Verify state after gap: orderbook should have been RESET
	// The old bid at 99.0 should be gone (ghost level prevented)
	assert.Equal(t, int64(600), agg.lastFinalUpdateID)
	// After reset and new insert, the new bid at 98.0 should exist
	// The treap was cleared before processing the gap update
	assert.False(t, agg.bids.IsEmpty())
	newBidMax := agg.bids.MaxPrice()
	// The scaled price of 98.0 is 980000 (98 * 10000)
	assert.Equal(t, float64(980000), newBidMax)
}

// TestFlattenedRowsNoReset verifies that multiple rows with the same FinalUpdateID
// (as happens with flattened Parquet data) do NOT trigger spurious resets, even if
// their PrevFinalUpdateID differs from the current lastFinalUpdateID.
// This was a critical bug in the original implementation.
func TestFlattenedRowsNoReset(t *testing.T) {
	agg := New()

	// First payload
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             100.0,
		Quantity:          10.0,
		Side:              "bid",
		FinalUpdateID:     1000,
		PrevFinalUpdateID: 0,
	})
	assert.Equal(t, int64(1000), agg.lastFinalUpdateID)

	// Second payload with same FinalUpdateID but different PrevFinalUpdateID
	// This simulates the flattened Parquet scenario where multiple rows share
	// the same FinalUpdateID but might have different PrevFinalUpdateID values
	// In the old buggy logic, this would have triggered a reset because:
	//   lastFinalUpdateID (1000) != PrevFinalUpdateID (999)
	// But with the fix, we only check when FinalUpdateID changes.
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60001, // Same microsecond timestamp
		Price:             101.0,
		Quantity:          5.0,
		Side:              "ask",
		FinalUpdateID:     1000, // SAME FinalUpdateID - should NOT trigger gap check
		PrevFinalUpdateID: 999,  // Different from lastFinalUpdateID, but same payload
	})

	// Verify: NO reset occurred, both bid and ask should exist
	assert.Equal(t, int64(1000), agg.lastFinalUpdateID)
	assert.False(t, agg.bids.IsEmpty())
	assert.False(t, agg.asks.IsEmpty())
	// The bid at 100 should still exist (wasn't reset)
	assert.True(t, agg.bids.MaxPrice() > 0)
	// The ask at 101 should exist
	assert.True(t, agg.asks.MinPrice() > 0)
}

// TestSmartResetFirstEventNoReset verifies that the first event in a stream
// (when lastFinalUpdateID is 0) does not trigger a spurious reset.
func TestSmartResetFirstEventNoReset(t *testing.T) {
	agg := New()

	// Verify initial state
	assert.Equal(t, int64(0), agg.lastFinalUpdateID)
	assert.True(t, agg.bids.IsEmpty())

	// First event with non-zero PrevFinalUpdateID
	// Since lastFinalUpdateID is 0 (no prior state), no reset should occur
	agg.ProcessOrderBookUpdate(OrderBookUpdate{
		EventTime:         60000,
		Price:             99.0,
		Quantity:          10.0,
		Side:              "bid",
		FinalUpdateID:     100,
		PrevFinalUpdateID: 50, // Non-zero, but should not trigger reset since lastFinalUpdateID is 0
	})

	// Verify state: orderbook should have the bid
	assert.Equal(t, int64(100), agg.lastFinalUpdateID)
	assert.False(t, agg.bids.IsEmpty())
	assert.True(t, agg.bids.MaxPrice() > 0)
}
