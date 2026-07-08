package aggregator

// PARITY CONTRACT with streaming/aggregator.go:
// Both implementations MUST agree on:
//   - Liquidation: BUY → LiqShortVol, SELL → LiqLongVol, raw quantity (not notional)
//   - OpenInterest: last sample in minute (not mean)
//   - Depth: averaged over update samples (sum/count, not single snapshot)
//   - Spread: BPS, mean + stddev
//   - liqCovered: passed via Finalize(bool), not a struct field
//   - FundingRate: not accumulated here (handled at pipeline level for batch)
// Any change to aggregation semantics must be mirrored in both files.

import (
	"cmp"
	"math"
	"slices"
	"time"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/orderbook/treap"
)

// Trade represents a single trade
type Trade struct {
	Timestamp    int64
	Price        float64
	Quantity     float64
	IsBuyerMaker bool
	TradeCount   int // Number of individual trades aggregated (for parity with streaming)
}

// OrderBookUpdate represents an orderbook level update
type OrderBookUpdate struct {
	EventTime         int64
	Price             float64
	Quantity          float64
	Side              string // "bid" or "ask"
	EventType         string // e.g., "depthUpdate", "snapshot", ""
	FinalUpdateID     int64  // Binance's final_update_id for stream continuity
	PrevFinalUpdateID int64  // Binance's prev_final_update_id for gap detection
}

// OpenInterest represents an open interest update
type OpenInterest struct {
	Timestamp int64
	Value     float64
}

// Liquidation represents a liquidation event
type Liquidation struct {
	Timestamp int64
	Quantity  float64
	Side      string // "BUY" or "SELL"
}

// BarBuilder accumulates data for a single minute bar
type BarBuilder struct {
	TradeCount       int
	TotalVolume      float64
	TotalValue       float64 // For VWAP
	BuyVolume        float64
	SellVolume       float64
	OISamples        []float64
	LiqLongVol       float64
	LiqShortVol      float64
	BidDepthSum      float64
	AskDepthSum      float64
	DepthSampleCount int
	// Welford's algorithm for running stddev (parity with streaming aggregator)
	SpreadCount int
	SpreadMean  float64
	SpreadM2    float64
}

// Aggregator processes HFT data into 1-minute bars
type Aggregator struct {
	bars                 map[int64]*BarBuilder
	bids                 *treap.Treap
	asks                 *treap.Treap
	lastTradePrice       float64
	lastFinalUpdateID    int64    // Tracks last seen FinalUpdateID for stream continuity
	lastSnapshotUpdateID int64    // Tracks last snapshot to avoid duplicate resets
	prevOI               *float64 // Persists OI state across hour boundaries
	warm                 bool     // True after first snapshot or sufficient depth built
}

// New creates a new aggregator
func New() *Aggregator {
	return &Aggregator{
		bars: make(map[int64]*BarBuilder),
		bids: treap.New(),
		asks: treap.New(),
	}
}

// ResetOrderbook clears the orderbook treaps to prevent ghost levels from accumulating.
// This is called automatically when a gap in the stream is detected (via update ID
// discontinuity), or can be called manually on startup when no prior state exists.
func (a *Aggregator) ResetOrderbook() {
	a.bids = treap.New()
	a.asks = treap.New()
	a.lastFinalUpdateID = 0
	a.warm = false
}

// ProcessTrade processes a trade and updates the corresponding minute bar
func (a *Aggregator) ProcessTrade(trade Trade) {
	minute := trade.Timestamp / 60000 // Convert ms to minute

	bar, exists := a.bars[minute]
	if !exists {
		bar = &BarBuilder{}
		a.bars[minute] = bar
	}

	count := trade.TradeCount
	if count < 1 {
		count = 1
	}
	bar.TradeCount += count
	bar.TotalVolume += trade.Quantity
	bar.TotalValue += trade.Price * trade.Quantity
	a.lastTradePrice = trade.Price

	if trade.IsBuyerMaker {
		bar.SellVolume += trade.Quantity
	} else {
		bar.BuyVolume += trade.Quantity
	}
}

const priceScale = 10000.0

func snapPrice(p float64) float64 {
	return float64(int64(p * priceScale))
}

func (a *Aggregator) bestBid() float64 {
	price := a.bids.MaxPrice()
	if price == 0 {
		return 0
	}
	return price / priceScale
}

func (a *Aggregator) bestAsk() float64 {
	price := a.asks.MinPrice()
	if price == 0 {
		return 0
	}
	return price / priceScale
}

// ProcessOrderBookUpdate processes an orderbook update and samples spread.
// It automatically detects stream discontinuities via Binance's update IDs and
// resets the orderbook state when gaps are detected to prevent ghost levels.
// It also handles explicit snapshot events by clearing the orderbook.
func (a *Aggregator) ProcessOrderBookUpdate(update OrderBookUpdate) {
	// Handle explicit snapshot events: clear orderbook to prevent ghost levels
	if update.EventType == "snapshot" {
		if update.FinalUpdateID != a.lastSnapshotUpdateID {
			a.ResetOrderbook()
			a.lastSnapshotUpdateID = update.FinalUpdateID
		}
		a.warm = true
	} else if a.lastFinalUpdateID != 0 && update.FinalUpdateID != a.lastFinalUpdateID {
		// Smart Stream Integrity Check for deltas:
		// Because Parquet flattens arrays, multiple rows share the same FinalUpdateID.
		// We only check for gaps when we transition to a NEW update payload.
		// It's a new payload. Does its prev_final match our last_final?
		if update.PrevFinalUpdateID != a.lastFinalUpdateID {
			// GAP DETECTED (dropped packets or script restart).
			a.ResetOrderbook()
		}
	}
	// Always keep tracking the current ID
	a.lastFinalUpdateID = update.FinalUpdateID

	snapped := snapPrice(update.Price)

	// Update treap
	if update.Side == "bid" {
		a.bids.Insert(snapped, update.Quantity)
	} else {
		a.asks.Insert(snapped, update.Quantity)
	}

	// Skip spread/depth sampling until orderbook has been warmed up.
	// Without this, early bars have wildly inaccurate metrics because
	// the treap is only partially populated from the first few deltas.
	// We require meaningful depth on both sides before trusting spread/depth.
	if !a.warm {
		const minWarmDepth = 100.0
		if a.bids.SumDepth() >= minWarmDepth && a.asks.SumDepth() >= minWarmDepth {
			a.warm = true
		} else {
			return
		}
	}

	// Sample spread and depth on every update
	bestBid := a.bestBid()
	bestAsk := a.bestAsk()

	minute := update.EventTime / 60000
	bar, exists := a.bars[minute]
	if !exists {
		bar = &BarBuilder{}
		a.bars[minute] = bar
	}

	if bestBid > 0 && bestAsk > bestBid {
		// Compute spread in basis points to match streaming aggregator
		spreadBPS := (bestAsk - bestBid) / bestBid * 10000
		a.updateSpreadWelford(bar, spreadBPS)
	}

	bar.BidDepthSum += a.bids.SumDepth()
	bar.AskDepthSum += a.asks.SumDepth()
	bar.DepthSampleCount++
}

// updateSpreadWelford implements Welford's online algorithm for running stddev
// (parity with streaming aggregator)
func (a *Aggregator) updateSpreadWelford(b *BarBuilder, value float64) {
	b.SpreadCount++
	delta := value - b.SpreadMean
	b.SpreadMean += delta / float64(b.SpreadCount)
	delta2 := value - b.SpreadMean
	b.SpreadM2 += delta * delta2
}

// spreadStdDev computes the sample standard deviation from Welford's M2
func (a *Aggregator) spreadStdDev(b *BarBuilder) float64 {
	if b.SpreadCount < 2 {
		return 0
	}
	return math.Sqrt(b.SpreadM2 / float64(b.SpreadCount-1))
}

// ProcessOpenInterest processes an OI update
func (a *Aggregator) ProcessOpenInterest(oi OpenInterest) {
	minute := oi.Timestamp / 60000

	bar, exists := a.bars[minute]
	if !exists {
		bar = &BarBuilder{}
		a.bars[minute] = bar
	}
	bar.OISamples = append(bar.OISamples, oi.Value)
}

// ProcessLiquidation processes a liquidation event
func (a *Aggregator) ProcessLiquidation(liq Liquidation) {
	minute := liq.Timestamp / 60000

	bar, exists := a.bars[minute]
	if !exists {
		bar = &BarBuilder{}
		a.bars[minute] = bar
	}

	// BUY liquidation = short liquidation (shorts got liquidated, market buys)
	// SELL liquidation = long liquidation (longs got liquidated, market sells)
	if liq.Side == "BUY" {
		bar.LiqShortVol += liq.Quantity
	} else {
		bar.LiqLongVol += liq.Quantity
	}
}

// Finalize converts accumulated data to OrderbookBars.
// liqCovered indicates whether the liquidation data source was available for this period.
func (a *Aggregator) Finalize(liqCovered bool) []types.OrderbookBar {
	bars := make([]types.OrderbookBar, 0, len(a.bars))

	for ts, builder := range a.bars {
		bar := types.OrderbookBar{
			Timestamp:  (ts + 1) * 60000, // Convert minute to close time in ms
			TradeCount: builder.TradeCount,
			BuyVolume:  builder.BuyVolume,
			SellVolume: builder.SellVolume,
			LiqCovered: boolToInt(liqCovered),
		}

		// VWAP
		if builder.TotalVolume > 0 {
			bar.VWAP = builder.TotalValue / builder.TotalVolume
		}

		// Spread statistics (Welford's algorithm)
		if builder.SpreadCount > 0 {
			bar.AvgSpread = builder.SpreadMean
			bar.SpreadStdDev = a.spreadStdDev(builder)
		}

		// Last OI sample (change computed across sorted bars below)
		if len(builder.OISamples) > 0 {
			lastOI := builder.OISamples[len(builder.OISamples)-1]
			bar.OpenInterest = &lastOI
		}

		// Liquidations: when covered, always emit values (even zero) so
		// downstream can distinguish "true zero liquidations" from "missing data".
		if liqCovered {
			lv := builder.LiqLongVol
			sv := builder.LiqShortVol
			bar.LiqLongVol = &lv
			bar.LiqShortVol = &sv
		} else {
			if builder.LiqLongVol > 0 {
				bar.LiqLongVol = &builder.LiqLongVol
			}
			if builder.LiqShortVol > 0 {
				bar.LiqShortVol = &builder.LiqShortVol
			}
		}

		// Depth metrics (sum/count for parity with streaming aggregator)
		if builder.DepthSampleCount > 0 {
			avgBidDepth := builder.BidDepthSum / float64(builder.DepthSampleCount)
			avgAskDepth := builder.AskDepthSum / float64(builder.DepthSampleCount)
			totalDepth := avgBidDepth + avgAskDepth
			if totalDepth > 0 {
				bar.DepthRatio = avgAskDepth / totalDepth
				bar.DepthImbalance = (avgBidDepth - avgAskDepth) / totalDepth
			}
		}

		bars = append(bars, bar)
	}

	// Sort bars by timestamp and compute OI change across bars
	slices.SortFunc(bars, func(a, b types.OrderbookBar) int {
		return cmp.Compare(a.Timestamp, b.Timestamp)
	})

	// Use struct-level prevOI to persist state across hour boundaries
	for i := range bars {
		if bars[i].OpenInterest != nil {
			if a.prevOI != nil {
				delta := *bars[i].OpenInterest - *a.prevOI
				bars[i].OpenInterestChange = &delta
			}
			a.prevOI = bars[i].OpenInterest
		} else if a.prevOI != nil {
			oi := *a.prevOI
			bars[i].OpenInterest = &oi
			zero := 0.0
			bars[i].OpenInterestChange = &zero
		}
	}

	// Clear the bars after finalization
	a.bars = make(map[int64]*BarBuilder)

	return bars
}

// boolToInt converts a boolean to 1 or 0
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// GenerateHours generates a list of hours between start and end dates
func GenerateHours(start, end time.Time) []time.Time {
	var hours []time.Time
	current := start.Truncate(time.Hour)
	endTruncated := end.Truncate(time.Hour)

	for !current.After(endTruncated) {
		hours = append(hours, current)
		current = current.Add(time.Hour)
	}

	return hours
}
