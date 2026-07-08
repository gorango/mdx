package streaming

// PARITY CONTRACT with ../aggregator.go (batch aggregator):
// Both implementations MUST agree on:
//   - Liquidation: BUY → LiqShortVol, SELL → LiqLongVol, raw quantity (not notional)
//   - OpenInterest: last sample in minute (not mean)
//   - Depth: averaged over update samples (sum/count, not single snapshot)
//   - Spread: BPS, mean + stddev (Welford's algorithm)
//   - liqCovered: passed via Finalize(bool), not a struct field
//   - FundingRate: accumulated in BarBuilder, last sample used
// Any change to aggregation semantics must be mirrored in both files.

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sync"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/orderbook/treap"
)

// Aggregator processes real-time streaming events into 1-minute bars
type Aggregator struct {
	symbol      string
	bids        *treap.Treap // price (scaled) -> quantity
	asks        *treap.Treap
	barMath     *BarMath
	mu          sync.RWMutex
	bars        map[int64]*BarBuilder // Use a map for out-of-order tolerance
	prevOI      *float64
	prevFunding *float64
}

// BarMath contains common bar calculation functions
type BarMath struct{}

func (b *BarMath) VWAP(totalValue, totalVolume float64) float64 {
	if totalVolume == 0 {
		return 0
	}
	return totalValue / totalVolume
}

func (b *BarMath) SpreadBPS(bestBid, bestAsk float64) float64 {
	if bestBid == 0 || bestAsk <= bestBid {
		return 0
	}
	return (bestAsk - bestBid) / bestBid * 10000
}

// BarBuilder accumulates data for a single minute bar
type BarBuilder struct {
	Timestamp          int64
	TradeCount         int
	TotalVolume        float64
	TotalValue         float64
	BuyVolume          float64
	SellVolume         float64
	OISamples          []float64
	FundingRateSamples []float64
	LiqLongVol         float64
	LiqShortVol        float64
	BidDepthSum        float64
	AskDepthSum        float64
	DepthSampleCount   int
	SpreadCount        int
	SpreadMean         float64
	SpreadM2           float64
}

// New creates a new streaming aggregator
func New(symbol string) *Aggregator {
	return &Aggregator{
		symbol:  symbol,
		bids:    treap.New(),
		asks:    treap.New(),
		barMath: &BarMath{},
		bars:    make(map[int64]*BarBuilder),
	}
}

// ProcessEvent handles incoming streaming events
func (a *Aggregator) ProcessEvent(event types.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Determine which minute this event belongs to (deterministic bucketing)
	eventMinute := event.Timestamp / 60000

	// Get or create the bar for this minute
	bar, exists := a.bars[eventMinute]
	if !exists {
		bar = &BarBuilder{Timestamp: eventMinute}
		a.bars[eventMinute] = bar
	}

	switch event.Type {
	case types.EventTypeTrade:
		trade, ok := event.Data.(types.Trade)
		if !ok {
			return fmt.Errorf("invalid trade data type")
		}
		a.processTrade(bar, trade)

	case types.EventTypeOrderbookUpdate:
		update, ok := event.Data.(types.OrderbookUpdate)
		if !ok {
			return fmt.Errorf("invalid orderbook update data type")
		}
		a.processOrderBookUpdate(bar, update)

	case types.EventTypeLiquidation:
		liq, ok := event.Data.(types.Liquidation)
		if !ok {
			return fmt.Errorf("invalid liquidation data type")
		}
		a.processLiquidation(bar, liq)

	case types.EventTypeFundingRate:
		fr, ok := event.Data.(types.FundingRate)
		if !ok {
			return fmt.Errorf("invalid funding rate data type")
		}
		a.processFundingRate(bar, fr)

	case types.EventTypeOpenInterest:
		oi, ok := event.Data.(types.OpenInterest)
		if !ok {
			return fmt.Errorf("invalid open interest data type")
		}
		a.processOpenInterest(bar, oi)

	default:
		return fmt.Errorf("unknown event type: %v", event.Type)
	}

	return nil
}

func (a *Aggregator) processTrade(b *BarBuilder, trade types.Trade) {
	count := trade.TradeCount
	if count < 1 {
		count = 1
	}
	b.TradeCount += count
	b.TotalVolume += trade.Quantity
	b.TotalValue += trade.Price * trade.Quantity

	if trade.IsBuyerMaker {
		b.SellVolume += trade.Quantity
	} else {
		b.BuyVolume += trade.Quantity
	}
}

// ResetDepth clears the orderbook treap state.
// Should be called after a WebSocket reconnect to discard stale levels.
func (a *Aggregator) ResetDepth() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bids = treap.New()
	a.asks = treap.New()
}

func (a *Aggregator) processOrderBookUpdate(b *BarBuilder, update types.OrderbookUpdate) {
	price := float64(int64(update.Price * 10000)) // Scale to avoid float issues

	if update.Side == "bid" {
		a.bids.Insert(price, update.Quantity)
	} else {
		a.asks.Insert(price, update.Quantity)
	}

	bestBid := a.bestBid()
	bestAsk := a.bestAsk()
	if bestBid > 0 && bestAsk > bestBid {
		spread := a.barMath.SpreadBPS(bestBid, bestAsk)
		b.updateSpreadWelford(spread)
	}

	b.BidDepthSum += a.bids.SumDepth()
	b.AskDepthSum += a.asks.SumDepth()
	b.DepthSampleCount++
}

func (a *Aggregator) bestBid() float64 {
	price := a.bids.MaxPrice()
	if price == 0 {
		return 0
	}
	return price / 10000
}

func (a *Aggregator) bestAsk() float64 {
	price := a.asks.MinPrice()
	if price == 0 {
		return 0
	}
	return price / 10000
}

func (a *Aggregator) processLiquidation(b *BarBuilder, liq types.Liquidation) {
	// BUY liquidation = short liquidation (shorts got liquidated, market buys)
	// SELL liquidation = long liquidation (longs got liquidated, market sells)
	if liq.Side == "BUY" {
		b.LiqShortVol += liq.Quantity
	} else {
		b.LiqLongVol += liq.Quantity
	}
}

func (a *Aggregator) processFundingRate(b *BarBuilder, fr types.FundingRate) {
	b.FundingRateSamples = append(b.FundingRateSamples, fr.Rate)
}

func (a *Aggregator) processOpenInterest(b *BarBuilder, oi types.OpenInterest) {
	b.OISamples = append(b.OISamples, oi.Value)
}

func (b *BarBuilder) updateSpreadWelford(value float64) {
	b.SpreadCount++
	delta := value - b.SpreadMean
	b.SpreadMean += delta / float64(b.SpreadCount)
	delta2 := value - b.SpreadMean
	b.SpreadM2 += delta * delta2
}

func (b *BarBuilder) spreadStdDev() float64 {
	if b.SpreadCount < 2 {
		return 0
	}
	return math.Sqrt(b.SpreadM2 / float64(b.SpreadCount-1))
}

// hasData returns true if the bar has accumulated any data
func (b *BarBuilder) hasData() bool {
	if b == nil {
		return false
	}
	return b.TradeCount > 0 ||
		b.SpreadCount > 0 ||
		b.DepthSampleCount > 0 ||
		len(b.OISamples) > 0 ||
		len(b.FundingRateSamples) > 0 ||
		b.LiqLongVol > 0 ||
		b.LiqShortVol > 0
}

// Finalize returns bars up to flushUpToMinute and removes them from the aggregator.
// liqCovered indicates whether the liquidation data source was available for this period.
// Use flushUpToMinute = math.MaxInt64 to flush all bars (for shutdown).
func (a *Aggregator) Finalize(liqCovered bool, flushUpToMinute int64) []types.OrderbookBar {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Collect minutes to flush (sorted to ensure prevOI/prevFunding calculate correctly)
	var minutes []int64
	for min := range a.bars {
		if min <= flushUpToMinute {
			minutes = append(minutes, min)
		}
	}

	if len(minutes) == 0 {
		return nil
	}

	slices.SortFunc(minutes, func(a, b int64) int {
		return cmp.Compare(a, b)
	})

	var result []types.OrderbookBar
	for _, min := range minutes {
		barBuilder := a.bars[min]
		if barBuilder.hasData() {
			result = append(result, a.buildBar(barBuilder, liqCovered))
		}
		delete(a.bars, min)
	}

	return result
}

func (a *Aggregator) buildBar(b *BarBuilder, liqCovered bool) types.OrderbookBar {
	vwap := a.barMath.VWAP(b.TotalValue, b.TotalVolume)
	avgSpread := b.SpreadMean
	spreadStdDev := b.spreadStdDev()

	if b.TradeCount > 0 && b.DepthSampleCount == 0 {
		fmt.Printf("[WARN] %s minute %d: %d trades but zero depth samples — possible depth connection issue\n",
			a.symbol, b.Timestamp, b.TradeCount)
	}

	var depthImbalance, depthRatio float64
	if b.DepthSampleCount > 0 {
		avgBidDepth := b.BidDepthSum / float64(b.DepthSampleCount)
		avgAskDepth := b.AskDepthSum / float64(b.DepthSampleCount)
		totalDepth := avgBidDepth + avgAskDepth
		if totalDepth > 0 {
			depthImbalance = (avgBidDepth - avgAskDepth) / totalDepth
			depthRatio = avgAskDepth / totalDepth
		}
	}

	var oiValue, oiChange, fundingRate, fundingRateChange *float64

	if len(b.OISamples) > 0 {
		last := b.OISamples[len(b.OISamples)-1]
		oiValue = &last
		if a.prevOI != nil {
			delta := last - *a.prevOI
			oiChange = &delta
		}
	}

	if len(b.FundingRateSamples) > 0 {
		fr := b.FundingRateSamples[len(b.FundingRateSamples)-1]
		fundingRate = &fr
		if a.prevFunding != nil {
			delta := fr - *a.prevFunding
			fundingRateChange = &delta
		}
	}

	// Only update prev state if current bar has values (maintains state across gaps)
	if oiValue != nil {
		a.prevOI = oiValue
	}
	if fundingRate != nil {
		a.prevFunding = fundingRate
	}

	var liqLongVol, liqShortVol *float64
	if liqCovered {
		lv := b.LiqLongVol
		sv := b.LiqShortVol
		liqLongVol = &lv
		liqShortVol = &sv
	} else {
		if b.LiqLongVol > 0 {
			llv := b.LiqLongVol
			liqLongVol = &llv
		}
		if b.LiqShortVol > 0 {
			lsv := b.LiqShortVol
			liqShortVol = &lsv
		}
	}

	liqCoveredInt := 0
	if liqCovered {
		liqCoveredInt = 1
	}

	return types.OrderbookBar{
		Timestamp:          (b.Timestamp + 1) * 60000,
		VWAP:               vwap,
		TradeCount:         b.TradeCount,
		BuyVolume:          b.BuyVolume,
		SellVolume:         b.SellVolume,
		AvgSpread:          avgSpread,
		SpreadStdDev:       spreadStdDev,
		DepthImbalance:     depthImbalance,
		DepthRatio:         depthRatio,
		OpenInterest:       oiValue,
		OpenInterestChange: oiChange,
		FundingRate:        fundingRate,
		FundingRateChange:  fundingRateChange,
		LiqLongVol:         liqLongVol,
		LiqShortVol:        liqShortVol,
		LiqCovered:         liqCoveredInt,
	}
}
