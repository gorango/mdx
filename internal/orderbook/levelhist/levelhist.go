// Package levelhist accumulates a per-minute trade histogram over exact trade
// prices and reduces it to the scalar "footprint" statistics persisted on
// types.OrderbookBar (migration 0006). The distribution itself is never
// persisted — only these reductions.
//
// PARITY CONTRACT (batch internal/orderbook/aggregator and live
// internal/orderbook/aggregator/streaming): both aggregators MUST feed an
// identical trade sequence into Hist and persist the result via Stats.Apply.
// All reductions are order-independent so that batch (vendor parquet order)
// and live (websocket arrival order) agree even under retransmit/reorder:
//   - OHLC uses first/last accepted trade; high/low are running extremes;
//   - argmax reductions (poc, buy/sell poc) maintain an incremental maximum
//     with a deterministic tie-break to the LOWER price key — never map
//     iteration;
//   - hi/lo band sums sort keys before summation;
//   - side VWAPs and moments are plain sums.
package levelhist

import (
	"math"
	"sort"

	"gorango/mdx/domain/types"
)

// KeyScale quantizes trade prices into int64 histogram keys at 1e-8 absolute
// resolution. Deliberately finer than the orderbook treap's priceScale=1e4:
// long-tail symbols tick below 1e-4 absolute price. int64 stays exact for
// prices up to ~9e7 (well above any tradable price), and trades execute on
// exchange tick grids so keys cluster naturally.
const KeyScale = 1e8

// BandK is the number of top-/bottom- distinct traded levels summarised into
// the hi_band_* / lo_band_* columns. Fixed for determinism across paths.
const BandK = 3

type sideVol struct{ buy, sell float64 }

// Hist accumulates one minute's trades over exact prices. Not safe for
// concurrent use; both aggregators call it under their existing locks.
type Hist struct {
	levels map[int64]*sideVol

	tradeCount             int
	haveTrade              bool
	open, high, low, close float64

	volBuy, volSell float64 // Σ qty per taker side
	valBuy, valSell float64 // Σ price·qty per taker side → side VWAPs
	sumVP, sumVP2   float64 // volume-weighted price moments → PriceStd

	// Incremental argmax state: (key, vol) per reduction. The tie-break to the
	// lower key makes the final argmax independent of insertion order.
	pocKey     int64
	pocVol     float64
	buyPocKey  int64
	buyPocVol  float64
	sellPocKey int64
	sellPocVol float64
}

// New creates an empty histogram.
func New() *Hist {
	return &Hist{levels: make(map[int64]*sideVol)}
}

func key(price float64) int64 { return int64(math.Round(price * KeyScale)) }

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// consider updates one incremental argmax: strictly-greater volume replaces;
// equal volume replaces only when the candidate key is LOWER. v is always > 0
// at call sites, so an untouched zero slot always loses to the first real hit.
func consider(keyP *int64, volP *float64, k int64, v float64) {
	if v > *volP || (v == *volP && k < *keyP) {
		*keyP = k
		*volP = v
	}
}

// Add records one trade. Degenerate rows (non-finite or non-positive
// price/quantity) are skipped entirely so they cannot poison any reduction;
// both aggregators feed identical sequences, so parity holds regardless of
// what the vendor stream contains.
func (h *Hist) Add(price, qty float64, buyerMaker bool) {
	if !finite(price) || price <= 0 || !finite(qty) || qty <= 0 {
		return
	}

	k := key(price)
	lv := h.levels[k]
	if lv == nil {
		lv = new(sideVol)
		h.levels[k] = lv
	}
	var dBuy, dSell float64
	if buyerMaker {
		dSell = qty // seller aggressed a resting bid → sell volume
	} else {
		dBuy = qty
	}
	lv.buy += dBuy
	lv.sell += dSell

	if !h.haveTrade {
		h.haveTrade = true
		h.open, h.high, h.low = price, price, price
	}
	h.close = price
	if price > h.high {
		h.high = price
	}
	if price < h.low {
		h.low = price
	}

	h.tradeCount++
	h.volBuy += dBuy
	h.volSell += dSell
	h.valBuy += price * dBuy
	h.valSell += price * dSell
	h.sumVP += price * qty
	h.sumVP2 += price * price * qty

	total := lv.buy + lv.sell
	consider(&h.pocKey, &h.pocVol, k, total)
	consider(&h.buyPocKey, &h.buyPocVol, k, lv.buy)
	consider(&h.sellPocKey, &h.sellPocVol, k, lv.sell)
}

// Stats is the reduced per-minute footprint scalar set. Field semantics match
// the migration-0006 columns one-to-one.
type Stats struct {
	TradeCount             int
	Open, High, Low, Close float64
	Volume                 float64 // volBuy + volSell over the accepted population
	BuyVWAP, SellVWAP      float64
	HasBuys, HasSells      bool
	POCPrice               float64
	POCVolumeShare         float64 // ∈ (0,1]
	BuyPOCPrice            float64 // valid only when HasBuys
	SellPOCPrice           float64 // valid only when HasSells
	PriceStd               float64
	HiBandBuyVol           float64
	HiBandSellVol          float64
	LoBandBuyVol           float64
	LoBandSellVol          float64
}

// Reduce collapses the histogram into the persisted scalar set. Safe to call
// repeatedly; does not mutate the Hist.
func (h *Hist) Reduce() Stats {
	s := Stats{
		TradeCount:   h.tradeCount,
		Open:         h.open,
		High:         h.high,
		Low:          h.low,
		Close:        h.close,
		Volume:       h.volBuy + h.volSell,
		BuyVWAP:      h.valBuy / h.volBuy,
		SellVWAP:     h.valSell / h.volSell,
		HasBuys:      h.volBuy > 0,
		HasSells:     h.volSell > 0,
		POCPrice:     float64(h.pocKey) / KeyScale,
		BuyPOCPrice:  float64(h.buyPocKey) / KeyScale,
		SellPOCPrice: float64(h.sellPocKey) / KeyScale,
	}
	if s.Volume > 0 {
		s.POCVolumeShare = h.pocVol / s.Volume
		mean := h.sumVP / s.Volume
		variance := h.sumVP2/s.Volume - mean*mean
		if variance < 0 {
			variance = 0 // float64 rounding guard
		}
		s.PriceStd = math.Sqrt(variance)
	}

	// Band sums: bottom-/top-BandK distinct traded levels. Assignments are
	// independent — with fewer than 2·BandK distinct levels a level can count
	// toward BOTH bands (a single-price bar trades at its own high and low).
	// Sorting makes this independent of insertion order.
	keys := make([]int64, 0, len(h.levels))
	for k := range h.levels {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	n := len(keys)
	for i, k := range keys {
		lv := h.levels[k]
		if i < BandK {
			s.LoBandBuyVol += lv.buy
			s.LoBandSellVol += lv.sell
		}
		if i >= n-BandK {
			s.HiBandBuyVol += lv.buy
			s.HiBandSellVol += lv.sell
		}
	}
	return s
}

// Apply writes the stats onto an OrderbookBar using the house NULL discipline:
// everything NULL when no trades populated the bar; side-specific VWAP/POC
// NULL when that side did not trade; band volumes are true zeros within a
// populated bar. Centralizing this here keeps batch and live byte-identical.
func (s Stats) Apply(bar *types.OrderbookBar) {
	if s.TradeCount == 0 {
		return
	}
	f := func(v float64) *float64 { x := v; return &x }
	bar.TradeOpen = f(s.Open)
	bar.TradeHigh = f(s.High)
	bar.TradeLow = f(s.Low)
	bar.TradeClose = f(s.Close)
	bar.POCPrice = f(s.POCPrice)
	bar.POCVolumeShare = f(s.POCVolumeShare)
	bar.TradePriceStd = f(s.PriceStd)
	bar.HiBandBuyVol = f(s.HiBandBuyVol)
	bar.HiBandSellVol = f(s.HiBandSellVol)
	bar.LoBandBuyVol = f(s.LoBandBuyVol)
	bar.LoBandSellVol = f(s.LoBandSellVol)
	if s.HasBuys {
		bar.BuyVWAP = f(s.BuyVWAP)
		bar.BuyPOCPrice = f(s.BuyPOCPrice)
	}
	if s.HasSells {
		bar.SellVWAP = f(s.SellVWAP)
		bar.SellPOCPrice = f(s.SellPOCPrice)
	}
}

// Apply reduces the histogram and writes the result onto bar in one step.
func (h *Hist) Apply(bar *types.OrderbookBar) {
	s := h.Reduce()
	s.Apply(bar)
}
