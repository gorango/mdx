package levelhist

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorango/mdx/domain/types"
)

// TestOHLCFirstLastExtremes: open = first accepted trade, close = last,
// high/low are running extremes regardless of order.
func TestOHLCFirstLastExtremes(t *testing.T) {
	trades := []struct {
		price, qty float64
		buyerMaker bool
	}{
		{100.5, 1.0, false},
		{101.25, 2.0, true},
		{99.75, 0.5, true},
		{100.9, 3.0, false},
	}

	fwd := New()
	for _, tr := range trades {
		fwd.Add(tr.price, tr.qty, tr.buyerMaker)
	}
	s := fwd.Reduce()

	assert.Equal(t, 4, s.TradeCount)
	assert.InDelta(t, 100.5, s.Open, 1e-12)
	assert.InDelta(t, 100.9, s.Close, 1e-12)
	assert.InDelta(t, 101.25, s.High, 1e-12)
	assert.InDelta(t, 99.75, s.Low, 1e-12)
	assert.InDelta(t, 6.5, s.Volume, 1e-12)

	// Reversed insertion order must flip only open/close.
	rev := New()
	for i := len(trades) - 1; i >= 0; i-- {
		tr := trades[i]
		rev.Add(tr.price, tr.qty, tr.buyerMaker)
	}
	rs := rev.Reduce()
	assert.InDelta(t, 100.9, rs.Open, 1e-12)
	assert.InDelta(t, 100.5, rs.Close, 1e-12)
	assert.InDelta(t, rs.High, s.High, 1e-12)
	assert.InDelta(t, rs.Low, s.Low, 1e-12)
}

// TestSideVWAPsAndPOCs: side VWAPs are per-side Σp·q/Σq; the POCs are the
// heaviest total / per-side levels.
func TestSideVWAPsAndPOCs(t *testing.T) {
	h := New()
	// buys at 100 (qty 2) and 104 (qty 2) → buyVWAP 102; buy POC ties → lower price 100
	h.Add(100.0, 2.0, false)
	h.Add(104.0, 2.0, false)
	// sells at 103 (qty 5) → sellVWAP 103; sell POC 103
	h.Add(103.0, 5.0, true)

	s := h.Reduce()
	require.True(t, s.HasBuys)
	require.True(t, s.HasSells)
	assert.InDelta(t, 102.0, s.BuyVWAP, 1e-12)
	assert.InDelta(t, 103.0, s.SellVWAP, 1e-12)
	assert.InDelta(t, 100.0, s.BuyPOCPrice, 1e-12)
	assert.InDelta(t, 103.0, s.SellPOCPrice, 1e-12)

	// Total volume = 2+2+5 = 9, POC level (103) carries 5 → share 5/9.
	assert.InDelta(t, 103.0, s.POCPrice, 1e-12)
	assert.InDelta(t, 5.0/9.0, s.POCVolumeShare, 1e-12)
}

// TestPOCTieBreakLowerPriceOrderIndependent: two levels with equal total
// volume must resolve to the LOWER price no matter the arrival order — the
// batch and live feeds can disagree on ordering under retransmit.
func TestPOCTieBreakLowerPriceOrderIndependent(t *testing.T) {
	type trade struct {
		price, qty float64
		buyerMaker bool
	}
	base := []trade{
		{101.0, 2.0, false},
		{100.0, 2.0, true},
		{105.0, 1.0, true},
		{99.0, 0.5, false},
	}

	reduce := func(order []int) Stats {
		h := New()
		for _, idx := range order {
			tt := base[idx]
			h.Add(tt.price, tt.qty, tt.buyerMaker)
		}
		return h.Reduce()
	}

	forward := reduce([]int{0, 1, 2, 3})
	shuffled := reduce([]int{2, 0, 3, 1})
	rev := reduce([]int{3, 2, 1, 0})

	for _, s := range []Stats{shuffled, rev} {
		assert.InDelta(t, forward.POCPrice, s.POCPrice, 1e-12)
		assert.InDelta(t, forward.POCVolumeShare, s.POCVolumeShare, 1e-12)
	}
	assert.InDelta(t, 100.0, forward.POCPrice, 1e-12) // tie between 100 and 101 → 100
}

// TestDeterminismUnderShuffle: full stat set identical across random
// permutations of the same trade multiset (kills any map-order dependence).
func TestDeterminismUnderShuffle(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	type trade struct {
		price, qty float64
		buyerMaker bool
	}
	var trades []trade
	prices := []float64{0.00012345, 0.5, 1.0, 123.456789, 98765.43} // sub-tick to large
	for i := 0; i < 200; i++ {
		trades = append(trades, trade{
			price:      prices[rng.Intn(len(prices))] * (1 + rng.Float64()*0.001),
			qty:        rng.Float64() + 1e-6,
			buyerMaker: rng.Intn(2) == 0,
		})
	}

	ref := New()
	for _, tt := range trades {
		ref.Add(tt.price, tt.qty, tt.buyerMaker)
	}
	want := ref.Reduce()

	for trial := 0; trial < 20; trial++ {
		rng.Shuffle(len(trades), func(i, j int) { trades[i], trades[j] = trades[j], trades[i] })
		h := New()
		for _, tt := range trades {
			h.Add(tt.price, tt.qty, tt.buyerMaker)
		}
		got := h.Reduce()

		assert.Equal(t, want.TradeCount, got.TradeCount)
		// Exact order-independence: running extremes.
		assert.True(t, floatEq(want.High, got.High) && floatEq(want.Low, got.Low))
		// Summed quantities re-associate under permutation → FP noise only;
		// argmax selections stay stable because random level totals are
		// separated far beyond float64 noise.
		near(t, want.Volume, got.Volume)
		near(t, want.POCPrice, got.POCPrice)
		near(t, want.BuyPOCPrice, got.BuyPOCPrice)
		near(t, want.SellPOCPrice, got.SellPOCPrice)
		near(t, want.HiBandBuyVol+want.HiBandSellVol, got.HiBandBuyVol+got.HiBandSellVol)
		near(t, want.LoBandBuyVol+want.LoBandSellVol, got.LoBandBuyVol+got.LoBandSellVol)
	}
}

// near asserts equality within float64 noise from summation reassociation.
func near(t *testing.T, want, got float64) {
	t.Helper()
	tol := 1e-9 * math.Max(math.Abs(want), math.Abs(got))
	if tol < 1e-12 {
		tol = 1e-12
	}
	assert.InDelta(t, want, got, tol)
}

func floatEq(a, b float64) bool { return a == b }

// TestBandsTopKBottomK: bands sum exactly the K lightest/heaviest distinct
// levels' side volumes.
func TestBandsTopKBottomK(t *testing.T) {
	h := New()
	// Distinct levels 99..103, one trade each (buy), qty = level - 98 →
	// volumes 1..5 sorted by price ascending.
	for lvl := 99.0; lvl <= 103.0; lvl++ {
		h.Add(lvl, lvl-98.0, false)
	}
	// One extra sell at the top level so band splits carry both sides.
	h.Add(103.0, 10.0, true)

	s := h.Reduce()
	// lo band = 3 lowest prices: 99(1) + 100(2) + 101(3) → buy vol 6
	assert.InDelta(t, 6.0, s.LoBandBuyVol, 1e-12)
	assert.InDelta(t, 0.0, s.LoBandSellVol, 1e-12)
	// hi band = 3 highest prices: 101(3) + 102(4) + 103(5+10) → buy 12, sell 10
	// (level 101 belongs to BOTH bands when BandK overlaps a 5-level histogram)
	assert.InDelta(t, 12.0, s.HiBandBuyVol, 1e-12)
	assert.InDelta(t, 10.0, s.HiBandSellVol, 1e-12)
}

// TestDegenerateRowsSkipped: non-finite / non-positive inputs never poison
// reductions.
func TestDegenerateRowsSkipped(t *testing.T) {
	h := New()
	h.Add(math.NaN(), 1.0, false)
	h.Add(math.Inf(1), 1.0, false)
	h.Add(100.0, math.NaN(), false)
	h.Add(100.0, 0.0, false)
	h.Add(-100.0, 1.0, false)
	h.Add(0.0, 1.0, false)
	h.Add(100.0, 2.0, true)

	s := h.Reduce()
	assert.Equal(t, 1, s.TradeCount)
	assert.InDelta(t, 100.0, s.Open, 1e-12)
	assert.InDelta(t, 100.0, s.Close, 1e-12)
	assert.False(t, s.HasBuys)
	assert.True(t, s.HasSells)
	assert.InDelta(t, 100.0, s.SellVWAP, 1e-12) // single trade @100 → VWAP = price
	assert.InDelta(t, 100.0, s.SellPOCPrice, 1e-12)
}

// TestReduceDoesNotMutate: repeated Reduce calls agree (Finalize may retry).
func TestReduceDoesNotMutate(t *testing.T) {
	h := New()
	h.Add(50.0, 1.0, false)
	h.Add(51.0, 1.0, true)
	a := h.Reduce()
	b := h.Reduce()
	assert.Equal(t, a, b)
}

// TestApplyNULLDiscipline: zero-trade stats leave the bar untouched;
// populated bars get OHLC/POC always and side fields only for traded sides;
// band columns exist (possibly zero) whenever trades exist.
func TestApplyNULLDiscipline(t *testing.T) {
	bar := types.OrderbookBar{}
	Stats{}.Apply(&bar)
	assert.Nil(t, bar.TradeOpen)
	assert.Nil(t, bar.POCPrice)
	assert.Nil(t, bar.HiBandBuyVol)

	bar = types.OrderbookBar{}
	h := New()
	h.Add(100.0, 3.0, true) // sells only
	h.Apply(&bar)
	require.NotNil(t, bar.TradeOpen)
	assert.InDelta(t, 100.0, *bar.TradeOpen, 1e-12)
	assert.NotNil(t, bar.POCPrice, "populated bar carries POC")
	assert.Nil(t, bar.BuyVWAP, "no taker buys → NULL buy VWAP")
	assert.Nil(t, bar.BuyPOCPrice)
	require.NotNil(t, bar.SellVWAP)
	assert.InDelta(t, 100.0, *bar.SellVWAP, 1e-12)
	require.NotNil(t, bar.HiBandBuyVol)
	assert.InDelta(t, 0.0, *bar.HiBandBuyVol, 1e-12) // true zero, not NULL
	assert.NotNil(t, bar.TradeHigh)
}
