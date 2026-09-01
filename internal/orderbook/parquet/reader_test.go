package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Row structs matching the spec
// ---------------------------------------------------------------------------

type orderBookRow struct {
	EventTime         int64   `parquet:"event_time"`
	EventType         string  `parquet:"event_type"`
	Price             float64 `parquet:"price"`
	Quantity          float64 `parquet:"quantity"`
	Side              string  `parquet:"side"`
	FinalUpdateID     int64   `parquet:"final_update_id"`
	PrevFinalUpdateID int64   `parquet:"prev_final_update_id"`
	LastUpdateID      int64   `parquet:"last_update_id"`
}

type tradeRow struct {
	TradeTime    int64   `parquet:"trade_time"`
	Price        float64 `parquet:"price"`
	Quantity     float64 `parquet:"quantity"`
	IsBuyerMaker bool    `parquet:"is_buyer_maker"`
}

type oiRow struct {
	Timestamp            int64   `parquet:"timestamp"`
	SumOpenInterest      float64 `parquet:"sum_open_interest"`
	SumOpenInterestValue float64 `parquet:"sum_open_interest_value"`
}

type liqRow struct {
	TradeTime          int64   `parquet:"trade_time"`
	LastFilledQuantity float64 `parquet:"last_filled_quantity"`
	Side               string  `parquet:"side"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeOrderBookParquet(t *testing.T, path string, rows []orderBookRow, opts ...parquet.WriterOption) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquet.NewGenericWriter[orderBookRow](f, opts...)
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func writeTradeParquet(t *testing.T, path string, rows []tradeRow) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquet.NewGenericWriter[tradeRow](f)
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func writeOIParquet(t *testing.T, path string, rows []oiRow) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquet.NewGenericWriter[oiRow](f)
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func writeLiqParquet(t *testing.T, path string, rows []liqRow) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquet.NewGenericWriter[liqRow](f)
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func collectOrderBook(t *testing.T, path string) []OrderBook {
	t.Helper()
	r, err := Open(path)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	var out []OrderBook
	require.NoError(t, r.StreamOrderBook(func(ob OrderBook) error {
		out = append(out, ob)
		return nil
	}))
	return out
}

func collectTrades(t *testing.T, path string) []Trade {
	t.Helper()
	r, err := Open(path)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	var out []Trade
	require.NoError(t, r.StreamTrades(func(tr Trade) error {
		out = append(out, tr)
		return nil
	}))
	return out
}

func collectOI(t *testing.T, path string) []OpenInterest {
	t.Helper()
	r, err := Open(path)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	var out []OpenInterest
	require.NoError(t, r.StreamOpenInterest(func(o OpenInterest) error {
		out = append(out, o)
		return nil
	}))
	return out
}

func collectLiq(t *testing.T, path string) []Liquidation {
	t.Helper()
	r, err := Open(path)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	var out []Liquidation
	require.NoError(t, r.StreamLiquidations(func(l Liquidation) error {
		out = append(out, l)
		return nil
	}))
	return out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestStreamOrderBook_SortsWithinGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	rows := []orderBookRow{
		{EventTime: 1, EventType: "update", Price: 300, Quantity: 1, Side: "bid", FinalUpdateID: 10, PrevFinalUpdateID: 9},
		{EventTime: 1, EventType: "update", Price: 100, Quantity: 1, Side: "bid", FinalUpdateID: 10, PrevFinalUpdateID: 9},
		{EventTime: 1, EventType: "update", Price: 200, Quantity: 1, Side: "ask", FinalUpdateID: 10, PrevFinalUpdateID: 9},
	}
	writeOrderBookParquet(t, path, rows)

	out := collectOrderBook(t, path)
	require.Len(t, out, 3)
	assert.Equal(t, 100.0, out[0].Price)
	assert.Equal(t, 200.0, out[1].Price)
	assert.Equal(t, 300.0, out[2].Price)
}

func TestStreamOrderBook_PreservesGroupOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	rows := []orderBookRow{
		{EventTime: 1, EventType: "update", Price: 30, Quantity: 1, Side: "bid", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 10, Quantity: 1, Side: "bid", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 2, EventType: "update", Price: 20, Quantity: 1, Side: "ask", FinalUpdateID: 2, PrevFinalUpdateID: 1},
		{EventTime: 2, EventType: "update", Price: 5, Quantity: 1, Side: "ask", FinalUpdateID: 2, PrevFinalUpdateID: 1},
		{EventTime: 3, EventType: "update", Price: 15, Quantity: 1, Side: "bid", FinalUpdateID: 3, PrevFinalUpdateID: 2},
	}
	writeOrderBookParquet(t, path, rows)

	out := collectOrderBook(t, path)
	require.Len(t, out, 5)

	// Group 1 (FinalUpdateID=1) sorted: 10, 30
	assert.Equal(t, int64(1), out[0].FinalUpdateID)
	assert.Equal(t, 10.0, out[0].Price)
	assert.Equal(t, int64(1), out[1].FinalUpdateID)
	assert.Equal(t, 30.0, out[1].Price)

	// Group 2 (FinalUpdateID=2) sorted: 5, 20
	assert.Equal(t, int64(2), out[2].FinalUpdateID)
	assert.Equal(t, 5.0, out[2].Price)
	assert.Equal(t, int64(2), out[3].FinalUpdateID)
	assert.Equal(t, 20.0, out[3].Price)

	// Group 3 (FinalUpdateID=3)
	assert.Equal(t, int64(3), out[4].FinalUpdateID)
	assert.Equal(t, 15.0, out[4].Price)
}

func TestStreamOrderBook_GroupSpanningRowGroups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	// Force multiple row groups by setting MaxRowsPerRowGroup=2.
	// 5 rows -> row groups [2,2,1].
	// Group spanning: middle FinalUpdateID=2 spans the boundary between RG1 and RG2.
	rows := []orderBookRow{
		{EventTime: 1, EventType: "update", Price: 30, Quantity: 1, Side: "bid", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 10, Quantity: 1, Side: "bid", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		// Group 2 starts here — will span row group boundary (rows 3-4 with same FinalUpdateID=2)
		{EventTime: 2, EventType: "update", Price: 50, Quantity: 1, Side: "ask", FinalUpdateID: 2, PrevFinalUpdateID: 1},
		{EventTime: 2, EventType: "update", Price: 20, Quantity: 1, Side: "ask", FinalUpdateID: 2, PrevFinalUpdateID: 1},
		{EventTime: 3, EventType: "update", Price: 15, Quantity: 1, Side: "bid", FinalUpdateID: 3, PrevFinalUpdateID: 2},
	}
	writeOrderBookParquet(t, path, rows, parquet.MaxRowsPerRowGroup(2))

	out := collectOrderBook(t, path)
	require.Len(t, out, 5)

	// Group 1 sorted
	assert.Equal(t, int64(1), out[0].FinalUpdateID)
	assert.Equal(t, 10.0, out[0].Price)
	assert.Equal(t, int64(1), out[1].FinalUpdateID)
	assert.Equal(t, 30.0, out[1].Price)

	// Group 2 sorted despite spanning row groups — O(N log k) invariant
	assert.Equal(t, int64(2), out[2].FinalUpdateID)
	assert.Equal(t, 20.0, out[2].Price)
	assert.Equal(t, int64(2), out[3].FinalUpdateID)
	assert.Equal(t, 50.0, out[3].Price)

	// Group 3
	assert.Equal(t, int64(3), out[4].FinalUpdateID)
	assert.Equal(t, 15.0, out[4].Price)
}

func TestStreamOrderBook_SnapshotFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	rows := []orderBookRow{
		// FinalUpdateID=0 but LastUpdateID set -> fallback to LastUpdateID (use 5 so groups stay sorted)
		{EventTime: 1, EventType: "update", Price: 100, Quantity: 1, Side: "bid", FinalUpdateID: 0, LastUpdateID: 5, PrevFinalUpdateID: 0},
		// Empty EventType + PrevFinalUpdateID=0 -> snapshot
		{EventTime: 2, EventType: "", Price: 200, Quantity: 1, Side: "ask", FinalUpdateID: 7, PrevFinalUpdateID: 0},
	}
	writeOrderBookParquet(t, path, rows)

	out := collectOrderBook(t, path)
	require.Len(t, out, 2)

	// First row: FinalUpdateID falls back to LastUpdateID
	assert.Equal(t, int64(5), out[0].FinalUpdateID)
	// EventType was "update" -> normalized to "depthUpdate"
	assert.Equal(t, "depthUpdate", out[0].EventType)

	// Second row: empty EventType + PrevFinalUpdateID=0 -> "snapshot"
	assert.Equal(t, "snapshot", out[1].EventType)
	assert.Equal(t, int64(7), out[1].FinalUpdateID)
}

func TestStreamOrderBook_SideNormalization(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	rows := []orderBookRow{
		{EventTime: 1, EventType: "update", Price: 1, Quantity: 1, Side: "b", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 2, Quantity: 1, Side: "a", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 3, Quantity: 1, Side: "buy", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 4, Quantity: 1, Side: "sell", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 5, Quantity: 1, Side: "BID", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 1, EventType: "update", Price: 6, Quantity: 1, Side: "ask", FinalUpdateID: 1, PrevFinalUpdateID: 0},
	}
	writeOrderBookParquet(t, path, rows)

	out := collectOrderBook(t, path)
	require.Len(t, out, 6)

	// All rows share same FinalUpdateID so they come back sorted by Price.
	// Map price -> expected normalized side.
	expected := map[float64]string{
		1: "bid", // "b" -> bid
		2: "ask", // "a" -> ask
		3: "bid", // "buy" -> bid
		4: "ask", // "sell" -> ask
		5: "bid", // "BID" -> bid
		6: "ask", // "ask" -> ask (default ask path)
	}
	for _, ob := range out {
		assert.Equal(t, expected[ob.Price], ob.Side, "price %v side mismatch", ob.Price)
	}
}

func TestStreamOrderBook_SortednessGuard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ob.parquet")

	// FinalUpdateID decreasing: 3, 1, 2 — violates sortedness but must not drop rows.
	rows := []orderBookRow{
		{EventTime: 1, EventType: "update", Price: 10, Quantity: 1, Side: "bid", FinalUpdateID: 3, PrevFinalUpdateID: 2},
		{EventTime: 2, EventType: "update", Price: 20, Quantity: 1, Side: "ask", FinalUpdateID: 1, PrevFinalUpdateID: 0},
		{EventTime: 3, EventType: "update", Price: 30, Quantity: 1, Side: "bid", FinalUpdateID: 2, PrevFinalUpdateID: 1},
	}
	writeOrderBookParquet(t, path, rows)

	out := collectOrderBook(t, path)
	// Guard should not drop rows — all must be returned.
	require.Len(t, out, 3)
	// Each group has 1 row, so order is preserved (groups emitted in file order,
	// each group sorted trivially).
	assert.Equal(t, int64(3), out[0].FinalUpdateID)
	assert.Equal(t, int64(1), out[1].FinalUpdateID)
	assert.Equal(t, int64(2), out[2].FinalUpdateID)
}

func TestStreamTrades_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trades.parquet")

	rows := []tradeRow{
		{TradeTime: 1000, Price: 50000.5, Quantity: 0.1, IsBuyerMaker: true},
		{TradeTime: 2000, Price: 50001.0, Quantity: 0.2, IsBuyerMaker: false},
	}
	writeTradeParquet(t, path, rows)

	out := collectTrades(t, path)
	require.Len(t, out, 2)
	assert.Equal(t, int64(1000), out[0].TradeTime)
	assert.InDelta(t, 50000.5, out[0].Price, 0.001)
	assert.InDelta(t, 0.1, out[0].Quantity, 0.0001)
	assert.True(t, out[0].IsBuyerMaker)

	assert.Equal(t, int64(2000), out[1].TradeTime)
	assert.InDelta(t, 50001.0, out[1].Price, 0.001)
	assert.False(t, out[1].IsBuyerMaker)
}

func TestStreamOpenInterest_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "oi.parquet")

	rows := []oiRow{
		{Timestamp: 1000, SumOpenInterest: 123.45, SumOpenInterestValue: 678.9},
		{Timestamp: 2000, SumOpenInterest: 111.0, SumOpenInterestValue: 222.0},
	}
	writeOIParquet(t, path, rows)

	out := collectOI(t, path)
	require.Len(t, out, 2)
	assert.Equal(t, int64(1000), out[0].Timestamp)
	assert.InDelta(t, 123.45, out[0].SumOpenInterest, 0.001)
	assert.InDelta(t, 678.9, out[0].SumOpenInterestValue, 0.001)

	assert.Equal(t, int64(2000), out[1].Timestamp)
	assert.InDelta(t, 111.0, out[1].SumOpenInterest, 0.001)
	assert.InDelta(t, 222.0, out[1].SumOpenInterestValue, 0.001)
}

func TestStreamLiquidations_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "liq.parquet")

	rows := []liqRow{
		{TradeTime: 1000, LastFilledQuantity: 1.5, Side: "BUY"},
		{TradeTime: 2000, LastFilledQuantity: 2.5, Side: "SELL"},
	}
	writeLiqParquet(t, path, rows)

	out := collectLiq(t, path)
	require.Len(t, out, 2)
	assert.Equal(t, int64(1000), out[0].TradeTime)
	assert.InDelta(t, 1.5, out[0].LastFilledQuantity, 0.001)
	assert.Equal(t, "BUY", out[0].Side)

	assert.Equal(t, int64(2000), out[1].TradeTime)
	assert.InDelta(t, 2.5, out[1].LastFilledQuantity, 0.001)
	assert.Equal(t, "SELL", out[1].Side)
}
