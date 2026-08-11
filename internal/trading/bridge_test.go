package trading

import (
	"context"
	"encoding/json"
	"gorango/mdx/domain/types"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTranslateOrderOpenMarket(t *testing.T) {
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.5}
	req, err := translateOrder(in)
	require.NoError(t, err)

	assert.Equal(t, "BTC/USDT:PERP", req.Symbol)
	assert.Equal(t, types.OrderSideBuy, req.Side)
	assert.Equal(t, types.OrderTypeMarket, req.Type)
	assert.Equal(t, 0.5, req.Amount)
	assert.Nil(t, req.ReduceOnly, "open orders must not be reduce-only")
}

func TestTranslateOrderCloseBecomesReduceOnly(t *testing.T) {
	in := &engineOrder{Symbol: "BTCUSDT", Side: "SELL", Action: "close", Size: 0.5}
	req, err := translateOrder(in)
	require.NoError(t, err)

	assert.Equal(t, types.OrderSideSell, req.Side)
	require.NotNil(t, req.ReduceOnly, "close orders must map to reduceOnly")
	assert.True(t, *req.ReduceOnly, "close must never flip into a reverse position")
	assert.Equal(t, "BTC/USDT:PERP", req.Symbol, "symbols must normalize to canonical form")
}

func TestTranslateOrderPostOnlyBecomesGTX(t *testing.T) {
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 1, Price: 50000, OrderType: "post_only"}
	req, err := translateOrder(in)
	require.NoError(t, err)

	assert.Equal(t, types.OrderTypeLimit, req.Type)
	require.NotNil(t, req.TimeInForce)
	assert.Equal(t, types.TIFGTX, *req.TimeInForce)
	require.NotNil(t, req.Price)
	assert.Equal(t, 50000.0, *req.Price)
}

func TestTranslateOrderLeveragePassthrough(t *testing.T) {
	lev := 10
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "SELL", Action: "open", Size: 1, Leverage: &lev}
	req, err := translateOrder(in)
	require.NoError(t, err)
	require.NotNil(t, req.Leverage)
	assert.Equal(t, 10, *req.Leverage)
}

func TestTranslateOrderInvalidSide(t *testing.T) {
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "HODL", Action: "open", Size: 1}
	_, err := translateOrder(in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported side")
}

func TestTranslateOrderInvalidType(t *testing.T) {
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 1, OrderType: "iceberg"}
	_, err := translateOrder(in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported order type")
}

func TestBuildReportFilledMarketOrder(t *testing.T) {
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.1}
	avg := 50123.40
	resp := &types.OrderResponse{ID: "123", Filled: 0.1, Average: &avg}

	report := buildReport("BTC/USDT:PERP", "open", in, resp)
	assert.True(t, report.Filled)
	assert.Equal(t, 50123.40, report.FillPrice)
	assert.Equal(t, "123", report.OrderID)
	assert.Equal(t, "BUY", report.Side)
}

func TestBuildReportBybitAsyncAck(t *testing.T) {
	// Bybit create-order returns only an order ID; no fill info is available
	// synchronously, so the report must be unfilled and the engine falls back.
	in := &engineOrder{Symbol: "BTC/USDT:PERP", Side: "SELL", Action: "close", Size: 0.1}
	resp := &types.OrderResponse{ID: "o-42", Filled: 0, Remaining: 0.1}

	report := buildReport("BTC/USDT:PERP", "close", in, resp)
	assert.False(t, report.Filled)
	assert.Equal(t, "o-42", report.OrderID)
}

func TestExecuteWithStubConnector(t *testing.T) {
	var submitted types.OrderRequest
	var setLeverageSymbol string
	var setLeverageVal int
	stub := &stubConnector{
		submit: func(req types.OrderRequest) (*types.OrderResponse, error) {
			submitted = req
			avg := 50000.0
			return &types.OrderResponse{ID: "o-1", Filled: req.Amount, Average: &avg}, nil
		},
		setLeverage: func(symbol string, lev int) error {
			setLeverageSymbol = symbol
			setLeverageVal = lev
			return nil
		},
	}

	bridge := NewOrderBridge(nil, stub, nil)
	lev := 10
	payload, _ := json.Marshal(engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.5, Leverage: &lev})
	report := bridge.execute(payload, "BTC/USDT:PERP", "open")

	require.NotNil(t, report)
	assert.True(t, report.Filled)
	assert.Equal(t, 50000.0, report.FillPrice)
	assert.Equal(t, "o-1", report.OrderID)
	assert.Equal(t, 10, setLeverageVal, "leverage must be set before opening")
	assert.Equal(t, "BTC/USDT:PERP", setLeverageSymbol)
	assert.Equal(t, types.OrderSideBuy, submitted.Side)
	assert.Equal(t, 0.5, submitted.Amount)
}

func TestExecuteRejectsOnConnectorError(t *testing.T) {
	stub := &stubConnector{
		submit: func(req types.OrderRequest) (*types.OrderResponse, error) {
			return nil, errInsufficientFunds
		},
	}
	bridge := NewOrderBridge(nil, stub, nil)
	payload, _ := json.Marshal(engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.5})
	report := bridge.execute(payload, "BTC/USDT:PERP", "open")

	require.NotNil(t, report)
	assert.False(t, report.Filled)
	assert.Contains(t, report.Error, "insufficient funds")
}

func TestExecuteRejectsOnLeverageError(t *testing.T) {
	stub := &stubConnector{
		setLeverage: func(symbol string, lev int) error {
			return errInsufficientFunds
		},
	}
	bridge := NewOrderBridge(nil, stub, nil)
	lev := 20
	payload, _ := json.Marshal(engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.5, Leverage: &lev})
	report := bridge.execute(payload, "BTC/USDT:PERP", "open")

	require.NotNil(t, report)
	assert.Contains(t, report.Error, "set leverage")
}

func TestBridgeSetLeverage(t *testing.T) {
	nc := testNATS(t)

	var setSymbol string
	var setLev int
	stub := &stubConnector{
		setLeverage: func(symbol string, lev int) error {
			setSymbol = symbol
			setLev = lev
			return nil
		},
	}
	bridge := NewOrderBridge(nc, stub, nil)
	require.NoError(t, bridge.Start())
	t.Cleanup(func() { _ = bridge.Stop() })

	payload, _ := json.Marshal(struct {
		Symbol   string `json:"symbol"`
		Leverage int    `json:"leverage"`
	}{Symbol: "BTC/USDT:PERP", Leverage: 30})

	reply, err := nc.Request("orders.BTC/USDT:PERP.set_leverage", payload, 2*time.Second)
	require.NoError(t, err)

	assert.Equal(t, "BTC/USDT:PERP", setSymbol)
	assert.Equal(t, 30, setLev, "derived leverage must reach the connector")

	var report executionReport
	require.NoError(t, json.Unmarshal(reply.Data, &report))
	assert.Equal(t, "set_leverage", report.Action)
	assert.Empty(t, report.Error)
}

func TestBridgeSetLeverageSurfacesError(t *testing.T) {
	nc := testNATS(t)

	stub := &stubConnector{
		setLeverage: func(symbol string, lev int) error {
			return errInsufficientFunds
		},
	}
	bridge := NewOrderBridge(nc, stub, nil)
	require.NoError(t, bridge.Start())
	t.Cleanup(func() { _ = bridge.Stop() })

	payload, _ := json.Marshal(struct {
		Symbol   string `json:"symbol"`
		Leverage int    `json:"leverage"`
	}{Symbol: "BTC/USDT:PERP", Leverage: 30})

	reply, err := nc.Request("orders.BTC/USDT:PERP.set_leverage", payload, 2*time.Second)
	require.NoError(t, err)

	var report executionReport
	require.NoError(t, json.Unmarshal(reply.Data, &report))
	assert.Equal(t, "set_leverage", report.Action)
	assert.Contains(t, report.Error, "set leverage")
}

// --- test helpers ---

// testNATS connects to a local NATS server, skipping the test when it is not
// running (mirrors the engine's exchange test setup).
func testNATS(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("NATS not available at localhost:4222: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

type stubConnector struct {
	submit      func(req types.OrderRequest) (*types.OrderResponse, error)
	setLeverage func(symbol string, lev int) error
}

func (s *stubConnector) ID() string { return "stub" }
func (s *stubConnector) GetHistory(context.Context, string, string, time.Time, time.Time) ([]types.Bar, error) {
	return nil, nil
}
func (s *stubConnector) StreamPrices(context.Context, string) (<-chan types.Bar, error) {
	return nil, nil
}
func (s *stubConnector) GetBalance(context.Context) (*types.Balance, error)     { return nil, nil }
func (s *stubConnector) GetPositions(context.Context) ([]types.Position, error) { return nil, nil }
func (s *stubConnector) GetOpenOrders(context.Context, string) ([]types.OrderResponse, error) {
	return nil, nil
}
func (s *stubConnector) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	if s.submit != nil {
		return s.submit(req)
	}
	return nil, nil
}
func (s *stubConnector) CancelOrder(context.Context, string, string) error { return nil }
func (s *stubConnector) SetLeverage(_ context.Context, symbol string, lev int) error {
	if s.setLeverage != nil {
		return s.setLeverage(symbol, lev)
	}
	return nil
}
func (s *stubConnector) Connect(context.Context) error { return nil }
func (s *stubConnector) Close() error                  { return nil }

var errInsufficientFunds = &paperError{message: "insufficient funds"}
