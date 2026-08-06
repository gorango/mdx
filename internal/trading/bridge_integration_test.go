package trading

import (
	"encoding/json"
	"gorango/exchanges/domain/types"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBridgeEndToEndOverNATS proves the full engine↔bridge loop over a real
// NATS server: an engine-format order published on `orders.<sym>.<action>`
// with a synchronous request-reply (as the engine's NATSExchange does) is
// translated, submitted, and answered with an execution report.
func TestBridgeEndToEndOverNATS(t *testing.T) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("NATS not available at localhost:4222: %v", err)
	}
	defer nc.Close()

	var submitted types.OrderRequest
	stub := &stubConnector{
		submit: func(req types.OrderRequest) (*types.OrderResponse, error) {
			submitted = req
			avg := 50123.40
			return &types.OrderResponse{ID: "o-live-1", Filled: req.Amount, Average: &avg}, nil
		},
	}

	bridge := NewOrderBridge(nc, stub, nil)
	require.NoError(t, bridge.Start())
	defer func() { _ = bridge.Stop() }()
	require.NoError(t, nc.Flush())

	// Engine side: NATSExchange.SendOrder → nc.Request on orders.<sym>.<action>.
	payload, _ := json.Marshal(engineOrder{
		Symbol:    "BTC/USDT:PERP",
		Side:      "SELL",
		Action:    "close",
		Size:      0.5,
		OrderType: "market",
		Exchange:  "binance",
	})

	reply, err := nc.Request("orders.BTC/USDT:PERP.close", payload, 3*time.Second)
	require.NoError(t, err, "bridge must reply to order requests")

	var report executionReport
	require.NoError(t, json.Unmarshal(reply.Data, &report))
	assert.Equal(t, "BTC/USDT:PERP", report.Symbol)
	assert.Equal(t, "close", report.Action)
	assert.Equal(t, "SELL", report.Side)
	assert.True(t, report.Filled)
	assert.Equal(t, 50123.40, report.FillPrice)
	assert.Equal(t, "o-live-1", report.OrderID)

	// The bridge must map close → reduceOnly so live closes never flip.
	require.NotNil(t, submitted.ReduceOnly)
	assert.True(t, *submitted.ReduceOnly)
	assert.Equal(t, types.OrderSideSell, submitted.Side)
	assert.Equal(t, 0.5, submitted.Amount)
	assert.Equal(t, "BTC/USDT:PERP", submitted.Symbol)

	// A rejected order must produce an error report (the engine skips).
	// Stop the first bridge first — queue-group members share the subject.
	require.NoError(t, bridge.Stop())
	require.NoError(t, nc.Flush())

	rejecting := &stubConnector{
		submit: func(req types.OrderRequest) (*types.OrderResponse, error) {
			return nil, &paperError{message: "quantity below minimum"}
		},
	}
	rejectBridge := NewOrderBridge(nc, rejecting, nil)
	require.NoError(t, rejectBridge.Start())
	defer func() { _ = rejectBridge.Stop() }()
	require.NoError(t, nc.Flush())

	openPayload, _ := json.Marshal(engineOrder{Symbol: "BTC/USDT:PERP", Side: "BUY", Action: "open", Size: 0.001})
	rejectReply, err := nc.Request("orders.BTC/USDT:PERP.open", openPayload, 3*time.Second)
	require.NoError(t, err)
	var rejectReport executionReport
	require.NoError(t, json.Unmarshal(rejectReply.Data, &rejectReport))
	assert.False(t, rejectReport.Filled)
	assert.Contains(t, rejectReport.Error, "below minimum")
}
