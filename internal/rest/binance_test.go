package rest

import (
	"context"
	"gorango/mdx/domain/types"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBinanceTestClient starts an httptest server and returns a BinanceClient
// pointed at it, plus the parsed query from the captured request.
func newBinanceTestClient(t *testing.T, handler http.HandlerFunc) (*BinanceClient, func() url.Values) {
	t.Helper()
	var lastQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q, err := url.ParseQuery(r.URL.RawQuery)
		require.NoError(t, err)
		lastQuery = q
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	client := NewBinance(Config{APIKey: "test-key", APISecret: "test-secret"})
	client.baseURL = srv.URL
	return client, func() url.Values { return lastQuery }
}

func TestBinanceSubmitOrderMarketNoLeverageParam(t *testing.T) {
	client, query := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/fapi/v1/order", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"orderId": 12345, "clientOrderId": "abc", "symbol": "BTCUSDT",
			"side": "BUY", "type": "MARKET", "price": "0",
			"origQty": "0.10000000", "executedQty": "0.10000000",
			"avgPrice": "50123.40", "status": "FILLED"
		}`))
	})

	resp, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 0.1,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Leverage must never appear on the order endpoint — it is set via
	// /fapi/v1/leverage only. This was the previous production bug.
	q := query()
	assert.NotContains(t, q, "leverage", "leverage is not a valid /fapi/v1/order parameter")
	assert.Equal(t, "BTCUSDT", q.Get("symbol"))
	assert.Equal(t, "BUY", q.Get("side"))
	assert.Equal(t, "MARKET", q.Get("type"))

	assert.Equal(t, "12345", resp.ID)
	assert.Equal(t, 0.1, resp.Filled)
	assert.Equal(t, types.OrderStatus("filled"), resp.Status)
}

func TestBinanceSubmitOrderReduceOnlyAndTIF(t *testing.T) {
	client, query := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderId": 1, "symbol": "BTCUSDT", "side": "SELL", "type": "LIMIT", "status": "NEW", "origQty": "1", "executedQty": "0", "avgPrice": "0", "price": "50000"}`))
	})

	price := 50000.0
	ro := true
	tif := types.TIFGTX
	clientOrderID := "engine-close-1"
	resp, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol:        "BTC/USDT:PERP",
		Type:          types.OrderTypeLimit,
		Side:          types.OrderSideSell,
		Amount:        1,
		Price:         &price,
		ReduceOnly:    &ro,
		TimeInForce:   &tif,
		ClientOrderID: &clientOrderID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	q := query()
	assert.Equal(t, "true", q.Get("reduceOnly"))
	assert.Equal(t, "GTX", q.Get("timeInForce"))
	assert.Equal(t, "50000.00000000", q.Get("price"))
	assert.Equal(t, "engine-close-1", q.Get("newClientOrderId"))
}

func TestBinanceSetLeverage(t *testing.T) {
	var capturedPath string
	client, query := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol": "BTCUSDT", "leverage": 5}`))
	})

	err := client.SetLeverage(context.Background(), "BTC/USDT:PERP", 5)
	require.NoError(t, err)

	assert.Equal(t, "/fapi/v1/leverage", capturedPath)
	q := query()
	assert.Equal(t, "BTCUSDT", q.Get("symbol"))
	assert.Equal(t, "5", q.Get("leverage"))
}

func TestBinanceSetLeverageOutOfRange(t *testing.T) {
	client, _ := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("request should not be sent for out-of-range leverage")
	})
	err := client.SetLeverage(context.Background(), "BTC/USDT:PERP", 0)
	require.Error(t, err)
}

func TestBinanceSetLeverageMismatch(t *testing.T) {
	client, _ := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol": "BTCUSDT", "leverage": 1}`))
	})
	err := client.SetLeverage(context.Background(), "BTC/USDT:PERP", 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 5 got 1")
}

func TestBinanceFetchLotSize(t *testing.T) {
	client, query := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/fapi/v1/exchangeInfo", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"symbols": [{
				"symbol": "BTCUSDT",
				"filters": [
					{"filterType": "PRICE_FILTER", "stepSize": "0.01"},
					{"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}
				]
			}]
		}`))
	})

	step, minQty, err := client.FetchLotSize(context.Background(), "BTC/USDT:PERP")
	require.NoError(t, err)
	assert.Equal(t, 0.001, step)
	assert.Equal(t, 0.001, minQty)
	assert.Equal(t, "BTCUSDT", query().Get("symbol"))
}

func TestBinanceSubmitOrderAPIError(t *testing.T) {
	client, _ := newBinanceTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-1104,"msg":"Not all sent parameters were read"}`))
	})

	_, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 0.1,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-1104")
}

func TestBinanceSignedRequestHmac(t *testing.T) {
	// Verify the request is actually HMAC-signed (the engine's live link
	// depends on valid signatures).
	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"assets":[]}`))
	}))
	defer srv.Close()

	client := NewBinance(Config{APIKey: "k", APISecret: "s"})
	client.baseURL = srv.URL

	_, err := client.FetchBalance(context.Background())
	require.NoError(t, err)

	q, err := url.ParseQuery(capturedQuery)
	require.NoError(t, err)
	sig := q.Get("signature")
	require.NotEmpty(t, sig, "signed request must carry a signature")

	// Recompute the expected HMAC over the sorted params and compare.
	params := make(map[string]string)
	for k, v := range q {
		if k == "signature" {
			continue
		}
		params[k] = v[0]
	}
	signed, err := client.signRequest(params)
	require.NoError(t, err)
	rebuilt, err := url.ParseQuery(signed)
	require.NoError(t, err)
	assert.Equal(t, sig, rebuilt.Get("signature"))
}
