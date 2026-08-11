package rest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"gorango/mdx/domain/types"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBybitTestClient starts an httptest server capturing the last request
// (headers + body) and returns a BybitClient pointed at it.
func newBybitTestClient(t *testing.T, handler http.HandlerFunc) (*BybitClient, func() (http.Header, []byte, string)) {
	t.Helper()
	var lastHeader http.Header
	var lastBody []byte
	var lastPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastHeader = r.Header.Clone()
		lastPath = r.URL.Path
		lastBody, _ = io.ReadAll(r.Body)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	client := NewBybit(Config{APIKey: "test-api-key", APISecret: "test-secret"})
	client.baseURL = srv.URL
	return client, func() (http.Header, []byte, string) { return lastHeader, lastBody, lastPath }
}

func bybitOK(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func TestBybitPostSignatureValid(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{"retCode":0,"retMsg":"OK","result":{"orderId":"o1"},"time":1}`))

	_, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 0.1,
	})
	require.NoError(t, err)

	header, body, path := capture()
	assert.Equal(t, "/v5/order/create", path)
	assert.Equal(t, "test-api-key", header.Get("X-BAPI-API-KEY"))
	assert.Equal(t, "5000", header.Get("X-BAPI-RECV-WINDOW"))
	timestamp := header.Get("X-BAPI-TIMESTAMP")
	require.NotEmpty(t, timestamp)

	// Bybit v5: sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + body).
	expected := hmacSHA256("test-secret", timestamp+"test-api-key"+"5000"+string(body))
	assert.Equal(t, expected, header.Get("X-BAPI-SIGN"), "POST signature must cover the JSON body")
}

func TestBybitGetSignatureValid(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{"retCode":0,"retMsg":"OK","result":{"list":[]},"time":1}`))

	_, err := client.FetchPositions(context.Background())
	require.NoError(t, err)

	header, _, path := capture()
	assert.Equal(t, "/v5/position/list", path)
	timestamp := header.Get("X-BAPI-TIMESTAMP")
	require.NotEmpty(t, timestamp)

	// GET signature covers timestamp + api_key + recv_window + sorted query.
	expected := hmacSHA256("test-secret", timestamp+"test-api-key"+"5000"+"category=linear")
	assert.Equal(t, expected, header.Get("X-BAPI-SIGN"), "GET signature must cover the sorted query string")
}

func TestBybitErrorRetCodeWithHTTP200(t *testing.T) {
	// Bybit returns HTTP 200 with a non-zero retCode for business failures —
	// these must surface as errors, not silent "successful" empty orders.
	client, _ := newBybitTestClient(t, bybitOK(`{"retCode":10001,"retMsg":"Request parameter error","result":{},"time":1}`))

	_, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 0.1,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retCode=10001")
}

func TestBybitSubmitOrderMapping(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{"retCode":0,"retMsg":"OK","result":{"orderId":"o-42"},"time":1}`))

	leverage := 10
	ro := true
	tif := types.TIFGTC
	positionType := 1
	price := 50000.0
	_, err := client.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol:       "BTC/USDT:PERP",
		Type:         types.OrderTypeLimit,
		Side:         types.OrderSideSell,
		Amount:       0.1,
		Price:        &price,
		Leverage:     &leverage,
		ReduceOnly:   &ro,
		TimeInForce:  &tif,
		PositionType: &positionType,
	})
	require.NoError(t, err)

	_, body, _ := capture()
	var order map[string]any
	require.NoError(t, json.Unmarshal(body, &order))

	assert.Equal(t, "linear", order["category"])
	assert.Equal(t, "BTCUSDT", order["symbol"])
	assert.Equal(t, "Sell", order["side"])
	assert.Equal(t, "Limit", order["orderType"])
	assert.Equal(t, "10", order["leverage"])
	assert.Equal(t, true, order["reduceOnly"])
	assert.Equal(t, "GTC", order["timeInForce"])
	assert.Equal(t, float64(1), order["positionIdx"])
}

func TestBybitSetLeverage(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{"retCode":0,"retMsg":"OK","result":{},"time":1}`))

	err := client.SetLeverage(context.Background(), "BTC/USDT:PERP", 25)
	require.NoError(t, err)

	_, body, path := capture()
	assert.Equal(t, "/v5/position/set-leverage", path)
	var order map[string]any
	require.NoError(t, json.Unmarshal(body, &order))
	assert.Equal(t, "25", order["buyLeverage"])
	assert.Equal(t, "25", order["sellLeverage"])
	assert.Equal(t, "BTCUSDT", order["symbol"])
}

func TestBybitFetchLotSize(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{
		"retCode":0,"retMsg":"OK","time":1,
		"result":{"list":[{
			"symbol":"BTCUSDT",
			"lotSizeFilter":{"step":"0.001","minOrderQty":"0.001"}
		}]}
	}`))

	step, minQty, err := client.FetchLotSize(context.Background(), "BTC/USDT:PERP")
	require.NoError(t, err)
	assert.Equal(t, 0.001, step)
	assert.Equal(t, 0.001, minQty)

	_, _, path := capture()
	assert.Equal(t, "/v5/market/instruments-info", path)
}

func TestBybitCancelOrder(t *testing.T) {
	client, capture := newBybitTestClient(t, bybitOK(`{"retCode":0,"retMsg":"OK","result":{},"time":1}`))

	err := client.CancelOrder(context.Background(), "o-42", "BTC/USDT:PERP")
	require.NoError(t, err)

	_, body, path := capture()
	assert.Equal(t, "/v5/order/cancel", path)
	var order map[string]any
	require.NoError(t, json.Unmarshal(body, &order))
	assert.Equal(t, "o-42", order["orderId"])
	assert.Equal(t, "BTCUSDT", order["symbol"])
}

func hmacSHA256(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
