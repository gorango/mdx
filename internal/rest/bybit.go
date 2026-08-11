package rest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gorango/mdx/domain/symbols"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type BybitClient struct {
	id      string
	baseURL string
	apiKey  string
	secret  string
	client  *http.Client
	testnet bool
}

func NewBybit(cfg Config) *BybitClient {
	baseURL := "https://api.bybit.com"
	if cfg.Testnet {
		baseURL = "https://api-testnet.bybit.com"
	}
	return &BybitClient{
		id:      "bybit",
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		secret:  cfg.APISecret,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		testnet: cfg.Testnet,
	}
}

func (c *BybitClient) ID() string {
	return c.id
}

// bybitRecvWindow is the request validity window used for all signed calls.
const bybitRecvWindow = "5000"

// signedRequest performs an authenticated Bybit v5 request and verifies the
// business-level retCode. For POST the params are sent as a JSON body and the
// signature covers timestamp+api_key+recv_window+body; for GET the params are
// sent as a URL query and the signature covers timestamp+api_key+recv_window+
// queryString (the canonical Bybit v5 auth scheme).
func (c *BybitClient) signedRequest(ctx context.Context, method, path string, params map[string]string, body any) ([]byte, error) {
	if c.secret == "" {
		return nil, fmt.Errorf("API secret required for authenticated bybit request")
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	recvWindow := params["recvWindow"]
	if recvWindow == "" {
		recvWindow = bybitRecvWindow
	}

	var reqURL string
	var payload string
	var bodyReader io.Reader

	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
		payload = timestamp + c.apiKey + recvWindow + string(bodyBytes)
		reqURL = c.baseURL + path
	} else {
		q := url.Values{}
		for k, v := range params {
			if k == "timestamp" || k == "recvWindow" {
				continue
			}
			q.Set(k, v)
		}
		queryString := q.Encode() // url.Values.Encode sorts keys
		payload = timestamp + c.apiKey + recvWindow + queryString
		reqURL = c.baseURL + path + "?" + queryString
	}

	signature := c.hmacSign(payload)

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-BAPI-API-KEY", c.apiKey)
	req.Header.Set("X-BAPI-TIMESTAMP", timestamp)
	req.Header.Set("X-BAPI-RECV-WINDOW", recvWindow)
	req.Header.Set("X-BAPI-SIGN", signature)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bybit API error: %s", string(respBody))
	}

	// Bybit reports business errors with HTTP 200 and a non-zero retCode —
	// ignoring this silently "succeeded" failed orders before.
	var envelope struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
	}
	if err := json.Unmarshal(respBody, &envelope); err == nil && envelope.RetCode != 0 {
		return nil, fmt.Errorf("bybit API error: retCode=%d retMsg=%q", envelope.RetCode, envelope.RetMsg)
	}

	return respBody, nil
}

// hmacSign computes HMAC-SHA256(secret, payload) hex-encoded, per Bybit v5.
func (c *BybitClient) hmacSign(payload string) string {
	mac := hmac.New(sha256.New, []byte(c.secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// sign is kept for compatibility with public endpoints; it builds the query
// string and appends a signature. Public endpoints ignore the signature, so
// this only needs to be deterministic.
func (c *BybitClient) sign(params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	queryString := q.Encode()
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	recvWindow := params["recvWindow"]
	if recvWindow == "" {
		recvWindow = bybitRecvWindow
	}
	payload := timestamp + c.apiKey + recvWindow + queryString
	return queryString + "&timestamp=" + timestamp + "&recvWindow=" + recvWindow + "&sign=" + c.hmacSign(payload)
}

func (c *BybitClient) FetchOHLCV(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
	if limit <= 0 {
		limit = 1000
	}

	params := map[string]string{
		"category": "linear",
		"symbol":   symbols.CanonicalToExchange(symbol, "bybit"),
		"interval": tf,
		"limit":    strconv.Itoa(limit),
	}
	if since > 0 {
		params["start"] = strconv.FormatInt(since, 10)
	}

	reqURL := c.baseURL + "/v5/market/kline?" + c.sign(params)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch klines: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bybit API error: %s", string(body))
	}

	var raw struct {
		Data struct {
			List [][]interface{} `json:"list"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	bars := make([]types.Bar, 0, len(raw.Data.List))
	tfMs := timeframe.MustParse(tf).Ms
	for i := len(raw.Data.List) - 1; i >= 0; i-- {
		o := raw.Data.List[i]
		if len(o) < 6 {
			continue
		}
		ts, _ := strconv.ParseInt(o[0].(string), 10, 64)
		open, _ := strconv.ParseFloat(o[1].(string), 64)
		high, _ := strconv.ParseFloat(o[2].(string), 64)
		low, _ := strconv.ParseFloat(o[3].(string), 64)
		close, _ := strconv.ParseFloat(o[4].(string), 64)
		vol, _ := strconv.ParseFloat(o[5].(string), 64)

		bars = append(bars, types.Bar{
			Time:   time.UnixMilli(ts + tfMs),
			Open:   open,
			High:   high,
			Low:    low,
			Close:  close,
			Volume: vol,
		})
	}

	return bars, nil
}

func (c *BybitClient) FetchBalance(ctx context.Context) (*types.Balance, error) {
	if c.secret == "" {
		return &types.Balance{
			Free:  map[string]float64{},
			Used:  map[string]float64{},
			Total: map[string]float64{},
		}, nil
	}

	params := map[string]string{
		"category":    "linear",
		"accountType": "UNIFIED",
	}

	body, err := c.signedRequest(ctx, "GET", "/v5/account/wallet-balance", params, nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Data struct {
			Coins []struct {
				Coin                string `json:"coin"`
				AvailableToWithdraw string `json:"availableToWithdraw"`
				TotalOrderIM        string `json:"totalOrderIM"`
				TotalPositionIM     string `json:"totalPositionIM"`
				WalletBalance       string `json:"walletBalance"`
			} `json:"coin"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	balance := &types.Balance{
		Free:  make(map[string]float64),
		Used:  make(map[string]float64),
		Total: make(map[string]float64),
	}

	for _, a := range raw.Data.Coins {
		available, _ := strconv.ParseFloat(a.AvailableToWithdraw, 64)
		wallet, _ := strconv.ParseFloat(a.WalletBalance, 64)
		orderIM, _ := strconv.ParseFloat(a.TotalOrderIM, 64)
		positionIM, _ := strconv.ParseFloat(a.TotalPositionIM, 64)
		used := orderIM + positionIM

		balance.Total[a.Coin] = wallet
		balance.Free[a.Coin] = available
		balance.Used[a.Coin] = used
	}

	return balance, nil
}

func (c *BybitClient) FetchPositions(ctx context.Context) ([]types.Position, error) {
	if c.secret == "" {
		return nil, nil
	}

	params := map[string]string{
		"category": "linear",
	}

	body, err := c.signedRequest(ctx, "GET", "/v5/position/list", params, nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Data struct {
			List []struct {
				Symbol     string `json:"symbol"`
				Size       string `json:"size"`
				EntryPrice string `json:"entryPrice"`
				Side       string `json:"side"`
				Leverage   string `json:"leverage"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	var positions []types.Position
	for _, p := range raw.Data.List {
		size, _ := strconv.ParseFloat(p.Size, 64)
		if size == 0 {
			continue
		}

		entryPrice, _ := strconv.ParseFloat(p.EntryPrice, 64)
		lev, _ := strconv.ParseInt(p.Leverage, 10, 32)

		side := types.PositionSideLong
		if p.Side == "Sell" {
			side = types.PositionSideShort
		}

		positions = append(positions, types.Position{
			Symbol:   symbols.ExchangeToCanonical("bybit", p.Symbol),
			Size:     size,
			AvgPrice: entryPrice,
			Side:     side,
			Leverage: func() *int { v := int(lev); return &v }(),
		})
	}

	return positions, nil
}

func (c *BybitClient) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	if c.secret == "" {
		return nil, fmt.Errorf("API secret required for trading")
	}

	side := "Buy"
	if req.Side == types.OrderSideSell {
		side = "Sell"
	}

	orderType := "Market"
	if req.Type == types.OrderTypeLimit {
		orderType = "Limit"
	}

	order := map[string]any{
		"category":  "linear",
		"symbol":    symbols.CanonicalToExchange(req.Symbol, "bybit"),
		"side":      side,
		"orderType": orderType,
		"qty":       strconv.FormatFloat(req.Amount, 'f', 8, 64),
	}

	if req.Type == types.OrderTypeLimit && req.Price != nil {
		order["price"] = strconv.FormatFloat(*req.Price, 'f', 8, 64)
		tif := types.TIFGTC
		if req.TimeInForce != nil {
			tif = *req.TimeInForce
		}
		order["timeInForce"] = string(tif)
	}

	if req.Leverage != nil {
		order["leverage"] = strconv.Itoa(*req.Leverage)
	}
	if req.ReduceOnly != nil && *req.ReduceOnly {
		order["reduceOnly"] = true
	}
	if req.ClientOrderID != nil && *req.ClientOrderID != "" {
		order["orderLinkId"] = *req.ClientOrderID
	}

	// Map the unified position side/type to Bybit's positionIdx:
	// 0 = one-way, 1 = hedge long, 2 = hedge short.
	switch {
	case req.PositionType != nil:
		order["positionIdx"] = *req.PositionType
	case req.PositionSide != nil:
		switch *req.PositionSide {
		case "long", "LONG":
			order["positionIdx"] = 1
		case "short", "SHORT":
			order["positionIdx"] = 2
		}
	}
	if req.OpenType != nil {
		order["openType"] = *req.OpenType
	}

	body, err := c.signedRequest(ctx, "POST", "/v5/order/create", nil, order)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Data struct {
			OrderID string `json:"orderId"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Bybit acknowledges orders asynchronously — create-order returns only the
	// order ID; fill status arrives via the order stream / position updates.
	return &types.OrderResponse{
		ID:        raw.Data.OrderID,
		Symbol:    req.Symbol,
		Type:      req.Type,
		Side:      req.Side,
		Amount:    req.Amount,
		Filled:    0,
		Remaining: req.Amount,
		Status:    types.OrderStatusOpen,
	}, nil
}

func (c *BybitClient) CancelOrder(ctx context.Context, orderID, symbol string) error {
	if c.secret == "" {
		return fmt.Errorf("API secret required")
	}

	order := map[string]any{
		"category": "linear",
		"symbol":   symbols.CanonicalToExchange(symbol, "bybit"),
		"orderId":  orderID,
	}

	_, err := c.signedRequest(ctx, "POST", "/v5/order/cancel", nil, order)
	return err
}

func (c *BybitClient) FetchOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	if c.secret == "" {
		return nil, nil
	}

	params := map[string]string{
		"category": "linear",
	}
	if symbol != "" {
		params["symbol"] = symbols.CanonicalToExchange(symbol, "bybit")
	}

	body, err := c.signedRequest(ctx, "GET", "/v5/order/realtime", params, nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Data struct {
			List []struct {
				OrderID     string `json:"orderId"`
				Symbol      string `json:"symbol"`
				Side        string `json:"side"`
				OrderType   string `json:"orderType"`
				Qty         string `json:"qty"`
				Price       string `json:"price"`
				OrderStatus string `json:"orderStatus"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	orders := make([]types.OrderResponse, 0, len(raw.Data.List))
	for _, o := range raw.Data.List {
		qty, _ := strconv.ParseFloat(o.Qty, 64)
		price, _ := strconv.ParseFloat(o.Price, 64)
		side := types.OrderSideBuy
		if o.Side == "Sell" {
			side = types.OrderSideSell
		}

		orders = append(orders, types.OrderResponse{
			ID:        o.OrderID,
			Symbol:    symbols.ExchangeToCanonical("bybit", o.Symbol),
			Type:      types.OrderType(o.OrderType),
			Side:      side,
			Amount:    qty,
			Filled:    0,
			Remaining: qty,
			Price:     price,
			Status:    types.OrderStatus(o.OrderStatus),
		})
	}

	return orders, nil
}

// SetLeverage changes the initial leverage for a symbol (linear category).
func (c *BybitClient) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	if c.secret == "" {
		return fmt.Errorf("API secret required")
	}
	if leverage < 1 || leverage > 100 {
		return fmt.Errorf("leverage %d out of range [1,100]", leverage)
	}

	order := map[string]any{
		"category":     "linear",
		"symbol":       symbols.CanonicalToExchange(symbol, "bybit"),
		"buyLeverage":  strconv.Itoa(leverage),
		"sellLeverage": strconv.Itoa(leverage),
	}

	_, err := c.signedRequest(ctx, "POST", "/v5/position/set-leverage", nil, order)
	return err
}

// FetchLotSize returns the quantity step and minimum order quantity for a
// symbol from Bybit's instruments-info endpoint (lotSizeFilter).
func (c *BybitClient) FetchLotSize(ctx context.Context, symbol string) (float64, float64, error) {
	exchangeSymbol := symbols.CanonicalToExchange(symbol, "bybit")

	params := map[string]string{
		"category": "linear",
		"symbol":   exchangeSymbol,
	}

	body, err := c.signedRequest(ctx, "GET", "/v5/market/instruments-info", params, nil)
	if err != nil {
		return 0, 0, err
	}

	var raw struct {
		Data struct {
			List []struct {
				Symbol        string `json:"symbol"`
				LotSizeFilter struct {
					Step        string `json:"step"`
					MinOrderQty string `json:"minOrderQty"`
				} `json:"lotSizeFilter"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return 0, 0, fmt.Errorf("decode response: %w", err)
	}

	for _, i := range raw.Data.List {
		if i.Symbol != exchangeSymbol {
			continue
		}
		step, _ := strconv.ParseFloat(i.LotSizeFilter.Step, 64)
		minQty, _ := strconv.ParseFloat(i.LotSizeFilter.MinOrderQty, 64)
		if step <= 0 {
			return 0, 0, fmt.Errorf("lot size step for %s is %q", exchangeSymbol, i.LotSizeFilter.Step)
		}
		return step, minQty, nil
	}

	return 0, 0, fmt.Errorf("symbol %s not found in instruments-info", exchangeSymbol)
}

func (c *BybitClient) DownloadMonthlyZip(ctx context.Context, symbol string, year, month int) ([]types.Bar, error) {
	return nil, nil
}
