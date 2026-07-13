package rest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"io"
	"net/http"
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

func (c *BybitClient) sign(params map[string]string) string {
	_sorted := func(in map[string]string) string {
		var keys []string
		for k := range in {
			keys = append(keys, k)
		}
		result := ""
		for _, k := range keys {
			if result != "" {
				result += "&"
			}
			result += k + "=" + in[k]
		}
		return result
	}

	paramStr := _sorted(params)
	signature := hmac.New(sha256.New, []byte(c.secret)).Sum(nil)
	paramStr += "&sign=" + hex.EncodeToString(signature)
	return paramStr
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
	defer resp.Body.Close()

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
		"timestamp":   strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow":  "5000",
	}

	reqURL := c.baseURL + "/v5/account/wallet-balance?" + c.sign(params)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-BAPI-API-KEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch balance: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bybit API error: %s", string(body))
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
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
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
		"category":   "linear",
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	reqURL := c.baseURL + "/v5/position/list?" + c.sign(params)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-BAPI-API-KEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch positions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bybit API error: %s", string(body))
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
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
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

	params := map[string]string{
		"category":   "linear",
		"symbol":     symbols.CanonicalToExchange(req.Symbol, "bybit"),
		"side":       side,
		"orderType":  orderType,
		"qty":        strconv.FormatFloat(req.Amount, 'f', 8, 64),
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	if req.Type == types.OrderTypeLimit && req.Price != nil {
		params["price"] = strconv.FormatFloat(*req.Price, 'f', 8, 64)
		params["timeInForce"] = "GTC"
	}

	body, _ := json.Marshal(params)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v5/order/create?"+c.sign(params), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("X-BAPI-API-KEY", c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("submit order: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bybit API error: %s", string(respBody))
	}

	var raw struct {
		Data struct {
			OrderID string `json:"orderId"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBody, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &types.OrderResponse{
		ID:        raw.Data.OrderID,
		Symbol:    req.Symbol,
		Type:      req.Type,
		Side:      req.Side,
		Amount:    req.Amount,
		Filled:    0,
		Remaining: req.Amount,
		Price:     0,
		Status:    types.OrderStatusOpen,
	}, nil
}

func (c *BybitClient) CancelOrder(ctx context.Context, orderID, symbol string) error {
	if c.secret == "" {
		return fmt.Errorf("API secret required")
	}

	params := map[string]string{
		"category":   "linear",
		"symbol":     symbols.CanonicalToExchange(symbol, "bybit"),
		"orderId":    orderID,
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	body, _ := json.Marshal(params)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v5/order/cancel?"+c.sign(params), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("X-BAPI-API-KEY", c.apiKey)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bybit API error: %s", string(respBody))
	}

	return nil
}

func (c *BybitClient) FetchOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	if c.secret == "" {
		return nil, nil
	}

	params := map[string]string{
		"category":   "linear",
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}
	if symbol != "" {
		params["symbol"] = symbols.CanonicalToExchange(symbol, "bybit")
	}

	reqURL := c.baseURL + "/v5/order/realtime?" + c.sign(params)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-BAPI-API-KEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch open orders: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bybit API error: %s", string(body))
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
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
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

func (c *BybitClient) DownloadMonthlyZip(ctx context.Context, symbol string, year, month int) ([]types.Bar, error) {
	return nil, nil
}
