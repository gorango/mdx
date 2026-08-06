package rest

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type BinanceClient struct {
	id      string
	baseURL string
	apiKey  string
	secret  string
	client  *http.Client
	testnet bool
}

func NewBinance(cfg Config) *BinanceClient {
	baseURL := "https://fapi.binance.com"
	if cfg.Testnet {
		baseURL = "https://testnet.binancefuture.com"
	}
	return &BinanceClient{
		id:      "binance",
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		secret:  cfg.APISecret,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		testnet: cfg.Testnet,
	}
}

func (c *BinanceClient) ID() string {
	return c.id
}

func (c *BinanceClient) signRequest(params map[string]string) (string, error) {
	if c.secret == "" {
		return "", nil
	}

	query := url.Values{}
	for k, v := range params {
		query.Set(k, v)
	}

	payload := query.Encode()
	signature := c.sign(payload)
	query.Set("signature", signature)

	return query.Encode(), nil
}

func (c *BinanceClient) sign(message string) string {
	var sig string
	if strings.HasPrefix(c.secret, "-----BEGIN PRIVATE KEY-----") {
		sig = c.ed25519Sign(message)
	} else {
		mac := hmac.New(sha256.New, []byte(c.secret))
		mac.Write([]byte(message))
		sig = hex.EncodeToString(mac.Sum(nil))
	}
	return sig
}

func (c *BinanceClient) ed25519Sign(message string) string {
	block, _ := pem.Decode([]byte(c.secret))
	if block == nil {
		return ""
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return ""
	}
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return ""
	}
	sig := ed25519.Sign(edKey, []byte(message))
	return base64.StdEncoding.EncodeToString(sig)
}

func (c *BinanceClient) FetchOHLCV(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
	if limit <= 0 {
		limit = 1000
	}

	params := map[string]string{
		"symbol":   symbols.CanonicalToExchange(symbol, "binance"),
		"interval": tf,
		"limit":    strconv.Itoa(limit),
	}
	if since > 0 {
		params["startTime"] = strconv.FormatInt(since, 10)
	}

	endpoint := "/fapi/v1/klines"
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	reqURL := c.baseURL + endpoint + "?" + q.Encode()

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
		return nil, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw [][]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	bars := make([]types.Bar, 0, len(raw))
	tfMs := timeframe.MustParse(tf).Ms
	for _, o := range raw {
		if len(o) < 6 {
			continue
		}
		var ts int64
		var open, high, low, close, vol float64

		_ = json.Unmarshal(o[0], &ts)
		parseFloat(o[1], &open)
		parseFloat(o[2], &high)
		parseFloat(o[3], &low)
		parseFloat(o[4], &close)
		parseFloat(o[5], &vol)

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

func parseFloat(data json.RawMessage, out *float64) {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*out, _ = strconv.ParseFloat(s, 64)
	}
}

func (c *BinanceClient) FetchBalance(ctx context.Context) (*types.Balance, error) {
	if c.secret == "" {
		return &types.Balance{
			Free:  map[string]float64{},
			Used:  map[string]float64{},
			Total: map[string]float64{},
		}, nil
	}

	params := map[string]string{
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	signed, err := c.signRequest(params)
	if err != nil {
		return nil, err
	}

	reqURL := c.baseURL + "/fapi/v2/account?" + signed
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch balance: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		Assets []struct {
			Asset            string `json:"asset"`
			WalletBalance    string `json:"walletBalance"`
			AvailableBalance string `json:"availableBalance"`
			MarginUsed       string `json:"maintMargin"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	balance := &types.Balance{
		Free:  make(map[string]float64),
		Used:  make(map[string]float64),
		Total: make(map[string]float64),
	}

	for _, a := range raw.Assets {
		total, _ := strconv.ParseFloat(a.WalletBalance, 64)
		free, _ := strconv.ParseFloat(a.AvailableBalance, 64)
		used := total - free

		balance.Total[a.Asset] = total
		balance.Free[a.Asset] = free
		balance.Used[a.Asset] = used
	}

	return balance, nil
}

func (c *BinanceClient) FetchPositions(ctx context.Context) ([]types.Position, error) {
	if c.secret == "" {
		return nil, nil
	}

	params := map[string]string{
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	signed, err := c.signRequest(params)
	if err != nil {
		return nil, err
	}

	reqURL := c.baseURL + "/fapi/v2/account?" + signed
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch positions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		Positions []struct {
			Symbol       string `json:"symbol"`
			EntryPrice   string `json:"entryPrice"`
			MarkPrice    string `json:"unrealizedProfit"`
			PositionAmt  string `json:"positionAmt"`
			PositionSide string `json:"positionSide"`
			Leverage     string `json:"leverage"`
		} `json:"positions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	var positions []types.Position
	for _, p := range raw.Positions {
		size, _ := strconv.ParseFloat(p.PositionAmt, 64)
		if size == 0 {
			continue
		}

		entryPrice, _ := strconv.ParseFloat(p.EntryPrice, 64)
		lev, _ := strconv.ParseInt(p.Leverage, 10, 32)

		side := types.PositionSideLong
		if size < 0 || p.PositionSide == "SHORT" {
			side = types.PositionSideShort
		}

		positions = append(positions, types.Position{
			Symbol:   symbols.ExchangeToCanonical("binance", p.Symbol),
			Size:     size,
			AvgPrice: entryPrice,
			Side:     side,
			Leverage: func() *int { v := int(lev); return &v }(),
		})
	}

	return positions, nil
}

func (c *BinanceClient) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	if c.secret == "" {
		return nil, fmt.Errorf("API secret required for trading")
	}

	// Binance requires uppercase side/type values while the domain uses
	// lower-case constants — mapping is mandatory or orders get rejected.
	side := strings.ToUpper(string(req.Side))
	orderType := strings.ToUpper(string(req.Type))
	if side != "BUY" && side != "SELL" {
		return nil, fmt.Errorf("unsupported side %q", req.Side)
	}
	if orderType != "MARKET" && orderType != "LIMIT" {
		return nil, fmt.Errorf("unsupported order type %q", req.Type)
	}

	params := map[string]string{
		"symbol":     symbols.CanonicalToExchange(req.Symbol, "binance"),
		"side":       side,
		"type":       orderType,
		"quantity":   strconv.FormatFloat(req.Amount, 'f', 8, 64),
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	if req.Type == types.OrderTypeLimit {
		if req.Price == nil {
			return nil, fmt.Errorf("limit order requires price")
		}
		params["price"] = strconv.FormatFloat(*req.Price, 'f', 8, 64)
		tif := types.TIFGTC
		if req.TimeInForce != nil {
			tif = *req.TimeInForce
		}
		params["timeInForce"] = string(tif)
	}

	if req.PositionSide != nil {
		params["positionSide"] = *req.PositionSide
	}
	if req.ReduceOnly != nil && *req.ReduceOnly {
		params["reduceOnly"] = "true"
	}
	if req.ClientOrderID != nil && *req.ClientOrderID != "" {
		params["newClientOrderId"] = *req.ClientOrderID
	}

	// Note: initial leverage is NOT an order parameter on Binance USDT-M
	// futures — it must be set via SetLeverage (/fapi/v1/leverage) first.

	signed, err := c.signRequest(params)
	if err != nil {
		return nil, err
	}

	reqURL := c.baseURL + "/fapi/v1/order?" + signed
	httpReq, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("submit order: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Symbol        string `json:"symbol"`
		Side          string `json:"side"`
		Type          string `json:"type"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		AvgPrice      string `json:"avgPrice"`
		Status        string `json:"status"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Binance returns numeric fields as JSON strings.
	price := parseFloatString(raw.Price)
	origQty := parseFloatString(raw.OrigQty)
	executedQty := parseFloatString(raw.ExecutedQty)
	avgPrice := parseFloatString(raw.AvgPrice)

	return &types.OrderResponse{
		ID:        strconv.FormatInt(raw.OrderID, 10),
		Symbol:    symbols.ExchangeToCanonical("binance", raw.Symbol),
		Type:      types.OrderType(strings.ToLower(raw.Type)),
		Side:      types.OrderSide(strings.ToLower(raw.Side)),
		Amount:    origQty,
		Filled:    executedQty,
		Remaining: origQty - executedQty,
		Price:     price,
		Average:   &avgPrice,
		Status:    types.OrderStatus(strings.ToLower(raw.Status)),
	}, nil
}

// parseFloatString parses a numeric string (Binance returns numbers as JSON
// strings), returning 0 on empty or invalid input.
func parseFloatString(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// SetLeverage changes the initial leverage for a symbol on Binance USDT-M
// futures. Leverage is a symbol-level account setting, not an order
// parameter, so it must be called via the dedicated /fapi/v1/leverage
// endpoint before placing a leveraged order.
func (c *BinanceClient) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	if c.secret == "" {
		return fmt.Errorf("API secret required")
	}
	if leverage < 1 || leverage > 125 {
		return fmt.Errorf("leverage %d out of range [1,125]", leverage)
	}

	params := map[string]string{
		"symbol":     symbols.CanonicalToExchange(symbol, "binance"),
		"leverage":   strconv.Itoa(leverage),
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	signed, err := c.signRequest(params)
	if err != nil {
		return err
	}

	reqURL := c.baseURL + "/fapi/v1/leverage?" + signed
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("set leverage: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		Symbol   string `json:"symbol"`
		Leverage int    `json:"leverage"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if raw.Leverage != leverage {
		return fmt.Errorf("set leverage: expected %d got %d for %s", leverage, raw.Leverage, raw.Symbol)
	}
	return nil
}

// FetchLotSize returns the quantity step size and minimum order quantity for
// a symbol from Binance's exchangeInfo filters (LOT_SIZE).
func (c *BinanceClient) FetchLotSize(ctx context.Context, symbol string) (float64, float64, error) {
	exchangeSymbol := symbols.CanonicalToExchange(symbol, "binance")

	q := url.Values{}
	q.Set("symbol", exchangeSymbol)
	reqURL := c.baseURL + "/fapi/v1/exchangeInfo?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("fetch exchange info: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		Symbols []struct {
			Symbol  string `json:"symbol"`
			Filters []struct {
				FilterType string `json:"filterType"`
				StepSize   string `json:"stepSize"`
				MinQty     string `json:"minQty"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return 0, 0, fmt.Errorf("decode response: %w", err)
	}

	for _, s := range raw.Symbols {
		if s.Symbol != exchangeSymbol {
			continue
		}
		for _, f := range s.Filters {
			if f.FilterType == "LOT_SIZE" {
				step, _ := strconv.ParseFloat(f.StepSize, 64)
				minQty, _ := strconv.ParseFloat(f.MinQty, 64)
				if step <= 0 {
					return 0, 0, fmt.Errorf("lot size step for %s is %s", exchangeSymbol, f.StepSize)
				}
				return step, minQty, nil
			}
		}
		return 0, 0, fmt.Errorf("LOT_SIZE filter not found for %s", exchangeSymbol)
	}

	return 0, 0, fmt.Errorf("symbol %s not found in exchangeInfo", exchangeSymbol)
}

func (c *BinanceClient) CancelOrder(ctx context.Context, orderID, symbol string) error {
	if c.secret == "" {
		return fmt.Errorf("API secret required")
	}

	params := map[string]string{
		"symbol":     symbols.CanonicalToExchange(symbol, "binance"),
		"orderId":    orderID,
		"timestamp":  strconv.FormatInt(time.Now().UnixMilli(), 10),
		"recvWindow": "5000",
	}

	signed, err := c.signRequest(params)
	if err != nil {
		return err
	}

	reqURL := c.baseURL + "/fapi/v1/order?" + signed
	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("binance API error: %s", string(body))
	}

	return nil
}

func (c *BinanceClient) FetchOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	if c.secret == "" {
		return nil, nil
	}

	params := map[string]string{
		"timestamp": strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	if symbol != "" {
		params["symbol"] = symbols.CanonicalToExchange(symbol, "binance")
	}

	signed, err := c.signRequest(params)
	if err != nil {
		return nil, err
	}

	reqURL := c.baseURL + "/fapi/v1/openOrders?" + signed
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch open orders: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw []struct {
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Symbol        string `json:"symbol"`
		Side          string `json:"side"`
		Type          string `json:"type"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		Status        string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	orders := make([]types.OrderResponse, 0, len(raw))
	for _, o := range raw {
		origQty := parseFloatString(o.OrigQty)
		executedQty := parseFloatString(o.ExecutedQty)
		orders = append(orders, types.OrderResponse{
			ID:        strconv.FormatInt(o.OrderID, 10),
			Symbol:    symbols.ExchangeToCanonical("binance", o.Symbol),
			Type:      types.OrderType(strings.ToLower(o.Type)),
			Side:      types.OrderSide(strings.ToLower(o.Side)),
			Amount:    origQty,
			Filled:    executedQty,
			Remaining: origQty - executedQty,
			Price:     parseFloatString(o.Price),
			Status:    types.OrderStatus(strings.ToLower(o.Status)),
		})
	}

	return orders, nil
}

type BinanceFuturesTradeReq struct {
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"`
	Type     string  `json:"type"`
	Quantity float64 `json:"quantity"`
	Price    float64 `json:"price,omitempty"`
}

func (c *BinanceClient) DownloadMonthlyZip(ctx context.Context, symbol string, year, month int) ([]types.Bar, error) {
	exchangeSymbol := strings.ToUpper(symbols.CanonicalToExchange(symbol, "binance"))
	url := fmt.Sprintf(
		"https://data.binance.vision/data/futures/um/monthly/klines/%s/1m/%s-1m-%04d-%02d.zip",
		exchangeSymbol, exchangeSymbol, year, month,
	)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download zip: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download zip: status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read zip body: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	var bars []types.Bar
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			continue
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			continue
		}

		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "open_time") {
				continue
			}
			fields := strings.Split(line, ",")
			if len(fields) < 6 {
				continue
			}
			openTime, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				continue
			}
			open, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				continue
			}
			high, err := strconv.ParseFloat(fields[2], 64)
			if err != nil {
				continue
			}
			low, err := strconv.ParseFloat(fields[3], 64)
			if err != nil {
				continue
			}
			close, err := strconv.ParseFloat(fields[4], 64)
			if err != nil {
				continue
			}
			volume, err := strconv.ParseFloat(fields[5], 64)
			if err != nil {
				continue
			}
			bars = append(bars, types.Bar{
				Time:   time.UnixMilli(openTime + timeframe.MustParse("1m").Ms),
				Open:   open,
				High:   high,
				Low:    low,
				Close:  close,
				Volume: volume,
			})
		}
		break
	}

	return bars, nil
}

func (c *BinanceClient) FetchOpenInterest(ctx context.Context, symbol string) (float64, error) {
	exchangeSymbol := symbols.CanonicalToExchange(symbol, "binance")
	endpoint := "/fapi/v1/openInterest"
	q := url.Values{}
	q.Set("symbol", exchangeSymbol)
	reqURL := c.baseURL + endpoint + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch open interest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("binance API error: %s", string(body))
	}

	var raw struct {
		OpenInterest string `json:"openInterest"`
		Symbol       string `json:"symbol"`
		Time         int64  `json:"time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	oi, err := strconv.ParseFloat(raw.OpenInterest, 64)
	if err != nil {
		return 0, fmt.Errorf("parse open interest: %w", err)
	}

	return oi, nil
}
