package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"gorango/mdx/domain/types"
	"gorango/mdx/internal/ws"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	publicWsURL       = "wss://fstream.binance.com/public/stream"
	marketWsURL       = "wss://fstream.binance.com/market/stream"
	testnetPublicURL  = "wss://stream.binancefuture.com/stream"
	testnetMarketURL  = "wss://stream.binancefuture.com/market/stream"
	maxReconnectDelay = 30 * time.Second

	// stallTimeout is the read-deadline that detects silent WS stalls.  Binance
	// sends a ping frame every 3 minutes; a connection that stops sending
	// anything (pings included) for stallTimeout is dead even though the TCP
	// socket stays half-open — without this, readLoop blocks forever and the
	// stream silently loses trades/funding/liquidations while liq_covered
	// still reports the connection as up.  4 minutes is safely past the 3m
	// ping interval so quiet-but-healthy connections never trip it.
	stallTimeout = 4 * time.Minute

	maxSymbolsPerDepthConn = 10
)

type reconnectState struct {
	attempts int
	timer    *time.Timer
	mu       sync.Mutex
}

type wsConn struct {
	conn   *websocket.Conn
	gen    int64
	reconn *reconnectState
	name   string
}

func newWSConn(name string) *wsConn {
	return &wsConn{name: name, reconn: &reconnectState{}}
}

type Client struct {
	publicURL string
	marketURL string

	handler           types.EventHandler
	connectionHandler exchange.ConnectionHandler
	parser            *Parser
	symbols           []string
	stopped           bool

	// OnDepthReset is called when a depth WebSocket reconnects.
	// The provided symbols had their orderbook treap state invalidated
	// and need to be reset in the aggregator.
	OnDepthReset func(symbols []string)

	mu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	depthConns []*wsConn
	marketConn *wsConn
}

func NewClient(testnet bool) *Client {
	c := &Client{
		publicURL:  publicWsURL,
		marketURL:  marketWsURL,
		parser:     NewParser(),
		marketConn: newWSConn("market"),
	}
	if testnet {
		c.publicURL = testnetPublicURL
		c.marketURL = testnetMarketURL
	}
	return c
}

func (c *Client) GetExchangeName() string { return "binance" }

func (c *Client) SetConnectionHandler(handler exchange.ConnectionHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectionHandler = handler
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.depthConns) > 0 || c.marketConn.conn != nil {
		return fmt.Errorf("already connected")
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.stopped = false
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("binance", exchange.ConnectionStatusConnecting)
	}
	return nil
}

func (c *Client) Subscribe(symbols []string, handler types.EventHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx == nil {
		return fmt.Errorf("not connected, call Connect first")
	}
	c.stopped = false
	c.handler = handler
	c.symbols = symbols

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}

	// Connect depth streams split across connections (maxSymbolsPerDepthConn symbols each)
	c.depthConns = nil
	nConns := (len(symbols) + maxSymbolsPerDepthConn - 1) / maxSymbolsPerDepthConn
	chunks := chunkSymbols(symbols, nConns)
	for i, chunk := range chunks {
		streams := c.buildPublicStreams(chunk)
		if len(streams) == 0 {
			continue
		}
		conn, err := c.dialCombined(dialer, c.publicURL, streams)
		if err != nil {
			c.closeAll()
			return fmt.Errorf("depth stream %d: %w", i, err)
		}
		wc := newWSConn(fmt.Sprintf("depth%d", i))
		wc.gen++
		wc.conn = conn
		c.depthConns = append(c.depthConns, wc)
		go c.readLoop(wc)
	}

	// Connect market data stream (trades, funding, liquidations)
	marketStreams := c.buildMarketStreams(symbols)
	if len(marketStreams) > 0 {
		conn, err := c.dialCombined(dialer, c.marketURL, marketStreams)
		if err != nil {
			c.closeAll()
			return fmt.Errorf("market stream: %w", err)
		}
		c.marketConn.gen++
		c.marketConn.conn = conn
		go c.readLoop(c.marketConn)
	}

	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("binance", exchange.ConnectionStatusConnected)
	}

	return nil
}

func (c *Client) closeAll() {
	for _, dc := range c.depthConns {
		if dc.conn != nil {
			_ = dc.conn.Close()
			dc.conn = nil
		}
	}
	c.depthConns = nil
	if c.marketConn.conn != nil {
		_ = c.marketConn.conn.Close()
		c.marketConn.conn = nil
	}
}

func chunkSymbols(symbols []string, n int) [][]string {
	if len(symbols) == 0 {
		return nil
	}
	chunks := make([][]string, n)
	for i, s := range symbols {
		chunks[i%n] = append(chunks[i%n], s)
	}
	var result [][]string
	for _, ch := range chunks {
		if len(ch) > 0 {
			result = append(result, ch)
		}
	}
	return result
}

func (c *Client) dialCombined(dialer websocket.Dialer, baseURL string, streams []string) (*websocket.Conn, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()
	conn, _, err := dialer.DialContext(c.ctx, u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Silent-stall detection: set an initial read deadline and extend it on
	// every ping (gorilla handles control frames internally, so ReadMessage
	// never sees them — the ping handler is the only place a quiet-but-alive
	// connection can refresh the deadline).
	_ = conn.SetReadDeadline(time.Now().Add(stallTimeout))
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(stallTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})
	return conn, nil
}

func (c *Client) buildPublicStreams(symbols []string) []string {
	streams := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		streams = append(streams, c.toExchangeSymbol(symbol)+"@depth@100ms")
	}
	return streams
}

func (c *Client) buildMarketStreams(symbols []string) []string {
	streams := make([]string, 0, len(symbols)*3)
	for _, symbol := range symbols {
		sym := c.toExchangeSymbol(symbol)
		streams = append(streams,
			sym+"@aggTrade",
			sym+"@markPrice@1s",
			sym+"@forceOrder",
		)
	}
	return streams
}

func (c *Client) toExchangeSymbol(canonical string) string {
	s := strings.ToLower(canonical)
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ReplaceAll(s, "perp", "")
	return s
}

func (c *Client) readLoop(wc *wsConn) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Extend the read deadline on every data message; a stall (no data AND
		// no pings for stallTimeout) makes ReadMessage return a timeout, which
		// falls through to handleDisconnect → reconnect below.
		if err := wc.conn.SetReadDeadline(time.Now().Add(stallTimeout)); err != nil {
			c.handleDisconnect(wc)
			return
		}
		_, message, err := wc.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("[Binance:%s] WebSocket error: %v\n", wc.name, err)
			}

			c.mu.RLock()
			currentGen := c.currentGen(wc.name)
			c.mu.RUnlock()

			if wc.gen != currentGen {
				return
			}
			c.handleDisconnect(wc)
			return
		}

		events, err := c.parser.ParseStreamMessage(message)
		if err != nil {
			fmt.Printf("[Binance:%s] Parse error: %v\n", wc.name, err)
			continue
		}

		c.mu.RLock()
		handler := c.handler
		c.mu.RUnlock()
		if handler != nil {
			for _, event := range events {
				handler(event)
			}
		}
	}
}

func (c *Client) currentGen(name string) int64 {
	if name == "market" {
		return c.marketConn.gen
	}
	for _, dc := range c.depthConns {
		if dc.name == name {
			return dc.gen
		}
	}
	return 0
}

func (c *Client) handleDisconnect(wc *wsConn) {
	c.mu.RLock()
	stopped := c.stopped
	c.mu.RUnlock()
	if stopped {
		return
	}

	wc.reconn.mu.Lock()
	if wc.reconn.timer != nil {
		wc.reconn.mu.Unlock()
		return
	}
	delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(wc.reconn.attempts))))
	fmt.Printf("[Binance:%s] Scheduling reconnect in %v (attempt %d)\n", wc.name, delay, wc.reconn.attempts+1)
	wc.reconn.timer = time.AfterFunc(delay, func() {
		c.reconnect(wc)
	})
	wc.reconn.mu.Unlock()

	// Only the market connection carries trades, funding, and liquidations;
	// gate liquidation coverage status on it, not on depth connection health.
	if wc.name == "market" && c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("binance", exchange.ConnectionStatusReconnecting)
	}
}

func (c *Client) reconnect(wc *wsConn) {
	c.mu.RLock()
	symbols := c.symbols
	handler := c.handler
	c.mu.RUnlock()

	if handler == nil || len(symbols) == 0 {
		return
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}

	c.mu.Lock()
	if wc.conn != nil {
		_ = wc.conn.Close()
		wc.conn = nil
	}
	c.mu.Unlock()

	var streams []string
	var baseURL string
	if wc.name == "market" {
		streams = c.buildMarketStreams(symbols)
		baseURL = c.marketURL
	} else {
		// Find which chunk this depth connection was responsible for
		chunks := chunkSymbols(symbols, len(c.depthConns))
		idx := -1
		for i, dc := range c.depthConns {
			if dc.name == wc.name {
				idx = i
				break
			}
		}
		if idx >= 0 && idx < len(chunks) {
			streams = c.buildPublicStreams(chunks[idx])
		} else {
			streams = c.buildPublicStreams(symbols)
		}
		baseURL = c.publicURL
	}

	fmt.Printf("[Binance:%s] Attempting to reconnect...\n", wc.name)
	conn, err := c.dialCombined(dialer, baseURL, streams)
	if err != nil {
		fmt.Printf("[Binance:%s] Reconnect failed: %v\n", wc.name, err)
		c.handleDisconnect(wc)
		return
	}

	c.mu.Lock()
	wc.gen++
	wc.conn = conn
	c.mu.Unlock()

	wc.reconn.mu.Lock()
	wc.reconn.attempts = 0
	wc.reconn.timer = nil
	wc.reconn.mu.Unlock()

	// Fire depth reset callback for affected symbols
	if c.OnDepthReset != nil && wc.name != "market" {
		chunks := chunkSymbols(symbols, len(c.depthConns))
		idx := -1
		for i, dc := range c.depthConns {
			if dc.name == wc.name {
				idx = i
				break
			}
		}
		if idx >= 0 && idx < len(chunks) {
			c.OnDepthReset(chunks[idx])
		}
	}

	fmt.Printf("[Binance:%s] Reconnected successfully\n", wc.name)
	if wc.name == "market" && c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("binance", exchange.ConnectionStatusConnected)
	}
	go c.readLoop(wc)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true

	for _, wc := range append(c.depthConns, c.marketConn) {
		if wc == nil || wc.reconn == nil {
			continue
		}
		wc.reconn.mu.Lock()
		if wc.reconn.timer != nil {
			wc.reconn.timer.Stop()
			wc.reconn.timer = nil
		}
		wc.reconn.mu.Unlock()
	}

	if c.cancel != nil {
		c.cancel()
	}
	c.closeAll()
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("binance", exchange.ConnectionStatusDisconnected)
	}
	return nil
}

func (c *Client) Unsubscribe(_ []string) error {
	return nil
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return false
	}
	if c.marketConn.conn != nil {
		return true
	}
	for _, dc := range c.depthConns {
		if dc.conn != nil {
			return true
		}
	}
	return false
}

type AggTrade struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeID      int64  `json:"a"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	Timestamp    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type BookTicker struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidQty    string `json:"B"`
	AskPrice  string `json:"a"`
	AskQty    string `json:"A"`
}

type DepthUpdate struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type MarkPriceUpdate struct {
	EventType       string `json:"e"`
	EventTime       int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	EstimatedPrice  string `json:"P"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

type ForceOrderData struct {
	Symbol          string `json:"s"`
	Side            string `json:"S"`
	OrderType       string `json:"o"`
	TimeInForce     string `json:"f"`
	Quantity        string `json:"q"`
	Price           string `json:"p"`
	AveragePrice    string `json:"ap"`
	OrderStatus     string `json:"X"`
	LastFilledQty   string `json:"l"`
	LastFilledPrice string `json:"L"`
	TradeTime       int64  `json:"T"`
}

type ForceOrder struct {
	EventType string         `json:"e"`
	EventTime int64          `json:"E"`
	Order     ForceOrderData `json:"o"`
}

type CombinedStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}
