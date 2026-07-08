package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/ws"

	"github.com/gorilla/websocket"
)

const (
	bybitWsURL        = "wss://stream.bybit.com/v5/public/linear"
	bybitTestnetURL   = "wss://stream-testnet.bybit.com/v5/public/linear"
	maxReconnectDelay = 30 * time.Second
)

type Client struct {
	wsURL             string
	conn              *websocket.Conn
	handler           types.EventHandler
	connectionHandler exchange.ConnectionHandler
	parser            *Parser
	symbols           []string
	reconnectAttempts int
	reconnectTimer    *time.Timer
	stopped           bool
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewClient(testnet bool) *Client {
	wsURL := bybitWsURL
	if testnet {
		wsURL = bybitTestnetURL
	}
	return &Client{
		wsURL:  wsURL,
		parser: NewParser(),
	}
}

func (c *Client) GetExchangeName() string {
	return "bybit"
}

func (c *Client) SetConnectionHandler(handler exchange.ConnectionHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectionHandler = handler
}

func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return fmt.Errorf("already connected")
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.stopped = false
	c.reconnectAttempts = 0
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("bybit", exchange.ConnectionStatusConnecting)
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(c.ctx, c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Bybit WebSocket: %w", err)
	}
	c.conn = conn
	go c.readLoop()
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("bybit", exchange.ConnectionStatusConnected)
	}
	return nil
}

func (c *Client) Subscribe(symbols []string, handler types.EventHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("not connected, call Connect first")
	}
	c.handler = handler
	c.symbols = symbols

	args := make([]Arg, 0, len(symbols)*2)
	for _, symbol := range symbols {
		bybitSym := toExchangeSymbol(symbol)
		args = append(args,
			Arg{Channel: "publicTrade", Symbol: bybitSym},
			Arg{Channel: "orderbook.1", Symbol: bybitSym},
		)
	}
	subReq := SubscriptionRequest{Op: "subscribe", Args: args}
	if err := c.conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	return nil
}

func (c *Client) Unsubscribe(symbols []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	symbolSet := make(map[string]bool)
	for _, sym := range symbols {
		symbolSet[sym] = true
	}

	remaining := make([]string, 0)
	for _, sym := range c.symbols {
		if !symbolSet[sym] {
			remaining = append(remaining, sym)
		}
	}

	args := make([]Arg, 0, len(symbols)*2)
	for _, symbol := range symbols {
		bybitSym := toExchangeSymbol(symbol)
		args = append(args,
			Arg{Channel: "publicTrade", Symbol: bybitSym},
			Arg{Channel: "orderbook.1", Symbol: bybitSym},
		)
	}
	unsubReq := SubscriptionRequest{Op: "unsubscribe", Args: args}
	if err := c.conn.WriteJSON(unsubReq); err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	c.symbols = remaining
	return nil
}

func (c *Client) readLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()
		if conn == nil {
			return
		}
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("[Bybit] WebSocket error: %v\n", err)
			}
			c.handleDisconnect()
			return
		}
		var subResp SubscriptionResponse
		if err := json.Unmarshal(message, &subResp); err == nil && subResp.Op == "subscribe" {
			if !subResp.Success {
				fmt.Printf("[Bybit] Subscription failed: %s\n", subResp.RetMsg)
			}
			continue
		}
		events, err := c.parser.ParseMessage(message)
		if err != nil {
			fmt.Printf("[Bybit] Parse error: %v\n", err)
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

func (c *Client) handleDisconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || c.reconnectTimer != nil {
		return
	}
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("bybit", exchange.ConnectionStatusReconnecting)
	}
	delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(c.reconnectAttempts))))
	fmt.Printf("[Bybit] Scheduling reconnect in %v (attempt %d)\n", delay, c.reconnectAttempts+1)
	c.reconnectTimer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		c.reconnectTimer = nil
		c.reconnectAttempts++
		c.mu.Unlock()
		c.reconnect()
	})
}

func (c *Client) reconnect() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	fmt.Println("[Bybit] Attempting to reconnect...")
	if err := c.Close(); err != nil {
		fmt.Printf("[Bybit] Error closing connection: %v\n", err)
	}
	if err := c.Connect(c.ctx); err != nil {
		fmt.Printf("[Bybit] Reconnect failed: %v\n", err)
		c.handleDisconnect()
		return
	}
	if err := c.Subscribe(c.symbols, c.handler); err != nil {
		fmt.Printf("[Bybit] Resubscribe failed: %v\n", err)
		c.handleDisconnect()
		return
	}
	fmt.Println("[Bybit] Reconnected successfully")
	c.mu.Lock()
	c.reconnectAttempts = 0
	c.mu.Unlock()
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("bybit", exchange.ConnectionStatusConnected)
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("bybit", exchange.ConnectionStatusDisconnected)
	}
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.stopped
}

type TradeMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  []struct {
		Symbol string `json:"s"`
		Price  string `json:"p"`
		Qty    string `json:"v"`
		Side   string `json:"S"`
		Time   string `json:"T"`
	} `json:"data"`
}

type OrderbookMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  struct {
		Symbol  string     `json:"s"`
		Bids    [][]string `json:"b"`
		Asks    [][]string `json:"a"`
		Seq     int64      `json:"seq"`
		PrevSeq int64      `json:"prevSeq,omitempty"`
	} `json:"data"`
}

type KlineMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Ts    int64  `json:"ts"`
	Data  []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Interval  string `json:"interval"`
		Open      string `json:"open"`
		Close     string `json:"close"`
		High      string `json:"high"`
		Low       string `json:"low"`
		Volume    string `json:"volume"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
		Timestamp int64  `json:"timestamp"`
	} `json:"data"`
}

type SubscriptionRequest struct {
	Op   string `json:"op"`
	Args []Arg  `json:"args"`
}

type Arg struct {
	Channel string `json:"channel"`
	Symbol  string `json:"symbol"`
}

type SubscriptionResponse struct {
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`
	ConnID  string `json:"conn_id"`
	Op      string `json:"op"`
}

func toExchangeSymbol(canonical string) string {
	s := strings.ToUpper(canonical)
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ReplaceAll(s, "PERP", "")
	return s
}
