package hyperliquid

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
	hyperliquidWsURL  = "wss://api.hyperliquid.xyz/ws"
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
	pendingSubs       map[string]bool
	confirmedSubs     map[string]bool
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewClient() *Client {
	return &Client{
		wsURL:         hyperliquidWsURL,
		parser:        NewParser(),
		pendingSubs:   make(map[string]bool),
		confirmedSubs: make(map[string]bool),
	}
}

func (c *Client) GetExchangeName() string {
	return "hyperliquid"
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
		c.connectionHandler.OnStatusChange("hyperliquid", exchange.ConnectionStatusConnecting)
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(c.ctx, c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Hyperliquid WebSocket: %w", err)
	}
	c.conn = conn
	go c.readLoop()
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("hyperliquid", exchange.ConnectionStatusConnected)
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

	for _, symbol := range symbols {
		coin := toExchangeSymbol(symbol)
		tradeSub := SubscriptionRequest{
			Method:       "subscribe",
			Subscription: SubRequest{Type: "trades", Coin: coin},
		}
		if err := c.conn.WriteJSON(tradeSub); err != nil {
			return fmt.Errorf("failed to subscribe to trades: %w", err)
		}
		c.pendingSubs["trades:"+coin] = true
		bboSub := SubscriptionRequest{
			Method:       "subscribe",
			Subscription: SubRequest{Type: "bbo", Coin: coin},
		}
		if err := c.conn.WriteJSON(bboSub); err != nil {
			return fmt.Errorf("failed to subscribe to bbo: %w", err)
		}
		c.pendingSubs["bbo:"+coin] = true
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

	for _, symbol := range symbols {
		coin := toExchangeSymbol(symbol)
		tradeSub := SubscriptionRequest{
			Method:       "unsubscribe",
			Subscription: SubRequest{Type: "trades", Coin: coin},
		}
		if err := c.conn.WriteJSON(tradeSub); err != nil {
			return fmt.Errorf("failed to unsubscribe from trades: %w", err)
		}
		bboSub := SubscriptionRequest{
			Method:       "unsubscribe",
			Subscription: SubRequest{Type: "bbo", Coin: coin},
		}
		if err := c.conn.WriteJSON(bboSub); err != nil {
			return fmt.Errorf("failed to unsubscribe from bbo: %w", err)
		}
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
				fmt.Printf("[Hyperliquid] WebSocket error: %v\n", err)
			}
			c.handleDisconnect()
			return
		}
		var resp SubscriptionResponse
		if err := json.Unmarshal(message, &resp); err == nil && resp.Channel == "subscriptionResponse" {
			c.mu.Lock()
			for key := range c.pendingSubs {
				c.confirmedSubs[key] = true
				delete(c.pendingSubs, key)
			}
			c.mu.Unlock()
			continue
		}
		var errMsg ErrorMessage
		if err := json.Unmarshal(message, &errMsg); err == nil && errMsg.Error != "" {
			fmt.Printf("[Hyperliquid] Error: %s\n", errMsg.Error)
			continue
		}
		events, err := c.parser.ParseMessage(message)
		if err != nil {
			fmt.Printf("[Hyperliquid] Parse error: %v\n", err)
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
		c.connectionHandler.OnStatusChange("hyperliquid", exchange.ConnectionStatusReconnecting)
	}
	delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(c.reconnectAttempts))))
	fmt.Printf("[Hyperliquid] Scheduling reconnect in %v (attempt %d)\n", delay, c.reconnectAttempts+1)
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
	fmt.Println("[Hyperliquid] Attempting to reconnect...")
	if err := c.Close(); err != nil {
		fmt.Printf("[Hyperliquid] Error closing connection: %v\n", err)
	}
	if err := c.Connect(c.ctx); err != nil {
		fmt.Printf("[Hyperliquid] Reconnect failed: %v\n", err)
		c.handleDisconnect()
		return
	}
	if err := c.Subscribe(c.symbols, c.handler); err != nil {
		fmt.Printf("[Hyperliquid] Resubscribe failed: %v\n", err)
		c.handleDisconnect()
		return
	}
	fmt.Println("[Hyperliquid] Reconnected successfully")
	c.mu.Lock()
	c.reconnectAttempts = 0
	c.mu.Unlock()
	if c.connectionHandler != nil {
		c.connectionHandler.OnStatusChange("hyperliquid", exchange.ConnectionStatusConnected)
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
		c.connectionHandler.OnStatusChange("hyperliquid", exchange.ConnectionStatusDisconnected)
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

type CandleMessage struct {
	Channel string `json:"channel"`
	Data    struct {
		Coin     string `json:"coin"`
		Interval string `json:"interval"`
		Open     string `json:"open"`
		Close    string `json:"close"`
		High     string `json:"high"`
		Low      string `json:"low"`
		Volume   string `json:"volume"`
		Time     int64  `json:"time"`
	} `json:"data"`
}

type TradesMessage struct {
	Channel string `json:"channel"`
	Data    []struct {
		Coin  string `json:"coin"`
		Side  string `json:"side"`
		Price string `json:"px"`
		Size  string `json:"sz"`
		Time  int64  `json:"time"`
		Hash  string `json:"hash"`
	} `json:"data"`
}

type BBOMessage struct {
	Channel string `json:"channel"`
	Data    struct {
		Coin  string `json:"coin"`
		Bid   string `json:"bid"`
		Ask   string `json:"ask"`
		BidSz string `json:"bidSz"`
		AskSz string `json:"askSz"`
		Time  int64  `json:"time"`
	} `json:"data"`
}

type L2BookMessage struct {
	Channel string `json:"channel"`
	Data    struct {
		Coin   string `json:"coin"`
		Levels [][]struct {
			Price string `json:"px"`
			Size  string `json:"sz"`
		} `json:"levels"`
		Time int64 `json:"time"`
	} `json:"data"`
}

type SubscriptionRequest struct {
	Method       string     `json:"method"`
	Subscription SubRequest `json:"subscription"`
}

type SubRequest struct {
	Type     string `json:"type"`
	Coin     string `json:"coin,omitempty"`
	Interval string `json:"interval,omitempty"`
}

type SubscriptionResponse struct {
	Channel string      `json:"channel"`
	Data    interface{} `json:"data"`
}

type ErrorMessage struct {
	Error string `json:"error"`
}

func toExchangeSymbol(canonical string) string {
	s := strings.ToUpper(canonical)
	s = strings.ReplaceAll(s, "/USDC:PERP", "")
	s = strings.ReplaceAll(s, "/USDT:PERP", "")
	s = strings.ReplaceAll(s, ":PERP", "")
	return s
}
