// Package trading bridges engine order requests arriving over NATS to a live
// exchange connector.
//
// The engine publishes orders to `orders.<symbol>.<action>` (action ∈
// {open, close}) and cancels on `orders.<symbol>.cancel`. This bridge
// subscribes to `orders.>`, translates the engine's JSON payload into
// domain types.OrderRequest (mapping action=close → reduceOnly, post_only →
// GTX, etc.), submits it through the Connector, and replies to the request
// with an ExecutionReport in the engine's wire format so the engine can
// await its fill synchronously.
package trading

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/types"

	"github.com/nats-io/nats.go"
)

// engineOrder mirrors engine/internal/exchange.OrderRequest's JSON wire
// format (the engine and this repo are separate modules, so the struct is
// duplicated here as the contract).
type engineOrder struct {
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`          // BUY | SELL
	Action      string  `json:"action"`        // open | close
	Size        float64 `json:"size"`          // base-asset quantity
	Price       float64 `json:"price"`         // 0 for market orders
	OrderType   string  `json:"order_type"`    // market | limit | post_only
	TimeInForce string  `json:"time_in_force"` // GTC | IOC | FOK
	Leverage    *int    `json:"leverage"`
	Exchange    string  `json:"exchange"` // routing hint (binance/bybit/…)
}

// executionReport mirrors engine/internal/exchange.ExecutionReport JSON.
type executionReport struct {
	Symbol    string  `json:"symbol"`
	OrderID   string  `json:"order_id"`
	Side      string  `json:"side"`
	Action    string  `json:"action"`
	Filled    bool    `json:"filled"`
	FillPrice float64 `json:"fill_price"`
	Size      float64 `json:"size"`
	Error     string  `json:"error,omitempty"`
}

// OrderBridge consumes engine orders from NATS and executes them through a
// Connector.
type OrderBridge struct {
	nc     *nats.Conn
	conn   Connector
	logger *slog.Logger

	// submitTimeout bounds each order submission (including leverage setup).
	submitTimeout time.Duration

	mu   sync.Mutex
	subs []*nats.Subscription
}

// NewOrderBridge creates a bridge that routes `orders.>` messages through
// conn. If logger is nil a discard logger is used.
func NewOrderBridge(nc *nats.Conn, conn Connector, logger *slog.Logger) *OrderBridge {
	if logger == nil {
		logger = slog.Default()
	}
	return &OrderBridge{
		nc:            nc,
		conn:          conn,
		logger:        logger,
		submitTimeout: 30 * time.Second,
	}
}

// Start subscribes to `orders.>` (queue group "order-bridge") and begins
// processing orders. Safe to call once; Stop tears the subscription down.
func (b *OrderBridge) Start() error {
	// Queue group: if multiple daemons run, exactly one bridge instance
	// consumes each order — otherwise every instance would double-execute.
	sub, err := b.nc.QueueSubscribe("orders.>", "order-bridge", b.handle)
	if err != nil {
		return fmt.Errorf("subscribe orders.>: %w", err)
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	b.logger.Info("order bridge listening", "subject", "orders.>", "queue", "order-bridge")
	return nil
}

// Stop unsubscribes from the orders subject.
func (b *OrderBridge) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range b.subs {
		_ = sub.Unsubscribe()
	}
	b.subs = nil
	return nil
}

func (b *OrderBridge) handle(msg *nats.Msg) {
	// subject: orders.<symbol>.<action>  (symbol contains no dots)
	parts := strings.Split(msg.Subject, ".")
	if len(parts) < 3 || parts[0] != "orders" {
		return
	}
	action := parts[len(parts)-1]
	symbol := strings.Join(parts[1:len(parts)-1], ".")

	switch action {
	case "cancel":
		b.handleCancel(msg, symbol)
	case "set_leverage":
		b.handleSetLeverage(msg, symbol)
	default:
		b.handleOrder(msg, symbol, action)
	}
}

func (b *OrderBridge) handleCancel(msg *nats.Msg, symbol string) {
	var req struct {
		Symbol  string `json:"symbol"`
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.logger.Warn("invalid cancel payload", "subject", msg.Subject, "err", err)
		return
	}
	if req.Symbol != "" {
		symbol = req.Symbol
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.submitTimeout)
	defer cancel()
	if err := b.conn.CancelOrder(ctx, req.OrderID, symbol); err != nil {
		b.logger.Error("cancel order failed", "order_id", req.OrderID, "symbol", symbol, "err", err)
		return
	}
	b.logger.Info("order canceled", "order_id", req.OrderID, "symbol", symbol)
}

// handleSetLeverage applies the engine's derived margin headroom to a symbol
// (a one-time account-level setting via the connector, not a per-trade knob).
// Replies with an ExecutionReport so the engine's request-reply can surface
// connector/range errors (e.g. leverage beyond the exchange maximum) loudly.
func (b *OrderBridge) handleSetLeverage(msg *nats.Msg, symbol string) {
	var req struct {
		Symbol   string `json:"symbol"`
		Leverage int    `json:"leverage"`
	}
	report := executionReport{Symbol: symbol, Action: "set_leverage"}
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		report.Error = "invalid set-leverage payload: " + err.Error()
	} else {
		if req.Symbol != "" {
			symbol = req.Symbol
		}
		ctx, cancel := context.WithTimeout(context.Background(), b.submitTimeout)
		defer cancel()
		if err := b.conn.SetLeverage(ctx, symbol, req.Leverage); err != nil {
			report.Error = fmt.Sprintf("set leverage %d: %v", req.Leverage, err)
		}
	}
	data, _ := json.Marshal(report)
	_ = msg.Respond(data)
	if report.Error == "" {
		b.logger.Info("leverage set", "symbol", symbol, "leverage", req.Leverage)
	} else {
		b.logger.Error("set leverage failed", "symbol", symbol, "err", report.Error)
	}
}

func (b *OrderBridge) handleOrder(msg *nats.Msg, symbol, action string) {
	report := b.execute(msg.Data, symbol, action)

	data, err := json.Marshal(report)
	if err != nil {
		b.logger.Error("marshal execution report", "err", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		b.logger.Warn("respond to order request", "subject", msg.Subject, "err", err)
	}
}

// execute translates and submits an order, returning the execution report.
func (b *OrderBridge) execute(data []byte, symbol, action string) *executionReport {
	var in engineOrder
	if err := json.Unmarshal(data, &in); err != nil {
		return &executionReport{Symbol: symbol, Action: action, Error: "invalid order payload: " + err.Error()}
	}
	if in.Symbol != "" {
		symbol = in.Symbol
	}
	if in.Action != "" {
		action = in.Action
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.submitTimeout)
	defer cancel()

	// Leverage must be set before opening a position (dedicated endpoint on
	// Binance; per-order param on Bybit).
	if in.Leverage != nil && *in.Leverage > 1 && action == "open" {
		if err := b.conn.SetLeverage(ctx, symbol, *in.Leverage); err != nil {
			return &executionReport{
				Symbol: symbol, Side: in.Side, Action: action, Size: in.Size,
				Error: fmt.Sprintf("set leverage %d: %v", *in.Leverage, err),
			}
		}
	}

	req, err := translateOrder(&in)
	if err != nil {
		return &executionReport{
			Symbol: symbol, Side: in.Side, Action: action, Size: in.Size,
			Error: err.Error(),
		}
	}

	resp, err := b.conn.SubmitOrder(ctx, req)
	if err != nil {
		return &executionReport{
			Symbol: symbol, Side: in.Side, Action: action, Size: in.Size,
			Error: err.Error(),
		}
	}

	report := buildReport(symbol, action, &in, resp)
	b.logger.Info("order executed",
		"symbol", symbol, "action", action, "side", in.Side,
		"size", in.Size, "order_id", resp.ID, "filled", report.Filled,
		"fill_price", report.FillPrice,
	)
	return report
}

// translateOrder converts an engine order payload into a domain order
// request, mapping engine-only concepts onto exchange semantics:
//   - action=close  → reduceOnly (never flip into a reverse position)
//   - order_type=post_only → limit with GTX (post-only) time-in-force
//   - side casing normalised to the domain's lower-case constants
func translateOrder(in *engineOrder) (types.OrderRequest, error) {
	var out types.OrderRequest
	out.Symbol = symbols.NormalizeCanonical(in.Symbol)
	out.Amount = in.Size

	switch in.Side {
	case "BUY":
		out.Side = types.OrderSideBuy
	case "SELL":
		out.Side = types.OrderSideSell
	default:
		return out, fmt.Errorf("unsupported side %q", in.Side)
	}

	switch in.OrderType {
	case "", "market":
		out.Type = types.OrderTypeMarket
	case "limit":
		out.Type = types.OrderTypeLimit
	case "post_only":
		out.Type = types.OrderTypeLimit
		tif := types.TIFGTX
		out.TimeInForce = &tif
	default:
		return out, fmt.Errorf("unsupported order type %q", in.OrderType)
	}

	if in.Price > 0 {
		p := in.Price
		out.Price = &p
		if out.Type == types.OrderTypeMarket {
			out.Type = types.OrderTypeLimit
		}
	}

	if out.TimeInForce == nil && in.TimeInForce != "" {
		tif := types.TimeInForce(in.TimeInForce)
		out.TimeInForce = &tif
	}

	out.Leverage = in.Leverage
	if in.Action == "close" {
		ro := true
		out.ReduceOnly = &ro
	}
	return out, nil
}

// buildReport maps an exchange order response onto the engine's execution
// report. Market orders that report a synchronous fill carry an accurate
// fill price; everything else (e.g. Bybit's async acknowledgment, resting
// limit orders) is reported as unfilled and the engine falls back to its
// reference price on timeout, matching paper behavior.
func buildReport(symbol, action string, in *engineOrder, resp *types.OrderResponse) *executionReport {
	report := &executionReport{
		Symbol:  symbol,
		Action:  action,
		Side:    in.Side,
		Size:    in.Size,
		OrderID: resp.ID,
	}
	if resp.Filled > 0 {
		report.Filled = true
		switch {
		case resp.Average != nil && *resp.Average > 0:
			report.FillPrice = *resp.Average
		case resp.Price > 0:
			report.FillPrice = resp.Price
		}
	}
	return report
}
