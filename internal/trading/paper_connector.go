package trading

import (
	"context"
	"gorango/exchanges/domain/types"
	"sync"
	"time"
)

type PaperConnector struct {
	id        string
	balance   types.Balance
	positions map[string]types.Position
	orders    map[string]types.OrderResponse
	leverage  map[string]int
	mu        sync.RWMutex
}

func NewPaperConnector(id string, initialBalance map[string]float64) *PaperConnector {
	if initialBalance == nil {
		initialBalance = map[string]float64{"USDT": 10000}
	}
	b := types.Balance{
		Free:  make(map[string]float64),
		Used:  make(map[string]float64),
		Total: make(map[string]float64),
		Info:  map[string]interface{}{"mode": "paper"},
	}
	for k, v := range initialBalance {
		b.Free[k] = v
		b.Total[k] = v
	}
	return &PaperConnector{
		id:        id,
		balance:   b,
		positions: make(map[string]types.Position),
		orders:    make(map[string]types.OrderResponse),
		leverage:  make(map[string]int),
	}
}

func (c *PaperConnector) ID() string {
	return c.id
}

func (c *PaperConnector) Connect(ctx context.Context) error {
	return nil
}

func (c *PaperConnector) Close() error {
	return nil
}

func (c *PaperConnector) GetHistory(ctx context.Context, symbol, tf string, start, end time.Time) ([]types.Bar, error) {
	return nil, nil
}

func (c *PaperConnector) StreamPrices(ctx context.Context, symbol string) (<-chan types.Bar, error) {
	return nil, nil
}

func (c *PaperConnector) GetBalance(ctx context.Context) (*types.Balance, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &c.balance, nil
}

func (c *PaperConnector) GetPositions(ctx context.Context) ([]types.Position, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	positions := make([]types.Position, 0, len(c.positions))
	for _, p := range c.positions {
		positions = append(positions, p)
	}
	return positions, nil
}

func (c *PaperConnector) GetOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	return nil, nil
}

func (c *PaperConnector) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if req.Price == nil && req.Type == types.OrderTypeLimit {
		return nil, &paperError{message: "limit order requires price"}
	}

	price := 0.0
	if req.Price != nil {
		price = *req.Price
	}
	cost := req.Amount * price

	quote := "USDT"
	for k := range c.balance.Free {
		quote = k
		break
	}

	if req.Side == types.OrderSideBuy {
		if c.balance.Free[quote] < cost {
			return nil, &paperError{message: "insufficient balance"}
		}
		c.balance.Free[quote] -= cost
		c.balance.Used[quote] += cost
	} else {
		pos, ok := c.positions[req.Symbol]
		if !ok || pos.Size < req.Amount {
			return nil, &paperError{message: "insufficient position size"}
		}
	}

	order := &types.OrderResponse{
		ID:            generateOrderID(),
		Timestamp:     time.Now().UnixMilli(),
		Status:        types.OrderStatusClosed,
		Symbol:        req.Symbol,
		Type:          req.Type,
		Side:          req.Side,
		Amount:        req.Amount,
		Filled:        req.Amount,
		Remaining:     0,
		Price:         price,
		Cost:          cost,
		ClientOrderID: req.ClientOrderID,
	}
	c.orders[order.ID] = *order

	if req.Type == types.OrderTypeMarket || req.Side == types.OrderSideBuy {
		avgPrice := price
		if avgPrice == 0 {
			avgPrice = 1
		}
		pos := c.positions[req.Symbol]
		oldSize := pos.Size
		if req.Side == types.OrderSideBuy {
			pos.Size += req.Amount
			if oldSize != 0 && pos.Size != 0 {
				pos.AvgPrice = (pos.AvgPrice*oldSize + avgPrice*req.Amount) / (oldSize + req.Amount)
			} else {
				pos.AvgPrice = avgPrice
			}
		} else {
			pos.Size -= req.Amount
		}
		pos.Symbol = req.Symbol
		if req.Leverage != nil {
			lev := *req.Leverage
			pos.Leverage = &lev
		} else if lev, ok := c.leverage[req.Symbol]; ok && lev > 0 {
			l := lev
			pos.Leverage = &l
		}
		if pos.Size > 0 {
			pos.Side = types.PositionSideLong
		} else if pos.Size < 0 {
			pos.Side = types.PositionSideShort
		}
		if pos.Size == 0 {
			delete(c.positions, req.Symbol)
		} else {
			c.positions[req.Symbol] = pos
		}

		if req.Side == types.OrderSideSell {
			proceeds := req.Amount * avgPrice
			c.balance.Free[quote] += proceeds
			c.balance.Used[quote] -= proceeds
		}
	}

	return order, nil
}

func (c *PaperConnector) CancelOrder(ctx context.Context, orderID string, symbol string) error {
	return nil
}

// SetLeverage records the requested leverage for a symbol; it is applied to
// positions opened afterwards.
func (c *PaperConnector) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	if leverage < 1 {
		return &paperError{message: "leverage must be >= 1"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leverage[symbol] = leverage
	return nil
}

type paperError struct {
	message string
}

func (e *paperError) Error() string {
	return e.message
}

var orderCounter int64

func generateOrderID() string {
	orderCounter++
	return time.Now().Format("20060102150405") + "-" + string(rune('A'+orderCounter%26)) + "-" + time.Now().Format("150405.000")
}
