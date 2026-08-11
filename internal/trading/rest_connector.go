package trading

import (
	"context"
	"fmt"
	"gorango/mdx/domain/symbols"
	"gorango/mdx/domain/types"
	"gorango/mdx/internal/rest"
	"math"
	"time"
)

type RESTConnector struct {
	id     string
	client rest.Client
}

func NewRESTConnector(exchangeID string, cfg rest.Config) (*RESTConnector, error) {
	var client rest.Client
	switch exchangeID {
	case "binance":
		client = rest.NewBinance(cfg)
	case "bybit":
		client = rest.NewBybit(cfg)
	default:
		return nil, fmt.Errorf("unsupported exchange: %s", exchangeID)
	}
	return &RESTConnector{id: exchangeID, client: client}, nil
}

func NewCCXTConnector(exchangeID, apiKey, secret string) (*RESTConnector, error) {
	return NewRESTConnector(exchangeID, rest.Config{
		APIKey:    apiKey,
		APISecret: secret,
	})
}

func (c *RESTConnector) ID() string {
	return c.id
}

func (c *RESTConnector) Connect(ctx context.Context) error {
	return nil
}

func (c *RESTConnector) Close() error {
	return nil
}

func (c *RESTConnector) GetHistory(ctx context.Context, symbol, tf string, start, end time.Time) ([]types.Bar, error) {
	canonical := symbols.NormalizeCanonical(symbol)

	var bars []types.Bar
	lastTs := start.UnixMilli()

	for {
		chunk, err := c.client.FetchOHLCV(ctx, canonical, tf, lastTs, 1000)
		if err != nil {
			return bars, err
		}
		if len(chunk) == 0 {
			break
		}

		for _, b := range chunk {
			if b.Time.After(start) && b.Time.Before(end.Add(time.Second)) {
				bars = append(bars, b)
			}
		}

		lastBar := chunk[len(chunk)-1]
		if !lastBar.Time.Before(end) {
			break
		}
		lastTs = lastBar.Time.UnixMilli()
	}

	return bars, nil
}

func (c *RESTConnector) StreamPrices(ctx context.Context, symbol string) (<-chan types.Bar, error) {
	canonical := symbols.NormalizeCanonical(symbol)
	out := make(chan types.Bar, 100)
	go func() {
		defer close(out)
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		var lastBar types.Bar
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				bars, err := c.client.FetchOHLCV(ctx, canonical, "1m",
					time.Now().Add(-10*time.Minute).UnixMilli(), 100)
				if err != nil {
					continue
				}
				for _, b := range bars {
					if b.Time.After(lastBar.Time) {
						lastBar = b
						select {
						case out <- b:
						case <-ctx.Done():
							return
						default:
						}
					}
				}
			}
		}
	}()
	return out, nil
}

func (c *RESTConnector) GetBalance(ctx context.Context) (*types.Balance, error) {
	return c.client.FetchBalance(ctx)
}

func (c *RESTConnector) GetPositions(ctx context.Context) ([]types.Position, error) {
	return c.client.FetchPositions(ctx)
}

func (c *RESTConnector) GetOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	return c.client.FetchOpenOrders(ctx, symbol)
}

func (c *RESTConnector) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	req.Symbol = symbols.NormalizeCanonical(req.Symbol)

	if req.Type == types.OrderTypeLimit && req.Price == nil {
		return nil, fmt.Errorf("limit order requires price")
	}

	// Round the quantity down to the exchange's lot size and enforce the
	// minimum order quantity — raw notional/price sizes from the engine do
	// not respect exchange step sizes and get rejected otherwise.
	step, minQty, err := c.client.FetchLotSize(ctx, req.Symbol)
	if err == nil && step > 0 {
		req.Amount = quantizeDown(req.Amount, step)
		if req.Amount < minQty {
			return nil, fmt.Errorf("quantity %.8f below minimum %.8f for %s", req.Amount, minQty, req.Symbol)
		}
	}

	return c.client.SubmitOrder(ctx, req)
}

// quantizeDown rounds qty down to the nearest multiple of step.
func quantizeDown(qty, step float64) float64 {
	if qty <= 0 || step <= 0 {
		return qty
	}
	const epsilon = 1e-9
	mult := math.Floor(qty/step + epsilon)
	return mult * step
}

func (c *RESTConnector) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	return c.client.SetLeverage(ctx, symbols.NormalizeCanonical(symbol), leverage)
}

func (c *RESTConnector) CancelOrder(ctx context.Context, orderID string, symbol string) error {
	var sym string
	if symbol != "" {
		sym = symbols.NormalizeCanonical(symbol)
	}
	return c.client.CancelOrder(ctx, orderID, sym)
}
