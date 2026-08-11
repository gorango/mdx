package trading

import (
	"context"
	"gorango/mdx/domain/types"
	"time"
)

type Connector interface {
	ID() string

	GetHistory(ctx context.Context, symbol, tf string, start, end time.Time) ([]types.Bar, error)
	StreamPrices(ctx context.Context, symbol string) (<-chan types.Bar, error)

	GetBalance(ctx context.Context) (*types.Balance, error)
	GetPositions(ctx context.Context) ([]types.Position, error)
	GetOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error)
	SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error)
	CancelOrder(ctx context.Context, orderID string, symbol string) error
	SetLeverage(ctx context.Context, symbol string, leverage int) error

	Connect(ctx context.Context) error
	Close() error
}
