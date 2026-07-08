package rest

import (
	"context"
	"gorango/exchanges/domain/types"
)

type Client interface {
	ID() string
	FetchOHLCV(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error)
	DownloadMonthlyZip(ctx context.Context, symbol string, year, month int) ([]types.Bar, error)
	FetchBalance(ctx context.Context) (*types.Balance, error)
	FetchPositions(ctx context.Context) ([]types.Position, error)
	SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error)
	CancelOrder(ctx context.Context, orderID, symbol string) error
	FetchOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error)
}

type Config struct {
	APIKey    string
	APISecret string
	Testnet   bool
}
