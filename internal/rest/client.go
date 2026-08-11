package rest

import (
	"context"
	"gorango/mdx/domain/types"
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
	// SetLeverage changes the initial leverage for a symbol. Must be called
	// before placing a leveraged order (Binance requires a dedicated endpoint).
	SetLeverage(ctx context.Context, symbol string, leverage int) error
	// FetchLotSize returns the quantity step size and minimum order quantity
	// for a symbol, so callers can round order sizes before submission.
	FetchLotSize(ctx context.Context, symbol string) (step, minQty float64, err error)
}

type Config struct {
	APIKey    string
	APISecret string
	Testnet   bool
}
