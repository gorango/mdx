package rest

import (
	"context"
	"gorango/exchanges/domain/types"
)

type MockRESTClient struct {
	IDFunc                 func() string
	FetchOHLCVFunc         func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error)
	DownloadMonthlyZipFunc func(ctx context.Context, symbol string, year, month int) ([]types.Bar, error)
	FetchBalanceFunc       func(ctx context.Context) (*types.Balance, error)
	FetchPositionsFunc     func(ctx context.Context) ([]types.Position, error)
	SubmitOrderFunc        func(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error)
	CancelOrderFunc        func(ctx context.Context, orderID, symbol string) error
	FetchOpenOrdersFunc    func(ctx context.Context, symbol string) ([]types.OrderResponse, error)
}

func (m *MockRESTClient) ID() string {
	if m.IDFunc != nil {
		return m.IDFunc()
	}
	return "mock"
}

func (m *MockRESTClient) FetchOHLCV(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
	if m.FetchOHLCVFunc != nil {
		return m.FetchOHLCVFunc(ctx, symbol, tf, since, limit)
	}
	return nil, nil
}

func (m *MockRESTClient) FetchBalance(ctx context.Context) (*types.Balance, error) {
	if m.FetchBalanceFunc != nil {
		return m.FetchBalanceFunc(ctx)
	}
	return nil, nil
}

func (m *MockRESTClient) FetchPositions(ctx context.Context) ([]types.Position, error) {
	if m.FetchPositionsFunc != nil {
		return m.FetchPositionsFunc(ctx)
	}
	return nil, nil
}

func (m *MockRESTClient) SubmitOrder(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
	if m.SubmitOrderFunc != nil {
		return m.SubmitOrderFunc(ctx, req)
	}
	return nil, nil
}

func (m *MockRESTClient) CancelOrder(ctx context.Context, orderID, symbol string) error {
	if m.CancelOrderFunc != nil {
		return m.CancelOrderFunc(ctx, orderID, symbol)
	}
	return nil
}

func (m *MockRESTClient) FetchOpenOrders(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
	if m.FetchOpenOrdersFunc != nil {
		return m.FetchOpenOrdersFunc(ctx, symbol)
	}
	return nil, nil
}

func (m *MockRESTClient) DownloadMonthlyZip(ctx context.Context, symbol string, year, month int) ([]types.Bar, error) {
	if m.DownloadMonthlyZipFunc != nil {
		return m.DownloadMonthlyZipFunc(ctx, symbol, year, month)
	}
	return nil, nil
}
