package trading

import (
	"context"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/rest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewRESTConnector(t *testing.T) {
	connector, err := NewRESTConnector("binance", rest.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, connector)
	assert.Equal(t, "binance", connector.id)
}

func TestNewRESTConnectorBybit(t *testing.T) {
	connector, err := NewRESTConnector("bybit", rest.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, connector)
	assert.Equal(t, "bybit", connector.id)
}

func TestNewRESTConnectorUnsupported(t *testing.T) {
	connector, err := NewRESTConnector("unsupported", rest.Config{})
	assert.Error(t, err)
	assert.Nil(t, connector)
	assert.Contains(t, err.Error(), "unsupported exchange")
}

func TestRESTConnectorID(t *testing.T) {
	connector, _ := NewRESTConnector("binance", rest.Config{})
	assert.Equal(t, "binance", connector.ID())
}

func TestRESTConnectorConnect(t *testing.T) {
	connector, _ := NewRESTConnector("binance", rest.Config{})
	err := connector.Connect(context.Background())
	assert.NoError(t, err)
}

func TestRESTConnectorClose(t *testing.T) {
	connector, _ := NewRESTConnector("binance", rest.Config{})
	err := connector.Close()
	assert.NoError(t, err)
}

func TestRESTConnectorGetHistory(t *testing.T) {
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			return []types.Bar{
				{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()
	bars, err := connector.GetHistory(context.Background(), "BTC/USDT", "1m", start, end)
	assert.NoError(t, err)
	assert.Len(t, bars, 1)
}

func TestRESTConnectorStreamPrices(t *testing.T) {
	mockClient := &rest.MockRESTClient{
		FetchOHLCVFunc: func(ctx context.Context, symbol, tf string, since int64, limit int) ([]types.Bar, error) {
			return []types.Bar{
				{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
			}, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ch, err := connector.StreamPrices(ctx, "BTC/USDT")
	assert.NoError(t, err)

	select {
	case <-ch:
	case <-ctx.Done():
	}
}

func TestRESTConnectorGetBalance(t *testing.T) {
	expectedBalance := &types.Balance{
		Free:  map[string]float64{"USDT": 1000},
		Total: map[string]float64{"USDT": 1000},
	}
	mockClient := &rest.MockRESTClient{
		FetchBalanceFunc: func(ctx context.Context) (*types.Balance, error) {
			return expectedBalance, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	balance, err := connector.GetBalance(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectedBalance, balance)
}

func TestRESTConnectorGetPositions(t *testing.T) {
	expectedPositions := []types.Position{
		{Symbol: "BTC/USDT:PERP", Size: 1, AvgPrice: 50000, Side: types.PositionSideLong},
	}
	mockClient := &rest.MockRESTClient{
		FetchPositionsFunc: func(ctx context.Context) ([]types.Position, error) {
			return expectedPositions, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	positions, err := connector.GetPositions(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectedPositions, positions)
}

func TestRESTConnectorGetOpenOrders(t *testing.T) {
	expectedOrders := []types.OrderResponse{
		{ID: "123", Symbol: "BTC/USDT:PERP"},
	}
	mockClient := &rest.MockRESTClient{
		FetchOpenOrdersFunc: func(ctx context.Context, symbol string) ([]types.OrderResponse, error) {
			return expectedOrders, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	orders, err := connector.GetOpenOrders(context.Background(), "BTC/USDT:PERP")
	assert.NoError(t, err)
	assert.Equal(t, expectedOrders, orders)
}

func TestRESTConnectorSubmitOrder(t *testing.T) {
	expectedOrder := &types.OrderResponse{
		ID: "order-123",
	}
	var capturedSymbol string
	mockClient := &rest.MockRESTClient{
		SubmitOrderFunc: func(ctx context.Context, req types.OrderRequest) (*types.OrderResponse, error) {
			capturedSymbol = req.Symbol
			return expectedOrder, nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	price := 50000.0
	order, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTCUSDT",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  &price,
	})
	assert.NoError(t, err)
	assert.Equal(t, expectedOrder, order)
	assert.Equal(t, "BTC/USDT:PERP", capturedSymbol)
}

func TestRESTConnectorCancelOrder(t *testing.T) {
	var capturedOrderID, capturedSymbol string
	mockClient := &rest.MockRESTClient{
		CancelOrderFunc: func(ctx context.Context, orderID, symbol string) error {
			capturedOrderID = orderID
			capturedSymbol = symbol
			return nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	err := connector.CancelOrder(context.Background(), "order-123", "BTCUSDT")
	assert.NoError(t, err)
	assert.Equal(t, "order-123", capturedOrderID)
	assert.Equal(t, "BTC/USDT:PERP", capturedSymbol)
}

func TestRESTConnectorCancelOrderEmptySymbol(t *testing.T) {
	var capturedSymbol string
	mockClient := &rest.MockRESTClient{
		CancelOrderFunc: func(ctx context.Context, orderID, symbol string) error {
			capturedSymbol = symbol
			return nil
		},
	}

	connector := &RESTConnector{id: "binance", client: mockClient}

	err := connector.CancelOrder(context.Background(), "order-123", "")
	assert.NoError(t, err)
	assert.Equal(t, "", capturedSymbol)
}
