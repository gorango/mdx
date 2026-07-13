package trading

import (
	"context"
	"gorango/exchanges/domain/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPaperConnector(t *testing.T) {
	connector := NewPaperConnector("test", nil)
	assert.NotNil(t, connector)
	assert.Equal(t, "test", connector.id)
	assert.NotNil(t, connector.balance.Free)
	assert.NotNil(t, connector.balance.Total)
	assert.Equal(t, 10000.0, connector.balance.Free["USDT"])
	assert.Equal(t, 10000.0, connector.balance.Total["USDT"])
}

func TestPaperConnectorGetBalance(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 5000})

	balance, err := connector.GetBalance(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5000.0, balance.Free["USDT"])
}

func TestPaperConnectorGetPositions(t *testing.T) {
	connector := NewPaperConnector("test", nil)

	positions, err := connector.GetPositions(context.Background())
	assert.NoError(t, err)
	assert.Len(t, positions, 0)
}

func TestSubmitOrderMarketBuy(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 10000})

	price := 100.0
	order, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  &price,
	})
	assert.NoError(t, err)
	assert.NotNil(t, order)
	assert.Equal(t, types.OrderStatusClosed, order.Status)
	assert.Equal(t, 100.0, order.Cost)

	balance, _ := connector.GetBalance(context.Background())
	assert.Equal(t, 9900.0, balance.Free["USDT"])

	positions, _ := connector.GetPositions(context.Background())
	assert.Len(t, positions, 1)
}

func TestSubmitOrderMarketSell(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 10000})

	price := 100.0
	_, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  &price,
	})
	assert.NoError(t, err)

	sellOrder, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideSell,
		Amount: 1.0,
		Price:  &price,
	})
	assert.NoError(t, err)
	assert.NotNil(t, sellOrder)
	assert.Equal(t, types.OrderStatusClosed, sellOrder.Status)

	positions, _ := connector.GetPositions(context.Background())
	assert.Len(t, positions, 0)
}

func TestInsufficientBalance(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 50})

	price := 100.0
	order, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  &price,
	})
	assert.Error(t, err)
	assert.Nil(t, order)
	var pe *paperError
	assert.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.message, "insufficient balance")
}

func TestLimitOrderRequiresPrice(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 10000})

	order, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeLimit,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  nil,
	})
	assert.Error(t, err)
	assert.Nil(t, order)
	var pe *paperError
	assert.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.message, "limit order requires price")
}

func TestLimitOrderWithPrice(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 10000})

	price := 100.0
	order, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeLimit,
		Side:   types.OrderSideBuy,
		Amount: 1.0,
		Price:  &price,
	})
	assert.NoError(t, err)
	assert.NotNil(t, order)
	assert.Equal(t, types.OrderStatusClosed, order.Status)
}

func TestInsufficientPositionSize(t *testing.T) {
	connector := NewPaperConnector("test", map[string]float64{"USDT": 10000})

	price := 100.0
	_, err := connector.SubmitOrder(context.Background(), types.OrderRequest{
		Symbol: "BTC/USDT:PERP",
		Type:   types.OrderTypeMarket,
		Side:   types.OrderSideSell,
		Amount: 1.0,
		Price:  &price,
	})
	assert.Error(t, err)
	var pe *paperError
	assert.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.message, "insufficient position size")
}

func TestPaperConnectorGetOpenOrders(t *testing.T) {
	connector := NewPaperConnector("test", nil)
	orders, err := connector.GetOpenOrders(context.Background(), "BTC/USDT:PERP")
	assert.NoError(t, err)
	assert.Nil(t, orders)
}

func TestPaperConnectorCancelOrder(t *testing.T) {
	err := NewPaperConnector("test", nil).CancelOrder(context.Background(), "123", "BTC/USDT:PERP")
	assert.NoError(t, err)
}

func TestPaperConnectorConnect(t *testing.T) {
	err := NewPaperConnector("test", nil).Connect(context.Background())
	assert.NoError(t, err)
}

func TestPaperConnectorClose(t *testing.T) {
	err := NewPaperConnector("test", nil).Close()
	assert.NoError(t, err)
}
