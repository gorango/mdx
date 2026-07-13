package db

import (
	"context"
	"gorango/exchanges/domain/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertPriceBarsEmpty(t *testing.T) {
	database := &DB{}

	err := database.InsertPriceBars(context.Background(), "binance", "BTC/USDT", []types.Bar{})
	assert.NoError(t, err)
}

func TestInsertOrderbookBarsEmpty(t *testing.T) {
	database := &DB{}

	err := database.InsertOrderbookBars(context.Background(), "binance", "BTC/USDT", []types.OrderbookBar{})
	assert.NoError(t, err)
}
