package flusher

import (
	"context"
	"testing"
	"time"
	"gorango/exchanges/domain/types"

	"github.com/stretchr/testify/assert"
)

func TestNewFlusher(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)
	assert.NotNil(t, flusher)
	assert.Equal(t, 30*time.Second, flusher.interval)
	assert.Equal(t, 1000, flusher.maxBatchSize)
}

func TestFlusherAddEmpty(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)
	flusher.Add("binance", "BTC/USDT:PERP", []types.OrderbookBar{})
	assert.Equal(t, 0, flusher.stats.BufferedBars)
}

func TestFlusherStats(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)
	stats := flusher.Stats()
	assert.Equal(t, 0, stats.BufferedBars)
	assert.Equal(t, 0, stats.BufferedSymbols)
}

func TestFlusherAddIncrementsStats(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)
	bars := []types.OrderbookBar{
		{Timestamp: time.Now().UnixMilli(), VWAP: 100, TradeCount: 10},
	}

	flusher.Add("binance", "BTC/USDT:PERP", bars)

	stats := flusher.Stats()
	assert.Equal(t, 1, stats.BufferedBars)
	assert.Equal(t, 1, stats.BufferedSymbols)
}

func TestFlusherFlushEmpty(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)
	err := flusher.Flush(context.Background())
	assert.NoError(t, err)
}

func TestFlusherFlushWithData(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)

	bars := []types.OrderbookBar{
		{Timestamp: time.Now().UnixMilli(), VWAP: 100, TradeCount: 10, BuyVolume: 50, SellVolume: 50},
	}
	flusher.Add("binance", "BTC/USDT:PERP", bars)
}

func TestFlusherMultipleSymbols(t *testing.T) {
	flusher := NewFlusher(nil, 30, 1000)

	flusher.Add("binance", "BTC/USDT:PERP", []types.OrderbookBar{{Timestamp: time.Now().UnixMilli(), VWAP: 100}})
	flusher.Add("bybit", "ETH/USDT:PERP", []types.OrderbookBar{{Timestamp: time.Now().UnixMilli(), VWAP: 200}})

	stats := flusher.Stats()
	assert.Equal(t, 2, stats.BufferedSymbols)
}

func TestFlusherStop(t *testing.T) {
	flusher := NewFlusher(nil, 1, 1000)
	flusher.Stop()
}

func TestFindCharIndex(t *testing.T) {
	assert.Equal(t, 0, findCharIndex("hello", 'h'))
	assert.Equal(t, -1, findCharIndex("hello", 'x'))
	assert.Equal(t, 2, findCharIndex("hello", 'l'))
}
