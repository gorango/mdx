package cache

import (
	"gorango/mdx/domain/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMemoryCache(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
}

func TestMemoryCacheDefaultSize(t *testing.T) {
	cache, err := NewMemoryCache(0)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
}

func TestMemoryCacheDefaultSizeNegative(t *testing.T) {
	cache, err := NewMemoryCache(-10)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
}

func TestGetSetHigherTfCache(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	key := CacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT:PERP",
		Timeframe: "1d",
		StartMs:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		EndMs:     time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	bars := []types.Bar{
		{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
	}

	cache.SetHigherTfCache(key, bars)

	result, ok := cache.GetHigherTfCache(key)
	assert.True(t, ok)
	assert.Len(t, result, 1)
	assert.Equal(t, 100.0, result[0].Open)
}

func TestCacheKeyDifferent(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	key1 := CacheKey{Exchange: "binance", Symbol: "BTC/USDT:PERP", Timeframe: "1d", StartMs: 1000, EndMs: 2000}
	key2 := CacheKey{Exchange: "binance", Symbol: "ETH/USDT:PERP", Timeframe: "1d", StartMs: 1000, EndMs: 2000}

	bars := []types.Bar{{Time: time.Now(), Open: 100, Volume: 1000}}
	cache.SetHigherTfCache(key1, bars)

	_, ok := cache.GetHigherTfCache(key2)
	assert.False(t, ok)
}

func TestInvalidate(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	key := CacheKey{Exchange: "binance", Symbol: "BTC/USDT:PERP", Timeframe: "1m", StartMs: 1000, EndMs: 2000}
	bars := []types.Bar{{Time: time.Now(), Open: 100, Volume: 1000}}
	cache.SetHigherTfCache(key, bars)

	_, ok := cache.GetHigherTfCache(key)
	assert.True(t, ok)

	cache.Invalidate("binance", "BTC/USDT:PERP", "1m")

	_, ok = cache.GetHigherTfCache(key)
	assert.False(t, ok)
}

func TestInvalidatePartial(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	key1 := CacheKey{Exchange: "binance", Symbol: "BTC/USDT:PERP", Timeframe: "1m", StartMs: 1000, EndMs: 2000}
	key2 := CacheKey{Exchange: "binance", Symbol: "ETH/USDT:PERP", Timeframe: "1m", StartMs: 1000, EndMs: 2000}

	bars := []types.Bar{{Time: time.Now(), Open: 100, Volume: 1000}}
	cache.SetHigherTfCache(key1, bars)
	cache.SetHigherTfCache(key2, bars)

	cache.Invalidate("binance", "BTC/USDT:PERP", "1m")

	_, ok := cache.GetHigherTfCache(key1)
	assert.False(t, ok)

	_, ok = cache.GetHigherTfCache(key2)
	assert.True(t, ok)
}

func TestStats(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	stats := cache.Stats()
	assert.Equal(t, 0, stats.Len)

	key := CacheKey{Exchange: "binance", Symbol: "BTC/USDT:PERP", Timeframe: "1m", StartMs: 1000, EndMs: 2000}
	bars := []types.Bar{{Time: time.Now(), Open: 100, Volume: 1000}}
	cache.SetHigherTfCache(key, bars)

	stats = cache.Stats()
	assert.Equal(t, 1, stats.Len)
}

func TestGetSetChunk(t *testing.T) {
	cache, err := NewMemoryCache(100)
	assert.NoError(t, err)

	key := ChunkCacheKey{
		Exchange:  "binance",
		Symbol:    "BTC/USDT:PERP",
		Timeframe: "1m",
		Date:      "2024-01-01",
	}
	bars := []types.Bar{
		{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
	}

	cache.SetChunk(key, bars)

	result, ok := cache.GetChunk(key)
	assert.True(t, ok)
	assert.Len(t, result, 1)
	assert.Equal(t, 100.0, result[0].Open)
}
