package cache

import (
	"gorango/mdx/domain/types"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

type CacheKey struct {
	Exchange  string
	Symbol    string
	Timeframe string
	StartMs   int64
	EndMs     int64
}

type ChunkCacheKey struct {
	Exchange  string
	Symbol    string
	Timeframe string
	Date      string
}

type MemoryCache struct {
	mu            sync.RWMutex
	chunkCache    *lru.Cache[ChunkCacheKey, []types.Bar]
	higherTfCache *lru.Cache[CacheKey, []types.Bar]
}

func NewMemoryCache(size int) (*MemoryCache, error) {
	if size <= 0 {
		size = 500
	}
	chunkCache, err := lru.New[ChunkCacheKey, []types.Bar](size)
	if err != nil {
		return nil, err
	}
	higherTfCache, err := lru.New[CacheKey, []types.Bar](size / 10)
	if err != nil {
		return nil, err
	}
	return &MemoryCache{
		chunkCache:    chunkCache,
		higherTfCache: higherTfCache,
	}, nil
}

func (c *MemoryCache) GetChunk(key ChunkCacheKey) ([]types.Bar, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	bars, ok := c.chunkCache.Get(key)
	if !ok || len(bars) == 0 {
		return nil, false
	}
	result := make([]types.Bar, len(bars))
	copy(result, bars)
	return result, true
}

func (c *MemoryCache) SetChunk(key ChunkCacheKey, bars []types.Bar) {
	if len(bars) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]types.Bar, len(bars))
	copy(result, bars)
	c.chunkCache.Add(key, result)
}

func (c *MemoryCache) GetHigherTfCache(key CacheKey) ([]types.Bar, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	bars, ok := c.higherTfCache.Get(key)
	if !ok || len(bars) == 0 {
		return nil, false
	}
	result := make([]types.Bar, len(bars))
	copy(result, bars)
	return result, true
}

func (c *MemoryCache) SetHigherTfCache(key CacheKey, bars []types.Bar) {
	if len(bars) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]types.Bar, len(bars))
	copy(result, bars)
	c.higherTfCache.Add(key, result)
}

func (c *MemoryCache) Invalidate(exchange, symbol, timeframe string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	chunkKeys := c.chunkCache.Keys()
	for _, key := range chunkKeys {
		if key.Exchange == exchange && key.Symbol == symbol && key.Timeframe == timeframe {
			c.chunkCache.Remove(key)
		}
	}

	higherKeys := c.higherTfCache.Keys()
	for _, key := range higherKeys {
		if key.Exchange == exchange && key.Symbol == symbol && key.Timeframe == timeframe {
			c.higherTfCache.Remove(key)
		}
	}
}

func (c *MemoryCache) Stats() MemoryCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return MemoryCacheStats{
		Len: c.chunkCache.Len() + c.higherTfCache.Len(),
	}
}

type MemoryCacheStats struct {
	Len int
}
