package flusher

import (
	"context"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/db"
	"strings"
	"sync"
	"time"
)

func findCharIndex(s string, c rune) int {
	return strings.IndexRune(s, c)
}

type FlusherStats struct {
	BufferedBars    int
	BufferedSymbols int
	LastFlushTime   int64
	TotalFlushed    int64
	FlushErrors     int
}

type Flusher struct {
	database     *db.DB
	buffer       map[string][]types.OrderbookBar
	bufferMu     sync.RWMutex
	interval     time.Duration
	maxBatchSize int
	stats        FlusherStats
	statsMu      sync.RWMutex
	stopCh       chan struct{}
	wg           sync.WaitGroup
	flushing     bool
	flushMu      sync.Mutex
}

func NewFlusher(database *db.DB, intervalSeconds int, maxBatchSize int) *Flusher {
	return &Flusher{
		database:     database,
		buffer:       make(map[string][]types.OrderbookBar),
		interval:     time.Duration(intervalSeconds) * time.Second,
		maxBatchSize: maxBatchSize,
		stopCh:       make(chan struct{}),
	}
}

func (f *Flusher) Add(exchange, symbol string, bars []types.OrderbookBar) {
	if len(bars) == 0 {
		return
	}

	key := exchange + ":" + symbol

	f.bufferMu.Lock()
	defer f.bufferMu.Unlock()

	f.buffer[key] = append(f.buffer[key], bars...)

	f.statsMu.Lock()
	f.stats.BufferedBars = len(f.buffer[key])
	f.stats.BufferedSymbols = len(f.buffer)
	f.statsMu.Unlock()

	if f.maxBatchSize > 0 && f.stats.BufferedBars >= f.maxBatchSize {
		f.flushMu.Lock()
		if f.flushing {
			f.flushMu.Unlock()
			return
		}
		f.flushing = true
		f.flushMu.Unlock()
		go func() {
			_ = f.Flush(context.Background())
			f.flushMu.Lock()
			f.flushing = false
			f.flushMu.Unlock()
		}()
	}
}

func (f *Flusher) Flush(ctx context.Context) error {
	f.bufferMu.Lock()
	if len(f.buffer) == 0 {
		f.bufferMu.Unlock()
		return nil
	}

	bufferCopy := make(map[string][]types.OrderbookBar, len(f.buffer))
	for k, v := range f.buffer {
		if len(v) > 0 {
			bufferCopy[k] = v
		}
	}
	f.buffer = make(map[string][]types.OrderbookBar)
	f.bufferMu.Unlock()

	for key, bars := range bufferCopy {
		if len(bars) == 0 {
			continue
		}

		// Key format: "exchange:canonicalSymbol"
		// e.g., "binance:BTC/USDT:PERP"
		idx := findCharIndex(key, ':')
		if idx == -1 {
			fmt.Printf("[Flusher] ERROR: invalid key format: %q\n", key)
			continue
		}
		exchange := key[:idx]
		canonicalSymbol := key[idx+1:]

		// Use canonical symbol for database storage
		dbExchange := symbols.MapExchangeToDB(exchange)
		dbSymbol := canonicalSymbol

		if err := f.database.InsertOrderbookBars(ctx, dbExchange, dbSymbol, bars); err != nil {
			f.statsMu.Lock()
			f.stats.FlushErrors++
			f.statsMu.Unlock()
			fmt.Printf("[Flusher] Error inserting bars for %s: %v\n", key, err)
			continue
		}

		f.statsMu.Lock()
		f.stats.TotalFlushed += int64(len(bars))
		f.stats.BufferedBars -= len(bars)
		f.statsMu.Unlock()

		fmt.Printf("%s Flushed %d bars for %s\n", time.Now().Format("2006-01-02 15:04:05"), len(bars), key)
	}

	f.statsMu.Lock()
	f.stats.LastFlushTime = time.Now().Unix()
	f.stats.BufferedSymbols = len(f.buffer)
	f.statsMu.Unlock()

	return nil
}

func (f *Flusher) Start(ctx context.Context) error {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()

		for {
			select {
			case <-f.stopCh:
				return
			case <-ticker.C:
				_ = f.Flush(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (f *Flusher) Stop() {
	close(f.stopCh)
	f.wg.Wait()
}

func (f *Flusher) Stats() FlusherStats {
	f.statsMu.RLock()
	defer f.statsMu.RUnlock()
	return f.stats
}
