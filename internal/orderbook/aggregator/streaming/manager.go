package streaming

import (
	"gorango/mdx/domain/types"
	"sync"
	"time"
)

// Manager maintains aggregators per symbol
type Manager struct {
	aggregators              map[string]*Aggregator
	liquidationFeedAvailable bool
	mu                       sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		aggregators: make(map[string]*Aggregator),
	}
}

// SetLiquidationFeedAvailable sets whether liquidation data feed is healthy.
func (m *Manager) SetLiquidationFeedAvailable(available bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.liquidationFeedAvailable = available
}

func (m *Manager) GetOrCreate(symbol string) *Aggregator {
	m.mu.Lock()
	defer m.mu.Unlock()

	if agg, exists := m.aggregators[symbol]; exists {
		return agg
	}

	agg := New(symbol)
	m.aggregators[symbol] = agg
	return agg
}

func (m *Manager) ProcessEvent(event types.Event) error {
	agg := m.GetOrCreate(event.Symbol)
	return agg.ProcessEvent(event)
}

// FlushAll flushes bars that are older than the current wall-clock minute.
// This safely allows late-arriving events for the active minute to settle.
func (m *Manager) FlushAll() map[string][]types.OrderbookBar {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[string][]types.OrderbookBar)
	liqCovered := m.liquidationFeedAvailable

	// Flush bars older than the current wall-clock minute.
	// This safely allows late-arriving events for the active minute to settle.
	flushUpTo := (time.Now().UTC().UnixMilli() / 60000) - 1

	for symbol, agg := range m.aggregators {
		bars := agg.Finalize(liqCovered, flushUpTo)
		if len(bars) > 0 {
			result[symbol] = bars
		}
	}

	return result
}

// ResetDepth clears the orderbook treap for the given symbol.
// Should be called after a WebSocket reconnect to discard stale levels.
func (m *Manager) ResetDepth(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if agg, exists := m.aggregators[symbol]; exists {
		agg.ResetDepth()
	}
}
