package aggregator

import (
	"fmt"
	"gorango/exchanges/domain/types"
	"sync"
)

type Manager struct {
	aggregators map[string]*Aggregator
	mu          sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		aggregators: make(map[string]*Aggregator),
	}
}

func (m *Manager) GetOrCreate(symbol string) *Aggregator {
	m.mu.Lock()
	defer m.mu.Unlock()

	if agg, exists := m.aggregators[symbol]; exists {
		return agg
	}

	agg := New()
	m.aggregators[symbol] = agg
	return agg
}

func (m *Manager) ProcessEvent(event types.Event) error {
	agg := m.GetOrCreate(event.Symbol)

	switch event.Type {
	case types.EventTypeTrade:
		trade, ok := event.Data.(types.Trade)
		if !ok {
			return fmt.Errorf("invalid trade data type")
		}
		agg.ProcessTrade(Trade{
			Timestamp:    event.Timestamp,
			Price:        trade.Price,
			Quantity:     trade.Quantity,
			IsBuyerMaker: trade.IsBuyerMaker,
		})

	case types.EventTypeOrderbookUpdate:
		update, ok := event.Data.(types.OrderbookUpdate)
		if !ok {
			return fmt.Errorf("invalid orderbook update data type")
		}
		agg.ProcessOrderBookUpdate(OrderBookUpdate{
			EventTime: event.Timestamp,
			Price:     update.Price,
			Quantity:  update.Quantity,
			Side:      update.Side,
		})

	case types.EventTypeLiquidation:
		liq, ok := event.Data.(types.Liquidation)
		if !ok {
			return fmt.Errorf("invalid liquidation data type")
		}
		agg.ProcessLiquidation(Liquidation{
			Timestamp: event.Timestamp,
			Quantity:  liq.Quantity,
			Side:      liq.Side,
		})

	case types.EventTypeOpenInterest:
		oi, ok := event.Data.(types.OpenInterest)
		if !ok {
			return fmt.Errorf("invalid open interest data type")
		}
		agg.ProcessOpenInterest(OpenInterest{
			Timestamp: event.Timestamp,
			Value:     oi.Value,
		})

	case types.EventTypeFundingRate:
		// Funding rate not currently processed by aggregator
		// Could be added later if needed

	default:
		return fmt.Errorf("unknown event type: %v", event.Type)
	}

	return nil
}

func (m *Manager) FlushAll() map[string][]types.OrderbookBar {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[string][]types.OrderbookBar)

	for symbol, agg := range m.aggregators {
		bars := agg.Finalize(true)
		if len(bars) > 0 {
			result[symbol] = bars
		}
		m.aggregators[symbol] = New()
	}

	return result
}

func (m *Manager) FlushSymbol(symbol string) ([]types.OrderbookBar, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	agg, exists := m.aggregators[symbol]
	if !exists {
		return nil, fmt.Errorf("no aggregator for symbol: %s", symbol)
	}

	bars := agg.Finalize(true)
	m.aggregators[symbol] = New()

	return bars, nil
}

func (m *Manager) GetSymbols() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	symbols := make([]string, 0, len(m.aggregators))
	for symbol := range m.aggregators {
		symbols = append(symbols, symbol)
	}
	return symbols
}
