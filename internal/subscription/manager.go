package subscription

import (
	"fmt"
	"gorango/exchanges/domain/types"
	exchangeClient "gorango/exchanges/internal/ws"
	"sync"
)

type Manager struct {
	mu       sync.RWMutex
	clients  map[string]exchangeClient.Client
	symbols  map[string]map[string]bool
	handlers map[string]types.EventHandler
}

func NewManager() *Manager {
	return &Manager{
		clients:  make(map[string]exchangeClient.Client),
		symbols:  make(map[string]map[string]bool),
		handlers: make(map[string]types.EventHandler),
	}
}

func (m *Manager) RegisterClient(exchange string, client exchangeClient.Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[exchange] = client
	m.symbols[exchange] = make(map[string]bool)
}

func (m *Manager) SetHandler(exchange string, handler types.EventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[exchange] = handler
}

func (m *Manager) Subscribe(exchange string, symbols []string) error {
	m.mu.RLock()
	client, ok := m.clients[exchange]
	handler := m.handlers[exchange]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown exchange: %s", exchange)
	}

	m.mu.Lock()
	for _, sym := range symbols {
		m.symbols[exchange][sym] = true
	}
	m.mu.Unlock()

	return client.Subscribe(symbols, handler)
}

func (m *Manager) Unsubscribe(exchange string, symbols []string) error {
	m.mu.RLock()
	client, ok := m.clients[exchange]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown exchange: %s", exchange)
	}

	m.mu.Lock()
	for _, sym := range symbols {
		delete(m.symbols[exchange], sym)
	}
	m.mu.Unlock()

	return client.Unsubscribe(symbols)
}

func (m *Manager) GetActiveSymbols(exchange string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	syms := make([]string, 0)
	for sym := range m.symbols[exchange] {
		syms = append(syms, sym)
	}
	return syms
}

func (m *Manager) GetActiveExchanges() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exchanges := make([]string, 0)
	for exchange := range m.clients {
		exchanges = append(exchanges, exchange)
	}
	return exchanges
}
