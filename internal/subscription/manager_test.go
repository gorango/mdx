package subscription

import (
	"context"
	"testing"
	"gorango/exchanges/domain/types"
	exchange "gorango/exchanges/internal/ws"
)

type mockClient struct {
	subscribedSymbols   []string
	unsubscribedSymbols []string
	connectCalled       bool
}

func (m *mockClient) Connect(ctx context.Context) error {
	m.connectCalled = true
	return nil
}

func (m *mockClient) Subscribe(symbols []string, handler types.EventHandler) error {
	m.subscribedSymbols = append(m.subscribedSymbols, symbols...)
	return nil
}

func (m *mockClient) Unsubscribe(symbols []string) error {
	m.unsubscribedSymbols = append(m.unsubscribedSymbols, symbols...)
	return nil
}

func (m *mockClient) Close() error {
	return nil
}

func (m *mockClient) IsConnected() bool {
	return true
}

func (m *mockClient) GetExchangeName() string {
	return "binance"
}

func (m *mockClient) SetConnectionHandler(handler exchange.ConnectionHandler) {
}

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("NewManager returned nil")
	}
	if mgr.clients == nil {
		t.Error("clients map not initialized")
	}
	if mgr.symbols == nil {
		t.Error("symbols map not initialized")
	}
	if mgr.handlers == nil {
		t.Error("handlers map not initialized")
	}
}

func TestRegisterClient(t *testing.T) {
	mgr := NewManager()
	mock := &mockClient{}

	mgr.RegisterClient("binance", mock)

	exchanges := mgr.GetActiveExchanges()
	if len(exchanges) != 1 || exchanges[0] != "binance" {
		t.Errorf("expected [binance], got %v", exchanges)
	}
}

func TestSetHandler(t *testing.T) {
	mgr := NewManager()
	handler := func(event types.Event) {}

	mgr.SetHandler("binance", handler)

	mgr.mu.RLock()
	h := mgr.handlers["binance"]
	mgr.mu.RUnlock()

	if h == nil {
		t.Error("handler not set")
	}
}

func TestSubscribe(t *testing.T) {
	mgr := NewManager()
	mock := &mockClient{}
	mgr.RegisterClient("binance", mock)

	err := mgr.Subscribe("binance", []string{"BTC/USDT:PERP", "ETH/USDT:PERP"})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if len(mock.subscribedSymbols) != 2 {
		t.Errorf("expected 2 subscribed symbols, got %d: %v", len(mock.subscribedSymbols), mock.subscribedSymbols)
	}
}

func TestUnsubscribe(t *testing.T) {
	mgr := NewManager()
	mock := &mockClient{}
	mgr.RegisterClient("binance", mock)
	mgr.SetHandler("binance", func(event types.Event) {})

	mgr.Subscribe("binance", []string{"BTC/USDT:PERP", "ETH/USDT:PERP"})

	err := mgr.Unsubscribe("binance", []string{"BTC/USDT:PERP"})
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	if len(mock.unsubscribedSymbols) != 1 || mock.unsubscribedSymbols[0] != "BTC/USDT:PERP" {
		t.Errorf("expected unsubscribed symbol BTC/USDT:PERP, got %v", mock.unsubscribedSymbols)
	}

	syms := mgr.GetActiveSymbols("binance")
	if len(syms) != 1 || syms[0] != "ETH/USDT:PERP" {
		t.Errorf("expected [ETH/USDT:PERP], got %v", syms)
	}
}

func TestSubscribeUnknownExchange(t *testing.T) {
	mgr := NewManager()

	err := mgr.Subscribe("unknown", []string{"BTC/USDT:PERP"})
	if err == nil {
		t.Error("expected error for unknown exchange")
	}
}

func TestUnsubscribeUnknownExchange(t *testing.T) {
	mgr := NewManager()

	err := mgr.Unsubscribe("unknown", []string{"BTC/USDT:PERP"})
	if err == nil {
		t.Error("expected error for unknown exchange")
	}
}

func TestGetActiveSymbolsEmpty(t *testing.T) {
	mgr := NewManager()
	mgr.RegisterClient("binance", &mockClient{})

	syms := mgr.GetActiveSymbols("binance")
	if len(syms) != 0 {
		t.Errorf("expected empty slice, got %v", syms)
	}
}

func TestGetActiveSymbolsUnknownExchange(t *testing.T) {
	mgr := NewManager()

	syms := mgr.GetActiveSymbols("unknown")
	if len(syms) != 0 {
		t.Errorf("expected empty slice for unknown exchange, got %v", syms)
	}
}

func TestGetActiveExchangesEmpty(t *testing.T) {
	mgr := NewManager()

	exchanges := mgr.GetActiveExchanges()
	if len(exchanges) != 0 {
		t.Errorf("expected empty slice, got %v", exchanges)
	}
}
