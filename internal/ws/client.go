package exchange

import (
	"context"
	"gorango/mdx/domain/types"
)

type ConnectionStatus int

const (
	ConnectionStatusDisconnected ConnectionStatus = iota
	ConnectionStatusConnecting
	ConnectionStatusConnected
	ConnectionStatusReconnecting
)

type ConnectionHandler interface {
	OnStatusChange(name string, status ConnectionStatus)
}

type Client interface {
	Connect(ctx context.Context) error
	Subscribe(symbols []string, handler types.EventHandler) error
	Unsubscribe(symbols []string) error
	Close() error
	IsConnected() bool
	GetExchangeName() string
	SetConnectionHandler(handler ConnectionHandler)
}
