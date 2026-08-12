package exchange

import (
	"context"
	"fmt"
	"gorango/mdx/domain/types"
	"math"
	"time"
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

// StartExchange connects a client and keeps it alive until ctx is done,
// retrying with exponential backoff on connect or subscribe failures.
// If subscribe is false, no subscriptions are made (execution-events only).
func StartExchange(ctx context.Context, name string, client Client, symbols []string, handler types.EventHandler, subscribe bool) {
	const maxReconnectDelay = 30 * time.Second
	reconnectAttempts := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Context cancelled, stopping\n", name)
			_ = client.Close()
			return
		default:
		}

		if err := client.Connect(ctx); err != nil {
			delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(reconnectAttempts))))
			fmt.Printf("[%s] Failed to connect: %v (attempt %d, retrying in %v)\n", name, err, reconnectAttempts+1, delay)
			reconnectAttempts++
			time.Sleep(delay)
			continue
		}

		if subscribe {
			if err := client.Subscribe(symbols, handler); err != nil {
				delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(reconnectAttempts))))
				fmt.Printf("[%s] Failed to subscribe: %v (attempt %d, retrying in %v)\n", name, err, reconnectAttempts+1, delay)
				reconnectAttempts++
				time.Sleep(delay)
				continue
			}
			fmt.Printf("[%s] Connected and subscribed\n", name)
		} else {
			fmt.Printf("[%s] Connected (execution events only)\n", name)
		}

		<-ctx.Done()
		fmt.Printf("[%s] Closing connection\n", name)
		_ = client.Close()
		return
	}
}
