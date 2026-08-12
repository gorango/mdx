package main

import (
	"context"
	"flag"
	"fmt"
	"gorango/mdx/internal/cache"
	"gorango/mdx/internal/config"
	"gorango/mdx/internal/db"
	"gorango/mdx/internal/pubsub"
	"gorango/mdx/internal/rest"
	"gorango/mdx/internal/trading"
	exchange "gorango/mdx/internal/ws"
	"gorango/mdx/internal/ws/binance"
	"gorango/mdx/internal/ws/bybit"
	"gorango/mdx/internal/ws/hyperliquid"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
)

type statusHandler struct{}

func (h *statusHandler) OnStatusChange(name string, status exchange.ConnectionStatus) {
	switch status {
	case exchange.ConnectionStatusConnecting:
		fmt.Printf("[%s] Connection status: connecting\n", name)
	case exchange.ConnectionStatusConnected:
		fmt.Printf("[%s] Connection status: connected\n", name)
	case exchange.ConnectionStatusReconnecting:
		fmt.Printf("[%s] Connection status: reconnecting\n", name)
	case exchange.ConnectionStatusDisconnected:
		fmt.Printf("[%s] Connection status: disconnected\n", name)
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	flag.Parse()

	_ = godotenv.Load()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		fmt.Printf("Failed to connect to database: PG_URL env var not set\n")
		os.Exit(1)
	}

	database, err := db.New(pgURL)
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()
	fmt.Println("Connected to database")

	var natsClient *pubsub.NATS
	natsClient, err = pubsub.NewNATS(*natsURL, "market")
	if err != nil {
		fmt.Printf("Warning: Failed to connect to NATS: %v\n", err)
		fmt.Println("Continuing without NATS...")
	} else {
		defer func() { _ = natsClient.Close() }()
		fmt.Println("Connected to NATS")
	}

	connectors := make(map[string]trading.Connector)

	if cfg.Exchanges.Binance.Enabled {
		conn, err := trading.NewCCXTConnector("binance", cfg.Exchanges.Binance.APIKey, cfg.Exchanges.Binance.Secret)
		if err != nil {
			fmt.Printf("Failed to create binance connector: %v\n", err)
		} else {
			connectors["binance"] = conn
		}
	}
	if cfg.Exchanges.Bybit.Enabled {
		conn, err := trading.NewCCXTConnector("bybit", cfg.Exchanges.Bybit.APIKey, cfg.Exchanges.Bybit.Secret)
		if err != nil {
			fmt.Printf("Failed to create bybit connector: %v\n", err)
		} else {
			connectors["bybit"] = conn
		}
	}

	for id, conn := range connectors {
		if err := conn.Connect(context.Background()); err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", id, err)
		} else {
			fmt.Printf("Connected trading to %s\n", id)
		}
	}

	// Route engine orders arriving on NATS (`orders.>`) to a live connector.
	if natsClient != nil {
		var liveID string
		var liveConn trading.Connector
		for _, id := range []string{"binance", "bybit"} {
			if conn, ok := connectors[id]; ok {
				liveID, liveConn = id, conn
				break
			}
		}
		if liveConn != nil {
			bridge := trading.NewOrderBridge(natsClient.GetConn(), liveConn, slog.Default())
			if err := bridge.Start(); err != nil {
				fmt.Printf("Warning: Failed to start order bridge: %v\n", err)
			} else {
				fmt.Printf("Order bridge listening for %s orders on NATS\n", liveID)
			}
		}
	}

	var priceCache *cache.PriceCache
	if cfg.Exchanges.Binance.Enabled {
		restClient := rest.NewBinance(rest.Config{Testnet: cfg.Exchanges.Binance.Testnet})
		priceCache = cache.NewPriceCache("binance", database, restClient, slog.Default())

		if natsClient != nil {
			if err := pubsub.HandleHistoryRequests(natsClient.GetConn(), priceCache, slog.Default()); err != nil {
				fmt.Printf("Warning: Failed to setup history handler: %v\n", err)
			} else {
				fmt.Println("NATS history handler registered")
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	exchangeClients := make(map[string]exchange.Client)
	connHandler := &statusHandler{}

	if cfg.Exchanges.Binance.Enabled {
		wg.Add(1)
		exchangeClients["binance"] = binance.NewClient(cfg.Exchanges.Binance.Testnet)
		exchangeClients["binance"].SetConnectionHandler(connHandler)
		go func() {
			defer wg.Done()
			exchange.StartExchange(ctx, "binance", exchangeClients["binance"], cfg.Exchanges.Binance.Symbols, nil, false)
		}()
	}

	if cfg.Exchanges.Bybit.Enabled {
		wg.Add(1)
		exchangeClients["bybit"] = bybit.NewClient(cfg.Exchanges.Bybit.Testnet)
		exchangeClients["bybit"].SetConnectionHandler(connHandler)
		go func() {
			defer wg.Done()
			exchange.StartExchange(ctx, "bybit", exchangeClients["bybit"], cfg.Exchanges.Bybit.Symbols, nil, false)
		}()
	}

	if cfg.Exchanges.Hyperliquid.Enabled {
		wg.Add(1)
		exchangeClients["hyperliquid"] = hyperliquid.NewClient()
		exchangeClients["hyperliquid"].SetConnectionHandler(connHandler)
		go func() {
			defer wg.Done()
			exchange.StartExchange(ctx, "hyperliquid", exchangeClients["hyperliquid"], cfg.Exchanges.Hyperliquid.Symbols, nil, false)
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")

	cancel()

	for _, client := range exchangeClients {
		_ = client.Close()
	}

	for id, conn := range connectors {
		if err := conn.Close(); err != nil {
			fmt.Printf("Error closing connector %s: %v\n", id, err)
		}
	}

	wg.Wait()

	fmt.Println("Shutdown complete")
}
