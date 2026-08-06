package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/config"
	"gorango/exchanges/internal/db"
	"gorango/exchanges/internal/orderbook/aggregator/streaming"
	"gorango/exchanges/internal/orderbook/api"
	"gorango/exchanges/internal/orderbook/flusher"
	"gorango/exchanges/internal/pubsub"
	"gorango/exchanges/internal/rest"
	exchange "gorango/exchanges/internal/ws"
	"gorango/exchanges/internal/ws/binance"
	"gorango/exchanges/internal/ws/bybit"
	"gorango/exchanges/internal/ws/hyperliquid"
)

type statusHandler struct {
	aggMgr *streaming.Manager
}

func (h *statusHandler) OnStatusChange(name string, status exchange.ConnectionStatus) {
	connected := status == exchange.ConnectionStatusConnected
	if h.aggMgr != nil {
		h.aggMgr.SetLiquidationFeedAvailable(connected)
	}

	switch status {
	case exchange.ConnectionStatusConnecting:
		fmt.Printf("[%s] Connection status: connecting, liq_covered=%v\n", name, connected)
	case exchange.ConnectionStatusConnected:
		fmt.Printf("[%s] Connection status: connected, liq_covered=%v\n", name, connected)
	case exchange.ConnectionStatusReconnecting:
		fmt.Printf("[%s] Connection status: reconnecting, liq_covered=%v\n", name, connected)
	case exchange.ConnectionStatusDisconnected:
		fmt.Printf("[%s] Connection status: disconnected, liq_covered=%v\n", name, connected)
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
		fmt.Println("Continuing without NATS publishing...")
	} else {
		defer func() { _ = natsClient.Close() }()
		fmt.Println("Connected to NATS")
	}

	aggMgr := streaming.NewManager()
	aggMgr.SetLiquidationFeedAvailable(true)

	flusher := flusher.NewFlusher(database, cfg.Flusher.IntervalSeconds, cfg.Flusher.MaxBatchSize)
	if err := flusher.Start(context.Background()); err != nil {
		fmt.Printf("Failed to start flusher: %v\n", err)
		os.Exit(1)
	}
	defer flusher.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				bars := aggMgr.FlushAll()
				for symbol, barList := range bars {
					exchangeName := "binance"
					flusher.Add(exchangeName, symbol, barList)
				}
			}
		}
	}()

	if cfg.Exchanges.Binance.Enabled {
		go startOpenInterestPoller(ctx, cfg, aggMgr)
		go startFundingRatePoller(ctx, cfg, aggMgr)
	}

	handler := func(event types.Event) {
		if err := aggMgr.ProcessEvent(event); err != nil {
			fmt.Printf("Error processing event: %v\n", err)
		}

		if natsClient != nil {
			if err := natsClient.PublishEvent(event); err != nil {
				fmt.Printf("Error publishing event: %v\n", err)
			}
		}
	}

	connHandler := &statusHandler{aggMgr: aggMgr}

	var wg sync.WaitGroup

	if cfg.Exchanges.Binance.Enabled {
		client := binance.NewClient(cfg.Exchanges.Binance.Testnet)
		client.SetConnectionHandler(connHandler)
		client.OnDepthReset = func(symbols []string) {
			for _, sym := range symbols {
				aggMgr.ResetDepth(sym)
			}
			fmt.Printf("[binance] Depth reset for %d symbols: %v\n", len(symbols), symbols)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			startExchange(ctx, "binance", client, cfg.Exchanges.Binance.Symbols, handler)
		}()
	}

	if cfg.Exchanges.Bybit.Enabled {
		client := bybit.NewClient(cfg.Exchanges.Bybit.Testnet)
		client.SetConnectionHandler(connHandler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			startExchange(ctx, "bybit", client, cfg.Exchanges.Bybit.Symbols, handler)
		}()
	}

	if cfg.Exchanges.Hyperliquid.Enabled {
		client := hyperliquid.NewClient()
		client.SetConnectionHandler(connHandler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			startExchange(ctx, "hyperliquid", client, cfg.Exchanges.Hyperliquid.Symbols, handler)
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")

	cancel()
	wg.Wait()

	bars := aggMgr.FlushAll()
	for symbol, barList := range bars {
		flusher.Add("binance", symbol, barList)
	}
	_ = flusher.Flush(context.Background())

	stats := flusher.Stats()
	fmt.Printf("Flusher stats: %+v\n", stats)

	fmt.Println("Shutdown complete")
}

func startOpenInterestPoller(ctx context.Context, cfg *config.Config, aggMgr *streaming.Manager) {
	restClient := rest.NewBinance(rest.Config{Testnet: cfg.Exchanges.Binance.Testnet})
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, symbol := range cfg.Exchanges.Binance.Symbols {
				oi, err := restClient.FetchOpenInterest(ctx, symbol)
				if err != nil {
					fmt.Printf("[OI] Failed to fetch for %s: %v\n", symbol, err)
					continue
				}

				event := types.Event{
					Type:      types.EventTypeOpenInterest,
					Symbol:    symbol,
					Timestamp: time.Now().UnixMilli(),
					Data: types.OpenInterest{
						Value: oi,
					},
				}

				if err := aggMgr.ProcessEvent(event); err != nil {
					fmt.Printf("[OI] Error processing event for %s: %v\n", symbol, err)
				}
			}
		}
	}
}

func startFundingRatePoller(ctx context.Context, cfg *config.Config, aggMgr *streaming.Manager) {
	bc := api.NewBinanceClient()
	lastRate := make(map[string]float64)

	poll := func() {
		for _, symbol := range cfg.Exchanges.Binance.Symbols {
			exchangeSymbol := symbols.CanonicalToExchange(symbol, "binance_futures")
			point, err := bc.FetchLatestFundingRate(exchangeSymbol)
			if err != nil {
				fmt.Printf("[FR] Failed to fetch for %s: %v\n", symbol, err)
				continue
			}

			if prev, ok := lastRate[symbol]; ok && point.Rate == prev {
				continue
			}
			lastRate[symbol] = point.Rate

			event := types.Event{
				Type:      types.EventTypeFundingRate,
				Symbol:    symbol,
				Timestamp: time.Now().UnixMilli(),
				Data: types.FundingRate{
					Rate: point.Rate,
				},
			}

			if err := aggMgr.ProcessEvent(event); err != nil {
				fmt.Printf("[FR] Error processing event for %s: %v\n", symbol, err)
			}
		}
	}

	poll()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

func startExchange(ctx context.Context, name string, client exchange.Client, symbols []string, handler types.EventHandler) {
	fmt.Printf("[%s] Starting client with symbols: %v\n", name, symbols)

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

		if err := client.Subscribe(symbols, handler); err != nil {
			delay := time.Duration(math.Min(float64(maxReconnectDelay), float64(time.Second)*math.Pow(2, float64(reconnectAttempts))))
			fmt.Printf("[%s] Failed to subscribe: %v (attempt %d, retrying in %v)\n", name, err, reconnectAttempts+1, delay)
			reconnectAttempts++
			time.Sleep(delay)
			continue
		}

		fmt.Printf("[%s] Connected and subscribed\n", name)

		<-ctx.Done()
		fmt.Printf("[%s] Closing connection\n", name)
		_ = client.Close()
		return
	}
}
