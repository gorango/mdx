package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"gorango/mdx/domain/symbols"
	"gorango/mdx/domain/types"
	"gorango/mdx/internal/cache"
	"gorango/mdx/internal/config"
	"gorango/mdx/internal/db"
	"gorango/mdx/internal/orderbook/aggregator/streaming"
	"gorango/mdx/internal/orderbook/flusher"
	"gorango/mdx/internal/orderbook/pipeline"
	"gorango/mdx/internal/pubsub"
	"gorango/mdx/internal/rest"
	exchange "gorango/mdx/internal/ws"
	"gorango/mdx/internal/ws/binance"
	"gorango/mdx/internal/ws/bybit"
	"gorango/mdx/internal/ws/hyperliquid"
)

type Streamer struct {
	cfg        *config.Config
	database   *db.DB
	natsURL    string
	natsClient *pubsub.NATS
	natsMu     sync.RWMutex
	aggMgr     *streaming.Manager
	flusher    *flusher.Flusher
	priceCache *cache.PriceCache

	clients     map[string]exchange.Client
	connHandler exchange.ConnectionHandler
	handler     types.EventHandler
	logger      *slog.Logger

	backfillOB bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Options struct {
	ConfigPath string
	NatsURL    string
	Symbols    []string
	Logger     *slog.Logger
	BackfillOB bool
}

func New(opts Options) (*Streamer, error) {
	_ = godotenv.Load()

	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	if len(opts.Symbols) > 0 {
		if cfg.Exchanges.Binance.Enabled {
			cfg.Exchanges.Binance.Symbols = opts.Symbols
		}
		if cfg.Exchanges.Bybit.Enabled {
			cfg.Exchanges.Bybit.Symbols = opts.Symbols
		}
		if cfg.Exchanges.Hyperliquid.Enabled {
			cfg.Exchanges.Hyperliquid.Symbols = opts.Symbols
		}
	}

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		return nil, fmt.Errorf("PG_URL env var not set")
	}

	database, err := db.New(pgURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	var natsClient *pubsub.NATS
	natsClient, err = pubsub.NewNATS(opts.NatsURL, "market")
	if err != nil {
		fmt.Printf("Warning: Failed to connect to NATS: %v\n", err)
		fmt.Println("Continuing without NATS publishing...")
	}

	ctx, cancel := context.WithCancel(context.Background())

	aggMgr := streaming.NewManager()

	flusher := flusher.NewFlusher(database, cfg.Flusher.IntervalSeconds, cfg.Flusher.MaxBatchSize)
	if err := flusher.Start(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("start flusher: %w", err)
	}

	var priceCache *cache.PriceCache
	if cfg.Exchanges.Binance.Enabled {
		restClient := rest.NewBinance(rest.Config{Testnet: cfg.Exchanges.Binance.Testnet})
		priceCache = cache.NewPriceCache("binance", database, restClient, opts.Logger)

		if natsClient != nil {
			if err := pubsub.HandleHistoryRequests(natsClient.GetConn(), priceCache, opts.Logger); err != nil {
				fmt.Printf("Warning: Failed to setup history handler: %v\n", err)
			} else {
				fmt.Println("NATS history handler registered")
			}
		}
	}

	s := &Streamer{
		cfg:        cfg,
		database:   database,
		natsURL:    opts.NatsURL,
		natsClient: natsClient,
		aggMgr:     aggMgr,
		flusher:    flusher,
		priceCache: priceCache,
		backfillOB: opts.BackfillOB,
		clients:    make(map[string]exchange.Client),
		logger:     opts.Logger,
		ctx:        ctx,
		cancel:     cancel,
	}

	s.connHandler = &statusHandler{logger: opts.Logger, aggMgr: aggMgr}
	s.handler = s.makeEventHandler()

	return s, nil
}

func (s *Streamer) Start() error {
	fmt.Println("Connected to database")
	if nc := s.GetNATS(); nc != nil {
		defer func() { _ = nc.Close() }()
		fmt.Println("Connected to NATS")
	} else {
		go s.retryNATS()
	}

	s.startFlusherTicker()
	s.startOpenInterestPoller()
	s.startExchangeClients()
	s.startOBBackfill()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	return s.Shutdown()
}

func (s *Streamer) Shutdown() error {
	fmt.Println("Shutting down...")

	s.cancel()
	s.wg.Wait()

	bars := s.aggMgr.FlushAll()
	for symbol, barList := range bars {
		s.flusher.Add("binance", symbol, barList)
	}
	_ = s.flusher.Flush(context.Background())

	stats := s.flusher.Stats()
	fmt.Printf("Flusher stats: %+v\n", stats)

	s.database.Close()

	fmt.Println("Shutdown complete")
	return nil
}

func (s *Streamer) GetPriceCache() *cache.PriceCache {
	return s.priceCache
}

func (s *Streamer) GetNATS() *pubsub.NATS {
	s.natsMu.RLock()
	defer s.natsMu.RUnlock()
	return s.natsClient
}

func (s *Streamer) SetNATS(nc *pubsub.NATS) {
	s.natsMu.Lock()
	defer s.natsMu.Unlock()
	s.natsClient = nc
}

func (s *Streamer) retryNATS() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			nc, err := pubsub.NewNATS(s.natsURL, "market")
			if err != nil {
				continue
			}
			s.SetNATS(nc)
			fmt.Println("NATS connection established (retry succeeded)")
			return
		}
	}
}

func (s *Streamer) GetConfig() *config.Config {
	return s.cfg
}

type tradeBar struct {
	time    time.Time
	open    float64
	high    float64
	low     float64
	close   float64
	volume  float64
	flushed bool
}

type priceBarWithSymbol struct {
	symbol string
	bar    types.Bar
}

func (s *Streamer) makeEventHandler() types.EventHandler {
	tradeBars := make(map[string]*tradeBar)
	var tradeBarsMu sync.Mutex
	var completedBars []priceBarWithSymbol
	var completedBarsMu sync.Mutex

	handler := func(event types.Event) {
		if err := s.aggMgr.ProcessEvent(event); err != nil {
			fmt.Printf("Error processing event: %v\n", err)
		}

		if nc := s.GetNATS(); nc != nil {
			if err := nc.PublishEvent(event); err != nil {
				fmt.Printf("Error publishing event: %v\n", err)
			}
		}

		if event.Type == types.EventTypeTrade {
			trade, ok := event.Data.(types.Trade)
			if !ok {
				return
			}
			tradeBarsMu.Lock()
			defer tradeBarsMu.Unlock()

			barStart := time.UnixMilli(event.Timestamp).Truncate(time.Minute).Add(time.Minute)
			pb, exists := tradeBars[event.Symbol]
			if !exists || pb.time.Before(barStart) {
				if exists && !pb.flushed {
					completedBarsMu.Lock()
					completedBars = append(completedBars, priceBarWithSymbol{
						symbol: event.Symbol,
						bar: types.Bar{
							Time:   pb.time,
							Open:   pb.open,
							High:   pb.high,
							Low:    pb.low,
							Close:  pb.close,
							Volume: pb.volume,
						},
					})
					completedBarsMu.Unlock()

					pb.flushed = true

					if nc := s.GetNATS(); nc != nil {
						subject := fmt.Sprintf("market.%s.%s.bars.1m", "binance", event.Symbol)
						barData := map[string]interface{}{
							"timestamp": pb.time.UnixMilli(),
							"open":      pb.open,
							"high":      pb.high,
							"low":       pb.low,
							"close":     pb.close,
							"volume":    pb.volume,
						}
						data, _ := json.Marshal(barData)
						_ = nc.GetConn().Publish(subject, data)
					}
				}
				tradeBars[event.Symbol] = &tradeBar{
					time:    barStart,
					open:    trade.Price,
					high:    trade.Price,
					low:     trade.Price,
					close:   trade.Price,
					volume:  trade.Quantity,
					flushed: false,
				}
			} else {
				pb.high = math.Max(pb.high, trade.Price)
				pb.low = math.Min(pb.low, trade.Price)
				pb.close = trade.Price
				pb.volume += trade.Quantity
			}
		}
	}

	s.startPriceBarFlusher(&completedBars, &completedBarsMu, &tradeBars, &tradeBarsMu)

	return handler
}

func (s *Streamer) startPriceBarFlusher(completedBars *[]priceBarWithSymbol, completedBarsMu *sync.Mutex, tradeBars *map[string]*tradeBar, tradeBarsMu *sync.Mutex) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				completedBarsMu.Lock()
				barsToFlush := *completedBars
				*completedBars = nil
				completedBarsMu.Unlock()

				if len(barsToFlush) > 0 {
					barsBySymbol := make(map[string][]types.Bar)
					for _, pb := range barsToFlush {
						barsBySymbol[pb.symbol] = append(barsBySymbol[pb.symbol], pb.bar)
					}
					for symbol, bars := range barsBySymbol {
						if err := s.database.InsertPriceBars(context.Background(), "binance", symbol, bars); err != nil {
							fmt.Printf("Error inserting price bars for %s: %v\n", symbol, err)
						} else {
							fmt.Printf("Flushed %d price bars for %s\n", len(bars), symbol)
						}
					}
				}
			}
		}
	}()
}

func (s *Streamer) startFlusherTicker() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(timeUntilNextMinute()):
				bars := s.aggMgr.FlushAll()
				for symbol, barList := range bars {
					exchangeName := "binance"
					s.flusher.Add(exchangeName, symbol, barList)
				}
			}
		}
	}()
}

func timeUntilNextMinute() time.Duration {
	now := time.Now()
	return time.Minute - time.Duration(now.Second())*time.Second - time.Duration(now.Nanosecond())
}

func (s *Streamer) startOpenInterestPoller() {
	if !s.cfg.Exchanges.Binance.Enabled {
		return
	}

	restClient := rest.NewBinance(rest.Config{Testnet: s.cfg.Exchanges.Binance.Testnet})

	// Sequential per-symbol polling in a 5s tick starves late symbols: with
	// ~110 symbols × HTTP round-trip latency the full cycle can exceed the
	// tick, so most symbols get OI samples only sporadically (many bars end up
	// with a single stale sample or none — the root of sparse oi_change data).
	// Poll the whole roster concurrently each tick with a bounded worker pool.
	const oiWorkers = 10

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		sem := make(chan struct{}, oiWorkers)
		symbols := s.cfg.Exchanges.Binance.Symbols

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				start := time.Now()
				var wg sync.WaitGroup
				var okMu sync.Mutex
				ok := 0

				for _, symbol := range symbols {
					select {
					case sem <- struct{}{}:
					case <-s.ctx.Done():
						return
					}
					wg.Add(1)
					go func(sym string) {
						defer wg.Done()
						defer func() { <-sem }()

						oi, err := restClient.FetchOpenInterest(s.ctx, sym)
						if err != nil {
							fmt.Printf("[OI] Failed to fetch for %s: %v\n", sym, err)
							return
						}
						okMu.Lock()
						ok++
						okMu.Unlock()

						event := types.Event{
							Type:      types.EventTypeOpenInterest,
							Symbol:    sym,
							Timestamp: time.Now().UnixMilli(),
							Data: types.OpenInterest{
								Value: oi,
							},
						}
						if err := s.aggMgr.ProcessEvent(event); err != nil {
							fmt.Printf("[OI] Error processing event for %s: %v\n", sym, err)
						}
					}(symbol)
				}

				wg.Wait()
				elapsed := time.Since(start)
				if elapsed > 5*time.Second {
					fmt.Printf(
						"[OI] WARN cycle took %v (> 5s tick) — %d/%d symbols sampled; OI samples may be stale\n",
						elapsed, ok, len(symbols),
					)
				}
			}
		}
	}()
}

func (s *Streamer) startExchangeClients() {
	if s.cfg.Exchanges.Binance.Enabled {
		s.wg.Add(1)
		client := binance.NewClient(s.cfg.Exchanges.Binance.Testnet)
		client.SetConnectionHandler(s.connHandler)
		client.OnDepthReset = func(symbols []string) {
			for _, sym := range symbols {
				s.aggMgr.ResetDepth(sym)
			}
			s.logger.Info("depth reset after reconnect", "symbols", len(symbols))
		}
		s.clients["binance"] = client
		go func() {
			defer s.wg.Done()
			exchange.StartExchange(s.ctx, "binance", client, s.cfg.Exchanges.Binance.Symbols, s.handler, true)
		}()
	}

	if s.cfg.Exchanges.Bybit.Enabled {
		s.wg.Add(1)
		client := bybit.NewClient(s.cfg.Exchanges.Bybit.Testnet)
		client.SetConnectionHandler(s.connHandler)
		s.clients["bybit"] = client
		go func() {
			defer s.wg.Done()
			exchange.StartExchange(s.ctx, "bybit", client, s.cfg.Exchanges.Bybit.Symbols, s.handler, true)
		}()
	}

	if s.cfg.Exchanges.Hyperliquid.Enabled {
		s.wg.Add(1)
		client := hyperliquid.NewClient()
		client.SetConnectionHandler(s.connHandler)
		s.clients["hyperliquid"] = client
		go func() {
			defer s.wg.Done()
			exchange.StartExchange(s.ctx, "hyperliquid", client, s.cfg.Exchanges.Hyperliquid.Symbols, s.handler, true)
		}()
	}
}

type statusHandler struct {
	logger *slog.Logger
	aggMgr *streaming.Manager
}

func (h *statusHandler) OnStatusChange(name string, status exchange.ConnectionStatus) {
	if name == "binance" && h.aggMgr != nil {
		available := status == exchange.ConnectionStatusConnected
		h.aggMgr.SetLiquidationFeedAvailable(available)
		h.logger.Info("liquidation feed status",
			"available", available,
			"connection", status,
		)
	}

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

func (s *Streamer) startOBBackfill() {
	if !s.backfillOB {
		return
	}
	if !s.cfg.Exchanges.Binance.Enabled {
		return
	}

	go func() {
		now := time.Now()
		next := now.Truncate(time.Hour).Add(time.Hour)
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}

		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			s.runOBBackfill()

			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Streamer) runOBBackfill() {
	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		s.logger.Error("CRYPTO_HFT_DATA not set, skipping ob backfill")
		return
	}

	now := time.Now().UTC()
	end := now.Truncate(time.Hour).Add(-time.Hour)
	start := end.Add(-time.Hour)

	allSymbols := s.cfg.Exchanges.Binance.Symbols
	if len(allSymbols) == 0 {
		return
	}

	const exchange = "binance_futures"

	for _, sym := range allSymbols {
		if !strings.Contains(sym, "USDT") {
			continue
		}

		canonical := symbols.NormalizeCanonical(sym)

		config := pipeline.Config{
			Symbol:    canonical,
			StartDate: start,
			EndDate:   end,
			Exchange:  exchange,
			APIKey:    apiKey,
			// OVERWRITE: rebuild the previous hour from cryptoHFT parquets +
			// settled Binance funding every hour.  The live stream may be down,
			// so the DB must be reconstructible from the historical source
			// alone; overwriting keeps every bar consistent (settled funding,
			// cryptoHFT orderbook/trades/OI/liq) instead of a mix of live and
			// historical cadences.  Overwrite:false runs are the manual
			// gap-fill-merge path (per-bar upsert in InsertOrderbookBars:
			// higher-trade-count bar wins, COALESCE fills NULL OI/funding/liq,
			// GREATEST liq_covered).
			Overwrite: true,
		}

		coordinator := pipeline.NewCoordinator(config, s.database)
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Minute)
		err := coordinator.Run(ctx)
		cancel()

		if err != nil {
			s.logger.Error("OB backfill failed",
				"symbol", canonical,
				"error", err,
			)
		} else {
			s.logger.Info("OB backfill completed",
				"symbol", canonical,
				"start", start.Format("2006-01-02T15"),
				"end", end.Format("2006-01-02T15"),
			)
		}
	}
}
