package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type HistoryRequest struct {
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
}

type HistoryResponse struct {
	Bars []types.Bar `json:"bars"`
}

type HistoryFetcher interface {
	GetHistory(ctx context.Context, symbol string, targetTf timeframe.Timeframe, start, end time.Time) ([]types.Bar, error)
}

type NATS struct {
	conn    *nats.Conn
	subject string
	logger  *slog.Logger
	mu      sync.RWMutex
	subs    []*nats.Subscription
}

func NewNATS(url, subject string) (*NATS, error) {
	return NewNATSWithLogger(url, subject, nil)
}

func NewNATSWithLogger(url, subject string, logger *slog.Logger) (*NATS, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	nc, err := nats.Connect(url,
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("nats connection closed")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}
	return &NATS{
		conn:    nc,
		subject: subject,
		logger:  logger,
		subs:    make([]*nats.Subscription, 0),
	}, nil
}

func (n *NATS) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, sub := range n.subs {
		_ = sub.Unsubscribe()
	}
	n.subs = nil

	if n.conn != nil {
		n.conn.Close()
	}
	return nil
}

func (n *NATS) GetConn() *nats.Conn {
	return n.conn
}

func (n *NATS) PublishEvent(event types.Event) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	subject := n.buildSubject(event)
	if err := n.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	if err := n.conn.Flush(); err != nil {
		return fmt.Errorf("flush event: %w", err)
	}

	return nil
}

func (n *NATS) buildSubject(event types.Event) string {
	parts := []string{"market", event.Symbol}

	switch event.Type {
	case types.EventTypeTrade:
		parts = append(parts, "trades")
	case types.EventTypeOrderbookUpdate:
		parts = append(parts, "ob")
	case types.EventTypeLiquidation:
		parts = append(parts, "liquidations")
	case types.EventTypeFundingRate:
		parts = append(parts, "funding")
	case types.EventTypeOpenInterest:
		parts = append(parts, "oi")
	}

	return strings.Join(parts, ".")
}

func (n *NATS) Subscribe(subject string, handler func(*types.Event)) (*nats.Subscription, error) {
	sub, err := n.conn.Subscribe(subject, func(msg *nats.Msg) {
		var event types.Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			n.logger.Error("unmarshal event failed", "subject", msg.Subject, "err", err)
			return
		}
		handler(&event)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	n.mu.Lock()
	n.subs = append(n.subs, sub)
	n.mu.Unlock()

	return sub, nil
}

func (n *NATS) QueueSubscribe(subject, queue string, handler func(*types.Event)) (*nats.Subscription, error) {
	sub, err := n.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		var event types.Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			n.logger.Error("unmarshal event failed", "subject", msg.Subject, "err", err)
			return
		}
		handler(&event)
	})
	if err != nil {
		return nil, fmt.Errorf("queue subscribe: %w", err)
	}

	n.mu.Lock()
	n.subs = append(n.subs, sub)
	n.mu.Unlock()

	return sub, nil
}

func (n *NATS) SubscribeHandler(ctx context.Context, subject string, handler func(*types.Event)) error {
	sub, err := n.Subscribe(subject, handler)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}

type Subscriber struct {
	conn   *nats.Conn
	logger *slog.Logger
	subs   []*nats.Subscription
	mu     sync.Mutex
}

func NewSubscriber(url string) (*Subscriber, error) {
	return NewSubscriberWithLogger(url, nil)
}

func NewSubscriberWithLogger(url string, logger *slog.Logger) (*Subscriber, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	nc, err := nats.Connect(url,
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Warn("nats subscriber disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("nats subscriber reconnected", "url", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}
	return &Subscriber{
		conn:   nc,
		logger: logger,
		subs:   make([]*nats.Subscription, 0),
	}, nil
}

func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}

	if s.conn != nil {
		s.conn.Close()
	}
	return nil
}

func (s *Subscriber) SubscribeMarketEvents(ctx context.Context, exchange, symbol string, handler func(*types.Event)) error {
	subjects := []string{
		fmt.Sprintf("market.%s.%s.trades", exchange, symbol),
		fmt.Sprintf("market.%s.%s.ob", exchange, symbol),
		fmt.Sprintf("market.%s.%s.liquidations", exchange, symbol),
		fmt.Sprintf("market.%s.%s.funding", exchange, symbol),
		fmt.Sprintf("market.%s.%s.oi", exchange, symbol),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var subs []*nats.Subscription

	for _, subject := range subjects {
		sub, err := s.conn.Subscribe(subject, func(msg *nats.Msg) {
			var event types.Event
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				s.logger.Error("unmarshal event failed", "subject", msg.Subject, "err", err)
				return
			}
			handler(&event)
		})
		if err != nil {
			for _, prevSub := range subs {
				_ = prevSub.Unsubscribe()
			}
			return fmt.Errorf("subscribe %s: %w", subject, err)
		}
		subs = append(subs, sub)
	}

	s.subs = append(s.subs, subs...)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, sub := range subs {
			_ = sub.Unsubscribe()
		}
	}()

	return nil
}

func BuildTradeSubject(exchange, symbol string) string {
	return fmt.Sprintf("market.%s.%s.trades", exchange, symbol)
}

func BuildOrderbookSubject(exchange, symbol string) string {
	return fmt.Sprintf("market.%s.%s.ob", exchange, symbol)
}

func BuildBarSubject(exchange, symbol, timeframe string) string {
	return fmt.Sprintf("market.%s.%s.bars.%s", exchange, symbol, timeframe)
}

type SubscriptionRequest struct {
	Exchange string   `json:"exchange"`
	Symbols  []string `json:"symbols"`
}

type SubscriptionResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type SubscriptionManager interface {
	Subscribe(exchange string, symbols []string) error
	Unsubscribe(exchange string, symbols []string) error
}

func HandleSubscriptionRequests(nc *nats.Conn, mgr SubscriptionManager, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	_, err := nc.Subscribe("subscriptions.subscribe", func(msg *nats.Msg) {
		logger.Info("subscription request received", "subject", msg.Subject, "data", string(msg.Data))

		var req SubscriptionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Error("unmarshal subscription request failed", "err", err, "data", string(msg.Data))
			resp := SubscriptionResponse{Success: false, Error: "invalid request"}
			data, _ := json.Marshal(resp)
			_ = msg.Respond(data)
			return
		}

		logger.Info("subscription request parsed", "exchange", req.Exchange, "symbols", req.Symbols)

		err := mgr.Subscribe(req.Exchange, req.Symbols)
		if err != nil {
			logger.Error("subscribe failed", "err", err, "exchange", req.Exchange)
			resp := SubscriptionResponse{Success: false, Error: err.Error()}
			data, _ := json.Marshal(resp)
			_ = msg.Respond(data)
			return
		}

		logger.Info("subscription successful", "exchange", req.Exchange, "symbols", req.Symbols)
		resp := SubscriptionResponse{Success: true}
		data, _ := json.Marshal(resp)
		_ = msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("subscribe to subscriptions.subscribe: %w", err)
	}

	_, err = nc.Subscribe("subscriptions.unsubscribe", func(msg *nats.Msg) {
		logger.Info("unsubscription request received", "subject", msg.Subject, "data", string(msg.Data))

		var req SubscriptionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Error("unmarshal unsubscription request failed", "err", err, "data", string(msg.Data))
			resp := SubscriptionResponse{Success: false, Error: "invalid request"}
			data, _ := json.Marshal(resp)
			_ = msg.Respond(data)
			return
		}

		logger.Info("unsubscription request parsed", "exchange", req.Exchange, "symbols", req.Symbols)

		err := mgr.Unsubscribe(req.Exchange, req.Symbols)
		if err != nil {
			logger.Error("unsubscribe failed", "err", err, "exchange", req.Exchange)
			resp := SubscriptionResponse{Success: false, Error: err.Error()}
			data, _ := json.Marshal(resp)
			_ = msg.Respond(data)
			return
		}

		logger.Info("unsubscription successful", "exchange", req.Exchange, "symbols", req.Symbols)
		resp := SubscriptionResponse{Success: true}
		data, _ := json.Marshal(resp)
		_ = msg.Respond(data)
	})
	if err != nil {
		return fmt.Errorf("subscribe to subscriptions.unsubscribe: %w", err)
	}

	return nil
}

func HandleHistoryRequests(nc *nats.Conn, fetcher HistoryFetcher, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	_, err := nc.Subscribe("history.bars", func(msg *nats.Msg) {
		logger.Info("history request received", "subject", msg.Subject, "data", string(msg.Data))

		var req HistoryRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Error("unmarshal history request failed", "err", err, "data", string(msg.Data))
			_ = msg.Respond([]byte(`{"error": "invalid request"}`))
			return
		}

		logger.Info("history request parsed", "exchange", req.Exchange, "symbol", req.Symbol, "timeframe", req.Timeframe, "start", req.StartTime, "end", req.EndTime)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		start := time.UnixMilli(req.StartTime)
		end := time.UnixMilli(req.EndTime)

		tf := timeframe.MustParse(req.Timeframe)

		logger.Info("fetching history", "symbol", req.Symbol, "tf", tf.ID, "start", start, "end", end)

		bars, err := fetcher.GetHistory(ctx, req.Symbol, tf, start, end)
		if err != nil {
			logger.Error("get history failed", "err", err, "symbol", req.Symbol, "tf", tf.ID)
			_ = msg.Respond([]byte(fmt.Sprintf(`{"error": %q}`, err.Error())))
			return
		}

		logger.Info("history fetched", "symbol", req.Symbol, "barCount", len(bars))

		type barJSON struct {
			Timestamp int64   `json:"timestamp"`
			Open      float64 `json:"open"`
			High      float64 `json:"high"`
			Low       float64 `json:"low"`
			Close     float64 `json:"close"`
			Volume    float64 `json:"volume"`
		}

		protoBars := make([]barJSON, len(bars))
		for i, bar := range bars {
			protoBars[i] = barJSON{
				Timestamp: bar.Time.UnixMilli(),
				Open:      bar.Open,
				High:      bar.High,
				Low:       bar.Low,
				Close:     bar.Close,
				Volume:    bar.Volume,
			}
		}

		response := map[string]interface{}{
			"bars": protoBars,
		}
		data, err := json.Marshal(response)
		if err != nil {
			logger.Error("marshal history response failed", "err", err)
			_ = msg.Respond([]byte(`{"error": "internal error"}`))
			return
		}

		_ = msg.Respond(data)
	})

	return err
}
