package bybit

import (
	"encoding/json"
	"fmt"
	"gorango/mdx/domain/types"
	"strconv"
	"strings"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) ParseMessage(data []byte) ([]types.Event, error) {
	var base struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return nil, fmt.Errorf("failed to unmarshal base message: %w", err)
	}
	switch {
	case strings.HasPrefix(base.Topic, "publicTrade."):
		return p.parseTrade(data)
	case strings.HasPrefix(base.Topic, "orderbook."):
		return p.parseOrderbook(data)
	case strings.HasPrefix(base.Topic, "kline."):
		return p.parseKline(data)
	default:
		return nil, nil
	}
}

func (p *Parser) parseTrade(data []byte) ([]types.Event, error) {
	var msg TradeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	parts := strings.Split(msg.Topic, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid trade topic: %s", msg.Topic)
	}
	symbol := p.normalizeSymbol(parts[1])
	events := make([]types.Event, 0, len(msg.Data))
	for _, trade := range msg.Data {
		price, _ := strconv.ParseFloat(trade.Price, 64)
		qty, _ := strconv.ParseFloat(trade.Qty, 64)
		side := strings.ToLower(trade.Side)
		isBuyerMaker := side == "sell"
		tradeTime, _ := strconv.ParseInt(trade.Time, 10, 64)
		events = append(events, types.Event{
			Type:      types.EventTypeTrade,
			Symbol:    symbol,
			Timestamp: tradeTime,
			Data: types.Trade{
				Price:        price,
				Quantity:     qty,
				Side:         side,
				IsBuyerMaker: isBuyerMaker,
			},
		})
	}
	return events, nil
}

func (p *Parser) parseOrderbook(data []byte) ([]types.Event, error) {
	var msg OrderbookMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	parts := strings.Split(msg.Topic, ".")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid orderbook topic: %s", msg.Topic)
	}
	symbol := p.normalizeSymbol(parts[2])
	events := make([]types.Event, 0, len(msg.Data.Bids)+len(msg.Data.Asks))
	for _, bid := range msg.Data.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			qty, _ := strconv.ParseFloat(bid[1], 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: msg.Ts,
				Data:      types.OrderbookUpdate{Side: "bid", Price: price, Quantity: qty},
			})
		}
	}
	for _, ask := range msg.Data.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			qty, _ := strconv.ParseFloat(ask[1], 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: msg.Ts,
				Data:      types.OrderbookUpdate{Side: "ask", Price: price, Quantity: qty},
			})
		}
	}
	return events, nil
}

func (p *Parser) parseKline(data []byte) ([]types.Event, error) {
	var msg KlineMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if len(msg.Data) == 0 || !msg.Data[0].Confirm {
		return nil, nil
	}
	return nil, nil
}

func (p *Parser) normalizeSymbol(bybitSymbol string) string {
	s := bybitSymbol
	if strings.HasSuffix(s, "USDT") {
		return s[:len(s)-4] + "/USDT:PERP"
	}
	if strings.HasSuffix(s, "USDC") {
		return s[:len(s)-4] + "/USDC:PERP"
	}
	if strings.HasSuffix(s, "USD") {
		return s[:len(s)-3] + "/USD:PERP"
	}
	return s + ":PERP"
}
