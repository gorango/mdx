package hyperliquid

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
		Channel string `json:"channel"`
		Error   string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return nil, fmt.Errorf("failed to unmarshal base message: %w", err)
	}
	if base.Error != "" {
		return nil, fmt.Errorf("hyperliquid error: %s", base.Error)
	}
	switch base.Channel {
	case "trades":
		return p.parseTrades(data)
	case "candle":
		return p.parseCandle(data)
	case "bbo":
		return p.parseBBO(data)
	case "l2Book":
		return p.parseL2Book(data)
	default:
		return nil, nil
	}
}

func (p *Parser) parseTrades(data []byte) ([]types.Event, error) {
	var msg TradesMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if len(msg.Data) == 0 {
		return nil, nil
	}
	symbol := p.normalizeSymbol(msg.Data[0].Coin)
	events := make([]types.Event, 0, len(msg.Data))
	for _, trade := range msg.Data {
		price, _ := strconv.ParseFloat(trade.Price, 64)
		size, _ := strconv.ParseFloat(trade.Size, 64)
		side := "buy"
		isBuyerMaker := false
		if trade.Side == "S" {
			side = "sell"
			isBuyerMaker = true
		}
		events = append(events, types.Event{
			Type:      types.EventTypeTrade,
			Symbol:    symbol,
			Timestamp: trade.Time,
			Data: types.Trade{
				Price:        price,
				Quantity:     size,
				Side:         side,
				IsBuyerMaker: isBuyerMaker,
			},
		})
	}
	return events, nil
}

func (p *Parser) parseCandle(data []byte) ([]types.Event, error) {
	var msg CandleMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p *Parser) parseBBO(data []byte) ([]types.Event, error) {
	var msg BBOMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	symbol := p.normalizeSymbol(msg.Data.Coin)
	events := make([]types.Event, 0, 2)
	bidPrice, _ := strconv.ParseFloat(msg.Data.Bid, 64)
	bidSz, _ := strconv.ParseFloat(msg.Data.BidSz, 64)
	askPrice, _ := strconv.ParseFloat(msg.Data.Ask, 64)
	askSz, _ := strconv.ParseFloat(msg.Data.AskSz, 64)
	if bidSz > 0 {
		events = append(events, types.Event{
			Type:      types.EventTypeOrderbookUpdate,
			Symbol:    symbol,
			Timestamp: msg.Data.Time,
			Data:      types.OrderbookUpdate{Side: "bid", Price: bidPrice, Quantity: bidSz},
		})
	}
	if askSz > 0 {
		events = append(events, types.Event{
			Type:      types.EventTypeOrderbookUpdate,
			Symbol:    symbol,
			Timestamp: msg.Data.Time,
			Data:      types.OrderbookUpdate{Side: "ask", Price: askPrice, Quantity: askSz},
		})
	}
	return events, nil
}

func (p *Parser) parseL2Book(data []byte) ([]types.Event, error) {
	var msg L2BookMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	symbol := p.normalizeSymbol(msg.Data.Coin)
	events := make([]types.Event, 0)
	if len(msg.Data.Levels) >= 1 {
		for _, level := range msg.Data.Levels[0] {
			price, _ := strconv.ParseFloat(level.Price, 64)
			size, _ := strconv.ParseFloat(level.Size, 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: msg.Data.Time,
				Data:      types.OrderbookUpdate{Side: "bid", Price: price, Quantity: size},
			})
		}
	}
	if len(msg.Data.Levels) >= 2 {
		for _, level := range msg.Data.Levels[1] {
			price, _ := strconv.ParseFloat(level.Price, 64)
			size, _ := strconv.ParseFloat(level.Size, 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: msg.Data.Time,
				Data:      types.OrderbookUpdate{Side: "ask", Price: price, Quantity: size},
			})
		}
	}
	return events, nil
}

func (p *Parser) normalizeSymbol(coin string) string {
	return strings.ToUpper(coin) + "/USDC:PERP"
}
