package binance

import (
	"encoding/json"
	"fmt"
	"gorango/exchanges/domain/types"
	"strconv"
	"strings"
	"time"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) ParseStreamMessage(data []byte) ([]types.Event, error) {
	var combined CombinedStreamMessage
	if err := json.Unmarshal(data, &combined); err != nil {
		return nil, fmt.Errorf("failed to unmarshal combined message: %w", err)
	}

	parts := strings.Split(combined.Stream, "@")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid stream format: %s", combined.Stream)
	}

	symbol := p.normalizeSymbol(parts[0])
	streamType := parts[1]
	streamTypeLower := strings.ToLower(streamType)

	switch {
	case strings.HasPrefix(streamTypeLower, "aggtrade"):
		return p.parseAggTrade(symbol, combined.Data)
	case streamTypeLower == "bookticker":
		return p.parseBookTicker(symbol, combined.Data)
	case strings.HasPrefix(streamTypeLower, "depth"):
		return p.parseDepthUpdate(symbol, combined.Data)
	case strings.HasPrefix(streamTypeLower, "markprice"):
		return p.parseMarkPrice(symbol, combined.Data)
	case streamTypeLower == "forceorder":
		return p.parseForceOrder(symbol, combined.Data)
	default:
		return nil, nil
	}
}

func (p *Parser) parseAggTrade(symbol string, data json.RawMessage) ([]types.Event, error) {
	var trade AggTrade
	if err := json.Unmarshal(data, &trade); err != nil {
		return nil, err
	}
	price, _ := strconv.ParseFloat(trade.Price, 64)
	qty, _ := strconv.ParseFloat(trade.Quantity, 64)
	side := "buy"
	if trade.IsBuyerMaker {
		side = "sell"
	}
	tradeCount := int(trade.LastTradeID - trade.FirstTradeID + 1)
	if tradeCount < 1 {
		tradeCount = 1
	}
	return []types.Event{{
		Type:      types.EventTypeTrade,
		Symbol:    symbol,
		Timestamp: trade.Timestamp,
		Data: types.Trade{
			Price:        price,
			Quantity:     qty,
			Side:         side,
			IsBuyerMaker: trade.IsBuyerMaker,
			TradeCount:   tradeCount,
		},
	}}, nil
}

func (p *Parser) parseBookTicker(symbol string, data json.RawMessage) ([]types.Event, error) {
	var ticker BookTicker
	if err := json.Unmarshal(data, &ticker); err != nil {
		return nil, err
	}
	bidPrice, _ := strconv.ParseFloat(ticker.BidPrice, 64)
	bidQty, _ := strconv.ParseFloat(ticker.BidQty, 64)
	askPrice, _ := strconv.ParseFloat(ticker.AskPrice, 64)
	askQty, _ := strconv.ParseFloat(ticker.AskQty, 64)

	events := make([]types.Event, 0, 2)
	if bidQty > 0 {
		events = append(events, types.Event{
			Type:      types.EventTypeOrderbookUpdate,
			Symbol:    symbol,
			Timestamp: ticker.EventTime,
			Data:      types.OrderbookUpdate{Side: "bid", Price: bidPrice, Quantity: bidQty},
		})
	}
	if askQty > 0 {
		events = append(events, types.Event{
			Type:      types.EventTypeOrderbookUpdate,
			Symbol:    symbol,
			Timestamp: ticker.EventTime,
			Data:      types.OrderbookUpdate{Side: "ask", Price: askPrice, Quantity: askQty},
		})
	}
	return events, nil
}

func (p *Parser) parseDepthUpdate(symbol string, data json.RawMessage) ([]types.Event, error) {
	var depth DepthUpdate
	if err := json.Unmarshal(data, &depth); err != nil {
		return nil, err
	}

	events := make([]types.Event, 0, len(depth.Bids)+len(depth.Asks))
	for _, bid := range depth.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			qty, _ := strconv.ParseFloat(bid[1], 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: depth.EventTime,
				Data:      types.OrderbookUpdate{Side: "bid", Price: price, Quantity: qty},
			})
		}
	}
	for _, ask := range depth.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			qty, _ := strconv.ParseFloat(ask[1], 64)
			events = append(events, types.Event{
				Type:      types.EventTypeOrderbookUpdate,
				Symbol:    symbol,
				Timestamp: depth.EventTime,
				Data:      types.OrderbookUpdate{Side: "ask", Price: price, Quantity: qty},
			})
		}
	}
	return events, nil
}

func (p *Parser) parseMarkPrice(symbol string, data json.RawMessage) ([]types.Event, error) {
	var mp MarkPriceUpdate
	if err := json.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	rate, _ := strconv.ParseFloat(mp.FundingRate, 64)
	return []types.Event{{
		Type:      types.EventTypeFundingRate,
		Symbol:    symbol,
		Timestamp: mp.EventTime,
		Data: types.FundingRate{
			Rate:     rate,
			NextTime: time.Unix(mp.NextFundingTime/1000, 0),
		},
	}}, nil
}

func (p *Parser) parseForceOrder(symbol string, data json.RawMessage) ([]types.Event, error) {
	var order ForceOrder
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, err
	}
	qty, _ := strconv.ParseFloat(order.Order.LastFilledQty, 64)
	price, _ := strconv.ParseFloat(order.Order.LastFilledPrice, 64)
	return []types.Event{{
		Type:      types.EventTypeLiquidation,
		Symbol:    p.normalizeSymbol(order.Order.Symbol),
		Timestamp: order.EventTime,
		Data: types.Liquidation{
			Side:     order.Order.Side,
			Quantity: qty,
			Price:    price,
		},
	}}, nil
}

func (p *Parser) normalizeSymbol(binanceSymbol string) string {
	s := strings.ToUpper(binanceSymbol)
	if strings.HasSuffix(s, "USDT") {
		return s[:len(s)-4] + "/USDT:PERP"
	}
	if strings.HasSuffix(s, "USD") {
		return s[:len(s)-3] + "/USD:PERP"
	}
	if strings.HasSuffix(s, "BUSD") {
		return s[:len(s)-4] + "/BUSD:PERP"
	}
	return s + ":PERP"
}
