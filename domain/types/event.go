package types

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventTypeTrade           EventType = "trade"
	EventTypeOrderbookUpdate EventType = "orderbook_update"
	EventTypeLiquidation     EventType = "liquidation"
	EventTypeFundingRate     EventType = "funding_rate"
	EventTypeOpenInterest    EventType = "open_interest"
)

type Trade struct {
	Price        float64 `json:"price"`
	Quantity     float64 `json:"quantity"`
	Side         string  `json:"side"`
	IsBuyerMaker bool    `json:"is_buyer_maker"`
	TradeCount   int     `json:"trade_count"`
}

type OrderbookUpdate struct {
	Side     string  `json:"side"`
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

type Liquidation struct {
	Side     string  `json:"side"`
	Quantity float64 `json:"quantity"`
	Price    float64 `json:"price"`
}

type FundingRate struct {
	Rate     float64   `json:"rate"`
	NextTime time.Time `json:"next_time"`
}

type OpenInterest struct {
	Value float64 `json:"value"`
}

type Event struct {
	Type      EventType   `json:"type"`
	Symbol    string      `json:"symbol"`
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

type EventHandler func(event Event)

func (e Event) MarshalJSON() ([]byte, error) {
	raw := rawEvent{
		Type:      e.Type,
		Symbol:    e.Symbol,
		Timestamp: e.Timestamp,
	}
	switch d := e.Data.(type) {
	case Trade:
		raw.Trade = &d
	case OrderbookUpdate:
		raw.OrderbookUpdate = &d
	case Liquidation:
		raw.Liquidation = &d
	case FundingRate:
		raw.FundingRate = &d
	case OpenInterest:
		raw.OpenInterest = &d
	}
	return json.Marshal(raw)
}

func (e *Event) UnmarshalJSON(b []byte) error {
	var raw rawEvent
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	e.Type = raw.Type
	e.Symbol = raw.Symbol
	e.Timestamp = raw.Timestamp
	switch {
	case raw.Trade != nil:
		e.Data = *raw.Trade
	case raw.OrderbookUpdate != nil:
		e.Data = *raw.OrderbookUpdate
	case raw.Liquidation != nil:
		e.Data = *raw.Liquidation
	case raw.FundingRate != nil:
		e.Data = *raw.FundingRate
	case raw.OpenInterest != nil:
		e.Data = *raw.OpenInterest
	}
	return nil
}

type rawEvent struct {
	Type            EventType        `json:"type"`
	Symbol          string           `json:"symbol"`
	Timestamp       int64            `json:"timestamp"`
	Trade           *Trade           `json:"trade,omitempty"`
	OrderbookUpdate *OrderbookUpdate `json:"orderbook_update,omitempty"`
	Liquidation     *Liquidation     `json:"liquidation,omitempty"`
	FundingRate     *FundingRate     `json:"funding_rate,omitempty"`
	OpenInterest    *OpenInterest    `json:"open_interest,omitempty"`
}
