package binance

import (
	"testing"
	"gorango/exchanges/domain/types"

	"github.com/stretchr/testify/assert"
)

func TestParseAggTrade(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@aggTrade",
		"data": {
			"e": "aggTrade",
			"E": 1234567890,
			"s": "BTCUSDT",
			"a": 123,
			"p": "100.50",
			"q": "0.5",
			"f": 1,
			"l": 1,
			"T": 1234567890,
			"m": false
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	trade := events[0]
	assert.Equal(t, types.EventTypeTrade, trade.Type)
	assert.Equal(t, "BTC/USDT:PERP", trade.Symbol)
	assert.Equal(t, int64(1234567890), trade.Timestamp)

	data := trade.Data.(types.Trade)
	assert.Equal(t, 100.50, data.Price)
	assert.Equal(t, 0.5, data.Quantity)
	assert.Equal(t, "buy", data.Side)
	assert.False(t, data.IsBuyerMaker)
}

func TestParseAggTradeBuyerMaker(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "ethusdt@aggTrade",
		"data": {
			"e": "aggTrade",
			"E": 1234567890,
			"s": "ETHUSDT",
			"a": 456,
			"p": "2000.00",
			"q": "1.0",
			"T": 1234567890,
			"m": true
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	data := events[0].Data.(types.Trade)
	assert.Equal(t, "sell", data.Side)
	assert.True(t, data.IsBuyerMaker)
}

func TestParseBookTicker(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@bookTicker",
		"data": {
			"e": "bookTicker",
			"E": 1234567890,
			"s": "BTCUSDT",
			"b": "100.0",
			"B": "1.0",
			"a": "101.0",
			"A": "2.0"
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 2)

	assert.Equal(t, types.EventTypeOrderbookUpdate, events[0].Type)
	bid := events[0].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid.Side)
	assert.Equal(t, 100.0, bid.Price)
	assert.Equal(t, 1.0, bid.Quantity)

	assert.Equal(t, types.EventTypeOrderbookUpdate, events[1].Type)
	ask := events[1].Data.(types.OrderbookUpdate)
	assert.Equal(t, "ask", ask.Side)
	assert.Equal(t, 101.0, ask.Price)
	assert.Equal(t, 2.0, ask.Quantity)
}

func TestParseBookTickerZeroQty(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@bookTicker",
		"data": {
			"e": "bookTicker",
			"E": 1234567890,
			"s": "BTCUSDT",
			"b": "100.0",
			"B": "0.0",
			"a": "101.0",
			"A": "2.0"
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "ask", events[0].Data.(types.OrderbookUpdate).Side)
}

func TestParseDepthUpdate(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@depth20",
		"data": {
			"e": "depthUpdate",
			"E": 1234567890,
			"s": "BTCUSDT",
			"U": 1,
			"u": 2,
			"b": [["100.0", "1.0"], ["99.0", "2.0"]],
			"a": [["101.0", "1.5"]]
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 3)

	bid1 := events[0].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid1.Side)
	assert.Equal(t, 100.0, bid1.Price)
	assert.Equal(t, 1.0, bid1.Quantity)

	bid2 := events[1].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid2.Side)
	assert.Equal(t, 99.0, bid2.Price)
}

func TestParseMarkPrice(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@markPrice@1s",
		"data": {
			"e": "markPriceUpdate",
			"E": 1234567890,
			"s": "BTCUSDT",
			"p": "100.0",
			"i": "99.5",
			"P": "100.2",
			"r": "0.0001",
			"T": 1234567891000
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	assert.Equal(t, types.EventTypeFundingRate, events[0].Type)
	fr := events[0].Data.(types.FundingRate)
	assert.Equal(t, 0.0001, fr.Rate)
}

func TestParseForceOrder(t *testing.T) {
	p := NewParser()

	msg := `{
		"stream": "btcusdt@forceOrder",
		"data": {
			"e": "forceOrder",
			"E": 1234567890,
			"o": {
				"s": "BTCUSDT",
				"S": "BUY",
				"o": "LIMIT",
				"f": "IOC",
				"q": "1.0",
				"p": "100.0",
				"ap": "100.0",
				"X": "FILLED",
				"l": "1.0",
				"L": "100.0",
				"T": 1234567890
			}
		}
	}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	assert.Equal(t, types.EventTypeLiquidation, events[0].Type)
	liq := events[0].Data.(types.Liquidation)
	assert.Equal(t, "BUY", liq.Side)
	assert.Equal(t, 1.0, liq.Quantity)
	assert.Equal(t, 100.0, liq.Price)
}

func TestNormalizeSymbol(t *testing.T) {
	p := NewParser()

	assert.Equal(t, "BTC/USDT:PERP", p.normalizeSymbol("BTCUSDT"))
	assert.Equal(t, "ETH/USD:PERP", p.normalizeSymbol("ETHUSD"))
	assert.Equal(t, "SOLB/USD:PERP", p.normalizeSymbol("SOLBUSD"))
	assert.Equal(t, "BTC:PERP", p.normalizeSymbol("BTC"))
}

func TestParseStreamMessageUnknown(t *testing.T) {
	p := NewParser()

	msg := `{"stream": "btcusdt@unknown", "data": {}}`

	events, err := p.ParseStreamMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestParseStreamMessageInvalidStream(t *testing.T) {
	p := NewParser()

	msg := `{"stream": "invalid", "data": {}}`

	_, err := p.ParseStreamMessage([]byte(msg))
	assert.Error(t, err)
}
