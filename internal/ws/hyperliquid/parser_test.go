package hyperliquid

import (
	"gorango/mdx/domain/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTrades(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "trades",
		"data": [
			{
				"coin": "BTC",
				"side": "B",
				"px": "50000.00",
				"sz": "1.5",
				"time": 1234567890000,
				"hash": "0x123"
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	assert.Equal(t, types.EventTypeTrade, events[0].Type)
	assert.Equal(t, "BTC/USDC:PERP", events[0].Symbol)
	assert.Equal(t, int64(1234567890000), events[0].Timestamp)

	data := events[0].Data.(types.Trade)
	assert.Equal(t, 50000.00, data.Price)
	assert.Equal(t, 1.5, data.Quantity)
	assert.Equal(t, "buy", data.Side)
	assert.False(t, data.IsBuyerMaker)
}

func TestParseTradesSeller(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "trades",
		"data": [
			{
				"coin": "ETH",
				"side": "S",
				"px": "3000.00",
				"sz": "2.0",
				"time": 1234567890000
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)

	data := events[0].Data.(types.Trade)
	assert.Equal(t, "sell", data.Side)
	assert.True(t, data.IsBuyerMaker)
}

func TestParseTradesEmpty(t *testing.T) {
	p := NewParser()

	msg := `{"channel": "trades", "data": []}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestParseBBO(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "bbo",
		"data": {
			"coin": "BTC",
			"bid": "50000.0",
			"ask": "50100.0",
			"bidSz": "1.5",
			"askSz": "2.0",
			"time": 1234567890000
		}
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, "BTC/USDC:PERP", events[0].Symbol)

	bid := events[0].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid.Side)
	assert.Equal(t, 50000.0, bid.Price)
	assert.Equal(t, 1.5, bid.Quantity)

	ask := events[1].Data.(types.OrderbookUpdate)
	assert.Equal(t, "ask", ask.Side)
	assert.Equal(t, 50100.0, ask.Price)
}

func TestParseBBOZeroSz(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "bbo",
		"data": {
			"coin": "BTC",
			"bid": "50000.0",
			"ask": "50100.0",
			"bidSz": "0",
			"askSz": "2.0",
			"time": 1234567890000
		}
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "ask", events[0].Data.(types.OrderbookUpdate).Side)
}

func TestParseL2Book(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "l2Book",
		"data": {
			"coin": "BTC",
			"time": 1234567890000,
			"levels": [
				[
					{"px": "50000.0", "sz": "1.5"},
					{"px": "49900.0", "sz": "2.0"}
				],
				[
					{"px": "50100.0", "sz": "1.0"},
					{"px": "50200.0", "sz": "0.5"}
				]
			]
		}
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 4)

	bid1 := events[0].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid1.Side)
	assert.Equal(t, 50000.0, bid1.Price)

	ask1 := events[2].Data.(types.OrderbookUpdate)
	assert.Equal(t, "ask", ask1.Side)
	assert.Equal(t, 50100.0, ask1.Price)
}

func TestParseCandle(t *testing.T) {
	p := NewParser()

	msg := `{
		"channel": "candle",
		"data": {
			"coin": "BTC",
			"interval": "1m"
		}
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestParseMessageUnknownChannel(t *testing.T) {
	p := NewParser()

	msg := `{"channel": "unknownChannel"}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestParseMessageError(t *testing.T) {
	p := NewParser()

	msg := `{"channel": "trades", "error": "some error"}`

	_, err := p.ParseMessage([]byte(msg))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hyperliquid error")
}

func TestNormalizeSymbol(t *testing.T) {
	p := NewParser()

	assert.Equal(t, "BTC/USDC:PERP", p.normalizeSymbol("BTC"))
	assert.Equal(t, "ETH/USDC:PERP", p.normalizeSymbol("eth"))
	assert.Equal(t, "SOL/USDC:PERP", p.normalizeSymbol("SOL"))
}
