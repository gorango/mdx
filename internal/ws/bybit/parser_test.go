package bybit

import (
	"gorango/mdx/domain/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTrade(t *testing.T) {
	p := NewParser()

	msg := `{
		"topic": "publicTrade.BTCUSDT",
		"type": "snapshot",
		"ts": 1234567890,
		"data": [
			{
				"s": "BTCUSDT",
				"p": "50000.00",
				"v": "1.5",
				"S": "Buy",
				"T": "1234567890000"
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 1)

	assert.Equal(t, types.EventTypeTrade, events[0].Type)
	assert.Equal(t, "BTC/USDT:PERP", events[0].Symbol)

	data := events[0].Data.(types.Trade)
	assert.Equal(t, 50000.00, data.Price)
	assert.Equal(t, 1.5, data.Quantity)
	assert.Equal(t, "buy", data.Side)
	assert.False(t, data.IsBuyerMaker)
}

func TestParseTradeSellerMaker(t *testing.T) {
	p := NewParser()

	msg := `{
		"topic": "publicTrade.ETHUSDT",
		"type": "snapshot",
		"ts": 1234567890,
		"data": [
			{
				"s": "ETHUSDT",
				"p": "3000.00",
				"v": "2.0",
				"S": "Sell",
				"T": "1234567890000"
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)

	data := events[0].Data.(types.Trade)
	assert.Equal(t, "sell", data.Side)
	assert.True(t, data.IsBuyerMaker)
}

func TestParseOrderbook(t *testing.T) {
	p := NewParser()

	msg := `{
		"topic": "orderbook.1.BTCUSDT",
		"type": "snapshot",
		"ts": 1234567890,
		"data": {
			"s": "BTCUSDT",
			"b": [["50000.0", "1.0"], ["49900.0", "2.0"]],
			"a": [["50100.0", "1.5"]]
		}
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 3)

	bid1 := events[0].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid1.Side)
	assert.Equal(t, 50000.0, bid1.Price)
	assert.Equal(t, 1.0, bid1.Quantity)

	bid2 := events[1].Data.(types.OrderbookUpdate)
	assert.Equal(t, "bid", bid2.Side)
	assert.Equal(t, 49900.0, bid2.Price)

	ask := events[2].Data.(types.OrderbookUpdate)
	assert.Equal(t, "ask", ask.Side)
	assert.Equal(t, 50100.0, ask.Price)
}

func TestParseKlineConfirm(t *testing.T) {
	p := NewParser()

	msg := `{
		"topic": "kline.1.BTCUSDT",
		"type": "snapshot",
		"ts": 1234567890,
		"data": [
			{
				"start": 1234567890,
				"end": 1234567950,
				"interval": "1",
				"confirm": true,
				"open": "50000",
				"close": "50100",
				"high": "50200",
				"low": "49900",
				"volume": "100"
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestParseKlineNotConfirm(t *testing.T) {
	p := NewParser()

	msg := `{
		"topic": "kline.1.BTCUSDT",
		"type": "snapshot",
		"ts": 1234567890,
		"data": [
			{
				"confirm": false
			}
		]
	}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestNormalizeSymbol(t *testing.T) {
	p := NewParser()

	assert.Equal(t, "BTC/USDT:PERP", p.normalizeSymbol("BTCUSDT"))
	assert.Equal(t, "ETH/USDC:PERP", p.normalizeSymbol("ETHUSDC"))
	assert.Equal(t, "SOL/USD:PERP", p.normalizeSymbol("SOLUSD"))
}

func TestParseMessageUnknown(t *testing.T) {
	p := NewParser()

	msg := `{"topic": "unknown"}`

	events, err := p.ParseMessage([]byte(msg))
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}
