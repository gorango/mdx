package binance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildMarketStreamsIncludesFunding(t *testing.T) {
	c := NewClient(false)
	streams := c.buildMarketStreams([]string{"BTC/USDT:PERP", "ETH/USDT:PERP"})

	assert.Len(t, streams, 6)
	assert.Contains(t, streams, "btcusdt@aggTrade")
	assert.Contains(t, streams, "btcusdt@markPrice@1s")
	assert.Contains(t, streams, "btcusdt@forceOrder")
	assert.Contains(t, streams, "ethusdt@markPrice@1s")
}

func TestBuildMarketStreamsEmpty(t *testing.T) {
	c := NewClient(false)
	assert.Empty(t, c.buildMarketStreams(nil))
}
