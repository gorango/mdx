package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeVWAP(t *testing.T) {
	result := computeVWAP(1000.0, 10.0)
	assert.InDelta(t, 100.0, result, 0.001)

	result = computeVWAP(500.0, 0.0)
	assert.Equal(t, 0.0, result)

	result = computeVWAP(0.0, 5.0)
	assert.Equal(t, 0.0, result)
}

func TestComputeSpreadBPS(t *testing.T) {
	result := computeSpreadBPS(100.0, 101.0)
	assert.InDelta(t, 100.0, result, 0.001)

	result = computeSpreadBPS(100.0, 100.0)
	assert.Equal(t, 0.0, result)

	result = computeSpreadBPS(100.0, 99.0)
	assert.Equal(t, 0.0, result)

	result = computeSpreadBPS(0.0, 101.0)
	assert.Equal(t, 0.0, result)
}

func TestAggregatorNew(t *testing.T) {
	agg := New("BTC/USDT:PERP")
	assert.NotNil(t, agg)
	assert.Equal(t, "BTC/USDT:PERP", agg.symbol)
	assert.NotNil(t, agg.bids)
	assert.NotNil(t, agg.asks)
	assert.NotNil(t, agg.bars)
}

func TestAggregatorFinalizeEmpty(t *testing.T) {
	agg := New("BTC/USDT:PERP")

	bars := agg.Finalize(true, 1000)
	assert.Nil(t, bars)
}
