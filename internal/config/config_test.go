package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadExchanges(t *testing.T) {
	content := `
exchanges:
  binance:
    enabled: true
    testnet: false
    symbols:
      - BTC/USDT:PERP
      - ETH/USDT:PERP
  bybit:
    enabled: false
    testnet: true
    symbols:
      - BTC/USDT:PERP
`
	tmp, err := os.CreateTemp("", "config-*.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(content)
	assert.NoError(t, err)
	tmp.Close()

	cfg, err := LoadExchanges(tmp.Name())
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	assert.True(t, cfg.Binance.Enabled)
	assert.False(t, cfg.Binance.Testnet)
	assert.Equal(t, []string{"BTC/USDT:PERP", "ETH/USDT:PERP"}, cfg.Binance.Symbols)

	assert.False(t, cfg.Bybit.Enabled)
	assert.True(t, cfg.Bybit.Testnet)
}

func TestLoadExchangesNotFound(t *testing.T) {
	cfg, err := LoadExchanges("/nonexistent/config.yaml")
	assert.Error(t, err)
	assert.Nil(t, cfg)
}
