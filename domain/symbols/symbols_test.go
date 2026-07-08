package symbols

import "testing"

func TestCanonicalToExchange(t *testing.T) {
	tests := []struct {
		name      string
		canonical string
		exchange  string
		want      string
	}{
		{"BTC/USDT:PERP to binance_futures", "BTC/USDT:PERP", "binance_futures", "BTCUSDT"},
		{"ETH/USDT:PERP to binance_futures", "ETH/USDT:PERP", "binance_futures", "ETHUSDT"},
		{"BTC/USDT:PERP to bybit_perpetual", "BTC/USDT:PERP", "bybit_perpetual", "BTCUSDT"},
		{"BTC/USDT:SPOT to binance", "BTC/USDT:SPOT", "binance", "BTC/USDT"},
		{"BTC/USDC:PERP to binance_futures", "BTC/USDC:PERP", "binance_futures", "BTCUSDC"},
		{"SOL/USDT:PERP to hyperliquid", "SOL/USDT:PERP", "hyperliquid", "SOL"},
		{"BTC/USDT:PERP to hyperliquid", "BTC/USDT:PERP", "hyperliquid", "BTC"},
		{"lowercase to uppercase", "btc/usdt:perp", "binance_futures", "BTCUSDT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalToExchange(tt.canonical, tt.exchange)
			if got != tt.want {
				t.Errorf("CanonicalToExchange(%q, %q) = %q, want %q", tt.canonical, tt.exchange, got, tt.want)
			}
		})
	}
}

func TestExchangeToCanonical(t *testing.T) {
	tests := []struct {
		name     string
		exchange string
		symbol   string
		want     string
	}{
		{"BTCUSDT to binance_futures", "binance_futures", "BTCUSDT", "BTC/USDT:PERP"},
		{"ETHUSDT to binance_futures", "binance_futures", "ETHUSDT", "ETH/USDT:PERP"},
		{"BTC/USDT to binance (spot)", "binance", "BTC/USDT", "BTC/USDT:SPOT"},
		{"BTCUSDT to binance (spot)", "binance", "BTCUSDT", "BTC/USDT:SPOT"},
		{"BTCUSDT to bybit_perpetual", "bybit_perpetual", "BTCUSDT", "BTC/USDT:PERP"},
		{"BTCUSDC to bybit_perpetual", "bybit_perpetual", "BTCUSDC", "BTC/USDC:PERP"},
		{"SOL to hyperliquid", "hyperliquid", "SOL", "SOL/USDC:PERP"},
		{"BTC to hyperliquid", "hyperliquid", "BTC", "BTC/USDC:PERP"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExchangeToCanonical(tt.exchange, tt.symbol)
			if got != tt.want {
				t.Errorf("ExchangeToCanonical(%q, %q) = %q, want %q", tt.exchange, tt.symbol, got, tt.want)
			}
		})
	}
}

func TestMapExchangeName(t *testing.T) {
	tests := []struct {
		name     string
		exchange string
		want     string
	}{
		{"binance to binance_futures", "binance", "binance_futures"},
		{"bybit to bybit_perpetual", "bybit", "bybit_perpetual"},
		{"hyperliquid stays same", "hyperliquid", "hyperliquid"},
		{"unknown stays same", "unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapExchangeName(tt.exchange)
			if got != tt.want {
				t.Errorf("MapExchangeName(%q) = %q, want %q", tt.exchange, got, tt.want)
			}
		})
	}
}

func TestNormalizeCanonical(t *testing.T) {
	tests := []struct {
		name   string
		symbol string
		want   string
	}{
		{"BTC/USDT:PERP unchanged", "BTC/USDT:PERP", "BTC/USDT:PERP"},
		{"BTCUSDT to BTC/USDT:PERP", "BTCUSDT", "BTC/USDT:PERP"},
		{"btcusdt to uppercase", "btcusdt", "BTC/USDT:PERP"},
		{"BTC/USDT:SPOT unchanged", "BTC/USDT:SPOT", "BTC/USDT:SPOT"},
		{"BTC/USDC:PERP unchanged", "BTC/USDC:PERP", "BTC/USDC:PERP"},
		{"BTCUSDC to BTC/USDC:PERP", "BTCUSDC", "BTC/USDC:PERP"},
		{"with spaces", "BTC / USDT : PERP", "BTC/USDT:PERP"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeCanonical(tt.symbol)
			if got != tt.want {
				t.Errorf("NormalizeCanonical(%q) = %q, want %q", tt.symbol, got, tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		canonical string
		exchange  string
	}{
		{"BTC/USDT:PERP", "binance_futures"},
		{"ETH/USDT:PERP", "binance_futures"},
		{"SOL/USDT:PERP", "bybit_perpetual"},
		{"BTC/USDT:SPOT", "binance"},
		{"ETH/USDC:PERP", "binance_futures"},
	}

	for _, c := range cases {
		t.Run(c.canonical+"/"+c.exchange, func(t *testing.T) {
			exchangeSymbol := CanonicalToExchange(c.canonical, c.exchange)
			canonical := ExchangeToCanonical(c.exchange, exchangeSymbol)
			if canonical != c.canonical {
				t.Errorf("Round trip failed: %q -> %q -> %q", c.canonical, exchangeSymbol, canonical)
			}
		})
	}
}
