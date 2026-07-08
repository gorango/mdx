package symbols

import "strings"

func CanonicalToExchange(canonical, exchange string) string {
	s := strings.ToUpper(canonical)

	hasSlash := strings.Contains(s, "/")
	hasColon := strings.Contains(s, ":")

	var base, quote, suffix string
	if hasSlash {
		parts := strings.Split(s, "/")
		base = parts[0]
		if len(parts) > 1 {
			rest := parts[1]
			if hasColon {
				suffixParts := strings.Split(rest, ":")
				quote = suffixParts[0]
				if len(suffixParts) > 1 {
					suffix = suffixParts[1]
				}
			} else {
				quote = rest
			}
		}
	} else if hasColon {
		suffixParts := strings.Split(s, ":")
		base = suffixParts[0]
		if len(suffixParts) > 1 {
			suffix = suffixParts[1]
		}
	}

	switch exchange {
	case "binance_futures", "bybit_perpetual", "dydx":
		return base + quote
	case "hyperliquid":
		return base
	case "binance":
		if suffix == "SPOT" {
			return base + "/" + quote
		}
		return base + quote
	default:
		if suffix == "PERP" {
			return base + quote
		}
		return base + "/" + quote
	}
}

func ExchangeToCanonical(exchange, symbol string) string {
	switch exchange {
	case "binance_futures":
		if strings.HasSuffix(symbol, "USDT") {
			return symbol[:len(symbol)-4] + "/USDT:PERP"
		}
		if strings.HasSuffix(symbol, "USDC") {
			return symbol[:len(symbol)-4] + "/USDC:PERP"
		}
	case "binance":
		if strings.Contains(symbol, "/") {
			return symbol + ":SPOT"
		}
		if strings.HasSuffix(symbol, "USDT") {
			return symbol[:len(symbol)-4] + "/USDT:SPOT"
		}
	case "bybit", "bybit_perpetual":
		if strings.HasSuffix(symbol, "USDT") {
			return symbol[:len(symbol)-4] + "/USDT:PERP"
		}
		if strings.HasSuffix(symbol, "USDC") {
			return symbol[:len(symbol)-4] + "/USDC:PERP"
		}
	case "hyperliquid":
		return symbol + "/USDC:PERP"
	case "dydx":
		return symbol + "/USDT:PERP"
	}
	return symbol + ":PERP"
}

func MapExchangeName(exchange string) string {
	switch exchange {
	case "binance":
		return "binance_futures"
	case "bybit":
		return "bybit_perpetual"
	case "hyperliquid":
		return "hyperliquid"
	default:
		return exchange
	}
}

func MapExchangeToDB(exchange string) string {
	if strings.HasSuffix(exchange, "_futures") {
		return strings.TrimSuffix(exchange, "_futures")
	}
	return exchange
}

func NormalizeCanonical(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	s = strings.ReplaceAll(s, " ", "")
	if !strings.Contains(s, "/") {
		if strings.HasSuffix(s, "USDT") {
			return s[:len(s)-4] + "/USDT:PERP"
		}
		if strings.HasSuffix(s, "USDC") {
			return s[:len(s)-4] + "/USDC:PERP"
		}
	}
	return s
}
