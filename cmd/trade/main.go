package main

import (
	"context"
	"flag"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/trading"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

func main() {
	exchange := flag.String("exchange", "paper", "Exchange or 'paper'")
	balance := flag.String("balance", "", "Initial balance (e.g. USDT=10000)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("Usage: trade <command> [args]")
		fmt.Println("Commands: balance, positions, order")
		fmt.Println("  balance              - Show account balance")
		fmt.Println("  positions             - Show open positions")
		fmt.Println("  order <symbol> <side> <amount> [price] - Submit order")
		fmt.Println("Examples:")
		fmt.Println("  trade -exchange=paper balance")
		fmt.Println("  trade -exchange=paper order BTC/USDT:PERP buy 0.1")
		fmt.Println("  trade -exchange=binance -balance=USDT=100000 order BTC/USDT:PERP sell 0.05 95000")
		os.Exit(1)
	}

	cmd := args[0]
	ctx := context.Background()

	var conn trading.Connector
	switch *exchange {
	case "paper":
		balances := parseBalance(*balance)
		conn = trading.NewPaperConnector("paper", balances)
	default:
		apiKey := os.Getenv("BINANCE_API_KEY")
		secret := os.Getenv("BINANCE_SECRET_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("EXCHANGE_API_KEY")
			secret = os.Getenv("EXCHANGE_SECRET")
		}
		secret = strings.ReplaceAll(secret, "\\n", "\n")
		conn2, err := trading.NewCCXTConnector(*exchange, apiKey, secret)
		if err != nil {
			fmt.Printf("Failed to create connector: %v\n", err)
			os.Exit(1)
		}
		conn = conn2
	}

	if err := conn.Connect(ctx); err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	switch cmd {
	case "balance":
		bal, err := conn.GetBalance(ctx)
		if err != nil {
			fmt.Printf("Failed to get balance: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Balance:")
		for k, v := range bal.Total {
			fmt.Printf("  %s: %.4f (free: %.4f, used: %.4f)\n", k, v, bal.Free[k], bal.Used[k])
		}

	case "positions":
		positions, err := conn.GetPositions(ctx)
		if err != nil {
			fmt.Printf("Failed to get positions: %v\n", err)
			os.Exit(1)
		}
		if len(positions) == 0 {
			fmt.Println("No open positions")
		} else {
			fmt.Println("Open positions:")
			for _, p := range positions {
				fmt.Printf("  %s: %.4f @ avg %.4f (side: %s)\n", p.Symbol, p.Size, p.AvgPrice, p.Side)
			}
		}

	case "order":
		if len(args) < 4 {
			fmt.Println("Usage: order <symbol> <side> <amount> [price]")
			os.Exit(1)
		}
		symbol := args[1]
		side := args[2]
		amount, _ := strconv.ParseFloat(args[3], 64)
		var price *float64
		if len(args) >= 5 {
			p, _ := strconv.ParseFloat(args[4], 64)
			price = &p
		}

		orderType := types.OrderTypeMarket
		if price != nil {
			orderType = types.OrderTypeLimit
		}

		req := types.OrderRequest{
			Symbol: symbols.NormalizeCanonical(symbol),
			Type:   orderType,
			Side:   types.OrderSide(side),
			Amount: amount,
			Price:  price,
		}

		logger.Info("submitting order", "symbol", req.Symbol, "type", req.Type, "side", req.Side, "amount", req.Amount)
		resp, err := conn.SubmitOrder(ctx, req)
		if err != nil {
			fmt.Printf("Failed to submit order: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Order submitted: ID=%s Status=%s Filled=%.4f/%.4f @ %.4f\n",
			resp.ID, resp.Status, resp.Filled, resp.Amount, resp.Price)

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func parseBalance(s string) map[string]float64 {
	balances := make(map[string]float64)
	if s == "" {
		balances["USDT"] = 10000
		return balances
	}
	for _, pair := range splitComma(s) {
		kv := splitEqual(pair)
		if len(kv) == 2 {
			if v, err := strconv.ParseFloat(kv[1], 64); err == nil {
				balances[kv[0]] = v
			}
		}
	}
	return balances
}

func splitComma(s string) []string {
	result := make([]string, 0)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}

func splitEqual(s string) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}
