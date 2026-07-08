package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"gorango/exchanges/internal/orderbook/api"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	var (
		date   = flag.String("date", "2026-01-15", "Date in YYYY-MM-DD format")
		hour   = flag.String("hour", "12", "Hour in HH format")
		symbol = flag.String("symbol", "BTCUSDT", "Symbol (e.g., BTCUSDT)")
	)
	flag.Parse()

	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		fmt.Println("Error: CRYPTO_HFT_DATA environment variable not set")
		os.Exit(1)
	}

	client := api.NewCryptoHFTClient(apiKey)

	fmt.Printf("Downloading orderbook parquet file for %s, %s, hour %s...\n", *symbol, *date, *hour)

	result, err := client.DownloadParquet("binance_futures", *symbol, *date, *hour, "orderbook")
	if err != nil {
		fmt.Printf("Error downloading: %v\n", err)
		os.Exit(1)
	}
	defer result.Cleanup()

	fmt.Printf("Downloaded to: %s\n", result.FilePath)

	// Copy to the inspect location
	inspectPath := filepath.Join("/tmp", "inspect_orderbook.parquet")
	input, err := os.ReadFile(result.FilePath)
	if err != nil {
		fmt.Printf("Error reading downloaded file: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(inspectPath, input, 0644); err != nil {
		fmt.Printf("Error writing to inspect path: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Copied to: %s\n", inspectPath)
	fmt.Println("\nRunning inspect script...")

	// Run the inspect script
	cmd := exec.Command("go", "run", "scripts/inspect_parquet/main.go")
	cmd.Dir = "/home/g/m/gorango/exchanges"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error running inspect script: %v\n", err)
		os.Exit(1)
	}
}
