package main

import (
	"flag"
	"fmt"
	"gorango/mdx/internal/orderbook/api"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/parquet-go/parquet-go"
)

func main() {
	exchange := flag.String("exchange", "binance_futures", "Exchange folder name on CryptoHFT")
	symbol := flag.String("symbol", "BTCUSDT", "Exchange formatted symbol (e.g., BTCUSDT)")
	date := flag.String("date", "2024-01-01", "Start date to look for data")
	hour := flag.String("hour", "00", "Hour string")
	flag.Parse()

	// Ignore if .env doesn't exist
	_ = godotenv.Load()

	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	if apiKey == "" {
		log.Fatal("CRYPTO_HFT_DATA environment variable is required")
	}

	client := api.NewCryptoHFTClient(apiKey)

	// The 4 endpoints that we pull data from
	dataTypes := []string{"orderbook", "trades", "open_interest", "liquidations"}

	// We try a few consecutive days just in case a specific hour (like liquidations) is empty
	datesToTry := []string{
		*date,
		"2026-02-02",
		"2026-02-03",
		"2026-02-04",
		"2026-02-05",
	}

	for _, dt := range dataTypes {
		fmt.Printf("\n==================================================\n")
		fmt.Printf("🔍 EXPLORING DATA TYPE: %s\n", strings.ToUpper(dt))
		fmt.Printf("==================================================\n")

		var res *api.DownloadResult
		var err error

		for _, d := range datesToTry {
			fmt.Printf("Fetching %s data for %s %s %s %s...\n", dt, *exchange, *symbol, d, *hour)
			res, err = client.DownloadParquet(*exchange, *symbol, d, *hour, dt)
			if err == nil {
				break
			}
			fmt.Printf("  -> Not found or error: %v\n", err)
		}

		if err != nil {
			fmt.Printf("❌ Could not fetch any sample for %s. Skipping.\n", dt)
			continue
		}

		inspectParquet(res.FilePath)
		_ = res.Cleanup()
	}
}

func inspectParquet(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		log.Printf("Failed to stat file: %v", err)
		return
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		log.Printf("Failed to open parquet file: %v", err)
		return
	}

	schema := pf.Schema()
	columns := schema.Columns()

	fmt.Println("\n📊 SCHEMA (KEYS):")
	for i, col := range columns {
		colName := strings.Join(col, ".")
		fmt.Printf("  %2d. %-25s\n", i+1, colName)
	}

	rowGroups := pf.RowGroups()
	if len(rowGroups) == 0 {
		fmt.Println("\n⚠️ No rows in file.")
		return
	}

	rows := rowGroups[0].Rows()
	defer func() { _ = rows.Close() }()

	// Read up to 2 rows for sampling
	buffer := make([]parquet.Row, 2)
	n, err := rows.ReadRows(buffer)
	if err != nil && err.Error() != "EOF" {
		log.Printf("Failed to read rows: %v", err)
		return
	}

	fmt.Printf("\n📄 SAMPLE DATA (%d rows extracted):\n", n)
	for i := 0; i < n; i++ {
		fmt.Printf("  [ Row %d ]\n", i+1)
		row := buffer[i]
		for j, col := range columns {
			if j < len(row) {
				colName := strings.Join(col, ".")
				val := row[j]
				fmt.Printf("    %-25s : %v\n", colName, val)
			}
		}
		fmt.Println()
	}
}
