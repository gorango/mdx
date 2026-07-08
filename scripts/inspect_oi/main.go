package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/parquet-go/parquet-go"
	"gorango/exchanges/internal/orderbook/api"
)

func main() {
	_ = godotenv.Load()
	apiKey := os.Getenv("CRYPTO_HFT_DATA")
	client := api.NewCryptoHFTClient(apiKey)

	result, err := client.DownloadParquet("binance_futures", "BTCUSDT", "2026-04-14", "20", "open_interest")
	if err != nil {
		fmt.Printf("Error downloading: %v\n", err)
		os.Exit(1)
	}
	defer result.Cleanup()

	f, err := os.Open(result.FilePath)
	if err != nil {
		fmt.Printf("Error opening: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	st, _ := f.Stat()
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		fmt.Printf("Error opening parquet: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Schema columns: %v\n", pf.Schema().Columns())
	fmt.Printf("Num rows: %d\n", pf.NumRows())

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 5)
		n, err := rows.ReadRows(buf)
		if err != nil {
			fmt.Printf("read error: %v\n", err)
		}
		for i := 0; i < n && i < 5; i++ {
			fmt.Printf("Row %d: ", i)
			for _, val := range buf[i] {
				fmt.Printf("%v ", val)
			}
			fmt.Println()
		}
		rows.Close()
		break
	}
}
