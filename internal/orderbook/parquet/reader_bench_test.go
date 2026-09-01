package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// benchOrderBookRow mirrors the spec schema for benchmarking.
type benchOBRow struct {
	EventTime         int64   `parquet:"event_time"`
	EventType         string  `parquet:"event_type"`
	Price             float64 `parquet:"price"`
	Quantity          float64 `parquet:"quantity"`
	Side              string  `parquet:"side"`
	FinalUpdateID     int64   `parquet:"final_update_id"`
	PrevFinalUpdateID int64   `parquet:"prev_final_update_id"`
	LastUpdateID      int64   `parquet:"last_update_id"`
}

type benchTradeRow struct {
	TradeTime    int64   `parquet:"trade_time"`
	Price        float64 `parquet:"price"`
	Quantity     float64 `parquet:"quantity"`
	IsBuyerMaker bool    `parquet:"is_buyer_maker"`
}

// makeBenchFile creates a temp parquet file with n rows distributed across
// groupsPerFile distinct FinalUpdateIDs. Returns the file path and cleanup.
func makeBenchFile(b *testing.B, n, groups int) string {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.parquet")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create bench file: %v", err)
	}
	w := parquet.NewGenericWriter[benchOBRow](f)
	rows := make([]benchOBRow, n)
	rowsPerGroup := n / groups
	if rowsPerGroup == 0 {
		rowsPerGroup = 1
	}
	for i := range rows {
		gid := int64(i/rowsPerGroup + 1)
		// Within each group, prices are deliberately unsorted to exercise sort.
		rows[i] = benchOBRow{
			EventTime:         int64(i),
			EventType:         "update",
			Price:             float64(n - i%rowsPerGroup), // descending within group
			Quantity:          1.0,
			Side:              "bid",
			FinalUpdateID:     gid,
			PrevFinalUpdateID: gid - 1,
		}
	}
	if _, err := w.Write(rows); err != nil {
		b.Fatalf("write bench rows: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("close bench writer: %v", err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close bench file: %v", err)
	}
	return path
}

func BenchmarkStreamOrderBook(b *testing.B) {
	for _, tc := range []struct {
		name   string
		rows   int
		groups int
	}{
		{"1k/10groups", 1000, 10},
		{"10k/100groups", 10000, 100},
		{"100k/1k-groups", 100000, 1000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			path := makeBenchFile(b, tc.rows, tc.groups)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := Open(path)
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				var count int
				if err := r.StreamOrderBook(func(ob OrderBook) error {
					count++
					return nil
				}); err != nil {
					b.Fatalf("stream: %v", err)
				}
				_ = r.Close()
				if count != tc.rows {
					b.Fatalf("expected %d rows, got %d", tc.rows, count)
				}
			}
		})
	}
}

func BenchmarkStreamTrades(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench_trades.parquet")
			f, err := os.Create(path)
			if err != nil {
				b.Fatalf("create: %v", err)
			}
			w := parquet.NewGenericWriter[benchTradeRow](f)
			rows := make([]benchTradeRow, n)
			for i := range rows {
				rows[i] = benchTradeRow{TradeTime: int64(i), Price: float64(i), Quantity: 1.0, IsBuyerMaker: i%2 == 0}
			}
			if _, err := w.Write(rows); err != nil {
				b.Fatalf("write: %v", err)
			}
			_ = w.Close()
			_ = f.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := Open(path)
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				var count int
				if err := r.StreamTrades(func(t Trade) error {
					count++
					return nil
				}); err != nil {
					b.Fatalf("stream: %v", err)
				}
				_ = r.Close()
				if count != n {
					b.Fatalf("expected %d, got %d", n, count)
				}
			}
		})
	}
}
