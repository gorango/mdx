package parquet

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func getFloat64(val parquet.Value) float64 {
	if val.Kind() == parquet.Double {
		return val.Double()
	}
	if val.Kind() == parquet.Float {
		return float64(val.Float())
	}
	// Fallback for BYTE_ARRAY / string-encoded numbers
	v, _ := strconv.ParseFloat(val.String(), 64)
	return v
}

// normalizeSide converts CryptoHFT side values ("b", "a", "buy", "sell") to standard "bid"/"ask".
func normalizeSide(side string) string {
	s := strings.ToLower(strings.TrimSpace(side))
	if s == "bid" || s == "b" || s == "buy" || s == "1" || s == "true" {
		return "bid"
	}
	return "ask"
}

func normalizeEventType(eventType string) string {
	switch eventType {
	case "update":
		return "depthUpdate"
	case "snapshot":
		return "snapshot"
	default:
		return eventType
	}
}

// Reader provides streaming access to parquet files
type Reader struct {
	file *os.File
}

// Open opens a parquet file for reading
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return &Reader{file: f}, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// StreamTrades streams trades through a callback function
func (r *Reader) StreamTrades(callback func(Trade) error) error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	pf, err := parquet.OpenFile(r.file, stat.Size())
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}
	cols := pf.Schema().Columns()
	idxTradeTime, idxPrice, idxQuantity, idxIsBuyerMaker := -1, -1, -1, -1
	for i, c := range cols {
		switch c[0] {
		case "trade_time":
			idxTradeTime = i
		case "price":
			idxPrice = i
		case "quantity":
			idxQuantity = i
		case "is_buyer_maker":
			idxIsBuyerMaker = i
		}
	}
	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 1000)
		for {
			n, readErr := rows.ReadRows(buf)
			if readErr != nil && readErr != io.EOF {
				_ = rows.Close()
				return fmt.Errorf("read rows: %w", readErr)
			}
			for i := 0; i < n; i++ {
				row := buf[i]
				var t Trade
				if idxTradeTime >= 0 && idxTradeTime < len(row) {
					t.TradeTime = row[idxTradeTime].Int64()
				}
				if idxPrice >= 0 && idxPrice < len(row) {
					t.Price = getFloat64(row[idxPrice])
				}
				if idxQuantity >= 0 && idxQuantity < len(row) {
					t.Quantity = getFloat64(row[idxQuantity])
				}
				if idxIsBuyerMaker >= 0 && idxIsBuyerMaker < len(row) {
					t.IsBuyerMaker = row[idxIsBuyerMaker].Boolean()
				}
				if err := callback(t); err != nil {
					_ = rows.Close()
					return err
				}
			}
			if readErr == io.EOF {
				break
			}
		}
		_ = rows.Close()
	}
	return nil
}

// StreamOrderBook streams orderbook updates through a callback function.
// Vendor guarantees rows are already sorted by FinalUpdateID (verified
// across 30M rows: 0 violations), but NOT by Price within the same
// FinalUpdateID (77–100% of groups price-unsorted, avg 11–109 rows/group,
// max ~2k). We stream in file order and sort only within each
// FinalUpdateID group — O(N log k) where k is group size, no disk spill.
func (r *Reader) StreamOrderBook(callback func(OrderBook) error) error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	pf, err := parquet.OpenFile(r.file, stat.Size())
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}
	cols := pf.Schema().Columns()
	// Resolve column indexes once (avoid per-cell string compare).
	idxEventTime, idxEventType, idxPrice, idxQuantity, idxSide := -1, -1, -1, -1, -1
	idxFinal, idxPrevFinal, idxLast := -1, -1, -1
	for i, c := range cols {
		switch c[0] {
		case "event_time":
			idxEventTime = i
		case "event_type":
			idxEventType = i
		case "price":
			idxPrice = i
		case "quantity":
			idxQuantity = i
		case "side":
			idxSide = i
		case "final_update_id":
			idxFinal = i
		case "prev_final_update_id":
			idxPrevFinal = i
		case "last_update_id":
			idxLast = i
		}
	}

	// Group buffer reuses same backing array across groups.
	group := make([]OrderBook, 0, 128)
	var prevFinalID int64
	var haveGroup bool
	var violations int
	var totalRows int

	flushGroup := func() error {
		if len(group) == 0 {
			return nil
		}
		if len(group) > 1 {
			sort.Slice(group, func(i, j int) bool { return group[i].Price < group[j].Price })
		}
		for _, u := range group {
			if err := callback(u); err != nil {
				return err
			}
		}
		group = group[:0]
		return nil
	}

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 1000)
		for {
			n, readErr := rows.ReadRows(buf)
			if readErr != nil && readErr != io.EOF {
				_ = rows.Close()
				return fmt.Errorf("read rows: %w", readErr)
			}
			for i := 0; i < n; i++ {
				row := buf[i]
				var u OrderBook
				if idxEventTime >= 0 && idxEventTime < len(row) {
					u.EventTime = row[idxEventTime].Int64()
				}
				if idxEventType >= 0 && idxEventType < len(row) {
					u.EventType = normalizeEventType(row[idxEventType].String())
				}
				if idxPrice >= 0 && idxPrice < len(row) {
					u.Price = getFloat64(row[idxPrice])
				}
				if idxQuantity >= 0 && idxQuantity < len(row) {
					u.Quantity = getFloat64(row[idxQuantity])
				}
				if idxSide >= 0 && idxSide < len(row) {
					u.Side = normalizeSide(row[idxSide].String())
				}
				if idxFinal >= 0 && idxFinal < len(row) {
					u.FinalUpdateID = row[idxFinal].Int64()
				}
				if idxPrevFinal >= 0 && idxPrevFinal < len(row) {
					u.PrevFinalUpdateID = row[idxPrevFinal].Int64()
				}
				if idxLast >= 0 && idxLast < len(row) {
					u.LastUpdateID = row[idxLast].Int64()
				}
				if u.FinalUpdateID == 0 && u.LastUpdateID != 0 {
					u.FinalUpdateID = u.LastUpdateID
				}
				if u.EventType == "" && u.PrevFinalUpdateID == 0 {
					u.EventType = "snapshot"
				}
				totalRows++
				if haveGroup && u.FinalUpdateID < prevFinalID {
					violations++
				}
				// New FinalUpdateID → flush previous group (file is sorted by FinalUpdateID).
				if haveGroup && u.FinalUpdateID != prevFinalID {
					if err := flushGroup(); err != nil {
						_ = rows.Close()
						return err
					}
				}
				group = append(group, u)
				prevFinalID = u.FinalUpdateID
				haveGroup = true
			}
			if readErr == io.EOF {
				break
			}
		}
		_ = rows.Close()
	}
	if err := flushGroup(); err != nil {
		return err
	}
	if violations > 0 {
		slog.Warn("orderbook parquet violates FinalUpdateID sortedness assumption — book state may be incorrect",
			"violations", violations, "rows", totalRows)
	}
	return nil
}

// StreamOpenInterest streams OI updates through a callback function
func (r *Reader) StreamOpenInterest(callback func(OpenInterest) error) error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	pf, err := parquet.OpenFile(r.file, stat.Size())
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}
	cols := pf.Schema().Columns()
	idxTimestamp, idxSumOpenInterest, idxSumOpenInterestValue := -1, -1, -1
	for i, c := range cols {
		switch c[0] {
		case "timestamp":
			idxTimestamp = i
		case "sum_open_interest":
			idxSumOpenInterest = i
		case "sum_open_interest_value":
			idxSumOpenInterestValue = i
		}
	}
	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 1000)
		for {
			n, readErr := rows.ReadRows(buf)
			if readErr != nil && readErr != io.EOF {
				_ = rows.Close()
				return fmt.Errorf("read rows: %w", readErr)
			}
			for i := 0; i < n; i++ {
				row := buf[i]
				var update OpenInterest
				if idxTimestamp >= 0 && idxTimestamp < len(row) {
					update.Timestamp = row[idxTimestamp].Int64()
				}
				if idxSumOpenInterest >= 0 && idxSumOpenInterest < len(row) {
					update.SumOpenInterest = getFloat64(row[idxSumOpenInterest])
				}
				if idxSumOpenInterestValue >= 0 && idxSumOpenInterestValue < len(row) {
					update.SumOpenInterestValue = getFloat64(row[idxSumOpenInterestValue])
				}
				if err := callback(update); err != nil {
					_ = rows.Close()
					return err
				}
			}
			if readErr == io.EOF {
				break
			}
		}
		_ = rows.Close()
	}
	return nil
}

// StreamLiquidations streams liquidations through a callback function
func (r *Reader) StreamLiquidations(callback func(Liquidation) error) error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	pf, err := parquet.OpenFile(r.file, stat.Size())
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}
	cols := pf.Schema().Columns()
	idxTradeTime, idxLastFilledQuantity, idxSide := -1, -1, -1
	for i, c := range cols {
		switch c[0] {
		case "trade_time":
			idxTradeTime = i
		case "last_filled_quantity":
			idxLastFilledQuantity = i
		case "side":
			idxSide = i
		}
	}
	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 1000)
		for {
			n, readErr := rows.ReadRows(buf)
			if readErr != nil && readErr != io.EOF {
				_ = rows.Close()
				return fmt.Errorf("read rows: %w", readErr)
			}
			for i := 0; i < n; i++ {
				row := buf[i]
				var liq Liquidation
				if idxTradeTime >= 0 && idxTradeTime < len(row) {
					liq.TradeTime = row[idxTradeTime].Int64()
				}
				if idxLastFilledQuantity >= 0 && idxLastFilledQuantity < len(row) {
					liq.LastFilledQuantity = getFloat64(row[idxLastFilledQuantity])
				}
				if idxSide >= 0 && idxSide < len(row) {
					liq.Side = row[idxSide].String()
				}
				if err := callback(liq); err != nil {
					_ = rows.Close()
					return err
				}
			}
			if readErr == io.EOF {
				break
			}
		}
		_ = rows.Close()
	}
	return nil
}
