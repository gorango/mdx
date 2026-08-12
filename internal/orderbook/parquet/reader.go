package parquet

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func parseFloat(val parquet.Value) float64 {
	v, _ := strconv.ParseFloat(val.String(), 64)
	return v
}

const orderBookSortChunkSize = 250000

type orderBookMergeItem struct {
	update   OrderBook
	readerID int
}

type orderBookMergeHeap []orderBookMergeItem

func (h orderBookMergeHeap) Len() int { return len(h) }

func (h orderBookMergeHeap) Less(i, j int) bool {
	if h[i].update.FinalUpdateID == h[j].update.FinalUpdateID {
		if h[i].update.Price == h[j].update.Price {
			return h[i].readerID < h[j].readerID
		}
		return h[i].update.Price < h[j].update.Price
	}
	return h[i].update.FinalUpdateID < h[j].update.FinalUpdateID
}

func (h orderBookMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *orderBookMergeHeap) Push(x any) {
	*h = append(*h, x.(orderBookMergeItem))
}

func (h *orderBookMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type orderBookChunkReader struct {
	file *os.File
	rd   *bufio.Reader
}

func (r *orderBookChunkReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func sideToByte(side string) uint8 {
	s := strings.ToLower(strings.TrimSpace(side))
	if s == "bid" || s == "b" || s == "buy" || s == "1" || s == "true" {
		return 1
	}
	return 2
}

func byteToSide(b uint8) string {
	if b == 1 {
		return "bid"
	}
	return "ask"
}

// normalizeSide converts CryptoHFT side values ("b", "a", "buy", "sell") to standard "bid"/"ask".
func normalizeSide(side string) string {
	s := strings.ToLower(strings.TrimSpace(side))
	if s == "bid" || s == "b" || s == "buy" || s == "1" || s == "true" {
		return "bid"
	}
	return "ask"
}

func eventTypeToByte(eventType string) uint8 {
	switch eventType {
	case "":
		return 0
	case "depthUpdate", "update":
		return 1
	case "snapshot":
		return 2
	default:
		return 255
	}
}

func byteToEventType(b uint8) string {
	switch b {
	case 0:
		return ""
	case 1:
		return "depthUpdate"
	case 2:
		return "snapshot"
	default:
		return "other"
	}
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

func writeOrderBookChunk(rows []OrderBook) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}

	tmp, err := os.CreateTemp("", "orderbook-sort-*.bin")
	if err != nil {
		return "", fmt.Errorf("create temp chunk: %w", err)
	}

	path := tmp.Name()
	wr := bufio.NewWriterSize(tmp, 1<<20)

	for _, u := range rows {
		if err := binary.Write(wr, binary.LittleEndian, u.EventTime); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write event_time: %w", err)
		}
		if err := binary.Write(wr, binary.LittleEndian, u.Price); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write price: %w", err)
		}
		if err := binary.Write(wr, binary.LittleEndian, u.Quantity); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write quantity: %w", err)
		}
		side := sideToByte(u.Side)
		if err := binary.Write(wr, binary.LittleEndian, side); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write side: %w", err)
		}
		eventType := eventTypeToByte(u.EventType)
		if err := binary.Write(wr, binary.LittleEndian, eventType); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write event_type: %w", err)
		}
		if err := binary.Write(wr, binary.LittleEndian, u.FinalUpdateID); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write final_update_id: %w", err)
		}
		if err := binary.Write(wr, binary.LittleEndian, u.PrevFinalUpdateID); err != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return "", fmt.Errorf("write prev_final_update_id: %w", err)
		}
	}

	if err := wr.Flush(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("flush chunk writer: %w", err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close chunk file: %w", err)
	}

	return path, nil
}

func readNextOrderBookChunkUpdate(rd *bufio.Reader) (OrderBook, bool, error) {
	var eventTime int64
	if err := binary.Read(rd, binary.LittleEndian, &eventTime); err != nil {
		if errors.Is(err, io.EOF) {
			return OrderBook{}, false, nil
		}
		return OrderBook{}, false, err
	}

	var price float64
	if err := binary.Read(rd, binary.LittleEndian, &price); err != nil {
		return OrderBook{}, false, err
	}

	var quantity float64
	if err := binary.Read(rd, binary.LittleEndian, &quantity); err != nil {
		return OrderBook{}, false, err
	}

	var sideByte uint8
	if err := binary.Read(rd, binary.LittleEndian, &sideByte); err != nil {
		return OrderBook{}, false, err
	}

	var eventTypeByte uint8
	if err := binary.Read(rd, binary.LittleEndian, &eventTypeByte); err != nil {
		return OrderBook{}, false, err
	}

	var finalUpdateID int64
	if err := binary.Read(rd, binary.LittleEndian, &finalUpdateID); err != nil {
		return OrderBook{}, false, err
	}

	var prevFinalUpdateID int64
	if err := binary.Read(rd, binary.LittleEndian, &prevFinalUpdateID); err != nil {
		return OrderBook{}, false, err
	}

	return OrderBook{
		EventTime:         eventTime,
		Price:             price,
		Quantity:          quantity,
		Side:              byteToSide(sideByte),
		EventType:         byteToEventType(eventTypeByte),
		FinalUpdateID:     finalUpdateID,
		PrevFinalUpdateID: prevFinalUpdateID,
	}, true, nil
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

// scan iterates over every row in the file, invoking visit for each row.
func (r *Reader) scan(visit func(columns [][]string, row parquet.Row) error) error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	pf, err := parquet.OpenFile(r.file, stat.Size())
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}

	columns := pf.Schema().Columns()

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()

		buffer := make([]parquet.Row, 1000)
		for {
			n, err := rows.ReadRows(buffer)
			if err != nil && err != io.EOF {
				_ = rows.Close()
				return fmt.Errorf("read rows: %w", err)
			}

			for i := 0; i < n; i++ {
				if err := visit(columns, buffer[i]); err != nil {
					_ = rows.Close()
					return err
				}
			}

			if err == io.EOF {
				break
			}
		}
		_ = rows.Close()
	}

	return nil
}

// StreamTrades streams trades through a callback function
func (r *Reader) StreamTrades(callback func(Trade) error) error {
	return r.scan(func(columns [][]string, row parquet.Row) error {
		trade := Trade{}

		for j, col := range columns {
			if j >= len(row) {
				continue
			}
			val := row[j]

			switch col[0] {
			case "trade_time":
				trade.TradeTime = val.Int64()
			case "price":
				trade.Price = parseFloat(val)
			case "quantity":
				trade.Quantity = parseFloat(val)
			case "is_buyer_maker":
				trade.IsBuyerMaker = val.Boolean()
			}
		}

		return callback(trade)
	})
}

// StreamOrderBook streams orderbook updates through a callback function,
// sorted by FinalUpdateID (then Price) to ensure correct orderbook state reconstruction.
func (r *Reader) StreamOrderBook(callback func(OrderBook) error) error {
	// Read and sort in bounded-size chunks to avoid OOM on large files.
	var (
		updates     = make([]OrderBook, 0, orderBookSortChunkSize)
		chunkPaths  []string
		cleanupDone bool
	)
	defer func() {
		if cleanupDone {
			return
		}
		for _, p := range chunkPaths {
			_ = os.Remove(p)
		}
	}()

	spillChunk := func() error {
		if len(updates) == 0 {
			return nil
		}
		sort.Slice(updates, func(i, j int) bool {
			if updates[i].FinalUpdateID == updates[j].FinalUpdateID {
				return updates[i].Price < updates[j].Price
			}
			return updates[i].FinalUpdateID < updates[j].FinalUpdateID
		})
		path, err := writeOrderBookChunk(updates)
		if err != nil {
			return err
		}
		chunkPaths = append(chunkPaths, path)
		updates = updates[:0]
		return nil
	}

	if err := r.scan(func(columns [][]string, row parquet.Row) error {
		update := OrderBook{}

		for j, col := range columns {
			if j >= len(row) {
				continue
			}
			val := row[j]

			switch col[0] {
			case "event_time":
				update.EventTime = val.Int64()
			case "event_type":
				update.EventType = normalizeEventType(val.String())
			case "price":
				update.Price = parseFloat(val)
			case "quantity":
				update.Quantity = parseFloat(val)
			case "side":
				update.Side = normalizeSide(val.String())
			case "final_update_id":
				update.FinalUpdateID = val.Int64()
			case "prev_final_update_id":
				update.PrevFinalUpdateID = val.Int64()
			case "last_update_id":
				update.LastUpdateID = val.Int64()
			}
		}

		if update.FinalUpdateID == 0 && update.LastUpdateID != 0 {
			update.FinalUpdateID = update.LastUpdateID
		}
		if update.EventType == "" && update.PrevFinalUpdateID == 0 {
			update.EventType = "snapshot"
		}

		updates = append(updates, update)
		if len(updates) >= orderBookSortChunkSize {
			if err := spillChunk(); err != nil {
				return fmt.Errorf("spill sorted chunk: %w", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if len(chunkPaths) == 0 {
		sort.Slice(updates, func(i, j int) bool {
			if updates[i].FinalUpdateID == updates[j].FinalUpdateID {
				return updates[i].Price < updates[j].Price
			}
			return updates[i].FinalUpdateID < updates[j].FinalUpdateID
		})
		for _, update := range updates {
			if err := callback(update); err != nil {
				return err
			}
		}
		cleanupDone = true
		return nil
	}

	if err := spillChunk(); err != nil {
		return fmt.Errorf("spill final sorted chunk: %w", err)
	}

	readers := make([]*orderBookChunkReader, 0, len(chunkPaths))
	for _, p := range chunkPaths {
		f, err := os.Open(p)
		if err != nil {
			for _, rd := range readers {
				_ = rd.Close()
			}
			return fmt.Errorf("open sorted chunk: %w", err)
		}
		readers = append(readers, &orderBookChunkReader{file: f, rd: bufio.NewReaderSize(f, 1<<20)})
	}
	defer func() {
		for _, rd := range readers {
			_ = rd.Close()
		}
	}()

	h := make(orderBookMergeHeap, 0, len(readers))
	heap.Init(&h)
	for i, rd := range readers {
		u, ok, err := readNextOrderBookChunkUpdate(rd.rd)
		if err != nil {
			return fmt.Errorf("read sorted chunk head: %w", err)
		}
		if ok {
			heap.Push(&h, orderBookMergeItem{update: u, readerID: i})
		}
	}

	for h.Len() > 0 {
		item := heap.Pop(&h).(orderBookMergeItem)
		if err := callback(item.update); err != nil {
			return err
		}

		next, ok, err := readNextOrderBookChunkUpdate(readers[item.readerID].rd)
		if err != nil {
			return fmt.Errorf("read sorted chunk next: %w", err)
		}
		if ok {
			heap.Push(&h, orderBookMergeItem{update: next, readerID: item.readerID})
		}
	}

	for _, p := range chunkPaths {
		_ = os.Remove(p)
	}
	cleanupDone = true

	return nil
}

// StreamOpenInterest streams OI updates through a callback function
func (r *Reader) StreamOpenInterest(callback func(OpenInterest) error) error {
	return r.scan(func(columns [][]string, row parquet.Row) error {
		update := OpenInterest{}

		for j, col := range columns {
			if j >= len(row) {
				continue
			}
			val := row[j]

			switch col[0] {
			case "timestamp":
				update.Timestamp = val.Int64()
			case "sum_open_interest":
				update.SumOpenInterest = parseFloat(val)
			case "sum_open_interest_value":
				update.SumOpenInterestValue = parseFloat(val)
			}
		}

		return callback(update)
	})
}

// StreamLiquidations streams liquidations through a callback function
func (r *Reader) StreamLiquidations(callback func(Liquidation) error) error {
	return r.scan(func(columns [][]string, row parquet.Row) error {
		liq := Liquidation{}

		for j, col := range columns {
			if j >= len(row) {
				continue
			}
			val := row[j]

			switch col[0] {
			case "trade_time":
				liq.TradeTime = val.Int64()
			case "last_filled_quantity":
				liq.LastFilledQuantity = parseFloat(val)
			case "side":
				liq.Side = val.String()
			}
		}

		return callback(liq)
	})
}
