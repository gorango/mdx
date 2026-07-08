package parquet

// Trade represents a trade from parquet file
type Trade struct {
	ReceivedTime int64
	EventTime    int64
	Symbol       string
	TradeID      int64
	Price        float64
	Quantity     float64
	TradeTime    int64
	IsBuyerMaker bool
}

// OrderBook represents an orderbook update from parquet file
type OrderBook struct {
	ReceivedTime      int64
	EventTime         int64
	TransactionTime   int64
	Symbol            string
	EventType         string
	FirstUpdateID     int64
	FinalUpdateID     int64
	PrevFinalUpdateID int64
	LastUpdateID      int64
	Side              string // "bid" or "ask"
	Price             float64
	Quantity          float64
	OrderCount        int32
}

// OpenInterest represents open interest from parquet file
type OpenInterest struct {
	ReceivedTime         int64
	Symbol               string
	SumOpenInterest      float64
	SumOpenInterestValue float64
	Timestamp            int64
}

// Liquidation represents a liquidation from parquet file
type Liquidation struct {
	ReceivedTime       int64
	EventTime          int64
	Symbol             string
	Side               string // "BUY" or "SELL"
	OrderType          string
	TimeInForce        string
	Quantity           float64
	Price              float64
	AveragePrice       float64
	OrderStatus        string
	LastFilledQuantity float64
	FilledQuantity     float64
	TradeTime          int64
}
