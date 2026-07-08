package types

type OrderRequest struct {
	Symbol        string
	Type          OrderType
	Side          OrderSide
	Amount        float64
	Price         *float64
	Leverage      *int
	PositionSide  *string
	OpenType      *int
	PositionType  *int
	ReduceOnly    *bool
	ClientOrderID *string
}

type OrderType string

const (
	OrderTypeMarket OrderType = "market"
	OrderTypeLimit  OrderType = "limit"
)

type OrderSide string

const (
	OrderSideBuy  OrderSide = "buy"
	OrderSideSell OrderSide = "sell"
)

type OrderResponse struct {
	ID            string
	ClientOrderID *string
	Timestamp     int64
	Status        OrderStatus
	Symbol        string
	Type          OrderType
	Side          OrderSide
	Amount        float64
	Filled        float64
	Remaining     float64
	Price         float64
	Average       *float64
	Cost          float64
	Fee           *OrderFee
	Info          map[string]interface{}
}

type OrderStatus string

const (
	OrderStatusOpen     OrderStatus = "open"
	OrderStatusClosed   OrderStatus = "closed"
	OrderStatusCanceled OrderStatus = "canceled"
	OrderStatusExpired  OrderStatus = "expired"
	OrderStatusRejected OrderStatus = "rejected"
	OrderStatusPartial  OrderStatus = "partial"
)

type OrderFee struct {
	Currency string
	Cost     float64
	Rate     *float64
}
