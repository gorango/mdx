package types

type Balance struct {
	Free  map[string]float64
	Used  map[string]float64
	Total map[string]float64
	Info  map[string]interface{}
}

type Position struct {
	Symbol   string
	Size     float64
	AvgPrice float64
	Side     PositionSide
	Leverage *int
}

type PositionSide string

const (
	PositionSideLong  PositionSide = "long"
	PositionSideShort PositionSide = "short"
)
