package types

import "time"

type Bar struct {
	Time   time.Time `json:"time"`
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	Volume float64   `json:"volume"`
}

type OrderbookBar struct {
	Timestamp          int64    `json:"timestamp"`
	VWAP               float64  `json:"vwap"`
	TradeCount         int      `json:"trade_count"`
	BuyVolume          float64  `json:"buy_volume"`
	SellVolume         float64  `json:"sell_volume"`
	AvgSpread          float64  `json:"avg_spread"`
	SpreadStdDev       float64  `json:"spread_std_dev"`
	DepthImbalance     float64  `json:"depth_imbalance"`
	DepthRatio         float64  `json:"depth_ratio"`
	OpenInterest       *float64 `json:"open_interest,omitempty"`
	OpenInterestChange *float64 `json:"open_interest_change,omitempty"`
	FundingRate        *float64 `json:"funding_rate,omitempty"`
	FundingRateChange  *float64 `json:"funding_rate_change,omitempty"`
	LiqLongVol         *float64 `json:"liq_long_vol,omitempty"`
	LiqShortVol        *float64 `json:"liq_short_vol,omitempty"`
	LiqCovered         int      `json:"liq_covered"`
}
