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

	// Footprint scalars (migration 0006): reductions of the per-minute trade
	// histogram over exact prices, produced by internal/orderbook/levelhist.
	// CONSISTENCY GROUP — computed from ONE trade population, written together
	// by both aggregators and merged as a set in db.InsertOrderbookBars.
	// NULL when TradeCount == 0; Buy/Sell VWAP+POC additionally NULL when that
	// side did not trade. Band volumes are true zeros within a populated bar.
	TradeOpen      *float64 `json:"trade_open,omitempty"`
	TradeHigh      *float64 `json:"trade_high,omitempty"`
	TradeLow       *float64 `json:"trade_low,omitempty"`
	TradeClose     *float64 `json:"trade_close,omitempty"`
	BuyVWAP        *float64 `json:"buy_vwap,omitempty"`
	SellVWAP       *float64 `json:"sell_vwap,omitempty"`
	POCPrice       *float64 `json:"poc_price,omitempty"`
	POCVolumeShare *float64 `json:"poc_volume_share,omitempty"`
	BuyPOCPrice    *float64 `json:"buy_poc_price,omitempty"`
	SellPOCPrice   *float64 `json:"sell_poc_price,omitempty"`
	TradePriceStd  *float64 `json:"trade_price_std,omitempty"`
	HiBandBuyVol   *float64 `json:"hi_band_buy_vol,omitempty"`
	HiBandSellVol  *float64 `json:"hi_band_sell_vol,omitempty"`
	LoBandBuyVol   *float64 `json:"lo_band_buy_vol,omitempty"`
	LoBandSellVol  *float64 `json:"lo_band_sell_vol,omitempty"`
}
