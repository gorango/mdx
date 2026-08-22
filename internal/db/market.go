package db

import (
	"context"
	"fmt"
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"gorango/mdx/internal/orderbook/api"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func (db *DB) InsertPriceBars(ctx context.Context, exchange, symbol string, bars []types.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	if db.pool == nil {
		return nil
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tmpTable := fmt.Sprintf("tmp_price_bars_%d", time.Now().UnixNano())

	_, err = tx.Exec(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s (LIKE price_bars INCLUDING DEFAULTS)`, tmpTable))
	if err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{tmpTable},
		[]string{"exchange", "symbol", "timestamp", "open", "high", "low", "close", "volume"},
		pgx.CopyFromSlice(len(bars), func(i int) ([]interface{}, error) {
			b := bars[i]
			return []interface{}{
				exchange,
				symbol,
				b.Time,
				b.Open,
				b.High,
				b.Low,
				b.Close,
				b.Volume,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy to temp table: %w", err)
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO price_bars (exchange, symbol, timestamp, open, high, low, close, volume)
		SELECT exchange, symbol, timestamp, open, high, low, close, volume FROM %s
		ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
	`, tmpTable))
	if err != nil {
		return fmt.Errorf("insert from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

func (db *DB) QueryPriceBars(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.Bar, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT timestamp, open, high, low, close, volume
		FROM price_bars
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4
		ORDER BY timestamp ASC
	`, exchange, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("query price bars: %w", err)
	}
	defer rows.Close()

	var bars []types.Bar
	for rows.Next() {
		var b types.Bar
		if err := rows.Scan(&b.Time, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		bars = append(bars, b)
	}
	return bars, nil
}

func (db *DB) PriceBarExists(ctx context.Context, exchange, symbol string, ts time.Time) (bool, error) {
	var count int64
	err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM price_bars
		WHERE exchange = $1 AND symbol = $2 AND timestamp = $3
	`, exchange, symbol, ts).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query bar exists: %w", err)
	}
	return count > 0, nil
}

func (db *DB) GetLastPriceBar(ctx context.Context, exchange, symbol string) (*types.Bar, error) {
	var b types.Bar
	err := db.pool.QueryRow(ctx, `
		SELECT timestamp, open, high, low, close, volume
		FROM price_bars
		WHERE exchange = $1 AND symbol = $2
		ORDER BY timestamp DESC
		LIMIT 1
	`, exchange, symbol).Scan(&b.Time, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query last bar: %w", err)
	}
	return &b, nil
}

func (db *DB) InsertOrderbookBars(ctx context.Context, exchange, symbol string, bars []types.OrderbookBar) error {
	if len(bars) == 0 {
		return nil
	}

	bars = deduplicateOrderbookBars(bars)
	if len(bars) == 0 {
		return nil
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tmpTable := fmt.Sprintf("tmp_orderbook_bars_%d", time.Now().UnixNano())

	_, err = tx.Exec(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s (LIKE orderbook_bars INCLUDING DEFAULTS)`, tmpTable))
	if err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{tmpTable},
		[]string{
			"exchange", "symbol", "timestamp",
			"vwap", "trade_count", "buy_volume", "sell_volume",
			"avg_spread", "spread_std_dev", "depth_imbalance",
			"depth_ratio", "open_interest", "open_interest_change",
			"funding_rate", "funding_rate_change", "liq_long_vol",
			"liq_short_vol", "liq_covered",
			// Footprint scalars (migration 0006). Consistency group: written
			// as one set, merged as one set in the upsert below.
			"trade_open", "trade_high", "trade_low", "trade_close",
			"buy_vwap", "sell_vwap", "poc_price", "poc_volume_share",
			"buy_poc_price", "sell_poc_price", "trade_price_std",
			"hi_band_buy_vol", "hi_band_sell_vol",
			"lo_band_buy_vol", "lo_band_sell_vol",
		},
		pgx.CopyFromSlice(len(bars), func(i int) ([]interface{}, error) {
			b := bars[i]
			return []interface{}{
				exchange,
				symbol,
				time.UnixMilli(b.Timestamp),
				b.VWAP,
				b.TradeCount,
				b.BuyVolume,
				b.SellVolume,
				b.AvgSpread,
				b.SpreadStdDev,
				b.DepthImbalance,
				b.DepthRatio,
				b.OpenInterest,
				b.OpenInterestChange,
				b.FundingRate,
				b.FundingRateChange,
				b.LiqLongVol,
				b.LiqShortVol,
				b.LiqCovered,
				b.TradeOpen,
				b.TradeHigh,
				b.TradeLow,
				b.TradeClose,
				b.BuyVWAP,
				b.SellVWAP,
				b.POCPrice,
				b.POCVolumeShare,
				b.BuyPOCPrice,
				b.SellPOCPrice,
				b.TradePriceStd,
				b.HiBandBuyVol,
				b.HiBandSellVol,
				b.LoBandBuyVol,
				b.LoBandSellVol,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy to temp table: %w", err)
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO orderbook_bars (exchange, symbol, timestamp, vwap, trade_count, buy_volume, sell_volume, avg_spread, spread_std_dev, depth_imbalance, depth_ratio, open_interest, open_interest_change, funding_rate, funding_rate_change, liq_long_vol, liq_short_vol, liq_covered, trade_open, trade_high, trade_low, trade_close, buy_vwap, sell_vwap, poc_price, poc_volume_share, buy_poc_price, sell_poc_price, trade_price_std, hi_band_buy_vol, hi_band_sell_vol, lo_band_buy_vol, lo_band_sell_vol)
		SELECT exchange, symbol, timestamp, vwap, trade_count, buy_volume, sell_volume, avg_spread, spread_std_dev, depth_imbalance, depth_ratio, open_interest, open_interest_change, funding_rate, funding_rate_change, liq_long_vol, liq_short_vol, liq_covered, trade_open, trade_high, trade_low, trade_close, buy_vwap, sell_vwap, poc_price, poc_volume_share, buy_poc_price, sell_poc_price, trade_price_std, hi_band_buy_vol, hi_band_sell_vol, lo_band_buy_vol, lo_band_sell_vol FROM %s
		ON CONFLICT (exchange, symbol, timestamp) DO UPDATE SET
			vwap = CASE WHEN EXCLUDED.trade_count >= orderbook_bars.trade_count AND EXCLUDED.trade_count > 0 THEN EXCLUDED.vwap ELSE orderbook_bars.vwap END,
			trade_count = GREATEST(orderbook_bars.trade_count, EXCLUDED.trade_count),
			buy_volume = CASE WHEN EXCLUDED.trade_count >= orderbook_bars.trade_count AND EXCLUDED.trade_count > 0 THEN EXCLUDED.buy_volume ELSE orderbook_bars.buy_volume END,
			sell_volume = CASE WHEN EXCLUDED.trade_count >= orderbook_bars.trade_count AND EXCLUDED.trade_count > 0 THEN EXCLUDED.sell_volume ELSE orderbook_bars.sell_volume END,
			avg_spread = CASE WHEN EXCLUDED.avg_spread > 0 THEN EXCLUDED.avg_spread ELSE orderbook_bars.avg_spread END,
			spread_std_dev = CASE WHEN EXCLUDED.spread_std_dev > 0 THEN EXCLUDED.spread_std_dev ELSE orderbook_bars.spread_std_dev END,
			depth_imbalance = CASE WHEN EXCLUDED.depth_ratio > 0 THEN EXCLUDED.depth_imbalance ELSE orderbook_bars.depth_imbalance END,
			depth_ratio = CASE WHEN EXCLUDED.depth_ratio > 0 THEN EXCLUDED.depth_ratio ELSE orderbook_bars.depth_ratio END,
			open_interest = COALESCE(EXCLUDED.open_interest, orderbook_bars.open_interest),
			open_interest_change = COALESCE(EXCLUDED.open_interest_change, orderbook_bars.open_interest_change),
			funding_rate = COALESCE(EXCLUDED.funding_rate, orderbook_bars.funding_rate),
			funding_rate_change = COALESCE(EXCLUDED.funding_rate_change, orderbook_bars.funding_rate_change),
			liq_long_vol = COALESCE(EXCLUDED.liq_long_vol, orderbook_bars.liq_long_vol),
			liq_short_vol = COALESCE(EXCLUDED.liq_short_vol, orderbook_bars.liq_short_vol),
			liq_covered = GREATEST(orderbook_bars.liq_covered, EXCLUDED.liq_covered),
			-- Footprint scalars (migration 0006): CONSISTENCY GROUP. These 15
			-- columns come from ONE trade population, so they merge as a set —
			-- the same winner rule as vwap/buy_volume/sell_volume (higher
			-- trade_count wins wholesale). Never COALESCE across writers: a
			-- stream-partial POC must never be stitched onto a batch-complete
			-- VWAP.
			trade_open = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.trade_open ELSE orderbook_bars.trade_open END,
			trade_high = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.trade_high ELSE orderbook_bars.trade_high END,
			trade_low = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.trade_low ELSE orderbook_bars.trade_low END,
			trade_close = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.trade_close ELSE orderbook_bars.trade_close END,
			buy_vwap = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.buy_vwap ELSE orderbook_bars.buy_vwap END,
			sell_vwap = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.sell_vwap ELSE orderbook_bars.sell_vwap END,
			poc_price = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.poc_price ELSE orderbook_bars.poc_price END,
			poc_volume_share = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.poc_volume_share ELSE orderbook_bars.poc_volume_share END,
			buy_poc_price = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.buy_poc_price ELSE orderbook_bars.buy_poc_price END,
			sell_poc_price = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.sell_poc_price ELSE orderbook_bars.sell_poc_price END,
			trade_price_std = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.trade_price_std ELSE orderbook_bars.trade_price_std END,
			hi_band_buy_vol = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.hi_band_buy_vol ELSE orderbook_bars.hi_band_buy_vol END,
			hi_band_sell_vol = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.hi_band_sell_vol ELSE orderbook_bars.hi_band_sell_vol END,
			lo_band_buy_vol = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.lo_band_buy_vol ELSE orderbook_bars.lo_band_buy_vol END,
			lo_band_sell_vol = CASE WHEN EXCLUDED.trade_open IS NOT NULL AND EXCLUDED.trade_count >= orderbook_bars.trade_count THEN EXCLUDED.lo_band_sell_vol ELSE orderbook_bars.lo_band_sell_vol END
	`, tmpTable))
	if err != nil {
		return fmt.Errorf("insert from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

func (db *DB) QueryOrderbookBars(ctx context.Context, exchange, symbol string, start, end time.Time) ([]types.OrderbookBar, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT timestamp, vwap, trade_count, buy_volume, sell_volume,
			avg_spread, spread_std_dev, depth_imbalance, depth_ratio,
			open_interest, open_interest_change, funding_rate, funding_rate_change,
			liq_long_vol, liq_short_vol, liq_covered,
			trade_open, trade_high, trade_low, trade_close,
			buy_vwap, sell_vwap, poc_price, poc_volume_share,
			buy_poc_price, sell_poc_price, trade_price_std,
			hi_band_buy_vol, hi_band_sell_vol, lo_band_buy_vol, lo_band_sell_vol
		FROM orderbook_bars
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4
		ORDER BY timestamp ASC
	`, exchange, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("query orderbook bars: %w", err)
	}
	defer rows.Close()

	var bars []types.OrderbookBar
	for rows.Next() {
		var b types.OrderbookBar
		var ts time.Time
		if err := rows.Scan(
			&ts, &b.VWAP, &b.TradeCount, &b.BuyVolume, &b.SellVolume,
			&b.AvgSpread, &b.SpreadStdDev, &b.DepthImbalance, &b.DepthRatio,
			&b.OpenInterest, &b.OpenInterestChange, &b.FundingRate, &b.FundingRateChange,
			&b.LiqLongVol, &b.LiqShortVol, &b.LiqCovered,
			&b.TradeOpen, &b.TradeHigh, &b.TradeLow, &b.TradeClose,
			&b.BuyVWAP, &b.SellVWAP, &b.POCPrice, &b.POCVolumeShare,
			&b.BuyPOCPrice, &b.SellPOCPrice, &b.TradePriceStd,
			&b.HiBandBuyVol, &b.HiBandSellVol, &b.LoBandBuyVol, &b.LoBandSellVol,
		); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		b.Timestamp = ts.UnixMilli()
		bars = append(bars, b)
	}
	return bars, nil
}

func (db *DB) DeletePriceBars(ctx context.Context, exchange, symbol string, startTime, endTime *time.Time) (int64, error) {
	var query string
	var args []interface{}
	if startTime == nil && endTime == nil {
		query = `DELETE FROM price_bars WHERE exchange = $1 AND symbol = $2`
		args = []interface{}{exchange, symbol}
	} else if startTime == nil {
		query = `DELETE FROM price_bars WHERE exchange = $1 AND symbol = $2 AND timestamp < $3`
		args = []interface{}{exchange, symbol, *endTime}
	} else if endTime == nil {
		query = `DELETE FROM price_bars WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3`
		args = []interface{}{exchange, symbol, *startTime}
	} else {
		query = `DELETE FROM price_bars WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4`
		args = []interface{}{exchange, symbol, *startTime, *endTime}
	}
	result, err := db.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete price bars: %w", err)
	}
	return result.RowsAffected(), nil
}

func (db *DB) DeleteOrderbookBars(ctx context.Context, exchange, symbol string, startTime, endTime *time.Time) (int64, error) {
	var query string
	var args []interface{}
	if startTime == nil && endTime == nil {
		query = `DELETE FROM orderbook_bars WHERE exchange = $1 AND symbol = $2`
		args = []interface{}{exchange, symbol}
	} else if startTime == nil {
		query = `DELETE FROM orderbook_bars WHERE exchange = $1 AND symbol = $2 AND timestamp < $3`
		args = []interface{}{exchange, symbol, *endTime}
	} else if endTime == nil {
		query = `DELETE FROM orderbook_bars WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3`
		args = []interface{}{exchange, symbol, *startTime}
	} else {
		query = `DELETE FROM orderbook_bars WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4`
		args = []interface{}{exchange, symbol, *startTime, *endTime}
	}
	result, err := db.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete orderbook bars: %w", err)
	}
	return result.RowsAffected(), nil
}

func (db *DB) HourExists(ctx context.Context, exchange, symbol string, hour time.Time) (bool, error) {
	startOfHour := hour.Truncate(time.Hour)
	endOfHour := startOfHour.Add(time.Hour)
	var count int64
	err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM orderbook_bars
		WHERE exchange = $1 AND symbol = $2
		AND timestamp >= $3 AND timestamp < $4
	`, exchange, symbol, startOfHour, endOfHour).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query hour exists: %w", err)
	}
	return count > 0, nil
}

// SetLiqUnknownRange stamps a symbol's bars in [start, end) with the
// "no liquidation data" encoding: liq_long_vol / liq_short_vol = NULL and
// liq_covered = 0.  Used when the cryptohftdata liquidations parquet is
// unavailable (404) for that period — the volumes are UNKNOWN, not zero.
func (db *DB) SetLiqUnknownRange(ctx context.Context, exchange, symbol string, start, end time.Time) (int64, error) {
	result, err := db.pool.Exec(ctx, `
		UPDATE orderbook_bars
		SET liq_long_vol = NULL, liq_short_vol = NULL, liq_covered = 0
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4
	`, exchange, symbol, start, end)
	if err != nil {
		return 0, fmt.Errorf("set liq unknown range: %w", err)
	}
	return result.RowsAffected(), nil
}

// SetLiqBars writes the aggregated liquidation volumes + liq_covered = 1 for a
// set of 1m bars (from a successfully-downloaded liquidations parquet).  Only
// the liquidation columns are touched; every other column is preserved.
func (db *DB) SetLiqBars(ctx context.Context, exchange, symbol string, ts []int64, liqLong, liqShort []float64) (int64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	result, err := db.pool.Exec(ctx, `
		UPDATE orderbook_bars ob
		SET liq_long_vol = u.liq_long, liq_short_vol = u.liq_short, liq_covered = 1
		FROM unnest($1::bigint[], $2::float8[], $3::float8[]) AS u(ts, liq_long, liq_short)
		WHERE ob.exchange = $4 AND ob.symbol = $5
		  AND ob.timestamp = to_timestamp(u.ts::double precision / 1000.0)
	`, ts, liqLong, liqShort, exchange, symbol)
	if err != nil {
		return 0, fmt.Errorf("set liq bars: %w", err)
	}
	return result.RowsAffected(), nil
}

// ── ob-backfill-liq resume progress ───────────────────────────────────────────

// InitLiqProgress creates the resume-progress table used by
// cmd/ob-backfill-liq: one row per fully-processed (symbol, hour).  A restart
// skips rows already present, so a multi-hour backfill can resume instead of
// re-doing everything.
func (db *DB) InitLiqProgress(ctx context.Context) error {
	_, err := db.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS liq_backfill_progress (
			symbol  TEXT   NOT NULL,
			hour_ts BIGINT NOT NULL,
			PRIMARY KEY (symbol, hour_ts)
		)`)
	if err != nil {
		return fmt.Errorf("create liq backfill progress table: %w", err)
	}
	return nil
}

// LiqProgressDone returns the set of hour_ts already processed for a symbol.
func (db *DB) LiqProgressDone(ctx context.Context, symbol string) (map[int64]bool, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT hour_ts FROM liq_backfill_progress WHERE symbol = $1`, symbol)
	if err != nil {
		return nil, fmt.Errorf("query liq progress: %w", err)
	}
	defer rows.Close()
	done := make(map[int64]bool)
	for rows.Next() {
		var ts int64
		if err := rows.Scan(&ts); err != nil {
			return nil, fmt.Errorf("scan liq progress: %w", err)
		}
		done[ts] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("liq progress rows: %w", err)
	}
	return done, nil
}

// MarkLiqProgress records processed hours so a restarted run can skip them.
func (db *DB) MarkLiqProgress(ctx context.Context, symbol string, hourTs []int64) error {
	if len(hourTs) == 0 {
		return nil
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO liq_backfill_progress (symbol, hour_ts)
		SELECT $1, u.ts FROM unnest($2::bigint[]) AS u(ts)
		ON CONFLICT (symbol, hour_ts) DO NOTHING
	`, symbol, hourTs)
	if err != nil {
		return fmt.Errorf("mark liq progress: %w", err)
	}
	return nil
}

// ResetLiqProgress clears all resume markers (forces a full re-run).
func (db *DB) ResetLiqProgress(ctx context.Context) error {
	_, err := db.pool.Exec(ctx, `TRUNCATE liq_backfill_progress`)
	if err != nil {
		return fmt.Errorf("reset liq progress: %w", err)
	}
	return nil
}

func (db *DB) QueryOrderbookBarsStream(ctx context.Context, exchange, symbol string, start, end time.Time) (interface {
	Next() bool
	Scan(...any) error
	Close()
	Err() error
}, error,
) {
	rows, err := db.pool.Query(ctx, `
		SELECT timestamp, vwap, trade_count, buy_volume, sell_volume,
			avg_spread, spread_std_dev, depth_imbalance, depth_ratio,
			open_interest, open_interest_change, funding_rate, funding_rate_change,
			liq_long_vol, liq_short_vol, liq_covered,
			trade_open, trade_high, trade_low, trade_close,
			buy_vwap, sell_vwap, poc_price, poc_volume_share,
			buy_poc_price, sell_poc_price, trade_price_std,
			hi_band_buy_vol, hi_band_sell_vol, lo_band_buy_vol, lo_band_sell_vol
		FROM orderbook_bars
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4
		ORDER BY timestamp ASC
	`, exchange, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("query orderbook bars stream: %w", err)
	}
	return rows, nil
}

func (db *DB) QueryPriceBarsGrouped(ctx context.Context, exchange, symbol string, start, end time.Time, tf timeframe.Timeframe) ([]types.Bar, error) {
	intervalSeconds := tf.Ms / 1000

	query := fmt.Sprintf(`
		SELECT
			to_timestamp(floor(EXTRACT(EPOCH FROM timestamp - interval '1 minute') / %[1]d) * %[1]d + %[1]d) AT TIME ZONE 'UTC' AS ts,
			(ARRAY_AGG(open ORDER BY timestamp ASC))[1] AS open,
			MAX(high) AS high,
			MIN(low) AS low,
			(ARRAY_AGG(close ORDER BY timestamp DESC))[1] AS close,
			SUM(volume) AS volume
		FROM price_bars
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 AND timestamp < $4
		GROUP BY floor(EXTRACT(EPOCH FROM timestamp - interval '1 minute') / %[1]d)
		ORDER BY ts ASC
	`, intervalSeconds)

	rows, err := db.pool.Query(ctx, query, exchange, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("query price bars grouped: %w", err)
	}
	defer rows.Close()

	var bars []types.Bar
	for rows.Next() {
		var b types.Bar
		if err := rows.Scan(&b.Time, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		bars = append(bars, b)
	}
	return bars, nil
}

func (db *DB) GetDistinctSymbols(ctx context.Context, exchange string) ([]string, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT DISTINCT symbol FROM orderbook_bars WHERE exchange = $1 ORDER BY symbol
	`, exchange)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var sym string
		if err := rows.Scan(&sym); err != nil {
			return nil, err
		}
		symbols = append(symbols, sym)
	}
	return symbols, rows.Err()
}

func (db *DB) GetOrderbookBarRange(ctx context.Context, exchange, symbol string) (minTime, maxTime *time.Time, err error) {
	var min, max time.Time
	err = db.pool.QueryRow(ctx, `
		SELECT MIN(timestamp), MAX(timestamp) FROM orderbook_bars
		WHERE exchange = $1 AND symbol = $2
	`, exchange, symbol).Scan(&min, &max)
	if err != nil {
		return nil, nil, fmt.Errorf("query orderbook bar range: %w", err)
	}
	if min.IsZero() {
		return nil, nil, nil
	}
	return &min, &max, nil
}

func (db *DB) BackfillFundingHistory(ctx context.Context, symbol string, fundingPoints []api.FundingPoint, startTime, endTime *time.Time, batchSize int) (updated int, errors int) {
	if len(fundingPoints) == 0 {
		return 0, 0
	}

	times := make([]int64, len(fundingPoints))
	rates := make([]float64, len(fundingPoints))
	for i, fp := range fundingPoints {
		times[i] = fp.Time
		rates[i] = fp.Rate
	}

	_, err := db.pool.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS _funding_pts (
			funding_time BIGINT NOT NULL,
			rate DOUBLE PRECISION NOT NULL
		);
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Create temp table: %v\n", err)
		return 0, 1
	}

	_, err = db.pool.Exec(ctx, `TRUNCATE _funding_pts`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Truncate temp table: %v\n", err)
		return 0, 1
	}

	_, err = db.pool.Exec(ctx, `
		INSERT INTO _funding_pts (funding_time, rate)
		SELECT (t.funding_time)::bigint, (t.rate)::double precision
		FROM unnest($1::bigint[], $2::float8[]) AS t(funding_time, rate)
	`, times, rates)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load funding points: %v\n", err)
		return 0, 1
	}

	_, err = db.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS _funding_pts_idx ON _funding_pts (funding_time DESC)`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Create index: %v\n", err)
		return 0, 1
	}

	for {
		result, err := db.pool.Exec(ctx, `
			UPDATE orderbook_bars ob
			SET
				funding_rate = sub.rate,
				funding_rate_change = sub.change
			FROM (
				WITH bar_funding AS (
					SELECT
						ob_inner.ctid,
						ob_inner.timestamp,
						ob_inner.symbol,
						ob_inner.exchange,
						(SELECT rate FROM _funding_pts
						 WHERE funding_time <= EXTRACT(EPOCH FROM ob_inner.timestamp)::bigint * 1000
						 ORDER BY funding_time DESC LIMIT 1) AS rate
					FROM orderbook_bars ob_inner
					WHERE ob_inner.funding_rate IS NULL
						AND ob_inner.exchange = 'binance_futures'
						AND ob_inner.symbol = $1
						AND ($2::timestamptz IS NULL OR ob_inner.timestamp >= $2)
						AND ($3::timestamptz IS NULL OR ob_inner.timestamp < $3)
					LIMIT $4
				)
				SELECT
					ctid,
					rate,
					CASE
						WHEN rate IS NULL THEN NULL
						WHEN LAG(rate) OVER prev_win IS NULL THEN NULL
						WHEN rate = LAG(rate) OVER prev_win THEN 0
						ELSE rate - LAG(rate) OVER prev_win
					END AS change
				FROM bar_funding
				WINDOW prev_win AS (PARTITION BY exchange, symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
			) sub
			WHERE ob.ctid = sub.ctid
		`, symbol, startTime, endTime, batchSize)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Batch update error: %v\n", err)
			errors++
			break
		}

		rowsUpdated := int(result.RowsAffected())
		updated += rowsUpdated
		fmt.Printf("Updated %d rows (total: %d)\n", rowsUpdated, updated)
		if rowsUpdated == 0 {
			break
		}
	}

	return updated, errors
}

func (db *DB) BackfillOpenInterestChange(ctx context.Context, symbolFilter *string, startTime, endTime *time.Time, batchSize int) (updated int, errors int) {
	label := "all symbols"
	if symbolFilter != nil {
		label = *symbolFilter
	}
	fmt.Printf("  Processing: %s\n", label)

	for {
		result, err := db.pool.Exec(ctx, `
			UPDATE orderbook_bars ob
			SET open_interest_change = sub.change
			FROM (
				SELECT
					ctid,
					open_interest - LAG(open_interest) OVER (PARTITION BY exchange, symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS change
				FROM orderbook_bars
				WHERE open_interest IS NOT NULL
					AND open_interest_change IS NULL
					AND ($1::text IS NULL OR symbol = $1)
					AND ($2::timestamptz IS NULL OR timestamp >= $2)
					AND ($3::timestamptz IS NULL OR timestamp < $3)
				LIMIT $4
			) sub
			WHERE ob.ctid = sub.ctid AND sub.change IS NOT NULL
		`, symbolFilter, startTime, endTime, batchSize)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Batch update error: %v\n", err)
			errors++
			break
		}

		rowsUpdated := int(result.RowsAffected())
		updated += rowsUpdated
		fmt.Printf("Updated %d rows (total: %d)\n", rowsUpdated, updated)
		if rowsUpdated == 0 {
			break
		}
	}

	return updated, errors
}

// deduplicateOrderbookBars removes bars with duplicate timestamps, keeping the
// bar with the highest trade_count for each timestamp. This prevents PostgreSQL's
// "ON CONFLICT DO UPDATE command cannot affect row a second time" error when
// the same minute-bar is accumulated multiple times in the flusher buffer.
func deduplicateOrderbookBars(bars []types.OrderbookBar) []types.OrderbookBar {
	if len(bars) < 2 {
		return bars
	}
	best := make(map[int64]types.OrderbookBar)
	for _, b := range bars {
		existing, ok := best[b.Timestamp]
		if !ok || b.TradeCount > existing.TradeCount ||
			(b.TradeCount == existing.TradeCount && b.AvgSpread > 0 && existing.AvgSpread == 0) {
			best[b.Timestamp] = b
		}
	}
	result := make([]types.OrderbookBar, 0, len(best))
	for _, b := range best {
		result = append(result, b)
	}
	return result
}
