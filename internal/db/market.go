package db

import (
	"context"
	"fmt"
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/orderbook/api"
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
	defer tx.Rollback(ctx)

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
	defer tx.Rollback(ctx)

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
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy to temp table: %w", err)
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO orderbook_bars (exchange, symbol, timestamp, vwap, trade_count, buy_volume, sell_volume, avg_spread, spread_std_dev, depth_imbalance, depth_ratio, open_interest, open_interest_change, funding_rate, funding_rate_change, liq_long_vol, liq_short_vol, liq_covered)
		SELECT exchange, symbol, timestamp, vwap, trade_count, buy_volume, sell_volume, avg_spread, spread_std_dev, depth_imbalance, depth_ratio, open_interest, open_interest_change, funding_rate, funding_rate_change, liq_long_vol, liq_short_vol, liq_covered FROM %s
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
			liq_covered = GREATEST(orderbook_bars.liq_covered, EXCLUDED.liq_covered)
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
			liq_long_vol, liq_short_vol, liq_covered
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
			liq_long_vol, liq_short_vol, liq_covered
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
