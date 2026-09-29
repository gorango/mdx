package db

import (
	"context"
	"gorango/mdx/domain/types"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertPriceBarsEmpty(t *testing.T) {
	database := &DB{}

	err := database.InsertPriceBars(context.Background(), "binance", "BTC/USDT", []types.Bar{})
	assert.NoError(t, err)
}

func TestInsertOrderbookBarsEmpty(t *testing.T) {
	database := &DB{}

	err := database.InsertOrderbookBars(context.Background(), "binance", "BTC/USDT", []types.OrderbookBar{})
	assert.NoError(t, err)
}

// countSessionTempTables counts the session-scoped tmp_* tables on the backend
// that serves pool. TEMP tables are session-scoped, so this is only meaningful
// while the pool is pinned to a single connection.
func countSessionTempTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_class
		WHERE relnamespace = pg_my_temp_schema()
		  AND relkind = 'r'
		  AND relname LIKE 'tmp\_%'
	`).Scan(&n))
	return n
}

// TestInsertBarsDoNotLeakTempTables is the regression test for the
// session-scoped TEMP table leak: both writers named a per-call temp table but
// never dropped it, so each *committed* call orphaned one table on the pooled
// backend for the life of the session. TEMP tables are session-scoped and a
// commit does not remove them, so under the live stream the leak is unbounded —
// that is what accumulated thousands of tmp_* tables in the catalog.
//
// The error path is safe by PostgreSQL semantics, not by the code: DDL is
// transactional, so a CREATE TEMP TABLE inside a rolled-back transaction is
// undone automatically. Only the commit path needed fixing; ON COMMIT DROP
// covers it (and also fires on rollback, so it is correct on both).
//
// The pool is pinned to one connection so repeated calls share a backend and the
// leak is observable. With the full multi-connection pool a temp table lands on
// an arbitrary backend, so the count query would see a different session.
func TestInsertBarsDoNotLeakTempTables(t *testing.T) {
	connString := os.Getenv("PG_URL")
	if connString == "" {
		t.Skip("PG_URL not set; skipping live-database temp-table leak regression test")
	}

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)
	cfg.MaxConns = 1
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("database unreachable at PG_URL: %v", err)
	}

	database := &DB{pool: pool}

	// Rows written below are cleaned up; the exchange tag keeps the test out of
	// every real symbol's way.
	const exchange = "__temp_leak_test__"
	const symbol = "LEAKTEST"
	base := time.Now().UTC().Truncate(time.Minute)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM price_bars WHERE exchange = $1`, exchange)
		_, _ = pool.Exec(ctx, `DELETE FROM orderbook_bars WHERE exchange = $1`, exchange)
	})

	// Not valid UTF-8, so the TEXT column rejects it during CopyFrom — after the
	// temp table has been created. The call must return an error and leave
	// nothing behind.
	const invalidExchange = "bad\xffexchange"

	before := countSessionTempTables(t, ctx, pool)

	for i := 0; i < 3; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)

		priceBars := []types.Bar{{Time: ts, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100}}
		require.NoError(t, database.InsertPriceBars(ctx, exchange, symbol, priceBars))
		assert.Error(t, database.InsertPriceBars(ctx, invalidExchange, symbol, priceBars))

		obBars := []types.OrderbookBar{{
			Timestamp:      ts.UnixMilli(),
			VWAP:           1.5,
			TradeCount:     3,
			BuyVolume:      60,
			SellVolume:     40,
			AvgSpread:      0.1,
			SpreadStdDev:   0.01,
			DepthImbalance: 0,
			DepthRatio:     1,
		}}
		require.NoError(t, database.InsertOrderbookBars(ctx, exchange, symbol, obBars))
		assert.Error(t, database.InsertOrderbookBars(ctx, invalidExchange, symbol, obBars))
	}

	after := countSessionTempTables(t, ctx, pool)
	assert.Equal(t, before, after,
		"InsertPriceBars/InsertOrderbookBars leaked session-scoped TEMP tables; "+
			"CREATE TEMP TABLE must include ON COMMIT DROP")
}
