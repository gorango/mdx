-- audit_orderbook_bars.sql — correctness audit for orderbook_bars, focused on
-- the migration-0006 footprint scalars plus legacy-column health.
--
-- Usage:
--   psql "$PG_URL" -v ON_ERROR_STOP=1 \
--     -v start="2026-08-21" -v end="2026-08-26" \
--     -f scripts/audit_orderbook_bars.sql
--
-- Or via just:  just audit-ob 2026-08-21 2026-08-26
--
-- Without -v arguments the window defaults to the trailing 7 days.
-- Window semantics match hydration close-time convention: timestamp holds the
-- CLOSE of each 1m bar, so [start, end) selects closes in that half-open span.
--
-- Interpretation: every check emits (check_name, violations, status). All
-- numeric bounds carry float64-noise tolerances sized from live data (worst
-- observed artifacts: side-VWAP 3.5e-9 of bar range; band-vs-total 1 ULP;
-- VWAP identity 1.1e-12 relative) — genuine bugs exceed them by orders of
-- magnitude. Any FAIL means: do not trust footprints for the affected rows.

\if :{?start}
\else
	SELECT (now() - interval '7 days')::text AS start \gset
\endif
\if :{?end}
\else
	SELECT now()::text AS end \gset
\endif

-- ══ 1. VERDICT SUMMARY ══════════════════════════════════════════════════════
-- Epsilon expressions are inlined per branch on purpose: joining a second
-- materialized CTE on (exchange,symbol,timestamp) produced pathological plans
-- against this table size. Each branch is one linear pass over the window.
WITH w AS (
	SELECT * FROM orderbook_bars
	WHERE timestamp >= :'start'::timestamptz AND timestamp < :'end'::timestamptz
)
SELECT check_name, violations,
	CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
	SELECT '01 footprint missing on traded bars' AS check_name,
		count(*) FILTER (WHERE trade_count > 0 AND trade_open IS NULL) AS violations FROM w
	UNION ALL
	SELECT '02 footprint present on trade-less bars',
		count(*) FILTER (WHERE trade_count = 0 AND trade_open IS NOT NULL) FROM w
	UNION ALL
	SELECT '03 traded bars with zero volume',
		count(*) FILTER (WHERE trade_count > 0 AND buy_volume + sell_volume = 0) FROM w
	UNION ALL
	SELECT '04 side-NULL mismatch (buy vwap vs poc)',
		count(*) FILTER (WHERE (buy_vwap IS NULL) <> (buy_poc_price IS NULL)) FROM w
	UNION ALL
	SELECT '05 side-NULL mismatch (sell vwap vs poc)',
		count(*) FILTER (WHERE (sell_vwap IS NULL) <> (sell_poc_price IS NULL)) FROM w
	UNION ALL
	SELECT '06 band columns NULL on populated bar',
		count(*) FILTER (WHERE trade_open IS NOT NULL
			AND (hi_band_buy_vol IS NULL OR hi_band_sell_vol IS NULL
			  OR lo_band_buy_vol IS NULL OR lo_band_sell_vol IS NULL)) FROM w
	UNION ALL
	SELECT '07 OHLC ordering broken',
		count(*) FILTER (WHERE trade_open IS NOT NULL AND NOT (
			   trade_low - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)),
				1e-6 * GREATEST(trade_high - trade_low, 0)) <= trade_open
			AND trade_open <= trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)),
				1e-6 * GREATEST(trade_high - trade_low, 0))
			AND trade_low <= trade_close AND trade_close <= trade_high)) FROM w
	UNION ALL
	SELECT '08 POC outside traded range',
		count(*) FILTER (WHERE trade_open IS NOT NULL AND (
			   poc_price < trade_low  - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)),
				1e-6 * GREATEST(trade_high - trade_low, 0))
			OR poc_price > trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)),
				1e-6 * GREATEST(trade_high - trade_low, 0)))) FROM w
	UNION ALL
	SELECT '09 POC share outside (0,1]',
		count(*) FILTER (WHERE poc_volume_share <= 0 OR poc_volume_share > 1 + 1e-9) FROM w
	UNION ALL
	SELECT '10 side VWAP outside traded range',
		count(*) FILTER (WHERE trade_open IS NOT NULL AND (
			   (buy_vwap  IS NOT NULL AND (buy_vwap  < trade_low  - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))
				 OR buy_vwap  > trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))))
			OR (sell_vwap IS NOT NULL AND (sell_vwap < trade_low  - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))
				 OR sell_vwap > trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0)))))) FROM w
	UNION ALL
	SELECT '11 side POC outside traded range',
		count(*) FILTER (WHERE trade_open IS NOT NULL AND (
			   (buy_poc_price  IS NOT NULL AND (buy_poc_price  < trade_low  - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))
				 OR buy_poc_price  > trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))))
			OR (sell_poc_price IS NOT NULL AND (sell_poc_price < trade_low  - GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0))
				 OR sell_poc_price > trade_high + GREATEST(1e-9 * GREATEST(abs(trade_high), abs(trade_low)), 1e-6 * GREATEST(trade_high - trade_low, 0)))))) FROM w
	UNION ALL
	SELECT '12 band volume exceeds side total',
		count(*) FILTER (WHERE trade_open IS NOT NULL AND (
			   hi_band_buy_vol  > buy_volume  + 1e-12 * GREATEST(buy_volume + sell_volume, 1e-6)
			OR hi_band_sell_vol > sell_volume + 1e-12 * GREATEST(buy_volume + sell_volume, 1e-6)
			OR lo_band_buy_vol  > buy_volume  + 1e-12 * GREATEST(buy_volume + sell_volume, 1e-6)
			OR lo_band_sell_vol > sell_volume + 1e-12 * GREATEST(buy_volume + sell_volume, 1e-6))) FROM w
	UNION ALL
	SELECT '13 VWAP identity broken (>1e-9 rel)',
		count(*) FILTER (WHERE buy_volume + sell_volume > 0 AND COALESCE(vwap, 0) <> 0
		AND abs(vwap - (COALESCE(buy_vwap * buy_volume, 0) + COALESCE(sell_vwap * sell_volume, 0))
			/ (buy_volume + sell_volume)) / abs(vwap) > 1e-9) FROM w
) checks
ORDER BY status DESC, check_name;

-- ══ 2. FOOTPRINT COVERAGE ═══════════════════════════════════════════════════
-- Informational: how much of the window carries footprint data. Missing day-1
-- style cohorts here usually mean the hydration run started after this window.
SELECT count(*) AS bars,
	count(DISTINCT symbol) AS symbols,
	min(timestamp) AS first_close, max(timestamp) AS last_close,
	count(*) FILTER (WHERE trade_count > 0) AS traded_bars,
	count(*) FILTER (WHERE trade_count > 0 AND trade_open IS NOT NULL) AS with_footprint,
	count(*) FILTER (WHERE trade_count = 0) AS trade_less
FROM orderbook_bars
WHERE timestamp >= :'start'::timestamptz AND timestamp < :'end'::timestamptz;

-- ══ 3. NULL-PATTERN TAXONOMY ════════════════════════════════════════════════
-- Expect exactly four legitimate patterns:
--   11111111111 both sides traded · 11110111111 sell-only ·
--   11111011111 buy-only         · 00000000000 trade-less
-- Anything else = consistency-group corruption; investigate immediately.
SELECT CASE WHEN trade_open    IS NULL THEN '0' ELSE '1' END
    || CASE WHEN trade_high    IS NULL THEN 0 ELSE 1 END
    || CASE WHEN trade_low     IS NULL THEN 0 ELSE 1 END
    || CASE WHEN trade_close   IS NULL THEN 0 ELSE 1 END
    || CASE WHEN buy_vwap      IS NULL THEN 0 ELSE 1 END
    || CASE WHEN sell_vwap     IS NULL THEN 0 ELSE 1 END
    || CASE WHEN poc_price     IS NULL THEN 0 ELSE 1 END
    || CASE WHEN buy_poc_price IS NULL THEN 0 ELSE 1 END
    || CASE WHEN sell_poc_price IS NULL THEN 0 ELSE 1 END
    || CASE WHEN trade_price_std IS NULL THEN 0 ELSE 1 END
    || CASE WHEN hi_band_buy_vol IS NULL THEN 0 ELSE 1 END
       AS pattern_ohlc_bvwap_svwap_poc_bpoc_spoc_std_hibb,
	count(*) AS bars
FROM orderbook_bars
WHERE timestamp >= :'start'::timestamptz AND timestamp < :'end'::timestamptz
GROUP BY 1 ORDER BY 2 DESC;

-- ══ 4. HOURLY COVERAGE ══════════════════════════════════════════════════════
-- Sparse edge buckets (first/last hour of the window, live in-progress hour)
-- are normal; interior hours far below the symbol floor indicate vendor gaps
-- or failed hours worth investigating in hydration logs.
WITH hrs AS (
	SELECT date_trunc('hour', timestamp) AS h,
		count(DISTINCT symbol) AS syms, count(*) AS bars
	FROM orderbook_bars
	WHERE timestamp >= :'start'::timestamptz AND timestamp < :'end'::timestamptz
	GROUP BY 1
)
SELECT count(*) AS hours, min(syms) AS min_syms_per_hour,
	min(bars) AS min_bars_per_hour,
	count(*) FILTER (WHERE syms < (SELECT max(syms) FROM hrs)) AS hours_below_symbol_floor,
	(SELECT max(syms) FROM hrs) AS universe_floor
FROM hrs;

-- ══ 5. LEGACY COLUMN HEALTH ═════════════════════════════════════════════════
-- Rates should match long-run baselines (funding/OI ~100%; liq_covered ≈69%
-- for the majors-covered/long-tail-gapped universe). A sudden drop means a
-- re-run damaged COALESCE/GREATEST-protected columns.
SELECT round(count(*) FILTER (WHERE funding_rate IS NOT NULL)::numeric / count(*), 4) AS funding_nonnull,
	round(count(*) FILTER (WHERE open_interest IS NOT NULL)::numeric / count(*), 4) AS oi_nonnull,
	round(avg(liq_covered)::numeric, 4) AS liq_covered_rate
FROM orderbook_bars
WHERE timestamp >= :'start'::timestamptz AND timestamp < :'end'::timestamptz;
