-- Symbol date ranges for orderbook_bars and price_bars tables
-- Run with: just sql-symbol-dates

\pset pager off

\echo '=== orderbook_bars ==='
SELECT
    symbol,
    MIN(timestamp) as min_date,
    MAX(timestamp) as max_date,
    COUNT(*) as bar_count
FROM orderbook_bars
GROUP BY symbol
ORDER BY symbol;

\echo '=== price_bars ==='
SELECT
    symbol,
    MIN(timestamp) as min_date,
    MAX(timestamp) as max_date,
    COUNT(*) as bar_count
FROM price_bars
GROUP BY symbol
ORDER BY symbol;
