#!/bin/bash
# Find daily bar outliers (bars != 1440 per day)
# Usage: ./scripts/sql/bar_outliers.sh [START_DATE] [END_DATE]
# Examples:
#   ./scripts/sql/bar_outliers.sh              # All data
#   ./scripts/sql/bar_outliers.sh 2025-01-01  # From date onwards
#   ./scripts/sql/bar_outliers.sh 2025-01-01 2025-12-31  # Date range

DB_URL="${PG_URL:-postgres://postgres:postgres@localhost:5432/twain?sslmode=disable}"

START=${1:-}
END=${2:-}

# Build the WHERE clause dynamically
WHERE_START=""
WHERE_END=""

if [ -n "$START" ]; then
	WHERE_START="DATE(timestamp) >= '$START'::date"
fi

if [ -n "$END" ]; then
	WHERE_END="DATE(timestamp) <= '$END'::date"
fi

# Combine conditions
if [ -n "$WHERE_START" ] && [ -n "$WHERE_END" ]; then
	WHERE="WHERE $WHERE_START AND $WHERE_END"
elif [ -n "$WHERE_START" ]; then
	WHERE="WHERE $WHERE_START"
elif [ -n "$WHERE_END" ]; then
	WHERE="WHERE $WHERE_END"
else
	WHERE=""
fi

# Run queries with dynamic WHERE clause
psql "$DB_URL" <<EOF
\pset pager off

\echo '=== orderbook_bars outliers (bars != 1440) ==='
SELECT
    symbol,
    DATE(timestamp) as date,
    COUNT(*) as bar_count,
    1440 - COUNT(*) as missing_bars
FROM orderbook_bars
$WHERE
GROUP BY symbol, DATE(timestamp)
HAVING COUNT(*) != 1440
ORDER BY symbol, DATE(timestamp);

\echo '=== price_bars outliers (bars != 1440) ==='
SELECT
    symbol,
    DATE(timestamp) as date,
    COUNT(*) as bar_count,
    1440 - COUNT(*) as missing_bars
FROM price_bars
$WHERE
GROUP BY symbol, DATE(timestamp)
HAVING COUNT(*) != 1440
ORDER BY symbol, DATE(timestamp);
EOF
