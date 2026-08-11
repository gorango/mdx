#!/bin/bash
# Show orderbook hydration progress per symbol within a date range
# Usage: ./scripts/sql/ob_progress.sh [START_DATE] [END_DATE]
# Examples:
#   ./scripts/sql/ob_progress.sh              # All data
#   ./scripts/sql/ob_progress.sh 2026-05-01  # From May onwards
#   ./scripts/sql/ob_progress.sh 2026-05-01 2026-06-01  # Date range

DB_URL="${PG_URL:-postgres://postgres:postgres@localhost:5432/mdx?sslmode=disable}"

START=${1:-}
END=${2:-}

WHERE="true"
if [ -n "$START" ]; then
	WHERE="$WHERE AND timestamp >= '$START'::timestamptz"
fi
if [ -n "$END" ]; then
	WHERE="$WHERE AND timestamp < '$END'::timestamptz"
fi

psql "$DB_URL" <<EOF
\pset pager off

SELECT
    symbol,
    COUNT(*)                    AS bars,
    to_char(MIN(timestamp), 'MM-DD HH24:MI') AS earliest,
    to_char(MAX(timestamp), 'MM-DD HH24:MI') AS latest,
    ROUND(EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) AS days
FROM orderbook_bars
WHERE $WHERE
GROUP BY symbol
ORDER BY symbol;
EOF
