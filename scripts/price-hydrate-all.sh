#!/usr/bin/env bash
set -euo pipefail

START="${1:?Usage: $0 START END [WORKERS]}"
END="${2:?Usage: $0 START END [WORKERS]}"
WORKERS="${3:-4}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR/.."

SYMBOLS=$(sed -n '/^ *symbols:/,/^[^ ]/p' config.yaml | grep -oP '\s+- \K.*')

echo "Hydrating price data for $(echo "$SYMBOLS" | wc -l) symbols from $START to $END with $WORKERS workers"

echo "$SYMBOLS" | xargs -P "$WORKERS" -I{} go run ./cmd/price \
	-exchange binance \
	-symbol {} \
	-timeframe 1m \
	-start "${START}T00:00:00Z" \
	-end "${END}T00:00:00Z"
