#!/usr/bin/env bash
set -euo pipefail

START="${1:?Usage: $0 START END [WORKERS]}"
END="${2:?Usage: $0 START END [WORKERS]}"
WORKERS="${3:-4}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYMBOLS_FILE="$PROJECT_ROOT/config/symbols.yaml"

# Read symbols from canonical config/symbols.yaml
SYMBOLS=$(grep -oP '(?<=- ").*(?=")' "$SYMBOLS_FILE")

echo "Building price..."
cd "$SCRIPT_DIR/.."
BINARY=$(mktemp /tmp/price.XXXXXX)
go build -o "$BINARY" ./cmd/price
trap 'rm -f "$BINARY"' EXIT

echo "Hydrating price data for $(echo "$SYMBOLS" | wc -l) symbols from $START to $END with $WORKERS workers"

echo "$SYMBOLS" | xargs -P "$WORKERS" -I{} "$BINARY" \
	-exchange binance \
	-symbol {} \
	-timeframe 1m \
	-start "${START}T00:00:00Z" \
	-end "${END}T00:00:00Z"
