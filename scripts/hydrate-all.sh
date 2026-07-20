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

echo "Building ob-hydrate..."
BINARY=$(mktemp /tmp/ob-hydrate.XXXXXX)
go build -o "$BINARY" "$SCRIPT_DIR/../cmd/ob-hydrate"
trap 'rm -f "$BINARY"; rm -rf "$FUNDING_CACHE"' EXIT

FUNDING_CACHE=$(mktemp -d /tmp/ob-hydrate-funding.XXXXXX)

echo "Hydrating $(echo "$SYMBOLS" | wc -l) symbols from $START to $END with $WORKERS workers"

echo "$SYMBOLS" | xargs -P "$WORKERS" -I{} "$BINARY" \
	-symbol {} \
	-start "$START" \
	-end "$END" \
	-funding-cache "$FUNDING_CACHE"
