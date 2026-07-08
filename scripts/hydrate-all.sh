#!/usr/bin/env bash
set -euo pipefail

START="${1:?Usage: $0 START END [WORKERS]}"
END="${2:?Usage: $0 START END [WORKERS]}"
WORKERS="${3:-4}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$SCRIPT_DIR/../config.yaml"

# Extract symbols from config.yaml (simple YAML list parsing)
SYMBOLS=$(sed -n '/^ *symbols:/,/^[^ ]/p' "$CONFIG" | grep -oP '\s+- \K.*')

echo "Hydrating $(echo "$SYMBOLS" | wc -l) symbols from $START to $END with $WORKERS workers"

echo "$SYMBOLS" | xargs -P "$WORKERS" -I{} go run "$SCRIPT_DIR/../cmd/ob-hydrate" \
	-symbol {} \
	-start "$START" \
	-end "$END"
