#!/bin/sh
set -e
for f in migrations/*.sql; do
  echo "Running $f..."
  psql "$PG_URL" -v ON_ERROR_STOP=1 -f "$f"
done
