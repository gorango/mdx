set dotenv-load

# --- Build ---

build:
	go build ./...

build-all:
	go build ./cmd/...

# --- Test ---

test:
	go test ./internal/... -v

test-cover:
	go test ./internal/... -coverprofile=cover.out -covermode=atomic
	go tool cover -html=cover.out -o cover.html
	@echo "Coverage report: exchanges/cover.html"

test-unit PACKAGE:
	go test {{PACKAGE}} -v

# --- Lint & Format ---

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# --- Database ---

PG_URL := env("PG_URL", "postgres://postgres:postgres@localhost:5432/twain?sslmode=disable")

migrate:
	@PG_URL="{{PG_URL}}" sh scripts/migrate.sh

migrate-status:
	psql {{PG_URL}} -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"

sql-listen-ob:
	python3 scripts/listen_bars.py orderbook_bar_insert

sql-listen-price:
	python3 scripts/listen_bars.py price_bar_insert

# --- CLI: Price Cache ---

pc HIST SYMBOL TF:
	go run ./cmd/price \
	    -exchange {{HIST}} \
	    -symbol {{SYMBOL}} \
	    -timeframe {{TF}}

pc-backfill HIST SYMBOL TF START END:
	go run ./cmd/price \
	    -exchange {{HIST}} \
	    -symbol {{SYMBOL}} \
	    -timeframe {{TF}} \
	    -start {{START}} \
	    -end {{END}}

pc-stats:
	go run ./cmd/price -stats

# --- CLI: Trade ---

trade-balance MODE:
	go run ./cmd/trade -exchange {{MODE}} balance

trade-positions MODE:
	go run ./cmd/trade -exchange {{MODE}} positions

trade-order MODE SYMBOL SIDE AMOUNT *PRICE:
	@if [ "{{PRICE}}" = "" ]; then \
	    go run ./cmd/trade -exchange {{MODE}} order {{SYMBOL}} {{SIDE}} {{AMOUNT}}; \
	else \
	    go run ./cmd/trade -exchange {{MODE}} order {{SYMBOL}} {{SIDE}} {{AMOUNT}} {{PRICE}}; \
	fi

# --- CLI: Stream ---

stream:
	go run ./cmd/stream -config config.yaml

ob-hydrate SYMBOL START END:
	go run ./cmd/ob-hydrate \
	    -symbol {{SYMBOL}} \
	    -start {{START}} \
	    -end {{END}}

ob-hydrate-all START END WORKERS='4':
	nohup bash scripts/hydrate-all.sh {{START}} {{END}} {{WORKERS}} > /tmp/ob-hydrate-{{START}}-{{END}}.log 2>&1 &
	@echo "PID: $$! | log: tail -f /tmp/ob-hydrate-{{START}}-{{END}}.log"

price-hydrate-all START END WORKERS='4':
	nohup bash scripts/price-hydrate-all.sh {{START}} {{END}} {{WORKERS}} > /tmp/price-hydrate-{{START}}-{{END}}.log 2>&1 &
	@echo "PID: $$! | log: tail -f /tmp/price-hydrate-{{START}}-{{END}}.log"

prune SYMBOL:
	go run ./cmd/prune -type orderbook -exchange binance_futures -symbol {{SYMBOL}}

prune-price SYMBOL:
	go run ./cmd/prune -type price -exchange binance_futures -symbol {{SYMBOL}}

prune-all SYMBOL:
	go run ./cmd/prune -type orderbook -exchange binance_futures -symbol {{SYMBOL}} -end 2020-01-01

prune-all-price SYMBOL:
	go run ./cmd/prune -type price -exchange binance_futures -symbol {{SYMBOL}} -end 2020-01-01

# --- SQL Scripts ---

sql-symbol-dates:
	psql {{PG_URL}} -f scripts/sql/symbol_date_ranges.sql

sql-bar-outliers START='' END='':
	@bash -c 'if [ -z "{{START}}" ] && [ -z "{{END}}" ]; then bash scripts/sql/bar_outliers.sh; elif [ -z "{{END}}" ]; then bash scripts/sql/bar_outliers.sh {{START}}; else bash scripts/sql/bar_outliers.sh {{START}} {{END}}; fi'

sql-ob-progress START='' END='':
	@bash -c 'if [ -z "{{START}}" ] && [ -z "{{END}}" ]; then bash scripts/sql/ob_progress.sh; elif [ -z "{{END}}" ]; then bash scripts/sql/ob_progress.sh {{START}}; else bash scripts/sql/ob_progress.sh {{START}} {{END}}; fi'

# --- USDT.D ---

# Fetch/update USDT.D — incremental by default (only fetches missing days).
# Pass a number to force-refetch that many days (e.g. 365 for full).
usdtd-fetch *FLAGS='':
	uv run scripts/fetch-usdtd.py {{FLAGS}}

usdtd-recompute:
	uv run scripts/fetch-usdtd.py --no-fetch

# --- Dev shortcuts ---

dev-pc:
	just pc binance BTC/USDT:PERP 1h

dev-pc-full:
	just pc-backfill binance BTC/USDT:PERP 1h 2024-01-01T00:00:00Z 2025-01-01T00:00:00Z

dev-paper-buy SYMBOL AMOUNT:
	just trade-order paper {{SYMBOL}} buy {{AMOUNT}}

dev-paper-limit SYMBOL AMOUNT PRICE:
	just trade-order paper {{SYMBOL}} buy {{AMOUNT}} {{PRICE}}


