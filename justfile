set dotenv-load

# --- Build ---

build:
	go build ./...

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

# Lint with golangci-lint (errcheck, staticcheck, unused, vet)
lint:
	golangci-lint run ./... --max-same-issues=0 --max-issues-per-linter=0

tidy:
	go mod tidy

# --- Database ---

PG_URL := env("PG_URL", "postgres://postgres:postgres@localhost:5432/mdx?sslmode=disable")

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

trade-order MODE SYMBOL SIDE AMOUNT *ARGS='':
	@go run ./cmd/trade -exchange {{MODE}} order {{SYMBOL}} {{SIDE}} {{AMOUNT}} {{ARGS}}

# --- CLI: Stream ---

daemon CONFIG='config.yaml' NATS='nats://localhost:4222':
	go run ./cmd/exchange -config {{CONFIG}} -nats {{NATS}}

# Extra args (e.g. `-backfill-ob`) are passed through to cmd/stream.
stream *ARGS='':
	@go run ./cmd/stream {{ARGS}}

stream-backfill:
	@go run ./cmd/stream -backfill-ob

ob-hydrate SYMBOL START END:
	go run ./cmd/ob-hydrate \
	    -symbol {{SYMBOL}} \
	    -start {{START}} \
	    -end {{END}}

# Re-derive ONLY the liquidation columns from cryptoHFT liquidations parquets
# across all symbols (vendor 404 → liq NULL / liq_covered=0).  Non-liq columns untouched.
ob-backfill-liq START='2025-07-01' END='2026-08-01' WORKERS='8':
	@go run ./cmd/ob-backfill-liq -start {{START}} -end {{END}} -workers {{WORKERS}}

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
	@bash scripts/sql/bar_outliers.sh {{START}} {{END}}

sql-ob-progress START='' END='':
	@bash scripts/sql/ob_progress.sh {{START}} {{END}}

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


