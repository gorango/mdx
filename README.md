# Market Data Exchange (MDX)

High-performance market data microservice for algorithmic trading — WebSocket and REST feeds from Binance, Bybit, and Hyperliquid, orderbook aggregation, and NATS pub/sub for real-time distribution.

## Packages

| Package | Description |
|---|---|
| `domain/types` | Core types: `Bar`, `OrderbookBar`, `Order`, `Balance`, `Position`, `Event`, `Trade`, `OrderbookUpdate`, `Liquidation`, `FundingRate`, `OpenInterest` |
| `domain/timeframe` | Timeframe constants (`1m`–`1M`) |
| `domain/symbols` | Symbol normalization: canonical ↔ exchange format |
| `internal/orderbook/treap` | Price-level order book tree (treap implementation) |
| `internal/config` | YAML config loading (`config.yaml`) |
| `internal/db` | PostgreSQL via pgx/v5: price bars, orderbook bars storage |
| `internal/ws` | WebSocket clients: Binance, Bybit, Hyperliquid |
| `internal/rest` | Direct REST API clients: Binance, Bybit |
| `internal/pubsub` | NATS publisher/subscriber for real-time event streaming |
| `internal/cache` | Multi-level price cache (LRU memory + PostgreSQL + REST) |
| `internal/trading` | Trading connectors: REST, paper trader |
| `internal/orderbook` | Orderbook bar aggregation, flusher, batch hydration pipeline, parquet |
| `internal/streamer` | Multi-exchange streaming aggregator and flusher |
| `internal/subscription` | Subscription manager for dynamic symbol subscription |

## Commands

| Command | Description |
|---|---|
| `cmd/exchange` | Unified daemon: WS streaming + NATS + trading connectors |
| `cmd/stream` | Streaming-only microservice (without trading) |
| `cmd/ob-stream` | WebSocket streaming with NATS publishing (orderbook) |
| `cmd/ob-hydrate` | Batch historical orderbook data hydration |
| `cmd/ob-parity` | Live-vs-historical orderbook parity harness |
| `cmd/ob-compare` | Orderbook comparison tool (invoked by ob-parity) |
| `cmd/ob-backfill-funding` | Backfill funding rate on orderbook bars |
| `cmd/ob-backfill-oi-change` | Backfill open interest change on orderbook bars |
| `cmd/price` | Price history cache CLI with gap detection |
| `cmd/prune` | Data pruning tool (orderbook + price data) |
| `cmd/trade` | Trading CLI (paper or live) |

## Quick Start

### Prerequisites

- Go 1.24+
- PostgreSQL 15+
- NATS Server (optional, for pub/sub)

### Setup

```bash
go mod download

# Run database migrations
psql $PG_URL -f migrations/0000_initial_schema.sql
psql $PG_URL -f migrations/0001_add_notify_trigger.sql

# Configure
cp config.yaml config.local.yaml
```

### Run the Daemon

```bash
./bin/exchange -config config.yaml -nats nats://localhost:4222
```

### CLI Examples

```bash
# Fetch price history
go run ./cmd/price \
  -exchange binance \
  -symbol BTC/USDT:PERP \
  -timeframe 1h \
  -start 2024-01-01T00:00:00Z

# Fetch with stats (detect gaps in data)
go run ./cmd/price \
  -exchange binance \
  -symbol BTC/USDT:PERP \
  -timeframe 1h \
  -stats

# Project to higher timeframe
go run ./cmd/price \
  -exchange binance \
  -symbol BTC/USDT:PERP \
  -timeframe 1m \
  -project 1h

# Prune orderbook data
go run ./cmd/prune -type orderbook -symbol BTC/USDT:PERP

# Prune price data
go run ./cmd/prune -type price -symbol BTC/USDT:PERP

# Batch hydrate historical orderbook data
go run ./cmd/ob-hydrate \
  -exchange binance_futures \
  -symbol BTC/USDT:PERP \
  -start 2024-01-01 \
  -end 2024-01-31 \
  -workers 4

**Note on Orderbook State Reset:** The hydration pipeline resets the orderbook state at the beginning of each hour. This is necessary because the source data provides delta updates without initial L2 snapshots. Processing multiple hours without resetting would cause "ghost levels" to accumulate, leading to incorrect spread and depth calculations.

# Paper trade
go run ./cmd/trade -exchange paper balance
go run ./cmd/trade -exchange paper order BTC/USDT:PERP buy 0.1
```

### Orderbook Parity Harness

The harness collects live orderbook bars into PostgreSQL, records the exact UTC capture window, hydrates historical bars for the same comparison window into JSON, then runs `ob-compare` against the live DB rows.

```bash
go run ./cmd/ob-parity \
  -symbol BTC/USDT:PERP \
  -duration 2h \
  -warmup 2m \
  -config config.yaml
```

Each run writes `parity-output/<run-id>/manifest.json` and historical JSON under `parity-output/<run-id>/hydrate/`. Use `-skip-live -start YYYY-MM-DDTHH:MM -end YYYY-MM-DDTHH:MM` to rerun historical hydration and comparison for an already captured live window without waiting another 1-2 hours.

## NATS Topics

Published events (no exchange segment in publish path):

```
market.{symbol}.trades
market.{symbol}.ob
market.{symbol}.liquidations
market.{symbol}.funding
market.{symbol}.oi
```

Bar data (exchange-aware):

```
market.{exchange}.{symbol}.bars.{timeframe}
```

Control plane (request-reply):

```
subscriptions.subscribe
subscriptions.unsubscribe
history.bars
```

Order execution (engine ↔ bridge, request-reply):

```
orders.{symbol}.{action}      # action ∈ {open, close}; payload = engine OrderRequest JSON
orders.{symbol}.cancel        # payload = {symbol, order_id}
```

The `cmd/exchange` daemon runs an order bridge subscribed to `orders.>` (queue group
`order-bridge`) that translates engine order JSON into `domain/types.OrderRequest`,
submits it through the live connector, and replies with an execution report:

- `action=close` maps to `reduceOnly=true` — a close can never flip into a reverse position.
- `order_type=post_only` maps to a limit order with `GTX` (post-only) time-in-force.
- Order quantities are rounded down to the exchange's lot size (`FetchLotSize`) and
  rejected below the minimum order quantity.
- `leverage` is applied before opening via `SetLeverage` (Binance `/fapi/v1/leverage`,
  Bybit `/v5/position/set-leverage`).

## Symbol Format

Uses a canonical format: `BASE/QUOTE:PERP`

| Exchange | Format | Example |
|---|---|---|
| Canonical | `BASE/QUOTE:PERP` | `BTC/USDT:PERP` |
| Binance | `BASeQuotEPERP` | `BTCUSDT` |
| Bybit | `BASeQuotEPERP` | `BTCUSDT` |
| Hyperliquid | `BASE/USDC:PERP` | `BTC/USDC:PERP` |

## Architecture

### Live Data Flow

```mermaid
flowchart LR
    WS[Exchange WS] --> Agg[Aggregator]
    WS --> Nats[NATS pub/sub]
    Agg --> Flush[Flusher] --> PG[(PostgreSQL)]
    Agg --> Daemon[Daemon]
    Nats --> Subs[Subscribers]
```

### Price Cache

```mermaid
flowchart LR
    Mem[Memory LRU] <--> PGC[(PostgreSQL)]
    PGC <--> REST[REST]
    PGC --> Gap[Gap Detection & Fill]
```

## Price Cache Features

### Gap Detection & Fill

The price cache automatically detects and fills gaps in historical data from REST providers during backfill operations. Use `-stats` to detect gaps:

```bash
go run ./cmd/price -exchange binance -symbol BTC/USDT:PERP -timeframe 1h -stats
```

### Timeframe Projection

Project lower timeframe bars to higher timeframes:

```bash
go run ./cmd/price -exchange binance -symbol BTC/USDT:PERP -timeframe 1m -project 1h
```

## Testing

```bash
# Run all tests
go test ./internal/... -v

# With coverage
go test ./internal/... -coverprofile=cover.out
go tool cover -html=cover.out
```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PG_URL` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5432/mdx?sslmode=disable` |
| `EXCHANGE_API_KEY` | Exchange API key (for live trading) | — |
| `EXCHANGE_SECRET` | Exchange secret | — |
| `CRYPTO_HFT_DATA` | Data API key (for hydration) | — |
