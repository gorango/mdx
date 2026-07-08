# Parquet Data Schemas

This package is responsible for streaming and parsing historical `.parquet` (specifically `.parquet.zst`) files downloaded from the CryptoHFT Data API.

Below are the exact schemas of the Parquet files provided by the API, and how they map to our internal hydration pipeline.

## 1. Orderbook Updates (`types.OrderBook`)

Delta updates and snapshots for the L2 Orderbook.

| Key | Type | Description |
|---|---|---|
| `received_time` | int64 | Timestamp when API gateway received it |
| `event_time` | int64 | **Used**: Timestamp from the exchange |
| `transaction_time` | int64 | Exchange transaction time |
| `symbol` | string | e.g. `BTCUSDT` |
| `event_type` | string | **Used**: `update` or `snapshot` |
| `first_update_id` | int64 | First update ID in the event |
| `final_update_id` | int64 | **Used**: Final update ID (used for stream continuity) |
| `prev_final_update_id` | int64 | **Used**: Previous final update ID (used for gap detection) |
| `last_update_id` | int64 | Used for snapshots where `final_update_id` is missing |
| `side` | string | **Used**: `ask` or `bid` |
| `price` | float64 | **Used**: Price level |
| `quantity` | float64 | **Used**: Total quantity at this price level |
| `order_count` | int32 | Number of orders at this level (often null) |

## 2. Trades (`types.Trade`)

Individual executed trades.

| Key | Type | Description |
|---|---|---|
| `received_time` | int64 | Timestamp when API gateway received it |
| `event_time` | int64 | Timestamp from the exchange |
| `symbol` | string | e.g. `BTCUSDT` |
| `trade_id` | int64 | Unique trade ID |
| `price` | float64 | **Used**: Trade execution price |
| `quantity` | float64 | **Used**: Quantity executed |
| `trade_time` | int64 | **Used**: Time the trade executed |
| `is_buyer_maker` | bool | **Used**: True if seller hit a resting bid (Sell volume) |
| `order_type` | string | e.g. `MARKET` (Ignored by our reader) |

## 3. Open Interest (`types.OpenInterest`)

Periodic snapshots of total open interest for the symbol.

| Key | Type | Description |
|---|---|---|
| `received_time` | int64 | Timestamp when API gateway received it |
| `symbol` | string | e.g. `BTCUSDT` |
| `sum_open_interest` | float64 | **Used**: Total OI in base asset (e.g. BTC) |
| `sum_open_interest_value` | float64 | Total OI in quote asset notional value |
| `timestamp` | int64 | **Used**: Time of the snapshot |

## 4. Liquidations (`types.Liquidation`)

Forced liquidation events.

| Key | Type | Description |
|---|---|---|
| `received_time` | int64 | Timestamp when API gateway received it |
| `event_time` | int64 | Timestamp from the exchange |
| `symbol` | string | e.g. `BTCUSDT` |
| `side` | string | **Used**: Side of the liquidation order (`BUY` = Shorts liquidated) |
| `order_type` | string | e.g. `LIMIT` |
| `time_in_force` | string | e.g. `IOC` |
| `quantity` | float64 | Total quantity of the liquidation |
| `price` | float64 | Bankruptcy price |
| `average_price` | float64 | Average execution price |
| `order_status` | string | e.g. `FILLED` |
| `last_filled_quantity` | float64 | **Used**: Exact amount liquidated in this event |
| `filled_quantity` | float64 | Total filled quantity so far |
| `trade_time` | int64 | **Used**: Time the liquidation occurred |
