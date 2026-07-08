-- Migration: Initial schema for exchanges module
-- Price bars: OHLCV data stored per exchange/symbol/timestamp

CREATE TABLE IF NOT EXISTS price_bars (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_price_bars_time ON price_bars (exchange, symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_price_bars_timestamp_brin ON price_bars USING BRIN (timestamp);

-- Orderbook bars: aggregated microstructure metrics per minute

CREATE TABLE IF NOT EXISTS orderbook_bars (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    vwap DOUBLE PRECISION NOT NULL,
    trade_count INTEGER NOT NULL,
    buy_volume DOUBLE PRECISION NOT NULL,
    sell_volume DOUBLE PRECISION NOT NULL,
    avg_spread DOUBLE PRECISION NOT NULL,
    spread_std_dev DOUBLE PRECISION NOT NULL,
    depth_imbalance DOUBLE PRECISION NOT NULL,
    depth_ratio DOUBLE PRECISION NOT NULL,
    open_interest DOUBLE PRECISION,
    open_interest_change DOUBLE PRECISION,
    funding_rate DOUBLE PRECISION,
    funding_rate_change DOUBLE PRECISION,
    liq_long_vol DOUBLE PRECISION,
    liq_short_vol DOUBLE PRECISION,
    liq_covered INTEGER,
    PRIMARY KEY (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_orderbook_bars_time ON orderbook_bars (exchange, symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_orderbook_bars_timestamp_brin ON orderbook_bars USING BRIN (timestamp);
