-- On-chain exchange flow features (BigQuery public datasets → postgres).
-- Hourly inflow/outflow/netflow of BTC, ETH (and ERC20 WBTC/USDT/USDC) into/out of
-- labeled exchange addresses. Populated by scripts/fetch-netflow.py on a schedule;
-- the script upserts into flow_bars and maintains the netflow_fetch_state watermark.
--
-- Conventions match the rest of the schema:
--   * flow_bars.timestamp is the CLOSE time of the hourly bucket [ts-1h, ts),
--     same as the migrated price/orderbook bars (see 0004).
--   * BigQuery batch lag (~3-18 blocks; BTC up to ~30 min) makes 1h the natural
--     base resolution. Aggregate up (4h/1d) downstream.
--   * Exchange flows are a slow regime signal — gate/size features, not direction.
--   * Timestamps are stored as TIMESTAMPTZ (server-local, same as price/orderbook).

CREATE TABLE IF NOT EXISTS address_labels (
    chain      text        NOT NULL,                -- 'bitcoin' | 'ethereum' | 'tron'
    address    text        NOT NULL,
    exchange   text        NOT NULL,                -- 'binance' (or 'tether'/'circle' for treasuries)
    kind       text        NOT NULL DEFAULT 'hot',  -- 'hot' | 'cold' | 'deposit' | 'treasury'
    source     text        NOT NULL,                -- 'binance_pow' | 'arkham' | 'public' | 'manual'
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address)
);

CREATE TABLE IF NOT EXISTS flow_bars (
    asset     text             NOT NULL,            -- 'BTC' | 'ETH' | 'WBTC' | 'USDT' | 'USDC'
    exchange  text             NOT NULL,            -- 'binance'
    timestamp timestamptz      NOT NULL,            -- close time of the hourly bucket
    inflow    double precision NOT NULL,            -- asset units arriving at labeled addresses
    outflow   double precision NOT NULL,            -- asset units leaving labeled addresses
    netflow   double precision NOT NULL,            -- inflow - outflow
    tx_count  integer          NOT NULL DEFAULT 0,
    source    text             NOT NULL,            -- 'bigquery'
    PRIMARY KEY (asset, exchange, timestamp)
);

-- BRIN matches the price/orderbook bar convention (no redundant btree over the PK).
CREATE INDEX IF NOT EXISTS idx_flow_bars_timestamp_brin ON flow_bars USING BRIN (timestamp);

-- Incremental-fetch watermark per (asset, exchange): last fully-computed hour.
CREATE TABLE IF NOT EXISTS netflow_fetch_state (
    asset    text        NOT NULL,
    exchange text        NOT NULL,
    last_ts  timestamptz NOT NULL,
    PRIMARY KEY (asset, exchange)
);
