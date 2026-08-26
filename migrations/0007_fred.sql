-- FRED macro series (Federal Reserve Economic Data → postgres).
-- Daily/weekly/monthly observations from api.stlouisfed.org, populated by
-- scripts/fetch-fred.py on a schedule. Long/narrow layout: one row per
-- (series, date). Tiny volume (~15 series × 365 rows/yr ≈ 5k rows/yr).
--
-- Conventions match the rest of the schema:
--   * fred_observations.timestamp is the observation date at 00:00 UTC
--     (FRED publishes calendar dates; stored as TIMESTAMPTZ for joins).
--   * Missing observations (FRED "." sentinel) are never stored — gaps are
--     gaps. Consumers forward-fill at query time (e.g. LEFT JOIN LATERAL
--     ... ORDER BY timestamp DESC LIMIT 1) or in feature builds.
--   * Incremental fetch watermark per series in fred_fetch_state; same
--     pattern as netflow_fetch_state / flow_bars.
--   * fred_series is a catalog/cache of series metadata (frequency/units)
--     hydrated from FRED's /fred/series endpoint — optional but useful.

CREATE TABLE IF NOT EXISTS fred_series (
    series_id  TEXT        NOT NULL PRIMARY KEY,  -- 'DFF', 'T10Y2Y', 'WALCL'
    title      TEXT        NOT NULL DEFAULT '',
    frequency  TEXT        NOT NULL DEFAULT '',   -- 'Daily', 'Weekly', 'Monthly'
    units      TEXT        NOT NULL DEFAULT '',   -- 'Percent', 'Billions of Dollars'
    seasonal_adjustment TEXT NOT NULL DEFAULT '',
    notes      TEXT        NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fred_observations (
    series_id TEXT             NOT NULL REFERENCES fred_series(series_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ      NOT NULL,  -- observation date 00:00 UTC
    value     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (series_id, timestamp)
);

-- BRIN matches price/orderbook/flow convention (PK already has btree).
CREATE INDEX IF NOT EXISTS idx_fred_observations_timestamp_brin
    ON fred_observations USING BRIN (timestamp);
CREATE INDEX IF NOT EXISTS idx_fred_observations_series_time
    ON fred_observations (series_id, timestamp);

-- Watermark per series: last observation date successfully upserted.
CREATE TABLE IF NOT EXISTS fred_fetch_state (
    series_id  TEXT        NOT NULL PRIMARY KEY REFERENCES fred_series(series_id) ON DELETE CASCADE,
    last_ts    TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
