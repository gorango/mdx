-- Store pre-computed feature vectors for the signal generator.
-- Populated by exchanges/cmd/seed-vectors/ (reads bars from PG, calls
-- the NATS features service, caches the 89-dimensional float4[] result).
-- The signalgen --from-vectors flag reads from this table instead of
-- hitting the NATS feature service on every run.
--
-- ~350 bytes per row (89 float32 + timestamptz + text overhead).
-- 1 month of 1m bars for 100 symbols ≈ 1.6 GB.
-- PK on (exchange, symbol, timestamp) enables fast range scans per symbol.

CREATE TABLE IF NOT EXISTS vector_bars (
    exchange      text         NOT NULL,
    symbol        text         NOT NULL,
    timestamp     timestamptz  NOT NULL,
    features      real[]       NOT NULL,
    feature_count smallint     NOT NULL DEFAULT 0,
    PRIMARY KEY (exchange, symbol, timestamp)
);
