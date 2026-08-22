-- Migration: Trade-derived level statistics on orderbook_bars ("footprint scalars").
--
-- Every column below is a REDUCTION of a transient per-minute trade histogram
-- kept by the aggregators (batch internal/orderbook/aggregator and live
-- .../aggregator/streaming, bound by a shared parity contract via the
-- levelhist package). No distributional data is persisted: one scalar row per
-- minute, ~15x8 bytes ≈ 19 MB/day across a ~110-symbol universe.
--
-- CONSISTENCY GROUP: all trade_* / *_vwap / *_poc / *_band_* columns are
-- computed from ONE trade population and MUST be written together. The upsert
-- rules in db.InsertOrderbookBars take the incoming group ONLY when it is
-- complete (trade_open IS NOT NULL — always set when any trades exist) AND it
-- wins on trade_count; otherwise the stored group survives untouched. This
-- prevents a higher-trade_count writer without footprint data (e.g. an
-- older-binary stream) from blanking known-good values. Never COALESCE the
-- group column-by-column across writers.
--
-- NULL discipline (same convention as liq_* columns — never fake values):
--   * All columns are NULL when trade_count = 0 for the minute
--     (spread/depth-only bars carry no trade population).
--   * buy_vwap / buy_poc_price are additionally NULL when the bar had zero
--     taker-buy volume; sell_vwap / sell_poc_price when zero taker-sell volume.
--   * The band volumes are true zeros when a side did not trade at the
--     extremes of an otherwise-populated bar.

ALTER TABLE orderbook_bars
    ADD COLUMN IF NOT EXISTS trade_open         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trade_high         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trade_low          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trade_close        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS buy_vwap           DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sell_vwap          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS poc_price          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS poc_volume_share   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS buy_poc_price      DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sell_poc_price     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trade_price_std    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS hi_band_buy_vol    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS hi_band_sell_vol   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS lo_band_buy_vol    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS lo_band_sell_vol   DOUBLE PRECISION;
