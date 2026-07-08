-- Migration: Migrate price_bars and orderbook_bars timestamp from open time to close time
-- Close time = open time + 1 minute for 1m bars (all base data uses 1m resolution)
--
-- Strategy: drop PK, do ONE full-table UPDATE per table, re-add PK.
-- A single-step UPDATE fails with 25M+ rows because PostgreSQL checks the
-- unique constraint per-row: a bar at T moving to T+1 collides with the old
-- value of the next bar at T+1.  Dropping the constraint temporarily avoids
-- this, and the single UPDATE is ~2x faster than the two-step workaround.

BEGIN;

-- Drop PK and the redundant B-tree index (same columns, same order — the PK
-- already covers every query this index would serve).  Keeping them during
-- the UPDATE means maintaining two B-trees instead of one.

ALTER TABLE price_bars DROP CONSTRAINT price_bars_pkey;
DROP INDEX IF EXISTS idx_price_bars_time;
UPDATE price_bars SET timestamp = timestamp + interval '1 minute';
ALTER TABLE price_bars ADD CONSTRAINT price_bars_pkey PRIMARY KEY (exchange, symbol, timestamp);

ALTER TABLE orderbook_bars DROP CONSTRAINT orderbook_bars_pkey;
DROP INDEX IF EXISTS idx_orderbook_bars_time;
UPDATE orderbook_bars SET timestamp = timestamp + interval '1 minute';
ALTER TABLE orderbook_bars ADD CONSTRAINT orderbook_bars_pkey PRIMARY KEY (exchange, symbol, timestamp);

COMMIT;

-- Note: AFTER running this migration, all timestamp columns store the CLOSE time
-- of the bar interval instead of the OPEN time.
-- For a 1m bar covering [10:00, 10:01), timestamp = 10:01 (previously 10:00).
