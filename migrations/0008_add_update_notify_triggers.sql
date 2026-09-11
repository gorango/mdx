-- Canonical home for the writer's notify triggers (complements
-- 0001_add_notify_trigger.sql, which created the AFTER INSERT triggers).
--
-- Fire the SAME pg_notify payload/channel on UPDATE as on INSERT, so
-- streamer revisions of boundary bars (hourly :25 ob-hydrate overwrite,
-- funding/OI/liq column backfills — all UPDATE-path, no INSERT notify)
-- still wake the live_writer's dual-channel tick gate. Reuses the existing
-- notify_price_bar() / notify_orderbook_bar() functions (NEW.* payload), so
-- payload shape and channel names are identical to the insert triggers by
-- construction. Separate trigger names so both coexist on the same tables.
--
-- Idempotent (DROP IF EXISTS) — safe to re-apply. Already applied to the
-- local twain DB 2026-09-11 and verified (UPDATE to a scratch row produced
-- the same notification shape as INSERT; scratch rows removed).
--
-- Apply with:
--   psql "postgresql://postgres:postgres@localhost:5432/twain" \
--     -v ON_ERROR_STOP=1 -f exchanges/migrations/0008_add_update_notify_triggers.sql

-- Trigger for price_bars updates
DROP TRIGGER IF EXISTS price_bar_update_trigger ON price_bars;
CREATE TRIGGER price_bar_update_trigger
AFTER UPDATE ON price_bars
FOR EACH ROW
EXECUTE FUNCTION notify_price_bar();

-- Trigger for orderbook_bars updates
DROP TRIGGER IF EXISTS orderbook_bar_update_trigger ON orderbook_bars;
CREATE TRIGGER orderbook_bar_update_trigger
AFTER UPDATE ON orderbook_bars
FOR EACH ROW
EXECUTE FUNCTION notify_orderbook_bar();
