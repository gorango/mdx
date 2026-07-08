-- Add NOTIFY trigger for real-time bar updates
-- Run with: just sql-notify-trigger

-- Trigger function for orderbook_bars
CREATE OR REPLACE FUNCTION notify_orderbook_bar()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'orderbook_bar_insert',
        json_build_object(
            'exchange', NEW.exchange,
            'symbol', NEW.symbol,
            'timestamp', NEW.timestamp
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for orderbook_bars
DROP TRIGGER IF EXISTS orderbook_bar_insert_trigger ON orderbook_bars;
CREATE TRIGGER orderbook_bar_insert_trigger
AFTER INSERT ON orderbook_bars
FOR EACH ROW
EXECUTE FUNCTION notify_orderbook_bar();

-- Trigger function for price_bars
CREATE OR REPLACE FUNCTION notify_price_bar()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'price_bar_insert',
        json_build_object(
            'exchange', NEW.exchange,
            'symbol', NEW.symbol,
            'timestamp', NEW.timestamp
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for price_bars
DROP TRIGGER IF EXISTS price_bar_insert_trigger ON price_bars;
CREATE TRIGGER price_bar_insert_trigger
AFTER INSERT ON price_bars
FOR EACH ROW
EXECUTE FUNCTION notify_price_bar();
