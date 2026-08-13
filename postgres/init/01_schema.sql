CREATE TABLE IF NOT EXISTS trade_metrics (
    symbol       TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    trade_count  BIGINT           NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION,
    open_price   DOUBLE PRECISION,
    close_price  DOUBLE PRECISION,
    min_price    DOUBLE PRECISION,
    max_price    DOUBLE PRECISION,
    avg_price    DOUBLE PRECISION,
    max_delay_ms BIGINT,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS trade_metrics_window_start_idx
    ON trade_metrics (window_start DESC);

CREATE UNLOGGED TABLE IF NOT EXISTS trade_metrics_staging (
    symbol       TEXT,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    trade_count  BIGINT,
    total_volume DOUBLE PRECISION,
    vwap         DOUBLE PRECISION,
    open_price   DOUBLE PRECISION,
    close_price  DOUBLE PRECISION,
    min_price    DOUBLE PRECISION,
    max_price    DOUBLE PRECISION,
    avg_price    DOUBLE PRECISION,
    max_delay_ms BIGINT
);

CREATE TABLE IF NOT EXISTS trade_quality_events (
    id            BIGSERIAL PRIMARY KEY,
    event_type    TEXT NOT NULL,
    reason        TEXT NOT NULL,
    symbol        TEXT,
    price         DOUBLE PRECISION,
    volume        DOUBLE PRECISION,
    event_time    TIMESTAMPTZ,
    ingest_time   TIMESTAMPTZ,
    delay_ms      BIGINT,
    payload_bytes INTEGER,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS trade_quality_events_recorded_at_idx
    ON trade_quality_events (recorded_at DESC);

CREATE OR REPLACE VIEW trade_metrics_latest AS
SELECT DISTINCT ON (symbol)
    symbol, window_start, window_end, trade_count, total_volume,
    vwap, open_price, close_price, min_price, max_price, updated_at
FROM trade_metrics
ORDER BY symbol, window_start DESC;
