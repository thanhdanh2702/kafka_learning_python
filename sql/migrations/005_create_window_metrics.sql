CREATE TABLE IF NOT EXISTS five_minute_order_metrics (
    window_start  TIMESTAMPTZ   PRIMARY KEY,
    order_count   BIGINT        NOT NULL CHECK (order_count >= 0),
    revenue       NUMERIC(18, 2) NOT NULL CHECK (revenue >= 0)
);
