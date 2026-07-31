CREATE TABLE IF NOT EXISTS  daily_order_metrics (
    metric_date     DATE            PRIMARY KEY,
    order_count     BIGINT          NOT NULL,
    revenue         NUMERIC(18, 2)  NOT NULL
)
