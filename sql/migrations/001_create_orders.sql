CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT            PRIMARY KEY,
    event_id        UUID            NOT NULL    UNIQUE,
    customer_id     TEXT            NOT NULL,
    amount          NUMERIC(18, 2)  NOT NULL    CHECK (amount > 0),
    currency        CHAR(3)         NOT NULL,
    occurred_at     TIMESTAMP       NOT NULL,
    ingested_at     TIMESTAMP       NOT NULL    DEFAULT NOW()
)
