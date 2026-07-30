CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID        PRIMARY KEY,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMP   NOT NULL    DEFAULT NOW(),
    published_at    TIMESTAMP
)
