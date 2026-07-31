CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group  TEXT        NOT NULL,
    event_id        UUID        NOT NULL,
    processed_at    TIMESTAMP   NOT NULL    DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
)
