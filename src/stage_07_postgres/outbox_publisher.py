import json
import os

import psycopg
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker:19092",
)
POSTGRES_DSN = os.environ["POSTGRES_DSN"]
TOPIC = "orders.state.v1"
BATCH_SIZE = 100

def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    with psycopg.connect(
        POSTGRES_DSN,
        autocommit=True,
    ) as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                aggregate_id,
                event_type,
                payload
            FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT %s
            """,
            (BATCH_SIZE,),
        ).fetchall()

        for event_id, aggregate_id, event_type, payload in rows:
            producer.produce(
                topic=TOPIC,
                key=aggregate_id.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            )

            if producer.flush(10) != 0:
                raise RuntimeError(
                    f"Outbox event chua gui xong: {event_id}"
                )

            connection.execute(
                """
                UPDATE outbox_events
                SET published_at = NOW()
                WHERE id = %s
                  AND published_at IS NULL
                """,
                (event_id,),
            )

            print(
                f"PUBLISHED event_id={event_id} "
                f"event_type={event_type} "
                f"key={aggregate_id}"
            )

    print(f"DONE published={len(rows)}")


if __name__ == "__main__":
    main()