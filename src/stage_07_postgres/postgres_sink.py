import json
import os
import signal
from datetime import datetime, UTC
import uuid
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
POSTGRES_DSN = os.environ["POSTGRES_DSN"]
GROUP_ID = "postgres-order-sink-v1"
TOPIC = "orders.validated.v1"
running = True


def stop(_signal_number, _frame):
    global running
    running = False


def store_event(connection, event):
    occurred_at = datetime.fromisoformat(event["occurred_at"])
    metric_date = occurred_at.date()

    with connection.transaction():
        inserted = connection.execute(
            """
            INSERT INTO processed_events (consumer_group, event_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (GROUP_ID, event["event_id"]),
        ).fetchone()

        if inserted is None:
            return "DUPLICATE"

        inserted_order = connection.execute(
            """
            INSERT INTO orders (
                order_id,
                event_id,
                customer_id,
                amount,
                currency,
                occurred_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            RETURNING order_id
            """,
            (
                event["order_id"],
                event["event_id"],
                event["customer_id"],
                Decimal(str(event["amount"])),
                event["currency"],
                occurred_at
            ),
        ).fetchone()

        if inserted_order is None:
            return "DUPLICATE_ORDER"

        connection.execute(
            """
            INSERT INTO daily_order_metrics (
                metric_date,
                order_count,
                revenue
            )
            VALUES (%s, 1, %s)
            ON CONFLICT (metric_date)
            DO UPDATE SET
                order_count = daily_order_metrics.order_count + 1,
                revenue = daily_order_metrics.revenue + EXCLUDED.revenue
            """,
            (metric_date, Decimal(str(event["amount"]))),
        )

        outbox_id = uuid.uuid4()

        state_event = {
            "event_id": str(outbox_id),
            "event_type": "OrderStored",
            "event_version": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "order_id": event["order_id"],
            "status": "STORED"
        }

        connection.execute(
            """
            INSERT INTO outbox_events (
                id,
                aggregate_id,
                event_type,
                payload
            )
            VALUES(%s, %s, %s, %s)
            """,
            (
                outbox_id,
                event["order_id"],
                "OrderStored",
                Jsonb(state_event)
            )
        )

    return "STORED"


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([TOPIC])

    with psycopg.connect(POSTGRES_DSN) as connection:
        try:
            while running:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())

                event = json.loads(message.value().decode("utf-8"))
                result = store_event(connection, event)

                consumer.commit(message=message, asynchronous=False)
                print(
                    f"{result} event_id={event['event_id']} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
