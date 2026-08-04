import json
import os
import signal
from datetime import datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID

import psycopg
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
POSTGRES_DSN = os.environ["POSTGRES_DSN"]
GROUP_ID = os.getenv("KAFKA_WINDOW_GROUP_ID", "window-aggregator-v1")
TOPIC = "orders.validated.v1"
running = True


def stop(_signal_number, _frame):
    global running
    running = False


def five_minute_window(occurred_at_text):
    timestamp = datetime.fromisoformat(occurred_at_text)
    if timestamp.tzinfo is None:
        raise ValueError("occurred_at_requires_timezone")

    minute = timestamp.minute - (timestamp.minute % 5)
    return timestamp.replace(minute=minute, second=0, microsecond=0)


def aggregate_event(connection, event):
    try:
        event_id = str(UUID(event["event_id"]))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("invalid_event_id") from error

    try:
        amount = Decimal(str(event["amount"]))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("invalid_amount") from error

    if amount <= 0:
        raise ValueError("amount_must_be_positive")

    window_start = five_minute_window(event["occurred_at"])

    with connection.transaction():
        inserted = connection.execute(
            """
            INSERT INTO processed_events (consumer_group, event_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (GROUP_ID, event_id),
        ).fetchone()

        if inserted is None:
            return "DUPLICATE"

        connection.execute(
            """
            INSERT INTO five_minute_order_metrics (
                window_start,
                order_count,
                revenue
            )
            VALUES (%s, 1, %s)
            ON CONFLICT (window_start)
            DO UPDATE SET
                order_count = five_minute_order_metrics.order_count + 1,
                revenue = five_minute_order_metrics.revenue + EXCLUDED.revenue
            """,
            (window_start, amount),
        )

    return "AGGREGATED"


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
                result = aggregate_event(connection, event)

                # Commit the Kafka offset only after the DB transaction succeeds.
                consumer.commit(message=message, asynchronous=False)
                print(
                    f"{result} window_event_id={event['event_id']} "
                    f"partition={message.partition()} offset={message.offset()}"
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
