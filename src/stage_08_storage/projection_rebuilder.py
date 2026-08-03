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
GROUP_ID = os.getenv(
    "KAFKA_REBUILD_GROUP_ID",
    "postgres-order-rebuild-v1",
)
TOPIC = "orders.validated.v1"
running = True


def stop(_signal_number, _frame):
    global running
    running = False


def ensure_projection_table(connection):
    with connection.transaction():
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_rebuild (
                order_id TEXT PRIMARY KEY,
                event_id UUID NOT NULL UNIQUE,
                customer_id TEXT NOT NULL,
                amount NUMERIC(18, 2) NOT NULL,
                currency CHAR(3) NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                rebuilt_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def upsert_order(connection, event):
    try:
        event_id = str(UUID(event["event_id"]))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("invalid_event_id") from error

    try:
        amount = Decimal(str(event["amount"]))
    except InvalidOperation as error:
        raise ValueError("invalid_amount") from error
    if amount <= 0:
        raise ValueError("amount_must_be_positive")

    occurred_at = datetime.fromisoformat(event["occurred_at"])

    with connection.transaction():
        connection.execute(
            """
            INSERT INTO orders_rebuild (
                order_id,
                event_id,
                customer_id,
                amount,
                currency,
                occurred_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                event["order_id"],
                event_id,
                event["customer_id"],
                amount,
                event["currency"],
                occurred_at,
            ),
        )


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
        ensure_projection_table(connection)

        try:
            while running:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())

                event = None
                try:
                    event = json.loads(message.value().decode("utf-8"))
                    upsert_order(connection, event)
                    result = "REBUILT"
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
                    result = f"SKIPPED reason={error}"

                consumer.commit(message=message, asynchronous=False)

                order_id = event.get("order_id") if isinstance(event, dict) else None
                print(
                    f"{result} order_id={order_id} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
