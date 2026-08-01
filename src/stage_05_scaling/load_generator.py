import json
import os
import sys

from confluent_kafka import Producer
from src.stage_02_producer.order_producer import make_order

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.raw.v1"
TOTAL_RECORDS = 100


def choose_key(number, mode):
    if mode == "same-key":
        return "ORD-HOT"
    if mode == "hot-key" and number <= 90:
        return "VIP-CUSTOMER"

    return f"ORD-{number:04d}"


def delivery_report(error, message):
    if error:
        print(f"FAILED: {error}")
        return

    print(
        f"DELIVERED key={message.key().decode('utf-8')} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "many-keys"

    if mode not in {"same-key", "hot-key", "many-keys"}:
        raise ValueError("Mode: same-key, many-keys, hot-key")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    for number in range(1, TOTAL_RECORDS + 1):
        key = choose_key(number, mode)
        event = make_order(number)
        event["order_id"] = key
        event["sequence"] = number

        producer.produce(
            topic=TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)

    if producer.flush(10) != 0:
        raise RuntimeError("Van con message chua gui xong")


if __name__ == "__main__":
    main()
