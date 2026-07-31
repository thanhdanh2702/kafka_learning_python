import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.raw.v1"

def delivery_report(error, message):
    if error is not None:
        print(f"FAILED: {error}")
        return
    print(
        "DELIVERED "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()} " 

        f"key={message.key().decode('utf-8')}"
    )

def make_order(number):

    order_id = f"ORD-{number:04d}"

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "event_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": order_id,
        "customer_id": f"CUS-{random.randint(100,105)}",
        "amount": random.choice([120000, 250000, 500000, 750000]),
        "currency": "VND"
    }

def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "client.id": "order-generator",
            "acks": "all",
            "enable.idempotence": True
        }
    )

    for number in range(1, 11):
        event = make_order(number)
        value = json.dumps(event).encode("utf-8")
        key = event["order_id"].encode("utf-8")

        producer.produce(
            topic= TOPIC,
            key=key,
            value=value,
            on_delivery=delivery_report
        )

        producer.poll(0)
        time.sleep(0.2)

    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"Con {remaining} message chua gui xong")


if __name__ == "__main__":
    main()