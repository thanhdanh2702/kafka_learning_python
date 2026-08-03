import json
import os
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.state.v1"


def delivery_report(error, message):
    if error is not None:
        print(f"FAILED: {error}")
        return

    key = message.key().decode("utf-8")
    print(
        f"DELIVERED key={key} "
        f"partition={message.partition()} offset={message.offset()}"
    )


def build_state_event(order_id, status):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderStateChanged",
        "event_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": order_id,
        "status": status,
    }


def publish_state(producer, order_id, status):
    event = build_state_event(order_id, status)
    producer.produce(
        topic=TOPIC,
        key=order_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_report,
    )


def publish_tombstone(producer, order_id):
    producer.produce(
        topic=TOPIC,
        key=order_id.encode("utf-8"),
        value=None,
        on_delivery=delivery_report,
    )


def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    # LAB prefix keeps this compaction experiment separate from Stage 7 orders.
    publish_state(producer, "LAB-ORD-0001", "CREATED")
    publish_state(producer, "LAB-ORD-0001", "VALIDATED")
    publish_state(producer, "LAB-ORD-0001", "STORED")
    publish_state(producer, "LAB-ORD-0002", "CREATED")
    publish_tombstone(producer, "LAB-ORD-0002")

    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"Con {remaining} state message chua gui xong")


if __name__ == "__main__":
    main()
