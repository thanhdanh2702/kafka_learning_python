import json
import os
from datetime import datetime
from uuid import UUID

from confluent_kafka import Consumer, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
INPUT_TOPIC = "orders.raw.v1"
OUTPUT_TOPIC = "orders.validated.v1"
DLQ_TOPIC = "orders.dlq.v1"

REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "order_id",
    "customer_id",
    "amount",
    "currency",
}


def validate(event, message_key):
    if not isinstance(event, dict):
        raise ValueError("event_must_be_a_json_object")

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"missing_fields={sorted(missing)}")

    if event["event_type"] != "OrderCreated":
        raise ValueError("unsupported_event_type")

    if type(event["event_version"]) is not int or event["event_version"] != 1:
        raise ValueError("unsupported_event_version")

    if not isinstance(event["event_id"], str):
        raise ValueError("event_id_must_be_a_string")
    try:
        UUID(event["event_id"])
    except ValueError as error:
        raise ValueError("event_id_must_be_a_valid_uuid") from error

    if not isinstance(event["order_id"], str) or not event["order_id"].strip():
        raise ValueError("invalid_order_id")
    if message_key != event["order_id"]:
        raise ValueError("message_key_must_equal_order_id")

    if not isinstance(event["customer_id"], str) or not event["customer_id"].strip():
        raise ValueError("invalid_customer_id")

    if (
        isinstance(event["amount"], bool)
        or not isinstance(event["amount"], (int, float))
        or event["amount"] <= 0
    ):
        raise ValueError("amount_must_be_greater_than_zero")

    if event["currency"] != "VND":
        raise ValueError("unsupported_currency")

    if not isinstance(event["occurred_at"], str):
        raise ValueError("occurred_at_must_be_a_string")
    try:
        occurred_at = datetime.fromisoformat(event["occurred_at"])
    except ValueError as error:
        raise ValueError("occurred_at_must_be_iso_datetime") from error
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("occurred_at_must_include_timezone")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "order-validator-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer.subscribe([INPUT_TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())

            raw_text = message.value().decode("utf-8")
            message_key = message.key().decode("utf-8") if message.key() else None

            try:
                event = json.loads(raw_text)
                validate(event, message_key)
                target_topic, output, result = OUTPUT_TOPIC, event, "VALID"
            except (json.JSONDecodeError, ValueError, TypeError) as error:
                target_topic, result = DLQ_TOPIC, "DLQ"
                output = {
                    "original_topic": message.topic(),
                    "original_partition": message.partition(),
                    "original_offset": message.offset(),
                    "original_key": message_key,
                    "raw_value": raw_text,
                    "error": str(error),
                }

            producer.produce(
                topic=target_topic,
                key=message.key(),
                value=json.dumps(output).encode("utf-8"),
            )

            if producer.flush(10) != 0:
                raise RuntimeError("Output event chua gui xong")

            # Chỉ commit input sau khi output đã gửi thành công.
            consumer.commit(message=message, asynchronous=False)
            print(
                f"{result} partition={message.partition()} "
                f"offset={message.offset()}"
            )
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
