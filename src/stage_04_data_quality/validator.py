import json
import os

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
    if event["event_type"] != "OrderCreated" or event["event_version"] != 1:
        raise ValueError("unsupported_event")
    if message_key != event["order_id"]:
        raise ValueError("message_key_must_equal_order_id")
    if not isinstance(event["amount"], (int, float)) or event["amount"] < 0:
        raise ValueError("invalid_amount")
    if event["currency"] != "VND":
        raise ValueError("unsupported_currency")


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
