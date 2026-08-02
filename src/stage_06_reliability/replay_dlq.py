import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
MAX_RECORDS = 10


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "dlq-replayer-lab-v1",
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

    consumer.subscribe(["orders.dlq.v1"])
    replayed = 0

    try:
        while replayed < MAX_RECORDS:
            message = consumer.poll(2.0)
            if message is None:
                break
            if message.error():
                raise RuntimeError(message.error())

            dlq_event = json.loads(message.value().decode("utf-8"))
            original_key = dlq_event["original_key"]
            raw_value = dlq_event["raw_value"]

            if original_key is None:
                print("STOPPED reason=THIEU_KEY")
                break

            producer.produce(
                "orders.raw.v1",
                key=original_key.encode("utf-8"),
                value=raw_value.encode("utf-8"),
            )
            if producer.flush(10) != 0:
                raise RuntimeError("Replay event chua gui xong")

            consumer.commit(message=message, asynchronous=False)
            replayed += 1
            print(
                f"REPLAYED original_topic={dlq_event['original_topic']} "
                f"original_partition={dlq_event['original_partition']} "
                f"original_offset={dlq_event['original_offset']}"
            )

    finally:
        producer.flush(10)
        consumer.close()

    print(f"DONE replayed={replayed}")


if __name__ == "__main__":
    main()
