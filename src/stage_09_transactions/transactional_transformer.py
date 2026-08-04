import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")

def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "transactional-transformer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed"
        }
    )

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "transactional.id": "transactional-transformer-instance-1",
            "enable.idempotence": True
        }
    )

    producer.init_transactions()
    consumer.subscribe(["orders.raw.v1"])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            try:
                producer.begin_transaction()

                event = json.loads(message.value().decode("utf-8"))
                event["transactional_transform"] = True
                producer.produce(
                    "orders.validated.v1",
                    key=message.key(),
                    value=json.dumps(event).encode("utf-8")
                )

                offsets = consumer.position(consumer.assignment())
                producer.send_offsets_to_transaction(
                    offsets,
                    consumer.consumer_group_metadata()
                )
                producer.commit_transaction()

                print(
                    f"COMMITTED partition={message.partition()} "
                    f"offset={message.offset()}"
                )
            except Exception:
                producer.abort_transaction()
                raise
    finally:
        consumer.close()

if __name__ == "__main__":
    main()