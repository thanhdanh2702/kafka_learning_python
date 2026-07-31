import json
import os
import signal

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.raw.v1"
running = True

def stop(_signal_number, _frame):
    global running
    running = False

def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "raw-order-printer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
    )
    consumer.subscribe([TOPIC])

    try:
        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            event = json.loads(message.value().decode("utf-8"))

            print(
                f"PROCESS order_id={event['order_id']} "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
        