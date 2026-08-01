import importlib
import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from unittest.mock import MagicMock, patch
from uuid import UUID


def load_producer_module():
    """Import Stage 2 without requiring confluent-kafka on the host."""
    kafka_stub = types.ModuleType("confluent_kafka")
    kafka_stub.Producer = object

    with patch.dict(sys.modules, {"confluent_kafka": kafka_stub}):
        sys.modules.pop("src.stage_02_producer.order_producer", None)
        return importlib.import_module("src.stage_02_producer.order_producer")


class DeliveryMessage:
    def topic(self):
        return "orders.raw.v1"

    def partition(self):
        return 2

    def offset(self):
        return 15

    def key(self):
        return b"ORD-0001"


class OrderProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.order_producer = load_producer_module()

    def test_make_order_creates_a_valid_order_event(self):
        event = self.order_producer.make_order(7)

        self.assertEqual(event["order_id"], "ORD-0007")
        self.assertEqual(event["event_type"], "OrderCreated")
        self.assertEqual(event["event_version"], 1)
        self.assertEqual(event["currency"], "VND")
        self.assertIn(event["amount"], [120000, 250000, 500000, 750000])
        self.assertRegex(event["customer_id"], r"^CUS-10[0-5]$")
        self.assertIsNotNone(UUID(event["event_id"]))
        self.assertIsNotNone(datetime.fromisoformat(event["occurred_at"]).tzinfo)

    def test_delivery_report_prints_success_metadata(self):
        output = StringIO()

        with redirect_stdout(output):
            self.order_producer.delivery_report(None, DeliveryMessage())

        self.assertIn(
            "DELIVERED topic=orders.raw.v1 partition=2 offset=15 key=ORD-0001",
            output.getvalue(),
        )

    def test_delivery_report_prints_failure(self):
        output = StringIO()

        with redirect_stdout(output):
            self.order_producer.delivery_report("broker unavailable", None)

        self.assertEqual(output.getvalue().strip(), "FAILED: broker unavailable")

    def test_main_sends_ten_keyed_json_messages(self):
        producer = MagicMock()
        producer.flush.return_value = 0

        with (
            patch.object(self.order_producer, "Producer", return_value=producer) as factory,
            patch.object(self.order_producer.time, "sleep"),
        ):
            self.order_producer.main()

        config = factory.call_args.args[0]
        self.assertEqual(config["acks"], "all")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual(producer.produce.call_count, 10)
        self.assertEqual(producer.poll.call_count, 10)
        producer.flush.assert_called_once_with(10)

        for number, call in enumerate(producer.produce.call_args_list, start=1):
            message = call.kwargs
            event = json.loads(message["value"].decode("utf-8"))
            expected_key = f"ORD-{number:04d}"

            self.assertEqual(message["topic"], "orders.raw.v1")
            self.assertEqual(message["key"].decode("utf-8"), expected_key)
            self.assertEqual(event["order_id"], expected_key)
            self.assertIs(message["on_delivery"], self.order_producer.delivery_report)

    def test_main_raises_when_messages_remain_after_flush(self):
        producer = MagicMock()
        producer.flush.return_value = 2

        with (
            patch.object(self.order_producer, "Producer", return_value=producer),
            patch.object(self.order_producer.time, "sleep"),
            self.assertRaisesRegex(RuntimeError, "Con 2 message chua gui xong"),
        ):
            self.order_producer.main()


if __name__ == "__main__":
    unittest.main()
