import importlib
import json
import os
import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch


def load_generator_module():
    kafka_stub = types.ModuleType("confluent_kafka")
    kafka_stub.Producer = object

    with (
        patch.dict(sys.modules, {"confluent_kafka": kafka_stub}),
        patch.dict(os.environ, {}, clear=True),
    ):
        sys.modules.pop("src.stage_02_producer.order_producer", None)
        sys.modules.pop("src.stage_05_scaling.load_generator", None)
        return importlib.import_module("src.stage_05_scaling.load_generator")


class DeliveryMessage:
    def key(self):
        return b"ORD-0001"

    def partition(self):
        return 2

    def offset(self):
        return 15


class LoadGeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.load_generator = load_generator_module()

    def test_uses_the_project_broker_and_raw_topic(self):
        self.assertEqual(self.load_generator.BOOTSTRAP_SERVERS, "broker:19092")
        self.assertEqual(self.load_generator.TOPIC, "orders.raw.v1")

    def test_choose_key_supports_all_three_modes(self):
        choose_key = self.load_generator.choose_key

        self.assertEqual(choose_key(1, "same-key"), "ORD-HOT")
        self.assertEqual(choose_key(100, "same-key"), "ORD-HOT")
        self.assertEqual(choose_key(90, "hot-key"), "VIP-CUSTOMER")
        self.assertEqual(choose_key(91, "hot-key"), "ORD-0091")
        self.assertEqual(choose_key(7, "many-keys"), "ORD-0007")

    def test_delivery_report_prints_success_and_failure(self):
        success_output = StringIO()
        failure_output = StringIO()

        with redirect_stdout(success_output):
            self.load_generator.delivery_report(None, DeliveryMessage())
        with redirect_stdout(failure_output):
            self.load_generator.delivery_report("broker unavailable", None)

        self.assertIn("DELIVERED key=ORD-0001", success_output.getvalue())
        self.assertEqual(
            failure_output.getvalue().strip(),
            "FAILED: broker unavailable",
        )

    def test_main_sends_one_hundred_messages_and_flushes(self):
        producer = MagicMock()
        producer.flush.return_value = 0

        def make_order(number):
            return {"order_id": f"ORD-{number:04d}"}

        with (
            patch.object(
                self.load_generator,
                "Producer",
                return_value=producer,
            ) as factory,
            patch.object(self.load_generator, "make_order", side_effect=make_order),
            patch.object(sys, "argv", ["load_generator.py", "many-keys"]),
        ):
            self.load_generator.main()

        config = factory.call_args.args[0]
        self.assertEqual(config["bootstrap.servers"], "broker:19092")
        self.assertEqual(config["acks"], "all")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual(producer.produce.call_count, 100)
        self.assertEqual(producer.poll.call_count, 100)
        producer.poll.assert_called_with(0)
        producer.flush.assert_called_once_with(10)

        first_message = producer.produce.call_args_list[0].kwargs
        first_event = json.loads(first_message["value"].decode("utf-8"))
        self.assertEqual(first_message["topic"], "orders.raw.v1")
        self.assertEqual(first_message["key"], b"ORD-0001")
        self.assertEqual(first_event["order_id"], "ORD-0001")
        self.assertEqual(first_event["sequence"], 1)

    def test_invalid_mode_is_rejected(self):
        with (
            patch.object(sys, "argv", ["load_generator.py", "invalid"]),
            self.assertRaisesRegex(ValueError, "same-key, many-keys, hot-key"),
        ):
            self.load_generator.main()


if __name__ == "__main__":
    unittest.main()
