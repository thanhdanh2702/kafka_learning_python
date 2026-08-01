import importlib
import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch


PARTITION_EOF = -191


def load_consumer_module():
    """Import Stage 3 without requiring confluent-kafka on the host."""
    kafka_stub = types.ModuleType("confluent_kafka")
    kafka_stub.Consumer = object
    kafka_stub.KafkaError = type(
        "KafkaError",
        (),
        {"_PARTITION_EOF": PARTITION_EOF},
    )

    with patch.dict(sys.modules, {"confluent_kafka": kafka_stub}):
        sys.modules.pop("src.stage_03_consumer.order_consumer", None)
        return importlib.import_module("src.stage_03_consumer.order_consumer")


class FakeError:
    def __init__(self, code, text):
        self._code = code
        self._text = text

    def code(self):
        return self._code

    def __str__(self):
        return self._text


class FakeMessage:
    def __init__(self, value=None, partition=1, offset=15, error=None):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class OrderConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.order_consumer = load_consumer_module()

    def setUp(self):
        self.order_consumer.running = True

    def run_main_with(self, messages):
        consumer = MagicMock()
        pending = iter(messages)

        def poll(_timeout):
            try:
                return next(pending)
            except StopIteration:
                self.order_consumer.running = False
                return None

        consumer.poll.side_effect = poll

        with (
            patch.object(
                self.order_consumer,
                "Consumer",
                return_value=consumer,
            ) as factory,
            patch.object(self.order_consumer.signal, "signal"),
        ):
            self.order_consumer.main()

        return consumer, factory

    def test_stop_ends_the_poll_loop(self):
        self.order_consumer.stop(None, None)

        self.assertFalse(self.order_consumer.running)

    def test_main_processes_then_commits_a_valid_message(self):
        event = {"order_id": "ORD-0100", "amount": 250000}
        message = FakeMessage(json.dumps(event).encode("utf-8"))
        output = StringIO()

        with redirect_stdout(output):
            consumer, factory = self.run_main_with([message])

        config = factory.call_args.args[0]
        self.assertEqual(config["group.id"], "raw-order-printer-v1")
        self.assertEqual(config["auto.offset.reset"], "earliest")
        self.assertFalse(config["enable.auto.commit"])
        consumer.subscribe.assert_called_once_with(["orders.raw.v1"])
        consumer.commit.assert_called_once_with(
            message=message,
            asynchronous=False,
        )
        consumer.close.assert_called_once()
        self.assertIn(
            "PROCESS order_id=ORD-0100 partition=1 offset=15",
            output.getvalue(),
        )

    def test_partition_eof_is_skipped_without_commit(self):
        eof = FakeMessage(error=FakeError(PARTITION_EOF, "partition EOF"))

        consumer, _factory = self.run_main_with([eof])

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    def test_broker_error_is_raised_and_consumer_is_closed(self):
        error_message = FakeMessage(error=FakeError(10, "broker error"))
        consumer = MagicMock()
        consumer.poll.return_value = error_message

        with (
            patch.object(self.order_consumer, "Consumer", return_value=consumer),
            patch.object(self.order_consumer.signal, "signal"),
            self.assertRaisesRegex(RuntimeError, "broker error"),
        ):
            self.order_consumer.main()

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    def test_invalid_json_is_not_committed(self):
        message = FakeMessage(b"not-json")
        consumer = MagicMock()
        consumer.poll.return_value = message

        with (
            patch.object(self.order_consumer, "Consumer", return_value=consumer),
            patch.object(self.order_consumer.signal, "signal"),
            self.assertRaises(json.JSONDecodeError),
        ):
            self.order_consumer.main()

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
