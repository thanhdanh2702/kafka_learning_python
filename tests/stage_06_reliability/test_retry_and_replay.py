import importlib
import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch


def load_replay_module():
    kafka_stub = types.ModuleType("confluent_kafka")
    kafka_stub.Consumer = object
    kafka_stub.Producer = object

    with patch.dict(sys.modules, {"confluent_kafka": kafka_stub}):
        sys.modules.pop("src.stage_06_reliability.replay_dlq", None)
        return importlib.import_module("src.stage_06_reliability.replay_dlq")


class FakeMessage:
    def __init__(self, event):
        self._value = json.dumps(event).encode("utf-8")

    def value(self):
        return self._value

    def error(self):
        return None


class ReplayDlqTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.replay_dlq = load_replay_module()

    def run_main(self, event):
        message = FakeMessage(event)
        consumer = MagicMock()
        producer = MagicMock()
        consumer.poll.side_effect = [message, None]
        producer.flush.return_value = 0
        output = StringIO()

        with (
            patch.object(
                self.replay_dlq,
                "Consumer",
                return_value=consumer,
            ) as consumer_factory,
            patch.object(
                self.replay_dlq,
                "Producer",
                return_value=producer,
            ) as producer_factory,
            redirect_stdout(output),
        ):
            self.replay_dlq.main()

        return consumer, producer, consumer_factory, producer_factory, output

    def test_replays_dlq_record_then_commits_it(self):
        event = {
            "original_topic": "orders.raw.v1",
            "original_partition": 1,
            "original_offset": 15,
            "original_key": "ORD-0001",
            "raw_value": '{"order_id":"ORD-0001"}',
        }

        consumer, producer, consumer_factory, producer_factory, output = (
            self.run_main(event)
        )

        consumer_config = consumer_factory.call_args.args[0]
        producer_config = producer_factory.call_args.args[0]
        self.assertEqual(consumer_config["bootstrap.servers"], "broker:19092")
        self.assertEqual(consumer_config["auto.offset.reset"], "earliest")
        self.assertFalse(consumer_config["enable.auto.commit"])
        self.assertEqual(producer_config["bootstrap.servers"], "broker:19092")
        consumer.subscribe.assert_called_once_with(["orders.dlq.v1"])
        producer.produce.assert_called_once_with(
            "orders.raw.v1",
            key=b"ORD-0001",
            value=b'{"order_id":"ORD-0001"}',
        )
        consumer.commit.assert_called_once_with(
            message=unittest.mock.ANY,
            asynchronous=False,
        )
        consumer.close.assert_called_once()
        self.assertIn(
            "REPLAYED original_topic=orders.raw.v1 "
            "original_partition=1 original_offset=15",
            output.getvalue(),
        )
        self.assertIn("DONE replayed=1", output.getvalue())

    def test_missing_original_key_stops_without_replay_or_commit(self):
        event = {
            "original_topic": "orders.raw.v1",
            "original_partition": 0,
            "original_offset": 3,
            "original_key": None,
            "raw_value": "{}",
        }

        consumer, producer, _consumer_factory, _producer_factory, output = (
            self.run_main(event)
        )

        producer.produce.assert_not_called()
        consumer.commit.assert_not_called()
        self.assertIn("STOPPED reason=THIEU_KEY", output.getvalue())
        self.assertIn("DONE replayed=0", output.getvalue())


if __name__ == "__main__":
    unittest.main()
