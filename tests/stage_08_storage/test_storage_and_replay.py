import importlib
import json
import sys
import types
import unittest
from contextlib import nullcontext
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID


def load_module(name):
    kafka = types.ModuleType("confluent_kafka")
    kafka.Consumer = object
    kafka.Producer = object
    psycopg = types.ModuleType("psycopg")
    psycopg.connect = MagicMock()

    with (
        patch.dict(
            sys.modules,
            {"confluent_kafka": kafka, "psycopg": psycopg},
        ),
        patch.dict("os.environ", {"POSTGRES_DSN": "postgresql://test"}),
    ):
        sys.modules.pop(name, None)
        return importlib.import_module(name)


class FakeConnection:
    def __init__(self):
        self.calls = []
        self.transaction_count = 0

    def transaction(self):
        self.transaction_count += 1
        return nullcontext()

    def execute(self, query, params=()):
        self.calls.append((" ".join(query.split()), params))


class StateProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state_producer = load_module("src.stage_08_storage.state_producer")

    def test_build_state_event_uses_stage_7_compatible_schema(self):
        event = self.state_producer.build_state_event(
            "LAB-ORD-0001",
            "VALIDATED",
        )

        UUID(event["event_id"])
        self.assertEqual(event["event_type"], "OrderStateChanged")
        self.assertEqual(event["event_version"], 1)
        self.assertEqual(event["order_id"], "LAB-ORD-0001")
        self.assertEqual(event["status"], "VALIDATED")
        self.assertIsNotNone(datetime.fromisoformat(event["occurred_at"]).tzinfo)

    def test_publish_state_uses_order_id_as_key(self):
        producer = MagicMock()

        self.state_producer.publish_state(
            producer,
            "LAB-ORD-0001",
            "STORED",
        )

        message = producer.produce.call_args.kwargs
        payload = json.loads(message["value"])
        self.assertEqual(message["topic"], "orders.state.v1")
        self.assertEqual(message["key"], b"LAB-ORD-0001")
        self.assertEqual(payload["order_id"], "LAB-ORD-0001")
        self.assertEqual(payload["status"], "STORED")
        self.assertIs(message["on_delivery"], self.state_producer.delivery_report)

    def test_publish_tombstone_sends_a_null_value(self):
        producer = MagicMock()

        self.state_producer.publish_tombstone(producer, "LAB-ORD-0002")

        message = producer.produce.call_args.kwargs
        self.assertEqual(message["key"], b"LAB-ORD-0002")
        self.assertIsNone(message["value"])

    def test_main_sends_state_history_and_tombstone(self):
        producer = MagicMock()
        producer.flush.return_value = 0

        with patch.object(
            self.state_producer,
            "Producer",
            return_value=producer,
        ):
            self.state_producer.main()

        self.assertEqual(producer.produce.call_count, 5)
        producer.flush.assert_called_once_with(10)

        values = [call.kwargs["value"] for call in producer.produce.call_args_list]
        states = [json.loads(value)["status"] for value in values[:-1]]
        self.assertEqual(states, ["CREATED", "VALIDATED", "STORED", "CREATED"])
        self.assertIsNone(values[-1])


class ProjectionRebuilderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rebuilder = load_module(
            "src.stage_08_storage.projection_rebuilder"
        )

    def event(self):
        return {
            "event_id": "7a83c308-5c59-4e23-a189-267a8de34101",
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": "2026-08-03T10:00:00+07:00",
            "order_id": "ORD-0100",
            "customer_id": "CUS-100",
            "amount": 250000,
            "currency": "VND",
        }

    def test_ensure_projection_table_creates_orders_rebuild(self):
        connection = FakeConnection()

        self.rebuilder.ensure_projection_table(connection)

        self.assertEqual(connection.transaction_count, 1)
        create_query = connection.calls[0][0]
        self.assertIn("CREATE TABLE IF NOT EXISTS orders_rebuild", create_query)
        self.assertIn("event_id UUID NOT NULL UNIQUE", create_query)

    def test_upsert_order_rebuilds_complete_order_projection(self):
        connection = FakeConnection()

        self.rebuilder.upsert_order(connection, self.event())

        self.assertEqual(connection.transaction_count, 1)
        query, params = connection.calls[0]
        self.assertIn("INSERT INTO orders_rebuild", query)
        self.assertIn("ON CONFLICT DO NOTHING", query)
        self.assertNotIn("DO UPDATE", query)
        self.assertEqual(params[0], "ORD-0100")
        self.assertEqual(params[1], "7a83c308-5c59-4e23-a189-267a8de34101")
        self.assertEqual(params[2], "CUS-100")
        self.assertEqual(params[3], Decimal("250000"))
        self.assertEqual(params[4], "VND")
        self.assertEqual(params[5], datetime.fromisoformat(self.event()["occurred_at"]))

    def test_upsert_order_rejects_an_invalid_event_id(self):
        connection = FakeConnection()
        event = self.event()
        event["event_id"] = "evt-001"

        with self.assertRaisesRegex(ValueError, "invalid_event_id"):
            self.rebuilder.upsert_order(connection, event)

        self.assertEqual(connection.transaction_count, 0)
        self.assertEqual(connection.calls, [])

    def test_upsert_order_rejects_a_non_positive_amount(self):
        connection = FakeConnection()
        event = self.event()
        event["amount"] = 0

        with self.assertRaisesRegex(ValueError, "amount_must_be_positive"):
            self.rebuilder.upsert_order(connection, event)

        self.assertEqual(connection.calls, [])

    def test_upsert_order_rejects_a_non_numeric_amount(self):
        connection = FakeConnection()
        event = self.event()
        event["amount"] = "not-a-number"

        with self.assertRaisesRegex(ValueError, "invalid_amount"):
            self.rebuilder.upsert_order(connection, event)

        self.assertEqual(connection.calls, [])

    def test_replay_uses_a_separate_consumer_group(self):
        self.assertEqual(self.rebuilder.TOPIC, "orders.validated.v1")
        self.assertNotEqual(
            self.rebuilder.GROUP_ID,
            "postgres-order-sink-v1",
        )


if __name__ == "__main__":
    unittest.main()
