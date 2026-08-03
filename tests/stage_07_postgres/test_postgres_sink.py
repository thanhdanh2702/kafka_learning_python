import importlib
import json
import sys
import types
import unittest
from contextlib import nullcontext, redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch
from uuid import UUID


class FakeJsonb:
    def __init__(self, value):
        self.value = value


def kafka_stub():
    module = types.ModuleType("confluent_kafka")
    module.Consumer = object
    module.Producer = object
    return module


def psycopg_stubs():
    psycopg = types.ModuleType("psycopg")
    psycopg.connect = MagicMock()
    types_module = types.ModuleType("psycopg.types")
    json_module = types.ModuleType("psycopg.types.json")
    json_module.Jsonb = FakeJsonb
    return psycopg, types_module, json_module


def load_module(name):
    psycopg, psycopg_types, psycopg_json = psycopg_stubs()
    stubs = {
        "confluent_kafka": kafka_stub(),
        "psycopg": psycopg,
        "psycopg.types": psycopg_types,
        "psycopg.types.json": psycopg_json,
    }
    with (
        patch.dict(sys.modules, stubs),
        patch.dict("os.environ", {"POSTGRES_DSN": "postgresql://test"}),
    ):
        sys.modules.pop(name, None)
        module = importlib.import_module(name)
    return module


class FakeCursor:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.calls = []
        self.transaction_count = 0

    def transaction(self):
        self.transaction_count += 1
        return nullcontext()

    def execute(self, query, params=()):
        self.calls.append((" ".join(query.split()), params))
        return FakeCursor(next(self.rows, None))


class PostgresSinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sink = load_module("src.stage_07_postgres.postgres_sink")

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

    def test_new_order_writes_metrics_and_outbox_in_one_transaction(self):
        connection = FakeConnection(rows=[("event",), ("order",)])

        result = self.sink.store_event(connection, self.event())

        self.assertEqual(result, "STORED")
        self.assertEqual(connection.transaction_count, 1)
        self.assertEqual(len(connection.calls), 4)
        self.assertIn("INSERT INTO processed_events", connection.calls[0][0])
        self.assertIn("INSERT INTO orders", connection.calls[1][0])
        self.assertIn("INSERT INTO daily_order_metrics", connection.calls[2][0])
        self.assertIn("INSERT INTO outbox_events", connection.calls[3][0])

        outbox_params = connection.calls[3][1]
        UUID(str(outbox_params[0]))
        self.assertEqual(outbox_params[1], "ORD-0100")
        self.assertEqual(outbox_params[2], "OrderStored")
        self.assertEqual(outbox_params[3].value["order_id"], "ORD-0100")
        self.assertEqual(outbox_params[3].value["status"], "STORED")

    def test_duplicate_event_stops_before_order_metrics_and_outbox(self):
        connection = FakeConnection(rows=[None])

        result = self.sink.store_event(connection, self.event())

        self.assertEqual(result, "DUPLICATE")
        self.assertEqual(len(connection.calls), 1)

    def test_duplicate_order_stops_before_metrics_and_outbox(self):
        connection = FakeConnection(rows=[("event",), None])

        result = self.sink.store_event(connection, self.event())

        self.assertEqual(result, "DUPLICATE_ORDER")
        self.assertEqual(len(connection.calls), 2)


class OutboxPublisherTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.publisher = load_module("src.stage_07_postgres.outbox_publisher")

    def run_publisher(self, rows, flush_result=0):
        producer = MagicMock()
        producer.flush.return_value = flush_result
        connection = MagicMock()
        connection.execute.return_value.fetchall.return_value = rows
        self.last_connection = connection
        connection_context = MagicMock()
        connection_context.__enter__.return_value = connection

        with (
            patch.object(self.publisher, "Producer", return_value=producer),
            patch.object(
                self.publisher.psycopg,
                "connect",
                return_value=connection_context,
            ),
        ):
            self.publisher.main()

        return producer, connection

    def test_unpublished_event_is_sent_then_marked_published(self):
        payload = {"order_id": "ORD-0100", "status": "STORED"}
        row = ("event-1", "ORD-0100", "OrderStored", payload)

        producer, connection = self.run_publisher([row])

        message = producer.produce.call_args.kwargs
        self.assertEqual(message["topic"], "orders.state.v1")
        self.assertEqual(message["key"], b"ORD-0100")
        self.assertEqual(json.loads(message["value"]), payload)
        producer.flush.assert_called_once_with(10)
        self.assertEqual(connection.execute.call_count, 2)
        update_query, update_params = connection.execute.call_args.args
        self.assertIn("SET published_at = NOW()", update_query)
        self.assertEqual(update_params, ("event-1",))

    def test_failed_flush_does_not_mark_event_as_published(self):
        row = (
            "event-1",
            "ORD-0100",
            "OrderStored",
            {"order_id": "ORD-0100"},
        )

        with self.assertRaisesRegex(RuntimeError, "event-1"):
            self.run_publisher([row], flush_result=1)

        self.assertEqual(self.last_connection.execute.call_count, 1)

    def test_no_unpublished_rows_sends_nothing(self):
        output = StringIO()

        with redirect_stdout(output):
            producer, connection = self.run_publisher([])

        producer.produce.assert_not_called()
        self.assertEqual(connection.execute.call_count, 1)
        self.assertIn("DONE published=0", output.getvalue())


if __name__ == "__main__":
    unittest.main()
