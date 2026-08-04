import importlib
import sys
import types
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch


def load_window_module():
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
        sys.modules.pop("src.stage_09_transactions.window_aggregator", None)
        return importlib.import_module(
            "src.stage_09_transactions.window_aggregator"
        )


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, rows=()):
        self.rows = iter(rows)
        self.calls = []
        self.transaction_count = 0

    def transaction(self):
        self.transaction_count += 1
        return nullcontext()

    def execute(self, query, params=()):
        self.calls.append((" ".join(query.split()), params))
        return FakeCursor(next(self.rows, None))


class WindowAggregatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.aggregator = load_window_module()

    def event(self, occurred_at="2026-08-04T09:04:55+00:00"):
        return {
            "event_id": "7a83c308-5c59-4e23-a189-267a8de34101",
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": occurred_at,
            "order_id": "ORD-0100",
            "customer_id": "CUS-100",
            "amount": 250000,
            "currency": "VND",
        }

    def test_five_minute_window_rounds_down_to_window_start(self):
        window = self.aggregator.five_minute_window(
            "2026-08-04T09:04:55+00:00"
        )

        self.assertEqual(
            window,
            datetime(2026, 8, 4, 9, 0, tzinfo=UTC),
        )

    def test_window_start_preserves_timezone_for_late_event(self):
        window = self.aggregator.five_minute_window(
            "2026-08-04T09:13:01+07:00"
        )

        self.assertEqual(window.minute, 10)
        self.assertEqual(window.utcoffset().total_seconds(), 7 * 3600)

    def test_new_event_updates_the_correct_window(self):
        connection = FakeConnection(rows=[("event",)])

        result = self.aggregator.aggregate_event(connection, self.event())

        self.assertEqual(result, "AGGREGATED")
        self.assertEqual(connection.transaction_count, 1)
        self.assertEqual(len(connection.calls), 2)
        self.assertIn("INSERT INTO processed_events", connection.calls[0][0])
        metric_query, metric_params = connection.calls[1]
        self.assertIn("INSERT INTO five_minute_order_metrics", metric_query)
        self.assertEqual(
            metric_params[0],
            datetime(2026, 8, 4, 9, 0, tzinfo=UTC),
        )
        self.assertEqual(metric_params[1:], (Decimal("250000"),))

    def test_late_event_updates_its_original_event_time_window(self):
        connection = FakeConnection(rows=[("event",)])

        result = self.aggregator.aggregate_event(
            connection,
            self.event("2026-08-04T09:03:00+00:00"),
        )

        self.assertEqual(result, "AGGREGATED")
        self.assertEqual(
            connection.calls[1][1][0],
            datetime(2026, 8, 4, 9, 0, tzinfo=UTC),
        )

    def test_duplicate_event_does_not_increment_metric(self):
        connection = FakeConnection(rows=[None])

        result = self.aggregator.aggregate_event(connection, self.event())

        self.assertEqual(result, "DUPLICATE")
        self.assertEqual(len(connection.calls), 1)

    def test_non_positive_amount_is_rejected_before_database_write(self):
        connection = FakeConnection()
        event = self.event()
        event["amount"] = 0

        with self.assertRaisesRegex(ValueError, "amount_must_be_positive"):
            self.aggregator.aggregate_event(connection, event)

        self.assertEqual(connection.calls, [])

    def test_aggregator_uses_a_separate_consumer_group_and_validated_topic(self):
        self.assertEqual(self.aggregator.TOPIC, "orders.validated.v1")
        self.assertNotEqual(
            self.aggregator.GROUP_ID,
            "postgres-order-sink-v1",
        )


if __name__ == "__main__":
    unittest.main()
