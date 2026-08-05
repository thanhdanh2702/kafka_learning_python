import json
import importlib
import sys
import types
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch


def load_modules():
    kafka = types.ModuleType("confluent_kafka")
    admin = types.ModuleType("confluent_kafka.admin")
    admin.AdminClient = object
    kafka.admin = admin

    psycopg = types.ModuleType("psycopg")
    psycopg.connect = MagicMock()

    with patch.dict(
        sys.modules,
        {"confluent_kafka": kafka, "confluent_kafka.admin": admin, "psycopg": psycopg},
    ):
        sys.modules.pop("src.stage_10_operations.metrics", None)
        sys.modules.pop("src.stage_10_operations.health_probe", None)
        metrics = importlib.import_module("src.stage_10_operations.metrics")
        health_probe = importlib.import_module(
            "src.stage_10_operations.health_probe"
        )
        return metrics, health_probe


class FakePostgresConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query):
        self.query = query
        return self

    def fetchone(self):
        return (1,)


class OperationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.metrics, cls.health_probe = load_modules()

    def test_build_log_contains_trace_fields_without_payload(self):
        result = self.metrics.build_log(
            service="validator",
            event_id="event-1",
            order_id="ORD-0001",
            topic="orders.raw.v1",
            partition=1,
            offset=42,
            consumer_group="validator-v1",
            processing_time_ms=2.5,
            result="VALID",
            retry_count=0,
        )

        self.assertEqual(result["service"], "validator")
        self.assertEqual(result["offset"], 42)
        self.assertEqual(result["result"], "VALID")
        self.assertNotIn("password", result)
        self.assertNotIn("payload", result)

    def test_metrics_increment_observe_and_snapshot(self):
        metrics = self.metrics.Metrics("validator")

        metrics.increment("records_valid")
        metrics.increment("records_valid", 2)
        metrics.observe("processing_time_ms", 4.5)
        metrics.observe("processing_time_ms", 5.5)

        snapshot = metrics.snapshot()

        self.assertEqual(snapshot["service"], "validator")
        self.assertEqual(snapshot["counters"]["records_valid"], 3)
        self.assertEqual(snapshot["timings"]["processing_time_ms"]["count"], 2)
        self.assertEqual(snapshot["timings"]["processing_time_ms"]["avg"], 5.0)

    def test_metrics_rejects_negative_increment_and_timing(self):
        metrics = self.metrics.Metrics("validator")

        with self.assertRaises(ValueError):
            metrics.increment("records_valid", -1)
        with self.assertRaises(ValueError):
            metrics.observe("processing_time_ms", -0.1)

    def test_health_probe_is_ready_when_kafka_and_postgres_are_ok(self):
        admin_client = MagicMock()
        admin_client.list_topics.return_value.topics = {
            "orders.validated.v1": object()
        }
        postgres_connection = FakePostgresConnection()

        with (
            patch.object(self.health_probe, "AdminClient", return_value=admin_client),
            patch.object(
                self.health_probe.psycopg,
                "connect",
                return_value=postgres_connection,
            ),
        ):
            report = self.health_probe.probe(
                "broker:19092",
                "postgresql://test",
                "orders.validated.v1",
            )

        self.assertEqual(report["status"], "READY")
        self.assertEqual(report["kafka"]["status"], "OK")
        self.assertEqual(report["postgres"]["status"], "OK")

    def test_health_probe_is_not_ready_when_kafka_fails(self):
        with patch.object(
            self.health_probe,
            "AdminClient",
            side_effect=RuntimeError("broker unavailable"),
        ):
            report = self.health_probe.probe(
                "broker:19092",
                "postgresql://test",
                "orders.validated.v1",
            )

        self.assertEqual(report["status"], "NOT_READY")
        self.assertEqual(report["kafka"]["status"], "ERROR")
        self.assertNotIn("postgresql://test", json.dumps(report))

    def test_main_prints_json_and_returns_nonzero_when_not_ready(self):
        with patch.object(
            self.health_probe,
            "probe",
            return_value={"status": "NOT_READY", "kafka": {"status": "ERROR"}},
        ):
            output = StringIO()
            with patch("sys.stdout", output):
                exit_code = self.health_probe.main(
                    [
                        "--bootstrap-servers",
                        "broker:19092",
                        "--postgres-dsn",
                        "postgresql://secret",
                        "--topic",
                        "orders.validated.v1",
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(output.getvalue())["status"], "NOT_READY")
        self.assertNotIn("postgresql://secret", output.getvalue())


if __name__ == "__main__":
    unittest.main()
