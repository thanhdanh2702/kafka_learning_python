import importlib
import sys
import types
import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4


def load_validator_module():
    """Import the validation logic without requiring Kafka for unit tests."""
    kafka_stub = types.ModuleType("confluent_kafka")
    kafka_stub.Consumer = object
    kafka_stub.Producer = object
    kafka_stub.KafkaError = type("KafkaError", (), {"_PARTITION_EOF": -191})

    with patch.dict(sys.modules, {"confluent_kafka": kafka_stub}):
        sys.modules.pop("src.stage_04_data_quality.validator", None)
        return importlib.import_module("src.stage_04_data_quality.validator")


class ValidateOrderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validator = load_validator_module()

    def valid_event(self):
        return {
            "event_id": str(uuid4()),
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "order_id": "ORD-0001",
            "customer_id": "CUS-100",
            "amount": 120000,
            "currency": "VND",
        }

    def test_accepts_a_complete_valid_order(self):
        event = self.valid_event()

        self.assertIsNone(self.validator.validate(event, "ORD-0001"))

    def test_rejects_a_missing_required_field(self):
        event = self.valid_event()
        del event["currency"]

        with self.assertRaisesRegex(ValueError, "missing_fields"):
            self.validator.validate(event, "ORD-0001")

    def test_rejects_a_non_object_json_value(self):
        with self.assertRaisesRegex(ValueError, "event_must_be_a_json_object"):
            self.validator.validate([], None)
