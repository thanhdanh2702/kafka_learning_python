from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CONFIGURATION_FILES = {
    ".dockerignore",
    ".env.example",
    ".gitignore",
    "compose.yaml",
    "Dockerfile",
    "Makefile",
    "requirements.txt",
    "config/topics.env",
    "scripts/create-topics.sh",
}

IMPLEMENTATION_FILES = {
    "src/common/__init__.py",
    "src/common/event_model.py",
    "src/common/kafka_factory.py",
    "src/common/settings.py",
    "src/stage_02_producer/__init__.py",
    "src/stage_02_producer/order_producer.py",
    "src/stage_03_consumer/__init__.py",
    "src/stage_03_consumer/order_consumer.py",
    "src/stage_04_data_quality/__init__.py",
    "src/stage_04_data_quality/validator.py",
    "src/stage_05_scaling/__init__.py",
    "src/stage_05_scaling/load_generator.py",
    "src/stage_06_reliability/__init__.py",
    "src/stage_06_reliability/replay_dlq.py",
    "src/stage_06_reliability/retry_policy.py",
    "src/stage_07_postgres/__init__.py",
    "src/stage_07_postgres/outbox_publisher.py",
    "src/stage_07_postgres/postgres_sink.py",
    "src/stage_08_storage/__init__.py",
    "src/stage_08_storage/projection_rebuilder.py",
    "src/stage_08_storage/state_producer.py",
    "src/stage_09_transactions/__init__.py",
    "src/stage_09_transactions/transactional_transformer.py",
    "src/stage_09_transactions/window_aggregator.py",
    "src/stage_10_operations/__init__.py",
    "src/stage_10_operations/health_probe.py",
    "src/stage_10_operations/metrics.py",
}

SQL_IMPLEMENTATION_FILES = {
    "sql/migrations/001_create_orders.sql",
    "sql/migrations/002_create_processed_events.sql",
    "sql/migrations/003_create_daily_metrics.sql",
    "sql/migrations/004_create_outbox.sql",
    "sql/migrations/005_create_window_metrics.sql",
}


class ProjectStructureTest(unittest.TestCase):
    def test_configuration_files_exist_and_are_not_empty(self):
        for relative_path in CONFIGURATION_FILES:
            with self.subTest(path=relative_path):
                file_path = PROJECT_ROOT / relative_path
                self.assertTrue(file_path.is_file())
                self.assertTrue(file_path.read_text(encoding="utf-8").strip())

    def test_python_implementation_files_exist(self):
        for relative_path in IMPLEMENTATION_FILES:
            with self.subTest(path=relative_path):
                file_path = PROJECT_ROOT / relative_path
                self.assertTrue(file_path.is_file())

    def test_sql_implementation_files_exist(self):
        for relative_path in SQL_IMPLEMENTATION_FILES:
            with self.subTest(path=relative_path):
                file_path = PROJECT_ROOT / relative_path
                self.assertTrue(file_path.is_file())


if __name__ == "__main__":
    unittest.main()
