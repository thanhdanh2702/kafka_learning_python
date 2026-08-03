# Stage 7 PostgreSQL test evidence

## Scope

The tests characterize the existing Stage 7 PostgreSQL sink and transactional
outbox publisher. No production behavior was changed.

## Guarantees

| # | Guarantee | Test | Result |
|---|---|---|---|
| 1 | A new order writes the processed event, order, daily metric, and outbox event in one transaction | `test_new_order_writes_metrics_and_outbox_in_one_transaction` | PASS |
| 2 | A repeated `event_id` stops before writing the order, metric, or outbox | `test_duplicate_event_stops_before_order_metrics_and_outbox` | PASS |
| 3 | A repeated `order_id` stops before updating metrics or creating an outbox event | `test_duplicate_order_stops_before_metrics_and_outbox` | PASS |
| 4 | An unpublished outbox row is serialized with `order_id` as the Kafka key and then marked published | `test_unpublished_event_is_sent_then_marked_published` | PASS |
| 5 | A failed producer flush does not update `published_at` | `test_failed_flush_does_not_mark_event_as_published` | PASS |
| 6 | An empty outbox sends no messages | `test_no_unpublished_rows_sends_nothing` | PASS |

## Validation

Stage 7:

```text
python3 -m unittest tests.stage_07_postgres.test_postgres_sink -v
Ran 6 tests - OK
```

Full project:

```text
python3 -m unittest discover -s tests -v
Ran 27 tests - OK
```

Python syntax:

```text
python3 -m py_compile src/stage_07_postgres/postgres_sink.py \
  src/stage_07_postgres/outbox_publisher.py \
  tests/stage_07_postgres/test_postgres_sink.py
PASS
```

## Known gaps

- These are isolated unit tests using Kafka and PostgreSQL test doubles; they do
  not require running containers.
- The local environment does not have a coverage tool installed, so no coverage
  percentage was recorded.
- A Docker integration test remains useful for verifying the real broker,
  PostgreSQL schema, and container networking together.
