# Stage 2 producer — test evidence

## User journey

As a Kafka learner, I want tests that prove the Python producer creates valid
orders, uses `order_id` as the Kafka key, attaches a delivery callback to every
send, calls `poll()`, and checks the result of `flush()`.

## Test result

The Stage 2 test file was initially empty. The producer behavior already existed,
so the new characterization tests passed on their first run and no production code
was changed.

```bash
python3 -m unittest tests.stage_02_producer.test_order_producer -v
```

Result: 5 tests passed.

| Guarantee | Test | Result |
|---|---|---|
| `make_order()` creates the expected order schema and values | `test_make_order_creates_a_valid_order_event` | PASS |
| Successful delivery reports include topic, partition, offset, and key | `test_delivery_report_prints_success_metadata` | PASS |
| Failed delivery reports show the error | `test_delivery_report_prints_failure` | PASS |
| `main()` sends 10 JSON messages whose key equals `order_id`, calls `poll()`, and flushes | `test_main_sends_ten_keyed_json_messages` | PASS |
| Unflushed messages raise an error | `test_main_raises_when_messages_remain_after_flush` | PASS |

## Coverage

Python's standard-library `trace` runner reported 98.1% line coverage for
`src/stage_02_producer/order_producer.py`.

The full discovered suite also passed: 8 tests, 0 failures.
