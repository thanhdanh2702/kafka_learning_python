# Stage 5 load generator — TDD evidence

## User journey

As a Kafka learner, I want a load generator that sends 100 records with
same-key, hot-key, or many-keys distribution so I can observe partitioning and
consumer-group scaling.

## RED evidence

Before the fix, 3 of 5 tests failed. They reproduced the wrong broker default,
wrong raw topic, byte-form delivery output, and exposed the remaining producer
configuration issues.

```bash
python3 -m unittest tests.stage_05_scaling.test_load_generator -v
```

## GREEN evidence

After the fix, all 5 Stage 5 tests passed. The full project suite passed 23 tests
with no failures.

| Guarantee | Test | Result |
|---|---|---|
| The project broker and raw topic are used | `test_uses_the_project_broker_and_raw_topic` | PASS |
| All three key-distribution modes return the expected keys | `test_choose_key_supports_all_three_modes` | PASS |
| Delivery reports print readable success and failure output | `test_delivery_report_prints_success_and_failure` | PASS |
| Main sends 100 records with `acks=all`, idempotence, `poll(0)`, and `flush(10)` | `test_main_sends_one_hundred_messages_and_flushes` | PASS |
| Unknown modes are rejected | `test_invalid_mode_is_rejected` | PASS |

## Coverage

Python's standard-library `trace` runner reported 95.7% line coverage for
`src/stage_05_scaling/load_generator.py`.
