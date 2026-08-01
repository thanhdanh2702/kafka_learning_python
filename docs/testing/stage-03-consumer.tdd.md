# Stage 3 consumer — test evidence

## User journey

As a Kafka learner, I want tests that prove the consumer reads an order, processes
it before committing its offset, skips partition EOF, and does not commit failed
messages.

## Test result

The Stage 3 test file was initially empty. The consumer behavior already existed,
so the new characterization tests passed without changing production code.

```bash
python3 -m unittest tests.stage_03_consumer.test_order_consumer -v
```

Result: 5 tests passed.

| Guarantee | Test | Result |
|---|---|---|
| SIGINT/SIGTERM handler stops the poll loop | `test_stop_ends_the_poll_loop` | PASS |
| A valid JSON order is printed and then manually committed | `test_main_processes_then_commits_a_valid_message` | PASS |
| Partition EOF is skipped without commit | `test_partition_eof_is_skipped_without_commit` | PASS |
| A broker error is raised, not committed, and the consumer closes | `test_broker_error_is_raised_and_consumer_is_closed` | PASS |
| Invalid JSON is not committed and the consumer closes | `test_invalid_json_is_not_committed` | PASS |

## Coverage

Python's standard-library `trace` runner reported 97.3% line coverage for
`src/stage_03_consumer/order_consumer.py`.

The full discovered suite also passed: 8 tests, 0 failures.
