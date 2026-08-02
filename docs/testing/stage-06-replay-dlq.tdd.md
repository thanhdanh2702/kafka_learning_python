# Stage 6 DLQ replay — TDD evidence

## User journey

As a Kafka learner, I want the DLQ replayer to read `orders.dlq.v1`, publish the
original record to `orders.raw.v1`, wait for broker acknowledgement, and only then
commit the DLQ offset.

## RED evidence

The first test run reproduced an `UnboundLocalError` caused by incrementing
`replay` instead of the declared `replayed` counter. Static review also found the
misspelled Kafka configuration, subscribe method, and topic names.

## GREEN evidence

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  tests.stage_06_reliability.test_retry_and_replay -v
```

Result: 2 tests passed.

| Guarantee | Test | Result |
|---|---|---|
| A valid DLQ record is replayed to raw, flushed, and then committed | `test_replays_dlq_record_then_commits_it` | PASS |
| A record without its original key is neither replayed nor committed | `test_missing_original_key_stops_without_replay_or_commit` | PASS |

## Coverage and known gap

Standard-library `trace` reported 93.9% line coverage for `replay_dlq.py`.
The complete suite ran 23 tests: 20 passed and 3 unrelated Stage 5 load-generator
tests failed because the current Stage 5 implementation still contains its older
broker/topic/delivery-output values.
