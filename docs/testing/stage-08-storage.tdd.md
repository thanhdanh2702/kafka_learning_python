# Stage 8 storage and replay test evidence

## User journeys

- As a learner, I can publish several values with the same Kafka key and a
  tombstone so I can observe log compaction behavior.
- As a data engineer, I can replay the validated event log into a separate
  PostgreSQL projection without running Stage 7 side effects again.
- As an operator, I can rebuild with the same first-event-wins duplicate rules
  used by the Stage 7 sink.

## RED evidence

The first Stage 8 test run executed seven tests and failed because both Python
files were empty. A later compatibility test failed because the first replay
implementation used last-event-wins while Stage 7 uses first-event-wins.

```text
python3 -m unittest tests.stage_08_storage.test_storage_and_replay -v
Ran 7 tests - FAILED
```

## GREEN evidence

```text
python3 -m unittest tests.stage_08_storage.test_storage_and_replay -v
Ran 10 tests - OK

python3 -m unittest discover -s tests -v
Ran 37 tests - OK
```

## Guarantees

| # | Guarantee | Result |
|---|---|---|
| 1 | State events use the Stage 7 envelope fields and `order_id` as Kafka key | PASS |
| 2 | Lab state history uses isolated `LAB-` keys | PASS |
| 3 | A tombstone is published as a Kafka record with a null value | PASS |
| 4 | Replay reads `orders.validated.v1` with a group separate from the Stage 7 sink | PASS |
| 5 | Replay writes to the separate `orders_rebuild` projection | PASS |
| 6 | Duplicate `event_id` and `order_id` values keep the first event, matching Stage 7 | PASS |
| 7 | Invalid UUID and amount values are skipped instead of stopping replay | PASS |

## Docker smoke test

The state producer delivered five lab records. The projection rebuilder consumed
the real validated topic, reported an invalid historical UUID as `SKIPPED`, and
continued processing. PostgreSQL counts matched after replay:

```text
original_orders = 122
rebuilt_orders  = 122
```

## Known gaps

- Kafka and PostgreSQL are replaced with test doubles in unit tests.
- Compaction timing and end-to-end container networking require the manual lab
  commands documented in the Stage 8 handoff.
- No coverage tool is installed in the local Python environment, so a numerical
  coverage percentage is not available.
