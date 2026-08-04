# Stage 9.2 window aggregation test evidence

## User journeys

- As a data engineer, I can bucket validated orders into five-minute windows by
  event time.
- As a data engineer, I can accept a late event into its original event-time
  window.
- As an operator, I can replay an event without increasing a metric twice.

## TDD evidence

The first test run executed eight tests and failed because
`window_aggregator.py` was empty. After implementation:

```text
python3 -m unittest tests.stage_09_transactions.test_transactions_and_windows -v
Ran 7 tests - OK

python3 -m unittest discover -s tests -q
Ran 44 tests - OK
```

## Guarantees

| # | Guarantee | Result |
|---|---|---|
| 1 | Timestamps are rounded down to five-minute event-time windows | PASS |
| 2 | Timezone information is preserved | PASS |
| 3 | The new window metric table is created by SQL migration 005 | PASS |
| 4 | A new event increments the correct window count and revenue | PASS |
| 5 | A late event updates its original window | PASS |
| 6 | A duplicate `event_id` does not increment metrics | PASS |
| 7 | Invalid non-positive amounts are rejected before a DB write | PASS |
| 8 | The aggregator uses its own group and reads `orders.validated.v1` | PASS |

## Design decision

Late events are accepted and update their original window. The existing Stage 7
`processed_events` table, scoped by this stage's consumer group, makes the update
idempotent during retries and replay. The Kafka offset is committed only after
the PostgreSQL transaction returns successfully.

## Known gaps

- The local environment has no coverage tool installed, so no percentage is
  recorded.
- The current policy has no grace-period cutoff; production systems should add a
  documented late-event SLA or late-event topic when required.
