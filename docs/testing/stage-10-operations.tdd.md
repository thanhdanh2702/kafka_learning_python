# Stage 10 operations TDD evidence

## User journeys

- As an operator, I want searchable JSON logs so that I can trace a record
  without exposing its payload or secrets.
- As an operator, I want counters and timing summaries so that I can see
  processing volume and latency.
- As an operator, I want a readiness probe so that I can distinguish a live
  process from one that can actually reach Kafka and PostgreSQL.

## Validation

| Guarantee | Test | Result |
|---|---|---|
| Trace logs contain topic/partition/offset and no payload | `test_build_log_contains_trace_fields_without_payload` | PASS |
| Counters and timing summaries are aggregated | `test_metrics_increment_observe_and_snapshot` | PASS |
| Negative metric values are rejected | `test_metrics_rejects_negative_increment_and_timing` | PASS |
| Kafka and PostgreSQL healthy means `READY` | `test_health_probe_is_ready_when_kafka_and_postgres_are_ok` | PASS |
| Kafka failure means `NOT_READY` without leaking the DSN | `test_health_probe_is_not_ready_when_kafka_fails` | PASS |
| CLI prints JSON and returns a failure exit code when not ready | `test_main_prints_json_and_returns_nonzero_when_not_ready` | PASS |

Commands run:

```text
python3 -m unittest tests.stage_10_operations.test_operations -v
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
git diff --check
```

Results: Stage 10 tests `6/6` passed; full project tests `50/50` passed.

## Known scope

The metrics object is in-memory and emits JSON to stdout for the local lab. It
does not yet export Prometheus/OpenTelemetry metrics or create alerts. Existing
producer and consumer files must call `build_log`, `Metrics.increment`, and
`Metrics.observe` to collect live application metrics.
