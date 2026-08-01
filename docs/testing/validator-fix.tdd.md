# Validator fix — TDD evidence

## User journey

As a data engineer learning Kafka, I want the validator to route valid order events
to the validated topic and malformed events to the DLQ without committing the input
before the output is acknowledged.

## RED evidence

Command:

```bash
python3 -m unittest tests.stage_04_data_quality.test_validator -v
```

Result before the fix: all three tests errored because `validate()` called the
non-existent `dict.key()` method.

## GREEN evidence

Commands:

```bash
python3 -m py_compile src/stage_04_data_quality/validator.py
python3 -m unittest tests.stage_04_data_quality.test_validator -v
git diff --check
docker compose exec -T app python -m py_compile src/stage_04_data_quality/validator.py
```

Result after the fix: syntax checks passed, all three unit tests passed, and Git
reported no whitespace errors.

The follow-up beginner-focused refactor reduced `validator.py` from 139 to 106
lines while the same three tests remained green. It keeps the core flow:
consume, validate, route to validated/DLQ, wait for output, then commit input.

| Guarantee | Test | Type | Result |
|---|---|---|---|
| A complete order with a matching Kafka key is accepted | `test_accepts_a_complete_valid_order` | Unit | PASS |
| A missing required field is rejected | `test_rejects_a_missing_required_field` | Unit | PASS |
| A JSON value that is not an object is rejected cleanly | `test_rejects_a_non_object_json_value` | Unit | PASS |

## Known gap

The repository does not currently include a coverage runner, so no numeric coverage
percentage was recorded. End-to-end message routing still requires a running Kafka
broker and is intentionally verified separately from these unit tests.
