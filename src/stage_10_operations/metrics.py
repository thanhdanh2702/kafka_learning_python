from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import UTC, datetime
def build_log(
    *,
    service: str,
    event_id: str | None,
    order_id: str | None,
    topic: str,
    partition: int,
    offset: int,
    consumer_group: str | None,
    processing_time_ms: float,
    result: str,
    retry_count: int = 0,
) -> dict:
    """Build a safe, searchable event log.

    Deliberately accept trace fields instead of an event payload.  This keeps
    passwords, tokens, payment data, and large JSON bodies out of logs.
    """

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "service": service,
        "event_id": event_id,
        "order_id": order_id,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "consumer_group": consumer_group,
        "processing_time_ms": processing_time_ms,
        "result": result,
        "retry_count": retry_count,
    }


class Metrics:
    """In-memory counters and timing summaries for one service instance."""

    def __init__(self, service: str):
        if not service or not service.strip():
            raise ValueError("service_required")
        self.service = service
        self._counters: dict[str, int] = defaultdict(int)
        self._timings: dict[str, list[float]] = defaultdict(list)

    def increment(self, name: str, amount: int = 1) -> None:
        if not name or not name.strip():
            raise ValueError("metric_name_required")
        if not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
            raise ValueError("increment_must_be_non_negative_integer")
        self._counters[name] += amount

    def observe(self, name: str, value: float) -> None:
        if not name or not name.strip():
            raise ValueError("metric_name_required")
        try:
            numeric_value = float(value)
        except (TypeError, ValueError) as error:
            raise ValueError("timing_must_be_number") from error
        if not math.isfinite(numeric_value) or numeric_value < 0:
            raise ValueError("timing_must_be_finite_and_non_negative")
        self._timings[name].append(numeric_value)

    def snapshot(self) -> dict:
        timings = {}
        for name, values in self._timings.items():
            timings[name] = {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
            }

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self.service,
            "counters": dict(self._counters),
            "timings": timings,
        }

    def emit(self, stream=None) -> dict:
        """Print and return the current snapshot as one JSON log line."""

        stream = stream or sys.stdout
        snapshot = self.snapshot()
        print(json.dumps(snapshot, sort_keys=True), file=stream)
        return snapshot


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Print an empty metrics snapshot")
    parser.add_argument("--service", default="kafka-de-learning")
    args = parser.parse_args(argv)
    Metrics(args.service).emit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
