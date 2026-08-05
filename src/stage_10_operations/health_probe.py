"""Readiness probe for the Kafka and PostgreSQL services used by the lab."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime

import psycopg
from confluent_kafka.admin import AdminClient


def check_kafka(
    bootstrap_servers: str,
    topic: str | None = None,
    timeout: float = 5.0,
) -> dict:
    """Check broker connectivity and optionally verify one topic exists."""

    try:
        metadata = AdminClient(
            {"bootstrap.servers": bootstrap_servers}
        ).list_topics(timeout=timeout)
        topics = getattr(metadata, "topics", {})
        if topic and topic not in topics:
            return {"status": "ERROR", "reason": "topic_missing", "topic": topic}
        result = {"status": "OK"}
        if topic:
            result["topic"] = topic
        return result
    except Exception as error:  # a probe must report failure, not crash
        return {"status": "ERROR", "reason": type(error).__name__}


def check_postgres(dsn: str | None, timeout: float = 5.0) -> dict:
    """Check that PostgreSQL accepts a connection and a trivial query."""

    if not dsn:
        return {"status": "ERROR", "reason": "postgres_dsn_missing"}

    try:
        with psycopg.connect(dsn, connect_timeout=timeout) as connection:
            row = connection.execute("SELECT 1").fetchone()
            if row != (1,):
                return {"status": "ERROR", "reason": "unexpected_query_result"}
        return {"status": "OK"}
    except Exception as error:  # do not expose DSN/password in probe output
        return {"status": "ERROR", "reason": type(error).__name__}


def probe(
    bootstrap_servers: str,
    postgres_dsn: str | None,
    topic: str | None = None,
    timeout: float = 5.0,
) -> dict:
    """Return liveness, dependency readiness, and an overall status."""

    kafka = check_kafka(bootstrap_servers, topic, timeout)
    postgres = check_postgres(postgres_dsn, timeout)
    ready = kafka["status"] == "OK" and postgres["status"] == "OK"

    return {
        "checked_at": datetime.now(UTC).isoformat(),
        "status": "READY" if ready else "NOT_READY",
        "liveness": {"status": "OK"},
        "kafka": kafka,
        "postgres": postgres,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Check Kafka lab dependencies")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092"),
    )
    parser.add_argument("--postgres-dsn", default=os.getenv("POSTGRES_DSN"))
    parser.add_argument("--topic", default="orders.validated.v1")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args(argv)

    report = probe(
        args.bootstrap_servers,
        args.postgres_dsn,
        args.topic,
        args.timeout,
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
