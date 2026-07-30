#!/usr/bin/env bash
set -euo pipefail

source /config/topics.env

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-broker:19092}"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
  local name="$1"
  local topic_config="${2:-}"
  local -a command=(
    "$kafka_topics"
    --bootstrap-server "$bootstrap_servers"
    --create
    --if-not-exists
    --topic "$name"
    --partitions "$TOPIC_PARTITIONS"
    --replication-factor "$TOPIC_REPLICATION_FACTOR"
  )

  if [[ -n "$topic_config" ]]; then
    command+=(--config "$topic_config")
  fi

  "${command[@]}"
}

create_topic "$RAW_TOPIC"
create_topic "$VALIDATED_TOPIC"
create_topic "$DLQ_TOPIC" "retention.ms=$DLQ_RETENTION_MS"
create_topic "$STATE_TOPIC" "cleanup.policy=compact"

"$kafka_topics" \
  --bootstrap-server "$bootstrap_servers" \
  --list
