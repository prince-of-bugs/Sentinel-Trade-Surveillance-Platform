#!/usr/bin/env bash
# Creates every Sentinel topic. Safe to re-run: existing topics are left alone.
# Runs inside the kafka-init container (see infra/docker-compose.yml), or by hand:
#   BOOTSTRAP=localhost:9092 KAFKA_BIN=/path/to/kafka/bin ./infra/kafka/create-topics.sh
#
# Keep this list in sync with common/src/main/java/com/sentinel/common/kafka/Topics.java
# (TopicsTest enforces it).
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
REPLICATION="${REPLICATION:-1}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
for attempt in $(seq 1 30); do
  if "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "Kafka not reachable after 30 attempts" >&2
    exit 1
  fi
  sleep 2
done

create_topic() {
  local name="$1" partitions="$2"
  shift 2
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${name}" --partitions "${partitions}" --replication-factor "${REPLICATION}" "$@"
  echo "  ok  ${name} (${partitions} partitions)"
}

# name                partitions  extra config
create_topic trades.orders      6
create_topic trades.executions  6
create_topic market.prices      3 --config retention.ms=3600000
create_topic trades.normalized  6
create_topic alerts.raised      3
create_topic trades.dlt         1 --config retention.ms=1209600000
create_topic alerts.dlt         1 --config retention.ms=1209600000

echo "All topics ready."
