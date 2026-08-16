#!/usr/bin/env bash
#
# Creates every topic before any service starts, then exits.
#
# This is not a convenience. Each service declares NewTopic beans only for the topics it
# *owns*, so whichever service starts first inevitably subscribes to topics that do not
# exist yet — inventory-service waits on a command topic saga-orchestrator owns, and the
# orchestrator waits on event topics the executors own. The dependency is a cycle, so no
# start order avoids it.
#
# A consumer subscribed to a missing topic does not fail. It logs UNKNOWN_TOPIC_OR_PARTITION
# and waits for its next metadata refresh, which defaults to five minutes. The service looks
# perfectly healthy and consumes nothing. That is exactly how it presented in Chunk 8's
# integration suite, which pre-creates topics for the same reason (ARCHITECTURE.md 8.4).
#
# It also matches what section 5.3 says happens in a real deployment: topic creation is an
# operations concern, not an application one.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION:-1}"

TOPICS=(
  # order-service owns these
  "order.events.order-created.v1"
  "order.events.order-confirmed.v1"
  "order.events.order-cancelled.v1"

  # saga-orchestrator owns every command topic — it is their only publisher
  "order.commands.confirm-order.v1"
  "order.commands.cancel-order.v1"
  "inventory.commands.reserve-inventory.v1"
  "inventory.commands.release-inventory.v1"
  "payment.commands.process-payment.v1"
  "payment.commands.refund-payment.v1"

  # inventory-service owns these
  "inventory.events.inventory-reserved.v1"
  "inventory.events.inventory-reservation-failed.v1"
  "inventory.events.inventory-released.v1"

  # payment-service owns these
  "payment.events.payment-completed.v1"
  "payment.events.payment-failed.v1"
  "payment.events.payment-refunded.v1"
)

echo "Creating ${#TOPICS[@]} topics on ${BOOTSTRAP} (partitions=${PARTITIONS}, replication=${REPLICATION})"

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
done

echo "Topics ready:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list | sort
