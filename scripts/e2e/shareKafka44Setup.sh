#!/bin/bash -e

# Upgrades the finalized share.version of the Kafka 4.4 test cluster to 2, which turns share
# dead-letter queues on. It runs once, before the live scenarios start, and exits non-zero when
# the upgrade or the read-back fails, so a cluster that is not ready stops the run instead of
# skipping any scenario. The scenarios then confirm the level through the broker's own answer.

FEATURES=/opt/kafka/bin/kafka-features.sh
BOOTSTRAP=kafka1:29092
ATTEMPTS=30

find_container_id() {
  docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q
}

container="$(find_container_id)"
if [ -z "${container}" ]; then
  echo "The kafka1 container is not running" >&2
  exit 1
fi

# The controller can still be settling right after the brokers answer, so the upgrade retries.
attempt=1
until docker exec "${container}" "${FEATURES}" --bootstrap-server "${BOOTSTRAP}" upgrade --feature share.version=2; do
  if [ "${attempt}" -ge "${ATTEMPTS}" ]; then
    echo "share.version=2 was not applied after ${ATTEMPTS} attempts" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 2
done

docker exec "${container}" "${FEATURES}" --bootstrap-server "${BOOTSTRAP}" describe
