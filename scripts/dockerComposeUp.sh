#!/bin/bash -e
COMPOSE_FILE=${COMPOSE_FILE:="docker-compose.yml"}
echo "Running compose file: ${COMPOSE_FILE} (KAFKA_VERSION=${KAFKA_VERSION:-4.3.1})"
for attempt in 1 2 3; do
  if docker compose -f "${COMPOSE_FILE}" up --force-recreate -d; then
    break
  else
    up_status=$?
  fi

  echo "Docker compose up failed (attempt ${attempt}/3, exit ${up_status})." >&2
  if [ "${attempt}" -eq 3 ]; then
    exit "${up_status}"
  fi

  if ! docker compose -f "${COMPOSE_FILE}" down --remove-orphans; then
    echo "Docker compose cleanup failed; aborting startup retries." >&2
    exit "${up_status}"
  fi
  sleep 2
done
if [ -z "${NO_LOGS}" ]; then
  docker compose -f "${COMPOSE_FILE}" logs -f
fi
