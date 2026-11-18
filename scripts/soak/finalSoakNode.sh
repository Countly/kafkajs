#!/usr/bin/env bash
# Final soak runbook: one smoke or 24-hour acceptance run on this host, on the frozen candidate.
#
#   scripts/soak/finalSoakNode.sh <40-hex sha> <smoke|acceptance>
#
# Pause every other agent and test on the host first; preflight fails on a busy host. Steps:
# checkout of <sha> (detached), `npm ci --ignore-scripts`, reporter preparation with the run's file
# cap, the three-broker Kafka 4.4.0 KRaft cluster from docker-compose.telemetry.yml plus the prepared
# override, share.version finalized, preflight, the driver under nohup with a heartbeat file, the
# outer exit witness and finalize, the driver's summary, cleanup (run topics and groups, then
# containers) and a tarball of the artifact.
#
# Whatever happens after the artifact directory exists, three files sit beside it: <artifact>.runbook.log
# (everything this script and its steps printed), <artifact>.runbook.json (the failed stage or null,
# the exit status and the driver's status) and <artifact>.tar.gz. Cleanup covers only what was
# started, and the script exits with the first failure's status. They sit beside the artifact so
# the header, which hashes every file inside it, never binds a file that is still being written.
#
# Environment:
#   FINAL_SOAK_ARTIFACTS          artifact root ($HOME/final-soak by default)
#   FINAL_SOAK_DIAGNOSTIC=1       smoke only: a host that cannot meet the quiet-host floors (a hosted
#                                 CI runner); preflight records those checks without failing on them
#                                 and the artifact is marked diagnostic. Never acceptance evidence.
#   FINAL_SOAK_COMPANIONS         space-separated <name>=<file>[@<run url>] companions for finalize
#   FINAL_SOAK_SUPPLIED_BROKERS   a supplied cluster (comma list); the Compose steps are skipped
#   FINAL_SOAK_SUPPLIED_EVIDENCE  that cluster's per-node build evidence (JSON naming the files)
#   TELEMETRY_JAVA_CACHE          the pinned Java inputs of the reporter build
#
# This script reads and writes no credentials.
set -euo pipefail

sha="${1:-}"
mode="${2:-}"
if [[ ! "$sha" =~ ^[0-9a-f]{40}$ ]] || [[ "$mode" != smoke && "$mode" != acceptance ]]; then
  echo "usage: $0 <40-hex sha> <smoke|acceptance>" >&2
  exit 2
fi
diagnostic=()
if [[ "${FINAL_SOAK_DIAGNOSTIC:-}" == 1 ]]; then
  if [[ "$mode" != smoke ]]; then
    echo "FINAL_SOAK_DIAGNOSTIC is accepted only in smoke mode" >&2
    exit 2
  fi
  diagnostic=(--diagnostic)
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
export KAFKA_VERSION=4.4.0
out="${FINAL_SOAK_ARTIFACTS:-$HOME/final-soak}/${sha:0:12}-${mode}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out/telemetry-sink" "$out/broker"
chmod 0777 "$out/telemetry-sink"
# tee ignores INT and TERM so it keeps logging while the exit handler cleans up after one.
exec > >(
  trap '' INT TERM
  exec tee -a "$out.runbook.log"
) 2>&1
export TELEMETRY_EVIDENCE_DIR="$out/telemetry-sink"
export COMPOSE_PROJECT_NAME=kafkajs
export COMPOSE_FILE="docker-compose.telemetry.yml:$out/broker/compose.json"
supplied="${FINAL_SOAK_SUPPLIED_BROKERS:-}"

stage=checkout
started=
driver=
driver_status=null

cleanup() {
  node scripts/soak/finalSoak.js --cleanup --out "$out" || true
  if [[ -z "$supplied" ]]; then docker compose down --volumes || true; fi
}

# Runs on every exit after the artifact directory exists: stops and reaps a driver not yet reaped
# (a signal to the process group may already have ended it), cleans up what was started, archives
# and writes the receipt.
finish() {
  local status=$?
  trap - EXIT
  set +e
  if [[ -n "$driver" ]]; then
    kill "$driver" 2> /dev/null
    wait "$driver"
    driver_status=$?
  fi
  if [[ -n "$started" ]]; then cleanup; fi
  if ! tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")" && ((status == 0)); then
    stage=archive
    status=1
  fi
  local failed=null
  if ((status != 0)); then
    failed="\"$stage\""
    echo "runbook failed at stage $stage (exit status $status); log: $out.runbook.log"
  fi
  printf '{"artifact":"%s","failedStage":%s,"exitStatus":%d,"driverStatus":%s}\n' \
    "$(basename "$out")" "$failed" "$status" "$driver_status" > "$out.runbook.json"
  echo "artifact: $out.tar.gz (receipt $out.runbook.json)"
  exit "$status"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

git checkout --detach "$sha"
stage=install
npm ci --ignore-scripts

if [[ -n "$supplied" ]]; then
  export KAFKA_BROKERS="$supplied"
  echo '{"services":{}}' > "$out/broker/compose.json"
  started=1
else
  export KAFKA_BROKERS=localhost:19092,localhost:19095,localhost:19098
  stage=prepare-reporter
  node scripts/soak/finalSoakPreflight.js --prepare-reporter --out "$out"
  stage=cluster-start
  started=1
  docker compose up -d --wait
  stage=cluster-wait
  # waitForKafka hands COMPOSE_FILE to `docker compose -f`, which takes one path, not a list.
  COMPOSE_FILE=docker-compose.telemetry.yml node scripts/waitForKafka.js
  # The controller can still be settling right after the brokers answer, so this retries.
  stage=share-version
  features=(docker compose exec -T kafka1 /opt/kafka/bin/kafka-features.sh --bootstrap-server kafka1:29092)
  share_level() {
    "${features[@]}" describe | sed -n 's/.*Feature: share\.version.*FinalizedVersionLevel: \([0-9]*\).*/\1/p'
  }
  attempt=1
  until level="$(share_level)" && [[ "$level" =~ ^[1-9][0-9]*$ ]]; do
    if ((attempt >= 30)); then
      echo "share.version is not finalized after $attempt attempts" >&2
      exit 1
    fi
    if [[ -z "$level" || "$level" == 0 ]]; then "${features[@]}" upgrade --feature share.version=1 || true; fi
    attempt=$((attempt + 1))
    sleep 2
  done
fi

stage=preflight
node scripts/soak/finalSoakPreflight.js --expect-sha "$sha" --mode "$mode" --out "$out" "${diagnostic[@]}"

stage=driver
nohup node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode "$mode" --expect-sha "$sha" \
  --matrix scripts/soak/finalSoakMatrix.json --out "$out" "${diagnostic[@]}" > "$out/driver.log" 2>&1 &
driver=$!
(
  beat=0
  while kill -0 "$driver" 2> /dev/null; do
    if ((beat % 60 == 0)); then date -u +%FT%TZ > "$out/heartbeat"; fi
    beat=$((beat + 1))
    sleep 1
  done
) &
heartbeat=$!

driver_status=0
wait "$driver" || driver_status=$?
driver=
wait "$heartbeat" || true
after=
companions=()
for companion in ${FINAL_SOAK_COMPANIONS:-}; do companions+=(--companion "$companion"); done
node scripts/soak/finalSoak.js --finalize --out "$out" --status "$driver_status" "${companions[@]}" || after=finalize
node scripts/soak/finalSoak.js --summary --out "$out" | tee "$out.summary.json" || after="${after:-summary}"
if ((driver_status != 0)); then exit "$driver_status"; fi
if [[ -n "$after" ]]; then
  stage=$after
  exit 1
fi
stage=done
