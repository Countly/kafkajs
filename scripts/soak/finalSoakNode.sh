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
export TELEMETRY_EVIDENCE_DIR="$out/telemetry-sink"
export COMPOSE_PROJECT_NAME=kafkajs
export COMPOSE_FILE="docker-compose.telemetry.yml:$out/broker/compose.json"

git checkout --detach "$sha"
npm ci --ignore-scripts

supplied="${FINAL_SOAK_SUPPLIED_BROKERS:-}"
if [[ -n "$supplied" ]]; then
  export KAFKA_BROKERS="$supplied"
  echo '{"services":{}}' > "$out/broker/compose.json"
else
  export KAFKA_BROKERS=localhost:19092,localhost:19095,localhost:19098
  node scripts/soak/finalSoakPreflight.js --prepare-reporter --out "$out"
  docker compose up -d --wait
  node scripts/waitForKafka.js
  features=(docker compose exec -T kafka1 /opt/kafka/bin/kafka-features.sh --bootstrap-server kafka1:29092)
  level="$("${features[@]}" describe | sed -n 's/.*Feature: share\.version.*FinalizedVersionLevel: \([0-9]*\).*/\1/p')"
  if [[ -z "$level" || "$level" == 0 ]]; then "${features[@]}" upgrade --feature share.version=1; fi
fi

cleanup() {
  node scripts/soak/finalSoak.js --cleanup --out "$out" || true
  if [[ -z "$supplied" ]]; then docker compose down --volumes || true; fi
}

if ! node scripts/soak/finalSoakPreflight.js --expect-sha "$sha" --mode "$mode" --out "$out" "${diagnostic[@]}"; then
  echo "preflight failed; see $out/preflight.json" >&2
  cleanup
  exit 1
fi

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

status=0
wait "$driver" || status=$?
wait "$heartbeat" || true
companions=()
for companion in ${FINAL_SOAK_COMPANIONS:-}; do companions+=(--companion "$companion"); done
node scripts/soak/finalSoak.js --finalize --out "$out" --status "$status" "${companions[@]}"
node scripts/soak/finalSoak.js --summary --out "$out" | tee "$out.summary.json"
cleanup
tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")"
echo "artifact: $out.tar.gz (driver exit status $status)"
