#!/usr/bin/env bash
# Final soak runbook: one smoke or 24-hour acceptance run on this host, on the frozen candidate.
#
#   scripts/soak/finalSoakNode.sh <40-hex sha> <smoke|acceptance>
#
# Pause every other agent and test on the host first; preflight fails on a busy host. Steps:
# checkout of <sha> (detached), `npm ci --ignore-scripts`, reporter preparation with the run's file
# cap, the three-broker Kafka 4.4.0 KRaft cluster from docker-compose.telemetry.yml plus the prepared
# override, preflight, the driver under nohup with a heartbeat file, the outer exit witness and
# finalize, cleanup (run topics and groups, then containers) and a tarball of the artifact.
#
# A supplied cluster: set FINAL_SOAK_SUPPLIED_BROKERS (comma list) and FINAL_SOAK_SUPPLIED_EVIDENCE
# (a JSON file naming per-node build evidence files); the Compose steps are skipped and the artifact
# records the supplied provenance. TELEMETRY_JAVA_CACHE names the pinned Java inputs.
# FINAL_SOAK_ARTIFACTS is the artifact root ($HOME/final-soak by default).
#
# This script reads and writes no credentials.
set -euo pipefail

sha="${1:-}"
mode="${2:-}"
if [[ ! "$sha" =~ ^[0-9a-f]{40}$ ]] || [[ "$mode" != smoke && "$mode" != acceptance ]]; then
  echo "usage: $0 <40-hex sha> <smoke|acceptance>" >&2
  exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
export KAFKA_VERSION=4.4.0
out="${FINAL_SOAK_ARTIFACTS:-$HOME/final-soak}/${sha:0:12}-${mode}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out/telemetry-sink"
export TELEMETRY_EVIDENCE_DIR="$out/telemetry-sink"
compose=(docker compose -p kafkajs -f docker-compose.telemetry.yml -f "$out/broker/compose.json")

git checkout --detach "$sha"
npm ci --ignore-scripts

supplied="${FINAL_SOAK_SUPPLIED_BROKERS:-}"
if [[ -n "$supplied" ]]; then
  export KAFKA_BROKERS="$supplied"
  mkdir -p "$out/broker"
  echo '{"services":{}}' > "$out/broker/compose.json"
else
  export KAFKA_BROKERS=localhost:19092,localhost:19095,localhost:19098
  node scripts/soak/finalSoakPreflight.js --prepare-reporter --out "$out"
  "${compose[@]}" up -d --wait
fi

cleanup() {
  node scripts/soak/finalSoak.js --cleanup --out "$out" || true
  if [[ -z "$supplied" ]]; then "${compose[@]}" down --volumes || true; fi
}

if ! node scripts/soak/finalSoakPreflight.js --expect-sha "$sha" --mode "$mode" --out "$out"; then
  echo "preflight failed; see $out/preflight.json" >&2
  cleanup
  exit 1
fi

nohup node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode "$mode" --expect-sha "$sha" \
  --matrix scripts/soak/finalSoakMatrix.json --out "$out" > "$out/driver.log" 2>&1 &
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
node scripts/soak/finalSoak.js --finalize --out "$out" --status "$status"
cleanup
tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")"
echo "artifact: $out.tar.gz (driver exit status $status)"
