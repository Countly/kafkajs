#!/usr/bin/env bash
# Final soak runbook: one smoke, rehearsal or 24-hour acceptance run on this host, on the frozen
# candidate. A rehearsal runs the first hours of the day plan (the matrix fixes how many), so the
# core checker judges the heap row on several quiet boundaries before the day; like a smoke it is
# diagnostic and never ACCEPTED.
#
#   scripts/soak/finalSoakNode.sh <40-hex sha> <smoke|rehearsal|acceptance>
#
# Pause every other agent and test on the host first; preflight fails on a busy host. Steps:
# checkout of <sha> (detached), `npm ci --ignore-scripts`, reporter preparation with the run's file
# cap, the three-broker Kafka 4.4.0 KRaft cluster from docker-compose.telemetry.yml plus the prepared
# override, share.version finalized, preflight, the driver under nohup with a heartbeat file, the
# outer exit receipt, the two container dumps (the standing topics' records read_committed and every
# consumer group's committed offsets, by the Apache tools inside the first broker's container, while
# the cluster and the topics are still up), finalize, the driver's summary, cleanup (run topics and
# groups, then containers) and a tarball of the artifact. A failed or interrupted run is sealed the
# same way, so its evidence is bound and its dumps kept, never reported as complete. A run whose
# driver exited 0 but whose workload receipt (supervisor.json) holds failures, problems, forced
# exits, an abort or an incomplete source fails at stage `workload`, after sealing; the core checker
# still judges it into <artifact>.core.txt for diagnosis, never changing the failed status. A passed
# workload check is followed by stage `core`: the same checker run, failing on REJECTED, on no
# verdict, or on an acceptance run that is not ACCEPTED; a smoke or a rehearsal is never ACCEPTED,
# so its NOT ACCEPTED passes.
#
# The driver runs in its own process group with its workers, under a watchdog on the host's
# monotonic clock: past its limit (`finalSoak.js --driver-limit`: the planned window plus its start,
# teardown and finish allowances) or after 15 minutes without a stream byte the whole group is sent
# TERM, then KILL, and <artifact>.watchdog names why; a process of the group still running once the
# driver ended is ended the same way. A run never ends with a synthetic clean exit: a supervisor
# that could not finish its evidence writes <artifact>/incomplete.json and exits non-zero.
#
# Whatever happens after the artifact directory exists, three files sit beside it: <artifact>.runbook.log
# (everything this script and its steps printed), <artifact>.runbook.json (the failed stage or null,
# the exit status, the driver's status and the stage still running) and <artifact>.tar.gz; a failed
# run with a Compose cluster also leaves the brokers' logs in <artifact>.compose.log. The receipt is
# written before every stage after the driver, with the stage about to run and no exit status yet,
# so the driver's status and a failed stage are on record even if a later stage never ends; each of
# those stages has its own limit, past which it is sent TERM, then KILL 30 s later, and what it
# wrote stays. Cleanup covers only what was started (run topics and groups only once the driver
# wrote its config), and the script exits with the first failure's status. They sit beside the
# artifact so the header, which hashes every file inside it, never binds a file that is still
# being written.
#
# Environment:
#   FINAL_SOAK_ARTIFACTS          artifact root ($HOME/final-soak by default)
#   FINAL_SOAK_DIAGNOSTIC=1       smoke or rehearsal: a host that cannot meet the quiet-host floors (a hosted
#                                 CI runner); preflight records those checks without failing on them
#                                 and the artifact is marked diagnostic. Never acceptance evidence.
#   FINAL_SOAK_HEAP_SNAPSHOTS=1   smoke or rehearsal: the selected processes write a V8 heap snapshot
#                                 into <artifact>/heap after each quiet boundary's last reading, for
#                                 scripts/soak/heapSnapshotReport.js; the config records them.
#                                 Refused in acceptance mode.
#   FINAL_SOAK_COMPANIONS         space-separated <name>=<file>[@<run url>] companions for finalize
#   TELEMETRY_JAVA_CACHE          the pinned Java inputs of the reporter build
#   FINAL_SOAK_STAGE_S            one limit in s for every stage after the driver, for a test of
#                                 this script (the per-stage limits below by default)
#
# What a green run does not prove (the core checker's seven rows are the verdict; these are outside it):
# - The share drain proves delivery (every offset below the end had a handler entry) and quiescence
#   (a live member kept fetching for the lock duration plus a margin and nothing more was handed
#   out). It does not prove how the broker settled an accept whose reply was lost; those are counted
#   as unresolved accepts in the last share drain record, as evidence only.
# - A recovery step proves a handler ran after the recovery (and, for consumers, that a drain
#   committed and read back every held partition); it does not pair that handler with that commit.
# - A recreation or regex step proves the topic change, its probes' acknowledgements and the
#   recipients' drain to its end; it does not prove each probe reached a handler, that a pause or
#   resume succeeded, or that the last recreated incarnation was drained before its final seal.
#
# This script reads and writes no credentials.
set -euo pipefail

sha="${1:-}"
mode="${2:-}"
if [[ ! "$sha" =~ ^[0-9a-f]{40}$ ]] || [[ "$mode" != smoke && "$mode" != rehearsal && "$mode" != acceptance ]]; then
  echo "usage: $0 <40-hex sha> <smoke|rehearsal|acceptance>" >&2
  exit 2
fi
if [[ -n "${FINAL_SOAK_SUPPLIED_BROKERS:-}" ]]; then
  echo "a supplied cluster is not supported: the run stops, restarts and dumps its brokers through the repository Compose services" >&2
  exit 2
fi
diagnostic=()
if [[ "${FINAL_SOAK_DIAGNOSTIC:-}" == 1 ]]; then
  if [[ "$mode" == acceptance ]]; then
    echo "FINAL_SOAK_DIAGNOSTIC is accepted only in smoke or rehearsal mode" >&2
    exit 2
  fi
  diagnostic=(--diagnostic)
fi
snapshots=()
if [[ "${FINAL_SOAK_HEAP_SNAPSHOTS:-}" == 1 ]]; then
  if [[ "$mode" == acceptance ]]; then
    echo "FINAL_SOAK_HEAP_SNAPSHOTS is accepted only in smoke or rehearsal mode" >&2
    exit 2
  fi
  snapshots=(--heap-snapshots)
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
# The runbook log, for messages of a stage whose own output goes to a file.
exec 3>&1
export TELEMETRY_EVIDENCE_DIR="$out/telemetry-sink"
export COMPOSE_PROJECT_NAME=kafkajs
export COMPOSE_FILE="docker-compose.telemetry.yml:$out/broker/compose.json"

stage=checkout
started=
driver=
driver_status=null
exit_ms=
sealed=
dump_status=0
after=
ending=

# Limits in s of the stages after the driver. The dump's covers its own bound of 30 min for each of
# its Apache tool calls (two dumps and up to eight describes) and a margin.
declare -A limits=(
  [outer-exit]=300 [dump]=19800 [finalize]=7200 [summary]=3600 [workload]=3600 [core]=7200
  [cleanup]=1800 [compose-logs]=600 [compose-down]=900 [archive]=7200
)

# The first failure after the preflight, in the order the run reports it: the driver, the dumps, then
# a stage after them.
failed_so_far() {
  if [[ -n "$ending" ]]; then
    ((ending != 0)) && echo "$stage"
  elif [[ "$driver_status" != null ]] && ((driver_status != 0)); then
    echo driver
  elif ((dump_status != 0)); then
    echo dump
  else
    echo "$after"
  fi
  return 0
}

# Writes the runbook receipt: <pending> is the stage about to run, or empty once the runbook ends
# with exit status <status>. A temporary file renamed over the receipt, so a reader never sees half
# of one.
receipt() {
  local pending=$1 status=${2:-null} failed
  failed="$(failed_so_far)"
  [[ -n "$failed" ]] && failed="\"$failed\"" || failed=null
  [[ -n "$pending" ]] && pending="\"$pending\"" || pending=null
  printf '{"artifact":"%s","failedStage":%s,"exitStatus":%s,"driverStatus":%s,"pendingStage":%s}\n' \
    "$(basename "$out")" "$failed" "$status" "$driver_status" "$pending" > "$out.runbook.json.tmp" &&
    mv -f "$out.runbook.json.tmp" "$out.runbook.json"
}

# Runs one stage after the driver under its limit: the receipt names it first; past the limit it is
# sent TERM, then KILL 30 s later, and its status is the timeout's.
bounded() {
  local name=$1 limit status=0
  shift
  limit=${FINAL_SOAK_STAGE_S:-${limits[$name]}}
  receipt "$name"
  timeout -k 30s "${limit}s" "$@" || status=$?
  if ((status == 124 || status == 137)); then echo "stage $name did not end within $limit s" >&3; fi
  return "$status"
}

# Removes the run's topics and groups, which exist only once the driver wrote config.json, then on
# failure keeps the brokers' logs and stops the Compose cluster.
cleanup() {
  local status=$1
  if [[ -f "$out/config.json" ]]; then
    bounded cleanup node scripts/soak/finalSoak.js --cleanup --out "$out" || true
  else
    echo "no driver config: the driver created no run topics or groups, so none are removed"
  fi
  if ((status != 0)); then
    bounded compose-logs docker compose logs --no-color --timestamps > "$out.compose.log" 2>&1 || true
    echo "broker logs: $out.compose.log"
  fi
  bounded compose-down docker compose down --volumes || true
}

# Seals the artifact once, while the cluster and the run's topics are still up: the receipt of the
# supervisor's exit as the shell saw it, the Apache dumps of the logs and the groups, then
# finalize, which binds every file (the dumps included) in the header. Only a run whose driver
# wrote its config has topics to dump. A failed dump is remembered, never hidden, and finalize still
# runs so the partial dumps are bound.
seal() {
  [[ -n "$sealed" ]] && return 0
  sealed=1
  [[ -f "$out/config.json" ]] || return 0
  local companions=() companion
  bounded outer-exit node scripts/soak/finalSoak.js --outer-exit --out "$out" --status "$driver_status" --exit-wall-ms "$exit_ms" || after=outer-exit
  bounded dump node scripts/soak/finalSoakDump.js --out "$out" || dump_status=$?
  for companion in ${FINAL_SOAK_COMPANIONS:-}; do companions+=(--companion "$companion"); done
  bounded finalize node scripts/soak/finalSoak.js --finalize --out "$out" --status "$driver_status" "${companions[@]}" || after="${after:-finalize}"
}

# Runs the core checker on the sealed artifact, writes its report to <artifact>.core.txt beside the
# receipt (outside the directory the header binds), prints it and sets core_status to its exit status.
run_core() {
  core_status=0
  bounded core node scripts/evidence/finalSoakCore.js "$out" > "$out.core.txt" || core_status=$?
  cat "$out.core.txt"
}

# For diagnosis only, after a run failed its workload check: the core checker of this checkout, when
# it has one, judges the sealed artifact through run_core. Its verdict and exit status never change
# the run's failed status.
diagnose_core() {
  [[ -f scripts/evidence/finalSoakCore.js ]] || return 0
  run_core
  echo "core checker report for diagnosis only (the run failed at stage workload): $out.core.txt"
}

# Seconds on the host's monotonic clock (since boot), which wall-clock steps never move.
uptime_s() {
  local up
  read -r up _ < /proc/uptime
  echo "${up%.*}"
}

# Ends the driver's process group: TERM, then KILL once <grace> s passed with any of it left; the
# escalation is appended to <artifact>.watchdog with <why>.
stop_group() {
  local why=$1 grace=$2 since
  kill -0 -- "-$driver_group" 2> /dev/null || return 0
  echo "$why: TERM, then KILL after $grace s" | tee -a "$out.watchdog"
  kill -TERM -- "-$driver_group" 2> /dev/null || true
  since=$(uptime_s)
  until (($(uptime_s) - since >= grace)) || ! kill -0 -- "-$driver_group" 2> /dev/null; do sleep 1; done
  if kill -KILL -- "-$driver_group" 2> /dev/null; then echo "driver processes killed" | tee -a "$out.watchdog"; fi
}

# Reaps the driver and ends what is left of its process group: its workers had their natural-exit
# wait inside the driver, so a process still running after it is stuck.
reap_driver() {
  local status=0
  wait "$driver" || status=$?
  driver_status=$status
  exit_ms=$((${EPOCHREALTIME/./} / 1000))
  driver=
  stop_group "driver processes outlived the driver" 10
}

# Runs on every exit after the artifact directory exists: stops and reaps a driver not yet reaped,
# records the receipt, cleans up what was started, archives and writes the final receipt.
finish() {
  local status=$?
  trap - EXIT
  set +e
  ending=$status
  if [[ -n "$driver" ]]; then
    stop_group "the runbook is ending" 60
    reap_driver
  fi
  if [[ -n "$started" ]]; then
    seal
    cleanup "$status"
  fi
  if ((status != 0)) && [[ "$stage" == workload && -n "$sealed" ]]; then diagnose_core; fi
  if ! bounded archive tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")" && ((status == 0)); then
    stage=archive
    status=1
    ending=1
  fi
  if ((status != 0)); then
    echo "runbook failed at stage $stage (exit status $status); log: $out.runbook.log"
  fi
  receipt "" "$status"
  echo "artifact: $out.tar.gz (receipt $out.runbook.json)"
  exit "$status"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

git checkout --detach "$sha"
stage=install
npm ci --ignore-scripts

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
# The describe output goes to the log too, so the finalized level is on record.
share_level() {
  "${features[@]}" describe | tee /dev/stderr | sed -n 's/.*Feature: share\.version.*FinalizedVersionLevel: \([0-9]*\).*/\1/p'
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

stage=preflight
node scripts/soak/finalSoakPreflight.js --expect-sha "$sha" --mode "$mode" --out "$out" "${diagnostic[@]}"

stage=driver
limit_s="$(node scripts/soak/finalSoak.js --driver-limit --mode "$mode" --out "$out")"
[[ "$limit_s" =~ ^[0-9]+$ ]]
# setsid makes the driver the leader of a new process group, which its workers join.
nohup setsid node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode "$mode" --expect-sha "$sha" \
  --matrix scripts/soak/finalSoakMatrix.json --out "$out" "${diagnostic[@]}" "${snapshots[@]}" > "$out/driver.log" 2>&1 &
driver=$!
driver_group=$driver
# Heartbeat and watchdog. Every 60 s the heartbeat file gets the time, the stream's size and how
# long that size has not changed, so a live but stalled driver shows. A driver that runs past its
# limit (planned window plus start and teardown allowances) or writes no stream bytes for
# $stall_s s has its process group sent TERM, then KILL after 60 s; <artifact>.watchdog names the
# escalation and the run fails at stage driver with the driver's status. The limit is checked
# before the stream is looked at.
stall_s=900
(
  start=$(uptime_s)
  beat=-60
  size=-1
  moved=$start
  while kill -0 "$driver" 2> /dev/null; do
    now=$(uptime_s)
    reason=
    if ((now - start >= limit_s)); then
      reason="ran past its limit of $limit_s s"
    else
      current=$(stat -c %s "$out/stream.ndjson" 2> /dev/null || echo 0)
      if ((current != size)); then
        size=$current
        moved=$now
      fi
      if ((now - beat >= 60)); then
        printf '%s stream-bytes=%s unchanged-s=%s\n' "$(date -u +%FT%TZ)" "$size" "$((now - moved))" > "$out/heartbeat"
        beat=$now
      fi
      if ((now - moved >= stall_s)); then reason="wrote no stream bytes for $stall_s s"; fi
    fi
    if [[ -n "$reason" ]]; then
      stop_group "driver $reason" 60
      break
    fi
    sleep 1
  done
) &
heartbeat=$!

reap_driver
receipt seal
# The watchdog's work ended with the driver's group; it is not waited for, since a stalled artifact
# filesystem could hold its last look at the stream.
kill "$heartbeat" 2> /dev/null || true
seal
bounded summary node scripts/soak/finalSoak.js --summary --out "$out" > "$out.summary.json" || after="${after:-summary}"
cat "$out.summary.json"
if ((driver_status != 0)); then exit "$driver_status"; fi
if ((dump_status != 0)); then
  stage=dump
  exit 1
fi
if [[ -n "$after" ]]; then
  stage=$after
  exit 1
fi
# A workload that aborted or recorded a problem fails the run once its artifact is sealed; the
# driver's own exit code, observed above, stays in the artifact as it was.
stage=workload
bounded workload node scripts/soak/finalSoak.js --check-workload --out "$out"
# The core checker judges the sealed artifact. Its report goes beside the receipt, outside the
# directory the header binds. A smoke or a rehearsal is never ACCEPTED (both are diagnostic), so NOT
# ACCEPTED, which has no FAIL row, is its expected verdict; REJECTED, no verdict at all, or an
# acceptance run that is not ACCEPTED fails here.
stage=core
run_core
verdict="$(sed -n '1s/^CORE VERDICT: \([A-Z ]*[A-Z]\).*/\1/p' "$out.core.txt")"
note="core checker verdict: ${verdict:-none} (exit status $core_status), report $out.core.txt"
if [[ "$mode" != acceptance ]]; then note="$note. A $mode is never ACCEPTED; NOT ACCEPTED with no FAIL row is its expected verdict."; fi
echo "$note"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  printf '### Final soak core checker\n\n%s\n\n```\n%s\n```\n' "$note" "$(cat "$out.core.txt")" >> "$GITHUB_STEP_SUMMARY"
fi
case "$mode:$verdict:$core_status" in
  acceptance:ACCEPTED:0 | "smoke:NOT ACCEPTED:1" | "rehearsal:NOT ACCEPTED:1") ;;
  *) exit 1 ;;
esac
stage=done
