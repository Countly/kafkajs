#!/usr/bin/env bash
# Verifies retained fixture identities and decodes a bounded raw MetricsData file offline.
# Usage: run.sh verify <line> | run.sh decode <line> <absolute-raw-file>
set -euo pipefail
MODE=${1:?verify or decode}
LINE=${2:?4.3.1 or 4.2.2}
case "$MODE:$LINE" in verify:4.3.1|verify:4.2.2|decode:4.3.1|decode:4.2.2) ;; *) exit 2 ;; esac
: "${TMPDIR:?set a private temporary directory}"
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
timeout --foreground -k 5s 20s node - "$ROOT" "$HERE" "$LINE" <<'NODE'
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const [root, here, line] = process.argv.slice(2)
const sha = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')
const check = (file, expected) => {
  if (sha(file) !== expected) throw new Error('Fixture identity mismatch: ' + file)
}
const file = path.join(here, 'fixtures/manifest.json')
check(file, 'd223d1c271816041b98a8ffcc143d5707f807cbf28a3f8629eb08a8474e7ca74')
const manifest = JSON.parse(fs.readFileSync(file, 'utf8'))
check(path.join(root, 'docker-compose.telemetry.yml'), manifest.composeSha256['docker-compose.telemetry.yml'])
for (const name of ['CountingTelemetryReporter.java', 'build.sh'])
  check(path.join(here, name), manifest.scripts[name])
for (const item of Object.values(manifest.lines)) {
  for (const kind of ['reporterJar', 'javaClientJar'])
    check(path.join(root, item[kind].path), item[kind].sha256)
}
if (process.env.TELEMETRY_JAVA_CACHE) {
  for (const key of ['kafka-clients-' + line, 'protobuf-java', 'slf4j-api']) {
    const input = manifest.inputs[key]
    check(path.join(process.env.TELEMETRY_JAVA_CACHE, input.cachePath), input.sha256)
  }
}
process.stdout.write(JSON.stringify({ verified: true, line, compose: manifest.composeSha256 }) + '\n')
NODE
if [ "$MODE" = verify ]; then exit 0; fi
RAW=${3:?absolute raw MetricsData file}
: "${TELEMETRY_JAVA_CACHE:?set the retained pinned Java cache}"
case "$RAW" in /*) ;; *) exit 2 ;; esac
[ -f "$RAW" ] && [ ! -L "$RAW" ]
[ "$(wc -c < "$RAW")" -le 4096 ]
CP="$HERE/fixtures/counting-telemetry-reporter-$LINE.jar:$TELEMETRY_JAVA_CACHE/jars/kafka-clients-$LINE.jar:$TELEMETRY_JAVA_CACHE/jars/protobuf-java-3.25.5.jar:$TELEMETRY_JAVA_CACHE/jars/slf4j-api-1.7.36.jar"
exec timeout --foreground -k 5s 20s java -Djava.io.tmpdir="$TMPDIR" -cp "$CP" org.countly.kafkajs.telemetry.CountingTelemetryReporter decode --file "$RAW"
