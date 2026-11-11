#!/usr/bin/env bash
# Verifies retained fixture identities and decodes a bounded raw MetricsData file offline.
# Usage: run.sh verify <line> | decode <line> <raw> | prepare <line> <raw> | client[-check] <line> <role arguments>
set -euo pipefail
MODE=${1:?verify, decode, prepare, client or client-check}
LINE=${2:?4.4.0, 4.3.1 or 4.2.2}
case "$MODE:$LINE" in
  verify:4.4.0|verify:4.3.1|verify:4.2.2|decode:4.4.0|decode:4.3.1|decode:4.2.2|prepare:4.4.0|prepare:4.3.1|prepare:4.2.2|client:4.4.0|client:4.3.1|client:4.2.2|client-check:4.4.0|client-check:4.3.1|client-check:4.2.2) ;;
  *) exit 2 ;;
esac
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
check(file, 'b46f5783aa6efc223921a64f6fff2296f0701cc62f1cc0a050b3b204367e3d1c')
const manifest = JSON.parse(fs.readFileSync(file, 'utf8'))
check(path.join(root, 'docker-compose.telemetry.yml'), manifest.composeSha256['docker-compose.telemetry.yml'])
for (const name of ['CountingTelemetryReporter.java', 'build.sh', 'prepare-reporter-source.sh', 'prepare-client-source.sh', 'legacy/JavaTelemetryClient.java'])
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
const decoder = {
  source: 'e2c0e1856b402d645368844b100ba00a1f11c1c1b8120c990adde1cf98ef1c1f',
  jar: '179e47004f17d9d9ce7b379364a4d9f19b95aa8c1d4e10bb1277521935e3887b',
}
check(path.join(here, 'FullTelemetryDecoder.java'), decoder.source)
check(path.join(here, 'fixtures/full-telemetry-decoder.jar'), decoder.jar)
process.stdout.write(JSON.stringify({ verified: true, line, compose: manifest.composeSha256, decoder }) + '\n')
NODE
if [ "$MODE" = verify ]; then exit 0; fi
if [ "$MODE" = client ] || [ "$MODE" = client-check ]; then
  [ "$#" -eq 9 ] || exit 2
  : "${TELEMETRY_INTEROP_CACHE:?set the pinned Java interop cache}"
  case "$TELEMETRY_INTEROP_CACHE" in /*) ;; *) exit 2 ;; esac
  timeout --foreground -k 1s 3s node - "$TELEMETRY_INTEROP_CACHE" <<'CLIENT_INPUTS'
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const cache = process.argv[2]
const expected = {
  'kafka-clients-4.3.1.jar': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36',
  'slf4j-api-1.7.36.jar': 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0',
  'lz4-java-1.10.2.jar': '252dabd586d22149a31661a8afc1a50747714eb1a298bd4a6e780c8c91a13866',
  'snappy-java-1.1.10.7.jar': '4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36',
  'zstd-jni-1.5.6-10.jar': 'b6c3237e24b8252a5a9c3a0e1ae30ef8f323412e74bd358548b7a9e527d676df',
}
for (const [name, digest] of Object.entries(expected)) {
  const file = path.join(cache, name)
  const info = fs.lstatSync(file)
  if (!info.isFile() || info.size > 16 * 1024 * 1024 ||
      crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex') !== digest)
    throw new Error('Java interop input identity mismatch: ' + name)
}
CLIENT_INPUTS
  CLIENT_CP="$TELEMETRY_INTEROP_CACHE/kafka-clients-4.3.1.jar:$TELEMETRY_INTEROP_CACHE/slf4j-api-1.7.36.jar:$TELEMETRY_INTEROP_CACHE/lz4-java-1.10.2.jar:$TELEMETRY_INTEROP_CACHE/snappy-java-1.1.10.7.jar:$TELEMETRY_INTEROP_CACHE/zstd-jni-1.5.6-10.jar"
  shift 2
  if [ "$MODE" = client-check ]; then set -- --check "$@"; fi
  exec timeout --foreground -k 2s 40s java -Xmx128m -Djava.io.tmpdir="$TMPDIR" --class-path "$CLIENT_CP" "$HERE/JavaTelemetryClient.java" "$@"
fi
RAW=${3:?absolute raw MetricsData file}
: "${TELEMETRY_JAVA_CACHE:?set the retained pinned Java cache}"
case "$RAW" in /*) ;; *) exit 2 ;; esac
[ -f "$RAW" ] && [ ! -L "$RAW" ]
[ "$(wc -c < "$RAW")" -le 4096 ]
CP="$HERE/fixtures/full-telemetry-decoder.jar:$HERE/fixtures/counting-telemetry-reporter-$LINE.jar:$TELEMETRY_JAVA_CACHE/jars/kafka-clients-$LINE.jar:$TELEMETRY_JAVA_CACHE/jars/protobuf-java-3.25.5.jar:$TELEMETRY_JAVA_CACHE/jars/slf4j-api-1.7.36.jar"
# A one-shot decoder has no long-running workload to amortize profiling/C2 compilation.
JVM=(-XX:TieredStopAtLevel=1)
if [ "$MODE" = prepare ]; then
  : "${TELEMETRY_DECODER_ARCHIVE:?set a new absolute AppCDS archive path}"
  case "$TELEMETRY_DECODER_ARCHIVE" in /*) ;; *) exit 2 ;; esac
  [ ! -e "$TELEMETRY_DECODER_ARCHIVE" ] && [ ! -L "$TELEMETRY_DECODER_ARCHIVE" ] || exit 2
  JVM+=(-Xshare:on "-XX:ArchiveClassesAtExit=$TELEMETRY_DECODER_ARCHIVE" -Xlog:disable -Xlog:all=warning:stderr)
elif [ -n "${TELEMETRY_DECODER_ARCHIVE:-}" ]; then
  case "$TELEMETRY_DECODER_ARCHIVE" in /*) ;; *) exit 2 ;; esac
  [ -f "$TELEMETRY_DECODER_ARCHIVE" ] && [ ! -L "$TELEMETRY_DECODER_ARCHIVE" ] || exit 2
  # Sharing is mandatory when prepared: stale/incompatible archives fail instead of falling back.
  JVM+=(-Xshare:on "-XX:SharedArchiveFile=$TELEMETRY_DECODER_ARCHIVE")
fi
exec timeout --foreground -k 5s 20s java "${JVM[@]}" -Djava.io.tmpdir="$TMPDIR" -cp "$CP" org.countly.kafkajs.telemetry.FullTelemetryDecoder "$RAW"
