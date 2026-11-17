#!/bin/bash
# Installs a packed OTel instrumentation and the packed client it instruments, exactly those two
# files, into a throwaway project. A CommonJS and an ES module child then load the instrumentation,
# register it with the installed client and observe one span from a stubbed send.
# Usage: otelPackSmoke.sh <otel tarball.tgz> <main tarball.tgz>
# The main tarball is the one already packed for the same run; nothing here packs.
set -eo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OTEL_NAME=@countly/opentelemetry-instrumentation-kafkajs
MAIN_NAME=@countly/kafkajs

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <otel tarball.tgz> <main tarball.tgz>" >&2
  exit 2
fi

# Prints the absolute path of $1 after checking it is a regular .tgz file that packs package $2.
tarball_of() {
  local name
  if [[ "$1" != *.tgz ]] || [ ! -f "$1" ]; then
    echo "not a regular .tgz file: $1" >&2
    exit 2
  fi
  name="$(tar -xOzf "$1" package/package.json | node -p 'JSON.parse(require("fs").readFileSync(0, "utf8")).name')" || {
    echo "cannot read package/package.json from $1" >&2
    exit 2
  }
  if [ "$name" != "$2" ]; then
    echo "$1 holds $name, expected $2" >&2
    exit 2
  fi
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}
OTEL_TGZ="$(tarball_of "$1" "$OTEL_NAME")"
MAIN_TGZ="$(tarball_of "$2" "$MAIN_NAME")"

# The tracing API and SDK the children use, at the versions the otel/ suite is locked to.
locked() {
  node -p 'require(process.argv[1]).packages["node_modules/" + process.argv[2]].version' \
    "$ROOT/otel/package-lock.json" "$1"
}
API_VERSION="$(locked @opentelemetry/api)"
SDK_VERSION="$(locked @opentelemetry/sdk-trace)"

WORK="$(mktemp -d)"
# Absolute, so the trap still finds it after the cd below when TMPDIR is relative.
WORK="$(cd "$WORK" && pwd)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
echo '{ "name": "otel-pack-smoke", "private": true }' > package.json
npm install --loglevel=error --no-audit --no-fund --ignore-scripts "$MAIN_TGZ" "$OTEL_TGZ" \
  "@opentelemetry/api@$API_VERSION" "@opentelemetry/sdk-trace@$SDK_VERSION"

# Each name has exactly one installed copy, resolved from its supplied tarball, never the registry.
node -e '
const lock = require("./package-lock.json")
for (const [name, tarball] of [[process.argv[1], process.argv[2]], [process.argv[3], process.argv[4]]]) {
  const copies = Object.keys(lock.packages).filter((key) => key.endsWith(`node_modules/${name}`))
  const resolved = lock.packages[`node_modules/${name}`]?.resolved ?? ""
  const base = tarball.slice(tarball.lastIndexOf("/") + 1)
  if (copies.length !== 1 || !resolved.startsWith("file:") || !resolved.endsWith(base)) {
    console.error(`${name} is not installed once from ${tarball}: ${JSON.stringify(copies)} ${resolved}`)
    process.exit(1)
  }
}
' "$OTEL_NAME" "$OTEL_TGZ" "$MAIN_NAME" "$MAIN_TGZ"

cat > otel-smoke-check.cjs <<'JS'
const assert = require('node:assert/strict')
const { SpanKind } = require('@opentelemetry/api')
const { isWrapped } = require('@opentelemetry/instrumentation')
const {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} = require('@opentelemetry/sdk-trace')
const { KafkaJsInstrumentation } = require('@countly/opentelemetry-instrumentation-kafkajs')

/**
 * Enables the instrumentation with an in-memory span exporter, before the client loads.
 * @returns {{ instrumentation: KafkaJsInstrumentation, exporter: InMemorySpanExporter }}
 */
exports.instrument = () => {
  const exporter = new InMemorySpanExporter()
  const instrumentation = new KafkaJsInstrumentation()
  instrumentation.setTracerProvider(
    new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] })
  )
  return { instrumentation, exporter }
}

/**
 * Checks the loaded client is wrapped, stubs the producer's send, re-registers the instrumentation
 * over the stub and asserts that one send records exactly one producer span.
 * @param {{ instrumentation: KafkaJsInstrumentation, exporter: InMemorySpanExporter }} state
 * @param {Function} Kafka the client's Kafka class, as the child loaded it
 * @param {string} label the child's module system
 * @returns {Promise<void>}
 */
exports.sendOnce = async ({ instrumentation, exporter }, Kafka, label) => {
  assert.ok(isWrapped(Kafka.prototype.producer), `${label}: the client is not instrumented`)
  const factory = Kafka.prototype.producer
  Kafka.prototype.producer = function (...args) {
    const producer = factory.apply(this, args)
    producer.send = async ({ topic }) => [{ topicName: topic, partition: 0, errorCode: 0 }]
    return producer
  }
  instrumentation.disable()
  instrumentation.enable()

  const producer = new Kafka({ clientId: 'otel-pack-smoke', brokers: ['localhost:1'] }).producer()
  await producer.send({ topic: 'otel-pack-smoke', messages: [{ value: label }] })
  instrumentation.disable()

  const spans = exporter.getFinishedSpans().map((span) => [span.name, span.kind])
  assert.deepEqual(spans, [['send otel-pack-smoke', SpanKind.PRODUCER]])
  console.log(`${label}: one producer span`)
}
JS
cat > otel-smoke.cjs <<'JS'
const smoke = require('./otel-smoke-check.cjs')
const state = smoke.instrument()
const { Kafka } = require('@countly/kafkajs')
smoke.sendOnce(state, Kafka, 'cjs').catch((error) => {
  console.error(error)
  process.exitCode = 1
})
JS
cat > otel-smoke.mjs <<'JS'
import { createRequire } from 'node:module'
const smoke = createRequire(import.meta.url)('./otel-smoke-check.cjs')
const state = smoke.instrument()
const { Kafka } = await import('@countly/kafkajs')
await smoke.sendOnce(state, Kafka, 'esm')
JS

node otel-smoke.cjs
# The loader hook lets the instrumentation wrap an ES module import of the client.
node --experimental-loader=@opentelemetry/instrumentation/hook.mjs otel-smoke.mjs
