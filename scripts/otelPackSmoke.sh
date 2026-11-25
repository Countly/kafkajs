#!/bin/bash
# Installs a packed OTel instrumentation and the packed client it instruments, exactly those two
# files, into a throwaway project. A CommonJS and an ES module child then load the instrumentation,
# register it with the installed client and observe one span from a stubbed send. The same project
# compiles a strict TypeScript consumer of the instrumentation, and a static import of the client
# is observed with the instrumentation started from a preload. A second project holds the client
# only under the npm alias `kafkajs`: a CommonJS child, an ES module child with the loader hook and,
# where import-in-the-middle supports it, an ES module child with synchronous module hooks each
# observe a span.
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

# Prints the version of package $2 that lockfile $1 pins.
locked() {
  node -p 'require(process.argv[1]).packages["node_modules/" + process.argv[2]].version' "$1" "$2"
}
# The tracing API and SDK the children use, at the versions the otel/ suite is locked to.
API_VERSION="$(locked "$ROOT/otel/package-lock.json" @opentelemetry/api)"
SDK_VERSION="$(locked "$ROOT/otel/package-lock.json" @opentelemetry/sdk-trace)"

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

exports.KafkaJsInstrumentation = KafkaJsInstrumentation

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
 * Checks the loaded client is wrapped, unwraps it, stubs the producer's send over the original
 * factory, re-registers the instrumentation over the stub and asserts that one send records
 * exactly one producer span and that disabling leaves nothing wrapped.
 * @param {{ instrumentation: KafkaJsInstrumentation, exporter: InMemorySpanExporter }} state
 * @param {Function} Kafka the client's Kafka class, as the child loaded it
 * @param {string} label the child's name, in its messages
 * @returns {Promise<void>}
 */
exports.sendOnce = async ({ instrumentation, exporter }, Kafka, label) => {
  assert.ok(isWrapped(Kafka.prototype.producer), `${label}: the client is not instrumented`)
  instrumentation.disable()
  const factory = Kafka.prototype.producer
  Kafka.prototype.producer = function (...args) {
    const producer = factory.apply(this, args)
    producer.send = async ({ topic }) => [{ topicName: topic, partition: 0, errorCode: 0 }]
    return producer
  }
  instrumentation.enable()

  const producer = new Kafka({ clientId: 'otel-pack-smoke', brokers: ['localhost:1'] }).producer()
  await producer.send({ topic: 'otel-pack-smoke', messages: [{ value: label }] })
  instrumentation.disable()
  assert.ok(!isWrapped(Kafka.prototype.producer), `${label}: disable() left the factory wrapped`)
  assert.ok(!isWrapped(factory), `${label}: the stub was built over a wrapped factory`)

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
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { KafkaJsInstrumentation } from '@countly/opentelemetry-instrumentation-kafkajs'
const smoke = createRequire(import.meta.url)('./otel-smoke-check.cjs')
assert.equal(KafkaJsInstrumentation, smoke.KafkaJsInstrumentation, 'import and require load one class')
const state = smoke.instrument()
const { Kafka } = await import('@countly/kafkajs')
await smoke.sendOnce(state, Kafka, 'esm')
JS

node otel-smoke.cjs
# The loader hook lets the instrumentation wrap an ES module import of the client.
node --experimental-loader=@opentelemetry/instrumentation/hook.mjs otel-smoke.mjs

# A static top-level import of the client is instrumented when the instrumentation is started by a
# preload, which runs before the importing module is loaded.
cat > otel-preload.mjs <<'JS'
import { createRequire } from 'node:module'
const smoke = createRequire(import.meta.url)('./otel-smoke-check.cjs')
globalThis[Symbol.for('otel-pack-smoke.state')] = smoke.instrument()
JS
cat > otel-static.mjs <<'JS'
import { createRequire } from 'node:module'
import { Kafka } from '@countly/kafkajs'
const smoke = createRequire(import.meta.url)('./otel-smoke-check.cjs')
await smoke.sendOnce(globalThis[Symbol.for('otel-pack-smoke.state')], Kafka, 'esm-static')
JS
node --experimental-loader=@opentelemetry/instrumentation/hook.mjs --import ./otel-preload.mjs otel-static.mjs

# A strict TypeScript consumer compiles against the declarations in a project with no ambient types.
# TypeScript is installed at the version the root lockfile pins, so this runs where only otel/ has
# its dependencies installed.
TS_VERSION="$(locked "$ROOT/package-lock.json" typescript)"
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
npm install --loglevel=error --no-audit --no-fund --no-save --ignore-scripts \
  "typescript@$TS_VERSION" "@types/node@^$NODE_MAJOR"
cat > tsconfig.json <<'JSON'
{
  "compilerOptions": {
    "strict": true,
    "exactOptionalPropertyTypes": true,
    "module": "node16",
    "moduleResolution": "node16",
    "target": "es2022",
    "types": [],
    "skipLibCheck": false,
    "noEmit": true
  },
  "files": ["otel-types.ts"]
}
JSON
cat > otel-types.ts <<'TS'
import { KafkaJsInstrumentation, type KafkaJsInstrumentationConfig, type KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
const withNull: KafkajsMessage = { value: null, offset: '1', headers: { traceparent: null }, timestamp: new Date() }
const config: KafkaJsInstrumentationConfig = {
  producerHook: undefined,
  consumerHook: (span, { message }) => {
    const header = message.headers?.traceparent
    // @ts-expect-error a header value can be null
    const value: Buffer | string | (Buffer | string)[] | undefined = header
    span.setAttribute('kafka.offset', message.offset ?? '')
    void value
  },
}
void [withNull, new KafkaJsInstrumentation(config)]
TS
./node_modules/.bin/tsc -p .

# A project that holds the client only under the npm alias `kafkajs` is instrumented too: by
# require, by import with the loader hook and, where import-in-the-middle supports synchronous
# module hooks on this Node.js, by import with those hooks.
ALIAS_ONLY="$WORK/alias-only"
mkdir "$ALIAS_ONLY"
(
  cd "$ALIAS_ONLY"
  echo '{ "name": "otel-pack-smoke-alias", "private": true }' > package.json
  npm install --loglevel=error --no-audit --no-fund --ignore-scripts "kafkajs@file:$MAIN_TGZ" "$OTEL_TGZ" \
    "@opentelemetry/api@$API_VERSION" "@opentelemetry/sdk-trace@$SDK_VERSION"
  if [ -e "node_modules/$MAIN_NAME" ] || [ "$(node -p 'require("./node_modules/kafkajs/package.json").name')" != "$MAIN_NAME" ]; then
    echo "the client is not installed only under the alias kafkajs" >&2
    exit 1
  fi
  cp "$WORK/otel-smoke-check.cjs" .
  cat > otel-alias.cjs <<'JS'
const smoke = require('./otel-smoke-check.cjs')
const state = smoke.instrument()
const { Kafka } = require('kafkajs')
smoke.sendOnce(state, Kafka, 'alias-cjs').catch((error) => {
  console.error(error)
  process.exitCode = 1
})
JS
  cat > otel-alias.mjs <<'JS'
import { createRequire } from 'node:module'
const smoke = createRequire(import.meta.url)('./otel-smoke-check.cjs')
const state = smoke.instrument()
const { Kafka } = await import('kafkajs')
await smoke.sendOnce(state, Kafka, process.argv[2])
JS
  cat > otel-register.mjs <<'JS'
import { register } from 'import-in-the-middle/register-hooks.mjs'
register()
JS
  node otel-alias.cjs
  node --experimental-loader=@opentelemetry/instrumentation/hook.mjs otel-alias.mjs alias-esm-loader
  # Assigned apart from the test: a failed probe must stop the script, not skip the child.
  sync_hooks="$(node --input-type=module -e "import { supportsSyncHooks } from 'import-in-the-middle/register-hooks.mjs'; process.stdout.write(String(supportsSyncHooks()))")"
  if [ "$sync_hooks" = true ]; then
    node --import ./otel-register.mjs otel-alias.mjs alias-esm-register
  else
    echo "synchronous module hooks are not supported on this Node.js: skipped"
  fi
)
