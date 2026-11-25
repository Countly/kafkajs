#!/bin/bash
# Installs a packed OTel instrumentation and the packed client it instruments, exactly those two
# files, into a throwaway project. A CommonJS and an ES module child then load the instrumentation,
# register it with the installed client and observe one span from a stubbed send. The same project
# compiles strict TypeScript consumers of the instrumentation, one fixture and one compiler run
# each, and a static import of the client is observed with the instrumentation started from a
# preload. A second project, a sibling of the first, holds the client only under the npm alias
# `kafkajs`: a CommonJS child, an ES module child with the loader hook and, where
# import-in-the-middle supports it, an ES module child with synchronous module hooks each observe
# a span.
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
  local name dir
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
  dir="$(cd "$(dirname "$1")" && pwd)" || {
    echo "cannot enter the directory of $1" >&2
    exit 2
  }
  echo "$dir/$(basename "$1")"
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
# TypeScript at the version the root lockfile pins, so this runs where only otel/ has its
# dependencies installed.
TS_VERSION="$(locked "$ROOT/package-lock.json" typescript)"
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"

WORK="$(mktemp -d)"
# Absolute, so the trap still finds it after the cd below when TMPDIR is relative.
WORK="$(cd "$WORK" && pwd)"
trap 'rm -rf "$WORK"' EXIT

# Installs into the project of the current directory. The cache and its logs stay in the temporary
# tree, and so do the modules and the lockfile, which npm writes into the project.
npm_install() {
  npm install --cache="$WORK/npm-cache" --loglevel=error --no-audit --no-fund --ignore-scripts "$@"
}

# Two sibling projects under $WORK, which holds no modules, so neither can resolve from the other.
SCOPED="$WORK/scoped"
ALIAS_ONLY="$WORK/alias-only"
mkdir "$SCOPED" "$ALIAS_ONLY"

# One strict TypeScript fixture per declaration form, each compiled apart so that a failing form
# does not hide another.
TYPE_FIXTURES=(buffer offset date timestamp-long timestamp-null timestamp-undefined null-header header-read producer-hook consumer-hook)

(
  cd "$SCOPED"
  echo '{ "name": "otel-pack-smoke", "private": true }' > package.json
  npm_install --save "$MAIN_TGZ" "$OTEL_TGZ" \
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

  # Strict TypeScript consumers compile against the declarations in a project with no ambient types.
  npm_install --no-save "typescript@$TS_VERSION" "@types/node@^$NODE_MAJOR"
  cat > tsconfig.base.json <<'JSON'
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
  }
}
JSON
  # The declarations reference Node's types, so Buffer resolves with no ambient types.
  cat > types-buffer.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const key: KafkajsMessage['key'] = Buffer.from('k')
TS
  # A consumed message's offset, in a literal and in a hook.
  cat > types-offset.ts <<'TS'
import type { KafkaJsInstrumentationConfig, KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const consumed: KafkajsMessage = { value: 'v', offset: '1' }
export const config: KafkaJsInstrumentationConfig = {
  consumerHook: (span, { message }) => {
    span.setAttribute('kafka.offset', message.offset ?? '')
  },
}
TS
  cat > types-date.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const dated: KafkajsMessage = { value: 'v', timestamp: new Date() }
TS
  cat > types-timestamp-long.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const long: KafkajsMessage = { value: 'v', timestamp: { value: 1n } }
TS
  cat > types-timestamp-null.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const none: KafkajsMessage = { value: 'v', timestamp: null }
TS
  cat > types-timestamp-undefined.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const none: KafkajsMessage = { value: 'v', timestamp: undefined }
TS
  cat > types-null-header.ts <<'TS'
import type { KafkajsMessage } from '@countly/opentelemetry-instrumentation-kafkajs'
export const withNull: KafkajsMessage = { value: null, headers: { traceparent: null } }
TS
  # A header read can be null, so it does not fit a type that excludes null.
  cat > types-header-read.ts <<'TS'
import type { KafkaJsInstrumentationConfig } from '@countly/opentelemetry-instrumentation-kafkajs'
export const config: KafkaJsInstrumentationConfig = {
  consumerHook: (_span, { message }) => {
    const header = message.headers?.traceparent
    // @ts-expect-error a header value can be null
    const value: NonNullable<unknown> | undefined = header
    void value
  },
}
TS
  cat > types-producer-hook.ts <<'TS'
import { KafkaJsInstrumentation } from '@countly/opentelemetry-instrumentation-kafkajs'
export const instrumentation = new KafkaJsInstrumentation({ producerHook: undefined })
TS
  cat > types-consumer-hook.ts <<'TS'
import { KafkaJsInstrumentation } from '@countly/opentelemetry-instrumentation-kafkajs'
export const instrumentation = new KafkaJsInstrumentation({ consumerHook: undefined })
TS
  failed=()
  for fixture in "${TYPE_FIXTURES[@]}"; do
    echo "{ \"extends\": \"./tsconfig.base.json\", \"files\": [\"types-$fixture.ts\"] }" > "tsconfig.$fixture.json"
    echo "compiling the $fixture type fixture"
    ./node_modules/.bin/tsc -p "tsconfig.$fixture.json" || failed+=("$fixture")
  done
  if [ "${#failed[@]}" -ne 0 ]; then
    echo "type fixtures failed: ${failed[*]}" >&2
    exit 1
  fi
)

# A project that holds the client only under the npm alias `kafkajs` is instrumented too: by
# require, by import with the loader hook and, where import-in-the-middle supports synchronous
# module hooks on this Node.js, by import with those hooks.
(
  cd "$ALIAS_ONLY"
  echo '{ "name": "otel-pack-smoke-alias", "private": true }' > package.json
  npm_install --save "kafkajs@file:$MAIN_TGZ" "$OTEL_TGZ" \
    "@opentelemetry/api@$API_VERSION" "@opentelemetry/sdk-trace@$SDK_VERSION"
  # The scoped project is a sibling, so nothing it installed is reachable from here: the scoped
  # name must not resolve, and the alias must resolve inside this project's own modules.
  node -e '
const path = require("path")
const scoped = process.argv[1]
try {
  require.resolve(scoped)
  console.error(`${scoped} resolves in the alias project`)
  process.exit(1)
} catch (error) {
  if (error.code !== "MODULE_NOT_FOUND") throw error
}
const alias = require.resolve("kafkajs/package.json")
const own = path.join(process.cwd(), "node_modules", "kafkajs") + path.sep
if (!alias.startsWith(own) || require(alias).name !== scoped) {
  console.error(`kafkajs resolves to ${alias}, expected the copy of ${scoped} in ${own}`)
  process.exit(1)
}
' "$MAIN_NAME"
  cp "$SCOPED/otel-smoke-check.cjs" .
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
  case "$sync_hooks" in
    true)
      node --import ./otel-register.mjs otel-alias.mjs alias-esm-register
      ;;
    false)
      echo "synchronous module hooks are not supported on this Node.js: skipped"
      ;;
    *)
      echo "the synchronous-hooks probe printed '$sync_hooks', expected true or false" >&2
      exit 1
      ;;
  esac
)
