const { test } = require('node:test')
const assert = require('node:assert')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { KafkaJsInstrumentation } = require('../../src')

// This file runs under `--import ./test/register/register.mjs`, which leaves `registered` or the
// reason it could not register. Run without it, `state` is undefined and the test fails.
const state = globalThis[Symbol.for('kafkajs-otel-test.syncHooks')]

test(
  'an ESM import of the client gets wrapped factories under the synchronous registration hooks',
  { skip: state === 'registered' ? false : state },
  async (t) => {
    t.diagnostic(`synchronous hooks: ${state}`)
    const instrumentation = new KafkaJsInstrumentation()
    const { Kafka } = await import('@countly/kafkajs')

    assert.ok(isWrapped(Kafka.prototype.producer))
    assert.ok(isWrapped(Kafka.prototype.consumer))
    instrumentation.disable()
  }
)
