const { test } = require('node:test')
const assert = require('node:assert')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { KafkaJsInstrumentation } = require('../../src')

// This file runs under `--import ./test/register/register.mjs`, which leaves `registered` or the
// reason it could not register. Only a Node.js that cannot register synchronous hooks skips; any
// other state, an unset one included, fails.
const state = globalThis[Symbol.for('kafkajs-otel-test.syncHooks')]
const NO_NODE_HOOKS = 'this Node.js has no synchronous module hooks'

test(
  'an ESM import of the client gets wrapped factories under the synchronous registration hooks',
  { skip: state === NO_NODE_HOOKS ? state : false },
  async (t) => {
    t.diagnostic(`synchronous hooks: ${state}`)
    assert.equal(state, 'registered')
    const instrumentation = new KafkaJsInstrumentation()
    const { Kafka } = await import('@countly/kafkajs')

    assert.ok(isWrapped(Kafka.prototype.producer))
    assert.ok(isWrapped(Kafka.prototype.consumer))
    instrumentation.disable()
  }
)
