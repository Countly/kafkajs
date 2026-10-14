const { test } = require('node:test')
const assert = require('node:assert')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { KafkaJsInstrumentation } = require('../../src')

// This file runs without a loader hook, as an ESM application started with plain `node` does.
test('an ESM import of the client without the loader hook wraps nothing and throws nothing', async () => {
  const instrumentation = new KafkaJsInstrumentation()
  const { Kafka } = await import('@countly/kafkajs')

  assert.strictEqual(isWrapped(Kafka.prototype.producer), false)
  assert.strictEqual(isWrapped(Kafka.prototype.consumer), false)

  const producer = new Kafka({ clientId: 'esm', brokers: ['localhost:1'] }).producer()
  assert.strictEqual(typeof producer.send, 'function')
  instrumentation.disable()
})
