const { test } = require('node:test')
const assert = require('node:assert')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { KafkaJsInstrumentation } = require('../../src')

// This file runs under `--experimental-loader=@opentelemetry/instrumentation/hook.mjs`.
test('an ESM import of the client gets wrapped factories under the loader hook', async () => {
  const instrumentation = new KafkaJsInstrumentation()
  const { Kafka } = await import('@countly/kafkajs')

  assert.ok(isWrapped(Kafka.prototype.producer))
  assert.ok(isWrapped(Kafka.prototype.consumer))

  const producer = new Kafka({ clientId: 'esm', brokers: ['localhost:1'] }).producer()
  assert.ok(isWrapped(producer.send))
  instrumentation.disable()
})
