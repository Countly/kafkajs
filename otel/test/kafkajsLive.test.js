const { describe, it, before, after } = require('node:test')
const assert = require('node:assert')
const crypto = require('node:crypto')
const { SpanKind } = require('@opentelemetry/api')
const {
  getTestSpans,
  initMeterProvider,
  mochaHooks,
  registerInstrumentationTesting,
} = require('@opentelemetry/contrib-test-utils')
const { KafkaJsInstrumentation } = require('../src')
const { writeEvidence } = require('../../testHelpers/e2eEvidence.js')

// Registered before the client is required, as an application must.
const instrumentation = registerInstrumentationTesting(new KafkaJsInstrumentation())
const { Kafka, logLevel } = require('@countly/kafkajs')

// Gated: it needs the compose cluster. Run alone: `npm run test:e2e:otel`.
const live = process.env.OTEL_E2E === '1'

const EVIDENCE = 'otel'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 90000
const CONSUME_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
// The compose cluster's plain-text listeners.
const BROKERS = ['localhost:9092', 'localhost:9095', 'localhost:9098']

/**
 * @param {Promise<T>} promise
 * @param {number} ms
 * @param {string} what named in the timeout error
 * @returns {Promise<T>} the promise's outcome, or a rejection once `ms` has passed
 * @template T
 */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} took longer than ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

describe('a producer to consumer round trip on a live cluster', { skip: !live }, () => {
  const id = crypto.randomBytes(6).toString('hex')
  const topic = `e2e-p8-otel-${id}`
  const kafka = new Kafka({
    clientId: `e2e-p8-otel-${id}`,
    brokers: BROKERS,
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `e2e-p8-otel-${id}` })
  let metricReader

  before(
    async () => {
      mochaHooks.beforeAll(() => {})
      mochaHooks.beforeEach(() => {})
      metricReader = initMeterProvider(instrumentation)
      await admin.connect()
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })
    },
    { timeout: SCENARIO_BUDGET_MS }
  )

  after(
    async () => {
      await admin.deleteTopics({ topics: [topic] }).catch(() => {})
      await Promise.allSettled(
        [consumer, producer, admin].map((client) =>
          within(client.disconnect(), DISCONNECT_BUDGET_MS, 'disconnect')
        )
      )
      instrumentation.disable()
    },
    { timeout: SCENARIO_BUDGET_MS }
  )

  it(
    'puts the consumer process span in the producer span trace and records Produce and Fetch durations',
    { timeout: SCENARIO_BUDGET_MS },
    async () => {
      await producer.connect()
      await producer.send({ topic, messages: [{ key: 'otel', value: 'round trip' }] })

      let consumed
      const received = new Promise((resolve) => {
        consumed = resolve
      })
      await consumer.connect()
      await consumer.subscribe({ topics: [topic], fromBeginning: true })
      await consumer.run({ eachMessage: async ({ message }) => consumed(message) })
      await within(received, CONSUME_BUDGET_MS, 'consuming the message')
      await consumer.stop()

      const spans = getTestSpans()
      const send = spans.find((s) => s.name === `send ${topic}` && s.kind === SpanKind.PRODUCER)
      const processed = spans.find(
        (s) => s.name === `process ${topic}` && s.kind === SpanKind.CONSUMER
      )
      const { resourceMetrics } = await metricReader.collect()
      const duration = resourceMetrics.scopeMetrics
        .flatMap(({ metrics }) => metrics)
        .find(({ descriptor }) => descriptor.name === 'messaging.client.operation.duration')
      const operations = {}
      for (const point of duration ? duration.dataPoints : []) {
        const name = point.attributes['messaging.operation.name']
        operations[name] = (operations[name] || 0) + point.value.count
      }

      const traceId = send?.spanContext().traceId
      const sendSpanId = send?.spanContext().spanId
      const processParentSpanId = processed?.parentSpanContext?.spanId
      writeEvidence(EVIDENCE, {
        kafkaVersion: process.env.KAFKA_VERSION || '4.3.1',
        topic,
        sendSpan: Boolean(send),
        processSpan: Boolean(processed),
        sameTrace: Boolean(send && processed) && traceId === processed.spanContext().traceId,
        traceId,
        sendSpanId,
        processParentSpanId,
        parentIsSend: Boolean(sendSpanId) && processParentSpanId === sendSpanId,
        operations,
      })

      assert.ok(send, 'a send span')
      assert.ok(processed, 'a process span')
      assert.strictEqual(processed.spanContext().traceId, send.spanContext().traceId)
      assert.strictEqual(processed.parentSpanContext?.spanId, send.spanContext().spanId)
      assert.ok(operations.Produce >= 1, 'at least one Produce duration')
      assert.ok(operations.Fetch >= 1, 'at least one Fetch duration')
    }
  )
})
