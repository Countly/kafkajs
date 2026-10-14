const { describe, it, before, beforeEach, after } = require('node:test')
const assert = require('node:assert')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { createRequire } = require('node:module')

// Required before any instrumentation exists in this process, as an application that sets up
// OpenTelemetry too late does.
const early = require('@countly/kafkajs')

const { SpanStatusCode } = require('@opentelemetry/api')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { ATTR_ERROR_TYPE } = require('@opentelemetry/semantic-conventions')
const {
  getTestSpans,
  initMeterProvider,
  mochaHooks,
  registerInstrumentationTesting,
} = require('@opentelemetry/contrib-test-utils')
const { KafkaJsInstrumentation } = require('../src')
const {
  ATTR_MESSAGING_SYSTEM,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_OPERATION_NAME,
  METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES,
  METRIC_MESSAGING_CLIENT_OPERATION_DURATION,
  METRIC_MESSAGING_PROCESS_DURATION,
  MESSAGING_SYSTEM_VALUE_KAFKA,
} = require('../src/semconv')
const { assertMetricCollection } = require('./utils')

const instrumentation = registerInstrumentationTesting(new KafkaJsInstrumentation())
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-otel-'))

/**
 * Installs a stand-in client under `node_modules/<name>` of its own directory and requires it,
 * so the hook sees the same path and version a real install gives it.
 * @param {string} name the directory the package is installed under
 * @param {string} version
 * @returns {{ Kafka: Function, send: Function, sendOffsets: Function, listened: string[],
 *   listeners: Function[], calls: { commit: Function, disconnect: Function } }} the stand-in's
 *   exports: `send` and `sendOffsets` are the methods of the transaction its producers resolve,
 *   `listened` the events and `listeners` the listeners its clients' `on` was called with, and
 *   `calls` what the transaction's `commit` and the producer's `disconnect` do, which a test
 *   replaces
 */
const install = (name, version) => {
  const project = fs.mkdtempSync(path.join(root, 'project-'))
  const dir = path.join(project, 'node_modules', name)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(
    path.join(dir, 'package.json'),
    JSON.stringify({ name: '@countly/kafkajs', version, main: 'index.js' })
  )
  fs.writeFileSync(
    path.join(dir, 'index.js'),
    `const send = async () => []
const sendOffsets = async () => {}
const calls = { commit: async () => {}, disconnect: async () => {} }
const transaction = { send, sendBatch: async () => [], commit: () => calls.commit(), abort: async () => {}, sendOffsets }
const listened = []
const listeners = []
const events = { REQUEST: 'request' }
const on = (event, listener) => { listened.push(event); listeners.push(listener) }
class Kafka {
  producer() { return { send: async () => [], sendBatch: async () => [], transaction: async () => transaction, disconnect: () => calls.disconnect(), events, on } }
  consumer() { return { run: async () => {}, events, on } }
}
module.exports = { Kafka, send, sendOffsets, listened, listeners, calls }`
  )
  return createRequire(path.join(project, 'index.js'))(name)
}

// The root hooks upstream's mocha loads: a test tracer provider with an in-memory exporter, reset
// before each test.
before(() => mochaHooks.beforeAll(() => {}))
beforeEach(() => mochaHooks.beforeEach(() => {}))

after(() => {
  instrumentation.disable()
  fs.rmSync(root, { recursive: true, force: true })
})

/**
 * @returns {import('@opentelemetry/sdk-trace').ReadableSpan[]} the `transaction` spans ended so far
 */
const transactionSpans = () => getTestSpans().filter(({ name }) => name === 'transaction')

/**
 * @returns {{ topic: string, partition: number, message: { headers: object } }} an `eachMessage`
 *   payload
 */
const eachMessagePayload = () => ({ topic: 'topic-name-1', partition: 1, message: { headers: {} } })

/**
 * @returns {{ batch: { topic: string, partition: number, messages: Array<{ headers: object }> } }}
 *   an `eachBatch` payload of two messages
 */
const eachBatchPayload = () => ({
  batch: {
    topic: 'topic-name-1',
    partition: 1234,
    messages: [{ headers: {} }, { headers: {} }],
  },
})

describe('fork-only', () => {
  it('leaves a client required before the instrumentation was enabled unwrapped and working', () => {
    assert.strictEqual(isWrapped(early.Kafka.prototype.producer), false)
    assert.strictEqual(isWrapped(early.Kafka.prototype.consumer), false)

    const producer = new early.Kafka({ clientId: 'early', brokers: ['localhost:1'] }).producer()
    assert.strictEqual(typeof producer.send, 'function')
  })

  it('hooks the client under both of its names, without file hooks', () => {
    assert.deepStrictEqual(
      instrumentation
        .getModuleDefinitions()
        .map(({ name, supportedVersions, files }) => ({ name, supportedVersions, files })),
      [
        { name: '@countly/kafkajs', supportedVersions: ['>=3.0.0-0 <4'], files: [] },
        { name: 'kafkajs', supportedVersions: ['>=3.0.0-0 <4'], files: [] },
      ]
    )
  })

  for (const [name, version, wrapped] of [
    ['@countly/kafkajs', '3.0.0-alpha.7', true],
    ['@countly/kafkajs', '3.0.0', true],
    ['@countly/kafkajs', '3.4.1', true],
    ['@countly/kafkajs', '2.2.4', false],
    ['@countly/kafkajs', '4.0.0-alpha.1', false],
    ['@countly/kafkajs', '4.0.0', false],
    ['kafkajs', '3.0.0-alpha.7', true],
    ['kafkajs', '2.2.4', false],
    ['kafkajs', '1.16.0', false],
  ]) {
    it(`${wrapped ? 'wraps' : 'leaves alone'} ${name} ${version}`, () => {
      const { Kafka } = install(name, version)

      assert.strictEqual(isWrapped(Kafka.prototype.producer), wrapped)
      assert.strictEqual(isWrapped(Kafka.prototype.consumer), wrapped)
    })
  }

  it('listens to the REQUEST event of every client it creates, once each', () => {
    const { Kafka, listened } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const kafka = new Kafka()

    kafka.producer()
    kafka.consumer()

    assert.deepStrictEqual(listened, ['request', 'request'])
  })

  it('unwraps the factories on disable() and wraps them again on enable()', () => {
    const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')

    instrumentation.disable()
    assert.strictEqual(isWrapped(Kafka.prototype.producer), false)
    assert.strictEqual(isWrapped(Kafka.prototype.consumer), false)

    instrumentation.enable()
    assert.ok(isWrapped(Kafka.prototype.producer))
    assert.ok(isWrapped(Kafka.prototype.consumer))
  })

  it('keeps instrumenting the clients it built after disable(), and leaves clients built afterwards alone', async () => {
    const hooked = []
    instrumentation.setConfig({
      producerHook: (_span, { topic }) => hooked.push(`send ${topic}`),
      consumerHook: (_span, { topic }) => hooked.push(`process ${topic}`),
    })
    const { Kafka, listened } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const kafka = new Kafka()
    const producer = kafka.producer()
    const config = { eachMessage: async () => {} }
    await kafka.consumer().run(config)

    instrumentation.disable()
    try {
      const later = kafka.producer()
      await producer.send({ topic: 'before', messages: [{ value: 'a' }] })
      await later.send({ topic: 'after', messages: [{ value: 'b' }] })
      await config.eachMessage({ topic: 'before', partition: 0, message: { headers: {} } })

      assert.deepStrictEqual(hooked, ['send before', 'process before'])
      assert.strictEqual(isWrapped(later.send), false)
      assert.deepStrictEqual(listened, ['request', 'request'])
    } finally {
      instrumentation.enable()
    }
  })

  it('passes transaction.sendOffsets through unwrapped', async () => {
    const { Kafka, send, sendOffsets } = install('@countly/kafkajs', '3.0.0-alpha.7')

    const transaction = await new Kafka().producer().transaction()

    assert.notStrictEqual(transaction.send, send)
    assert.strictEqual(transaction.sendOffsets, sendOffsets)
    await transaction.commit()
  })
})

describe('fork-only: the transaction span of a commit that can be pending or joined', () => {
  const unknownOutcome = () =>
    Object.assign(new Error('the commit did not complete'), { retriable: true })

  it('returns a distinct promise to each of two joined commit() calls and ends one span with the outcome', async () => {
    const { Kafka, calls } = install('@countly/kafkajs', '3.0.0-alpha.7')
    let complete
    const joined = new Promise((resolve) => {
      complete = resolve
    })
    calls.commit = () => joined
    const transaction = await new Kafka().producer().transaction()

    const first = transaction.commit()
    const second = transaction.commit()
    complete()
    await Promise.all([first, second])

    assert.notStrictEqual(first, second)
    assert.deepStrictEqual(
      transactionSpans().map(({ status }) => status.code),
      [SpanStatusCode.OK]
    )
  })

  it('ends the span with the error of a commit whose outcome is unknown, and a later commit() that succeeds is not recorded on it', async () => {
    const { Kafka, calls } = install('@countly/kafkajs', '3.0.0-alpha.7')
    let attempts = 0
    calls.commit = async () => {
      if (++attempts === 1) {
        throw unknownOutcome()
      }
    }
    const transaction = await new Kafka().producer().transaction()

    await assert.rejects(transaction.commit(), { retriable: true })
    assert.deepStrictEqual(
      transactionSpans().map(({ status, attributes }) => [status.code, status.message, attributes]),
      [[SpanStatusCode.ERROR, 'the commit did not complete', { 'error.type': 'Error' }]]
    )

    await transaction.commit()

    assert.strictEqual(attempts, 2)
    assert.deepStrictEqual(
      transactionSpans().map(({ status }) => status.code),
      [SpanStatusCode.ERROR]
    )
  })

  it('adds no second transaction span when producer.disconnect() follows a commit whose outcome is unknown', async () => {
    const { Kafka, calls } = install('@countly/kafkajs', '3.0.0-alpha.7')
    let disconnected = false
    calls.commit = async () => {
      throw unknownOutcome()
    }
    calls.disconnect = async () => {
      disconnected = true
    }
    const producer = new Kafka().producer()
    const transaction = await producer.transaction()

    await assert.rejects(transaction.commit(), { retriable: true })
    assert.strictEqual(transactionSpans().length, 1)
    await producer.disconnect()

    assert.ok(disconnected)
    assert.deepStrictEqual(
      transactionSpans().map(({ status }) => status.code),
      [SpanStatusCode.ERROR]
    )
  })
})

describe('fork-only: the duration histograms', () => {
  // The boundaries of `@opentelemetry/instrumentation-kafkajs` 0.32.0, in seconds.
  const upstreamBoundaries = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
  ]

  it('export every bucket boundary of the upstream package, on both duration metrics', async () => {
    const metricReader = initMeterProvider(instrumentation)
    const { Kafka, listeners } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const kafka = new Kafka()
    const config = { eachMessage: async () => {} }
    kafka.producer()
    await kafka.consumer().run(config)

    listeners[0]({ payload: { broker: 'localhost:9092', duration: 250, apiName: 'Produce' } })
    await config.eachMessage(eachMessagePayload())

    const { resourceMetrics } = await metricReader.collect()
    const boundaries = Object.fromEntries(
      resourceMetrics.scopeMetrics[0].metrics
        .filter(({ descriptor }) => descriptor.name.endsWith('.duration'))
        .map(({ descriptor, dataPoints }) => [
          descriptor.name,
          dataPoints[0].value.buckets.boundaries,
        ])
    )
    assert.deepStrictEqual(boundaries, {
      [METRIC_MESSAGING_CLIENT_OPERATION_DURATION]: upstreamBoundaries,
      [METRIC_MESSAGING_PROCESS_DURATION]: upstreamBoundaries,
    })
  })
})

describe('fork-only: a handler that returns a plain value, not a promise', () => {
  const processAttributes = (partition) => ({
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_DESTINATION_NAME]: 'topic-name-1',
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
  })

  it('resolves eachMessage with the value, ends the process span and records the metrics', async () => {
    const metricReader = initMeterProvider(instrumentation)
    const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const config = { eachMessage: () => 7 }
    await new Kafka().consumer().run(config)

    const returned = await config.eachMessage(eachMessagePayload())

    assert.strictEqual(returned, 7)
    assert.deepStrictEqual(
      getTestSpans().map(({ name }) => name),
      ['process topic-name-1']
    )
    assertMetricCollection(await metricReader.collect(), {
      [METRIC_MESSAGING_PROCESS_DURATION]: [{ count: 1, attributes: processAttributes('1') }],
      [METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES]: [
        { value: 1, attributes: processAttributes('1') },
      ],
    })
  })

  it('resolves eachBatch with the value, ends the poll and process spans and records the metrics', async () => {
    const metricReader = initMeterProvider(instrumentation)
    const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const config = { eachBatch: () => 8 }
    await new Kafka().consumer().run(config)

    const returned = await config.eachBatch(eachBatchPayload())

    assert.strictEqual(returned, 8)
    assert.deepStrictEqual(
      getTestSpans().map(({ name }) => name),
      ['poll topic-name-1', 'process topic-name-1', 'process topic-name-1']
    )
    assertMetricCollection(await metricReader.collect(), {
      [METRIC_MESSAGING_PROCESS_DURATION]: [{ count: 2, attributes: processAttributes('1234') }],
      [METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES]: [
        { value: 2, attributes: processAttributes('1234') },
      ],
    })
  })
})

describe('fork-only: a handler that fails', () => {
  const failureAttributes = (partition, errorType) => ({
    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
    [ATTR_MESSAGING_DESTINATION_NAME]: 'topic-name-1',
    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition,
    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
    [ATTR_ERROR_TYPE]: errorType,
  })

  /**
   * @param {Promise<unknown>} pending
   * @returns {Promise<unknown>} the reason `pending` rejects with; fails the test when it resolves
   */
  const rejection = (pending) =>
    pending.then(
      () => assert.fail('the call resolved'),
      (reason) => reason
    )

  /**
   * Asserts that exactly the named spans ended, each in ERROR status with the same message and
   * error type.
   * @param {string[]} names
   * @param {string | undefined} message
   * @param {string} errorType
   */
  const assertSpansFailed = (names, message, errorType) => {
    const spans = getTestSpans()
    assert.deepStrictEqual(
      spans.map(({ name }) => name),
      names
    )
    for (const span of spans) {
      assert.strictEqual(span.status.code, SpanStatusCode.ERROR)
      assert.strictEqual(span.status.message, message)
      assert.strictEqual(span.attributes[ATTR_ERROR_TYPE], errorType)
    }
  }

  it('ends the process span and records the failure when eachMessage throws synchronously', async () => {
    const metricReader = initMeterProvider(instrumentation)
    const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const error = new Error('thrown synchronously')
    let called = false
    const config = {
      eachMessage: () => {
        called = true
        throw error
      },
    }
    await new Kafka().consumer().run(config)

    const pending = config.eachMessage(eachMessagePayload())

    assert.ok(called, 'the handler was not called synchronously')
    assert.strictEqual(await rejection(pending), error)
    assertSpansFailed(['process topic-name-1'], 'thrown synchronously', 'Error')
    assertMetricCollection(await metricReader.collect(), {
      [METRIC_MESSAGING_PROCESS_DURATION]: [
        { count: 1, attributes: failureAttributes('1', 'Error') },
      ],
      [METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES]: [
        { value: 1, attributes: failureAttributes('1', 'Error') },
      ],
    })
  })

  it('ends the poll and process spans and records the failure when eachBatch throws synchronously', async () => {
    const metricReader = initMeterProvider(instrumentation)
    const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')
    const error = new Error('thrown synchronously')
    let called = false
    const config = {
      eachBatch: () => {
        called = true
        throw error
      },
    }
    await new Kafka().consumer().run(config)

    const pending = config.eachBatch(eachBatchPayload())

    assert.ok(called, 'the handler was not called synchronously')
    assert.strictEqual(await rejection(pending), error)
    assertSpansFailed(
      ['poll topic-name-1', 'process topic-name-1', 'process topic-name-1'],
      'thrown synchronously',
      'Error'
    )
    assertMetricCollection(await metricReader.collect(), {
      [METRIC_MESSAGING_PROCESS_DURATION]: [
        { count: 2, attributes: failureAttributes('1234', 'Error') },
      ],
      [METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES]: [
        { value: 2, attributes: failureAttributes('1234', 'Error') },
      ],
    })
  })

  for (const [title, reason, message, errorType] of [
    ['null', null, undefined, '_OTHER'],
    [
      'an object with a message and no prototype',
      Object.assign(Object.create(null), { message: 'no prototype' }),
      'no prototype',
      '_OTHER',
    ],
  ]) {
    it(`passes on ${title} that eachMessage rejects with, and records the failure`, async () => {
      const metricReader = initMeterProvider(instrumentation)
      const { Kafka } = install('@countly/kafkajs', '3.0.0-alpha.7')
      const config = { eachMessage: () => Promise.reject(reason) }
      await new Kafka().consumer().run(config)

      assert.strictEqual(await rejection(config.eachMessage(eachMessagePayload())), reason)
      assertSpansFailed(['process topic-name-1'], message, errorType)
      assertMetricCollection(await metricReader.collect(), {
        [METRIC_MESSAGING_PROCESS_DURATION]: [
          { count: 1, attributes: failureAttributes('1', errorType) },
        ],
        [METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES]: [
          { value: 1, attributes: failureAttributes('1', errorType) },
        ],
      })
    })
  }
})
