const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const { execFile } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')
const { promisify } = require('node:util')

const otelRoot = path.join(__dirname, '..')
const aliasDir = path.join(otelRoot, 'node_modules', 'kafkajs')

// Runs in a fresh process with this sub-package as its working directory. It loads the client by
// the alias name only, stubs the producer's `send` the way `kafkajs.test.js` does (no broker),
// sends one message and prints what the instrumentation recorded.
const CHILD = `
const { isWrapped } = require('@opentelemetry/instrumentation')
const {
  getTestSpans,
  initMeterProvider,
  mochaHooks,
  registerInstrumentationTesting,
} = require('@opentelemetry/contrib-test-utils')
const { KafkaJsInstrumentation } = require('./src')

const instrumentation = registerInstrumentationTesting(new KafkaJsInstrumentation())
mochaHooks.beforeAll(() => {})
const metricReader = initMeterProvider(instrumentation)

const main = async () => {
  const { Kafka } = process.argv[1] === 'esm' ? await import('kafkajs') : require('kafkajs')
  const wrappedAtLoad = isWrapped(Kafka.prototype.producer)

  instrumentation.disable()
  const factory = Kafka.prototype.producer
  Kafka.prototype.producer = function (...args) {
    const producer = factory.apply(this, args)
    producer.send = async ({ topic, messages }) =>
      messages.map(() => ({ topicName: topic, partition: 0, errorCode: 0 }))
    return producer
  }
  instrumentation.enable()

  const producer = new Kafka({ clientId: 'alias', brokers: ['localhost:1'] }).producer()
  const sent = await producer.send({ topic: 'alias-topic', messages: [{ value: 'one' }] })
  const { errors, resourceMetrics } = await metricReader.collect()
  instrumentation.disable()

  process.stdout.write(JSON.stringify({
    wrappedAtLoad,
    sent,
    spans: getTestSpans().map((span) => ({
      name: span.name,
      kind: span.kind,
      status: span.status.code,
      topic: span.attributes['messaging.destination.name'],
    })),
    errors: errors.length,
    metrics: resourceMetrics.scopeMetrics.flatMap(({ metrics }) =>
      metrics.map(({ descriptor, dataPoints }) => ({
        name: descriptor.name,
        points: dataPoints.map(({ value, attributes }) => ({
          value,
          topic: attributes['messaging.destination.name'],
        })),
      }))
    ),
  }))
}

main().catch((error) => {
  process.stderr.write(String(error && error.stack))
  process.exitCode = 1
})
`

/**
 * Runs the child under the given Node.js flags and returns what it recorded.
 * @param {string[]} flags Node.js options placed before the script
 * @param {'cjs'|'esm'} mode how the child loads `kafkajs`
 * @returns {Promise<object>} the child's report
 */
const runChild = async (flags, mode) => {
  const { stdout } = await promisify(execFile)(process.execPath, [...flags, '-e', CHILD, mode], {
    cwd: otelRoot,
    timeout: 60000,
  }).catch((error) => {
    throw new Error(`the ${mode} child failed: ${error.stderr || error.message}`)
  })
  return JSON.parse(stdout)
}

/**
 * Asserts the report of one aliased send: the factory was wrapped when the client loaded, and
 * exactly one PRODUCER `send` span and one sent-messages data point were recorded.
 * @param {object} report a child's report
 * @returns {void}
 */
const assertInstrumented = (report) => {
  assert.equal(report.wrappedAtLoad, true)
  assert.deepEqual(report.sent, [{ topicName: 'alias-topic', partition: 0, errorCode: 0 }])
  // SpanKind.PRODUCER is 3; SpanStatusCode.UNSET is 0.
  assert.deepEqual(report.spans, [
    { name: 'send alias-topic', kind: 3, status: 0, topic: 'alias-topic' },
  ])
  assert.equal(report.errors, 0)
  assert.deepEqual(report.metrics, [
    { name: 'messaging.client.sent.messages', points: [{ value: 1, topic: 'alias-topic' }] },
  ])
}

describe('the client installed under the npm alias kafkajs', () => {
  it('CJS require of the aliased kafkajs is instrumented', async () => {
    const stat = fs.lstatSync(aliasDir)
    assert.equal(stat.isSymbolicLink(), false, 'node_modules/kafkajs must not be a link')
    assert.equal(stat.isDirectory(), true)
    const manifest = JSON.parse(fs.readFileSync(path.join(aliasDir, 'package.json'), 'utf8'))
    assert.equal(manifest.name, '@countly/kafkajs')

    assertInstrumented(await runChild([], 'cjs'))
  })

  it('ESM import of the aliased kafkajs is instrumented', async () => {
    assertInstrumented(
      await runChild(['--experimental-loader=@opentelemetry/instrumentation/hook.mjs'], 'esm')
    )
  })
})
