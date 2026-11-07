const { execFile } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')
const { promisify } = require('node:util')
const {
  PublicTelemetryRound,
  TELEMETRY_KINDS,
  assertTelemetryOutcome,
} = require('../../../testHelpers/lifecycle')
const gzip = require('../../protocol/message/compression/gzip')
const zstd = require('../../protocol/message/compression/zstd')
const otlp = require('../otlp')
const Connection = require('../../network/connection')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

/** Installs one counted private failure; restoration belongs to the round.
 * @param {PublicTelemetryRound} round owner @param {object} target function owner
 * @param {string} key method @param {*} reason private sentinel
 * @param {boolean} [reject] promise failure @returns {{hits:number}} live observation
 */
function failing(round, target, key, reason, reject = false) {
  const counter = { hits: 0 }
  round.patch(
    target,
    key,
    () =>
      function () {
        counter.hits++
        if (reject) return Promise.reject(reason)
        throw reason
      }
  )
  return counter
}

/** Probes the disabled path at every private collaborator without constructing telemetry.
 * @param {PublicTelemetryRound} round disabled owner @param {*} reason unique private failure
 * @returns {{hits:number}} zero only if no optional work ran
 */
function disabledProbe(round, reason) {
  const counter = { hits: 0 }
  const Metrics = require('../metrics')
  const Payload = require('../payload')
  const Client = require('../client')
  const { TelemetryFactory } = require('../factory')
  const targets = [
    ...[
      'observeConnection',
      'observeRequest',
      'snapshot',
      'select',
      'written',
      'abandon',
      'resetTransient',
    ].map((key) => [Metrics.prototype, key]),
    [Payload.prototype, 'buildPayload'],
    [Client.prototype, 'run'],
    [Client.prototype, 'warn'],
    [TelemetryFactory.prototype, 'captureResources'],
    [gzip, 'compress'],
    [zstd, 'compress'],
    [otlp, 'measureMetricsData'],
    [otlp, 'encodeMetricsData'],
  ]
  for (const [target, key] of targets)
    round.patch(
      target,
      key,
      () =>
        function () {
          counter.hits++
          throw reason
        }
    )
  return counter
}

/** Arms a failure at the real response decoder, selecting only private connections.
 * @param {PublicTelemetryRound} round owner @param {*} reason private sentinel
 * @param {number} apiKey request key @returns {{hits:number}} decoder visits
 */
function decodeFailure(round, reason, apiKey) {
  const counter = { hits: 0 }
  round.patch(
    Connection.prototype,
    'send',
    (original) =>
      function (options) {
        if (!this.authenticationOwner || options.request.apiKey !== apiKey)
          return original.call(this, options)
        return original.call(this, {
          ...options,
          response: {
            ...options.response,
            decode: async () => {
              counter.hits++
              throw reason
            },
          },
        })
      }
  )
  return counter
}

/** Each entry installs exactly one independently reached failure in a real private collaborator. */
const INJECTORS = {
  O02: { early: true, inject: (r, s) => failing(r, r.metrics, 'observeConnection', s) },
  O03: { early: true, inject: (r, s) => failing(r, r.metrics, 'observeRequest', s) },
  'O04 reader': { inject: (r, s) => failing(r, r.binding, 'resourceReader', s) },
  'O04 currentness': {
    inject: (r, s) => {
      const counter = { hits: 0 }
      r.patch(r.binding, 'resourceReader', () => () => ({
        values: {},
        isCurrent() {
          counter.hits++
          throw s
        },
      }))
      return counter
    },
  },
  'O05 snapshot': { inject: (r, s) => failing(r, r.metrics, 'snapshot', s) },
  'O05 select': { get: true, inject: (r, s) => failing(r, r.metrics, 'select', s) },
  O06: { inject: (r, s) => failing(r, otlp, 'measureMetricsData', s) },
  O07: { inject: (r, s) => failing(r, otlp, 'encodeMetricsData', s) },
  O08: {
    inject: (r, s) => {
      const counter = { hits: 0 }
      r.patch(r.metrics, 'snapshot', (original) => (...args) => {
        const sample = original.apply(r.metrics, args)
        return {
          ...sample,
          points: sample.points.map((point) => ({
            ...point,
            get name() {
              counter.hits++
              throw s
            },
          })),
        }
      })
      return counter
    },
  },
  O09: { inject: (r, s) => failing(r, gzip, 'compress', s, true) },
  'O10 failing zstd': {
    inject: (r, s) => {
      r.patch(zstd, 'isSupported', () => true)
      return failing(r, zstd, 'compress', s, true)
    },
  },
  'O10 unavailable zstd': {
    inject: (r) => {
      const counter = { hits: 0 }
      const descriptor = Object.getOwnPropertyDescriptor(zstd, 'isSupported')
      Object.defineProperty(zstd, 'isSupported', {
        configurable: true,
        get() {
          counter.hits++
          return false
        },
      })
      r.restores.push(() => Object.defineProperty(zstd, 'isSupported', descriptor))
      r.patch(zstd, 'compress', () => () => {
        throw new Error('Unavailable codec was invoked')
      })
      return counter
    },
  },
  O11: { inject: (r, s) => failing(r, r.metrics, 'written', s) },
  'O12 abandon': {
    inject: (r, s) => {
      r.patch(r.machine.payload, 'buildPayload', () => async () => null)
      return failing(r, r.metrics, 'abandon', s)
    },
  },
  'O12 reset': { close: true, inject: (r, s) => failing(r, r.metrics, 'resetTransient', s) },
  'O12 retirement': {
    close: true,
    inject: (r, s) => {
      const counter = { hits: 0 }
      r.patch(r.machine.handle, 'retire', (original) => async (...args) => {
        await original(...args)
        counter.hits++
        throw s
      })
      return counter
    },
  },
  'O13 supplier throw': {
    sasl: true,
    setup: true,
    inject: (r, s) => failing(r, r.dedicatedConnection.sasl, 'oauthBearerProvider', s),
  },
  'O13 supplier reject': {
    sasl: true,
    setup: true,
    inject: (r, s) => failing(r, r.dedicatedConnection.sasl, 'oauthBearerProvider', s, true),
  },
  'O14 Get decode': { get: true, inject: (r, s) => decodeFailure(r, s, 71) },
  'O14 Push decode': { inject: (r, s) => decodeFailure(r, s, 72) },
  O15: { inject: (r, s) => failing(r, r.machine, 'run', s, true) },
  O16: {
    inject: (r, s) => {
      const counter = { hits: 0 }
      r.patch(r.machine, 'logger', () => ({
        warn() {
          counter.hits++
          throw s
        },
      }))
      r.wire.answer('PushTelemetry', () => ({ errorCode: 42 }))
      return counter
    },
  },
}

/** Pumps only event-loop turns, never a foreground clock or global timer.
 * @returns {Promise<void>} all already queued continuations receive a turn
 */
async function settle() {
  for (let turn = 0; turn < 200; turn++) await new Promise(setImmediate)
}

/** Reaches the selected fault through the existing scheduler or original public close.
 * @param {PublicTelemetryRound} round owner @param {object} spec injector metadata
 * @returns {Promise<void>} attempted private work has settled
 */
async function drive(round, spec) {
  if (spec.setup) round.armRenewal()
  if (spec.get) {
    round.wire.answer('PushTelemetry', () => ({ errorCode: 117 }))
    round.tick()
    await round.until(() => round.machine.timer !== null && !round.machine.running)
    round.wire.answer('PushTelemetry', () => ({}))
  }
  if (spec.close) {
    const record = await publicOutcome(round, 'disconnect', [round.closeOptions(1000)])
    expect(record.actual).toEqual({ kind: 'return', value: undefined })
  } else {
    round.tick()
    await settle()
  }
}

/** Taps the original accumulator, admin deadline or transaction broker below the actual public API.
 * Void lifecycle calls use an outside holder; no returned client method is replaced.
 * @param {PublicTelemetryRound} round owner @param {string} key public method
 * @param {Array} args exact public arguments @param {object} [target] actual client or transaction
 * @returns {Promise<object>} exact original outcome, receiver, arguments and count
 */
async function publicOutcome(round, key, args, target = round.client) {
  let original = { original: target[key] }
  let originalKey = 'original'
  let select = null
  let normalize = false
  if (round.kind === 'producer' && ['send', 'sendBatch'].includes(key)) {
    original = require('../../producer/state/accumulator').prototype
    originalKey = 'append'
  } else if (round.kind === 'producer' && ['commit', 'abort'].includes(key)) {
    original = require('../../broker').prototype
    originalKey = 'endTxn'
    normalize = true
  } else if (round.kind === 'admin' && key === 'fetchTopicMetadata') {
    original = require('../../admin/deadline').AdminDeadline.prototype
    originalKey = 'wait'
    const entered = new Set()
    select = (receiver) => {
      if (entered.has(receiver)) return false
      entered.add(receiver)
      return true
    }
  }
  const call =
    originalKey === 'original'
      ? () => original.original.apply(target, args)
      : () => target[key](...args)
  const restoredAt = round.restores.length
  const record = await round.exerciseForeground({
    target: original,
    key: originalKey,
    tap: true,
    select,
    run: call,
  })
  while (round.restores.length > restoredAt) round.restores.pop()()
  if (normalize) {
    expect(record.expected.kind).toBe('return')
    expect(record.expected.value).toEqual(expect.any(Object))
    expect(record.actual).toEqual({ kind: 'return', value: undefined })
  } else assertTelemetryOutcome(record.actual, record.expected, record.log.length)
  if (originalKey === 'original') {
    expect(record.log[0].receiver).toBe(target)
    expect(record.log[0].args).toHaveLength(args.length)
    record.log[0].args.forEach((value, index) => expect(value).toBe(args[index]))
  } else if (originalKey === 'append') {
    expect(record.log[0].receiver).toBeInstanceOf(require('../../producer/state/accumulator'))
    expect(record.log[0].args[0].topicMessages).toHaveLength(1)
  } else if (originalKey === 'wait')
    expect(record.log[0].receiver.method).toBe('fetchTopicMetadata')
  else expect(record.log[0].args[0].transactionResult).toBe(key === 'commit')
  return record
}

/** Exercises actual foreground APIs and handler normalization with three independent core sets.
 * @param {PublicTelemetryRound} round started public client
 * @returns {Promise<void>} each documented original entry has been observed once
 */
async function exerciseOperations(round) {
  const before = round.wire.requests().length
  if (round.kind === 'producer') {
    let nextMessage = 0
    const message = () => ({ value: Buffer.from(`unique foreground record ${++nextMessage}`) })
    for (const key of ['send', 'sendBatch']) {
      const request =
        key === 'send'
          ? { topic: TOPIC, messages: [message()] }
          : { topicMessages: [{ topic: TOPIC, messages: [message()] }] }
      const record = await publicOutcome(round, key, [request])
      expect(record.actual.kind).toBe('return')
      expect(Array.isArray(record.actual.value)).toBe(true)
    }
    const transaction = await round.client.transaction()
    expect(transaction.clientInstanceId).toBeUndefined()
    await publicOutcome(round, 'send', [{ topic: TOPIC, messages: [message()] }], transaction)
    await publicOutcome(round, 'commit', [], transaction)
    const aborted = await round.client.transaction()
    await publicOutcome(round, 'send', [{ topic: TOPIC, messages: [message()] }], aborted)
    await publicOutcome(round, 'abort', [], aborted)
    const rows = round.wire.requests().slice(before)
    const batches = rows
      .filter((row) => row.api === 'Produce')
      .flatMap((row) =>
        row.fields.topics.flatMap((topic) =>
          topic.partitions.flatMap((partition) => partition.batches)
        )
      )
    expect(batches.map((batch) => batch.recordCount)).toEqual([1, 1, 1, 1])
    expect(new Set(batches.map((batch) => batch.digest)).size).toBe(4)
    expect(
      rows.filter((row) => row.api === 'EndTxn').map((row) => row.fields.transactionResult)
    ).toEqual([true, false])
  } else if (round.kind === 'admin') {
    const result = await publicOutcome(round, 'fetchTopicMetadata', [{ topics: [TOPIC] }])
    expect(result.actual.kind).toBe('return')
    expect(result.actual.value).toEqual(expect.any(Object))
  } else if (round.kind === 'consumer') {
    const runner = round.runner
    await round.until(() => round.foregroundBatch != null)
    for (const key of ['eachMessage', 'eachBatch']) {
      for (const outcome of ['return', 'throw', 'reject']) {
        const identity =
          outcome === 'return'
            ? Object.freeze({ marker: Symbol(key) })
            : new (require('../../errors').KafkaJSNonRetriableError)(`original ${key} refusal`)
        const result = await round.exerciseForeground({
          target: runner,
          key,
          identity,
          outcome,
          run: () =>
            runner[key === 'eachMessage' ? 'processEachMessage' : 'processEachBatch'](
              round.foregroundBatch
            ),
        })
        if (outcome === 'return') {
          expect(result.expected.value).toBe(identity)
          expect(result.actual).toEqual({ kind: 'return', value: undefined })
        } else {
          expect(result.expected.reason).toBe(identity)
          assertTelemetryOutcome(
            result.actual,
            { kind: 'reject', reason: identity },
            result.log.length
          )
        }
        expect(result.log[0].receiver).toBe(runner)
        expect(result.log[0].args[0][key === 'eachBatch' ? 'batch' : 'message']).toBe(
          key === 'eachBatch' ? round.foregroundBatch : round.foregroundBatch.messages[0]
        )
      }
    }
    expect(
      round.wire
        .requests()
        .slice(before)
        .filter((row) => row.api === 'OffsetCommit')
    ).toEqual([])
  } else {
    const {
      A,
      part,
      acquired,
      recordsHex,
      fetchAnswer,
    } = require('../../../testHelpers/shareHarness')
    const run = round.runner.current
    const called = []
    const handlerIdentity = Object.freeze({ marker: Symbol('share handler') })
    round.patch(run, 'handler', () => async (payload) => {
      const target = { acknowledge: payload.acknowledge }
      const record = await round.exerciseForeground({
        target,
        key: 'acknowledge',
        tap: true,
        run: () => target.acknowledge('accept'),
      })
      assertTelemetryOutcome(record.actual, record.expected, record.log.length)
      called.push(payload.message.offset)
      return handlerIdentity
    })
    const handler = await round.exerciseForeground({
      target: run,
      key: 'handler',
      tap: true,
      run: async () => {
        round.harness.script('ShareFetch', [
          fetchAnswer([
            part({
              topicId: A,
              partition: 0,
              acquire: [acquired(0, 0)],
              records: recordsHex(0, ['unique share record']),
            }),
          ]),
        ])
        await round.until(() => called.length === 1)
      },
    })
    expect(handler.expected.value).toBe(handlerIdentity)
    expect(handler.actual).toEqual({ kind: 'return', value: undefined })
    expect(handler.log[0].receiver).toBe(run)
    await round.until(() =>
      round.wire
        .requests()
        .slice(before)
        .some(
          (row) =>
            ['ShareFetch', 'ShareAcknowledge'].includes(row.api) &&
            row.fields.topics?.some((topic) =>
              topic.partitions.some((partition) => partition.acknowledgementBatches?.length)
            )
        )
    )
    expect(called).toEqual(['0'])
    const failures = []
    round.patch(
      run,
      'afterFailure',
      (original) =>
        async function (...args) {
          failures.push(args[3])
          return original.apply(this, args)
        }
    )
    for (const [index, outcome] of ['throw', 'reject'].entries()) {
      const identity = new Error(`original share handler ${outcome}`)
      const record = await round.exerciseForeground({
        target: run,
        key: 'handler',
        outcome,
        identity,
        run: async () => {
          const offset = index + 1
          round.harness.script('ShareFetch', [
            fetchAnswer([
              part({
                topicId: A,
                partition: 0,
                acquire: [acquired(offset, offset)],
                records: recordsHex(offset, [`share ${outcome}`]),
              }),
            ]),
          ])
          await round.until(() => failures.length === offset)
        },
      })
      expect(record.expected.reason).toBe(identity)
      expect(failures[index]).toBe(identity)
      expect(record.actual).toEqual({ kind: 'return', value: undefined })
      expect(record.log[0].receiver).toBe(run)
    }
    expect(
      round.wire
        .requests()
        .slice(before)
        .filter((row) => row.api === 'OffsetCommit')
    ).toEqual([])
  }
}

/** Captures a real consumed batch before the runner invokes its application handler.
 * @param {PublicTelemetryRound} round owner @returns {void} scoped passive original wrapper
 */
function captureBatch(round) {
  const Runner = require('../../consumer/runner')
  round.patch(
    Runner.prototype,
    'processEachMessage',
    (original) =>
      function (batch) {
        round.foregroundBatch = batch
        return original.call(this, batch)
      }
  )
}

describe.each(TELEMETRY_KINDS)('%s private failure preserves foreground identity', (kind) => {
  test.each(Object.entries(INJECTORS))(
    '%s and its enabled/disabled controls',
    async (_id, spec) => {
      for (const arm of ['control', 'fault', 'disabled']) {
        const round = new PublicTelemetryRound({ kind, enabled: arm !== 'disabled' })
        let reads = 0
        const reason = Object.freeze({
          get message() {
            reads++
            throw new Error('private getter')
          },
        })
        let counter = { hits: 0 }
        try {
          captureBatch(round)
          if (arm === 'disabled') counter = disabledProbe(round, reason)
          await round.start({
            points: 'large',
            sasl: spec.sasl,
            subscription: { acceptedCompressionTypes: _id.startsWith('O10') ? [4, 1, 0] : [1, 0] },
            client: kind === 'producer' ? { transactionalId: 'foreground-tx', linger: 0 } : {},
            beforeConnect: (r) => {
              if (spec.early && arm === 'fault') counter = spec.inject(r, reason)
            },
          })
          if (round.machine) await round.ready()
          if (_id.startsWith('O10') && arm === 'control')
            expect(round.pushes()[0].fields.compressionType).toBe(zstd.isSupported ? 4 : 1)
          const normalPools = round.binding?.cluster?.brokerPool.brokers
          const normalBrokers = normalPools ? Object.values(normalPools) : []
          if (arm === 'fault' && !spec.early) {
            const restoreAt = round.restores.length
            const written = round.written.length
            if (['O06', 'O07', 'O08'].includes(_id))
              round.client.registerMetricForSubscription({
                name: 'com.example.total',
                type: 'sum',
                value: 17n,
              })
            if (spec.close) await exerciseOperations(round)
            counter = spec.inject(round, reason)
            await drive(round, spec)
            if (['O06', 'O07', 'O08'].includes(_id)) {
              expect(round.written).toHaveLength(written)
              expect(counter.hits).toBeGreaterThan(0)
              while (round.restores.length > restoreAt) round.restores.pop()()
              await round.establish()
              expect(round.decodedLatestPoint({ name: 'com.example.total' }).value).toBe(6n)
            }
            if (_id === 'O09' || _id === 'O10 unavailable zstd')
              expect(round.pushes().at(-1).fields.compressionType).toBe(_id === 'O09' ? 0 : 1)
            if (_id === 'O16') expect(counter.hits).toBe(1)
          }
          // Cleanup faults are checked against the actual original close, not a second new close.
          if (!spec.close || arm !== 'fault') await exerciseOperations(round)
          if (arm === 'fault') expect(counter.hits).toBeGreaterThan(0)
          else expect(counter.hits).toBe(0)
          if (normalPools) expect(Object.values(normalPools)).toEqual(normalBrokers)
          expect(reads).toBe(0)
          expect(round.unhandled).toEqual([])
          if (arm === 'disabled') {
            expect(round.machine).toBeNull()
            expect(round.telemetryRequests()).toEqual([])
            expect(round.dedicatedCount()).toBe(0)
          }
        } finally {
          await round.dispose()
        }
      }
    },
    60000
  )
})

describe('original rejection identity and call normalization', () => {
  test.each(['return', 'throw', 'reject'])(
    '%s retains the unique sentinel and receiver',
    async (outcome) => {
      const round = new PublicTelemetryRound({ kind: 'producer' })
      const identity = Object.freeze({ marker: Symbol('foreground') })
      const target = { original() {} }
      const args = [Object.freeze({ marker: Symbol('argument') })]
      try {
        const record = await round.exerciseForeground({
          target,
          key: 'original',
          outcome,
          identity,
          run: () => target.original(...args),
        })
        assertTelemetryOutcome(
          record.actual,
          {
            kind: outcome === 'return' ? 'return' : outcome === 'throw' ? 'throw' : 'reject',
            [outcome === 'return' ? 'value' : 'reason']: identity,
          },
          record.log.length
        )
        expect(record.log[0].receiver).toBe(target)
        expect(record.log[0].args[0]).toBe(args[0])
      } finally {
        await round.dispose()
      }
    }
  )
})

test.each(TELEMETRY_KINDS)(
  'O01 %s collector construction is contained by the actual factory',
  async (kind) => {
    const reason = Object.freeze({ marker: Symbol('construction') })
    let hits = 0
    jest.doMock(
      '../metrics',
      () =>
        class {
          constructor() {
            hits++
            throw reason
          }
        }
    )
    try {
      await jest.isolateModulesAsync(async () => {
        const { PublicTelemetryRound: Round } = require('../../../testHelpers/lifecycle')
        const round = new Round({ kind })
        try {
          captureBatch(round)
          await round.start({
            client: kind === 'producer' ? { transactionalId: 'foreground-tx', linger: 0 } : {},
          })
          expect(hits).toBe(1)
          expect(round.machine).toBeNull()
          expect(round.telemetryRequests()).toEqual([])
          await exerciseOperations(round)
          await expect(round.client.disconnect(round.closeOptions(0))).resolves.toBeUndefined()
        } finally {
          await round.dispose()
        }
      })
    } finally {
      jest.dontMock('../metrics')
    }
  },
  60000
)

test('O17 O18 O19 rerun the existing meter/span/context/getter/logger containment matrix on the packed candidate', async () => {
  const cwd = path.resolve(__dirname, '../../../otel')
  const candidate = path.join(cwd, 'node_modules/@countly/kafkajs')
  expect(fs.lstatSync(candidate).isSymbolicLink()).toBe(false)
  for (const file of [
    'src/telemetry/client.js',
    'src/telemetry/payload.js',
    'src/telemetry/factory.js',
    'src/network/connection.js',
  ])
    expect(fs.readFileSync(path.join(candidate, file))).toEqual(
      fs.readFileSync(path.resolve(__dirname, '../../..', file))
    )
  const { stdout, stderr } = await promisify(execFile)(
    process.execPath,
    ['--test', 'test/failureContainment.test.js'],
    {
      cwd,
      timeout: 600000,
      maxBuffer: 16 * 1024 * 1024,
      env: { ...process.env, NO_UPDATE_NOTIFIER: '1' },
    }
  )
  expect(stdout).toMatch(/# fail 0/)
  expect(stdout).toMatch(/the meters throw/)
  expect(stdout).toMatch(/context/)
  expect(stdout).toMatch(/message cannot be read/)
  expect(stderr).not.toMatch(/unhandledRejection|uncaughtException/)
}, 610000)

describe('original public rejection under observer failure', () => {
  test.each([
    'send',
    'sendBatch',
    'transaction.send',
    'transaction.commit',
    'transaction.abort',
    'admin',
  ])(
    '%s preserves a unique original rejection once',
    async (operation) => {
      const kind = operation === 'admin' ? 'admin' : 'producer'
      const reason = new (require('../../errors').KafkaJSNonRetriableError)(
        'original foreground refusal'
      )
      const round = new PublicTelemetryRound({ kind })
      try {
        await round.start({
          client: kind === 'producer' ? { transactionalId: 'foreground-tx', linger: 0 } : {},
        })
        await round.ready()
        const fault = failing(
          round,
          round.metrics,
          'observeRequest',
          Object.freeze({ private: true })
        )
        const broker = await round.binding.cluster.findConnectedBroker()
        const transaction = operation.startsWith('transaction.')
          ? await round.client.transaction()
          : null
        const key = operation.split('.').at(-1)
        if (transaction && ['commit', 'abort'].includes(key))
          await transaction.send({ topic: TOPIC, messages: [{ value: 'before end' }] })
        const method =
          operation === 'admin'
            ? 'metadata'
            : key === 'commit' || key === 'abort'
              ? 'endTxn'
              : 'produce'
        const target = transaction ?? round.client
        let args = []
        if (key === 'send') args = [{ topic: TOPIC, messages: [{ value: 'original' }] }]
        if (key === 'sendBatch')
          args = [{ topicMessages: [{ topic: TOPIC, messages: [{ value: 'original' }] }] }]
        if (operation === 'admin') args = [{ topics: [TOPIC] }]
        const result = await round.exerciseForeground({
          target: broker,
          key: method,
          identity: reason,
          outcome: 'reject',
          run: () => target[operation === 'admin' ? 'fetchTopicMetadata' : key](...args),
        })
        if (key === 'abort') expect(result.actual).toEqual({ kind: 'return', value: undefined })
        else {
          expect(result.actual.kind).toBe('reject')
          expect(result.actual.reason).toBe(reason)
        }
        expect(result.expected.reason).toBe(reason)
        expect(result.log[0].receiver).toBe(broker)
        expect(result.log).toHaveLength(1)
        // An ordinary successful core request reaches the private observer independently.
        await broker.apiVersions()
        expect(fault.hits).toBeGreaterThan(0)
        expect(round.unhandled).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

test.each([undefined, null, false, 0, ''])(
  'original synchronous falsy throw retains its identity: %p',
  async (identity) => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const target = { original() {} }
    try {
      const result = await round.exerciseForeground({
        target,
        key: 'original',
        outcome: 'throw',
        identity,
        run: () => target.original(),
      })
      assertTelemetryOutcome(result.actual, { kind: 'throw', reason: identity }, result.log.length)
    } finally {
      await round.dispose()
    }
  }
)
