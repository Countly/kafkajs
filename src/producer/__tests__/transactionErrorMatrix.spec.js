const mockHooks = { afterBegin: null }
jest.mock('../eosManager', () => {
  const create = jest.requireActual('../eosManager')
  const wrapped = (options) => {
    const manager = create(options)
    const begin = manager.beginTransaction
    manager.beginTransaction = async () => {
      await begin()
      mockHooks.afterBegin?.(options)
    }
    return manager
  }
  return Object.assign(wrapped, create)
})
const createEosManager = require('../eosManager')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')
const rows = [
  'InitProducerId',
  'AddPartitionsToTxn',
  'AddOffsetsToTxn',
  'EndTxn',
  'TxnOffsetCommit',
].flatMap((api) => [47, 120].map((code) => ({ api, code })))
rows.push({ api: 'InitProducerId', code: 90 })
/** Creates an initialized transaction facade. @returns {object} fixture @throws Never. */
const fixture = () => {
  const cluster = createFakeCluster()
  const broker = cluster.broker
  Object.assign(broker, {
    negotiatedVersion: () => 4,
    addOffsetsToTxn: jest.fn(async () => ({})),
    txnOffsetCommit: jest.fn(async () => ({})),
  })
  broker.initProducerId.mockImplementation(async ({ producerEpoch }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  const manager = createEosManager({
    cluster,
    logger: newLogger(),
    transactional: true,
    transactionalId: 'matrix',
    retry: { retries: 0 },
  })
  return { cluster, broker, manager }
}
it.each(rows)(
  '$api with code $code has its declared state and recovery boundary',
  async ({ api, code }) => {
    const { cluster, broker, manager } = fixture()
    const method = {
      InitProducerId: 'initProducerId',
      AddPartitionsToTxn: 'addPartitionsToTxn',
      AddOffsetsToTxn: 'addOffsetsToTxn',
      EndTxn: 'endTxn',
      TxnOffsetCommit: 'txnOffsetCommit',
    }[api]
    const failure = createErrorFromCode(code)
    if (api !== 'InitProducerId') {
      await manager.initProducerId()
      await manager.beginTransaction()
    }
    if (api === 'EndTxn')
      await manager.addPartitionsToTransaction([
        { topic: 'test-topic', partitions: [{ partition: 0 }] },
      ])
    broker[method].mockRejectedValueOnce(failure)
    const operation =
      api === 'InitProducerId'
        ? manager.initProducerId()
        : api === 'AddPartitionsToTxn'
          ? manager.addPartitionsToTransaction([
              { topic: 'test-topic', partitions: [{ partition: 0 }] },
            ])
          : api === 'EndTxn'
            ? manager.commit()
            : manager.sendOffsets({
                consumerGroupId: 'group',
                topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '3' }] }],
              })
    const result = await operation.then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    expect(broker[method]).toHaveBeenCalledTimes(1)
    const initial = api === 'InitProducerId'
    const fatal = code === 47 || code === 90
    const raw = (initial && !fatal) || api === 'AddPartitionsToTxn'
    expect(result.error).toMatchObject({
      name: raw
        ? 'KafkaJSProtocolError'
        : fatal
          ? 'KafkaJSApplicationRecoverableError'
          : 'KafkaJSTransactionAbortableError',
      category: fatal ? 'applicationRecoverable' : 'abortable',
    })
    expect(manager.stateManager.state()).toBe(
      fatal ? 'FATAL_ERROR' : initial ? 'UNINITIALIZED' : 'ABORTABLE_ERROR'
    )
    expect(raw ? result.error.code : result.error.cause.code).toBe(code)
    if (raw) expect(result.error).toBe(failure)
    expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    const armed = manager.stateManager.transaction.armedError()
    if (armed) {
      expect(armed).toMatchObject({
        name: fatal ? 'KafkaJSApplicationRecoverableError' : 'KafkaJSTransactionAbortableError',
        category: fatal ? 'applicationRecoverable' : 'abortable',
        cause: { code },
      })
      const calls = [
        () => manager.initProducerId(),
        () => manager.beginTransaction(),
        () =>
          manager.addPartitionsToTransaction([
            { topic: 'test-topic', partitions: [{ partition: 0 }] },
          ]),
        () => manager.sendOffsets({ consumerGroupId: 'group', topics: [] }),
        () => manager.commit(),
        ...(fatal ? [() => manager.abort()] : []),
      ]
      for (const call of calls) await expect(call()).rejects.toBe(armed)
      expect(manager.stateManager.state()).toBe(fatal ? 'FATAL_ERROR' : 'ABORTABLE_ERROR')
    }
    if (!fatal) {
      if (!initial) await manager.abort()
      if (initial) await manager.initProducerId()
      expect(manager.stateManager.state()).toBe('READY')
      await manager.beginTransaction()
      await manager.abort()
    }
  }
)

it('includes both codes at every transactional API', () => {
  expect(rows).toHaveLength(11)
  expect(new Set(rows.map(({ api, code }) => api + ':' + code)).size).toBe(11)
})

const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { KafkaJSApplicationRecoverableError, KafkaJSRequestTimeoutError } = require('../../errors')

/** Creates a wire-backed public producer with finite retries.
 * @param {string} profile transaction protocol profile
 * @param {object} [retry] client retry options
 * @param {object} [options] producer options over the defaults
 * @returns {object} cluster and producer
 * @throws {Error} for invalid client options
 */
const publicFixture = (profile, retry = { retries: 1 }, options = {}) => {
  const cluster = producerScript(
    createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile,
    })
  )
  cluster.answer('InitProducerId', ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: Math.max(0, producerEpoch + 1),
  }))
  const producer = new Kafka(cluster.kafkaOptions({ retry })).producer({
    idempotent: true,
    linger: 0,
    ...(profile === 'default' ? {} : { transactionalId: 'guard' }),
    ...options,
  })
  return { cluster, producer }
}

/** Closes independently and keeps an assertion's diagnostic.
 * @param {Error|undefined} primary test failure
 * @param {object} producer public client
 * @param {object} cluster scripted broker
 * @returns {Promise<void>} after close and cleanliness checks
 * @throws {Error|AggregateError} primary with cleanupErrors or cleanup errors alone
 */
const close = async (primary, producer, cluster) => {
  const failures = []
  try {
    await producer.disconnect()
  } catch (error) {
    failures.push(error)
  }
  try {
    cluster.assertClean()
  } catch (error) {
    failures.push(error)
  }
  if (primary) {
    if (failures.length) primary.cleanupErrors = failures
    throw primary
  }
  if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed')
}

it.each(
  ['default', 'tv1', 'tv2'].flatMap((profile) => [47, 90].map((code) => ({ profile, code })))
)(
  'initial fencing code $code under $profile permanently rejects public operations',
  async ({ profile, code }) => {
    const { cluster, producer } = publicFixture(profile)
    let primary
    try {
      if (profile !== 'default') await producer.connect()
      cluster.answer('InitProducerId', () => ({
        errorCode: code,
        producerId: '-1',
        producerEpoch: -1,
      }))
      const error = await (
        profile === 'default' ? producer.connect() : producer.transaction()
      ).catch((failure) => failure)
      expect(error).toMatchObject({
        name: 'KafkaJSApplicationRecoverableError',
        category: 'applicationRecoverable',
        cause: { name: 'KafkaJSProtocolError', code },
      })
      const before = cluster.requests().length
      for (const call of [
        () => producer.flush(),
        () => producer.flush({ timeout: 0 }),
        () => producer.flush({ timeout: 2147483647 }),
        () => producer.connect(),
        () => producer.send({ topic: TOPIC, messages: [{ value: 'later' }] }),
        () =>
          producer.sendBatch({ topicMessages: [{ topic: TOPIC, messages: [{ value: 'later' }] }] }),
        ...(profile === 'default' ? [] : [() => producer.transaction()]),
      ])
        await expect(call()).rejects.toBe(error)
      expect(cluster.requests()).toHaveLength(before)
      await expect(producer.disconnect()).resolves.toBeUndefined()
      await expect(producer.flush()).rejects.toBe(error)
      await expect(producer.connect()).rejects.toBe(error)
      await expect(producer.flush({ timeout: 0 })).rejects.toBe(error)
      expect(cluster.requests().filter(({ api }) => api === 'EndTxn')).toHaveLength(0)
    } catch (error) {
      primary = error
    } finally {
      await close(primary, producer, cluster)
    }
  }
)

it.each(
  ['AddPartitionsToTxn', 'AddOffsetsToTxn', 'EndTxn', 'TxnOffsetCommit'].flatMap((api) =>
    [47, 120].map((code) => ({ api, code }))
  )
)('$api code $code pins public guards and the abort recovery boundary', async ({ api, code }) => {
  const { cluster, producer } = publicFixture('tv1')
  let primary
  try {
    await producer.connect()
    const transaction = await producer.transaction()
    if (api === 'EndTxn') await transaction.send({ topic: TOPIC, messages: [{ value: 'first' }] })
    cluster.schedule.once({ api }, ({ fields }) =>
      api === 'AddPartitionsToTxn'
        ? {
            errors: fields.topics.map(({ topic, partitions }) => ({
              topic,
              partitionErrors: partitions.map((partition) => ({ partition, errorCode: code })),
            })),
          }
        : api === 'TxnOffsetCommit'
          ? {
              topics: fields.topics.map(({ topic, partitions }) => ({
                topic,
                partitions: partitions.map(({ partition }) => ({ partition, errorCode: code })),
              })),
            }
          : { errorCode: code }
    )
    const offsets = {
      consumerGroupId: 'group',
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '3' }] }],
    }
    const first = await (
      api === 'AddPartitionsToTxn'
        ? transaction.send({ topic: TOPIC, messages: [{ value: 'failed' }] })
        : api === 'EndTxn'
          ? transaction.commit()
          : transaction.sendOffsets(offsets)
    ).catch((error) => error)
    expect(first).toMatchObject({
      name: code === 47 ? 'KafkaJSApplicationRecoverableError' : 'KafkaJSTransactionAbortableError',
      category: code === 47 ? 'applicationRecoverable' : 'abortable',
      cause: { code },
    })
    for (const call of [
      () => transaction.send({ topic: TOPIC, messages: [{ value: 'later' }] }),
      () =>
        transaction.sendBatch({
          topicMessages: [{ topic: TOPIC, messages: [{ value: 'later' }] }],
        }),
      () => transaction.sendOffsets(offsets),
      () => transaction.commit(),
      () => producer.transaction(),
    ])
      await expect(call()).rejects.toBe(first)
    if (code === 47) {
      await expect(producer.flush()).rejects.toBe(first)
      await expect(transaction.abort()).rejects.toBe(first)
      await expect(producer.send({ topic: TOPIC, messages: [{ value: 'later' }] })).rejects.toBe(
        first
      )
    } else {
      await transaction.abort()
      await expect(
        transaction.send({ topic: TOPIC, messages: [{ value: 'old' }] })
      ).rejects.toMatchObject({ name: 'KafkaJSNonRetriableError' })
      const next = await producer.transaction()
      await next.send({ topic: TOPIC, messages: [{ value: 'recovered' }] })
      await next.commit()
    }
  } catch (error) {
    primary = error
  } finally {
    await close(primary, producer, cluster)
  }
})

const FENCED_AT_INIT = [47, 90].flatMap((code) =>
  ['tv1', 'tv2'].map((profile) => ({ profile, code }))
)
const FAST_RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const fencedInit = (code) => () => ({ errorCode: code, producerId: '-1', producerEpoch: -1 })
const ordinaryInit = { api: 'InitProducerId', match: ({ fields }) => !fields.transactionalId }
const transactionalInit = { api: 'InitProducerId', match: ({ fields }) => !!fields.transactionalId }
const framesOf = (cluster, api, transactional) =>
  cluster
    .requests()
    .filter(
      (request) =>
        request.api === api &&
        (transactional === undefined || Boolean(request.fields.transactionalId) === transactional)
    ).length

/** Waits for a promise that never rejects, or `'pending'` when it has not settled by `ms`, so a call
 * that never settles fails an assertion instead of the test's timeout.
 * @param {Promise<unknown>} promise
 * @param {number} [ms]
 * @returns {Promise<unknown>} the promise's value, or `'pending'`
 * @throws Never.
 */
const settledWithin = (promise, ms = 500) => {
  let timer
  const pending = new Promise((resolve) => {
    timer = setTimeout(() => resolve('pending'), ms)
  })
  return Promise.race([promise, pending]).finally(() => clearTimeout(timer))
}

/** Ends a test's producer at once, whatever it still holds, and checks the wire stayed clean.
 * @param {object} producer public client
 * @param {object} cluster scripted broker
 * @returns {Promise<void>}
 * @throws {Error} a failure of the scripted cluster itself
 */
const closeNow = async (producer, cluster) => {
  cluster.schedule.rules.length = 0
  await producer.disconnect({ timeout: 0 })
  cluster.assertClean()
}

describe('initial fencing of one identity stops the whole producer', () => {
  it.each(FENCED_AT_INIT)(
    'an open transaction cannot send once the ordinary initialization is fenced $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY)
      const gate = cluster.schedule.hold(ordinaryInit)
      try {
        const connecting = producer.connect().catch((error) => error)
        await gate.arrived()
        const transaction = await producer.transaction()
        gate.release(fencedInit(code)())
        const fatal = await connecting
        expect(fatal).toMatchObject({
          name: 'KafkaJSApplicationRecoverableError',
          cause: { code },
        })

        const before = framesOf(cluster, 'Produce')
        await expect(
          transaction.send({ topic: TOPIC, messages: [{ value: 'later' }] })
        ).rejects.toBe(fatal)
        await expect(
          transaction.sendBatch({
            topicMessages: [{ topic: TOPIC, messages: [{ value: 'later' }] }],
          })
        ).rejects.toBe(fatal)
        await expect(transaction.commit()).rejects.toBe(fatal)
        await expect(transaction.abort()).rejects.toBe(fatal)
        expect(framesOf(cluster, 'Produce')).toBe(before)

        await expect(producer.disconnect()).resolves.toBeUndefined()
        await expect(producer.connect()).rejects.toBe(fatal)
        await expect(producer.transaction()).rejects.toBe(fatal)
        await expect(producer.flush()).rejects.toBe(fatal)
        expect(framesOf(cluster, 'EndTxn')).toBe(0)
      } finally {
        await closeNow(producer, cluster)
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'an ordinary send in flight stops once the transactional initialization is fenced $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY)
      const gate = cluster.schedule.hold({ api: 'Produce' })
      let pending
      try {
        await producer.connect()
        pending = producer
          .send({ topic: TOPIC, messages: [{ value: 'earlier', timestamp: 1000 }] })
          .then(
            (value) => ({ value }),
            (error) => ({ error })
          )
        await gate.arrived()
        cluster.schedule.once(transactionalInit, fencedInit(code))
        const fatal = await producer.transaction().catch((error) => error)
        expect(fatal.name).toBe('KafkaJSApplicationRecoverableError')

        const before = framesOf(cluster, 'Produce')
        gate.fail(new KafkaJSRequestTimeoutError('answer lost after the producer became unusable'))
        const result = await settledWithin(pending)
        expect(result.error).toBe(fatal)
        expect(framesOf(cluster, 'Produce')).toBe(before)
      } finally {
        await closeNow(producer, cluster)
        await pending
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'a send waiting for buffer room rejects with the fatal error once the transactional initialization is fenced $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY, { bufferMemory: 4096 })
      const gate = cluster.schedule.hold({ api: 'Produce' })
      const settle = (promise) =>
        promise.then(
          (value) => ({ value }),
          (error) => ({ error })
        )
      let holding, waiting
      try {
        await producer.connect()
        const messages = [{ value: 'x'.repeat(1000), timestamp: 1000 }]
        holding = settle(producer.send({ topic: TOPIC, messages }))
        await gate.arrived()
        waiting = settle(producer.send({ topic: TOPIC, messages }))
        await new Promise((resolve) => setImmediate(resolve))
        cluster.schedule.once(transactionalInit, fencedInit(code))
        const fatal = await producer.transaction().catch((error) => error)

        expect((await settledWithin(waiting)).error).toBe(fatal)
        expect((await settledWithin(holding)).error).toBe(fatal)
      } finally {
        await closeNow(producer, cluster)
        await Promise.all([holding, waiting])
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'a send granted its buffer room by a retry-queued send that fencing rejects fails with the fatal error $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(
        profile,
        { retries: 2, initialRetryTime: 10000, maxRetryTime: 10000, factor: 0 },
        { bufferMemory: 4096 }
      )
      const gate = cluster.schedule.hold({ api: 'Produce' })
      const settle = (promise) => promise.catch((error) => error)
      const messages = [{ value: 'x'.repeat(1000), timestamp: 1000 }]
      let queued, waiting
      try {
        await producer.connect()
        queued = settle(producer.send({ topic: TOPIC, messages }))
        await gate.arrived()
        gate.release({
          responses: [
            {
              name: TOPIC,
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  errorCode: 19,
                  baseOffset: '-1',
                  logAppendTime: '-1',
                  logStartOffset: '0',
                },
              ],
            },
          ],
        })
        for (let i = 0; i < 10; i++) await new Promise(setImmediate)
        waiting = settle(producer.send({ topic: TOPIC, messages }))
        await new Promise(setImmediate)
        cluster.schedule.once(transactionalInit, fencedInit(code))
        const fatal = await producer.transaction().catch((error) => error)
        expect(fatal.name).toBe('KafkaJSApplicationRecoverableError')

        expect(await settledWithin(queued)).toBe(fatal)
        expect(await settledWithin(waiting)).toBe(fatal)
        expect(framesOf(cluster, 'Produce')).toBe(1)
      } finally {
        await closeNow(producer, cluster)
        await Promise.all([queued, waiting])
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'a commit waiting for its answer rejects with the fatal error once the ordinary initialization is fenced $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY)
      const initGate = cluster.schedule.hold(ordinaryInit)
      let endGate
      try {
        const connecting = producer.connect().catch((error) => error)
        await initGate.arrived()
        const transaction = await producer.transaction()
        await transaction.send({ topic: TOPIC, messages: [{ value: 'first' }] })
        endGate = cluster.schedule.hold({ api: 'EndTxn' })
        const committing = transaction.commit().catch((error) => error)
        await endGate.arrived()
        initGate.release(fencedInit(code)())
        const fatal = await connecting

        expect(await settledWithin(committing)).toBe(fatal)
        expect(framesOf(cluster, 'EndTxn')).toBe(1)
        endGate.release()
        await expect(producer.disconnect()).resolves.toBeUndefined()
        await expect(producer.send({ topic: TOPIC, messages: [{ value: 'later' }] })).rejects.toBe(
          fatal
        )
      } finally {
        if (endGate?.settle) endGate.fail(new Error('cleanup'))
        await closeNow(producer, cluster)
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'a transactional initialization answered in the same turn as the fencing yields no transaction $profile/$code',
    async ({ profile, code }) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY)
      const ordinaryGate = cluster.schedule.hold(ordinaryInit)
      const transactionalGate = cluster.schedule.hold(transactionalInit)
      try {
        const connecting = producer.connect().catch((error) => error)
        await ordinaryGate.arrived()
        const opening = producer.transaction().catch((error) => error)
        await transactionalGate.arrived()
        transactionalGate.release({ producerId: '1000', producerEpoch: 0 })
        ordinaryGate.release(fencedInit(code)())
        const fatal = await connecting

        expect(await settledWithin(opening)).toBe(fatal)
      } finally {
        await closeNow(producer, cluster)
      }
    }
  )

  it.each(['tv1', 'tv2'])(
    'a transaction whose begin completes just as the producer is fenced is not returned %s',
    async (profile) => {
      const { cluster, producer } = publicFixture(profile, FAST_RETRY)
      const fatal = new KafkaJSApplicationRecoverableError('fenced', {
        cause: createErrorFromCode(47),
      })
      try {
        await producer.connect()
        mockHooks.afterBegin = ({ onFenced }) => onFenced(fatal)
        expect(await settledWithin(producer.transaction().catch((error) => error))).toBe(fatal)
      } finally {
        mockHooks.afterBegin = null
        await closeNow(producer, cluster)
      }
    }
  )

  it.each(FENCED_AT_INIT)(
    'a transactional initialization waiting to retry does not retry once the ordinary one is fenced $profile/$code',
    async ({ profile, code }) => {
      const backoff = 40
      const { cluster, producer } = publicFixture(profile, {
        retries: 3,
        initialRetryTime: backoff,
        maxRetryTime: backoff,
        factor: 0,
      })
      const gate = cluster.schedule.hold(ordinaryInit)
      try {
        const connecting = producer.connect().catch((error) => error)
        await gate.arrived()
        cluster.schedule.once(transactionalInit, () => ({
          errorCode: 14,
          producerId: '-1',
          producerEpoch: -1,
        }))
        const opening = producer.transaction().catch((error) => error)
        while (framesOf(cluster, 'InitProducerId', true) === 0) await new Promise(setImmediate)
        expect(framesOf(cluster, 'InitProducerId', true)).toBe(1)

        gate.release(fencedInit(code)())
        const fatal = await connecting
        await new Promise((resolve) => setTimeout(resolve, backoff * 4))

        expect(framesOf(cluster, 'InitProducerId', true)).toBe(1)
        expect(await settledWithin(opening)).toBe(fatal)
      } finally {
        await closeNow(producer, cluster)
      }
    }
  )
})
