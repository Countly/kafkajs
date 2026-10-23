const {
  bufferedProducer,
  deferred,
  succeeded,
  flushAll,
  watch,
} = require('./fixtures/bufferHarness')
const ProducerStateManager = require('../state')
const { KafkaJSConnectionError } = require('../../errors')

/** @param {number} version @returns {Promise<object>} one usable transactional producer */
const build = async (version) => {
  const fixture = bufferedProducer({
    transactionalId: 'close-transaction',
    idempotent: true,
    deliveryTimeout: 120000,
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
  })
  const { broker } = fixture.cluster
  broker.produce.mockImplementation(async (request) => {
    const error = request.guard?.(100)
    if (error) throw error
    return succeeded(request)
  })
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: version } },
    finalizedFeaturesEpoch: 1,
    negotiatedVersion: () => 5,
    addOffsetsToTxn: jest.fn(async () => ({})),
    txnOffsetCommit: jest.fn(async () => ({})),
  })
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.endTxn.mockImplementation(async () => ({ producerId: '-1', producerEpoch: -1 }))
  await fixture.producer.connect()
  const transaction = await fixture.producer.transaction()
  const sent = transaction.send({ topic: 'topic', messages: [{ value: 'value' }] })
  await flushAll()
  await sent
  return { ...fixture, transaction }
}

describe('transaction close', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test.each([1, 2])(
    'an open transaction is aborted once at close with transaction version %s',
    async (version) => {
      const { producer, cluster, transaction } = await build(version)
      try {
        await producer.disconnect()
        expect(
          cluster.broker.endTxn.mock.calls.map(([request]) => request.transactionResult)
        ).toEqual([false])
        expect(transaction.isActive()).toBe(false)
        await producer.disconnect()
        expect(cluster.broker.endTxn).toHaveBeenCalledTimes(1)
        expect(cluster.disconnect).toHaveBeenCalledTimes(1)
      } finally {
        await producer.disconnect()
      }
    }
  )

  test.each([1, 2])(
    'a forced unknown commit is never aborted or published with transaction version %s',
    async (version) => {
      const { producer, cluster, transaction, admission } = await build(version)
      await transaction.sendOffsets({
        consumerGroupId: 'group',
        topics: [{ topic: 'input', partitions: [{ partition: 0, offset: '1' }] }],
      })
      const held = deferred()
      cluster.broker.endTxn.mockImplementation(async () => held.promise)
      const committing = watch(transaction.commit())
      await flushAll()
      expect(cluster.broker.endTxn).toHaveBeenCalledTimes(1)
      const closed = watch(producer.disconnect({ timeout: 10 }))
      try {
        await jest.advanceTimersByTimeAsync(10)
        await flushAll()
        expect(closed.state).toBe('fulfilled')
        expect(committing).toMatchObject({
          state: 'rejected',
          value: { name: 'KafkaJSProducerClosedError', outcome: 'unknown' },
        })
        expect(
          cluster.broker.endTxn.mock.calls.map(([request]) => request.transactionResult)
        ).toEqual([true])
        expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(admission.available).toBe(admission.capacity)
        await producer.disconnect()
        expect(cluster.broker.endTxn).toHaveBeenCalledTimes(1)
        await producer.connect()
        held.resolve({ producerId: '1000', producerEpoch: 90 })
        await flushAll()
        expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        const refused = watch(transaction.abort())
        await flushAll()
        expect(refused).toMatchObject({
          state: 'rejected',
          value: { message: expect.stringMatching(/commit is pending/) },
        })
        const sent = producer.send({ topic: 'other', messages: [{ value: 'nontransactional' }] })
        await flushAll()
        await sent
      } finally {
        held.resolve({ producerId: '-1', producerEpoch: -1 })
        await flushAll()
        if (closed.state === 'pending') await producer.disconnect()
        else await producer.disconnect({ timeout: 0 })
      }
    }
  )
})

/**
 * Runs `callback` once `turns` further microtask turns have passed.
 * @param {number} turns
 * @param {() => void} callback
 * @returns {void}
 */
const afterTurns = (turns, callback) =>
  queueMicrotask(() => (turns === 0 ? callback() : afterTurns(turns - 1, callback)))

/**
 * Commits a transaction with staged offsets whose EndTxn answers at once, and forces a close
 * `turns` microtask turns after that answer left the fake broker.
 * @param {number} version transaction version of the coordinator
 * @param {number} turns microtask turns between the EndTxn answer and the forced close
 * @returns {Promise<object>} the commit's outcome, the manager's state when the close began and
 *   after it, whether the commit is held, and the fixture
 */
const commitForcedAfter = async (version, turns) => {
  const fixture = await build(version)
  const { producer, cluster, transaction } = fixture
  await transaction.sendOffsets({
    consumerGroupId: 'group',
    topics: [{ topic: 'input', partitions: [{ partition: 0, offset: '1' }] }],
  })
  const transitionTo = ProducerStateManager.prototype.transitionTo
  let manager
  jest.spyOn(ProducerStateManager.prototype, 'transitionTo').mockImplementation(function (state) {
    manager = this
    return transitionTo.call(this, state)
  })
  let stateAtClose, closing
  cluster.broker.endTxn.mockImplementation(async () => {
    afterTurns(turns, () => {
      stateAtClose = manager.state()
      closing = producer.disconnect({ timeout: 0 })
    })
    return { producerId: '1000', producerEpoch: 3 }
  })
  const committing = watch(transaction.commit())
  await flushAll()
  await closing
  return {
    ...fixture,
    committing,
    stateAtClose,
    state: manager.state(),
    held: manager.transaction.commitPending(),
  }
}

describe('a commit answered just before a forced close', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'queueMicrotask'] }))
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test.each([1, 2])(
    'a close that lands before the commit resumes rejects it and keeps it pending: version %s',
    async (version) => {
      const { committing, stateAtClose, state, held, cluster } = await commitForcedAfter(version, 4)

      expect(stateAtClose).toBe('COMMITTING')
      expect(committing).toMatchObject({
        state: 'rejected',
        value: { name: 'KafkaJSProducerClosedError', outcome: 'unknown' },
      })
      expect(state).toBe('COMMITTING')
      expect(held).toBe(true)
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    }
  )

  test.each([1, 2])(
    'a close that lands after the commit finished leaves it fulfilled: version %s',
    async (version) => {
      const { committing, stateAtClose, state, held, cluster } = await commitForcedAfter(version, 8)

      expect(stateAtClose).toBe('READY')
      expect(committing.state).toBe('fulfilled')
      expect(state).toBe('READY')
      expect(held).toBe(false)
      expect(cluster.markOffsetAsCommitted).toHaveBeenCalledTimes(1)
    }
  )
})

/**
 * A connected transactional producer over the fake cluster, its coordinator at transaction version
 * `version`, with no transaction begun and no producer id held by its transaction.
 * @param {number} version
 * @returns {Promise<object>} the fixture of `bufferedProducer`
 */
const connected = async (version) => {
  const fixture = bufferedProducer({
    transactionalId: 'close-transaction',
    idempotent: true,
    deliveryTimeout: 120000,
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
  })
  const { broker } = fixture.cluster
  broker.produce.mockImplementation(async (request) => succeeded(request))
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: version } },
    finalizedFeaturesEpoch: 1,
    negotiatedVersion: () => 5,
    addOffsetsToTxn: jest.fn(async () => ({})),
    txnOffsetCommit: jest.fn(async () => ({})),
  })
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.endTxn.mockImplementation(async () => ({ producerId: '-1', producerEpoch: -1 }))
  await fixture.producer.connect()
  return fixture
}

/** @returns {number} every request the fake broker answered and every lookup the cluster served */
const requestsSeen = ({ cluster }) =>
  Object.values(cluster.broker)
    .concat(cluster.findGroupCoordinator, cluster.findConnectedBroker, cluster.refreshMetadata)
    .filter((member) => jest.isMockFunction(member))
    .reduce((total, member) => total + member.mock.calls.length, 0)

const TRANSACTION = { topic: 'topic', messages: [{ value: 'value' }] }

/**
 * Sends one record in `transaction` and waits for its answer, under fake timers.
 * @param {object} transaction
 * @returns {Promise<void>}
 */
const sendOne = async (transaction) => {
  const sent = transaction.send(TRANSACTION)
  await flushAll()
  await sent
}
const OFFSETS = {
  consumerGroupId: 'group',
  topics: [{ topic: 'input', partitions: [{ partition: 0, offset: '1' }] }],
}

/**
 * Each control-plane step that can be held when a close is forced: how it is reached, the call that
 * holds it, and the caller that must settle locally without waiting for the held answer.
 */
const HELD_STEPS = [
  [
    'initialization',
    [1, 2],
    async ({ cluster, producer }) => {
      const held = deferred()
      cluster.broker.initProducerId.mockImplementation(async () => held.promise)
      return { held, caller: watch(producer.transaction()) }
    },
  ],
  [
    'the coordinator lookup of a commit',
    [1, 2],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      await sendOne(transaction)
      const held = deferred()
      cluster.findGroupCoordinator.mockImplementation(async () => held.promise)
      return { held, caller: watch(transaction.commit()) }
    },
  ],
  [
    'AddPartitionsToTxn',
    [1],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      const held = deferred()
      cluster.broker.addPartitionsToTxn.mockImplementation(async () => held.promise)
      return { held, caller: watch(transaction.send(TRANSACTION)) }
    },
  ],
  [
    'AddOffsetsToTxn',
    [1],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      const held = deferred()
      cluster.broker.addOffsetsToTxn.mockImplementation(async () => held.promise)
      return { held, caller: watch(transaction.sendOffsets(OFFSETS)) }
    },
  ],
  [
    'TxnOffsetCommit',
    [1, 2],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      const held = deferred()
      cluster.broker.txnOffsetCommit.mockImplementation(async () => held.promise)
      return { held, caller: watch(transaction.sendOffsets(OFFSETS)) }
    },
  ],
  [
    'the EndTxn of an abort',
    [1, 2],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      await sendOne(transaction)
      const held = deferred()
      cluster.broker.endTxn.mockImplementation(async () => held.promise)
      return { held, caller: watch(transaction.abort()) }
    },
  ],
  [
    'the reinitialization after an abort',
    [1, 2],
    async ({ cluster, producer }) => {
      const transaction = await producer.transaction()
      await sendOne(transaction)
      const held = deferred()
      cluster.broker.endTxn.mockRejectedValue(new KafkaJSConnectionError('Connection error'))
      let reinitializing = false
      cluster.broker.initProducerId.mockImplementation(async () => {
        reinitializing = true
        return held.promise
      })
      const caller = watch(transaction.abort())
      // eslint-disable-next-line no-unmodified-loop-condition -- the broker mock sets it while the loop awaits
      for (let i = 0; i < 20 && !reinitializing; i += 1) {
        await jest.advanceTimersByTimeAsync(5)
        await flushAll()
      }
      expect(reinitializing).toBe(true)
      return { held, caller }
    },
  ],
]

describe('transaction close with a control-plane step held', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test.each(
    HELD_STEPS.flatMap(([step, versions, hold]) => versions.map((version) => [step, version, hold]))
  )(
    '%s settles its caller at the deadline, and nothing runs when it is released: version %s',
    async (_, version, hold) => {
      const fixture = await connected(version)
      const { producer, cluster, admission } = fixture
      const { held, caller } = await hold(fixture)
      try {
        await flushAll()
        expect(caller.state).toBe('pending')
        const closed = watch(producer.disconnect({ timeout: 10 }))
        await jest.advanceTimersByTimeAsync(10)
        await flushAll()
        expect(closed.state).toBe('fulfilled')
        expect(caller.value).toMatchObject({ name: 'KafkaJSProducerClosedError' })
        expect(cluster.disconnect).toHaveBeenCalledTimes(1)

        const seen = requestsSeen(fixture)
        held.resolve({ producerId: '1000', producerEpoch: 7 })
        await jest.advanceTimersByTimeAsync(1000)
        await flushAll()
        expect(requestsSeen(fixture)).toBe(seen)
        expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(jest.getTimerCount()).toBe(0)
        expect(admission.available).toBe(admission.capacity)

        await producer.connect()
        const sent = producer.send({ topic: 'other', messages: [{ value: 'plain' }] })
        await flushAll()
        await sent
      } finally {
        held.resolve({ producerId: '-1', producerEpoch: -1 })
        await flushAll()
        await producer.disconnect({ timeout: 0 })
      }
    }
  )
})
