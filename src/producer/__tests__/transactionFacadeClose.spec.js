const { bufferedProducer } = require('./fixtures/bufferHarness')
const ProducerStateManager = require('../state')

/**
 * Runs `fn` once `count` promise continuations have passed: the producer's own awaits between
 * the begin call returning and `transaction()` resuming.
 * @param {number} count empty continuations to let pass first
 * @param {() => void} fn
 * @returns {Promise<void>}
 */
const later = (count, fn) => {
  let next = Promise.resolve()
  for (let i = 0; i < count; i += 1) next = next.then(() => {})
  return next.then(fn)
}

describe('transaction facade after a forced close', () => {
  test.each([1, 2])(
    'a close between the begin and the facade rejects the start, transaction version %i',
    async (version) => {
      const { producer, cluster } = bufferedProducer({ transactionalId: 'facade-close' })
      Object.assign(cluster.broker, {
        finalizedFeatures: { 'transaction.version': { maxVersionLevel: version } },
        finalizedFeaturesEpoch: 1,
        negotiatedVersion: () => 5,
      })
      await producer.connect()

      let state
      let stateAtClose
      let closing
      const transition = ProducerStateManager.prototype.transitionTo
      const observation = jest
        .spyOn(ProducerStateManager.prototype, 'transitionTo')
        .mockImplementation(function (...args) {
          const result = transition.apply(this, args)
          state = this.state()
          return result
        })

      // The second lookup is the begin's, after initialization; the close lands once the manager
      // is in the transaction but `transaction()` has not resumed yet.
      let lookups = 0
      cluster.findGroupCoordinator.mockImplementation(async () => {
        lookups += 1
        if (lookups === 2) {
          later(2, () => {
            stateAtClose = state
            closing = producer.disconnect({ timeout: 0 })
          })
        }
        return cluster.broker
      })

      try {
        const outcome = await producer.transaction().then(
          (transaction) => ({ transaction }),
          (error) => ({ error })
        )
        await closing
        expect(stateAtClose).toBe('IN_TRANSACTION')
        expect(outcome.error?.name).toBe('KafkaJSProducerClosedError')
        const stateAfterReject = state
        const endTxnCalls = cluster.broker.endTxn.mock.calls.length
        await producer.connect()
        const again = await producer.transaction().then(
          (transaction) => ({ started: true, transaction }),
          (error) => ({ started: false, name: error.name, message: error.message })
        )
        expect({ stateAfterReject, endTxnCalls, again }).toEqual({
          stateAfterReject: 'READY',
          endTxnCalls: 0,
          again: { started: true, transaction: expect.any(Object) },
        })
        if (again.transaction) await again.transaction.abort()
      } finally {
        observation.mockRestore()
        await producer.disconnect({ timeout: 0 })
      }
    }
  )
})

describe('reconnecting after a close that forced an abort or a commit', () => {
  const { flushAll } = require('./fixtures/bufferHarness')
  const { TransactionCoordinatorModel } = require('./fixtures/transactionCoordinator')
  const TOPIC = 'forced-close'

  /**
   * A transactional producer on transaction version `version` over a coordinator that follows the
   * Kafka 4.4.0 rules (`fixtures/transactionCoordinator.js`), logging every request in order.
   * @param {number} version
   * @returns {{ producer: object, coordinator: TransactionCoordinatorModel, log: string[],
   *   setEndTxn: (mode: string) => void, holdAddPartitions: (hold: boolean) => void }}
   */
  const setUp = (version) => {
    const { producer, cluster } = bufferedProducer({
      transactionalId: 'forced-close',
      idempotent: true,
      deliveryTimeout: 60000,
      retry: { initialRetryTime: 5 },
    })
    const coordinator = new TransactionCoordinatorModel({ version })
    Object.assign(cluster.broker, {
      finalizedFeatures: { 'transaction.version': { maxVersionLevel: version } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
    })
    return { producer, coordinator, ...coordinator.install(cluster.broker) }
  }

  /**
   * Starts a transaction and sends one record in it.
   * @param {object} producer
   * @returns {Promise<object>} the transaction
   */
  const openTransaction = async (producer) => {
    const transaction = await producer.transaction()
    await transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] })
    return transaction
  }

  /**
   * Waits until the broker has seen an EndTxn.
   * @param {string[]} log
   * @returns {Promise<void>}
   */
  const untilEndTxn = async (log) => {
    for (let turn = 0; turn < 50 && !log.some((entry) => entry.startsWith('endTxn')); turn += 1) {
      await flushAll()
    }
    if (!log.some((entry) => entry.startsWith('endTxn'))) throw new Error(`no EndTxn: ${log}`)
  }

  /**
   * After a reconnect, runs a whole transaction and returns the requests it sent, in order.
   * @param {object} producer
   * @param {string[]} log
   * @returns {Promise<string[]>}
   */
  const runTransaction = async (producer, log) => {
    await producer.connect()
    const from = log.length
    const transaction = await openTransaction(producer)
    await transaction.commit()
    return log.slice(from)
  }

  /**
   * The requests of the next transaction after a close left the coordinator holding the
   * transaction at epoch 0: a fresh InitProducerId fences and aborts it (CONCURRENT_TRANSACTIONS),
   * its retry gets epoch 2, and the transaction commits at that epoch.
   * @param {number} version
   * @returns {string[]}
   */
  const afterFence = (version) =>
    version === 1
      ? [
          'initProducerId:-1',
          'initProducerId:-1',
          'addPartitionsToTxn:2',
          'produce:2',
          'endTxn:commit:2',
        ]
      : ['initProducerId:-1', 'initProducerId:-1', 'produce:2', 'endTxn:commit:2']

  /**
   * @param {TransactionCoordinatorModel} coordinator
   * @returns {{ state: string, epoch: number }}
   */
  const coordinatorAt = ({ state, epoch }) => ({ state, epoch })

  test.each([1, 2])(
    'a close forced while abort() waits for its EndTxn leaves a producer whose next transaction bumps the epoch first and commits, transaction version %i',
    async (version) => {
      const { producer, coordinator, log, setEndTxn } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        setEndTxn('lost')
        const aborted = transaction.abort().then(
          () => null,
          (error) => error
        )
        await untilEndTxn(log)
        await producer.disconnect({ timeout: 0 })
        const abortError = await aborted
        setEndTxn('answer')

        const requests = await runTransaction(producer, log)
        expect({ abortError: abortError?.name, requests, at: coordinatorAt(coordinator) }).toEqual({
          abortError: 'KafkaJSProducerClosedError',
          requests: afterFence(version),
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 2 : 3 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a disconnect whose timeout ends the abort of an open transaction leaves a producer whose next transaction bumps the epoch first and commits, transaction version %i',
    async (version) => {
      const { producer, coordinator, log, setEndTxn } = setUp(version)
      try {
        await producer.connect()
        await openTransaction(producer)
        setEndTxn('lost')
        jest.useFakeTimers({ doNotFake: ['Date', 'nextTick', 'setImmediate', 'queueMicrotask'] })
        const closing = producer.disconnect({ timeout: 1000 })
        await untilEndTxn(log)
        expect(log.at(-1)).toBe('endTxn:abort:0')
        await jest.advanceTimersByTimeAsync(1000)
        await closing
        jest.useRealTimers()
        setEndTxn('answer')

        const requests = await runTransaction(producer, log)
        expect({ requests, at: coordinatorAt(coordinator) }).toEqual({
          requests: afterFence(version),
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 2 : 3 },
        })
      } finally {
        jest.useRealTimers()
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a disconnect that completes the abort of an open transaction owes no epoch bump, transaction version %i',
    async (version) => {
      const { producer, coordinator, log } = setUp(version)
      try {
        await producer.connect()
        await openTransaction(producer)
        await producer.disconnect()
        expect(log.at(-1)).toBe('endTxn:abort:0')

        const requests = await runTransaction(producer, log)
        expect({ requests, at: coordinatorAt(coordinator) }).toEqual({
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:0']
              : ['produce:1', 'endTxn:commit:1'],
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 0 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a close forced while commit() waits for its EndTxn keeps the commit pending across a reconnect, transaction version %i',
    async (version) => {
      const { producer, coordinator, log, setEndTxn } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        setEndTxn('lost')
        const committed = transaction.commit().then(
          () => null,
          (error) => error
        )
        await untilEndTxn(log)
        await producer.disconnect({ timeout: 0 })
        const commitError = await committed
        setEndTxn('answer')

        await producer.connect()
        const begin = await producer.transaction().then(
          () => null,
          (error) => error.message
        )
        const from = log.length
        await transaction.commit()
        const retried = log.slice(from)
        const requests = await runTransaction(producer, log)
        expect({
          commitError: commitError?.name,
          outcome: commitError?.outcome,
          begin,
          retried,
          requests,
          at: coordinatorAt(coordinator),
        }).toEqual({
          commitError: 'KafkaJSProducerClosedError',
          outcome: 'unknown',
          begin:
            'Transaction state exception: Cannot call "beginTransaction" while a commit is pending: commit() did not complete; call commit() again',
          retried: ['endTxn:commit:0'],
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:0']
              : ['produce:1', 'endTxn:commit:1'],
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 0 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([
    {
      version: 1,
      endTxn: 'applied, its answer lost',
      mode: 'unanswered',
      markerRounds: 0,
      expected: ['initProducerId:-1', 'addPartitionsToTxn:1', 'produce:1', 'endTxn:commit:1'],
      epoch: 1,
    },
    {
      version: 2,
      endTxn: 'applied, its answer lost',
      mode: 'unanswered',
      markerRounds: 0,
      expected: ['initProducerId:-1', 'produce:2', 'endTxn:commit:2'],
      epoch: 3,
    },
    {
      version: 1,
      endTxn: 'applied with its markers pending',
      mode: 'unanswered',
      markerRounds: 1,
      expected: [
        'initProducerId:-1',
        'initProducerId:-1',
        'addPartitionsToTxn:1',
        'produce:1',
        'endTxn:commit:1',
      ],
      epoch: 1,
    },
    {
      version: 2,
      endTxn: 'applied with its markers pending',
      mode: 'unanswered',
      markerRounds: 1,
      expected: afterFence(2),
      epoch: 3,
    },
  ])(
    'a close forced while abort() waits for an EndTxn the coordinator $endTxn leaves a producer whose next transaction starts fresh and commits, transaction version $version',
    async ({ version, mode, markerRounds, expected, epoch }) => {
      const { producer, coordinator, log, setEndTxn } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        coordinator.markerRounds = markerRounds
        setEndTxn(mode)
        const aborted = transaction.abort().then(
          () => null,
          (error) => error
        )
        await untilEndTxn(log)
        await producer.disconnect({ timeout: 0 })
        const abortError = await aborted
        setEndTxn('answer')

        const requests = await runTransaction(producer, log)
        expect({ abortError: abortError?.name, requests, at: coordinatorAt(coordinator) }).toEqual({
          abortError: 'KafkaJSProducerClosedError',
          requests: expected,
          at: { state: 'COMPLETE_COMMIT', epoch },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test('a close forced while abort() waits for an AddPartitionsToTxn the coordinator applied leaves a producer whose next transaction starts fresh and commits, transaction version 1', async () => {
    const { producer, coordinator, log, holdAddPartitions } = setUp(1)
    try {
      await producer.connect()
      const transaction = await producer.transaction()
      holdAddPartitions(true)
      const sent = transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] }).then(
        () => null,
        (error) => error
      )
      for (let turn = 0; turn < 50 && !log.includes('addPartitionsToTxn:0'); turn += 1) {
        await flushAll()
      }
      const aborted = transaction.abort().then(
        () => null,
        (error) => error
      )
      await flushAll()
      await producer.disconnect({ timeout: 0 })
      const abortError = await aborted
      await sent
      holdAddPartitions(false)

      const before = log.length
      const requests = await runTransaction(producer, log)
      expect({
        added: log.slice(0, before).filter((entry) => !entry.startsWith('initProducerId')),
        abortError: abortError?.name,
        requests,
        at: coordinatorAt(coordinator),
      }).toEqual({
        added: ['addPartitionsToTxn:0'],
        abortError: 'KafkaJSProducerClosedError',
        requests: afterFence(1),
        at: { state: 'COMPLETE_COMMIT', epoch: 2 },
      })
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })

  test.each([1, 2])(
    "an abort whose EndTxn is refused until its retries run out ends the coordinator's transaction with a fresh InitProducerId, transaction version %i",
    async (version) => {
      const { producer, coordinator, log, setEndTxn } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        setEndTxn('refuse')
        const from = log.length
        await transaction.abort()
        const abort = log.slice(from).filter((entry) => !entry.startsWith('endTxn'))
        const atAbort = coordinatorAt(coordinator)
        setEndTxn('answer')

        const requests = await runTransaction(producer, log)
        expect({ abort, atAbort, requests, at: coordinatorAt(coordinator) }).toEqual({
          abort: ['initProducerId:-1', 'initProducerId:-1'],
          atAbort: { state: 'EMPTY', epoch: 2 },
          requests:
            version === 1
              ? ['addPartitionsToTxn:2', 'produce:2', 'endTxn:commit:2']
              : ['produce:2', 'endTxn:commit:2'],
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 2 : 3 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )
})
