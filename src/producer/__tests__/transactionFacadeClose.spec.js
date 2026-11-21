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
  const { deferred, flushAll } = require('./fixtures/bufferHarness')
  const TOPIC = 'forced-close'

  /**
   * A transactional producer on transaction version `version` whose broker logs every request in
   * order, bumps the epoch on each InitProducerId, and holds an EndTxn while `holdEndTxn` is set.
   * @param {number} version
   * @returns {{ producer: object, log: string[], hold: (on: boolean) => void }}
   */
  const setUp = (version) => {
    const { producer, cluster } = bufferedProducer({
      transactionalId: 'forced-close',
      idempotent: true,
      deliveryTimeout: 60000,
    })
    const { broker } = cluster
    const log = []
    let holdEndTxn = false
    broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => {
      log.push(`initProducerId:${producerEpoch}`)
      return { producerId: '1000', producerEpoch: producerEpoch + 1 }
    })
    const addPartitions = broker.addPartitionsToTxn.getMockImplementation()
    broker.addPartitionsToTxn.mockImplementation(async (request) => {
      log.push(`addPartitionsToTxn:${request.producerEpoch}`)
      return addPartitions(request)
    })
    const produce = broker.produce.getMockImplementation()
    broker.produce.mockImplementation(async (request) => {
      log.push(`produce:${request.producerEpoch}`)
      return produce(request)
    })
    broker.endTxn.mockImplementation(async ({ producerEpoch, transactionResult }) => {
      log.push(`endTxn:${transactionResult ? 'commit' : 'abort'}:${producerEpoch}`)
      if (holdEndTxn) await deferred().promise
      return { producerId: '-1', producerEpoch: -1 }
    })
    Object.assign(broker, {
      finalizedFeatures: { 'transaction.version': { maxVersionLevel: version } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
    })
    return { producer, log, hold: (on) => (holdEndTxn = on) }
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

  test.each([1, 2])(
    'a close forced while abort() waits for its EndTxn leaves a producer whose next transaction bumps the epoch first and commits, transaction version %i',
    async (version) => {
      const { producer, log, hold } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        hold(true)
        const aborted = transaction.abort().then(
          () => null,
          (error) => error
        )
        await untilEndTxn(log)
        await producer.disconnect({ timeout: 0 })
        const abortError = await aborted
        hold(false)

        const requests = await runTransaction(producer, log)
        expect({ abortError: abortError?.name, requests }).toEqual({
          abortError: 'KafkaJSProducerClosedError',
          requests:
            version === 1
              ? ['initProducerId:0', 'addPartitionsToTxn:1', 'produce:1', 'endTxn:commit:1']
              : ['initProducerId:0', 'produce:1', 'endTxn:commit:1'],
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a disconnect whose timeout ends the abort of an open transaction leaves a producer whose next transaction bumps the epoch first and commits, transaction version %i',
    async (version) => {
      const { producer, log, hold } = setUp(version)
      try {
        await producer.connect()
        await openTransaction(producer)
        hold(true)
        jest.useFakeTimers({ doNotFake: ['Date', 'nextTick', 'setImmediate', 'queueMicrotask'] })
        const closing = producer.disconnect({ timeout: 1000 })
        await untilEndTxn(log)
        expect(log.at(-1)).toBe('endTxn:abort:0')
        await jest.advanceTimersByTimeAsync(1000)
        await closing
        jest.useRealTimers()
        hold(false)

        const requests = await runTransaction(producer, log)
        expect(requests).toEqual(
          version === 1
            ? ['initProducerId:0', 'addPartitionsToTxn:1', 'produce:1', 'endTxn:commit:1']
            : ['initProducerId:0', 'produce:1', 'endTxn:commit:1']
        )
      } finally {
        jest.useRealTimers()
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a disconnect that completes the abort of an open transaction owes no epoch bump, transaction version %i',
    async (version) => {
      const { producer, log } = setUp(version)
      try {
        await producer.connect()
        await openTransaction(producer)
        await producer.disconnect()
        expect(log.at(-1)).toBe('endTxn:abort:0')

        const requests = await runTransaction(producer, log)
        expect(requests).toEqual(
          version === 1
            ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:0']
            : ['produce:0', 'endTxn:commit:0']
        )
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a close forced while commit() waits for its EndTxn keeps the commit pending across a reconnect, transaction version %i',
    async (version) => {
      const { producer, log, hold } = setUp(version)
      try {
        await producer.connect()
        const transaction = await openTransaction(producer)
        hold(true)
        const committed = transaction.commit().then(
          () => null,
          (error) => error
        )
        await untilEndTxn(log)
        await producer.disconnect({ timeout: 0 })
        const commitError = await committed
        hold(false)

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
        }).toEqual({
          commitError: 'KafkaJSProducerClosedError',
          outcome: 'unknown',
          begin:
            'Transaction state exception: Cannot call "beginTransaction" while a commit is pending: commit() did not complete; call commit() again',
          retried: ['endTxn:commit:0'],
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:0']
              : ['produce:0', 'endTxn:commit:0'],
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )
})
