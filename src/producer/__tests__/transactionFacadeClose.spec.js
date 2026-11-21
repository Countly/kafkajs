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
  const { KafkaJSApplicationRecoverableError } = require('../../errors')
  const TOPIC = 'forced-close'
  const TAKEN_OVER =
    /^Another producer took over transactional id forced-close, or the transaction coordinator aborted its transaction on the transaction timeout \(.+\); create a new producer$/

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
   * Opens a transaction at epoch 0 and leaves its abort's outcome unknown, then reconnects:
   * `'close'` closes the producer while `abort()` waits for an EndTxn handled as `mode`;
   * `'disconnect'` lets a `disconnect({ timeout })` end the abort it runs for the open
   * transaction; `'refused'` has every EndTxn of `abort()` refused until its retries run out.
   * @param {ReturnType<typeof setUp>} setup
   * @param {object} options
   * @param {'close'|'disconnect'|'refused'} [options.via='close']
   * @param {string} [options.mode='lost'] the EndTxn mode while the abort runs
   * @param {number} [options.markerRounds=0]
   * @returns {Promise<?Error>} what the abort rejected with
   */
  const abortUnknown = async (
    { producer, coordinator, log, setEndTxn },
    { via = 'close', mode = 'lost', markerRounds = 0 } = {}
  ) => {
    await producer.connect()
    const transaction = await openTransaction(producer)
    coordinator.markerRounds = markerRounds
    let abortError = null
    if (via === 'refused') {
      setEndTxn('refuse')
      await transaction.abort()
    } else if (via === 'disconnect') {
      setEndTxn(mode)
      jest.useFakeTimers({ doNotFake: ['Date', 'nextTick', 'setImmediate', 'queueMicrotask'] })
      try {
        const closing = producer.disconnect({ timeout: 1000 })
        await untilEndTxn(log)
        await jest.advanceTimersByTimeAsync(1000)
        await closing
      } finally {
        jest.useRealTimers()
      }
    } else {
      setEndTxn(mode)
      const aborted = transaction.abort().then(
        () => null,
        (error) => error
      )
      await untilEndTxn(log)
      await producer.disconnect({ timeout: 0 })
      abortError = await aborted
    }
    setEndTxn('answer')
    await producer.connect()
    return abortError
  }

  /**
   * Runs a whole transaction and returns the requests it sent, in order.
   * @param {object} producer
   * @param {string[]} log
   * @returns {Promise<string[]>}
   */
  const runTransaction = async (producer, log) => {
    const from = log.length
    const transaction = await openTransaction(producer)
    await transaction.commit()
    return log.slice(from)
  }

  /**
   * The requests of the next transaction after an abort at epoch 0 whose outcome was unknown: the
   * abort sent again with the pair held (`resends` times), then under version 1 the epoch bump
   * with that pair, under version 2 the epoch the EndTxn answer carries, and the transaction.
   * @param {number} version
   * @param {number} [resends=1]
   * @returns {string[]}
   */
  const afterResend = (version, resends = 1) => [
    ...Array(resends).fill('endTxn:abort:1000/0'),
    ...(version === 1 ? ['initProducerId:1000/0', 'addPartitionsToTxn:1'] : []),
    'produce:1',
    'endTxn:commit:1000/1',
  ]

  /** @returns {{ state: string, epoch: number }} the coordinator's state and epoch */
  const coordinatorAt = ({ state, epoch }) => ({ state, epoch })

  /**
   * @param {string[]} log
   * @returns {string[]} the requests that carried no producer id and epoch
   */
  const fresh = (log) => log.filter((entry) => entry.includes('-1/-1'))

  /**
   * A rival producer initializing the same transactional id with -1/-1, retrying
   * CONCURRENT_TRANSACTIONS as a client does.
   * @param {TransactionCoordinatorModel} coordinator
   * @returns {{ producerId: string, producerEpoch: number }}
   */
  const rivalInit = (coordinator) => {
    for (;;) {
      try {
        return coordinator.initProducerId({})
      } catch (error) {
        if (error.type !== 'CONCURRENT_TRANSACTIONS') throw error
      }
    }
  }

  /**
   * The next transaction() after a takeover, and the one after it.
   * @param {object} producer
   * @returns {Promise<{ first: Error, second: Error }>}
   */
  const twoRefusedStarts = async (producer) => {
    const first = await producer.transaction().then(
      () => null,
      (error) => error
    )
    const second = await producer.transaction().then(
      () => null,
      (error) => error
    )
    return { first, second }
  }

  describe.each([1, 2])('transaction version %i', (version) => {
    test.each(['close', 'disconnect', 'refused'])(
      'an abort that never reached the coordinator (%s) is sent again with the pair held before the next transaction, which commits',
      async (via) => {
        const setup = setUp(version)
        const { producer, coordinator, log } = setup
        try {
          const abortError = await abortUnknown(setup, { via })
          const atAbort = coordinatorAt(coordinator)
          const requests = await runTransaction(producer, log)
          expect({
            abortError: abortError?.name ?? null,
            atAbort,
            requests,
            fresh: fresh(log),
            at: coordinatorAt(coordinator),
          }).toEqual({
            abortError: via === 'close' ? 'KafkaJSProducerClosedError' : null,
            atAbort: { state: 'ONGOING', epoch: 0 },
            requests: afterResend(version),
            fresh: ['initProducerId:-1/-1'],
            at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 1 : 2 },
          })
        } finally {
          await producer.disconnect({ timeout: 0 })
        }
      }
    )

    test('an abort the coordinator applied whose answer was lost is answered with success when sent again, with no second bump, and the next transaction commits', async () => {
      const setup = setUp(version)
      const { producer, coordinator, log } = setup
      try {
        await abortUnknown(setup, { mode: 'unanswered' })
        const atAbort = coordinatorAt(coordinator)
        const requests = await runTransaction(producer, log)
        expect({ atAbort, requests, at: coordinatorAt(coordinator) }).toEqual({
          atAbort: { state: 'COMPLETE_ABORT', epoch: version === 1 ? 0 : 1 },
          requests: afterResend(version),
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 1 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    })

    test('an abort sent again while the coordinator writes its markers retries CONCURRENT_TRANSACTIONS, then the next transaction commits', async () => {
      const setup = setUp(version)
      const { producer, coordinator, log } = setup
      try {
        await abortUnknown(setup, { mode: 'unanswered', markerRounds: 2 })
        const atAbort = coordinatorAt(coordinator)
        const requests = await runTransaction(producer, log)
        expect({ atAbort, requests, at: coordinatorAt(coordinator) }).toEqual({
          atAbort: { state: 'PREPARE_ABORT', epoch: version === 1 ? 0 : 1 },
          requests: afterResend(version, 3),
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 1 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    })

    test.each([
      { rival: 'idle', state: 'EMPTY' },
      { rival: 'in a transaction', state: 'ONGOING' },
    ])(
      'a rival that took over the transactional id ($rival) makes the old producer fatal on the abort sent again, and is left untouched',
      async ({ rival, state }) => {
        const setup = setUp(version)
        const { producer, coordinator, log } = setup
        try {
          await abortUnknown(setup)
          const { producerEpoch } = rivalInit(coordinator)
          if (rival !== 'idle') coordinator.addPartitions({ producerEpoch })
          const from = log.length

          const { first, second } = await twoRefusedStarts(producer)
          expect(first).toBeInstanceOf(KafkaJSApplicationRecoverableError)
          expect({
            message: first.message,
            cause: first.cause?.type,
            second,
            sent: log.slice(from),
            inits: log.filter((entry) => entry.startsWith('initProducerId')),
            at: coordinatorAt(coordinator),
          }).toEqual({
            message: expect.stringMatching(TAKEN_OVER),
            cause: 'PRODUCER_FENCED',
            second: first,
            sent: ['endTxn:abort:1000/0'],
            inits: ['initProducerId:-1/-1'],
            at: { state, epoch: 2 },
          })
        } finally {
          await producer.disconnect({ timeout: 0 })
        }
      }
    )
  })

  test('a transaction the coordinator aborted on its timeout makes the producer fatal on the abort sent again, transaction version 1', async () => {
    const setup = setUp(1)
    const { producer, coordinator, log } = setup
    try {
      await abortUnknown(setup)
      coordinator.timeout()
      const from = log.length

      const { first, second } = await twoRefusedStarts(producer)
      expect(first).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect({
        message: first.message,
        cause: first.cause?.type,
        second,
        sent: log.slice(from),
        at: coordinatorAt(coordinator),
      }).toEqual({
        message: expect.stringMatching(TAKEN_OVER),
        cause: 'PRODUCER_FENCED',
        second: first,
        sent: ['endTxn:abort:1000/0'],
        at: { state: 'COMPLETE_ABORT', epoch: 1 },
      })
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })

  test('a transaction the coordinator aborted on its timeout accepts the abort sent again as a retry, and the next transaction commits, transaction version 2', async () => {
    const setup = setUp(2)
    const { producer, coordinator, log } = setup
    try {
      await abortUnknown(setup)
      coordinator.timeout()
      const atTimeout = coordinatorAt(coordinator)
      const requests = await runTransaction(producer, log)
      expect({ atTimeout, requests, at: coordinatorAt(coordinator) }).toEqual({
        atTimeout: { state: 'COMPLETE_ABORT', epoch: 1 },
        requests: afterResend(2),
        at: { state: 'COMPLETE_COMMIT', epoch: 2 },
      })
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })

  test("an abort sent again between a rival's fence and the rival's retry is accepted, the rival then gets the next epoch untouched, and the old producer is fatal on its next request, transaction version 2", async () => {
    const setup = setUp(2)
    const { producer, coordinator, log } = setup
    try {
      await abortUnknown(setup)
      expect(() => coordinator.initProducerId({})).toThrow(
        expect.objectContaining({ type: 'CONCURRENT_TRANSACTIONS' })
      )
      const transaction = await producer.transaction()
      const fenced = coordinatorAt(coordinator)
      const rival = rivalInit(coordinator)
      const from = log.length
      const sent = await transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] }).then(
        () => null,
        (error) => error
      )
      const next = await producer.transaction().then(
        () => null,
        (error) => error
      )
      expect({
        fenced,
        rival,
        sentCode: sent?.type,
        requests: log.slice(from),
        next: next?.message,
        nextCause: next?.cause?.type,
        at: coordinatorAt(coordinator),
      }).toEqual({
        fenced: { state: 'COMPLETE_ABORT', epoch: 1 },
        rival: { producerId: '1000', producerEpoch: 2 },
        sentCode: 'PRODUCER_FENCED',
        requests: ['produce:1'],
        next: expect.stringMatching(TAKEN_OVER),
        nextCause: 'PRODUCER_FENCED',
        at: { state: 'EMPTY', epoch: 2 },
      })
      expect(log.filter((entry) => entry.startsWith('endTxn'))).toEqual([
        'endTxn:abort:1000/0',
        'endTxn:abort:1000/0',
      ])
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })

  test.each([1, 2])(
    'an abort that failed to be sent again stays owed: the next transaction() sends it again, and concurrent calls share one, transaction version %i',
    async (version) => {
      const setup = setUp(version)
      const { producer, coordinator, log, setEndTxn } = setup
      try {
        await abortUnknown(setup)
        setEndTxn('refuse')
        const before = log.length
        const failed = await producer.transaction().then(
          () => null,
          (error) => error
        )
        const refused = log.slice(before)
        const atFailure = coordinatorAt(coordinator)
        setEndTxn('answer')
        const from = log.length
        const starts = await Promise.allSettled([producer.transaction(), producer.transaction()])
        const resent = log.slice(from).filter((entry) => entry.startsWith('endTxn'))
        const begun = starts.filter(({ status }) => status === 'fulfilled')
        await begun[0].value.abort()
        expect({
          failed: failed?.name,
          refused: [...new Set(refused)],
          attempts: refused.length > 1,
          atFailure,
          resent,
          begun: begun.length > 0,
        }).toEqual({
          failed: 'KafkaJSNumberOfRetriesExceeded',
          refused: ['endTxn:abort:1000/0'],
          attempts: true,
          atFailure: { state: 'ONGOING', epoch: 0 },
          resent: ['endTxn:abort:1000/0'],
          begun: true,
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a disconnect that completes the abort of an open transaction owes nothing, transaction version %i',
    async (version) => {
      const { producer, coordinator, log } = setUp(version)
      try {
        await producer.connect()
        await openTransaction(producer)
        await producer.disconnect()
        expect(log.at(-1)).toBe('endTxn:abort:1000/0')

        await producer.connect()
        const requests = await runTransaction(producer, log)
        expect({ requests, at: coordinatorAt(coordinator) }).toEqual({
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:1000/0']
              : ['produce:1', 'endTxn:commit:1000/1'],
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 0 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test.each([1, 2])(
    'a plain disconnect and connect with nothing owed sends no InitProducerId, transaction version %i',
    async (version) => {
      const { producer, log } = setUp(version)
      try {
        await producer.connect()
        await runTransaction(producer, log)
        await producer.disconnect()
        await producer.connect()
        const requests = await runTransaction(producer, log)
        expect({
          inits: log.filter((entry) => entry.startsWith('initProducerId')),
          requests,
        }).toEqual({
          inits: ['initProducerId:-1/-1'],
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:1000/0']
              : ['produce:1', 'endTxn:commit:1000/1'],
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
          retried: ['endTxn:commit:1000/0'],
          requests:
            version === 1
              ? ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:1000/0']
              : ['produce:1', 'endTxn:commit:1000/1'],
          at: { state: 'COMPLETE_COMMIT', epoch: version === 1 ? 0 : 2 },
        })
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    }
  )

  test('a close forced while abort() waits for an AddPartitionsToTxn of an otherwise empty transaction owes no EndTxn, and the next transaction commits, transaction version 1', async () => {
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

      await producer.connect()
      const requests = await runTransaction(producer, log)
      expect({
        abortError: abortError?.name,
        endTxn: log.filter((entry) => entry.startsWith('endTxn')),
        requests,
        at: coordinatorAt(coordinator),
      }).toEqual({
        abortError: 'KafkaJSProducerClosedError',
        endTxn: ['endTxn:commit:1000/0'],
        requests: ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:1000/0'],
        at: { state: 'COMPLETE_COMMIT', epoch: 0 },
      })
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })

  test('an abort of a transaction with nothing in it sends no EndTxn, then or before the next transaction, transaction version 1', async () => {
    const { producer, log, setEndTxn } = setUp(1)
    try {
      await producer.connect()
      const transaction = await producer.transaction()
      setEndTxn('refuse')
      await transaction.abort()
      setEndTxn('answer')
      const requests = await runTransaction(producer, log)
      expect({ endTxn: log.filter((entry) => entry.startsWith('endTxn')), requests }).toEqual({
        endTxn: ['endTxn:commit:1000/0'],
        requests: ['addPartitionsToTxn:0', 'produce:0', 'endTxn:commit:1000/0'],
      })
    } finally {
      await producer.disconnect({ timeout: 0 })
    }
  })
})
