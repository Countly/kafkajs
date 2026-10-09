/**
 * The per-partition limit the next manager's sequencer is built with. Unset, it is the production
 * default of one batch in flight; the blocks that call `withSeveralBatchesInFlight()` raise it.
 */
let mockMaxInFlightPerPartition
jest.mock('../partitionSequencer', () => {
  const PartitionSequencer = jest.requireActual('../partitionSequencer')
  return class extends PartitionSequencer {
    constructor(options) {
      super({ ...options, maxInFlightPerPartition: mockMaxInFlightPerPartition })
    }
  }
})

const ProducerStateManager = require('..')
const createRetry = require('../../../retry')
const { createErrorFromCode } = require('../../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSTransactionAbortableError,
} = require('../../../errors')
const {
  UNINITIALIZED,
  READY,
  IN_TRANSACTION,
  COMMITTING,
  ABORTING,
  ABORTABLE_ERROR,
  FATAL_ERROR,
} = require('../../eosManager/transactionStates')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const NODE = { nodeId: 1 }
/** Broker 1 leads both partitions the cases use. */
const LEADERS = { [TOPIC]: { 1: [0, 1] } }
const RETRIES = 3
const RETRY = { retries: RETRIES, initialRetryTime: 1, maxRetryTime: 5 }
const TV2_COORDINATOR = {
  finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
  finalizedFeaturesEpoch: 1,
}

const REQUEST_TIMED_OUT = 7
const CLUSTER_AUTHORIZATION_FAILED = 31
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const DUPLICATE_SEQUENCE_NUMBER = 46
const INVALID_PRODUCER_EPOCH = 47
const INVALID_PRODUCER_ID_MAPPING = 49
const CONCURRENT_TRANSACTIONS = 51
const UNKNOWN_PRODUCER_ID = 59
const INVALID_RECORD = 87
const PRODUCER_FENCED = 90
const TRANSACTION_ABORTABLE = 120

let ids = 0

/**
 * A batch as the send path builds it: never numbered, never sent, one caller waiting on it.
 * @param {{ partition?: number, records?: number }} [options]
 */
const createBatch = ({ partition = 0, records = 5 } = {}) => {
  ids += 1
  return {
    id: ids,
    topic: TOPIC,
    partition,
    messages: Array.from({ length: records }, (_, index) => ({ value: `value-${index}` })),
    acks: -1,
    timeout: 30000,
    compression: 0,
    firstSequence: null,
    producerId: null,
    producerEpoch: null,
    attempt: 0,
    settlers: [{ resolve: jest.fn(), reject: jest.fn() }],
    backoffUntil: 0,
    createdAt: 0,
    sequenceReset: false,
  }
}

/**
 * A coordinator answering InitProducerId: producer id 1000 at epoch 0 for a first
 * initialization, the same id one epoch higher for a re-initialization.
 */
const createCoordinator = () => ({
  nodeId: 1,
  negotiatedVersion: () => 5,
  initProducerId: jest.fn(async ({ producerId, producerEpoch }) =>
    String(producerId) === '-1'
      ? { producerId: '1000', producerEpoch: 0 }
      : { producerId: String(producerId), producerEpoch: producerEpoch + 1 }
  ),
})

/**
 * @param {{ transactional?: boolean, retry?: object, coordinator?: object, addPartitionsToTxn?: Function, logger?: object }} [options]
 */
const createManager = ({
  logger = newLogger(),
  transactional = false,
  retry = RETRY,
  coordinator = createCoordinator(),
  addPartitionsToTxn = jest.fn(async () => {}),
} = {}) =>
  new ProducerStateManager({
    logger,
    transactional,
    transactionalId: transactional ? 'test-transactional-id' : null,
    transactionTimeout: 30000,
    retry,
    findBroker: async () => coordinator,
    addPartitionsToTxn,
  })

/** Initializes the manager and, for a transactional one, begins a transaction. */
const ready = async (manager, { transactional = false } = {}) => {
  await manager.initialize()
  manager.transitionTo(READY)
  if (transactional) {
    manager.transitionTo(IN_TRANSACTION)
  }
  return manager
}

/** An initialized idempotent-only manager. */
const idempotentManager = (options) => ready(createManager(options))

/** An initialized transactional manager inside a transaction, partitions 0 and 1 already in it. */
const transactionalManager = async (options) => {
  const manager = await ready(createManager({ ...options, transactional: true }), {
    transactional: true,
  })
  await Promise.all([
    manager.addPartitionToTransaction(TOPIC, 0),
    manager.addPartitionToTransaction(TOPIC, 1),
  ])
  return manager
}

/** Queues the batches and drains the one request broker 1 may send now. */
const send = (manager, ...batches) => {
  manager.enqueue(batches)
  return manager.drain(NODE, LEADERS)
}

/** A protocol error as the broker's answer for one partition. */
const failure = (code) => createErrorFromCode(code)

/** Lets every pending promise callback run. */
const settle = () => new Promise((resolve) => setImmediate(resolve))

/**
 * Builds every manager of the enclosing block with room for several batches of one partition in
 * flight: states that cannot occur at the production limit of one batch per partition, kept for a
 * future per-partition pipelining.
 */
const withSeveralBatchesInFlight = () => {
  beforeEach(() => {
    mockMaxInFlightPerPartition = 5
  })
  afterEach(() => {
    mockMaxInFlightPerPartition = undefined
  })
}

describe('Producer > state > ProducerStateManager', () => {
  describe('resets of the producer id and the sequences', () => {
    /** Two partitions with acknowledged sequences, so a reset is visible. */
    const withAcknowledgedPartitions = async (manager) => {
      const batches = [createBatch({ partition: 0 }), createBatch({ partition: 1 })]
      send(manager, ...batches)
      batches.forEach((batch) => manager.complete(batch, { baseOffset: '0' }))
      expect(manager.nextSequence(TOPIC, 0)).toEqual(5)
      expect(manager.nextSequence(TOPIC, 1)).toEqual(5)
    }

    test('initialize() resets every partition, and the next batch is numbered from 0', async () => {
      const manager = await idempotentManager()
      await withAcknowledgedPartitions(manager)

      await manager.initialize()

      expect(manager.sequencer.entry(TOPIC, 0)).toBeNull()
      expect(manager.sequencer.entry(TOPIC, 1)).toBeNull()
      const [batch] = send(manager, createBatch())
      expect(batch).toMatchObject({ firstSequence: 0, producerId: '1000', producerEpoch: 0 })
    })

    test('reinitialize() resets every partition under the bumped epoch', async () => {
      const manager = await transactionalManager()
      await withAcknowledgedPartitions(manager)

      await manager.reinitialize()

      expect(manager.producerEpoch).toEqual(1)
      expect(manager.sequencer.entry(TOPIC, 0)).toBeNull()
      expect(manager.sequencer.entry(TOPIC, 1)).toBeNull()
    })

    test('adopt() takes the pair and resets every partition', async () => {
      const manager = await transactionalManager()
      await withAcknowledgedPartitions(manager)

      manager.adopt({ producerId: '1000', producerEpoch: 5 })

      expect([manager.producerId, manager.producerEpoch]).toEqual(['1000', 5])
      expect(manager.sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('adopt() with a batch in flight throws and changes nothing', async () => {
      const manager = await transactionalManager()
      const [batch] = send(manager, createBatch())

      expect(() => manager.adopt({ producerId: '1000', producerEpoch: 5 })).toThrow(
        KafkaJSInvariantViolation
      )
      expect(manager.producerEpoch).toEqual(0)
      expect(manager.sequencer.entry(TOPIC, 0).inFlight).toEqual([batch])
    })

    test('reinitialize() with a batch in flight throws before any request', async () => {
      const coordinator = createCoordinator()
      const manager = await transactionalManager({ coordinator })
      send(manager, createBatch())
      coordinator.initProducerId.mockClear()

      await expect(manager.reinitialize()).rejects.toThrow(KafkaJSInvariantViolation)
      expect(coordinator.initProducerId).not.toHaveBeenCalled()
    })

    test('a re-initialization the coordinator did not bump makes the producer unusable', async () => {
      const coordinator = createCoordinator()
      const manager = await transactionalManager({ coordinator })
      coordinator.initProducerId.mockResolvedValueOnce({ producerId: '1000', producerEpoch: 0 })
      manager.transitionTo(COMMITTING)
      manager.transitionTo(READY)

      await expect(manager.reinitialize()).rejects.toThrow(KafkaJSApplicationRecoverableError)
      expect(manager.state()).toEqual(FATAL_ERROR)
    })

    test('a retriable re-initialization failure arms nothing, for the caller to retry', async () => {
      const coordinator = createCoordinator()
      const manager = await transactionalManager({ coordinator })
      coordinator.initProducerId.mockRejectedValueOnce(failure(CONCURRENT_TRANSACTIONS))
      manager.transitionTo(ABORTING)
      manager.transitionTo(READY)

      await expect(manager.reinitialize()).rejects.toMatchObject({ code: CONCURRENT_TRANSACTIONS })
      expect(manager.state()).toEqual(READY)
    })

    test('initialize() called while one is running shares its InitProducerId', async () => {
      const coordinator = createCoordinator()
      const manager = createManager({ coordinator })

      const first = manager.initialize()
      expect(manager.initialize()).toBe(first)
      await first

      expect(coordinator.initProducerId).toHaveBeenCalledTimes(1)
    })
  })

  describe('OUT_OF_ORDER_SEQUENCE_NUMBER that is the next sequence', () => {
    test('idempotent-only: bumps the epoch, restarts that partition at 0 and re-sends', async () => {
      const manager = await idempotentManager()
      const [first] = send(manager, createBatch())
      manager.complete(first, { baseOffset: '0' })
      const [next] = send(manager, createBatch())
      expect(next.firstSequence).toEqual(5)

      expect(manager.fail(next, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('BUMP_EPOCH')

      expect(manager.producerEpoch).toEqual(1)
      expect(next).toMatchObject({ firstSequence: 0, producerEpoch: null, sequenceReset: true })
      manager.sequencer.requeue(next, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([next])
      expect(next).toMatchObject({ firstSequence: 0, producerId: '1000', producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })

    test('idempotent-only: two in one response restart each partition once, so neither numbers from 0 twice under one epoch', async () => {
      const manager = await idempotentManager()
      const [first, second] = send(manager, createBatch(), createBatch({ partition: 1 }))

      expect(manager.fail(first, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('BUMP_EPOCH')
      expect(manager.fail(second, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('BUMP_EPOCH')
      manager.sequencer.requeue(first, 0)
      manager.sequencer.requeue(second, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([first, second])
      manager.complete(first, { baseOffset: '0' })
      manager.complete(second, { baseOffset: '0' })

      const [next] = send(manager, createBatch())

      expect(next).toMatchObject({ firstSequence: 5, producerId: '1000', producerEpoch: 2 })
    })

    test('transactional: fails the batch and arms ABORTABLE_ERROR, epoch unchanged', async () => {
      const manager = await transactionalManager()
      const [first] = send(manager, createBatch())
      manager.complete(first, { baseOffset: '0' })
      const [next] = send(manager, createBatch())

      expect(manager.fail(next, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH')

      expect(manager.state()).toEqual(ABORTABLE_ERROR)
      expect(manager.transaction.armedError()).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(manager.producerEpoch).toEqual(0)
      expect(manager.sequencer.entry(TOPIC, 0).inFlight).toEqual([])
    })
  })

  describe('INVALID_PRODUCER_ID_MAPPING', () => {
    test.each([
      ['idempotent-only', idempotentManager],
      ['transactional', transactionalManager],
    ])(
      '%s: a produce failure fails the batch and makes the producer unusable',
      async (_, build) => {
        const manager = await build()
        const [batch] = send(manager, createBatch())

        expect(manager.fail(batch, failure(INVALID_PRODUCER_ID_MAPPING))).toEqual('FAIL_BATCH')

        expect(manager.state()).toEqual(FATAL_ERROR)
        expect(manager.transaction.armedError()).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect(() => manager.transaction.guard('initProducerId')).toThrow(
          KafkaJSApplicationRecoverableError
        )
      }
    )

    test.each([
      ['idempotent-only', idempotentManager],
      ['transactional', transactionalManager],
    ])('%s: the code on a transaction API makes the producer unusable too', async (_, build) => {
      const manager = await build()
      const error = failure(INVALID_PRODUCER_ID_MAPPING)
      manager.classify(error, 'txn')

      manager.arm(error)

      expect(manager.state()).toEqual(FATAL_ERROR)
      expect(manager.transaction.armedError()).toBeInstanceOf(KafkaJSApplicationRecoverableError)
    })
  })

  describe('a fenced producer', () => {
    /** Brings a fresh transactional manager to `state`. */
    const inState = async (state) => {
      const manager = createManager({ transactional: true })
      if (state === UNINITIALIZED) {
        return manager
      }
      await ready(manager)
      const path = {
        [READY]: [],
        [IN_TRANSACTION]: [IN_TRANSACTION],
        [COMMITTING]: [IN_TRANSACTION, COMMITTING],
        [ABORTING]: [IN_TRANSACTION, ABORTING],
        [ABORTABLE_ERROR]: [IN_TRANSACTION],
      }[state]
      path.forEach((next) => manager.transitionTo(next))
      if (state === ABORTABLE_ERROR) {
        const abortable = failure(TRANSACTION_ABORTABLE)
        manager.classify(abortable, 'txn')
        manager.arm(abortable)
      }
      expect(manager.state()).toEqual(state)
      return manager
    }

    test.each(
      [PRODUCER_FENCED, INVALID_PRODUCER_EPOCH].flatMap((code) =>
        [UNINITIALIZED, READY, IN_TRANSACTION, COMMITTING, ABORTING, ABORTABLE_ERROR].map(
          (state) => [code, state]
        )
      )
    )('code %i moves the state machine from %s to FATAL_ERROR', async (code, state) => {
      const manager = await inState(state)
      const fenced = failure(code)
      manager.classify(fenced, 'txn')

      manager.arm(fenced)

      expect(manager.state()).toEqual(FATAL_ERROR)
      for (const method of [
        'initProducerId',
        'beginTransaction',
        'sendOffsets',
        'commit',
        'abort',
      ]) {
        expect(() => manager.transaction.guard(method)).toThrow(KafkaJSApplicationRecoverableError)
      }
    })
  })

  describe('UNKNOWN_PRODUCER_ID after the log start moved past the last acknowledged offset', () => {
    /**
     * A batch of 3 records acknowledged at base offset 10 - so the last acknowledged offset is 12 -
     * and the next batch in flight.
     */
    const acknowledgedThenInFlight = async (build) => {
      const manager = await build()
      const [first] = send(manager, createBatch({ records: 3 }))
      manager.complete(first, { baseOffset: '10', logStartOffset: '0' })
      expect(manager.sequencer.entry(TOPIC, 0).lastAckedOffset).toEqual(12)
      const [next] = send(manager, createBatch())
      return { manager, next }
    }

    test('transactional: restarts that partition at 0 and re-sends, staying in the transaction', async () => {
      const { manager, next } = await acknowledgedThenInFlight(transactionalManager)

      const verdict = manager.fail(next, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '13' })

      expect(verdict).toEqual('RETRY_IN_ORDER')
      expect(next).toMatchObject({ firstSequence: 0, sequenceReset: true })
      expect(manager.state()).toEqual(IN_TRANSACTION)
      expect(manager.transaction.armedError()).toBeNull()
    })

    test('transactional, at the boundary (log start = last acknowledged offset): not a truncation', async () => {
      const { manager, next } = await acknowledgedThenInFlight(transactionalManager)

      const verdict = manager.fail(next, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '12' })

      expect(verdict).toEqual('FAIL_BATCH')
      expect(manager.state()).toEqual(ABORTABLE_ERROR)
    })

    test('idempotent-only: bumps the epoch, and the re-send goes out at 0 under the new epoch', async () => {
      const { manager, next } = await acknowledgedThenInFlight(idempotentManager)

      const verdict = manager.fail(next, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '13' })

      expect(verdict).toEqual('BUMP_EPOCH')
      expect(manager.producerEpoch).toEqual(1)
      manager.sequencer.requeue(next, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([next])
      expect(next).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })

    test('a failed UNKNOWN_PRODUCER_ID forgets the partition: it numbers from 0 again', async () => {
      const { manager, next } = await acknowledgedThenInFlight(transactionalManager)

      manager.fail(next, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '12' })

      expect(manager.nextSequence(TOPIC, 0)).toEqual(0)
    })

    test('transactional, with no log start offset in the response: re-sends at its own sequence', async () => {
      const { manager, next } = await acknowledgedThenInFlight(transactionalManager)

      expect(manager.fail(next, failure(UNKNOWN_PRODUCER_ID))).toEqual('RETRY_IN_ORDER')

      expect(next).toMatchObject({ firstSequence: 3, sequenceReset: false })
      expect(manager.nextSequence(TOPIC, 0)).toEqual(8)
    })
  })

  describe('a permanent failure with attempts left', () => {
    withSeveralBatchesInFlight()

    test('transactional: gives the sequences back and renumbers what follows, arming nothing', async () => {
      const manager = await transactionalManager()
      const [a] = send(manager, createBatch())
      const [b] = send(manager, createBatch())
      const [c] = send(manager, createBatch())
      const queued = createBatch()
      manager.enqueue([queued])
      expect([a, b, c].map(({ firstSequence }) => firstSequence)).toEqual([0, 5, 10])

      expect(manager.fail(b, failure(INVALID_RECORD))).toEqual('FAIL_BATCH_AND_REBASE')

      expect(manager.nextSequence(TOPIC, 0)).toEqual(10)
      expect(c).toMatchObject({ firstSequence: 5, sequenceReset: true })
      expect(manager.sequencer.entry(TOPIC, 0).inFlight).toEqual([a, c])
      expect(manager.state()).toEqual(IN_TRANSACTION)
      expect(queued.settlers[0].reject).not.toHaveBeenCalled()
    })

    test('idempotent-only: fails the batch and bumps the epoch with the narrow reset', async () => {
      const manager = await idempotentManager()
      const [other] = send(manager, createBatch({ partition: 1 }))
      const [a] = send(manager, createBatch())

      expect(manager.fail(a, failure(INVALID_RECORD))).toEqual('FAIL_BATCH_AND_REBASE')

      expect(manager.producerEpoch).toEqual(1)
      expect(manager.sequencer.entry(TOPIC, 0)).toBeNull()
      expect(manager.sequencer.entry(TOPIC, 1)).toMatchObject({ stale: true, inFlight: [other] })
    })

    test('with its attempts spent it reassigns nothing', async () => {
      const manager = await transactionalManager()
      const [a] = send(manager, createBatch())
      const [b] = send(manager, createBatch())
      a.attempt = RETRIES

      expect(manager.fail(a, failure(INVALID_RECORD))).toEqual('FAIL_BATCH')

      expect(manager.nextSequence(TOPIC, 0)).toEqual(10)
      expect(b.firstSequence).toEqual(5)
    })
  })

  describe('DUPLICATE_SEQUENCE_NUMBER', () => {
    test('completes the batch as a success that moves no offset and touches no producer id', async () => {
      const manager = await transactionalManager()
      const [first] = send(manager, createBatch())
      manager.complete(first, { baseOffset: '10' })
      const [duplicate] = send(manager, createBatch())

      expect(manager.fail(duplicate, failure(DUPLICATE_SEQUENCE_NUMBER))).toEqual(
        'COMPLETE_AS_DUPLICATE'
      )
      manager.complete(duplicate, { duplicate: true })

      expect(manager.sequencer.entry(TOPIC, 0)).toMatchObject({
        nextSequence: 10,
        lastAckedSequence: 9,
        lastAckedOffset: 14,
        inFlight: [],
      })
      expect(manager.producerEpoch).toEqual(0)
      expect(manager.state()).toEqual(IN_TRANSACTION)
      expect(duplicate.settlers[0].reject).not.toHaveBeenCalled()

      // A later UNKNOWN_PRODUCER_ID compares the log start with offset 14, not with -1.
      const [after] = send(manager, createBatch())
      const verdict = manager.fail(after, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '12' })
      expect(verdict).toEqual('FAIL_BATCH')
    })
  })

  describe('an OUT_OF_ORDER_SEQUENCE_NUMBER retried until its attempts are spent', () => {
    withSeveralBatchesInFlight()

    /**
     * Batch a fails for good on a non-retriable error with its re-sends used up, so nothing is
     * given back or left unresolved, and b, still in flight at sequence 5, can never be the next
     * sequence: every answer for it is a 45.
     */
    const gapBehind = async (build) => {
      const manager = await build()
      const [a] = send(manager, createBatch())
      const [b] = send(manager, createBatch())
      a.attempt = RETRIES
      expect(manager.fail(a, failure(INVALID_RECORD))).toEqual('FAIL_BATCH')

      for (let attempt = 0; attempt < RETRIES; attempt += 1) {
        expect(manager.fail(b, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('RETRY_IN_ORDER')
        manager.sequencer.requeue(b, 0)
        expect(manager.drain(NODE, LEADERS)).toEqual([b])
      }
      return { manager, b }
    }

    test('idempotent-only: the batch fails and the epoch is bumped, so the next batch starts at 0', async () => {
      const { manager, b } = await gapBehind(idempotentManager)

      expect(manager.fail(b, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH_AND_BUMP')

      expect(manager.producerEpoch).toEqual(1)
      const [next] = send(manager, createBatch())
      expect(next).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })

    test('transactional: the batch fails, nothing is renumbered, and only abort() is left', async () => {
      const { manager, b } = await gapBehind(transactionalManager)

      expect(manager.fail(b, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH')

      expect(manager.nextSequence(TOPIC, 0)).toEqual(10)
      expect(manager.state()).toEqual(ABORTABLE_ERROR)
      expect(() => manager.transaction.guard('commit')).toThrow(KafkaJSTransactionAbortableError)
      expect(() => manager.transaction.guard('abort')).not.toThrow()
      manager.transitionTo(ABORTING)
      manager.transitionTo(READY)
      expect(manager.state()).toEqual(READY)
    })
  })

  describe('an UNKNOWN_PRODUCER_ID with its attempts spent', () => {
    test('idempotent-only: the batch fails and the epoch is bumped, so the next batch starts at 0', async () => {
      const manager = await idempotentManager()
      const [first] = send(manager, createBatch())
      manager.complete(first, { baseOffset: '0' })
      const [batch] = send(manager, createBatch())
      batch.attempt = RETRIES

      expect(manager.fail(batch, failure(UNKNOWN_PRODUCER_ID), { logStartOffset: '0' })).toEqual(
        'FAIL_BATCH_AND_BUMP'
      )

      expect(manager.producerEpoch).toEqual(1)
      const [next] = send(manager, createBatch())
      expect(next).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })
  })

  describe('a batch that expires after it was re-sent (deliveryTimeout)', () => {
    withSeveralBatchesInFlight()

    const expiry = () =>
      new KafkaJSDeliveryTimeout('The batch expired', { cause: failure(REQUEST_TIMED_OUT) })

    /** a at 0 in flight; b at 5 sent once, failed and waiting for its re-send. */
    const reSendWaiting = async (build) => {
      const manager = await build()
      const [a] = send(manager, createBatch())
      const [b] = send(manager, createBatch())
      expect(manager.fail(b, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
      manager.sequencer.requeue(b, 0)
      return { manager, a, b }
    }

    test('marks the sequence unresolved, and no new batch is numbered while anything is in flight', async () => {
      const { manager, b } = await reSendWaiting(idempotentManager)

      expect(manager.fail(b, expiry())).toEqual('FAIL_BATCH')

      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(10)
      const c = createBatch()
      expect(send(manager, c)).toEqual([])
      expect(c.firstSequence).toBeNull()
    })

    test('clears the mark and changes nothing when the batches after it were acknowledged', async () => {
      const manager = await idempotentManager()
      const [a] = send(manager, createBatch())
      manager.fail(a, failure(REQUEST_TIMED_OUT))
      manager.sequencer.requeue(a, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([a])
      const [b] = send(manager, createBatch())

      manager.fail(a, expiry())
      manager.complete(b, { baseOffset: '20' })
      const c = createBatch()

      expect(send(manager, c)).toEqual([c])
      expect(c).toMatchObject({ firstSequence: 10, producerEpoch: 0 })
      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toBeNull()
    })

    test('idempotent-only: a gap left behind bumps the epoch, and the next batch starts at 0', async () => {
      const { manager, a, b } = await reSendWaiting(idempotentManager)
      manager.fail(b, expiry())
      const c = createBatch()
      manager.enqueue([c])

      manager.complete(a, { baseOffset: '0' })

      expect(manager.drain(NODE, LEADERS)).toEqual([c])
      expect(c).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })

    test('transactional: the expiry arms ABORTABLE_ERROR, and the gap that follows keeps that error', async () => {
      const { manager, a, b } = await reSendWaiting(transactionalManager)
      const expired = expiry()

      expect(manager.fail(b, expired)).toEqual('FAIL_BATCH')
      expect(manager.state()).toEqual(ABORTABLE_ERROR)

      manager.complete(a, { baseOffset: '0' })
      expect(manager.drain(NODE, LEADERS)).toEqual([])
      expect(manager.transaction.armedError().cause).toBe(expired)
      expect(manager.sequencer.entry(TOPIC, 0)?.unresolvedNextSequence ?? null).toBeNull()
      expect(manager.producerEpoch).toEqual(0)
    })

    test("transactional: a gap with nothing armed arms ABORTABLE_ERROR with Java's message", async () => {
      const manager = await transactionalManager()
      const [a] = send(manager, createBatch())
      const [b] = send(manager, createBatch())
      b.attempt = RETRIES
      expect(manager.fail(b, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
      expect(manager.state()).toEqual(IN_TRANSACTION)
      // However it got there, a partition holds a sequence nobody acknowledged.
      manager.sequencer.markUnresolved(b)
      manager.complete(a, { baseOffset: '0' })

      expect(manager.drain(NODE, LEADERS)).toEqual([])

      expect(manager.state()).toEqual(ABORTABLE_ERROR)
      expect(manager.transaction.armedError().message).toMatch(
        /hasn't received acknowledgment .* It is safe to abort the transaction and continue\./
      )
    })

    test('without a delivery deadline, a re-sent batch whose retries are spent marks its sequence unresolved as well', async () => {
      const { manager, b } = await reSendWaiting(idempotentManager)
      b.attempt = RETRIES

      expect(manager.fail(b, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')

      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(10)
    })

    test('an expiry is judged by its own error, not by the UNKNOWN_PRODUCER_ID it gave up on', async () => {
      const manager = await transactionalManager()
      const [first] = send(manager, createBatch())
      manager.complete(first, { baseOffset: '10' })
      const [b] = send(manager, createBatch())
      manager.fail(b, failure(REQUEST_TIMED_OUT))
      manager.sequencer.requeue(b, 0)
      const expired = new KafkaJSDeliveryTimeout('The batch expired', {
        cause: failure(UNKNOWN_PRODUCER_ID),
      })

      expect(manager.fail(b, expired)).toEqual('FAIL_BATCH')

      expect(manager.sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedOffset: 14,
        unresolvedNextSequence: 10,
      })
    })
  })

  describe('one batch per partition in flight: a re-sent batch that fails for good', () => {
    const expiry = () =>
      new KafkaJSDeliveryTimeout('The batch expired', { cause: failure(REQUEST_TIMED_OUT) })

    /**
     * Sends a and queues b behind it, then fails a retriably and re-sends it until its retries
     * are spent; b stays unnumbered all along.
     */
    const reSentUntilSpent = async (build) => {
      const manager = await build()
      const [a] = send(manager, createBatch())
      const b = createBatch()
      manager.enqueue([b])
      for (let attempt = 0; attempt < RETRIES; attempt += 1) {
        expect(manager.fail(a, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
        manager.sequencer.requeue(a, 0)
        expect(manager.drain(NODE, LEADERS)).toEqual([a])
      }
      expect(b.firstSequence).toBeNull()
      return { manager, a, b }
    }

    test('idempotent-only: out of retries, its sequence is unresolved and the next batch goes out at 0 after the bump', async () => {
      const { manager, a, b } = await reSentUntilSpent(idempotentManager)

      expect(manager.fail(a, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(5)

      expect(manager.drain(NODE, LEADERS)).toEqual([b])
      expect(b).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
      expect(manager.state()).toEqual(READY)
    })

    test('transactional: out of retries, its sequence is unresolved and the next drain arms ABORTABLE_ERROR', async () => {
      const { manager, a, b } = await reSentUntilSpent(transactionalManager)

      expect(manager.fail(a, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(5)

      expect(manager.drain(NODE, LEADERS)).toEqual([])
      expect(manager.state()).toEqual(ABORTABLE_ERROR)
      expect(b.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
      expect(manager.producerEpoch).toEqual(0)
    })

    test('a batch that was never re-sent fails as before: nothing is unresolved and the next batch follows it', async () => {
      const manager = await idempotentManager({ retry: { ...RETRY, retries: 0 } })
      const [a] = send(manager, createBatch())
      const b = createBatch()
      manager.enqueue([b])

      expect(manager.fail(a, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')

      expect(manager.sequencer.entry(TOPIC, 0)?.unresolvedNextSequence ?? null).toBeNull()
      expect(manager.drain(NODE, LEADERS)).toEqual([b])
      expect(b).toMatchObject({ firstSequence: 5, producerEpoch: 0 })
    })

    test('idempotent-only: an expired re-send leaves the next batch waiting, then it goes out at 0 after the bump', async () => {
      const manager = await idempotentManager()
      const [a] = send(manager, createBatch())
      const b = createBatch()
      manager.enqueue([b])
      expect(manager.fail(a, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
      manager.sequencer.requeue(a, 0)
      expect(b.firstSequence).toBeNull()

      expect(manager.fail(a, expiry())).toEqual('FAIL_BATCH')
      expect(manager.sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(5)

      expect(manager.drain(NODE, LEADERS)).toEqual([b])
      expect(b).toMatchObject({ firstSequence: 0, producerEpoch: 1 })
    })
  })

  describe('queued batches when an error state is armed', () => {
    test('arming rejects every queued batch with the armed error; the batch in flight settles on its own answer', async () => {
      const manager = await transactionalManager()
      const [inFlight] = send(manager, createBatch())
      const [other] = send(manager, createBatch({ partition: 1 }))
      const queued = [createBatch(), createBatch()]
      manager.enqueue(queued)

      expect(manager.fail(other, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH')

      const armed = manager.transaction.armedError()
      expect(armed).toBeInstanceOf(KafkaJSTransactionAbortableError)
      queued.forEach(({ settlers }) => expect(settlers[0].reject).toHaveBeenCalledWith(armed))
      expect(manager.sequencer.entry(TOPIC, 0)).toMatchObject({ queued: [], inFlight: [inFlight] })

      // A retriable answer can no longer be re-sent: nothing is drained once armed.
      expect(manager.fail(inFlight, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
      expect(manager.sequencer.hasInFlight()).toBe(false)
      expect(inFlight.settlers[0].reject).not.toHaveBeenCalled()
    })

    test('a batch queued after the arming is rejected at once, and nothing is drained', async () => {
      const manager = await transactionalManager()
      const [batch] = send(manager, createBatch())
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      const late = createBatch({ partition: 1 })

      expect(send(manager, late)).toEqual([])

      expect(late.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
    })

    test('a re-send requeued after a later failure in the same response armed is rejected by the next drain', async () => {
      const manager = await transactionalManager()
      const [first, second] = send(manager, createBatch(), createBatch({ partition: 1 }))
      expect(manager.fail(first, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
      expect(manager.fail(second, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH')
      manager.sequencer.requeue(first, 0)

      expect(manager.drain(NODE, LEADERS)).toEqual([])

      expect(first.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
      expect(manager.sequencer.hasInFlight()).toBe(false)
    })

    test('a failure while the commit is under way still arms ABORTABLE_ERROR', async () => {
      const manager = await transactionalManager()
      const [batch] = send(manager, createBatch())
      manager.transitionTo(COMMITTING)

      expect(manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('FAIL_BATCH')

      expect(manager.state()).toEqual(ABORTABLE_ERROR)
    })

    test('after a fatal error a new batch is rejected at once and nothing is drained', async () => {
      const manager = await idempotentManager()
      const [batch] = send(manager, createBatch())
      expect(manager.fail(batch, failure(INVALID_PRODUCER_ID_MAPPING))).toEqual('FAIL_BATCH')
      expect(manager.state()).toEqual(FATAL_ERROR)
      const late = createBatch({ partition: 1 })

      expect(send(manager, late)).toEqual([])

      expect(late.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
    })

    test('a permanent failure that gives its sequences back arms nothing and rejects no queued batch', async () => {
      const manager = await transactionalManager()
      const [batch] = send(manager, createBatch())
      const queued = createBatch()
      manager.enqueue([queued])

      expect(manager.fail(batch, failure(INVALID_RECORD))).toEqual('FAIL_BATCH_AND_REBASE')

      expect(manager.state()).toEqual(IN_TRANSACTION)
      expect(queued.settlers[0].reject).not.toHaveBeenCalled()
      expect(manager.drain(NODE, LEADERS)).toEqual([queued])
    })

    test('a batch queued while armed is rejected by enqueue() itself, before any drain', async () => {
      const manager = await transactionalManager()
      const [batch] = send(manager, createBatch())
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      const late = createBatch({ partition: 1 })

      manager.enqueue([late])

      expect(late.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
      expect(manager.sequencer.entry(TOPIC, 1)).toBeNull()
    })

    test('a second fatal answer keeps the first armed error and logs nothing more', async () => {
      const logger = newLogger()
      const logged = jest.spyOn(logger, 'error')
      const manager = await idempotentManager({ logger })
      const [first, second] = send(manager, createBatch(), createBatch({ partition: 1 }))

      manager.fail(first, failure(INVALID_PRODUCER_ID_MAPPING))
      const armed = manager.transaction.armedError()
      manager.fail(second, failure(INVALID_PRODUCER_ID_MAPPING))

      expect(logged).toHaveBeenCalledTimes(1)
      expect(manager.transaction.armedError()).toBe(armed)
    })

    test.each([
      ['a permanent failure', INVALID_RECORD, 0, 'FAIL_BATCH_AND_REBASE'],
      ['an out-of-order sequence', OUT_OF_ORDER_SEQUENCE_NUMBER, 0, 'FAIL_BATCH'],
      [
        'an out-of-order sequence, attempts spent',
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        RETRIES,
        'FAIL_BATCH_AND_BUMP',
      ],
    ])(
      'after a fatal error %s on another partition does not bump the epoch',
      async (_, code, attempt, verdict) => {
        const manager = await idempotentManager()
        const [first, second] = send(manager, createBatch(), createBatch({ partition: 1 }))
        manager.fail(first, failure(INVALID_PRODUCER_ID_MAPPING))
        expect(manager.state()).toEqual(FATAL_ERROR)
        second.attempt = attempt

        expect(manager.fail(second, failure(code))).toEqual(verdict)

        expect(manager.producerEpoch).toEqual(0)
      }
    )
  })

  describe('the drain clock', () => {
    test('a re-send is drained once the clock reaches its backoff, and not before', async () => {
      const manager = await idempotentManager()
      const now = jest.spyOn(Date, 'now').mockReturnValue(1000)
      try {
        const [batch] = send(manager, createBatch())
        expect(manager.fail(batch, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
        manager.sequencer.requeue(batch, 2000)

        expect(manager.drain(NODE, LEADERS)).toEqual([])
        now.mockReturnValue(2000)
        expect(manager.drain(NODE, LEADERS)).toEqual([batch])
      } finally {
        now.mockRestore()
      }
    })
  })

  describe('classification', () => {
    test.each([
      ['an idempotent-only', false, 'applicationRecoverable'],
      ['a transactional', true, 'abortable'],
    ])(
      '%s manager stamps an UNKNOWN_PRODUCER_ID on Produce with its own category',
      (_, transactional, category) => {
        const manager = createManager({ transactional })
        const error = failure(UNKNOWN_PRODUCER_ID)

        expect(manager.classify(error, 'produce')).toMatchObject({ category })

        expect(error.category).toEqual(category)
      }
    )
  })

  describe('an idempotent-only producer recovers by itself', () => {
    test.each([
      ['an out-of-order sequence', OUT_OF_ORDER_SEQUENCE_NUMBER, {}, 0],
      ['an unknown producer id', UNKNOWN_PRODUCER_ID, { logStartOffset: '0' }, 0],
      ['a retriable error, attempts spent', REQUEST_TIMED_OUT, {}, RETRIES],
      ['an out-of-order sequence, attempts spent', OUT_OF_ORDER_SEQUENCE_NUMBER, {}, RETRIES],
      ['an invalid record', INVALID_RECORD, {}, 0],
      ['an invalid record, attempts spent', INVALID_RECORD, {}, RETRIES],
    ])('%s never arms an error state', async (_, code, response, attempt) => {
      const manager = await idempotentManager()
      const [batch] = send(manager, createBatch())
      batch.attempt = attempt

      manager.fail(batch, failure(code), response)

      expect(manager.state()).toEqual(READY)
      expect(manager.transaction.armedError()).toBeNull()
    })

    test('an expired batch does not arm anything either', async () => {
      const manager = await idempotentManager()
      const [batch] = send(manager, createBatch())

      expect(manager.fail(batch, new KafkaJSDeliveryTimeout('expired'))).toEqual('FAIL_BATCH')
      expect(manager.state()).toEqual(READY)
    })
  })

  describe('the retry budget', () => {
    test("a producer that configures no retries gives every batch the retrier's default budget", async () => {
      const manager = await idempotentManager({ retry: {} })
      const [batch] = send(manager, createBatch())
      const { retries } = createRetry.defaults

      batch.attempt = retries - 1
      expect(manager.fail(batch, failure(REQUEST_TIMED_OUT))).toEqual('RETRY_IN_ORDER')
      batch.attempt = retries
      expect(manager.fail(batch, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
    })

    test('an error with no Kafka code fails by its own category: a retriable one is re-sent', async () => {
      const manager = await idempotentManager()
      const [batch] = send(manager, createBatch())
      const noResult = new KafkaJSError('The Produce response has no result for this partition')

      expect(manager.fail(batch, noResult)).toEqual('RETRY_IN_ORDER')
    })
  })

  describe('partitions joining a transaction', () => {
    test('under transaction version 1 a new partition drains only once its AddPartitionsToTxn lands', async () => {
      let land
      const addPartitionsToTxn = jest.fn(() => new Promise((resolve) => (land = resolve)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      const batch = createBatch()

      const added = manager.addPartitionToTransaction(TOPIC, 0)
      expect(send(manager, batch)).toEqual([])
      expect(manager.transactionStarted()).toBe(false)

      await settle()
      expect(addPartitionsToTxn).toHaveBeenCalledWith([{ topic: TOPIC, partitions: [0] }])
      expect(manager.drain(NODE, LEADERS)).toEqual([])
      land()
      await added

      expect(manager.transactionStarted()).toBe(true)
      expect(manager.drain(NODE, LEADERS)).toEqual([batch])
    })

    test('under transaction version 2 the partition joins at once, with no request, and the transaction starts', async () => {
      const addPartitionsToTxn = jest.fn()
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }))
      manager.version.begin(TV2_COORDINATOR)
      manager.transitionTo(IN_TRANSACTION)
      const batch = createBatch()

      expect(manager.addPartitionToTransaction(TOPIC, 0)).toBeNull()

      expect(manager.transactionStarted()).toBe(true)
      expect(send(manager, batch)).toEqual([batch])
      await settle()
      expect(addPartitionsToTxn).not.toHaveBeenCalled()
    })

    test('the partitions registered in one turn go in one request, each only once', async () => {
      const addPartitionsToTxn = jest.fn(async () => {})
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })

      const first = manager.addPartitionToTransaction(TOPIC, 0)
      const second = manager.addPartitionToTransaction(TOPIC, '1')
      expect(manager.addPartitionToTransaction(TOPIC, 0)).toBe(first)
      await Promise.all([first, second])

      expect(addPartitionsToTxn).toHaveBeenCalledTimes(1)
      expect(addPartitionsToTxn).toHaveBeenCalledWith([{ topic: TOPIC, partitions: [0, 1] }])
      expect(manager.addPartitionToTransaction(TOPIC, 1)).toBeNull()
    })

    test('a partition registered in a later turn goes in a request of its own', async () => {
      const answers = []
      const addPartitionsToTxn = jest.fn(() => new Promise((resolve) => answers.push(resolve)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })

      const first = manager.addPartitionToTransaction(TOPIC, 0)
      await settle()
      const second = manager.addPartitionToTransaction(TOPIC, 1)
      await settle()
      answers.forEach((answer) => answer())
      await Promise.all([first, second])

      expect(addPartitionsToTxn.mock.calls).toEqual([
        [[{ topic: TOPIC, partitions: [0] }]],
        [[{ topic: TOPIC, partitions: [1] }]],
      ])
    })

    test('a retriable answer is retried', async () => {
      const addPartitionsToTxn = jest
        .fn()
        .mockRejectedValueOnce(failure(CONCURRENT_TRANSACTIONS))
        .mockResolvedValueOnce()
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })

      await manager.addPartitionToTransaction(TOPIC, 0)

      expect(addPartitionsToTxn).toHaveBeenCalledTimes(2)
      expect(manager.state()).toEqual(IN_TRANSACTION)
    })

    test.each([
      [TRANSACTION_ABORTABLE, ABORTABLE_ERROR],
      [PRODUCER_FENCED, FATAL_ERROR],
    ])('a failure with code %i arms %s and rejects the queued batches', async (code, state) => {
      const error = failure(code)
      const addPartitionsToTxn = jest.fn().mockRejectedValue(error)
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      const batch = createBatch()

      const added = manager.addPartitionToTransaction(TOPIC, 0)
      manager.enqueue([batch])

      await expect(added).rejects.toBe(error)
      expect(manager.state()).toEqual(state)
      expect(batch.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
    })

    test.each([TRANSACTION_ABORTABLE, PRODUCER_FENCED])(
      'an answer with code %i that lands after the transaction ended touches nothing',
      async (code) => {
        let fail
        const addPartitionsToTxn = jest.fn(() => new Promise((_, reject) => (fail = reject)))
        const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
          transactional: true,
        })
        const added = manager.addPartitionToTransaction(TOPIC, 0)
        const settled = manager.whenPartitionsAdded()
        await settle()

        manager.transitionTo(ABORTING)
        manager.transitionTo(READY)
        fail(failure(code))

        await expect(added).rejects.toMatchObject({ code })
        await expect(settled).resolves.toBeUndefined()
        expect(manager.state()).toEqual(READY)
        expect(manager.transactionStarted()).toBe(false)
      }
    )

    test('a fatal answer that lands while the transaction is aborting still arms FATAL_ERROR', async () => {
      let fail
      const addPartitionsToTxn = jest.fn(() => new Promise((_, reject) => (fail = reject)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      const added = manager.addPartitionToTransaction(TOPIC, 0)
      await settle()

      manager.transitionTo(ABORTING)
      fail(failure(PRODUCER_FENCED))

      await expect(added).rejects.toMatchObject({ code: PRODUCER_FENCED })
      expect(manager.state()).toEqual(FATAL_ERROR)
    })

    test('a success that lands after the transaction ended joins nothing to the next one', async () => {
      let land
      const addPartitionsToTxn = jest.fn(() => new Promise((resolve) => (land = resolve)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      const added = manager.addPartitionToTransaction(TOPIC, 0)
      await settle()

      manager.transitionTo(ABORTING)
      manager.transitionTo(READY)
      land()
      await added
      manager.transitionTo(IN_TRANSACTION)

      expect(manager.transactionStarted()).toBe(false)
      expect(send(manager, createBatch())).toEqual([])
    })

    test('a partition registered again in the next transaction while the old request is pending goes in a request of its own', async () => {
      const answers = []
      const addPartitionsToTxn = jest.fn(() => new Promise((resolve) => answers.push(resolve)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      manager.addPartitionToTransaction(TOPIC, 0)
      await settle()
      manager.transitionTo(ABORTING)
      manager.transitionTo(READY)
      manager.transitionTo(IN_TRANSACTION)
      const batch = createBatch()

      const again = manager.addPartitionToTransaction(TOPIC, 0)
      manager.enqueue([batch])
      await settle()
      answers.forEach((answer) => answer())
      await again

      expect(addPartitionsToTxn).toHaveBeenCalledTimes(2)
      expect(manager.drain(NODE, LEADERS)).toEqual([batch])
    })

    test('a registration whose transaction ended before its request went out sends nothing', async () => {
      const addPartitionsToTxn = jest.fn(async () => {})
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })

      const added = manager.addPartitionToTransaction(TOPIC, 0)
      manager.transitionTo(ABORTING)
      manager.transitionTo(READY)
      await added

      expect(addPartitionsToTxn).not.toHaveBeenCalled()
      expect(manager.transactionStarted()).toBe(false)
    })

    test('entering READY forgets the partitions and offsets of the transaction that ended', async () => {
      const manager = await transactionalManager()
      manager.markOffsetsAdded()
      expect(manager.transactionStarted()).toBe(true)

      manager.transitionTo(COMMITTING)
      manager.transitionTo(READY)
      manager.transitionTo(IN_TRANSACTION)

      expect(manager.transactionStarted()).toBe(false)
      expect(manager.addPartitionToTransaction(TOPIC, 0)).not.toBeNull()
      await manager.whenPartitionsAdded()
    })

    test('offsets alone start the transaction', async () => {
      const manager = await ready(createManager({ transactional: true }), { transactional: true })
      expect(manager.transactionStarted()).toBe(false)

      manager.markOffsetsAdded()

      expect(manager.transactionStarted()).toBe(true)
    })

    test('whenPartitionsAdded() waits for an AddPartitionsToTxn still pending', async () => {
      let land
      const addPartitionsToTxn = jest.fn(() => new Promise((resolve) => (land = resolve)))
      const manager = await ready(createManager({ transactional: true, addPartitionsToTxn }), {
        transactional: true,
      })
      manager.addPartitionToTransaction(TOPIC, 0)
      const done = jest.fn()

      manager.whenPartitionsAdded().then(done)
      await settle()
      expect(done).not.toHaveBeenCalled()

      land()
      await settle()
      expect(done).toHaveBeenCalled()
    })
  })

  describe('construction', () => {
    test('with only a logger, a timeout and a broker lookup it is an idempotent-only producer', async () => {
      const coordinator = createCoordinator()
      const manager = new ProducerStateManager({
        logger: newLogger(),
        transactionTimeout: 30000,
        findBroker: async () => coordinator,
      })

      await manager.initialize()

      expect(coordinator.initProducerId).toHaveBeenCalledWith(
        expect.objectContaining({ transactionalId: null, producerId: -1, producerEpoch: -1 })
      )
      const [batch] = send(manager, createBatch())
      batch.attempt = createRetry.defaults.retries
      expect(manager.fail(batch, failure(REQUEST_TIMED_OUT))).toEqual('FAIL_BATCH')
    })

    test.each([
      ['a transactional', true, 'transactional-id'],
      ['an idempotent-only', false, null],
    ])('%s manager sends transactional id %p on InitProducerId', async (_, transactional, sent) => {
      const coordinator = createCoordinator()
      const manager = new ProducerStateManager({
        logger: newLogger(),
        transactional,
        transactionalId: 'transactional-id',
        transactionTimeout: 30000,
        findBroker: async () => coordinator,
      })

      await manager.initialize()

      expect(coordinator.initProducerId).toHaveBeenCalledWith(
        expect.objectContaining({ transactionalId: sent })
      )
    })

    test('nothing is drained before a producer id is held', () => {
      const manager = createManager()
      const batch = createBatch()

      expect(send(manager, batch)).toEqual([])

      expect(batch).toMatchObject({ firstSequence: null, producerId: null })
    })
  })

  describe('the reads the facade passes through', () => {
    test('ensureProducerId() with a producer id held resolves at once and sends nothing', async () => {
      const coordinator = createCoordinator()
      const manager = await idempotentManager({ coordinator })
      coordinator.initProducerId.mockClear()

      await expect(manager.ensureProducerId()).resolves.toBeUndefined()

      expect(coordinator.initProducerId).not.toHaveBeenCalled()
    })

    test('transactionVersion() is 1 until a begin() reads version 2', async () => {
      const manager = await ready(createManager({ transactional: true }))

      expect(manager.transactionVersion()).toEqual(1)
      manager.version.begin(TV2_COORDINATOR)
      expect(manager.transactionVersion()).toEqual(2)
    })

    test('nextSequence() and advance() keep the per-broker send path working on these numbers', async () => {
      const manager = await idempotentManager()

      expect(manager.nextSequence(TOPIC, 3)).toEqual(0)
      expect(manager.advance(TOPIC, 3, 7)).toEqual(7)
      expect(manager.advance(TOPIC, 3, -2)).toEqual(5)
      expect(manager.nextSequence(TOPIC, 3)).toEqual(5)
    })
  })

  describe('a producer id lost at the int16 epoch ceiling', () => {
    /**
     * An idempotent-only manager whose producer id sits at epoch 32767, a batch in flight on
     * partition 1 and a batch on partition 0 whose next-sequence 45 asks for a bump.
     */
    const atTheCeiling = async (coordinator, retry = RETRY) => {
      coordinator.initProducerId.mockResolvedValueOnce({ producerId: '1000', producerEpoch: 32767 })
      const manager = await idempotentManager({ coordinator, retry })
      const [other] = send(manager, createBatch({ partition: 1 }))
      const [batch] = send(manager, createBatch())
      return { manager, other, batch }
    }

    test('initialize() and ensureProducerId() share the acquisition the ceiling started', async () => {
      const coordinator = createCoordinator()
      const { manager, batch } = await atTheCeiling(coordinator)
      let grant
      coordinator.initProducerId.mockImplementationOnce(
        () => new Promise((resolve) => (grant = resolve))
      )

      expect(manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))).toEqual('BUMP_EPOCH')
      expect(manager.isInitialized()).toBe(false)
      const ensured = manager.ensureProducerId()
      expect(manager.ensureProducerId()).toBe(ensured)
      const initialized = manager.initialize()
      await settle()
      grant({ producerId: '2000', producerEpoch: 0 })
      await Promise.all([ensured, initialized])

      expect(coordinator.initProducerId).toHaveBeenCalledTimes(2)
      expect([manager.producerId, manager.producerEpoch]).toEqual(['2000', 0])
    })

    test('when that acquisition failed, ensureProducerId() acquires again, retrying, without resetting what is in flight', async () => {
      const coordinator = createCoordinator()
      const { manager, other, batch } = await atTheCeiling(coordinator)
      coordinator.initProducerId
        .mockRejectedValueOnce(failure(REQUEST_TIMED_OUT))
        .mockRejectedValueOnce(failure(REQUEST_TIMED_OUT))
        .mockResolvedValueOnce({ producerId: '2000', producerEpoch: 0 })
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      await settle()
      expect(manager.isInitialized()).toBe(false)

      await manager.ensureProducerId()

      expect(coordinator.initProducerId).toHaveBeenCalledTimes(4)
      expect(manager.producerId).toEqual('2000')
      expect(manager.sequencer.entry(TOPIC, 1)).toMatchObject({ stale: true, inFlight: [other] })
      manager.sequencer.requeue(batch, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([batch])
      expect(batch).toMatchObject({ firstSequence: 0, producerId: '2000', producerEpoch: 0 })
    })

    test('a fatal failure there arms FATAL_ERROR, rejecting what is queued, and never rejects itself', async () => {
      const coordinator = createCoordinator()
      const { manager, batch } = await atTheCeiling(coordinator)
      coordinator.initProducerId.mockRejectedValue(failure(CLUSTER_AUTHORIZATION_FAILED))
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      manager.sequencer.requeue(batch, 0)
      await settle()

      await expect(manager.ensureProducerId()).resolves.toBeUndefined()

      expect(manager.state()).toEqual(FATAL_ERROR)
      expect(manager.transaction.armedError().cause).toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(batch.settlers[0].reject).toHaveBeenCalledWith(manager.transaction.armedError())
    })

    test('a retriable failure that outlasts the budget rejects what is queued and leaves the producer usable', async () => {
      const coordinator = createCoordinator()
      const { manager, batch } = await atTheCeiling(coordinator)
      coordinator.initProducerId.mockRejectedValue(failure(REQUEST_TIMED_OUT))
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      manager.sequencer.requeue(batch, 0)
      await settle()

      await manager.ensureProducerId()

      // The first producer id, the ceiling's own request, then one attempt and RETRIES retries.
      expect(coordinator.initProducerId).toHaveBeenCalledTimes(2 + RETRIES + 1)
      expect(manager.state()).toEqual(READY)
      expect(batch.settlers[0].reject).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'KafkaJSNumberOfRetriesExceeded' })
      )
      coordinator.initProducerId.mockResolvedValueOnce({ producerId: '2000', producerEpoch: 0 })
      await manager.ensureProducerId()
      expect(manager.isInitialized()).toBe(true)
    })

    test('a producer id another caller acquired during the backoff is kept, not replaced', async () => {
      const coordinator = createCoordinator()
      const slowRetry = { retries: RETRIES, initialRetryTime: 100, maxRetryTime: 100, factor: 0 }
      const { manager, batch } = await atTheCeiling(coordinator, slowRetry)
      coordinator.initProducerId
        .mockRejectedValueOnce(failure(REQUEST_TIMED_OUT))
        .mockRejectedValueOnce(failure(REQUEST_TIMED_OUT))
        .mockResolvedValueOnce({ producerId: '2000', producerEpoch: 0 })
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      await settle()

      const ensured = manager.ensureProducerId()
      await settle()
      await manager.initialize()
      await ensured

      expect(coordinator.initProducerId).toHaveBeenCalledTimes(4)
      expect(manager.producerId).toEqual('2000')
    })

    test('the partition that asked for the bump numbers on from its re-send under the new producer id', async () => {
      const coordinator = createCoordinator()
      const { manager, batch } = await atTheCeiling(coordinator)
      coordinator.initProducerId
        .mockRejectedValueOnce(failure(REQUEST_TIMED_OUT))
        .mockResolvedValueOnce({ producerId: '2000', producerEpoch: 0 })
      manager.fail(batch, failure(OUT_OF_ORDER_SEQUENCE_NUMBER))
      await settle()
      await manager.ensureProducerId()
      manager.sequencer.requeue(batch, 0)
      expect(manager.drain(NODE, LEADERS)).toEqual([batch])
      manager.complete(batch, { baseOffset: '0' })

      const [next] = send(manager, createBatch())

      expect(next).toMatchObject({ firstSequence: 5, producerId: '2000', producerEpoch: 0 })
    })
  })
})
