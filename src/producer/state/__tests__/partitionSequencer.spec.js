const PartitionSequencer = require('../partitionSequencer')
const { KafkaJSInvariantViolation, KafkaJSTransactionAbortableError } = require('../../../errors')
const { INT_32_MAX_VALUE } = require('../../../constants')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const OTHER = 'topic-b'
const NODE = 1
/** Broker 1 leads every partition the cases use. */
const LEADERS = { [TOPIC]: { 1: [0, 1, 2] }, [OTHER]: { 1: [0] } }
const PAIR = { producerId: '1000', producerEpoch: 0 }
const BUMPED = { producerId: '1000', producerEpoch: 1 }
const OTHER_PRODUCER = { producerId: '2000', producerEpoch: 0 }

const REQUEST_TIMED_OUT = 7
const INVALID_RECORD = 87
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const DUPLICATE_SEQUENCE_NUMBER = 46
const INVALID_PRODUCER_ID_MAPPING = 49
const UNKNOWN_PRODUCER_ID = 59

let ids = 0

/**
 * A batch as the send path builds it: never numbered, never sent.
 * @param {{ topic?: string, partition?: number, records?: number, acks?: number,
 *   timeout?: number, compression?: number }} [options]
 */
const createBatch = ({
  topic = TOPIC,
  partition = 0,
  records = 5,
  acks = -1,
  timeout = 30000,
  compression = 0,
} = {}) => ({
  id: ids++,
  topic,
  partition,
  messages: Array.from({ length: records }, (_, index) => ({ value: `record-${index}` })),
  acks,
  timeout,
  compression,
  firstSequence: null,
  producerId: null,
  producerEpoch: null,
  attempt: 0,
  settlers: [],
  backoffUntil: 0,
  createdAt: 0,
  sequenceReset: false,
})

const createGate = (overrides = {}) => ({
  now: 0,
  producerIdValid: true,
  ...PAIR,
  isPartitionInTransaction: () => true,
  ...overrides,
})

/**
 * A sequencer with no partition state.
 * @param {{ maxInFlightPerPartition?: number }} [options] `PIPELINED` raises the production
 *   limit of one batch in flight per partition
 */
const build = (options = {}) => new PartitionSequencer({ logger: newLogger(), ...options })

/**
 * The limit of the cases with several batches of one partition in flight: states that cannot occur
 * at the production limit of one batch per partition, kept for a future per-partition pipelining.
 */
const PIPELINED = { maxInFlightPerPartition: 5 }

/** One drain for broker 1. */
const drain = (sequencer, gate) => sequencer.drain(NODE, LEADERS, createGate(gate))

/** Enqueues the batches and drains once, returning what the drain took. */
const send = (sequencer, ...batches) => {
  sequencer.enqueue(batches)
  return drain(sequencer)
}

/** The answer to a failed Produce: a request time-out with the whole budget left, by default. */
const failure = (overrides = {}) => ({
  category: 'retriable',
  code: REQUEST_TIMED_OUT,
  logStartOffset: '-1',
  transactional: false,
  attemptsLeft: 5,
  expired: false,
  ...overrides,
})

const sequencesOf = (batches) => batches.map(({ firstSequence }) => firstSequence)

/**
 * The broker's sequence check for one producer on one partition: a batch is appended only when
 * its first sequence follows the last appended one, or is 0 right after INT_32_MAX_VALUE.
 * @see https://github.com/apache/kafka/blob/4.3/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java
 * @param {number} [lastSequence] the last sequence the log already holds; -1 for an empty log
 */
const createBrokerLog = (lastSequence = -1) => ({
  append(batch) {
    const accepted =
      batch.firstSequence === lastSequence + 1 ||
      (batch.firstSequence === 0 && lastSequence === INT_32_MAX_VALUE)
    if (accepted) {
      lastSequence = (batch.firstSequence + batch.messages.length - 1) % (INT_32_MAX_VALUE + 1)
    }
    return accepted
  },
})

/** Two batches in flight; the first was re-sent once and then expired. */
const expiredInRetry = () => {
  const sequencer = build(PIPELINED)
  const batches = [createBatch(), createBatch()]
  sequencer.enqueue(batches)
  drain(sequencer)
  drain(sequencer)
  const [expired, next] = batches
  sequencer.requeue(expired, 0)
  drain(sequencer)
  sequencer.markUnresolved(expired)
  sequencer.fail(expired)
  return { sequencer, expired, next }
}

describe('PartitionSequencer', () => {
  describe('the per-partition limit', () => {
    test('by default holds a never-sent batch while its partition has a batch in flight', () => {
      const sequencer = build()
      const [first, second] = [createBatch(), createBatch()]
      sequencer.enqueue([first, second])

      expect(drain(sequencer)).toEqual([first])
      expect(drain(sequencer)).toEqual([])
      expect(second.firstSequence).toBeNull()

      sequencer.complete(first, { baseOffset: '0' })
      expect(drain(sequencer)).toEqual([second])
      expect(second.firstSequence).toEqual(5)
    })

    test('does not hold a re-send', () => {
      const sequencer = build()
      const [first, second] = [createBatch(), createBatch()]
      sequencer.enqueue([first, second])
      drain(sequencer)

      sequencer.requeue(first, 0)

      expect(drain(sequencer)).toEqual([first])
      expect(first.attempt).toEqual(1)
      expect(second.firstSequence).toBeNull()
    })

    test('a higher limit lets that many batches of a partition be in flight', () => {
      const sequencer = build({ maxInFlightPerPartition: 3 })
      const batches = [createBatch(), createBatch(), createBatch(), createBatch()]
      sequencer.enqueue(batches)

      expect([drain(sequencer), drain(sequencer), drain(sequencer)]).toEqual([
        [batches[0]],
        [batches[1]],
        [batches[2]],
      ])
      expect(drain(sequencer)).toEqual([])

      sequencer.complete(batches[0], { baseOffset: '0' })
      expect(drain(sequencer)).toEqual([batches[3]])
      expect(sequencesOf(batches)).toEqual([0, 5, 10, 15])
    })
  })

  describe('drain()', () => {
    test('takes one batch per partition per call and numbers it there, up to the limit', () => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch(), createBatch()]
      sequencer.enqueue(batches)

      expect(batches.every(({ firstSequence }) => firstSequence === null)).toBe(true)
      expect(drain(sequencer)).toEqual([batches[0]])
      expect(drain(sequencer)).toEqual([batches[1]])
      expect(drain(sequencer)).toEqual([batches[2]])
      expect(drain(sequencer)).toEqual([])
      expect(sequencesOf(batches)).toEqual([0, 5, 10])
      expect(sequencer.entry(TOPIC, 0).inFlight).toEqual(batches)
      expect(sequencer.entry(TOPIC, 0).nextSequence).toEqual(15)
    })

    test('numbers each partition contiguously across interleaved calls, independently of the others', () => {
      const sequencer = build(PIPELINED)
      const a = [
        createBatch({ records: 2 }),
        createBatch({ records: 3 }),
        createBatch({ records: 4 }),
      ]
      const b = [
        createBatch({ partition: 1, records: 7 }),
        createBatch({ partition: 1, records: 1 }),
      ]
      const c = [createBatch({ topic: OTHER, records: 9 })]

      sequencer.enqueue([a[0], b[0]])
      drain(sequencer)
      sequencer.enqueue([a[1], c[0]])
      sequencer.enqueue([b[1], a[2]])
      drain(sequencer)
      drain(sequencer)
      drain(sequencer)

      expect(sequencesOf(a)).toEqual([0, 2, 5])
      expect(sequencesOf(b)).toEqual([0, 7])
      expect(sequencesOf(c)).toEqual([0])
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(9)
      expect(sequencer.nextSequence(TOPIC, 1)).toEqual(8)
      expect(sequencer.nextSequence(OTHER, 0)).toEqual(9)
    })

    test('takes every drainable partition the broker leads, and only those', () => {
      const sequencer = build()
      const batches = [
        createBatch({ partition: 0 }),
        createBatch({ partition: 1 }),
        createBatch({ topic: OTHER }),
      ]
      sequencer.enqueue(batches)

      expect(sequencer.drain(2, LEADERS, createGate())).toEqual([])
      expect(drain(sequencer)).toEqual(batches)
    })

    test('stamps a batch with the producer id and epoch it is numbered under', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(batch).toMatchObject({ firstSequence: 0, producerId: '1000', producerEpoch: 0 })
    })

    test.each([
      ['acks', { acks: 1 }],
      ['timeout', { timeout: 1000 }],
      ['compression', { compression: 1 }],
    ])('a batch with a different %s waits for the next request', (_, options) => {
      const sequencer = build()
      const first = createBatch({ partition: 0 })
      const other = createBatch({ partition: 1, ...options })
      sequencer.enqueue([first, other])

      expect(drain(sequencer)).toEqual([first])
      expect(other.firstSequence).toBeNull()
      expect(drain(sequencer)).toEqual([other])
    })

    test('drains nothing while no producer id is held', () => {
      const sequencer = build()
      const batch = createBatch()
      sequencer.enqueue([batch])

      expect(drain(sequencer, { producerIdValid: false })).toEqual([])
      expect(batch.firstSequence).toBeNull()
      expect(drain(sequencer)).toEqual([batch])
    })

    test('skips a partition not yet in the transaction and drains the next one in the same pass', () => {
      const sequencer = build()
      const waiting = createBatch({ partition: 0 })
      const added = createBatch({ partition: 1 })
      sequencer.enqueue([waiting, added])
      const isPartitionInTransaction = (topic, partition) => partition !== 0

      expect(drain(sequencer, { isPartitionInTransaction })).toEqual([added])
      expect(waiting.firstSequence).toBeNull()
      expect(drain(sequencer)).toEqual([waiting])
    })

    test('skips a partition whose head batch waits out a backoff, until the deadline', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      const sibling = createBatch({ partition: 1 })
      sequencer.requeue(batch, 100)
      sequencer.enqueue([sibling])

      expect(drain(sequencer, { now: 99 })).toEqual([sibling])
      expect(drain(sequencer, { now: 100 })).toEqual([batch])
    })

    test('a stale partition that still has a batch in flight is skipped, and the pass goes on', () => {
      const sequencer = build()
      send(sequencer, createBatch({ partition: 0 }))
      sequencer.markStaleExcept(TOPIC, 1)
      const held = createBatch({ partition: 0 })
      const sibling = createBatch({ partition: 1 })
      sequencer.enqueue([held, sibling])

      expect(drain(sequencer)).toEqual([sibling])
      expect(held.firstSequence).toBeNull()
    })

    test('a partition with an unresolved sequence is skipped, and the pass goes on', () => {
      const { sequencer } = expiredInRetry()
      const held = createBatch({ partition: 0 })
      const sibling = createBatch({ partition: 1 })
      sequencer.enqueue([held, sibling])

      expect(drain(sequencer)).toEqual([sibling])
      expect(held.firstSequence).toBeNull()
    })

    test('a batch that does not fit the request is skipped, and the pass goes on', () => {
      const sequencer = build()
      const fits = createBatch({ partition: 0 })
      const odd = createBatch({ partition: 1, acks: 1 })
      const alsoFits = createBatch({ partition: 2 })
      sequencer.enqueue([fits, odd, alsoFits])

      expect(drain(sequencer)).toEqual([fits, alsoFits])
      expect(drain(sequencer)).toEqual([odd])
    })

    test('a batch for another producer id waits for its own request, at the same epoch', () => {
      const sequencer = build()
      const [resend] = send(sequencer, createBatch())
      sequencer.requeue(resend, 0)
      const fresh = createBatch({ partition: 1 })
      sequencer.enqueue([fresh])

      expect(drain(sequencer, OTHER_PRODUCER)).toEqual([resend])
      expect(resend).toMatchObject(PAIR)
      expect(drain(sequencer, OTHER_PRODUCER)).toEqual([fresh])
      expect(fresh).toMatchObject(OTHER_PRODUCER)
    })

    test('accepts a partition number given as a string', () => {
      const sequencer = build()
      const batch = createBatch({ partition: '1' })
      sequencer.enqueue([batch])

      expect(drain(sequencer)).toEqual([batch])
      sequencer.complete(batch, { baseOffset: '0' })
      expect(sequencer.entry(TOPIC, 1)).toMatchObject({ lastAckedSequence: 4, inFlight: [] })
    })
  })

  describe('retrying in order', () => {
    test('a retriable failure re-sends the same batch with its sequence', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(sequencer.verdict(batch, failure())).toEqual('RETRY_IN_ORDER')
      sequencer.requeue(batch, 0)

      const [again] = drain(sequencer)
      expect(again).toBe(batch)
      expect(again).toMatchObject({ firstSequence: 0, attempt: 1, producerEpoch: 0 })
    })

    test('a batch queued behind a failing one is not sent before the failing one is re-sent', () => {
      const sequencer = build()
      const [failing] = send(sequencer, createBatch())
      const behind = createBatch()
      sequencer.enqueue([behind])
      sequencer.requeue(failing, 100)

      expect(drain(sequencer, { now: 50 })).toEqual([])
      expect(behind.firstSequence).toBeNull()
      expect(drain(sequencer, { now: 100 })).toEqual([failing])
      sequencer.complete(failing, { baseOffset: '0' })
      expect(drain(sequencer, { now: 100 })).toEqual([behind])
      expect(behind.firstSequence).toEqual(5)
    })

    test('a batch queued behind a re-send that waits for a lower sequence stays queued too', () => {
      const sequencer = build(PIPELINED)
      const inFlight = [createBatch(), createBatch()]
      sequencer.enqueue(inFlight)
      drain(sequencer)
      drain(sequencer)
      const [lower, failing] = inFlight
      const behind = createBatch()
      sequencer.enqueue([behind])
      sequencer.requeue(failing, 0)

      expect(drain(sequencer)).toEqual([])
      expect(behind.firstSequence).toBeNull()
      sequencer.complete(lower, { baseOffset: '0' })
      expect(drain(sequencer)).toEqual([failing])
      expect(drain(sequencer)).toEqual([behind])
      expect(sequencesOf([failing, behind])).toEqual([5, 10])
    })

    test('holds a re-send until it is the lowest sequence in flight, while other partitions flow', () => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      drain(sequencer)
      const [first, second, third] = batches
      sequencer.requeue(third, 0)

      const sibling = createBatch({ partition: 1 })
      expect(send(sequencer, sibling)).toEqual([sibling])
      expect(drain(sequencer)).toEqual([])
      sequencer.complete(first, { baseOffset: '0' })
      const nextSibling = createBatch({ partition: 1 })
      expect(send(sequencer, nextSibling)).toEqual([nextSibling])
      sequencer.complete(second, { baseOffset: '5' })
      expect(drain(sequencer)).toEqual([third])
      expect(third.firstSequence).toEqual(10)
    })

    test('re-sends with the same producer stamp share a request', () => {
      const sequencer = build()
      const resends = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      for (const batch of resends) {
        sequencer.requeue(batch, 0)
      }

      expect(drain(sequencer, BUMPED)).toEqual(resends)
      expect(resends.every(({ producerEpoch }) => producerEpoch === 0)).toBe(true)
    })

    test.each([
      ['in sequence order', [0, 1]],
      ['in reverse order', [1, 0]],
    ])('re-sends failed %s go back by sequence, ahead of every batch never sent', (_, order) => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      const fresh = batches[2]
      for (const index of order) {
        sequencer.requeue(batches[index], 0)
      }

      expect(sequencer.entry(TOPIC, 0).queued).toEqual([batches[0], batches[1], fresh])
      expect(drain(sequencer)).toEqual([batches[0]])
      expect(drain(sequencer)).toEqual([])
      sequencer.complete(batches[0], { baseOffset: '0' })
      expect(drain(sequencer)).toEqual([batches[1]])
      sequencer.complete(batches[1], { baseOffset: '5' })
      expect(drain(sequencer)).toEqual([fresh])
      expect(sequencesOf(batches)).toEqual([0, 5, 10])
    })
  })

  describe('verdict()', () => {
    test('DUPLICATE_SEQUENCE_NUMBER completes the batch, whatever the budget', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      const duplicate = { category: null, code: DUPLICATE_SEQUENCE_NUMBER }

      expect(sequencer.verdict(batch, failure(duplicate))).toEqual('COMPLETE_AS_DUPLICATE')
      expect(sequencer.verdict(batch, failure({ ...duplicate, attemptsLeft: 0 }))).toEqual(
        'COMPLETE_AS_DUPLICATE'
      )
    })

    test.each([
      ['retriable', 'retriable'],
      ['refreshRetriable', 'refreshRetriable'],
    ])('a %s error is re-sent while attempts remain, then fails', (_, category) => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(sequencer.verdict(batch, failure({ category }))).toEqual('RETRY_IN_ORDER')
      expect(sequencer.verdict(batch, failure({ category, attemptsLeft: 0 }))).toEqual('FAIL_BATCH')
    })

    test.each([
      ['an invalid record', 'invalidConfiguration', INVALID_RECORD],
      ['an unknown code', 'applicationRecoverable', 9999],
    ])(
      '%s gives its sequences back while attempts remain, and nothing once they are spent',
      (_, category, code) => {
        const sequencer = build()
        const [batch] = send(sequencer, createBatch())

        for (const transactional of [false, true]) {
          expect(sequencer.verdict(batch, failure({ category, code, transactional }))).toEqual(
            'FAIL_BATCH_AND_REBASE'
          )
          expect(
            sequencer.verdict(batch, failure({ category, code, transactional, attemptsLeft: 0 }))
          ).toEqual('FAIL_BATCH')
        }
      }
    )

    test('INVALID_PRODUCER_ID_MAPPING fails the batch for both producer kinds', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      const mapping = { category: 'applicationRecoverable', code: INVALID_PRODUCER_ID_MAPPING }

      expect(sequencer.verdict(batch, failure(mapping))).toEqual('FAIL_BATCH')
      expect(sequencer.verdict(batch, failure({ ...mapping, transactional: true }))).toEqual(
        'FAIL_BATCH'
      )
    })

    test('an expired batch fails, whatever attempts remain and whatever the error', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      const expired = { category: 'abortable', code: undefined, expired: true }

      expect(sequencer.verdict(batch, failure(expired))).toEqual('FAIL_BATCH')
      expect(sequencer.verdict(batch, failure({ ...expired, transactional: true }))).toEqual(
        'FAIL_BATCH'
      )
    })
  })

  describe('OUT_OF_ORDER_SEQUENCE_NUMBER', () => {
    const outOfOrder = (overrides) =>
      failure({ category: 'abortable', code: OUT_OF_ORDER_SEQUENCE_NUMBER, ...overrides })

    test('is re-sent in order, with no epoch bump, while an earlier batch is in flight', () => {
      const sequencer = build()
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      for (const transactional of [false, true]) {
        const verdict = sequencer.verdict(batches[1], outOfOrder({ transactional }))
        expect(verdict).toEqual('RETRY_IN_ORDER')
      }
    })

    test('is re-sent in order, with no epoch bump, after an earlier batch already failed', () => {
      const sequencer = build()
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      sequencer.fail(batches[0])

      for (const transactional of [false, true]) {
        const verdict = sequencer.verdict(batches[1], outOfOrder({ transactional }))
        expect(verdict).toEqual('RETRY_IN_ORDER')
      }
    })

    test('on the next sequence it bumps the epoch (idempotent) or fails the batch (transactional)', () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '0' })
      const [next] = send(sequencer, createBatch())

      expect(sequencer.verdict(next, outOfOrder())).toEqual('BUMP_EPOCH')
      expect(sequencer.verdict(next, outOfOrder({ transactional: true }))).toEqual('FAIL_BATCH')
    })

    test('on a partition with no state, sequence 0 is the next one', () => {
      const sequencer = build()
      const batch = { ...createBatch({ partition: 2 }), firstSequence: 0 }

      expect(sequencer.verdict(batch, outOfOrder())).toEqual('BUMP_EPOCH')
      expect(sequencer.verdict({ ...batch, firstSequence: 5 }, outOfOrder())).toEqual(
        'RETRY_IN_ORDER'
      )
    })

    test('a batch renumbered since it was sent is not the next sequence', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      sequencer.restartPartition(TOPIC, 0)

      expect(batch).toMatchObject({ firstSequence: 0, sequenceReset: true })
      expect(sequencer.verdict(batch, outOfOrder())).toEqual('RETRY_IN_ORDER')
    })

    test('with the budget spent it fails the batch, bumping the epoch of an idempotent producer', () => {
      const sequencer = build()
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      expect(sequencer.verdict(batches[1], outOfOrder({ attemptsLeft: 0 }))).toEqual(
        'FAIL_BATCH_AND_BUMP'
      )
      expect(
        sequencer.verdict(batches[1], outOfOrder({ attemptsLeft: 0, transactional: true }))
      ).toEqual('FAIL_BATCH')
    })
  })

  describe('UNKNOWN_PRODUCER_ID', () => {
    const unknownProducer = (overrides) =>
      failure({ category: 'abortable', code: UNKNOWN_PRODUCER_ID, ...overrides })

    /** A partition whose last acknowledged batch is records 100-104. */
    const acknowledgedUpTo104 = () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '100', logStartOffset: '0' })
      const [batch] = send(sequencer, createBatch())
      return { sequencer, batch }
    }

    test('stores the last acknowledged offset as baseOffset + record count - 1', () => {
      const { sequencer } = acknowledgedUpTo104()

      expect(sequencer.entry(TOPIC, 0).lastAckedOffset).toEqual(104)
    })

    test('an unknown log start offset is re-sent with no other action, for both producer kinds', () => {
      const { sequencer, batch } = acknowledgedUpTo104()

      for (const transactional of [false, true]) {
        for (const logStartOffset of ['-1', undefined]) {
          const verdict = sequencer.verdict(
            batch,
            unknownProducer({ logStartOffset, transactional })
          )
          expect(verdict).toEqual('RETRY_IN_ORDER')
        }
        expect(sequencer.isTruncated(batch, '-1')).toBe(false)
      }
    })

    test('a batch renumbered since it was sent is re-sent with no other action', () => {
      const { sequencer, batch } = acknowledgedUpTo104()
      sequencer.restartPartition(TOPIC, 0)

      for (const transactional of [false, true]) {
        const failed = unknownProducer({ logStartOffset: '500', transactional })
        expect(sequencer.verdict(batch, failed)).toEqual('RETRY_IN_ORDER')
      }
      expect(sequencer.isTruncated(batch, '500')).toBe(false)
    })

    test('a log truncated past the last acknowledged offset is re-sent (transactional) or bumps (idempotent)', () => {
      const { sequencer, batch } = acknowledgedUpTo104()

      expect(sequencer.isTruncated(batch, '105')).toBe(true)
      expect(
        sequencer.verdict(batch, unknownProducer({ logStartOffset: '105', transactional: true }))
      ).toEqual('RETRY_IN_ORDER')
      expect(sequencer.verdict(batch, unknownProducer({ logStartOffset: '105' }))).toEqual(
        'BUMP_EPOCH'
      )
    })

    test('a log start at the last acknowledged offset is not a truncation', () => {
      const { sequencer, batch } = acknowledgedUpTo104()

      expect(sequencer.isTruncated(batch, '104')).toBe(false)
      expect(
        sequencer.verdict(batch, unknownProducer({ logStartOffset: '104', transactional: true }))
      ).toEqual('FAIL_BATCH')
      expect(sequencer.verdict(batch, unknownProducer({ logStartOffset: '104' }))).toEqual(
        'BUMP_EPOCH'
      )
    })

    test('a log start of 0 is a known log start, not a missing one', () => {
      const { sequencer, batch } = acknowledgedUpTo104()

      expect(sequencer.isTruncated(batch, '0')).toBe(false)
      expect(
        sequencer.verdict(batch, unknownProducer({ logStartOffset: '0', transactional: true }))
      ).toEqual('FAIL_BATCH')
      expect(sequencer.verdict(batch, unknownProducer({ logStartOffset: '0' }))).toEqual(
        'BUMP_EPOCH'
      )
    })

    test('a partition never acknowledged reads as truncated by any known log start', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(sequencer.isTruncated(batch, '0')).toBe(true)
      expect(sequencer.isTruncated(createBatch({ partition: 2 }), '0')).toBe(true)
    })

    test('with the budget spent it fails the batch, bumping the epoch of an idempotent producer', () => {
      const { sequencer, batch } = acknowledgedUpTo104()

      expect(sequencer.verdict(batch, unknownProducer({ attemptsLeft: 0 }))).toEqual(
        'FAIL_BATCH_AND_BUMP'
      )
      expect(
        sequencer.verdict(batch, unknownProducer({ attemptsLeft: 0, transactional: true }))
      ).toEqual('FAIL_BATCH')
    })
  })

  describe('complete()', () => {
    test('a duplicate advances the acknowledged sequence and leaves the acknowledged offset alone', () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '100', logStartOffset: '0' })
      const [duplicate] = send(sequencer, createBatch())

      sequencer.complete(duplicate, { duplicate: true })

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedSequence: 9,
        lastAckedOffset: 104,
        nextSequence: 10,
        inFlight: [],
      })
    })

    test('a duplicate as the first acknowledgement leaves the partition with no acknowledged offset', () => {
      const sequencer = build()
      const [duplicate] = send(sequencer, createBatch())

      sequencer.complete(duplicate, { duplicate: true })

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ lastAckedSequence: 4, lastAckedOffset: -1 })
    })

    test('a first batch at offset 0 stores its last offset, the record count minus one', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      sequencer.complete(batch, { baseOffset: '0' })

      expect(sequencer.entry(TOPIC, 0).lastAckedOffset).toBe(4)
    })

    test('an earlier batch completing late with its own offset leaves the higher offset', () => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      sequencer.complete(batches[1], { baseOffset: '105' })
      sequencer.complete(batches[0], { baseOffset: '100' })

      expect(sequencer.entry(TOPIC, 0).lastAckedOffset).toBe(109)
    })

    test('an earlier batch completing late moves nothing back', () => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      sequencer.complete(batches[1], { baseOffset: '105' })
      sequencer.complete(batches[0], { duplicate: true })

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedSequence: 9,
        lastAckedOffset: 109,
      })
    })

    test('clears the renumbered flag, as a re-drain does', () => {
      const sequencer = build(PIPELINED)
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      sequencer.restartPartition(TOPIC, 0)
      sequencer.requeue(batches[0], 0)

      expect(drain(sequencer)).toEqual([batches[0]])
      expect(batches[0].sequenceReset).toBe(false)
      expect(batches[1].sequenceReset).toBe(true)
      sequencer.complete(batches[1], { baseOffset: '5' })
      expect(batches[1].sequenceReset).toBe(false)
    })
  })

  describe('fail()', () => {
    test('removes a batch still waiting in the queue, so it is never sent', () => {
      const sequencer = build()
      const batch = createBatch()
      sequencer.enqueue([batch])

      sequencer.fail(batch)

      expect(drain(sequencer)).toEqual([])
      expect(sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('removes a batch waiting for its re-send from the queue and the in-flight list', () => {
      const sequencer = build()
      const [batch, other] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      sequencer.requeue(batch, 0)

      sequencer.fail(batch)

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ inFlight: [], queued: [], nextSequence: 5 })
      expect(drain(sequencer)).toEqual([])
      expect(sequencer.entry(TOPIC, 1).inFlight).toEqual([other])
    })

    test('clears the renumbered flag of the batch it settles', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      sequencer.restartPartition(TOPIC, 0)
      expect(batch.sequenceReset).toBe(true)

      sequencer.fail(batch)

      expect(batch.sequenceReset).toBe(false)
    })
  })

  describe('giving sequences back', () => {
    test('giving back the only batch in flight returns the partition to 0 and drops it', () => {
      const sequencer = build()
      const [only] = send(sequencer, createBatch())

      sequencer.rebase(only)

      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(0)
      expect(sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('with 0, 5 and 10 in flight, failing 5 moves 10 down to 5; the queued batch follows at 10', () => {
      const sequencer = build(PIPELINED)
      const inFlight = [createBatch(), createBatch(), createBatch()]
      sequencer.enqueue(inFlight)
      drain(sequencer)
      drain(sequencer)
      drain(sequencer)
      const queued = createBatch()
      sequencer.enqueue([queued])
      const [first, failed, third] = inFlight

      sequencer.rebase(failed)

      expect(sequencesOf([first, third])).toEqual([0, 5])
      expect(first.sequenceReset).toBe(false)
      expect(third.sequenceReset).toBe(true)
      expect(queued.firstSequence).toBeNull()
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        nextSequence: 10,
        inFlight: [first, third],
      })

      expect(drain(sequencer)).toEqual([queued])
      expect(queued.firstSequence).toEqual(10)
      const broker = createBrokerLog()
      expect([first, third, queued].map((batch) => broker.append(batch))).toEqual([
        true,
        true,
        true,
      ])
    })

    test('gives sequences back across the int32 wrap', () => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 4)
      const [failed] = send(sequencer, createBatch({ records: 5 }))
      const [later] = send(sequencer, createBatch({ records: 3 }))

      expect(sequencesOf([failed, later])).toEqual([INT_32_MAX_VALUE - 4, 0])
      sequencer.rebase(failed)

      expect(later.firstSequence).toEqual(INT_32_MAX_VALUE - 4)
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(INT_32_MAX_VALUE - 1)
    })

    test('a failed batch that is never given back leaves a gap the broker refuses', () => {
      const sequencer = build(PIPELINED)
      const broker = createBrokerLog()
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      sequencer.fail(batches[0])

      expect(sequencesOf(batches)).toEqual([0, 5])
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(10)
      expect(broker.append(batches[1])).toBe(false)
    })
  })

  describe('restarting at 0', () => {
    test('renumbers the batches in flight from 0 and keeps counting from their total', () => {
      const sequencer = build(PIPELINED)
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '0' })
      const pending = [createBatch({ records: 5 }), createBatch({ records: 3 })]
      const queued = createBatch()
      sequencer.enqueue(pending)
      drain(sequencer)
      drain(sequencer)
      sequencer.enqueue([queued])

      sequencer.restartPartition(TOPIC, 0)

      expect(sequencesOf(pending)).toEqual([0, 5])
      expect(pending.every(({ sequenceReset }) => sequenceReset)).toBe(true)
      expect(pending.every(({ producerEpoch }) => producerEpoch === null)).toBe(true)
      expect(queued.firstSequence).toBeNull()
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ nextSequence: 8, lastAckedSequence: -1 })
    })

    test('a restarted batch is stamped again with the pair in force when it is re-sent', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      sequencer.restartPartition(TOPIC, 0)
      sequencer.requeue(batch, 0)

      const [again] = drain(sequencer, BUMPED)

      expect(again).toMatchObject({ firstSequence: 0, ...BUMPED, sequenceReset: false })
    })
  })

  describe('stale partitions', () => {
    test('a partition with a batch in flight keeps its sequence across a bump and restarts once it settles', () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch({ partition: 1 }))
      sequencer.complete(acked, { baseOffset: '0' })
      const [trigger, other] = send(sequencer, createBatch(), createBatch({ partition: 1 }))

      sequencer.restartPartition(TOPIC, 0)
      sequencer.markStaleExcept(TOPIC, 0)

      expect(trigger.firstSequence).toEqual(0)
      expect(other).toMatchObject({ firstSequence: 5, ...PAIR })
      expect(sequencer.entry(TOPIC, 1)).toMatchObject({ stale: true, nextSequence: 10 })
      expect(sequencer.entry(TOPIC, 0).stale).toBe(false)

      const fresh = createBatch({ partition: 1 })
      sequencer.enqueue([fresh])
      expect(drain(sequencer, BUMPED)).toEqual([])

      sequencer.complete(other, { baseOffset: '5' })
      expect(drain(sequencer, BUMPED)).toEqual([fresh])
      expect(fresh).toMatchObject({ firstSequence: 0, ...BUMPED })
      expect(sequencer.entry(TOPIC, 1)).toMatchObject({ stale: false, nextSequence: 5 })
    })

    test('a re-send on a stale partition keeps its sequence and old epoch, in a request of its own', () => {
      const sequencer = build()
      const [, old] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      sequencer.restartPartition(TOPIC, 0)
      sequencer.markStaleExcept(TOPIC, 0)
      sequencer.requeue(old, 0)
      const fresh = createBatch({ partition: 2 })
      sequencer.enqueue([fresh])

      const firstRequest = drain(sequencer, BUMPED)
      const secondRequest = drain(sequencer, BUMPED)

      expect(firstRequest).toEqual([old])
      expect(old).toMatchObject({ firstSequence: 0, ...PAIR })
      expect(secondRequest).toEqual([fresh])
      expect(fresh).toMatchObject({ firstSequence: 0, ...BUMPED })
    })

    test('a stale partition with one batch in flight drains nothing new until it settles, then restarts at 0', () => {
      const sequencer = build(PIPELINED)
      const [inFlight] = send(sequencer, createBatch({ partition: 1 }))
      sequencer.complete(send(sequencer, createBatch({ partition: 1 }))[0], { baseOffset: '0' })
      sequencer.markStaleExcept(TOPIC, 0)
      const fresh = createBatch({ partition: 1 })
      sequencer.enqueue([fresh])

      expect(drain(sequencer, BUMPED)).toEqual([])
      expect(drain(sequencer, BUMPED)).toEqual([])
      sequencer.complete(inFlight, { baseOffset: '5' })

      expect(drain(sequencer, BUMPED)).toEqual([fresh])
      expect(fresh.firstSequence).toEqual(0)
      expect(sequencer.entry(TOPIC, 1).lastAckedSequence).toEqual(-1)
    })

    test('markAllStale() marks every partition stale, holding new batches behind those in flight', () => {
      const sequencer = build()
      const [first, second] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      const fresh = createBatch()
      sequencer.enqueue([fresh])

      sequencer.markAllStale()

      expect(sequencer.entry(TOPIC, 0).stale).toBe(true)
      expect(sequencer.entry(TOPIC, 1).stale).toBe(true)
      expect(drain(sequencer, BUMPED)).toEqual([])
      sequencer.complete(first, { baseOffset: '0' })
      sequencer.complete(second, { baseOffset: '0' })
      expect(drain(sequencer, BUMPED)).toEqual([fresh])
      expect(fresh).toMatchObject({ firstSequence: 0, ...BUMPED })
    })

    test('two bumps in one response leave each restarted partition numbering on after its re-send', () => {
      const sequencer = build()
      const [first, second] = send(sequencer, createBatch(), createBatch({ partition: 1 }))

      sequencer.restartPartition(TOPIC, 0)
      sequencer.markStaleExcept(TOPIC, 0)
      sequencer.requeue(first, 0)
      sequencer.restartPartition(TOPIC, 1)
      sequencer.markStaleExcept(TOPIC, 1)
      sequencer.requeue(second, 0)

      expect(drain(sequencer, BUMPED)).toEqual([first, second])
      sequencer.complete(first, { baseOffset: '0' })
      sequencer.complete(second, { baseOffset: '0' })
      const next = createBatch()
      sequencer.enqueue([next])

      expect(drain(sequencer, BUMPED)).toEqual([next])
      expect(next).toMatchObject({ firstSequence: 5, ...BUMPED })
    })

    const markers = [
      ['markStaleExcept()', (sequencer) => sequencer.markStaleExcept(OTHER, 0)],
      ['markAllStale()', (sequencer) => sequencer.markAllStale()],
    ]

    test.each(markers)(
      '%s leaves alone a partition whose batches in flight were all restarted since they were sent',
      (_, mark) => {
        const sequencer = build()
        const [batch] = send(sequencer, createBatch())
        sequencer.restartPartition(TOPIC, 0)

        mark(sequencer)

        expect(sequencer.entry(TOPIC, 0).stale).toBe(false)
        sequencer.requeue(batch, 0)
        expect(drain(sequencer, OTHER_PRODUCER)).toEqual([batch])
        sequencer.complete(batch, { baseOffset: '0' })
        const next = createBatch()
        sequencer.enqueue([next])
        expect(drain(sequencer, OTHER_PRODUCER)).toEqual([next])
        expect(next).toMatchObject({ firstSequence: 5, ...OTHER_PRODUCER })
      }
    )

    test.each(markers)('%s still marks a partition with nothing in flight', (_, mark) => {
      const sequencer = build()
      sequencer.complete(send(sequencer, createBatch())[0], { baseOffset: '0' })

      mark(sequencer)

      expect(sequencer.entry(TOPIC, 0).stale).toBe(true)
      const next = createBatch()
      sequencer.enqueue([next])
      expect(drain(sequencer, BUMPED)).toEqual([next])
      expect(next).toMatchObject({ firstSequence: 0, ...BUMPED })
    })

    test.each(markers)('%s still marks a partition with a stamped batch in flight', (_, mark) => {
      const sequencer = build()
      send(sequencer, createBatch())

      mark(sequencer)

      expect(sequencer.entry(TOPIC, 0).stale).toBe(true)
    })

    test.each(markers)(
      '%s still marks a partition whose batches in flight are only partly restarted',
      (_, mark) => {
        const sequencer = build(PIPELINED)
        const [first] = send(sequencer, createBatch())
        const [second] = send(sequencer, createBatch())
        sequencer.restartPartition(TOPIC, 0)
        sequencer.requeue(first, 0)
        sequencer.requeue(second, 0)
        expect(drain(sequencer, BUMPED)).toEqual([first])

        mark(sequencer)

        expect(sequencer.entry(TOPIC, 0).stale).toBe(true)
      }
    )
  })

  describe('the unresolved sequence', () => {
    test('marks the sequence of a first written attempt that expires', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      sequencer.markUnresolved(batch)
      sequencer.fail(batch)

      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toBe(5)
      expect(sequencer.resolveUnresolved()).toEqual([
        { topic: TOPIC, partition: 0, outcome: 'gap' },
      ])
    })

    test('is set past an expired re-send, and stops new batches but not re-sends', () => {
      const { sequencer, next } = expiredInRetry()
      const fresh = createBatch()
      sequencer.enqueue([fresh])

      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(5)
      expect(drain(sequencer)).toEqual([])
      expect(fresh.firstSequence).toBeNull()

      sequencer.requeue(next, 0)
      expect(drain(sequencer)).toEqual([next])
    })

    test('only grows', () => {
      const { sequencer, next } = expiredInRetry()
      sequencer.requeue(next, 0)
      drain(sequencer)
      sequencer.markUnresolved(next)

      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(10)
    })

    test.each([
      ['in numbering order', [0, 1]],
      ['in reverse order', [1, 0]],
    ])('only grows across the int32 wrap, marked %s', (_, order) => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 9)
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)

      for (const index of order) {
        sequencer.requeue(batches[index], 0)
        sequencer.markUnresolved(batches[index])
      }

      expect(sequencesOf(batches)).toEqual([INT_32_MAX_VALUE - 9, INT_32_MAX_VALUE - 4])
      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toEqual(0)
    })

    test('makes the batch at the unresolved sequence the next one for an OUT_OF_ORDER_SEQUENCE_NUMBER', () => {
      const { sequencer, next } = expiredInRetry()
      const outOfOrder = failure({ category: 'abortable', code: OUT_OF_ORDER_SEQUENCE_NUMBER })

      expect(next.firstSequence).toEqual(5)
      expect(sequencer.verdict(next, outOfOrder)).toEqual('BUMP_EPOCH')
      expect(sequencer.verdict(next, { ...outOfOrder, transactional: true })).toEqual('FAIL_BATCH')
    })

    test('a batch elsewhere than the unresolved sequence is re-sent without a bump', () => {
      const { sequencer } = expiredInRetry()
      const outOfOrder = failure({ category: 'abortable', code: OUT_OF_ORDER_SEQUENCE_NUMBER })
      const elsewhere = { ...createBatch(), firstSequence: 0 }

      expect(sequencer.verdict(elsewhere, outOfOrder)).toEqual('RETRY_IN_ORDER')
      expect(sequencer.verdict(elsewhere, { ...outOfOrder, transactional: true })).toEqual(
        'FAIL_BATCH'
      )
    })

    test('waits while anything is in flight, then clears when the acknowledged sequences reach the next one', () => {
      const { sequencer, next } = expiredInRetry()

      expect(sequencer.resolveUnresolved()).toEqual([])
      sequencer.complete(next, { baseOffset: '0' })

      expect(sequencer.resolveUnresolved()).toEqual([
        { topic: TOPIC, partition: 0, outcome: 'cleared' },
      ])
      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toBeNull()
      expect(sequencer.resolveUnresolved()).toEqual([])
    })

    test('reports a gap when the in-flight batches drained without reaching it', () => {
      const { sequencer, next } = expiredInRetry()
      sequencer.fail(next)

      expect(sequencer.resolveUnresolved()).toEqual([
        { topic: TOPIC, partition: 0, outcome: 'gap' },
      ])
      expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toBeNull()
    })

    test('is cleared by a restart and by a partition reset', () => {
      for (const reset of ['restartPartition', 'resetPartition']) {
        const { sequencer, next } = expiredInRetry()
        sequencer.complete(next, { baseOffset: '0' })

        sequencer[reset](TOPIC, 0)

        expect(sequencer.resolveUnresolved()).toEqual([])
      }
    })

    test('is forgotten by a partition reset that waits for batches in flight', () => {
      const { sequencer } = expiredInRetry()

      sequencer.resetPartition(TOPIC, 0)

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ unresolvedNextSequence: null, stale: true })
    })

    test('is forgotten by resetAll() together with its partition', () => {
      const { sequencer, next } = expiredInRetry()
      sequencer.fail(next)

      sequencer.resetAll()

      expect(sequencer.entry(TOPIC, 0)).toBeNull()
      expect(sequencer.resolveUnresolved()).toEqual([])
    })

    test('clears across the int32 wrap once the acknowledged sequences reach the next one', () => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 9)
      const batches = [createBatch(), createBatch()]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      const [expired, last] = batches
      sequencer.requeue(expired, 0)
      drain(sequencer)
      sequencer.markUnresolved(expired)
      sequencer.fail(expired)

      sequencer.complete(last, { baseOffset: '5' })

      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(0)
      expect(sequencer.resolveUnresolved()).toEqual([
        { topic: TOPIC, partition: 0, outcome: 'cleared' },
      ])
    })

    test('a resolution check with nothing marked allocates nothing', () => {
      const sequencer = build()

      expect(sequencer.resolveUnresolved()).toBe(sequencer.resolveUnresolved())
      expect(Object.isFrozen(sequencer.resolveUnresolved())).toBe(true)
    })
  })

  describe('abortQueued()', () => {
    test('rejects every waiting batch with the one error and leaves the batches on the wire alone', async () => {
      const sequencer = build()
      const [onTheWire, retrying] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      sequencer.requeue(retrying, 0)
      const neverSent = createBatch()
      const outcomes = []
      for (const batch of [onTheWire, retrying, neverSent]) {
        batch.settlers.push({
          resolve: () => outcomes.push([batch.id, 'resolved']),
          reject: (error) => outcomes.push([batch.id, error]),
        })
      }
      sequencer.enqueue([neverSent])
      const error = new KafkaJSTransactionAbortableError('aborted')

      const aborted = sequencer.abortQueued(error)

      expect(aborted).toEqual([neverSent, retrying])
      expect(outcomes).toEqual([
        [neverSent.id, error],
        [retrying.id, error],
      ])
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ inFlight: [onTheWire], queued: [] })
      expect(sequencer.entry(TOPIC, 1)).toMatchObject({ inFlight: [], queued: [] })
      expect(drain(sequencer)).toEqual([])
    })

    test('rejects nothing when nothing waits', () => {
      const sequencer = build()
      send(sequencer, createBatch())

      expect(sequencer.abortQueued(new Error('unused'))).toEqual([])
      expect(sequencer.hasInFlight()).toBe(true)
    })

    test('leaves nothing behind for a partition that held only batches never sent', () => {
      const sequencer = build()
      sequencer.enqueue([createBatch()])

      sequencer.abortQueued(new Error('aborted'))

      expect(sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('clears the renumbered flag of a re-send it takes out', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())
      sequencer.restartPartition(TOPIC, 0)
      sequencer.requeue(batch, 0)
      expect(batch.sequenceReset).toBe(true)

      sequencer.abortQueued(new Error('aborted'))

      expect(batch.sequenceReset).toBe(false)
    })
  })

  describe('entry()', () => {
    test('hands out copies, so changing them changes nothing in the sequencer', () => {
      const sequencer = build()
      const [sent] = send(sequencer, createBatch())
      const waiting = createBatch()
      sequencer.enqueue([waiting])

      const copy = sequencer.entry(TOPIC, 0)
      copy.inFlight.length = 0
      copy.queued.length = 0

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ inFlight: [sent], queued: [waiting] })
    })
  })

  describe('resets and memory', () => {
    test('resetAll() forgets every partition and keeps the batches never sent, numbered afresh', () => {
      const sequencer = build()
      const [acked, other] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      sequencer.complete(acked, { baseOffset: '0' })
      sequencer.complete(other, { baseOffset: '0' })
      const waiting = createBatch()
      const alsoWaiting = createBatch({ partition: 2 })
      sequencer.enqueue([waiting, alsoWaiting])

      sequencer.resetAll()

      expect(sequencer.entry(TOPIC, 1)).toBeNull()
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        nextSequence: 0,
        lastAckedSequence: -1,
        lastAckedOffset: -1,
        queued: [waiting],
      })
      expect(drain(sequencer, BUMPED)).toEqual([waiting, alsoWaiting])
      expect(waiting).toMatchObject({ firstSequence: 0, ...BUMPED })
      expect(alsoWaiting).toMatchObject({ firstSequence: 0, ...BUMPED })
    })

    test('resetAll() refuses to run with a batch in flight and changes nothing', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(() => sequencer.resetAll()).toThrow(KafkaJSInvariantViolation)
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ nextSequence: 5, inFlight: [batch] })
    })

    test('a partition idle after a reset is dropped; one still counting is kept', () => {
      const sequencer = build()
      const [batch, sibling] = send(sequencer, createBatch(), createBatch({ partition: 1 }))
      sequencer.complete(batch, { baseOffset: '0' })

      expect(sequencer.entry(TOPIC, 0)).not.toBeNull()
      sequencer.restartPartition(TOPIC, 0)
      expect(sequencer.entry(TOPIC, 0)).toBeNull()
      expect(sequencer.entry(TOPIC, 1).inFlight).toEqual([sibling])

      const [other] = send(sequencer, createBatch({ topic: OTHER }))
      sequencer.complete(other, { baseOffset: '0' })
      sequencer.complete(sibling, { baseOffset: '0' })
      sequencer.resetAll()
      expect(sequencer.entry(OTHER, 0)).toBeNull()
      expect(sequencer.entry(TOPIC, 1)).toBeNull()
    })

    test('resetPartition() forgets the acknowledged state, and waits for the batches in flight', () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '100' })
      const [pending] = send(sequencer, createBatch())
      const fresh = createBatch()
      sequencer.enqueue([fresh])

      sequencer.resetPartition(TOPIC, 0)

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ lastAckedOffset: -1, stale: true })
      expect(pending.firstSequence).toEqual(5)
      expect(drain(sequencer)).toEqual([])
      sequencer.fail(pending)
      expect(drain(sequencer)).toEqual([fresh])
      expect(fresh.firstSequence).toEqual(0)
    })

    test('resetPartition() with nothing in flight numbers from 0 at once', () => {
      const sequencer = build()
      const [acked] = send(sequencer, createBatch())
      sequencer.complete(acked, { baseOffset: '100' })

      sequencer.resetPartition(TOPIC, 0)
      sequencer.restartPartition(TOPIC, 2)
      sequencer.resetPartition(TOPIC, 2)

      expect(sequencer.entry(TOPIC, 0)).toBeNull()
      expect(sequencer.isTruncated({ ...acked, sequenceReset: false }, '0')).toBe(true)
    })

    test.each(['restartPartition', 'resetPartition'])(
      '%s() keeps a partition that still holds a batch never sent',
      (reset) => {
        const sequencer = build()
        const waiting = createBatch()
        sequencer.enqueue([waiting])

        sequencer[reset](TOPIC, 0)

        expect(sequencer.entry(TOPIC, 0).queued).toEqual([waiting])
        expect(drain(sequencer)).toEqual([waiting])
      }
    )

    test('a partition moved back to 0 by advance() holds nothing, so it is dropped', () => {
      const sequencer = build()
      sequencer.advance(TOPIC, 0, 5)
      expect(sequencer.entry(TOPIC, 0)).not.toBeNull()

      sequencer.advance(TOPIC, 0, -5)

      expect(sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('a partition number given as a string reaches the same partition everywhere', () => {
      const sequencer = build()
      const [batch] = send(sequencer, createBatch())

      expect(sequencer.nextSequence(TOPIC, '0')).toEqual(5)
      expect(sequencer.entry(TOPIC, '0')).not.toBeNull()
      sequencer.restartPartition(TOPIC, '0')
      expect(batch.sequenceReset).toBe(true)
      sequencer.markStaleExcept(TOPIC, '0')
      expect(sequencer.entry(TOPIC, 0).stale).toBe(false)
      sequencer.resetPartition(TOPIC, '0')
      expect(sequencer.entry(TOPIC, 0).stale).toBe(true)
    })

    test('hasInFlight() ignores batches never sent', () => {
      const sequencer = build()
      sequencer.enqueue([createBatch()])

      expect(sequencer.hasInFlight()).toBe(false)
    })

    test('hasInFlight() counts a batch waiting for its re-send', () => {
      const sequencer = build()
      expect(sequencer.hasInFlight()).toBe(false)

      const [batch] = send(sequencer, createBatch())
      sequencer.requeue(batch, 0)
      expect(sequencer.hasInFlight()).toBe(true)

      drain(sequencer)
      sequencer.complete(batch, { baseOffset: '0' })
      expect(sequencer.hasInFlight()).toBe(false)
    })
  })

  describe('the int32 sequence', () => {
    test('wraps to 0 right after INT_32_MAX_VALUE, as the broker expects', () => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 4)
      const broker = createBrokerLog(INT_32_MAX_VALUE - 5)

      const [last] = send(sequencer, createBatch({ records: 5 }))
      const [first] = send(sequencer, createBatch({ records: 2 }))

      expect(last.firstSequence).toEqual(INT_32_MAX_VALUE - 4)
      expect(first.firstSequence).toEqual(0)
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(2)
      sequencer.complete(last, { baseOffset: '0' })
      expect(sequencer.entry(TOPIC, 0).lastAckedSequence).toEqual(INT_32_MAX_VALUE)
      expect([last, first].map((batch) => broker.append(batch))).toEqual([true, true])
    })

    test('a batch that crosses INT_32_MAX_VALUE continues from the wrapped sequence', () => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 5)

      const [crossing] = send(sequencer, createBatch({ records: 5 }))
      const [atMax] = send(sequencer, createBatch({ records: 3 }))

      expect(crossing.firstSequence).toEqual(INT_32_MAX_VALUE - 5)
      expect(atMax.firstSequence).toEqual(INT_32_MAX_VALUE)
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(2)
    })

    test.each([
      ['in numbering order', [0, 1]],
      ['in reverse order', [1, 0]],
    ])('re-sends requeued %s across the wrap go out in numbering order', (_, order) => {
      const sequencer = build(PIPELINED)
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 4)
      const batches = [createBatch({ records: 5 }), createBatch({ records: 2 })]
      sequencer.enqueue(batches)
      drain(sequencer)
      drain(sequencer)
      for (const index of order) {
        sequencer.requeue(batches[index], 0)
      }

      expect(sequencesOf(batches)).toEqual([INT_32_MAX_VALUE - 4, 0])
      expect(sequencer.entry(TOPIC, 0).queued).toEqual(batches)
      expect(drain(sequencer)).toEqual([batches[0]])
      expect(drain(sequencer)).toEqual([])
      sequencer.complete(batches[0], { baseOffset: '0' })
      expect(drain(sequencer)).toEqual([batches[1]])
      sequencer.complete(batches[1], { baseOffset: '5' })
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedSequence: 1,
        inFlight: [],
        queued: [],
      })
    })

    test('a batch that ends past the wrap moves the acknowledged sequence on, so the next batch is next', () => {
      const sequencer = build()
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 5)
      const [below] = send(sequencer, createBatch({ records: 4 }))
      sequencer.complete(below, { baseOffset: '0' })
      const [crossing] = send(sequencer, createBatch({ records: 5 }))
      sequencer.complete(crossing, { baseOffset: '4' })
      const [next] = send(sequencer, createBatch())
      const outOfOrder = failure({ category: 'abortable', code: OUT_OF_ORDER_SEQUENCE_NUMBER })

      expect(sequencesOf([below, crossing, next])).toEqual([
        INT_32_MAX_VALUE - 5,
        INT_32_MAX_VALUE - 1,
        3,
      ])
      expect(sequencer.entry(TOPIC, 0).lastAckedSequence).toEqual(2)
      expect(sequencer.verdict(next, outOfOrder)).toEqual('BUMP_EPOCH')
    })

    test('a partition whose last batch ended at INT_32_MAX_VALUE keeps its acknowledged offset', () => {
      const sequencer = build()
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 4)
      const [batch] = send(sequencer, createBatch({ records: 5 }))
      sequencer.complete(batch, { baseOffset: '100' })

      expect(sequencer.entry(TOPIC, 0)).toMatchObject({ nextSequence: 0, lastAckedOffset: 104 })
      expect(sequencer.isTruncated(createBatch(), '50')).toBe(false)
    })

    test('the batch after one that ended at INT_32_MAX_VALUE is the next sequence, at 0', () => {
      const sequencer = build()
      sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 4)
      const [last] = send(sequencer, createBatch({ records: 5 }))
      sequencer.complete(last, { baseOffset: '0' })
      const [next] = send(sequencer, createBatch())
      const outOfOrder = failure({ category: 'abortable', code: OUT_OF_ORDER_SEQUENCE_NUMBER })

      expect(next.firstSequence).toEqual(0)
      expect(sequencer.verdict(next, outOfOrder)).toEqual('BUMP_EPOCH')
    })

    test('advance() moves the next sequence by a delta and rotates to 0 once INT_32_MAX_VALUE is reached', () => {
      const sequencer = build()

      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(0)
      expect(sequencer.advance(TOPIC, 0, INT_32_MAX_VALUE - 1)).toEqual(INT_32_MAX_VALUE - 1)
      expect(sequencer.advance(TOPIC, 0, 1)).toEqual(0)
      expect(sequencer.advance(TOPIC, 0, 7)).toEqual(7)
      expect(sequencer.advance(TOPIC, 0, -7)).toEqual(0)
      expect(sequencer.nextSequence(TOPIC, 0)).toEqual(0)
    })
  })
})

describe('PartitionSequencer delivery incarnations', () => {
  it('disposes from the captured entry without recreating a removed partition', () => {
    const sequencer = build()
    const [old] = send(sequencer, createBatch())
    sequencer.rebase(old)
    expect(sequencer.entry(TOPIC, 0)).toBeNull()
    sequencer.complete(old, { baseOffset: '80' })
    expect(old.entry.inFlight).toEqual([])
    expect(sequencer.entry(TOPIC, 0)).toBeNull()
  })

  it.each([false, true])(
    'does not acknowledge a superseded incarnation, duplicate %s',
    (duplicate) => {
      const sequencer = build()
      const [old] = send(sequencer, createBatch())
      const incarnation = old.incarnation
      sequencer.restartPartition(TOPIC, 0)
      expect(old.entry.incarnation).toBeGreaterThan(incarnation)
      sequencer.complete(old, duplicate ? { duplicate: true } : { baseOffset: '80' })
      expect(old.entry.inFlight).toEqual([])
      expect(sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedSequence: -1,
        lastAckedOffset: -1,
      })
    }
  )

  it('keeps a recreated partition unchanged when its old entry is disposed', () => {
    const sequencer = build()
    const [old] = send(sequencer, createBatch())
    sequencer.rebase(old)
    const [fresh] = send(sequencer, createBatch())
    const before = sequencer.entry(TOPIC, 0)
    expect(fresh.entry).not.toBe(old.entry)
    expect(fresh.incarnation).toBeGreaterThan(old.incarnation)
    sequencer.complete(old, { baseOffset: '80' })
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
  })

  it('acknowledges another partition across two epoch bumps without skipping disposal', () => {
    const sequencer = build()
    const old = createBatch({ partition: 1 })
    const trigger = createBatch({ partition: 0 })
    send(sequencer, trigger, old)
    const incarnation = old.incarnation
    sequencer.restartPartition(TOPIC, 0)
    sequencer.markStaleExcept(TOPIC, 0)
    sequencer.restartPartition(TOPIC, 0)
    sequencer.markStaleExcept(TOPIC, 0)
    expect(old.entry.incarnation).toBe(incarnation)
    sequencer.complete(old, { baseOffset: '80' })
    expect(sequencer.entry(TOPIC, 1)).toMatchObject({
      lastAckedSequence: 4,
      lastAckedOffset: 84,
      inFlight: [],
    })
    expect(drain(sequencer, BUMPED)).toEqual([])
  })

  it('returns an unwritten refused attempt to its original order without retry or backoff', () => {
    const sequencer = build()
    const [batch] = send(sequencer, createBatch())
    sequencer.requeue(batch, 0, { counted: false })
    expect(batch.attempt).toBe(0)
    expect(batch.backoffUntil).toBe(0)
    expect(drain(sequencer)).toEqual([batch])
    expect(batch.firstSequence).toBe(0)
  })
})

describe('sequence reset boundaries', () => {
  test('resetPartition invalidates a late acknowledgement before restart', () => {
    const sequencer = build()
    const [old] = send(sequencer, createBatch())
    const incarnation = old.incarnation
    sequencer.resetPartition(TOPIC, 0)
    expect(old.entry.incarnation).toBeGreaterThan(incarnation)
    sequencer.complete(old, { baseOffset: '80' })
    expect(old.entry.inFlight).toEqual([])
    expect(sequencer.entry(TOPIC, 0)).toMatchObject({ lastAckedSequence: -1, lastAckedOffset: -1 })
  })

  test('resetAll assigns fresh incarnations to kept queued entries', () => {
    const sequencer = build()
    const [old] = send(sequencer, createBatch())
    sequencer.complete(old, { baseOffset: '0' })
    const queued = createBatch()
    sequencer.enqueue([queued])
    sequencer.resetAll()
    expect(drain(sequencer)).toEqual([queued])
    expect(queued.entry).not.toBe(old.entry)
    expect(queued.incarnation).toBeGreaterThan(old.incarnation)
    const before = sequencer.entry(TOPIC, 0)
    sequencer.complete(old, { baseOffset: '80' })
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
  })
})

describe('settlement without a live numbered range', () => {
  test('ignores completion, failure, rebase and gap marking for an unregistered batch', () => {
    const sequencer = build()
    const batch = createBatch()
    sequencer.complete(batch, { baseOffset: '80' })
    sequencer.fail(batch)
    sequencer.rebase(batch)
    sequencer.markUnresolved(batch)
    expect(sequencer.entry(TOPIC, 0)).toBeNull()
    expect(sequencer.resolveUnresolved()).toEqual([])
  })

  test('does not mark an unnumbered queued batch unresolved', () => {
    const sequencer = build()
    const queued = createBatch()
    sequencer.enqueue([queued])
    const before = sequencer.entry(TOPIC, 0)
    sequencer.markUnresolved(queued)
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
    expect(sequencer.resolveUnresolved()).toEqual([])
    expect(drain(sequencer)).toEqual([queued])
    expect(queued.firstSequence).toBe(0)
  })

  test('does not rebase a numbered response snapshot absent from the in-flight list', () => {
    const sequencer = build()
    const [batch] = send(sequencer, createBatch())
    const snapshot = { ...batch }
    delete snapshot.entry
    const before = sequencer.entry(TOPIC, 0)
    sequencer.rebase(snapshot)
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
    expect(batch.entry.inFlight).toEqual([batch])
  })

  test('does not rebase an unstamped retry before its restart is drained', () => {
    const sequencer = build()
    const [batch] = send(sequencer, createBatch())
    sequencer.restartPartition(TOPIC, 0)
    const before = sequencer.entry(TOPIC, 0)
    sequencer.rebase(batch)
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
    expect(batch.entry.inFlight).toEqual([batch])
  })

  test('does not mark or rebase a captured entry after its replacement is created', () => {
    const sequencer = build()
    const [old] = send(sequencer, createBatch())
    sequencer.rebase(old)
    const [fresh] = send(sequencer, createBatch())
    const before = sequencer.entry(TOPIC, 0)
    sequencer.markUnresolved(old)
    sequencer.rebase(old)
    expect(sequencer.entry(TOPIC, 0)).toEqual(before)
    expect(fresh.entry.inFlight).toEqual([fresh])
    expect(sequencer.resolveUnresolved()).toEqual([])
  })

  test('rejects a stamped obsolete attempt but retains its unstamped pending retry gap', () => {
    const sequencer = build()
    const [batch] = send(sequencer, createBatch({ records: 1 }))
    const obsolete = { ...batch }
    const incarnation = batch.incarnation
    sequencer.restartPartition(TOPIC, 0)
    expect(batch.incarnation).toBe(incarnation)
    expect(batch.producerEpoch).toBeNull()
    sequencer.markUnresolved(obsolete)
    expect(sequencer.entry(TOPIC, 0).unresolvedNextSequence).toBeNull()
    sequencer.markUnresolved(batch)
    expect(sequencer.entry(TOPIC, 0)).toMatchObject({
      unresolvedNextSequence: 1,
      lastAckedSequence: -1,
      lastAckedOffset: -1,
    })
    sequencer.fail(batch)
    expect(sequencer.resolveUnresolved()).toEqual([{ topic: TOPIC, partition: 0, outcome: 'gap' }])
  })
})
