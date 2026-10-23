const { Admission, Reservation } = require('../admission')
const Long = require('../../../utils/long')
const Accumulator = require('../accumulator')
const { Deadline } = require('../../deadline')
const {
  KafkaJSBufferExhaustedError,
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSRecordTooLargeError,
  KafkaJSTransactionAbortableError,
} = require('../../../errors')
const {
  bufferedProducer,
  transactionalProducer,
  collect,
  countersOf,
  flushAll,
  quiescent,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'

/**
 * Observes prompt refusal before awaiting a send the predecessor never refuses.
 * @param {Promise<*>} promise the send under test
 * @returns {Promise<Error>} its rejection, after asserting that it has rejected
 */
const refusalOf = async (promise) => {
  const result = watch(promise)
  await flushAll()
  expect(result.state).toBe('rejected')
  return result.value
}

/** A deadline that never comes. */
const noDeadline = () => new Deadline(Date.now(), null)

describe('Producer > the buffer > admission', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  test('grants a send that fits while nobody waits at once, with no promise and no timer', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    const timers = jest.getTimerCount()

    const granted = admission.acquire(600, noDeadline())

    expect(granted).toBeInstanceOf(Reservation)
    expect(jest.getTimerCount()).toBe(timers)
    expect(countersOf(admission)).toEqual({
      available: 400,
      reserved: 600,
      waitingBytes: 0,
      debts: 0,
    })
  })

  test('grants in arrival order: a small send behind a large waiter waits too', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 10000 })
    const first = admission.acquire(900, noDeadline())
    const order = []
    // Promise.resolve: a grant made at once, which no waiter may take, must show in the order too
    const large = Promise.resolve(admission.acquire(800, noDeadline())).then(() =>
      order.push('large')
    )
    const small = Promise.resolve(admission.acquire(50, noDeadline())).then(() =>
      order.push('small')
    )
    await flushAll()
    expect(order).toEqual([])
    expect(admission.waitingBytes).toBe(850)

    first.release(700)
    await flushAll()
    expect(order).toEqual(['large'])
    expect(admission.waitingBytes).toBe(50)

    first.releaseAll()
    await Promise.all([large, small])
    expect(order).toEqual(['large', 'small'])
    expect(countersOf(admission)).toEqual({
      available: 150,
      reserved: 850,
      waitingBytes: 0,
      debts: 0,
    })
  })

  test('a waiter whose maxBlockMs runs out rejects and leaves the queue at once', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    admission.acquire(1000, noDeadline())
    const timers = jest.getTimerCount()
    const waiting = watch(admission.acquire(300, noDeadline()))
    expect(admission.waitingBytes).toBe(300)

    await jest.advanceTimersByTimeAsync(99)
    expect(waiting.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(waiting.state).toBe('rejected')
    expect(waiting.value).toBeInstanceOf(KafkaJSBufferExhaustedError)
    expect(waiting.value).toMatchObject({
      retriable: true,
      reason: 'timeout',
      requested: 300,
      available: 0,
      capacity: 1000,
      waitedMs: 100,
    })
    expect(admission.waitingBytes).toBe(0)
    expect(jest.getTimerCount()).toBe(timers)
  })

  test('a waiter whose delivery deadline comes before maxBlockMs rejects at its deadline, unsent', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 60000 })
    admission.acquire(1000, noDeadline())
    const waiting = watch(admission.acquire(300, new Deadline(0, 50)))

    await jest.advanceTimersByTimeAsync(49)
    expect(waiting.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(waiting.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(waiting.value.outcome).toBe('unsent')
    expect(admission.waitingBytes).toBe(0)
  })

  test('a waiter that leaves the head of the queue lets the waiters behind it in', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    admission.acquire(900, noDeadline())
    const large = watch(admission.acquire(800, noDeadline()))
    await jest.advanceTimersByTimeAsync(50)
    const small = watch(admission.acquire(50, noDeadline()))

    await jest.advanceTimersByTimeAsync(50)

    expect(large.value).toBeInstanceOf(KafkaJSBufferExhaustedError)
    expect(small.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual({
      available: 50,
      reserved: 950,
      waitingBytes: 0,
      debts: 0,
    })
  })

  test('a granted waiter leaves no timer behind and no bytes waiting', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    const timers = jest.getTimerCount()
    const first = admission.acquire(1000, noDeadline())
    const waiting = watch(admission.acquire(300, noDeadline()))
    expect(jest.getTimerCount()).toBe(timers + 1)

    first.releaseAll()
    await flushAll()
    expect(waiting.state).toBe('fulfilled')
    expect(jest.getTimerCount()).toBe(timers)
    await jest.advanceTimersByTimeAsync(200)

    expect(admission.waitingBytes).toBe(0)
    expect(admission.reserved).toBe(300)
  })

  test('with maxBlockMs 0 a send that does not fit is refused at once, with no timer', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 0 })
    admission.acquire(800, noDeadline())
    const timers = jest.getTimerCount()

    expect(() => admission.acquire(300, noDeadline())).toThrow(
      expect.objectContaining({
        name: 'KafkaJSBufferExhaustedError',
        reason: 'timeout',
        requested: 300,
        available: 200,
        capacity: 1000,
        waitedMs: 0,
      })
    )
    expect(jest.getTimerCount()).toBe(timers)
    expect(admission.waitingBytes).toBe(0)
  })

  test('a send whose deadline has passed and that would wait is refused at once, unsent', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    admission.acquire(1000, noDeadline())
    const passed = new Deadline(-10, 5)

    let error
    try {
      admission.acquire(1, passed)
    } catch (e) {
      error = e
    }

    expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(error.outcome).toBe('unsent')
    expect(admission.waitingBytes).toBe(0)
  })

  test('a send that would take the waiting sends past the capacity is refused at once: queue-full', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    admission.acquire(1000, noDeadline())
    admission.acquire(700, noDeadline()).catch(() => {})
    const timers = jest.getTimerCount()

    expect(() => admission.acquire(400, noDeadline())).toThrow(
      expect.objectContaining({
        name: 'KafkaJSBufferExhaustedError',
        reason: 'queue-full',
        requested: 400,
        available: 0,
        capacity: 1000,
        waitedMs: 0,
      })
    )
    expect(jest.getTimerCount()).toBe(timers)
    expect(admission.waitingBytes).toBe(700)
    expect(() => admission.acquire(300, noDeadline()).catch(() => {})).not.toThrow()
    expect(admission.waitingBytes).toBe(1000)
  })

  test('a send above the capacity is refused before the queue and the timer', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    const timers = jest.getTimerCount()

    expect(() => admission.acquire(1001, noDeadline())).toThrow(
      expect.objectContaining({
        name: 'KafkaJSRecordTooLargeError',
        retriable: false,
        type: 'MESSAGE_TOO_LARGE',
        code: 10,
        reason: 'bufferMemory',
        scope: 'send',
        size: 1001,
        limit: 1000,
      })
    )
    expect(jest.getTimerCount()).toBe(timers)
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('a debit is taken without waiting, and admission waits until it is repaid', async () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    admission.debit(1024)
    expect(countersOf(admission)).toEqual({
      available: 0,
      reserved: 0,
      waitingBytes: 0,
      debts: 1024,
    })
    const waiting = watch(admission.acquire(10, noDeadline()))
    await flushAll()
    expect(waiting.state).toBe('pending')

    admission.repay(1024)
    await flushAll()

    expect(waiting.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual({
      available: 990,
      reserved: 10,
      waitingBytes: 0,
      debts: 0,
    })
  })

  test('a reservation gives back no byte twice', () => {
    const admission = new Admission({ capacity: 1000, maxBlockMs: 100 })
    const reservation = admission.acquire(600, noDeadline())

    reservation.release(400)
    reservation.release(400)
    reservation.releaseAll()
    reservation.releaseAll()
    reservation.release(1)

    expect(countersOf(admission)).toEqual(quiescent(admission))
  })
})

describe('Producer > the buffer > a send waiting for room', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  /** An accumulator over a buffer, whose send path keeps every batch until `answer()`. */
  const create = ({ capacity, maxBlockMs = 100, deadline = 60000 } = {}) => {
    const admission = new Admission({ capacity, maxBlockMs })
    const handedOn = []
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: 16384,
      deadline,
      admission,
      partition: async (send) =>
        send.records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async (request) => {
        handedOn.push(...request.batches)
      },
    })
    const answer = () => {
      for (const batch of handedOn.splice(0)) {
        batch.settlers.forEach((settler) =>
          settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
        )
        batch.retire()
      }
    }
    return { accumulator, admission, answer }
  }

  const append = (accumulator, messages) =>
    accumulator.append({ acks: -1, topicMessages: [{ topic: TOPIC, messages }] })

  test('holds its records while it waits, and drops them with its rejection when maxBlockMs runs out', async () => {
    const { accumulator, admission } = create({ capacity: 8192 })
    const admitted = watch(append(accumulator, [{ value: Buffer.alloc(3000) }]))
    await flushAll()
    /** Starts the waiting send in a frame of its own, so this test keeps only the weak reference. */
    const startWaiting = () => {
      const value = Buffer.alloc(3000)
      return { ref: new WeakRef(value), waiting: watch(append(accumulator, [{ value }])) }
    }
    const { ref, waiting } = startWaiting()
    await flushAll()
    expect(admission.waitingBytes).toBe(3089 + 1536)

    await collect()
    expect(ref.deref()).toBeDefined()
    await jest.advanceTimersByTimeAsync(100)
    await collect()

    expect(ref.deref()).toBeUndefined()
    expect(waiting.value).toBeInstanceOf(KafkaJSBufferExhaustedError)
    expect(admission.waitingBytes).toBe(0)
    expect(admitted.state).toBe('pending')
  })

  test('a batch detaches and retires once, however often either is called', async () => {
    const admission = new Admission({ capacity: 8192, maxBlockMs: 100 })
    const handedOn = []
    const sent = watch(
      new Accumulator({
        linger: 0,
        batchSize: 16384,
        admission,
        partition: async (send) =>
          send.records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
        sendPartitioned: async ({ batches }) => {
          handedOn.push(...batches)
        },
      }).append({ acks: -1, topicMessages: [{ topic: TOPIC, messages: [{ value: 'v' }] }] })
    )
    await flushAll()
    const [batch] = handedOn
    batch.settlers.forEach((settler) =>
      settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
    )

    batch.detach()
    batch.detach()
    // the record's share is back; the send's shell (1536) and the batch's debit are not
    expect(batch.messages).toBeNull()
    expect(countersOf(admission)).toEqual({
      available: 8192 - 1536 - 1024,
      reserved: 1536,
      waitingBytes: 0,
      debts: 1024,
    })
    batch.retire()
    batch.retire()
    expect(() => batch.detach()).not.toThrow()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('a full admitted population and a full waiting population at once keep within the capacity each', async () => {
    const { accumulator, admission, answer } = create({ capacity: 8192, maxBlockMs: 1000 })
    const sends = []
    for (let i = 0; i < 12; i += 1) {
      sends.push(append(accumulator, [{ value: Buffer.alloc(1000, i) }]).catch((e) => e))
    }
    await flushAll()

    // Each send is charged 2625 bytes before its first await, so the same turn admits three,
    // queues the next three and refuses the six after them; the three admitted then join one
    // batch, which debits 1024 without waiting.
    expect(countersOf(admission)).toEqual({
      available: 0,
      reserved: 3 * 2625,
      waitingBytes: 3 * 2625,
      debts: 1024,
    })
    for (let round = 0; round < 6; round += 1) {
      answer()
      await flushAll()
    }
    const outcomes = await Promise.all(sends)
    expect(outcomes.map((outcome) => (Array.isArray(outcome) ? 'sent' : outcome.reason))).toEqual([
      ...Array(6).fill('sent'),
      ...Array(6).fill('queue-full'),
    ])
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })
})

describe('Producer > the buffer > through the producer', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  test('an empty send resolves [] and charges, arms, times and leaves nothing', async () => {
    const { producer, admission, cluster } = bufferedProducer({
      transactionalId: 'tx-empty',
      idempotent: true,
    })
    await producer.connect()
    const transaction = await producer.transaction()
    const timers = jest.getTimerCount()

    await expect(transaction.send({ topic: TOPIC, messages: [] })).resolves.toEqual([])
    await expect(producer.send({ topic: TOPIC, messages: [] })).resolves.toEqual([])
    await expect(
      producer.sendBatch({ topicMessages: [{ topic: TOPIC, messages: [] }] })
    ).resolves.toEqual([])
    await expect(producer.sendBatch({})).resolves.toEqual([])

    expect(jest.getTimerCount()).toBe(timers)
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    await expect(transaction.commit()).resolves.toBeUndefined()
    await producer.disconnect()
  })

  test('with bufferMemory 1 every send with a record is refused at once as too large, never waits', async () => {
    const { producer, admission, cluster } = bufferedProducer({ capacity: 1 })
    await producer.connect()
    try {
      const timers = jest.getTimerCount()

      const error = await refusalOf(producer.send({ topic: TOPIC, messages: [{ value: '' }] }))

      expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
      expect(error).toMatchObject({
        reason: 'bufferMemory',
        scope: 'record',
        size: 88,
        limit: 1,
        topic: TOPIC,
      })
      expect(jest.getTimerCount()).toBe(timers)
      expect(countersOf(admission)).toEqual(quiescent(admission))
      expect(cluster.broker.produce).not.toHaveBeenCalled()
    } finally {
      await flushAll()
      await producer.disconnect()
    }
  })

  const transactional = (options) => transactionalProducer(options)

  test.each([
    [
      'an admission timeout',
      { capacity: 8192, maxBlockMs: 100 },
      async (transaction) => {
        const pending = transaction.send({
          topic: TOPIC,
          messages: [{ value: Buffer.alloc(2000) }],
        })
        await flushAll()
        const refused = transaction.send({
          topic: TOPIC,
          messages: [{ value: Buffer.alloc(4000) }],
        })
        const outcome = watch(refused)
        await jest.advanceTimersByTimeAsync(100)
        await flushAll()
        expect(outcome.state).toBe('rejected')
        return { pending, refused: outcome.value, reason: 'timeout' }
      },
    ],
    [
      'a full waiting room',
      { capacity: 8192, maxBlockMs: 60000 },
      async (transaction) => {
        const pending = transaction.send({
          topic: TOPIC,
          messages: [{ value: Buffer.alloc(2000) }],
        })
        await flushAll()
        const waiting = transaction
          .send({ topic: TOPIC, messages: [{ value: Buffer.alloc(4000) }] })
          .catch((e) => e)
        await flushAll()
        const refused = watch(
          transaction.send({ topic: TOPIC, messages: [{ value: Buffer.alloc(4000) }] })
        )
        await flushAll()
        expect(refused.state).toBe('rejected')
        return { pending, waiting, refused: refused.value, reason: 'queue-full' }
      },
    ],
  ])(
    'inside a transaction, %s arms the abortable state and the send keeps its own retriable error',
    async (_, options, refuse) => {
      const { producer, transaction, cluster, admission, release } = await transactional(options)

      try {
        const { pending, waiting, refused, reason } = await refuse(transaction)
        const produced = cluster.broker.produce.mock.calls.length

        expect(refused).toBeInstanceOf(KafkaJSBufferExhaustedError)
        expect(refused).toMatchObject({ reason, retriable: true })
        const later = watch(transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
        const offsets = watch(
          transaction.sendOffsets({
            consumerGroupId: 'group-a',
            topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
          })
        )
        await flushAll()
        expect(later.value).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(offsets.value).toBeInstanceOf(KafkaJSTransactionAbortableError)
        await release()
        await pending.catch(() => {})
        await waiting
        const commit = await transaction.commit().catch((e) => e)
        expect(commit).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(commit.cause).toBe(refused)
        expect(cluster.broker.produce.mock.calls.length).toBe(produced)
        expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()

        await transaction.abort()
        const next = await producer.transaction()
        const sent = next.send({ topic: TOPIC, messages: [{ value: 'after' }] })
        await flushAll()
        await release()
        await expect(sent).resolves.toHaveLength(1)
        await expect(next.commit()).resolves.toBeUndefined()
        expect(countersOf(admission)).toEqual(quiescent(admission))
      } finally {
        await release()
        await transaction.abort().catch(() => {})
        await release()
        await producer.disconnect()
      }
    }
  )

  test.each([
    [
      'an unencodable record',
      () => ({ messages: [{ value: 42 }] }),
      KafkaJSInvalidConfigurationError,
      null,
    ],
    [
      'a Long timestamp whose value cannot be read',
      () => {
        const timestamp = Long.fromValue(5n)
        Object.defineProperty(timestamp, 'value', {
          get: () => {
            throw new Error('the value cannot be read')
          },
        })
        return { messages: [{ value: 'v', timestamp }] }
      },
      KafkaJSInvalidConfigurationError,
      null,
    ],
    [
      'a timestamp that only claims to be a Date',
      () => ({ messages: [{ value: 'v', timestamp: Object.create(Date.prototype) }] }),
      KafkaJSInvalidConfigurationError,
      null,
    ],
    [
      'a throwing custom partitioner',
      () => ({ messages: [{ value: 'v' }] }),
      Error,
      () => () => () => {
        throw new Error('the partitioner failed')
      },
    ],
  ])(
    'inside a transaction, %s leaves the transaction unarmed',
    async (_, send, ErrorClass, createPartitioner) => {
      const { producer, transaction, admission, release } = await transactional({
        capacity: 1048576,
        ...(createPartitioner ? { createPartitioner: createPartitioner() } : {}),
      })

      const error = await transaction.send({ topic: TOPIC, ...send() }).catch((e) => e)

      expect(error).toBeInstanceOf(ErrorClass)
      expect(error).not.toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(countersOf(admission)).toEqual(quiescent(admission))
      await release()
      await expect(transaction.commit()).resolves.toBeUndefined()
      await producer.disconnect()
    }
  )
})
