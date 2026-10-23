const Accumulator = require('../accumulator')
const { Admission } = require('../admission')
const { chargeOf, RequestBudget } = require('../recordSize')
const {
  collect,
  countersOf,
  deferred,
  succeeded,
  flushAll,
  quiescent,
  transactionalProducer,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'

/** The heap and the backing stores retained once the garbage is collected. */
const retained = async () => {
  await collect()
  const { heapUsed, arrayBuffers } = process.memoryUsage()
  return heapUsed + arrayBuffers
}

/** The shapes of send the bound must hold for: `records(i, shared)` gives the records of send i. */
const SHAPES = [
  [
    'varying records per send',
    (i) =>
      Array.from({ length: 1 + (i % 7) }, (_, j) => ({ key: `k${j}`, value: 'v'.repeat(20 + j) })),
  ],
  ['one record per partition', (i) => [{ value: `fan-${i}`, partition: i }]],
  [
    'many empty header values',
    () => [
      {
        value: '',
        headers: Object.fromEntries(Array.from({ length: 20 }, (_, j) => [`h${j}`, ''])),
      },
    ],
  ],
  ['independent 16 KiB slabs', (i) => [{ value: Buffer.alloc(16384, i % 256) }]],
  [
    'Buffer-valued headers and a shared store',
    (i, shared) => [
      {
        value: shared.subarray((i % 64) * 16, (i % 64) * 16 + 16),
        headers: { a: Buffer.alloc(32), b: [Buffer.alloc(8), shared.subarray(1000, 1008)] },
      },
    ],
  ],
]

/**
 * An accumulator over a buffer whose send path never answers: every batch it is handed is kept in
 * `batches` until `drain()` settles and retires it.
 */
const holding = (capacity) => {
  const admission = new Admission({ capacity, maxBlockMs: 60000 })
  const batches = []
  const accumulator = new Accumulator({
    linger: 0,
    batchSize: 16384,
    admission,
    partition: async (send) =>
      send.records.flatMap(({ topic, messages }) =>
        messages.map((message) => ({
          topic,
          partition: message.partition ?? 0,
          messages: [message],
        }))
      ),
    sendPartitioned: async ({ batches: handed }) => {
      batches.push(...handed)
    },
  })
  return { accumulator, admission, batches }
}

/**
 * Sends a shape's sends, all in one turn, until the buffer refuses one as queue-full, and lets
 * their joins run.
 * @returns {Promise<object>} the population: its buffer, its batches, its sends' outcomes, and the
 *   counters when the first send had to wait
 */
const fill = async (capacity, records) => {
  const population = holding(capacity)
  const { accumulator, admission } = population
  let shared = Buffer.alloc(1024)
  const sends = []
  let wave = null
  for (let i = 0; ; i += 1) {
    const mark = admission.reserved + admission.waitingBytes
    sends.push(
      accumulator
        .append({ acks: -1, topicMessages: [{ topic: TOPIC, messages: records(i, shared) }] })
        .catch((error) => error)
    )
    if (admission.reserved + admission.waitingBytes === mark) break
    if (admission.waitingBytes > 0 && wave === null) wave = countersOf(admission)
  }
  shared = null
  await flushAll()
  return { ...population, sends, wave }
}

/**
 * Answers and retires every batch of a population, the waiting sends' included once they are
 * admitted, until none is left.
 * @returns {Promise<object[]>} the outcome of every send
 */
const drain = async ({ batches, sends }) => {
  for (let round = 0; round < 1000; round += 1) {
    await flushAll()
    if (batches.length === 0) break
    for (const batch of batches.splice(0)) {
      batch.settlers.forEach((settler) =>
        settler.resolve({ topicName: TOPIC, partition: batch.partition, baseOffset: '0' })
      )
      batch.retire?.()
    }
  }
  return Promise.all(sends)
}

/** Each capacity with each shape, after one unmeasured population that also compiles the path. */
const populations = async (each) => {
  await drain(await fill(1048576, SHAPES[0][1]))
  for (const capacity of [1048576, 4194304]) {
    for (const [, records] of SHAPES) {
      await each(capacity, records)
    }
  }
}

describe('Producer > the buffer > the bound on held records', () => {
  /**
   * The byte budget bounds the records the producer holds, the sends waiting for room included:
   * with C the capacity, the admitted sends hold at most C, the waiting sends at most another C,
   * and the batches' debits at most 1024 for each live batch, so the records held at once are at
   * most floor(2C / 88). This is the bounded-count protection of franz-go's MaxBufferedRecords by
   * another mechanism: there is no independent record-count option and no fixed 50 000 default,
   * and the bound moves with bufferMemory.
   */
  test('R2§2:MaxBufferedRecords', async () => {
    await populations(async (capacity, records) => {
      const population = await fill(capacity, records)
      const { admission, batches, wave } = population
      let outcomes
      try {
        // the same turn admitted its sends before any of their batches existed
        expect(wave).not.toBeNull()
        expect(wave.debts).toBe(0)
        expect(admission.reserved).toBeLessThanOrEqual(capacity)
        expect(admission.waitingBytes).toBeLessThanOrEqual(capacity)
        expect(admission.waitingBytes).toBeGreaterThan(0)
        expect(admission.debts).toBeLessThanOrEqual(1024 * batches.length)
        const heldRecords = batches.reduce((sum, batch) => sum + batch.messages.length, 0)
        expect(heldRecords).toBeLessThanOrEqual(Math.floor((2 * capacity) / 88))
      } finally {
        outcomes = await drain(population)
      }
      expect(outcomes.at(-1)).toMatchObject({ reason: 'queue-full' })
      expect(countersOf(admission)).toEqual(quiescent(admission))
    })
  })

  test('a split send whose final chunk is held keeps every bound, and its shell until that chunk retires', async () => {
    // 2000 bytes of request carry 1845 bytes of records: chunks of three 525-byte records
    const budget = new RequestBudget({ maxRequestSize: 2000 })
    const split = Array.from({ length: 10 }, () => ({ value: 'x'.repeat(500) }))
    const charge = chargeOf([{ topic: TOPIC, messages: split }], Number.MAX_SAFE_INTEGER).total
    const capacity = charge + 1000
    const admission = new Admission({ capacity, maxBlockMs: 60000 })
    const batches = []
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: 16384,
      admission,
      budget,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async ({ batches: handed }) => {
        batches.push(...handed)
      },
    })
    const sent = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: split }],
    })
    const waiting = accumulator.append({
      acks: -1,
      topicMessages: [
        { topic: TOPIC, messages: Array.from({ length: 8 }, () => ({ value: 'w'.repeat(600) })) },
      ],
    })
    await flushAll()

    expect(batches.map(({ messages }) => messages.length)).toEqual([3, 3, 3, 1])
    const last = batches.pop()
    await drain({ batches, sends: [] })
    expect(admission.reserved).toBe(1536 + 525 + 64)
    expect(admission.debts).toBe(1024)
    expect(admission.reserved).toBeLessThanOrEqual(capacity)
    expect(admission.waitingBytes).toBeGreaterThan(0)
    expect(admission.waitingBytes).toBeLessThanOrEqual(capacity)

    last.settlers.forEach((settler) =>
      settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '9' })
    )
    last.retire()
    await flushAll()
    expect(admission.waitingBytes).toBe(0)
    await drain({ batches, sends: [sent, waiting] })
    expect(await sent).toEqual([{ topicName: TOPIC, partition: 0, baseOffset: '0' }])
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('the storage the held records retain is within what they are charged, over 0.9', async () => {
    await populations(async (capacity, records) => {
      const before = await retained()
      const population = await fill(capacity, records)
      const { admission } = population
      try {
        const after = await retained()
        const counted = admission.reserved + admission.waitingBytes + admission.debts
        expect(after - before).toBeLessThanOrEqual(counted / 0.9)
      } finally {
        await drain(population)
      }
    })
  })

  test('one buffer serves the producer and its transactions', async () => {
    const charge = (value) =>
      chargeOf([{ topic: TOPIC, messages: [{ value }] }], Number.MAX_SAFE_INTEGER).total
    const { producer, admission, transaction, release } = await transactionalProducer()

    try {
      const own = watch(producer.send({ topic: TOPIC, messages: [{ value: 'own' }] }))
      const transactional = watch(
        transaction.send({ topic: TOPIC, messages: [{ value: 'in a transaction' }] })
      )

      expect(admission.reserved).toBe(charge('own') + charge('in a transaction'))
      await release()
      expect([own.state, transactional.state]).toEqual(['fulfilled', 'fulfilled'])
      await transaction.commit()
      expect(countersOf(admission)).toEqual(quiescent(admission))
    } finally {
      await release()
      await transaction.abort().catch(() => {})
      await release()
      await producer.disconnect()
    }
  })

  test('a producer that sets neither option has a 32 MiB buffer and waits 60 seconds for room', async () => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    const createProducer = require('../../index')
    const { createFakeCluster } = require('../../__tests__/fixtures/fakeCluster')
    const { createLogger, LEVELS } = require('../../../loggers')
    const cluster = createFakeCluster()
    const answer = deferred()
    cluster.broker.produce.mockImplementation(async (request) => {
      await answer.promise
      return succeeded(request)
    })
    const producer = createProducer({
      cluster,
      logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
      linger: 0,
      maxRequestSize: 2147483647,
    })
    try {
      await producer.connect()

      const refusal = watch(
        producer.send({ topic: TOPIC, messages: [{ value: 'x'.repeat(33554432) }] })
      )
      await flushAll()
      expect(refusal.state).toBe('rejected')
      expect(refusal.value).toMatchObject({ name: 'KafkaJSRecordTooLargeError', limit: 33554432 })
      producer.send({ topic: TOPIC, messages: [{ value: Buffer.alloc(31457280) }] }).catch(() => {})
      await flushAll()
      const waiting = watch(
        producer.send({ topic: TOPIC, messages: [{ value: Buffer.alloc(4194304) }] })
      )
      await jest.advanceTimersByTimeAsync(59999)
      expect(waiting.state).toBe('pending')
      await jest.advanceTimersByTimeAsync(1)
      expect(waiting.value).toMatchObject({
        name: 'KafkaJSBufferExhaustedError',
        reason: 'timeout',
        capacity: 33554432,
        waitedMs: 60000,
      })
    } finally {
      answer.resolve()
      await flushAll()
      await producer.disconnect()
      jest.useRealTimers()
    }
  })
})
