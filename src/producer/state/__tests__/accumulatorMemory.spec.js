const { spawnSync } = require('child_process')
const path = require('path')
const Accumulator = require('../accumulator')
const { Admission } = require('../admission')
const Generation = require('../../generation')
const { chargeOf, RequestBudget } = require('../recordSize')
const {
  deferred,
  countersOf,
  succeeded,
  flushAll,
  quiescent,
  transactionalProducer,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')
const {
  POPULATION_COUNT,
  TOPIC,
  drain,
  fill,
  pinCluster,
  populations,
} = require('./fixtures/retainedStorage')

const RETAINED_STORAGE = path.join(__dirname, 'fixtures', 'retainedStorage.js')

/** Well under the 90 s a test may take, so a measurement that hangs fails with its own message. */
const MEASUREMENT_TIMEOUT_MS = 45000

/**
 * Measures a scenario's retained storage in a plain Node process, because Jest's own heap carries
 * the coverage counters and the async-hook bookkeeping of `--detectOpenHandles`.
 * @param {'held'|'pinned'|'tracked'} scenario
 * @returns {{ shape: string, capacity: number, measured: number, counted: number }[]} one sample
 *   per population, in bytes
 * @throws {Error} naming the cause when the process fails, times out or prints anything but JSON
 */
const measuredInChild = (scenario) => {
  const run = spawnSync(process.execPath, [RETAINED_STORAGE, scenario], {
    encoding: 'utf8',
    timeout: MEASUREMENT_TIMEOUT_MS,
    killSignal: 'SIGKILL',
    env: { ...process.env, NODE_OPTIONS: undefined, NODE_V8_COVERAGE: undefined },
  })
  if (run.error || run.status !== 0) {
    const cause = run.error?.code ?? `exit ${run.status ?? run.signal}`
    throw new Error(`the ${scenario} measurement did not complete (${cause}): ${run.stderr}`)
  }
  try {
    return JSON.parse(run.stdout)
  } catch {
    throw new Error(`the ${scenario} measurement printed non-JSON: ${run.stdout.slice(0, 200)}`)
  }
}

/**
 * Asserts that no population retained more than what it is charged, over 0.9.
 * @param {{ shape: string, capacity: number, measured: number, counted: number }[]} samples
 */
const expectWithinCharge = (samples) => {
  expect(samples).toHaveLength(POPULATION_COUNT)
  samples.forEach(({ counted }) => expect(counted).toBeGreaterThan(0))
  expect(samples.filter(({ measured, counted }) => measured > counted / 0.9)).toEqual([])
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

  test('the storage the held records retain is within what they are charged, over 0.9', () => {
    expectWithinCharge(measuredInChild('held'))
  })

  test('pinned on the producer cluster, the storage the held records retain is within what they are charged, over 0.9, and the pins end with the charge', async () => {
    expectWithinCharge(measuredInChild('pinned'))
    const cluster = pinCluster()
    await populations(async (capacity, records) => {
      const population = await fill(capacity, records, cluster)
      const { admission, sends } = population
      try {
        // every send but the refused last one pins its topic while it is charged
        expect(cluster.topicPins.get(TOPIC)).toBe(sends.length - 1)
      } finally {
        await drain(population)
      }
      expect(cluster.topicPins.size).toBe(0)
      expect(countersOf(admission)).toEqual(quiescent(admission))
    })
  })

  test('tracked for a flush whose snapshot stays open, the storage the held records retain is within what they are charged, over 0.9, and the inventory empties with the sends', async () => {
    expectWithinCharge(measuredInChild('tracked'))
    const cluster = pinCluster()
    const generation = new Generation()
    await populations(async (capacity, records) => {
      const population = await fill(capacity, records, cluster, () => generation)
      const { accumulator, admission } = population
      const flushed = watch(accumulator.unsettled()[0])
      try {
        expect(flushed.state).toBe('pending')
      } finally {
        await drain(population)
      }
      await flushAll()
      expect(flushed.state).toBe('fulfilled')
      expect(accumulator.unsettled()).toEqual([])
      expect(cluster.topicPins.size).toBe(0)
      expect(countersOf(admission)).toEqual(quiescent(admission))
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
