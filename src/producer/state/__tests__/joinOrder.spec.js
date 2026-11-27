const fs = require('fs')
const path = require('path')
const Accumulator = require('../accumulator')
const JoinOrder = require('../joinOrder')
const { Admission } = require('../admission')
const { Deadline } = require('../../deadline')
const {
  KafkaJSBufferExhaustedError,
  KafkaJSDeliveryTimeout,
  KafkaJSProducerClosedError,
} = require('../../../errors')
const { takeSent } = require('../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'
const OTHER_TOPIC = 'topic-b'
const EQUIVALENCE = path.join(__dirname, 'fixtures', 'joinOrderEquivalence.json')

/** A promise with its resolve and reject. */
const deferred = () => {
  let resolve, reject
  const promise = new Promise((done, fail) => {
    resolve = done
    reject = fail
  })
  return { promise, resolve, reject }
}

/** A record's value as a broker reads it, as text. */
const textOf = ({ value }) => value.toString()

/** Every record goes to the partition it names. */
const byMessage = ({ records }) =>
  records.flatMap(({ topic, messages }) =>
    [...new Set(messages.map(({ partition }) => partition))].map((partition) => ({
      topic,
      partition,
      messages: messages.filter((message) => message.partition === partition),
    }))
  )

/**
 * An accumulator whose sends are partitioned when the test says so: `assign(value)` resolves the
 * partitioning of the send whose first record has that value, `fail(value, error)` rejects it.
 * Every hand-off is recorded and answered at once.
 */
const createGated = ({ deadline, linger = 0, admission } = {}) => {
  const gates = new Map()
  const gateOf = (value) => {
    if (!gates.has(value)) gates.set(value, deferred())
    return gates.get(value)
  }
  const calls = []
  const accumulator = new Accumulator({
    linger,
    batchSize: 16384,
    deadline,
    admission,
    partition: async (send) => {
      const value = send.records[0].messages[0].value
      await gateOf(value).promise
      return byMessage(send)
    },
    sendPartitioned: async (request) => {
      calls.push(takeSent(request))
      return request.batches.map(({ topic, partition, settlers }) => {
        const metadata = { topicName: topic, partition, errorCode: 0, baseOffset: '0' }
        settlers.forEach((settler) => settler.resolve(metadata))
        return metadata
      })
    },
  })
  const written = async () =>
    (await Promise.all(calls.map(({ read }) => read()))).flatMap((batches) =>
      batches.flatMap(({ records }) => records.map(textOf))
    )
  const writtenCount = () =>
    calls.reduce((sum, { batches }) => batches.reduce((total, { count }) => total + count, sum), 0)
  return {
    accumulator,
    written,
    writtenCount,
    assign: (value) => gateOf(value).resolve(),
    fail: (value, error) => gateOf(value).reject(error),
  }
}

/** A send of one record to partition 0 of `topic`. */
const sendOne = (accumulator, value, topic = TOPIC) =>
  accumulator.append({ acks: -1, topicMessages: [{ topic, messages: [{ partition: 0, value }] }] })

/** Lets every queued job run. */
const flushJobs = () => new Promise((resolve) => setImmediate(resolve))

describe('Producer > the accumulator joins sends to their batches in call order', () => {
  test('a send partitioned before an earlier send of its topic waits for it, then both join in call order', async () => {
    const { accumulator, written, writtenCount, assign } = createGated()
    const first = sendOne(accumulator, 'first')
    const second = sendOne(accumulator, 'second')

    assign('second')
    await flushJobs()
    expect(writtenCount()).toBe(0)

    assign('first')
    await Promise.all([first, second])
    expect(await written()).toEqual(['first', 'second'])
  })

  test('an earlier send whose partitioning fails for good leaves the order, and the later ones join', async () => {
    const { accumulator, written, writtenCount, assign, fail } = createGated()
    const first = sendOne(accumulator, 'first')
    const second = sendOne(accumulator, 'second')
    const third = sendOne(accumulator, 'third')

    assign('third')
    assign('second')
    await flushJobs()
    expect(writtenCount()).toBe(0)

    const refused = new Error('no metadata, for good')
    fail('first', refused)
    await expect(first).rejects.toBe(refused)
    await Promise.all([second, third])
    expect(await written()).toEqual(['second', 'third'])
  })

  test('a send of another topic is not delayed by an earlier send still waiting for its partitions', async () => {
    const { accumulator, written, assign } = createGated()
    const stuck = sendOne(accumulator, 'stuck', TOPIC)
    const other = sendOne(accumulator, 'other', OTHER_TOPIC)

    assign('other')
    await other
    expect(await written()).toEqual(['other'])

    assign('stuck')
    await stuck
    expect(await written()).toEqual(['other', 'stuck'])
  })

  test('a send of two topics waits for the earlier sends of each, and holds the later sends of each', async () => {
    const { accumulator, written, writtenCount, assign } = createGated()
    const a = sendOne(accumulator, 'a', TOPIC)
    const both = accumulator.append({
      acks: -1,
      topicMessages: [
        { topic: TOPIC, messages: [{ partition: 0, value: 'both' }] },
        { topic: OTHER_TOPIC, messages: [{ partition: 0, value: 'both-b' }] },
      ],
    })
    const b = sendOne(accumulator, 'b', OTHER_TOPIC)

    assign('b')
    assign('both')
    await flushJobs()
    expect(writtenCount()).toBe(0)

    assign('a')
    await Promise.all([a, both, b])
    expect(await written()).toEqual(['a', 'both', 'both-b', 'b'])
  })

  test('a forced close rejects the sends waiting for an earlier one with the existing error and joins none of them', async () => {
    const { accumulator, writtenCount, assign } = createGated()
    const first = sendOne(accumulator, 'first')
    const second = sendOne(accumulator, 'second')
    assign('second')
    await flushJobs()

    accumulator.forceClose()

    for (const send of [first, second]) {
      const error = await send.catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSProducerClosedError)
      expect(error).toMatchObject({
        message: 'The producer was closed before the send was partitioned',
        outcome: 'unsent',
      })
    }
    assign('first')
    await flushJobs()
    expect(writtenCount()).toBe(0)
  })

  test('a send waiting for an earlier one still ends at its own delivery deadline, and the later ones then join', async () => {
    jest.useFakeTimers({ now: 0 })
    try {
      const { accumulator, written, writtenCount, assign } = createGated({ deadline: 100 })
      const first = sendOne(accumulator, 'first')
      first.catch(() => {})
      await jest.advanceTimersByTimeAsync(50)
      const second = sendOne(accumulator, 'second')
      assign('second')
      await jest.advanceTimersByTimeAsync(49)
      expect(writtenCount()).toBe(0)

      await jest.advanceTimersByTimeAsync(1)
      const error = await first.catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      await second
      expect(await written()).toEqual(['second'])
    } finally {
      jest.useRealTimers()
    }
  })

  test('a waiting send that reaches its deadline first ends alone, and the earlier one still joins', async () => {
    jest.useFakeTimers({ now: 0 })
    try {
      const { accumulator, written, writtenCount, assign } = createGated({ deadline: 100 })
      const first = sendOne(accumulator, 'first', TOPIC)
      const second = accumulator.append({
        acks: -1,
        startedAt: -50,
        topicMessages: [{ topic: TOPIC, messages: [{ partition: 0, value: 'second' }] }],
      })
      assign('second')
      await jest.advanceTimersByTimeAsync(50)

      const error = await second.catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(writtenCount()).toBe(0)

      assign('first')
      await jest.advanceTimersByTimeAsync(0)
      await first
      expect(await written()).toEqual(['first'])
    } finally {
      jest.useRealTimers()
    }
  })

  describe('a send that waits for buffer room', () => {
    const CAPACITY = 200000

    /** Holds all of the buffer but `free` bytes, too few for any one-record send. */
    const fill = (admission, free = 1000) =>
      admission.acquire(CAPACITY - free, new Deadline(Date.now(), 30000))

    test('keeps its call order against a later send admitted at once when room comes back', async () => {
      const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 10000 })
      const { accumulator, written, assign } = createGated({ admission })
      const blocker = fill(admission)
      const first = sendOne(accumulator, 'first')
      await flushJobs()

      blocker.releaseAll()
      const second = sendOne(accumulator, 'second')
      assign('second')
      assign('first')
      await Promise.all([first, second])
      expect(await written()).toEqual(['first', 'second'])
    })

    test('refused at once, leaves the order and the later sends of its topic join', async () => {
      const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 0 })
      const { accumulator, written, assign } = createGated({ admission })
      const blocker = fill(admission)
      const refused = sendOne(accumulator, 'refused')
      await expect(refused).rejects.toBeInstanceOf(KafkaJSBufferExhaustedError)

      blocker.releaseAll()
      const later = sendOne(accumulator, 'later')
      assign('later')
      await later
      expect(await written()).toEqual(['later'])
    })

    test('refused when its wait runs out, leaves the order and the later sends of its topic join', async () => {
      jest.useFakeTimers({ now: 0 })
      try {
        const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 50 })
        const { accumulator, written, assign } = createGated({ admission })
        const blocker = fill(admission)
        const refused = sendOne(accumulator, 'refused')
        refused.catch(() => {})
        await jest.advanceTimersByTimeAsync(50)
        await expect(refused).rejects.toBeInstanceOf(KafkaJSBufferExhaustedError)

        blocker.releaseAll()
        const later = sendOne(accumulator, 'later')
        assign('later')
        await jest.advanceTimersByTimeAsync(0)
        await later
        expect(await written()).toEqual(['later'])
      } finally {
        jest.useRealTimers()
      }
    })
  })

  describe('the order itself', () => {
    /** A send as `JoinOrder` sees it. */
    const orderedSend = (lanes) => ({ lanes, settled: false, entries: null })

    test('keeps no lane once every send has joined or ended, in any order', () => {
      const joined = []
      const order = new JoinOrder((send) => {
        joined.push(send)
        send.settled = true
      })
      const sends = [
        orderedSend('a'),
        orderedSend(['a', 'b']),
        orderedSend('b'),
        orderedSend('c'),
        orderedSend('a'),
      ]
      sends.forEach((send) => order.enter(send))
      expect(order.size).toBe(3)

      sends[4].entries = []
      order.ready(sends[4])
      sends[2].settled = true
      order.passed(sends[2])
      sends[1].entries = []
      order.ready(sends[1])
      sends[3].entries = []
      order.ready(sends[3])
      expect(joined).toEqual([sends[3]])

      sends[0].settled = true
      order.passed(sends[0])
      expect(joined).toEqual([sends[3], sends[1], sends[4]])
      expect(order.size).toBe(0)
    })

    test('joins a long run of waiting sends without recursing', () => {
      let count = 0
      const order = new JoinOrder((send) => {
        count += 1
        send.settled = true
      })
      const sends = Array.from({ length: 100000 }, () => orderedSend('a'))
      sends.forEach((send) => order.enter(send))
      for (const send of sends.slice(1)) {
        send.entries = []
        order.ready(send)
      }
      expect(count).toBe(0)

      sends[0].entries = []
      order.ready(sends[0])
      expect(count).toBe(sends.length)
      expect(order.size).toBe(0)
    })
  })

  describe('with no failure, sends join exactly as before', () => {
    /** A small deterministic generator (LCG), so the workloads are the same on every run. */
    const generator = (seed) => {
      let state = seed
      return (n) => {
        state = (state * 1103515245 + 12345) % 2147483648
        return state % n
      }
    }

    /**
     * Rounds of concurrent sends over three topics and four partitions, with two `acks` values
     * and records of several sizes; each round's sends all wait for one shared metadata load, as a
     * producer's sends wait for one refresh, and their batches are recorded as they leave.
     */
    const runWorkload = async ({ seed, linger, batchSize }) => {
      const next = generator(seed)
      const calls = []
      let gate = deferred()
      const accumulator = new Accumulator({
        linger,
        batchSize,
        partition: async (send) => {
          await gate.promise
          return byMessage(send)
        },
        sendPartitioned: async (request) => {
          const shapes = request.batches.map(({ topic, partition, acks }) => ({
            key: `${topic}/${partition}`,
            acks,
          }))
          calls.push({ shapes, sent: takeSent(request) })
          return request.batches.map(({ topic, partition, settlers }) => {
            const metadata = { topicName: topic, partition, errorCode: 0, baseOffset: '10' }
            settlers.forEach((settler) => settler.resolve(metadata))
            return metadata
          })
        },
      })
      const outcomes = []
      for (let round = 0; round < 6; round++) {
        const sends = []
        for (let i = 0, count = 1 + next(12); i < count; i++) {
          const topics = ['t0', 't1', 't2'].filter(() => next(2) === 0)
          if (topics.length === 0) topics.push(`t${next(3)}`)
          const topicMessages = topics.map((topic) => ({
            topic,
            messages: Array.from({ length: 1 + next(4) }, (_, n) => ({
              partition: next(4),
              value: `${round}.${i}.${topic}.${n}.${'x'.repeat(next(24))}`,
            })),
          }))
          sends.push(
            accumulator.append({ acks: next(3) === 0 ? 1 : -1, topicMessages }).then(
              (result) => result,
              (error) => ({ error: error.message })
            )
          )
        }
        gate.resolve()
        await jest.advanceTimersByTimeAsync(linger + 1)
        outcomes.push(await Promise.all(sends))
        gate = deferred()
      }
      const read = await Promise.all(calls.map(({ sent }) => sent.read()))
      return {
        calls: calls.map(({ shapes }, index) =>
          shapes.map((shape, at) => ({ ...shape, values: read[index][at].records.map(textOf) }))
        ),
        outcomes,
      }
    }

    const WORKLOADS = [1, 7, 42].flatMap((seed) => [
      { seed, linger: 0, batchSize: 16384 },
      { seed, linger: 5, batchSize: 16384 },
      { seed, linger: 5, batchSize: 64 },
    ])

    beforeEach(() => jest.useFakeTimers({ now: 0 }))
    afterEach(() => jest.useRealTimers())

    test('the batches handed on and every send’s result match those recorded before call order was kept', async () => {
      const results = []
      for (const workload of WORKLOADS) {
        results.push({ workload, ...(await runWorkload(workload)) })
      }
      if (process.env.RECORD_JOIN_ORDER_EQUIVALENCE) {
        fs.writeFileSync(EQUIVALENCE, `${JSON.stringify(results)}\n`)
      }
      expect(results).toEqual(JSON.parse(fs.readFileSync(EQUIVALENCE, 'utf8')))
      expect(results.flatMap(({ calls }) => calls).length).toBeGreaterThan(WORKLOADS.length)
    })
  })
})
