const path = require('path')
const { spawnSync } = require('child_process')
const Accumulator = require('../accumulator')
const { Admission } = require('../admission')
const createProducer = require('../../index')
const Decoder = require('../../../protocol/decoder')
const Encoder = require('../../../protocol/encoder')
const RecordBatchDecoder = require('../../../protocol/recordBatch/v0/decoder')
const Long = require('../../../utils/long')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../../protocol/error')
const { parse: parseProduce } = require('../../../protocol/requests/produce/partitionResults')
const { createFakeCluster } = require('../../__tests__/fixtures/fakeCluster')
const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSConnectionError,
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSTransactionAbortableError,
} = require('../../../errors')
const { newLogger } = require('testHelpers')
const Generation = require('../../generation')

const TOPIC = 'topic-a'
const OTHER_TOPIC = 'topic-b'
const LINGER = 5
const BATCH_SIZE = 16384
const GZIP = 1
const REQUEST_TIMED_OUT = 7
const UNKNOWN_TOPIC_OR_PARTITION = 3
const MESSAGE_TOO_LARGE = 10

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/**
 * @param {Promise} promise
 * @returns {() => boolean} whether `promise` has settled
 */
const settledFlag = (promise) => {
  let settled = false
  const flag = () => {
    settled = true
  }
  promise.then(flag, flag)
  return () => settled
}

/**
 * The decoded success answer to a Produce request, every partition at base offset 0.
 * @param {{ topicData: { topic: string, partitions: { partition: number }[] }[] }} request
 * @returns {object}
 */
const accepted = ({ topicData }) => ({
  topics: topicData.map(({ topic, partitions }) => ({
    topicName: topic,
    partitions: partitions.map(({ partition }) => ({
      partition,
      errorCode: 0,
      baseOffset: '0',
      logAppendTime: '-1',
      logStartOffset: '0',
    })),
  })),
})

/**
 * Moves the fake clock `ms` on, then hands `promise` back only if it has settled by then: a
 * promise still pending fails the case, where awaiting it would wait on a clock nothing moves.
 * @param {Promise} promise
 * @param {number} [ms=0]
 * @returns {Promise}
 */
const settledWithin = async (promise, ms = 0) => {
  const settled = settledFlag(promise)
  await jest.advanceTimersByTimeAsync(ms)
  expect(settled()).toBe(true)
  return promise
}

/**
 * Awaits `promise` for at most `ms` real milliseconds: one still pending then fails the case, where
 * awaiting it would hold the run for the suite's timeout.
 * @param {Promise} promise
 * @param {number} [ms=5000]
 * @returns {Promise}
 */
const withinRealTime = async (promise, ms = 5000) => {
  let timer
  const late = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`still pending after ${ms} ms`)), ms)
  })
  try {
    return await Promise.race([promise, late])
  } finally {
    clearTimeout(timer)
  }
}

/** Runs the queued microtasks, and those they queue, without moving the fake clock. */
const microtasks = async () => {
  for (let i = 0; i < 100; i++) {
    await Promise.resolve()
  }
}

/** `count` one-record messages for `partition`, values `<prefix>-<n>`. */
const records = (partition, prefix, count = 1) =>
  Array.from({ length: count }, (_, index) => ({ partition, value: `${prefix}-${index}` }))

/**
 * Every record goes to the partition it names, as `sendMessages.partition` would assign it. The
 * accumulator hands the send that holds the records.
 */
const byMessage = async ({ records }) =>
  records.flatMap(({ topic, messages }) =>
    [...new Set(messages.map(({ partition }) => partition))].map((partition) => ({
      topic,
      partition,
      messages: messages.filter((message) => message.partition === partition),
    }))
  )

/**
 * An accumulator over a fake send path that records every call and answers each batch with base
 * offset 100 unless `sendPartitioned` is given.
 */
const createAccumulator = ({
  linger = LINGER,
  batchSize = BATCH_SIZE,
  partition = jest.fn(byMessage),
  sendPartitioned,
  lingers,
  holds,
  leaderOf,
  deadline,
} = {}) => {
  const calls = []
  const answer = async (request) => {
    calls.push(request)
    return request.batches.map(({ topic, partition: id, settlers }) => {
      const metadata = { topicName: topic, partition: id, errorCode: 0, baseOffset: '100' }
      settlers.forEach((settler) => settler.resolve(metadata))
      return metadata
    })
  }
  const accumulator = new Accumulator({
    linger,
    batchSize,
    partition,
    sendPartitioned: sendPartitioned || jest.fn(answer),
    lingers,
    holds,
    leaderOf,
    deadline,
  })
  /** The values of each call's batches. */
  const sent = () =>
    calls.map(({ batches }) =>
      batches.map(({ topic, partition: id, messages }) => ({
        [`${topic}/${id}`]: messages.map(({ value }) => value),
      }))
    )
  return { accumulator, calls, sent, partition }
}

/**
 * A send of `messages` with the request-level options a `send()` passes; `startedAt` is the public
 * call's time when it is earlier than now.
 */
const append = (
  accumulator,
  messages,
  { acks = -1, timeout = 30000, compression = 0, startedAt } = {}
) =>
  accumulator.append({
    acks,
    timeout,
    compression,
    topicMessages: [{ topic: TOPIC, messages }],
    startedAt,
  })

/**
 * Runs an accumulator whose send path throws synchronously, in a child process, and reports where
 * the failure went: a rejection nobody awaits, an uncaught exception, or the append itself.
 * @param {{ linger: number, batchSize: number }} options
 * @returns {string[]} one line per event, in the order they happened
 */
const whereASendPathFailureGoes = ({ linger, batchSize }) => {
  const script = `
    const Accumulator = require(${JSON.stringify(path.join(__dirname, '..', 'accumulator.js'))})
    process.on('unhandledRejection', (e) => console.log('unhandledRejection: ' + e.message))
    process.on('uncaughtException', (e) => console.log('uncaughtException: ' + e.message))
    const accumulator = new Accumulator({
      linger: ${linger},
      batchSize: ${batchSize},
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: () => {
        throw new Error('send path failure')
      },
    })
    accumulator
      .append({ acks: -1, topicMessages: [{ topic: 't', messages: [{ value: 'v' }] }] })
      .then(
        () => console.log('append resolved'),
        (e) => console.log('append rejected: ' + e.message)
      )
    setTimeout(() => {}, 20)
  `
  const child = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8', timeout: 20000 })

  expect(child.stderr).toBe('')
  expect(child.status).toBe(0)
  return child.stdout.trim().split('\n')
}

/**
 * A producer over the fake cluster - one broker leading every partition, InitProducerId answered
 * with producer id 1000, transaction version `level` (2 unless given) - whose accumulator takes
 * its defaults unless `linger` or `batchSize` is given.
 */
const createLingeringProducer = ({ produce, partitions = 2, level = 2, ...options } = {}) => {
  const cluster = createFakeCluster({
    partitions,
    produce: (request) => {
      const refusal = request.guard?.(100)
      if (refusal) throw refusal
      return produce ? produce(request) : accepted(request)
    },
  })
  Object.assign(cluster.broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
    finalizedFeaturesEpoch: 1,
  })
  cluster.broker.endTxn.mockImplementation(async ({ producerEpoch }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  const producer = createProducer({
    cluster,
    logger: newLogger(),
    createPartitioner: () => () => 0,
    retry: { retries: 3, initialRetryTime: 10, maxRetryTime: 40, factor: 0 },
    ...options,
  })
  /** What each Produce request carried. */
  const produced = () =>
    cluster.broker.produce.mock.calls.map(([request]) => ({
      acks: request.acks,
      compression: request.compression,
      producerId: request.producerId,
      partitions: request.topicData.flatMap(({ partitions: written }) =>
        written.map(({ partition, firstSequence, messages }) => ({
          partition,
          firstSequence,
          values: messages.map(({ value }) => value),
        }))
      ),
    }))
  return { cluster, producer, produced }
}

/**
 * The partitions of a Produce v3-v7 request body, each with its record batch's bytes.
 * @see https://kafka.apache.org/43/design/protocol
 * @param {Buffer} body
 * @returns {{ topic: string, partition: number, batch: Buffer }[]}
 */
const partitionsOf = (body) => {
  const decoder = new Decoder(body)
  decoder.readString()
  decoder.readInt16()
  decoder.readInt32()
  return decoder
    .readArray((topics) => {
      const topic = topics.readString()
      return topics.readArray((entries) => ({
        topic,
        partition: entries.readInt32(),
        batch: entries.readBytes(),
      }))
    })
    .flat()
}

/**
 * A Produce v7 response body accepting every partition of a request at base offset 100.
 * @param {Buffer} body the request's body
 * @returns {Buffer}
 */
const acceptAll = (body) => {
  const partitions = partitionsOf(body)
  return new Encoder()
    .writeArray(
      [...new Set(partitions.map(({ topic }) => topic))].map((topic) =>
        new Encoder()
          .writeString(topic)
          .writeArray(
            partitions
              .filter((entry) => entry.topic === topic)
              .map(({ partition }) =>
                new Encoder()
                  .writeInt32(partition)
                  .writeInt16(0)
                  .writeInt64(100)
                  .writeInt64(-1)
                  .writeInt64(0)
              )
          )
      )
    )
    .writeInt32(0).buffer
}

describe('Producer > the accumulator', () => {
  afterEach(() => {
    jest.useRealTimers()
  })

  describe('coalescing', () => {
    beforeEach(() => {
      jest.useFakeTimers({ now: 0 })
    })

    test('the records of several sends to one partition go out together, in arrival order, once linger has elapsed', async () => {
      const { accumulator, sent } = createAccumulator()

      const first = append(accumulator, records(0, 'a', 2))
      const second = append(accumulator, records(0, 'b'))
      const third = append(accumulator, records(0, 'c'))
      await jest.advanceTimersByTimeAsync(LINGER - 1)
      expect(sent()).toEqual([])
      expect(jest.getTimerCount()).toBe(1)

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0', 'a-1', 'b-0', 'c-0'] }]])
      const results = await settledWithin(Promise.all([first, second, third]))
      expect(results.map(([{ baseOffset }]) => baseOffset)).toEqual(['100', '102', '103'])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('linger counts from a batch’s first records, not from its last', async () => {
      const { accumulator, sent } = createAccumulator()

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(LINGER - 2)
      append(accumulator, records(0, 'b'))
      await jest.advanceTimersByTimeAsync(2)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0', 'b-0'] }]])
    })

    test('each partition lingers on its own timer, and none is left once every batch went out', async () => {
      const { accumulator, sent } = createAccumulator()

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(3)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(2)
      await jest.advanceTimersByTimeAsync(2)
      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }]])

      await jest.advanceTimersByTimeAsync(3)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }], [{ [`${TOPIC}/1`]: ['b-0'] }]])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a batch that reaches batchSize goes out at once, before linger elapses', async () => {
      const { accumulator, sent } = createAccumulator({ batchSize: 6 })

      append(accumulator, [{ partition: 0, key: 'k', value: 'v1' }])
      append(accumulator, [{ partition: 0, value: 'v2-' }])
      await jest.advanceTimersByTimeAsync(0)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['v1', 'v2-'] }]])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('records that would take a batch past batchSize start the next one, after the pending one went out', async () => {
      const { accumulator, sent } = createAccumulator({ batchSize: 6 })

      append(accumulator, [{ partition: 0, value: 'aaaa' }])
      append(accumulator, [{ partition: 0, value: 'bbbb' }])
      await jest.advanceTimersByTimeAsync(0)
      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['aaaa'] }]])

      await jest.advanceTimersByTimeAsync(LINGER)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['aaaa'] }], [{ [`${TOPIC}/0`]: ['bbbb'] }]])
    })

    test('a send larger than batchSize goes out on its own and is never split', async () => {
      const { accumulator, sent } = createAccumulator({ batchSize: 6 })

      append(accumulator, [{ partition: 0, value: 'a' }])
      append(accumulator, records(0, 'large', 3))
      await jest.advanceTimersByTimeAsync(0)

      expect(sent()).toEqual([
        [{ [`${TOPIC}/0`]: ['a'] }],
        [{ [`${TOPIC}/0`]: ['large-0', 'large-1', 'large-2'] }],
      ])
    })

    test.each([
      ['acks', { acks: 1 }],
      ['compression', { compression: GZIP }],
      ['timeout', { timeout: 1000 }],
    ])(
      'a send with a different %s sends the pending batch first and starts a new one',
      async (_, options) => {
        const { accumulator, calls, sent } = createAccumulator()

        append(accumulator, records(0, 'a'))
        append(accumulator, records(0, 'b'), options)
        await jest.advanceTimersByTimeAsync(0)
        expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }]])

        await jest.advanceTimersByTimeAsync(LINGER)

        expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }], [{ [`${TOPIC}/0`]: ['b-0'] }]])
        expect(calls[1]).toEqual(
          expect.objectContaining({ acks: -1, timeout: 30000, compression: 0, ...options })
        )
      }
    )

    test('batches of different partitions that fall due together go out in one call', async () => {
      const { accumulator, calls } = createAccumulator()

      for (let partition = 0; partition < 20; partition++) {
        append(accumulator, records(partition, `p${partition}`))
      }
      await jest.advanceTimersByTimeAsync(LINGER)

      expect(calls).toHaveLength(1)
      expect(calls[0].batches).toHaveLength(20)
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a hand-off whose batches share their options is checked without a callback allocated per hand-off', async () => {
      const { accumulator, calls } = createAccumulator()
      for (let partition = 0; partition < 3; partition++) {
        append(accumulator, records(partition, `p${partition}`))
      }
      const every = jest.spyOn(Array.prototype, 'every')

      try {
        await jest.advanceTimersByTimeAsync(LINGER)

        expect(every).not.toHaveBeenCalled()
      } finally {
        every.mockRestore()
      }
      expect(calls).toHaveLength(1)
      expect(calls[0].batches).toHaveLength(3)
    })

    test('the batches one send creates fall due together, though the clock moves while it joins them', async () => {
      const { accumulator, calls } = createAccumulator()
      const now = Date.now
      let drift = 0
      let drifting = true
      const clock = jest
        .spyOn(Date, 'now')
        .mockImplementation(() => now() + (drifting ? drift++ : 0))

      append(accumulator, [...records(0, 'a'), ...records(1, 'b'), ...records(2, 'c')])
      await jest.advanceTimersByTimeAsync(0)
      drifting = false
      await jest.advanceTimersByTimeAsync(LINGER)
      clock.mockRestore()

      expect(calls).toHaveLength(1)
      expect(calls[0].batches.map(({ partition }) => partition)).toEqual([0, 1, 2])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('the batches one send fills go out in one call, after the older batch of their partition', async () => {
      const { accumulator, sent } = createAccumulator({ batchSize: 6 })

      append(accumulator, [{ partition: 0, value: 'a' }])
      append(accumulator, [
        { partition: 0, value: 'bbbbbb' },
        { partition: 1, value: 'cccccc' },
        { partition: 2, value: 'dddddd' },
      ])
      await jest.advanceTimersByTimeAsync(0)

      expect(sent()).toEqual([
        [{ [`${TOPIC}/0`]: ['a'] }],
        [
          { [`${TOPIC}/0`]: ['bbbbbb'] },
          { [`${TOPIC}/1`]: ['cccccc'] },
          { [`${TOPIC}/2`]: ['dddddd'] },
        ],
      ])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a send that fills a batch takes its other batches along, in the same call', async () => {
      const { accumulator, sent } = createAccumulator({ batchSize: 6 })

      append(accumulator, [{ partition: 0, value: 'a' }])
      append(accumulator, [
        { partition: 1, value: 'bbbbbb' },
        { partition: 2, value: 'c' },
      ])
      await jest.advanceTimersByTimeAsync(0)
      expect(sent()).toEqual([[{ [`${TOPIC}/1`]: ['bbbbbb'] }, { [`${TOPIC}/2`]: ['c'] }]])
      expect(jest.getTimerCount()).toBe(1)

      await jest.advanceTimersByTimeAsync(LINGER)

      expect(sent()).toEqual([
        [{ [`${TOPIC}/1`]: ['bbbbbb'] }, { [`${TOPIC}/2`]: ['c'] }],
        [{ [`${TOPIC}/0`]: ['a'] }],
      ])
      expect(jest.getTimerCount()).toBe(0)
    })

    describe('with linger: 0', () => {
      beforeEach(() => {
        jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask'] })
      })

      test('a send goes out at once, arming no timer', async () => {
        const { accumulator, sent } = createAccumulator({ linger: 0 })

        const sending = append(accumulator, records(0, 'a'))
        await microtasks()

        expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }]])
        expect(jest.getTimerCount()).toBe(0)
        await expect(sending).resolves.toEqual([expect.objectContaining({ baseOffset: '100' })])
      })

      test('sends made together to different partitions go out in one call', async () => {
        const { accumulator, sent } = createAccumulator({ linger: 0 })

        append(accumulator, records(0, 'a'))
        append(accumulator, records(1, 'b'))
        await microtasks()

        expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }, { [`${TOPIC}/1`]: ['b-0'] }]])
        expect(jest.getTimerCount()).toBe(0)
      })

      test('a hand-off whose batches share their options reaches the send path as one call, with no grouping', async () => {
        const { accumulator, calls, sent } = createAccumulator({ linger: 0 })
        // The single-send path is the one whose latency counts: it awaits no Promise.all.
        const all = jest.spyOn(Promise, 'all')

        try {
          const first = append(accumulator, records(0, 'a'))
          const second = append(accumulator, records(1, 'b'))
          await microtasks()

          expect(all).not.toHaveBeenCalled()
          await expect(first).resolves.toEqual([expect.objectContaining({ partition: 0 })])
          await expect(second).resolves.toEqual([expect.objectContaining({ partition: 1 })])
        } finally {
          all.mockRestore()
        }
        expect(calls).toHaveLength(1)
        expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }, { [`${TOPIC}/1`]: ['b-0'] }]])
      })

      test('flush() with nothing pending settles at once and calls the send path never', async () => {
        const { accumulator, calls } = createAccumulator({ linger: 0 })

        await expect(settledWithin(accumulator.flush())).resolves.toBeUndefined()

        expect(calls).toEqual([])
      })
    })

    describe('while holds() is true', () => {
      /** A send path whose first call is answered only when `answer` resolves. */
      const heldFirstCall = () => {
        const answer = deferred()
        const calls = []
        const sendPartitioned = jest.fn(async (request) => {
          calls.push(request)
          if (calls.length === 1) {
            await answer.promise
          }
          request.batches.forEach(({ partition, settlers }) =>
            settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
          )
        })
        const values = () =>
          calls.map(({ batches }) =>
            batches.flatMap(({ messages }) => messages.map(({ value }) => value))
          )
        return { answer, sendPartitioned, values }
      }

      test('a due batch keeps collecting while its partition has a batch in flight, and goes out when that batch is answered', async () => {
        const { answer, sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({ sendPartitioned, holds: () => true })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(LINGER * 3)
        append(accumulator, records(0, 'c'))
        await jest.advanceTimersByTimeAsync(0)
        expect(values()).toEqual([['a-0']])
        expect(jest.getTimerCount()).toBe(0)

        answer.resolve()
        await jest.advanceTimersByTimeAsync(0)

        expect(values()).toEqual([['a-0'], ['b-0', 'c-0']])
      })

      test('with linger: 0, the sends behind a partition’s batch in flight merge into one batch, arming no timer', async () => {
        jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask'] })
        const { answer, sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          linger: 0,
          sendPartitioned,
          holds: () => true,
        })

        append(accumulator, records(0, 'a'))
        await microtasks()
        append(accumulator, records(0, 'b'))
        await microtasks()
        append(accumulator, records(0, 'c'))
        await microtasks()
        expect(values()).toEqual([['a-0']])
        expect(jest.getTimerCount()).toBe(0)

        answer.resolve()
        await microtasks()

        expect(values()).toEqual([['a-0'], ['b-0', 'c-0']])
        expect(jest.getTimerCount()).toBe(0)
      })

      test('a held batch that reaches batchSize goes out at once, behind the one in flight', async () => {
        const { sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          sendPartitioned,
          holds: () => true,
          batchSize: 6,
        })

        append(accumulator, [{ partition: 0, value: 'a' }])
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, [{ partition: 0, value: 'bbb' }])
        await jest.advanceTimersByTimeAsync(LINGER * 2)
        append(accumulator, [{ partition: 0, value: 'ccc' }])
        await jest.advanceTimersByTimeAsync(0)

        expect(values()).toEqual([['a'], ['bbb', 'ccc']])
      })

      test('a partition with no batch in flight is not held', async () => {
        const { sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({ sendPartitioned, holds: () => true })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(1, 'b'))
        await jest.advanceTimersByTimeAsync(LINGER)

        expect(values()).toEqual([['a-0'], ['b-0']])
      })

      test('a held batch with a delivery deadline goes on at that deadline, its partition’s batch still in flight', async () => {
        const { sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          sendPartitioned,
          holds: () => true,
          deadline: 50,
        })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(49)
        expect(values()).toEqual([['a-0']])

        await jest.advanceTimersByTimeAsync(1)

        expect(values()).toEqual([['a-0'], ['b-0']])
        expect(sendPartitioned.mock.calls[1][0].batches[0].createdAt).toBe(LINGER + 1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('a held batch an older send joins goes on at that send’s deadline', async () => {
        const { sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          sendPartitioned,
          holds: () => true,
          deadline: 50,
        })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'c'), { startedAt: 0 })
        await jest.advanceTimersByTimeAsync(37)
        expect(values()).toEqual([['a-0']])
        await jest.advanceTimersByTimeAsync(1)

        expect(values()).toEqual([['a-0'], ['b-0', 'c-0']])
        expect(jest.getTimerCount()).toBe(0)
      })

      test('a hand-off that fails after its batches settled does not release a held batch early', async () => {
        const answer = deferred()
        const calls = []
        const sendPartitioned = jest.fn(async (request) => {
          calls.push(request)
          if (calls.length === 1) {
            await answer.promise
          }
          request.batches.forEach(({ partition, settlers }) =>
            settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
          )
          if (calls.length === 2) {
            throw new KafkaJSError('after the fact')
          }
        })
        const values = () =>
          calls.map(({ batches }) =>
            batches.flatMap(({ messages }) => messages.map(({ value }) => value))
          )
        const { accumulator } = createAccumulator({
          sendPartitioned,
          holds: () => true,
          batchSize: 6,
        })

        append(accumulator, [{ partition: 0, value: 'a' }])
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, [{ partition: 0, value: 'bbb' }])
        await jest.advanceTimersByTimeAsync(LINGER * 2)
        append(accumulator, [{ partition: 0, value: 'cccc' }])
        await jest.advanceTimersByTimeAsync(LINGER * 2)
        expect(values()).toEqual([['a'], ['bbb']])

        answer.resolve()
        await jest.advanceTimersByTimeAsync(0)

        expect(values()).toEqual([['a'], ['bbb'], ['cccc']])
      })

      test('the partitions one answer releases go out together', async () => {
        const { answer, sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({ sendPartitioned, holds: () => true })

        append(accumulator, [...records(0, 'a'), ...records(1, 'b')])
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, [...records(0, 'c'), ...records(1, 'd')])
        await jest.advanceTimersByTimeAsync(LINGER * 2)
        expect(values()).toEqual([['a-0', 'b-0']])

        answer.resolve()
        await jest.advanceTimersByTimeAsync(0)

        expect(values()).toEqual([
          ['a-0', 'b-0'],
          ['c-0', 'd-0'],
        ])
      })

      test('a partition held twice in a row goes out each time the batch before it is answered', async () => {
        const answers = []
        const calls = []
        const sendPartitioned = jest.fn(async (request) => {
          calls.push(request)
          const answer = deferred()
          answers.push(answer.resolve)
          await answer.promise
          request.batches.forEach(({ partition, settlers }) =>
            settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
          )
        })
        const values = () =>
          calls.map(({ batches }) =>
            batches.flatMap(({ messages }) => messages.map(({ value }) => value))
          )
        const { accumulator } = createAccumulator({ sendPartitioned, holds: () => true })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        answers[0]()
        await jest.advanceTimersByTimeAsync(0)
        append(accumulator, records(0, 'c'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        expect(values()).toEqual([['a-0'], ['b-0']])

        answers[1]()
        await jest.advanceTimersByTimeAsync(0)

        expect(values()).toEqual([['a-0'], ['b-0'], ['c-0']])
        answers[2]()
      })

      test('a held batch that goes out before its delivery deadline leaves no timer and goes out once', async () => {
        const { answer, sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          sendPartitioned,
          holds: () => true,
          deadline: 50,
        })

        append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
        expect(jest.getTimerCount()).toBe(1)
        answer.resolve()
        await jest.advanceTimersByTimeAsync(0)
        expect(jest.getTimerCount()).toBe(0)

        append(accumulator, records(0, 'c'))
        await jest.advanceTimersByTimeAsync(50)

        expect(values()).toEqual([['a-0'], ['b-0'], ['c-0']])
      })
    })

    test('while holds() is false a due batch goes out even with its partition’s batch in flight', async () => {
      const answer = deferred()
      const calls = []
      const { accumulator } = createAccumulator({
        sendPartitioned: jest.fn(async (request) => {
          calls.push(request)
          await answer.promise
        }),
      })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(LINGER + 1)
      append(accumulator, records(0, 'b'))
      await jest.advanceTimersByTimeAsync(LINGER)

      expect(calls).toHaveLength(2)
      answer.resolve()
    })

    test('a batch that forms while lingers() is false goes out at once', async () => {
      let lingers = true
      const { accumulator, sent } = createAccumulator({ lingers: () => lingers })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(0)
      lingers = false
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(0)

      expect(sent()).toEqual([[{ [`${TOPIC}/1`]: ['b-0'] }]])
      expect(jest.getTimerCount()).toBe(1)
    })

    test('flush() settles only once the call of every set of options has settled', async () => {
      const gzipAnswered = deferred()
      const sendPartitioned = jest.fn(async ({ compression, batches }) => {
        if (compression === GZIP) {
          await gzipAnswered.promise
        }
        batches.forEach(({ partition, settlers }) =>
          settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
        )
      })
      const { accumulator } = createAccumulator({ sendPartitioned })
      append(accumulator, records(0, 'a'))
      accumulator.append({
        acks: -1,
        timeout: 30000,
        compression: GZIP,
        topicMessages: [{ topic: OTHER_TOPIC, messages: records(0, 'c') }],
      })
      await jest.advanceTimersByTimeAsync(0)

      const flushed = settledFlag(accumulator.flush())
      await jest.advanceTimersByTimeAsync(0)

      expect(sendPartitioned).toHaveBeenCalledTimes(2)
      expect(flushed()).toBe(false)
      gzipAnswered.resolve()
      await jest.advanceTimersByTimeAsync(0)
      expect(flushed()).toBe(true)
    })

    test('flush() sends every pending batch now, one call per options, and clears every timer', async () => {
      const { accumulator, sent } = createAccumulator()
      const sends = [
        append(accumulator, records(0, 'a')),
        append(accumulator, records(1, 'b')),
        accumulator.append({
          acks: -1,
          timeout: 30000,
          compression: GZIP,
          topicMessages: [{ topic: OTHER_TOPIC, messages: records(0, 'c') }],
        }),
      ]
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(3)

      const flushed = settledFlag(accumulator.flush())

      expect(sent()).toEqual([
        [{ [`${TOPIC}/0`]: ['a-0'] }, { [`${TOPIC}/1`]: ['b-0'] }],
        [{ [`${OTHER_TOPIC}/0`]: ['c-0'] }],
      ])
      expect(jest.getTimerCount()).toBe(0)
      await settledWithin(Promise.all(sends))
      await jest.advanceTimersByTimeAsync(0)
      expect(flushed()).toBe(true)
    })

    test('a batch’s delivery deadline counts from the earliest send in it', async () => {
      const { accumulator, calls } = createAccumulator({ linger: 10 })

      await jest.advanceTimersByTimeAsync(2)
      append(accumulator, records(0, 'b'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(0, 'a'), { startedAt: 0 })
      await jest.advanceTimersByTimeAsync(10)

      expect(calls).toHaveLength(1)
      expect(calls[0].batches[0]).toEqual(
        expect.objectContaining({
          messages: [
            { partition: 0, value: 'b-0' },
            { partition: 0, value: 'a-0' },
          ],
          createdAt: 0,
        })
      )
    })

    describe('an older send that joins a lingering batch late', () => {
      test('brings the batch’s timer forward to its own delivery deadline', async () => {
        const { accumulator, calls } = createAccumulator({ linger: 100, deadline: 80 })

        await jest.advanceTimersByTimeAsync(20)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(30)
        append(accumulator, records(0, 'a'), { startedAt: 0 })
        await jest.advanceTimersByTimeAsync(29)
        expect(calls).toHaveLength(0)
        await jest.advanceTimersByTimeAsync(1)

        expect(calls).toHaveLength(1)
        expect(calls[0].batches[0].createdAt).toBe(0)
      })

      test('does not restart the batch’s linger', async () => {
        const { accumulator, calls } = createAccumulator({ linger: 50, deadline: 1000 })

        await jest.advanceTimersByTimeAsync(20)
        append(accumulator, records(0, 'b'))
        await jest.advanceTimersByTimeAsync(10)
        append(accumulator, records(0, 'a'), { startedAt: 0 })
        await jest.advanceTimersByTimeAsync(39)
        expect(calls).toHaveLength(0)
        await jest.advanceTimersByTimeAsync(1)

        expect(calls).toHaveLength(1)
        expect(jest.getTimerCount()).toBe(0)
      })
    })
  })

  describe('merging the batches a broker has waiting', () => {
    const MERGE_LINGER = 50

    beforeEach(() => {
      jest.useFakeTimers({ now: 0 })
    })

    /** A `leaderOf` over `leaders[topic][partition]`; a partition it does not list has no leader. */
    const ledBy = (leaders) => jest.fn((topic, partition) => leaders[topic]?.[partition])

    /** The same for the one topic most cases use. */
    const ledByOne = (leaders) => ledBy({ [TOPIC]: leaders })

    /** One batch of a send, as `sent()` shows it. */
    const batchOf = (partition, value, topic = TOPIC) => ({ [`${topic}/${partition}`]: [value] })

    /** A send of one record to a partition of any topic. */
    const appendTo = (accumulator, topic, partition, value) =>
      accumulator.append({
        acks: -1,
        timeout: 30000,
        compression: 0,
        topicMessages: [{ topic, messages: records(partition, value) }],
      })

    /** A send path whose first call is answered only when `answer` resolves. */
    const heldFirstCall = () => {
      const answer = deferred()
      const calls = []
      const sendPartitioned = jest.fn(async (request) => {
        calls.push(request)
        if (calls.length === 1) {
          await answer.promise
        }
        request.batches.forEach(({ partition, settlers }) =>
          settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
        )
      })
      const values = () =>
        calls.map(({ batches }) => batches.map(({ messages }) => messages[0].value))
      return { answer, sendPartitioned, values }
    }

    test('batches of partitions one broker leads, made 1 ms apart, leave in one hand-off and leave no timer', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 1 }),
      })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 2)
      expect(sent()).toEqual([])

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0'), batchOf(1, 'b-0')]])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a batch for another broker keeps its own timer', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 2 }),
      })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toEqual([[batchOf(0, 'a-0')]])
      expect(jest.getTimerCount()).toBe(1)

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])
    })

    test.each([
      ['acks', { acks: 1 }],
      ['timeout', { timeout: 15000 }],
      ['compression', { compression: GZIP }],
    ])('batches that ask for a different %s never merge', async (_, options) => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 1 }),
      })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'), options)
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toEqual([[batchOf(0, 'a-0')]])

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])
    })

    test('six partitions on three brokers, sent 1 ms apart, leave in three hand-offs', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 2, 2: 3, 3: 1, 4: 2, 5: 3 }),
      })

      for (let partition = 0; partition < 6; partition += 1) {
        append(accumulator, records(partition, `p${partition}`))
        await jest.advanceTimersByTimeAsync(1)
      }
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)

      expect(sent()).toEqual([
        [batchOf(0, 'p0-0'), batchOf(3, 'p3-0')],
        [batchOf(1, 'p1-0'), batchOf(4, 'p4-0')],
        [batchOf(2, 'p2-0'), batchOf(5, 'p5-0')],
      ])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a batch whose linger has run out when another expires takes its broker’s waiting batches too', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 2, 2: 2 }),
      })

      // a and b fall due together, as timers that fire late make them: b's partition is the one
      // c's broker leads, and c is a millisecond short of its own linger
      append(accumulator, records(0, 'a'))
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(2, 'c'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 2)
      expect(sent()).toEqual([])

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0'), batchOf(1, 'b-0'), batchOf(2, 'c-0')]])
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a batch whose linger has run out takes only the batches that ask for its own options', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 2, 2: 2 }),
      })

      append(accumulator, records(0, 'a'))
      append(accumulator, records(1, 'b'), { acks: 1 })
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(2, 'c'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')], [batchOf(2, 'c-0')]])
    })

    test('a held batch whose linger has run out when another expires takes no other batch with it', async () => {
      const { answer, sendPartitioned, values } = heldFirstCall()
      const { accumulator } = createAccumulator({
        linger: MERGE_LINGER,
        sendPartitioned,
        holds: () => true,
        leaderOf: ledByOne({ 0: 2, 1: 1, 2: 1 }),
      })
      append(accumulator, records(1, 'x'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)
      expect(values()).toEqual([['x-0']])

      // w and y fall due together; y is held behind x, and z, led by y's broker, is not due
      append(accumulator, records(0, 'w'))
      append(accumulator, records(1, 'y'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(2, 'z'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(values()).toEqual([['x-0'], ['w-0']])

      await jest.advanceTimersByTimeAsync(1)
      expect(values()).toEqual([['x-0'], ['w-0'], ['z-0']])

      answer.resolve()
      await jest.advanceTimersByTimeAsync(0)
      expect(values()).toEqual([['x-0'], ['w-0'], ['z-0'], ['y-0']])
    })

    test('a merged partition starts a fresh batch on its own linger', async () => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne({ 0: 1, 1: 1 }),
      })
      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toHaveLength(1)

      append(accumulator, records(1, 'c'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toHaveLength(1)
      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0'), batchOf(1, 'b-0')], [batchOf(1, 'c-0')]])
    })

    test('without a leaderOf, batches never merge', async () => {
      const { accumulator, sent } = createAccumulator({ linger: MERGE_LINGER })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)

      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])
    })

    test('batches of two topics one broker leads leave together, and a batch of another broker stays', async () => {
      const leaderOf = ledBy({ [TOPIC]: { 0: 1, 1: 1 }, [OTHER_TOPIC]: { 0: 1, 1: 2 } })
      const { accumulator, sent } = createAccumulator({ linger: MERGE_LINGER, leaderOf })

      appendTo(accumulator, TOPIC, 0, 'a')
      await jest.advanceTimersByTimeAsync(1)
      appendTo(accumulator, OTHER_TOPIC, 0, 'b')
      await jest.advanceTimersByTimeAsync(1)
      appendTo(accumulator, OTHER_TOPIC, 1, 'c')
      await jest.advanceTimersByTimeAsync(1)
      appendTo(accumulator, TOPIC, 1, 'd')
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 3)

      expect(sent()).toEqual([
        [batchOf(0, 'a-0'), batchOf(0, 'b-0', OTHER_TOPIC), batchOf(1, 'd-0')],
      ])
      expect(leaderOf.mock.calls).toEqual([
        [TOPIC, 0],
        [OTHER_TOPIC, 0],
        [OTHER_TOPIC, 1],
        [TOPIC, 1],
      ])

      await jest.advanceTimersByTimeAsync(2)

      expect(sent()[1]).toEqual([batchOf(1, 'c-0', OTHER_TOPIC)])
    })

    test('the leader of a waiting batch is read once, when its timer is armed, and the merge goes by that answer', async () => {
      const leaders = { 0: 1, 1: 1 }
      const leaderOf = jest.fn((topic, partition) => leaders[partition])
      const { accumulator, sent } = createAccumulator({ linger: MERGE_LINGER, leaderOf })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      append(accumulator, records(1, 'c'))
      await microtasks()
      leaders[1] = 2
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)

      expect(leaderOf.mock.calls).toEqual([
        [TOPIC, 0],
        [TOPIC, 1],
      ])
      expect(sent()).toEqual([[batchOf(0, 'a-0'), { [`${TOPIC}/1`]: ['b-0', 'c-0'] }]])
    })

    test.each([
      ['linger is 0', { linger: 0 }],
      ['the batch is full when it forms', { linger: MERGE_LINGER, batchSize: 1 }],
      ['lingers() is false', { linger: MERGE_LINGER, lingers: () => false }],
    ])('a batch that does not wait for its linger reads no leader when %s', async (_, options) => {
      const leaderOf = ledByOne({ 0: 1 })
      const { accumulator, sent } = createAccumulator({ ...options, leaderOf })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)

      expect(sent()).toEqual([[batchOf(0, 'a-0')]])
      expect(leaderOf).not.toHaveBeenCalled()
    })

    test('a batch held behind its partition’s batch in flight is neither handed on nor made due', async () => {
      const { answer, sendPartitioned, values } = heldFirstCall()
      const { accumulator } = createAccumulator({
        linger: MERGE_LINGER,
        sendPartitioned,
        holds: () => true,
        leaderOf: ledByOne({ 0: 1, 1: 1 }),
      })
      append(accumulator, records(1, 'x'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)
      expect(values()).toEqual([['x-0']])

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'y'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)

      // partition 0's linger ran out: partition 1 is led by the same broker but is held
      expect(values()).toEqual([['x-0'], ['a-0']])
      expect(jest.getTimerCount()).toBe(1)

      answer.resolve()
      await jest.advanceTimersByTimeAsync(1)

      expect(values()).toEqual([['x-0'], ['a-0'], ['y-0']])
    })

    test('a held batch whose linger runs out does not send its broker’s other batches early', async () => {
      const { answer, sendPartitioned, values } = heldFirstCall()
      const { accumulator } = createAccumulator({
        linger: MERGE_LINGER,
        sendPartitioned,
        holds: () => true,
        leaderOf: ledByOne({ 0: 1, 1: 1 }),
      })
      append(accumulator, records(1, 'x'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER)
      append(accumulator, records(1, 'y'))
      await jest.advanceTimersByTimeAsync(10)
      append(accumulator, records(0, 'z'))

      // y's linger runs out at 100 while x is still in flight: a muted partition makes nothing ready
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 10 + 5)
      expect(values()).toEqual([['x-0']])

      await jest.advanceTimersByTimeAsync(MERGE_LINGER)
      expect(values()).toEqual([['x-0'], ['z-0']])

      answer.resolve()
      await jest.advanceTimersByTimeAsync(0)
      expect(values()).toEqual([['x-0'], ['z-0'], ['y-0']])
    })

    test.each([
      ['the batch behind the one in flight is appended first', ['y', 'z']],
      ['the batch behind the one in flight is appended second', ['z', 'y']],
    ])(
      'with holds() false, a batch behind one in flight is not held and merges when %s',
      async (_, order) => {
        const { answer, sendPartitioned, values } = heldFirstCall()
        const { accumulator } = createAccumulator({
          linger: MERGE_LINGER,
          sendPartitioned,
          holds: () => false,
          leaderOf: ledByOne({ 0: 1, 1: 1 }),
        })
        append(accumulator, records(1, 'x'))
        await jest.advanceTimersByTimeAsync(MERGE_LINGER)
        for (const name of order) {
          append(accumulator, records(name === 'y' ? 1 : 0, name))
          await jest.advanceTimersByTimeAsync(1)
        }

        await jest.advanceTimersByTimeAsync(MERGE_LINGER - 2)

        expect(values()).toEqual([['x-0'], order.map((name) => `${name}-0`)])
        answer.resolve()
      }
    )

    test.each([
      ['a partition whose leader is unknown', { 0: 1, 1: null }],
      ['a partition whose leader is not reported', { 0: 1 }],
      ['an expired batch whose leader is unknown', { 0: null, 1: 1 }],
      ['two partitions with no leader, which is -1 for both', { 0: -1, 1: -1 }],
      ['a leader that is not a node id', { 0: '1', 1: '1' }],
      ['a fractional leader', { 0: 1.5, 1: 1.5 }],
      ['a NaN leader', { 0: NaN, 1: NaN }],
      ['an infinite leader', { 0: Infinity, 1: Infinity }],
    ])('%s neither merges nor takes the other batch with it', async (_, leaders) => {
      const { accumulator, sent } = createAccumulator({
        linger: MERGE_LINGER,
        leaderOf: ledByOne(leaders),
      })

      append(accumulator, records(0, 'a'))
      await jest.advanceTimersByTimeAsync(1)
      append(accumulator, records(1, 'b'))
      await jest.advanceTimersByTimeAsync(MERGE_LINGER - 1)
      expect(sent()).toEqual([[batchOf(0, 'a-0')]])

      await jest.advanceTimersByTimeAsync(1)

      expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])
    })

    test.each([
      ['the first batch', (partition) => partition === 0],
      ['the second batch', (partition) => partition === 1],
      ['every batch', () => true],
    ])(
      'a leaderOf that throws for %s loses no send and leaves each batch on its own linger',
      async (_, throwsFor) => {
        const leaderOf = jest.fn((topic, partition) => {
          if (throwsFor(partition)) {
            throw new Error('metadata not loaded')
          }
          return 1
        })
        const { accumulator, sent } = createAccumulator({ linger: MERGE_LINGER, leaderOf })

        const first = append(accumulator, records(0, 'a'))
        await jest.advanceTimersByTimeAsync(1)
        const second = append(accumulator, records(1, 'b'))
        await jest.advanceTimersByTimeAsync(MERGE_LINGER)

        expect(sent()).toEqual([[batchOf(0, 'a-0')], [batchOf(1, 'b-0')]])
        await expect(Promise.all([first, second])).resolves.toHaveLength(2)
      }
    )
  })

  describe('settling the sends', () => {
    beforeEach(() => {
      jest.useFakeTimers({ now: 0 })
    })

    test.each([
      ['topics listed out of name order', [OTHER_TOPIC, TOPIC]],
      ['topics listed in name order', [TOPIC, OTHER_TOPIC]],
    ])(
      'a send over several partitions resolves once all of them landed, by topic and then partition, %s',
      async (_, topics) => {
        const { accumulator } = createAccumulator()

        const sent = accumulator.append({
          acks: -1,
          topicMessages: topics.map((topic) => ({
            topic,
            messages: [...records(1, 'a'), ...records(0, 'b')],
          })),
        })
        const metadata = await settledWithin(sent, LINGER)

        expect(metadata.map(({ topicName, partition }) => `${topicName}/${partition}`)).toEqual([
          `${TOPIC}/0`,
          `${TOPIC}/1`,
          `${OTHER_TOPIC}/0`,
          `${OTHER_TOPIC}/1`,
        ])
      }
    )

    test('a send rejects with the first error a batch carrying it failed with, after every one has settled', async () => {
      const failures = [new KafkaJSError('first'), new KafkaJSError('second')]
      const later = deferred()
      const sendPartitioned = jest.fn(async ({ batches }) => {
        for (const { partition, settlers } of batches) {
          if (partition !== 2) {
            settlers.forEach((settler) => settler.reject(failures[partition]))
          }
        }
        await later.promise
        for (const { partition, settlers } of batches) {
          if (partition === 2) {
            settlers.forEach((settler) => settler.resolve({ partition: 2, baseOffset: '0' }))
          }
        }
      })
      const { accumulator } = createAccumulator({ sendPartitioned })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b'), ...records(2, 'c')])
      const settled = settledFlag(sent)
      await jest.advanceTimersByTimeAsync(LINGER)
      expect(settled()).toBe(false)

      later.resolve()

      await expect(settledWithin(sent)).rejects.toBe(failures[0])
    })

    /**
     * A send path that settles each batch by its partition: `resolve` partitions get metadata
     * (undefined for `acks: 0`), the others are rejected with `errors[partition]`.
     */
    const settleBy =
      ({ resolve = [], errors = {} }) =>
      async ({ acks, batches }) => {
        for (const { partition, settlers } of batches) {
          const metadata = acks === 0 ? undefined : { partition, baseOffset: '0' }
          for (const settler of settlers) {
            if (resolve.includes(partition)) settler.resolve(metadata)
            else settler.reject(errors[partition])
          }
        }
      }

    test('a send with one acknowledged batch and one never-written batch rejects with outcome unknown and the batch error as cause', async () => {
      const unsent = new KafkaJSDeliveryTimeout('deadline', { outcome: 'unsent' })
      const { accumulator } = createAccumulator({
        sendPartitioned: jest.fn(settleBy({ resolve: [0], errors: { 1: unsent } })),
      })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b')])
      const error = await settledWithin(sent, LINGER).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error).not.toBe(unsent)
      expect(error).toMatchObject({
        name: 'KafkaJSDeliveryTimeout',
        outcome: 'unknown',
        message: 'Some records of this send may have been written: deadline',
      })
      expect(error.cause).toBe(unsent)
      expect(unsent.outcome).toBe('unsent')
    })

    test('a send whose every batch failed unsent keeps outcome unsent', async () => {
      const errors = {
        0: new KafkaJSDeliveryTimeout('first', { outcome: 'unsent' }),
        1: new KafkaJSDeliveryTimeout('second', { outcome: 'unsent' }),
      }
      const { accumulator } = createAccumulator({ sendPartitioned: jest.fn(settleBy({ errors })) })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b')])

      await expect(settledWithin(sent, LINGER)).rejects.toBe(errors[0])
      expect(errors[0].outcome).toBe('unsent')
    })

    test('two sends sharing a never-written batch: only the one with an acknowledged batch gets a relabelled error, and the batch error is unchanged', async () => {
      const unsent = new KafkaJSDeliveryTimeout('deadline', { outcome: 'unsent' })
      const settle = settleBy({ resolve: [1], errors: { 0: unsent } })
      const shared = []
      const sendPartitioned = jest.fn((request) => {
        for (const { partition, messages } of request.batches) {
          if (partition === 0) shared.push(messages.map(({ value }) => value))
        }
        return settle(request)
      })
      const { accumulator } = createAccumulator({ sendPartitioned })

      const written = append(accumulator, [...records(0, 'a'), ...records(1, 'b')])
      const unwritten = append(accumulator, records(0, 'c'))
      const [partly, never] = await settledWithin(
        Promise.all([written.catch((e) => e), unwritten.catch((e) => e)]),
        LINGER
      )

      expect(shared).toEqual([['a-0', 'c-0']])
      expect(partly).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(partly).toMatchObject({ outcome: 'unknown' })
      expect(partly.cause).toBe(unsent)
      expect(never).toBe(unsent)
      expect(unsent.outcome).toBe('unsent')
    })

    test('an acks 0 batch counts as written', async () => {
      const unsent = new KafkaJSDeliveryTimeout('deadline', { outcome: 'unsent' })
      const { accumulator } = createAccumulator({
        sendPartitioned: jest.fn(settleBy({ resolve: [0], errors: { 1: unsent } })),
      })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b')], { acks: 0 })
      const error = await settledWithin(sent, LINGER).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error).toMatchObject({ outcome: 'unknown' })
      expect(error.cause).toBe(unsent)
    })

    test('a send whose only failed batch has no relabel rejects with that batch error object itself', async () => {
      const refused = new KafkaJSNonRetriableError('refused')
      const { accumulator } = createAccumulator({
        sendPartitioned: jest.fn(settleBy({ resolve: [0], errors: { 1: refused } })),
      })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b')])

      await expect(settledWithin(sent, LINGER)).rejects.toBe(refused)
      expect('outcome' in refused).toBe(false)
    })

    test('a send is settled once, whatever its batch’s settlers are told afterwards', async () => {
      const later = deferred()
      const sendPartitioned = jest.fn(async ({ batches }) => {
        for (const { partition, settlers } of batches) {
          if (partition === 0) {
            settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '7' }))
            settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '8' }))
            settlers.forEach((settler) => settler.reject(new KafkaJSError('after the fact')))
          }
        }
        await later.promise
        for (const { partition, settlers } of batches) {
          if (partition === 1) {
            settlers.forEach((settler) => settler.resolve({ partition: 1, baseOffset: '9' }))
          }
        }
      })
      const { accumulator } = createAccumulator({ sendPartitioned })

      const sent = append(accumulator, [...records(0, 'a'), ...records(1, 'b')])
      const settled = settledFlag(sent)
      await jest.advanceTimersByTimeAsync(LINGER)
      expect(settled()).toBe(false)

      later.resolve()

      await expect(settledWithin(sent)).resolves.toEqual([
        { partition: 0, baseOffset: '7' },
        { partition: 1, baseOffset: '9' },
      ])
    })

    test('a call that failed before settling any batch rejects every send it carried', async () => {
      const refused = new KafkaJSNonRetriableError('refused')
      const { accumulator } = createAccumulator({
        sendPartitioned: jest.fn(async () => {
          throw refused
        }),
      })

      const sends = [append(accumulator, records(0, 'a')), append(accumulator, records(0, 'b'))]
      const errors = Promise.all(sends.map((sent) => sent.catch((e) => e)))

      expect(await settledWithin(errors, LINGER)).toEqual([refused, refused])
    })

    test('metadata with no known offset passes unchanged: none for acks: 0, -1 for a duplicate, or absent', async () => {
      const answers = [undefined, { partition: 1, baseOffset: '-1' }, { partition: 2 }]
      const sendPartitioned = jest.fn(async ({ batches }) =>
        batches.forEach(({ partition, settlers }) =>
          settlers.forEach((settler) => settler.resolve(answers[partition]))
        )
      )
      const { accumulator } = createAccumulator({ sendPartitioned })

      const sends = [0, 1, 2].map((partition) => [
        append(accumulator, records(partition, 'a')),
        append(accumulator, records(partition, 'b')),
      ])
      const results = await settledWithin(
        Promise.all(sends.map((pair) => Promise.all(pair))),
        LINGER
      )

      expect(results).toEqual([
        [[], []],
        [[answers[1]], [answers[1]]],
        [[answers[2]], [answers[2]]],
      ])
    })

    test('an empty send resolves [] at once, assigns no partition and schedules nothing', async () => {
      const { accumulator, partition } = createAccumulator()

      await expect(append(accumulator, [])).resolves.toEqual([])
      await expect(accumulator.append({ acks: -1, topicMessages: [] })).resolves.toEqual([])
      expect(partition).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a send whose last topic carries no record still sends the records of the others', async () => {
      const { accumulator, sent } = createAccumulator()

      const sending = accumulator.append({
        acks: -1,
        timeout: 30000,
        compression: 0,
        topicMessages: [
          { topic: TOPIC, messages: records(0, 'a') },
          { topic: OTHER_TOPIC, messages: [] },
        ],
      })
      await jest.advanceTimersByTimeAsync(LINGER)

      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }]])
      await expect(sending).resolves.toEqual([expect.objectContaining({ topicName: TOPIC })])
    })
  })

  describe('a record the encoder cannot write', () => {
    test.each([
      ['a numeric key', { key: 1, value: 'v' }, 'key'],
      ['an object value', { value: { a: 1 } }, 'value'],
      ['a typed-array value', { value: new Uint8Array([1, 2, 3]) }, 'value'],
      ['a numeric header', { value: 'v', headers: { h: 1 } }, 'headers'],
      ['a null header', { value: 'v', headers: { h: null } }, 'headers'],
      ['a numeric header in an array', { value: 'v', headers: { h: ['a', 2] } }, 'headers'],
      ['null headers', { value: 'v', headers: null }, 'headers'],
      ['a fractional timestamp', { value: 'v', timestamp: 1.5 }, 'timestamp'],
      ['a fractional string timestamp', { value: 'v', timestamp: '1.0' }, 'timestamp'],
      ['an exponent timestamp', { value: 'v', timestamp: '1e3' }, 'timestamp'],
      ['an empty timestamp', { value: 'v', timestamp: '' }, 'timestamp'],
      ['an object timestamp', { value: 'v', timestamp: {} }, 'timestamp'],
      ['a NaN timestamp', { value: 'v', timestamp: NaN }, 'timestamp'],
    ])(
      '%s fails its send before anything is queued, as a configuration error',
      async (_, message, field) => {
        const { accumulator, partition } = createAccumulator()

        const error = await append(accumulator, [{ partition: 0, ...message }]).catch((e) => e)

        expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
        expect(error.category).toBe('invalidConfiguration')
        expect(error.message).toMatch(`Invalid message for topic "${TOPIC}": its ${field} must be`)
        expect(partition).not.toHaveBeenCalled()
      }
    )

    test('fails its own send only: the batch it would have joined still goes out', async () => {
      jest.useFakeTimers({ now: 0 })
      const { accumulator, sent } = createAccumulator()

      const good = append(accumulator, records(0, 'a'))
      const bad = append(accumulator, [{ partition: 0, value: 1 }]).catch((e) => e)

      expect(await settledWithin(bad, LINGER)).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      await expect(settledWithin(good)).resolves.toEqual([
        expect.objectContaining({ baseOffset: '100' }),
      ])
      expect(sent()).toEqual([[{ [`${TOPIC}/0`]: ['a-0'] }]])
    })

    test('a send whose records are gone by the end of the check resolves [] at once, queueing nothing', async () => {
      jest.useFakeTimers({ now: 0 })
      const { accumulator, calls, partition } = createAccumulator()
      const first = [{ partition: 0, value: 'a' }]
      const last = [
        {
          partition: 0,
          get value() {
            first.length = 0
            last.length = 0
            return 'b'
          },
        },
      ]

      const sending = accumulator.append({
        acks: -1,
        topicMessages: [
          { topic: TOPIC, messages: first },
          { topic: OTHER_TOPIC, messages: last },
        ],
      })

      await expect(settledWithin(sending)).resolves.toEqual([])
      expect(partition).not.toHaveBeenCalled()
      expect(calls).toEqual([])
    })

    test.each([
      [
        'a topic deleted by the read of the one before it is skipped',
        (topicMessages) => {
          delete topicMessages[1]
        },
      ],
      [
        'a topic appended by the read of the one before it is not read',
        (topicMessages) => {
          topicMessages.push({ topic: OTHER_TOPIC, messages: records(0, 'late') })
        },
      ],
    ])(
      'the check for records: %s, so the send resolves [] at once, queueing nothing',
      async (_, change) => {
        jest.useFakeTimers({ now: 0 })
        const { accumulator, calls, partition } = createAccumulator()
        let reads = 0
        const topicMessages = [
          {
            topic: TOPIC,
            get messages() {
              if (++reads === 2) {
                change(topicMessages)
              }
              return []
            },
          },
          { topic: OTHER_TOPIC, messages: [] },
        ]

        const sending = accumulator.append({ acks: -1, topicMessages })

        await expect(settledWithin(sending)).resolves.toEqual([])
        expect(partition).not.toHaveBeenCalled()
        expect(calls).toEqual([])
      }
    )

    test.each([
      [
        'strings and Buffers',
        { key: Buffer.from('k'), value: 'v', headers: { a: 'x', b: [Buffer.from('y'), 'z'] } },
      ],
      ['a null key and value', { key: null, value: null }],
      ['a numeric timestamp', { value: 'v', timestamp: 1700000000000 }],
      ['a timestamp of digits', { value: 'v', timestamp: '1700000000000' }],
      ['a timestamp of digits with spaces', { value: 'v', timestamp: ' 1700000000000' }],
      ['a Date timestamp', { value: 'v', timestamp: new Date(1700000000000) }],
      ['a BigInt timestamp', { value: 'v', timestamp: 1700000000000n }],
      ['a Long timestamp', { value: 'v', timestamp: Long.fromNumber(1700000000000) }],
      ['a timestamp beyond 2^53', { value: 'v', timestamp: 2 ** 60 }],
    ])('%s are accepted, as the encoder writes them', async (_, message) => {
      const { accumulator } = createAccumulator({ lingers: () => false })

      await expect(append(accumulator, [{ partition: 0, ...message }])).resolves.toHaveLength(1)
    })
  })

  describe('a send path that throws before it answers', () => {
    test.each([
      ['a linger of 0 hands the batch on from a queued microtask', { linger: 0, batchSize: 16384 }],
      ['a send that fills its batch hands it on from its own call', { linger: 5, batchSize: 1 }],
    ])(
      '%s: the failure rejects the append, never an uncaught exception or an unhandled rejection',
      (_, options) => {
        expect(whereASendPathFailureGoes(options)).toEqual(['append rejected: send path failure'])
      }
    )

    test.each([
      ['batches that share their options', [{}, {}], 1],
      ['batches of different options', [{ timeout: 1 }, { timeout: 2 }], 2],
    ])(
      'flush() resolves and every send it carried rejects with that failure, for %s',
      async (_, optionSets, handOffs) => {
        jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask', 'nextTick', 'setImmediate'] })
        const { watch, flushAll } = require('../../__tests__/fixtures/bufferHarness')
        const failure = new Error('send path failure')
        const sendPartitioned = jest.fn(() => {
          throw failure
        })
        const { accumulator } = createAccumulator({ sendPartitioned })
        const sends = optionSets.map((options, partition) =>
          watch(append(accumulator, records(partition, 'a'), options))
        )
        await flushAll()

        const flushed = watch(accumulator.flush())
        await flushAll()

        expect(flushed).toEqual({ state: 'fulfilled', value: undefined })
        expect(sendPartitioned).toHaveBeenCalledTimes(handOffs)
        expect(sends.map(({ state, value }) => [state, value])).toEqual(
          optionSets.map(() => ['rejected', failure])
        )
      }
    )
  })

  describe('a producer', () => {
    beforeEach(() => {
      jest.useFakeTimers({ now: 0 })
    })

    test('sendBatch sends its records as they were at the call, though an accessor would empty them on a second read, and never stays pending', async () => {
      jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask'] })
      const { producer } = createLingeringProducer({ linger: 0 })
      await producer.connect()
      const first = [{ value: 'a' }]
      let reads = 0
      const last = [
        {
          get value() {
            if (++reads === 2) {
              first.length = 0
              last.length = 0
            }
            return 'b'
          },
        },
      ]

      try {
        const sending = producer.sendBatch({
          topicMessages: [
            { topic: 'a', messages: first },
            { topic: 'b', messages: last },
          ],
        })

        await expect(settledWithin(sending)).resolves.toHaveLength(2)
        expect(reads).toBe(1)
        expect([first.length, last.length]).toEqual([1, 1])
      } finally {
        await withinRealTime(producer.disconnect())
      }
    })

    describe.each([
      ['an idempotent producer', undefined],
      ['an idempotent: false producer', false],
    ])('%s', (_, idempotent) => {
      test('without linger or batchSize, sends wait 5 ms and go out together, batches up to 16384 bytes', async () => {
        const { producer, produced } = createLingeringProducer({ idempotent })
        await producer.connect()

        const sends = ['a', 'b'].map((value) =>
          producer.send({ topic: TOPIC, messages: [{ value }] })
        )
        await jest.advanceTimersByTimeAsync(4)
        expect(produced()).toEqual([])
        await settledWithin(Promise.all(sends), 1)
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a', 'b']])

        const full = producer.send({ topic: TOPIC, messages: [{ value: 'x'.repeat(16383) }] })
        await jest.advanceTimersByTimeAsync(0)
        expect(produced()).toHaveLength(1)
        producer.send({ topic: TOPIC, messages: [{ value: 'y' }] })
        await jest.advanceTimersByTimeAsync(0)
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([
          ['a', 'b'],
          ['x'.repeat(16383), 'y'],
        ])
        await settledWithin(full)
      })

      test('with linger: 0, a send goes out at once and nothing is scheduled', async () => {
        jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask'] })
        const { producer, produced } = createLingeringProducer({ idempotent, linger: 0 })
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await microtasks()

        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
        expect(jest.getTimerCount()).toBe(0)
        await expect(sent).resolves.toEqual([expect.objectContaining({ partition: 0 })])
      })

      test.each([
        ['every partition', [BATCH_SIZE, BATCH_SIZE, BATCH_SIZE]],
        ['one partition', [BATCH_SIZE, 1, 1]],
      ])(
        'a sendBatch filling batchSize on %s of three on one broker is one Produce request',
        async (_, sizes) => {
          const { producer, produced } = createLingeringProducer({
            idempotent,
            partitions: 3,
            createPartitioner:
              () =>
              ({ message }) =>
                message.partition,
          })
          await producer.connect()

          const sent = producer.sendBatch({
            topicMessages: [
              {
                topic: TOPIC,
                messages: sizes.map((size, partition) => ({ partition, value: 'x'.repeat(size) })),
              },
            ],
          })

          await expect(settledWithin(sent)).resolves.toHaveLength(3)
          expect(
            produced().map(({ partitions }) => partitions.map(({ partition }) => partition))
          ).toEqual([[0, 1, 2]])
        }
      )

      test('three sends to one partition inside the linger window are one Produce request, each resolving with its own offset', async () => {
        const { producer, produced } = createLingeringProducer({ idempotent })
        await producer.connect()

        const sends = ['a', 'b', 'c'].map((value) =>
          producer.send({ topic: TOPIC, messages: [{ value }] })
        )
        const results = await settledWithin(Promise.all(sends), LINGER)

        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a', 'b', 'c']])
        expect(results.map(([{ baseOffset }]) => baseOffset)).toEqual(['0', '1', '2'])
      })

      test('sends to two partitions a millisecond apart reach the broker that leads both in one Produce request', async () => {
        const { producer, produced } = createLingeringProducer({
          idempotent,
          partitions: 2,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
        })
        await producer.connect()

        const first = producer.send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
        await jest.advanceTimersByTimeAsync(1)
        const second = producer.send({ topic: TOPIC, messages: [{ partition: 1, value: 'b' }] })
        await settledWithin(Promise.all([first, second]), LINGER)

        expect(produced().map(({ partitions }) => partitions.map(({ values }) => values))).toEqual([
          [['a'], ['b']],
        ])
      })

      test('partitions led by different brokers go in separate requests, by what the cluster has cached', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent,
          partitions: 2,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
        })
        cluster.cachedLeader = jest.fn((topic, partition) => partition)
        await producer.connect()

        const first = producer.send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
        await jest.advanceTimersByTimeAsync(1)
        const second = producer.send({ topic: TOPIC, messages: [{ partition: 1, value: 'b' }] })
        await settledWithin(Promise.all([first, second]), LINGER)

        expect(produced().map(({ partitions }) => partitions.map(({ values }) => values))).toEqual([
          [['a']],
          [['b']],
        ])
        expect(cluster.cachedLeader.mock.calls).toEqual([
          [TOPIC, 0],
          [TOPIC, 1],
        ])
      })

      test('batchSize reached sends the batch before linger elapses', async () => {
        const { producer, produced } = createLingeringProducer({
          idempotent,
          linger: 60000,
          batchSize: 4,
        })
        await producer.connect()

        await settledWithin(
          Promise.all([
            producer.send({ topic: TOPIC, messages: [{ value: 'ab' }] }),
            producer.send({ topic: TOPIC, messages: [{ value: 'cd' }] }),
          ])
        )

        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['ab', 'cd']])
      })

      test('a send with a different compression sends the pending batch first and starts a new one', async () => {
        const { producer, produced } = createLingeringProducer({ idempotent })
        await producer.connect()

        producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)
        producer.send({ topic: TOPIC, messages: [{ value: 'b' }], compression: GZIP })
        await jest.advanceTimersByTimeAsync(0)
        expect(
          produced().map(({ compression, partitions }) => [compression, partitions[0].values])
        ).toEqual([[undefined, ['a']]])

        await jest.advanceTimersByTimeAsync(LINGER)

        expect(
          produced().map(({ compression, partitions }) => [compression, partitions[0].values])
        ).toEqual([
          [undefined, ['a']],
          [GZIP, ['b']],
        ])
      })

      test('disconnect() sends every lingering batch and waits for it, refuses sends meanwhile, and leaves no timer', async () => {
        const answered = deferred()
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent,
          linger: 60000,
          produce: async (request) => {
            await answered.promise
            return {
              topics: request.topicData.map(({ topic, partitions }) => ({
                topicName: topic,
                partitions: partitions.map(({ partition }) => ({
                  partition,
                  errorCode: 0,
                  baseOffset: '0',
                  logAppendTime: '-1',
                  logStartOffset: '0',
                })),
              })),
            }
          },
        })
        await producer.connect()
        const lingering = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        const disconnected = settledFlag(producer.disconnect())
        const refused = producer.send({ topic: TOPIC, messages: [{ value: 'b' }] }).catch((e) => e)

        expect(await settledWithin(refused)).toEqual(
          new KafkaJSNonRetriableError(
            `The producer is disconnecting; therefore, it can't safely accept messages anymore`
          )
        )
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
        expect(cluster.disconnect).not.toHaveBeenCalled()
        expect(disconnected()).toBe(false)

        answered.resolve()

        await expect(settledWithin(lingering)).resolves.toEqual([
          expect.objectContaining({ partition: 0 }),
        ])
        await jest.advanceTimersByTimeAsync(0)
        expect(disconnected()).toBe(true)
        expect(cluster.disconnect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('two disconnect() calls at once both wait for the lingering send to be delivered', async () => {
        const answered = deferred()
        const { cluster, producer } = createLingeringProducer({
          idempotent,
          linger: 60000,
          produce: async (request) => {
            await answered.promise
            return accepted(request)
          },
        })
        await producer.connect()
        const lingering = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        const disconnects = [producer.disconnect(), producer.disconnect()].map(settledFlag)
        await jest.advanceTimersByTimeAsync(0)
        expect(disconnects.map((disconnected) => disconnected())).toEqual([false, false])
        expect(cluster.disconnect).not.toHaveBeenCalled()

        answered.resolve()

        await expect(settledWithin(lingering)).resolves.toEqual([
          expect.objectContaining({ partition: 0 }),
        ])
        await jest.advanceTimersByTimeAsync(0)
        expect(disconnects.map((disconnected) => disconnected())).toEqual([true, true])
      })

      test('connect() after disconnect() accumulates and sends again', async () => {
        const { producer, produced } = createLingeringProducer({ idempotent })
        await producer.connect()
        await settledWithin(producer.disconnect())
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)
        expect(produced()).toEqual([])

        await expect(settledWithin(sent, LINGER)).resolves.toHaveLength(1)
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
      })

      test('a send still being assigned partitions when disconnect() starts is answered before the producer disconnects', async () => {
        const held = deferred()
        const answered = deferred()
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent,
          linger: 60000,
          produce: async (request) => {
            await answered.promise
            return accepted(request)
          },
        })
        await producer.connect()
        cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => held.promise)
        const pending = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        const disconnected = settledFlag(producer.disconnect())
        held.resolve()
        await jest.advanceTimersByTimeAsync(0)
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
        expect(cluster.disconnect).not.toHaveBeenCalled()

        answered.resolve()

        await expect(settledWithin(pending)).resolves.toEqual([
          expect.objectContaining({ partition: 0 }),
        ])
        await jest.advanceTimersByTimeAsync(0)
        expect(disconnected()).toBe(true)
        expect(cluster.broker.produce.mock.invocationCallOrder[0]).toBeLessThan(
          cluster.disconnect.mock.invocationCallOrder[0]
        )
        expect(cluster.connect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('connect waits for active close and sends during it keep the disconnecting refusal', async () => {
        const torn = deferred()
        const { cluster, producer } = createLingeringProducer({ idempotent, linger: 60000 })
        await producer.connect()
        cluster.disconnect.mockImplementationOnce(() => torn.promise)
        const disconnected = producer.disconnect()
        await jest.advanceTimersByTimeAsync(0)
        const connecting = producer.connect()
        const connected = settledFlag(connecting)
        try {
          await jest.advanceTimersByTimeAsync(0)
          expect(connected()).toBe(false)
          expect(cluster.connect).toHaveBeenCalledTimes(1)
          const refused = await producer
            .send({ topic: TOPIC, messages: [{ value: 'x' }] })
            .catch((error) => error)
          expect(refused).toBeInstanceOf(KafkaJSNonRetriableError)
          expect(refused.message).toMatch(/producer is disconnecting/)
          expect(cluster.broker.produce).not.toHaveBeenCalled()
          expect(jest.getTimerCount()).toBe(0)
        } finally {
          torn.resolve()
          await disconnected
          await connecting
        }
        expect(cluster.connect).toHaveBeenCalledTimes(2)
      })

      test('a send that joins a batch while disconnect() delivers the lingering ones goes out at once, arming no timer', async () => {
        const answered = deferred()
        const held = deferred()
        const { cluster, producer } = createLingeringProducer({
          idempotent,
          linger: 60000,
          produce: async (request) => {
            await answered.promise
            return accepted(request)
          },
        })
        await producer.connect()
        const lingering = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)
        cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => held.promise)
        const late = producer.send({ topic: TOPIC, messages: [{ value: 'b' }] })
        await jest.advanceTimersByTimeAsync(0)

        const disconnected = producer.disconnect()
        held.resolve()
        await jest.advanceTimersByTimeAsync(0)
        expect(jest.getTimerCount()).toBe(0)

        answered.resolve()

        await settledWithin(Promise.all([lingering, late, disconnected]))
        await expect(late).resolves.toEqual([expect.objectContaining({ partition: 0 })])
        expect(cluster.disconnect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('no send passes the connection check between the delivery and the disconnect', async () => {
        const answered = deferred()
        const partitioner = jest.fn(() => 0)
        const { producer } = createLingeringProducer({
          idempotent,
          linger: 60000,
          createPartitioner: () => partitioner,
          produce: async (request) => {
            await answered.promise
            return accepted(request)
          },
        })
        await producer.connect()
        producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        let disconnected = false
        const disconnecting = producer.disconnect().then(() => {
          disconnected = true
        })
        // One send per microtask until disconnect() resolves, so one lands in any gap.
        const refused = []
        const attempt = () => {
          if (!disconnected && refused.length < 1000) {
            refused.push(
              producer.send({ topic: TOPIC, messages: [{ value: 'late' }] }).catch((e) => e)
            )
            Promise.resolve().then(attempt)
          }
        }
        attempt()
        answered.resolve()
        await settledWithin(disconnecting)

        expect(refused.length).toBeGreaterThan(1)
        expect(refused.length).toBeLessThan(1000)
        const errors = await settledWithin(Promise.all(refused))
        expect(errors.every((error) => error instanceof KafkaJSError)).toBe(true)
        expect(partitioner.mock.calls.filter(([{ message }]) => message.value === 'late')).toEqual(
          []
        )
      })
    })

    test('a send still waiting for the producer id when disconnect() starts is answered before the producer disconnects', async () => {
      const held = deferred()
      const { cluster, producer } = createLingeringProducer({ linger: 60000 })
      cluster.broker.initProducerId.mockImplementationOnce(async () => {
        await held.promise
        return { producerId: '1000', producerEpoch: 0 }
      })
      const connected = producer.connect()
      await jest.advanceTimersByTimeAsync(0)
      const pending = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
      await jest.advanceTimersByTimeAsync(0)

      const disconnected = producer.disconnect()
      held.resolve()

      await expect(settledWithin(pending)).resolves.toEqual([
        expect.objectContaining({ partition: 0 }),
      ])
      await settledWithin(Promise.all([connected, disconnected]))
      expect(cluster.broker.produce.mock.invocationCallOrder[0]).toBeLessThan(
        cluster.disconnect.mock.invocationCallOrder[0]
      )
    })

    describe('an idempotent: false producer settles and retries each partition on its own', () => {
      /**
       * A broker answering each partition with `codeOf(partition, request)`, the 1-based count of
       * Produce requests so far; as `Broker#produce` does, it throws the first failing partition's
       * error unless the request asks for every partition's result.
       */
      const answering = (codeOf) => {
        let count = 0
        return async (request) => {
          count += 1
          const response = {
            topics: request.topicData.map(({ topic, partitions }) => ({
              topicName: topic,
              partitions: partitions.map(({ partition }) => {
                const errorCode = codeOf(partition, count)
                return {
                  partition,
                  errorCode,
                  baseOffset: errorCode === 0 ? '0' : '-1',
                  logAppendTime: '-1',
                  logStartOffset: '0',
                }
              }),
            })),
          }
          return request.perPartitionResults ? response : parseProduce(response)
        }
      }

      const partitionsSent = (produced) =>
        produced().map(({ partitions }) => partitions.map(({ partition }) => partition))

      test('a partition answered with a non-retriable error rejects only the sends its batch carries', async () => {
        const { producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering((partition) => (partition === 0 ? MESSAGE_TOO_LARGE : 0)),
        })
        await producer.connect()

        const a = producer
          .send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
          .catch((e) => e)
        const b = producer.send({ topic: TOPIC, messages: [{ partition: 1, value: 'b' }] })

        await expect(settledWithin(b, LINGER)).resolves.toEqual([
          expect.objectContaining({ partition: 1, baseOffset: '0' }),
        ])
        expect(await a).toBeInstanceOf(KafkaJSProtocolError)
        expect(await a).toMatchObject({ code: MESSAGE_TOO_LARGE, partition: 0 })
        expect(partitionsSent(produced)).toEqual([[0, 1]])
      })

      test('only a partition answered with a retriable error is sent again; one answered OK is written once', async () => {
        const { producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering((partition, count) =>
            partition === 0 && count <= 2 ? REQUEST_TIMED_OUT : 0
          ),
        })
        await producer.connect()

        const sends = [0, 1].map((partition) =>
          producer.send({ topic: TOPIC, messages: [{ partition, value: `${partition}` }] })
        )

        await expect(settledWithin(Promise.all(sends), 200)).resolves.toEqual([
          [expect.objectContaining({ partition: 0 })],
          [expect.objectContaining({ partition: 1 })],
        ])
        expect(partitionsSent(produced)).toEqual([[0, 1], [0], [0]])
      })

      test('a partition missing from the answer is sent again', async () => {
        const answer = answering(() => 0)
        let count = 0
        const { producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: async (request) => {
            const response = await answer(request)
            if ((count += 1) === 1) {
              const [entry] = response.topics
              entry.partitions = entry.partitions.filter(({ partition }) => partition !== 1)
            }
            return response
          },
        })
        await producer.connect()

        const sends = [0, 1].map((partition) =>
          producer.send({ topic: TOPIC, messages: [{ partition, value: `${partition}` }] })
        )

        await expect(settledWithin(Promise.all(sends), 100)).resolves.toEqual([
          [expect.objectContaining({ partition: 0 })],
          [expect.objectContaining({ partition: 1, baseOffset: '0' })],
        ])
        expect(partitionsSent(produced)).toEqual([[0, 1], [1]])
      })

      test('a broker that cannot be reached leaves only its own partitions for the next attempt', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering(() => 0),
        })
        const { broker } = cluster
        cluster.findLeaderForPartitions = (topic, ids) =>
          Object.fromEntries(ids.map((id) => [Number(id), [Number(id)]]))
        cluster.findBroker = jest.fn(async ({ nodeId }) => {
          if (Number(nodeId) === 1) {
            throw new KafkaJSConnectionError('Connection timeout')
          }
          return broker
        })
        await producer.connect()

        const a = producer.send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
        const b = producer
          .send({ topic: TOPIC, messages: [{ partition: 1, value: 'b' }] })
          .catch((e) => e)

        await expect(settledWithin(a, LINGER)).resolves.toEqual([
          expect.objectContaining({ partition: 0 }),
        ])
        expect(await settledWithin(b, 200)).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        expect(partitionsSent(produced)).toEqual([[0]])
        expect(
          cluster.findBroker.mock.calls.filter(([{ nodeId }]) => Number(nodeId) === 1)
        ).toHaveLength(4)
      })

      test('a broker lookup that fails for good rejects only that broker’s partitions, at once, while the others retry', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent: false,
          partitions: 3,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering((partition, count) =>
            partition === 2 && count === 1 ? REQUEST_TIMED_OUT : 0
          ),
        })
        const { broker } = cluster
        const refused = new KafkaJSNonRetriableError('Authentication failed')
        cluster.findLeaderForPartitions = (topic, ids) =>
          Object.fromEntries(ids.map((id) => [Number(id), [Number(id)]]))
        cluster.findBroker = jest.fn(async ({ nodeId }) => {
          if (Number(nodeId) === 1) {
            throw refused
          }
          return broker
        })
        await producer.connect()

        const sends = [0, 1, 2].map((partition) =>
          producer
            .send({ topic: TOPIC, messages: [{ partition, value: `${partition}` }] })
            .catch((e) => e)
        )

        const [a, b, c] = await settledWithin(Promise.all(sends), 100)
        expect(b).toBe(refused)
        expect(a).toEqual([expect.objectContaining({ partition: 0 })])
        expect(c).toEqual([expect.objectContaining({ partition: 2 })])
        expect(partitionsSent(produced)).toEqual([[0, 2], [2]])
        expect(cluster.findBroker).toHaveBeenCalledTimes(4)
      })

      test('a topic that has lost its metadata leaves only its own partitions for the next attempt', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent: false,
          produce: answering(() => 0),
        })
        const { findTopicPartitionMetadata } = cluster
        let lookups = 0
        cluster.findTopicPartitionMetadata = (topic) =>
          topic === OTHER_TOPIC && (lookups += 1) === 2 ? [] : findTopicPartitionMetadata(topic)
        await producer.connect()

        const sent = producer.sendBatch({
          topicMessages: [
            { topic: TOPIC, messages: [{ value: 'a' }] },
            { topic: OTHER_TOPIC, messages: [{ value: 'b' }] },
          ],
        })

        await expect(settledWithin(sent, 100)).resolves.toHaveLength(2)
        expect(
          cluster.broker.produce.mock.calls.map(([{ topicData }]) =>
            topicData.map(({ topic }) => topic)
          )
        ).toEqual([[TOPIC], [OTHER_TOPIC]])
        expect(produced()).toHaveLength(2)
      })

      test('the delivery deadline counts from the send, through the wait for its metadata', async () => {
        const { cluster, producer } = createLingeringProducer({
          idempotent: false,
          deliveryTimeout: 300,
          retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
          produce: answering(() => REQUEST_TIMED_OUT),
        })
        const { findTopicPartitionMetadata } = cluster
        cluster.findTopicPartitionMetadata = (topic) =>
          Date.now() < 250 ? [] : findTopicPartitionMetadata(topic)
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e)
        const settled = settledFlag(sent)
        await jest.advanceTimersByTimeAsync(270)
        expect(settled()).toBe(false)
        await jest.advanceTimersByTimeAsync(30)

        expect(settled()).toBe(true)
        expect(await sent).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(cluster.broker.produce.mock.calls.length).toBeGreaterThan(0)
      })

      test('a send whose partitions come after the delivery deadline rejects without sending', async () => {
        const { cluster, producer } = createLingeringProducer({
          idempotent: false,
          deliveryTimeout: 300,
          retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
          produce: answering(() => 0),
        })
        const { findTopicPartitionMetadata } = cluster
        cluster.findTopicPartitionMetadata = (topic) =>
          Date.now() < 296 ? [] : findTopicPartitionMetadata(topic)
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e)
        const error = await settledWithin(sent, 320)

        expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(error.message).toEqual(
          'Gave up before the send reached a broker: its 300 ms delivery deadline passed'
        )
        expect(error.outcome).toBe('unsent')
        expect(cluster.broker.produce).not.toHaveBeenCalled()
      })

      describe('a hand-off whose batches are of different ages', () => {
        const DEADLINE = 150

        /**
         * Puts `Date.now()` `ms` ahead of the fake timers' clock from now on, as an event loop that
         * stalled for `ms` does: the timers armed before the stall fire late.
         * @param {number} ms
         */
        const stallFor = (ms) => {
          const fakeNow = Date.now
          jest.spyOn(Date, 'now').mockImplementation(() => fakeNow() + ms)
        }

        /** A producer whose partitions are the ones the records name, led by `cachedLeader`. */
        const agingProducer = ({ cachedLeader, ...options } = {}) => {
          const made = createLingeringProducer({
            idempotent: false,
            linger: 50,
            deliveryTimeout: DEADLINE,
            createPartitioner:
              () =>
              ({ message }) =>
                message.partition,
            ...options,
          })
          if (cachedLeader !== undefined) {
            made.cluster.cachedLeader = cachedLeader
          }
          return made
        }

        const sendTo = (producer, partition) =>
          producer
            .send({ topic: TOPIC, messages: [{ partition, value: `p${partition}` }] })
            .catch((e) => e)

        const partitionsSent = (produced) =>
          produced().map(({ partitions }) => partitions.map(({ partition }) => partition))

        const lateMessage = (partition) =>
          `Gave up on ${TOPIC} partition ${partition} before sending it: its ${DEADLINE} ms delivery deadline passed`

        test('a batch pulled in from another partition is sent when only the oldest batch is past its deadline', async () => {
          const { producer, produced } = agingProducer()
          await producer.connect()

          const older = sendTo(producer, 0)
          await jest.advanceTimersByTimeAsync(0)
          stallFor(160)
          await jest.advanceTimersByTimeAsync(1)
          const younger = sendTo(producer, 1)
          await jest.advanceTimersByTimeAsync(49)

          const [late, sent] = await settledWithin(Promise.all([older, younger]))

          expect(late).toBeInstanceOf(KafkaJSDeliveryTimeout)
          expect(late.message).toEqual(lateMessage(0))
          expect(sent).toEqual([expect.objectContaining({ partition: 1 })])
          expect(partitionsSent(produced)).toEqual([[1]])
        })

        test('a batch for another broker is left to its own linger and is sent', async () => {
          const { producer, produced } = agingProducer({
            cachedLeader: (_, partition) => partition,
          })
          await producer.connect()

          const older = sendTo(producer, 0)
          await jest.advanceTimersByTimeAsync(0)
          stallFor(160)
          await jest.advanceTimersByTimeAsync(1)
          const younger = sendTo(producer, 1)
          const youngerSettled = settledFlag(younger)
          await jest.advanceTimersByTimeAsync(49)

          expect(await settledWithin(older)).toEqual(
            expect.objectContaining({ message: lateMessage(0) })
          )
          expect(youngerSettled()).toBe(false)
          expect(partitionsSent(produced)).toEqual([])

          expect(await settledWithin(younger, 1)).toEqual([
            expect.objectContaining({ partition: 1 }),
          ])
          expect(partitionsSent(produced)).toEqual([[1]])
        })

        test('two batches that are both due go out together when only the older is past its deadline', async () => {
          const { producer, produced } = agingProducer({
            cachedLeader: (_, partition) => partition,
          })
          await producer.connect()

          const older = sendTo(producer, 0)
          await jest.advanceTimersByTimeAsync(40)
          const younger = sendTo(producer, 1)
          await jest.advanceTimersByTimeAsync(0)
          stallFor(120)
          await jest.advanceTimersByTimeAsync(10)

          const [late, sent] = await settledWithin(Promise.all([older, younger]))

          expect(late).toBeInstanceOf(KafkaJSDeliveryTimeout)
          expect(late.message).toEqual(lateMessage(0))
          expect(sent).toEqual([expect.objectContaining({ partition: 1 })])
          expect(partitionsSent(produced)).toEqual([[1]])
        })

        test('the batches that are sent each retry until their own delivery deadline', async () => {
          const { producer } = agingProducer({
            linger: 200,
            deliveryTimeout: 400,
            retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
            produce: answering(() => REQUEST_TIMED_OUT),
          })
          await producer.connect()

          const older = sendTo(producer, 0)
          await jest.advanceTimersByTimeAsync(150)
          const younger = sendTo(producer, 1)
          const olderSettled = settledFlag(older)
          const youngerSettled = settledFlag(younger)
          await jest.advanceTimersByTimeAsync(230)
          expect(olderSettled()).toBe(false)
          expect(youngerSettled()).toBe(false)
          await jest.advanceTimersByTimeAsync(20)
          expect(olderSettled()).toBe(true)
          expect(youngerSettled()).toBe(false)
          expect(await older).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
          await jest.advanceTimersByTimeAsync(149)
          expect(youngerSettled()).toBe(false)
          await jest.advanceTimersByTimeAsync(1)
          expect(youngerSettled()).toBe(true)
          expect(await younger).toMatchObject({
            name: 'KafkaJSDeliveryTimeout',
            outcome: 'unknown',
          })
        })
      })

      test('a partition the metadata does not list yet waits for it within its retries', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent: false,
          produce: answering(() => 0),
        })
        const { findLeaderForPartitions } = cluster
        let lookups = 0
        cluster.findLeaderForPartitions = (topic, ids) =>
          (lookups += 1) === 1 ? {} : findLeaderForPartitions(topic, ids)
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })

        await expect(settledWithin(sent, 100)).resolves.toHaveLength(1)
        expect(lookups).toBe(2)
        expect(produced()).toHaveLength(1)
      })

      test('a hand-off whose topic has lost its metadata waits for it within its retries', async () => {
        const { cluster, producer, produced } = createLingeringProducer({ idempotent: false })
        const { findTopicPartitionMetadata } = cluster
        let lookups = 0
        cluster.findTopicPartitionMetadata = (topic) =>
          (lookups += 1) === 2 ? [] : findTopicPartitionMetadata(topic)
        await producer.connect()

        const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })

        await expect(settledWithin(sent, 100)).resolves.toEqual([
          expect.objectContaining({ partition: 0 }),
        ])
        expect(lookups).toBe(3)
        expect(cluster.refreshMetadata).toHaveBeenCalled()
        expect(produced()).toHaveLength(1)
      })

      test('a send whose records an older batch carries rejects once every partition it wrote to has settled', async () => {
        const { cluster, producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering((partition) => (partition === 0 ? MESSAGE_TOO_LARGE : 0)),
        })
        // each partition has a leader of its own, so the later batch keeps its own linger
        cluster.cachedLeader = (topic, partition) => partition
        await producer.connect()

        const a = producer
          .send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
          .catch((e) => e)
        await jest.advanceTimersByTimeAsync(2)
        const b = producer
          .send({
            topic: TOPIC,
            messages: [
              { partition: 0, value: 'b0' },
              { partition: 1, value: 'b1' },
            ],
          })
          .catch((e) => e)
        const bSettled = settledFlag(b)
        await jest.advanceTimersByTimeAsync(4)
        expect(await settledWithin(a)).toMatchObject({ code: MESSAGE_TOO_LARGE })
        expect(bSettled()).toBe(false)

        expect(await settledWithin(b, 1)).toMatchObject({ code: MESSAGE_TOO_LARGE })
        expect(
          produced().map(({ partitions }) =>
            partitions.map(({ partition, values }) => [partition, values])
          )
        ).toEqual([[[0, ['a', 'b0']]], [[1, ['b1']]]])
      })

      test('partitions one broker leads share one request, and a partition it rejects fails only the sends it carries', async () => {
        const { producer, produced } = createLingeringProducer({
          idempotent: false,
          createPartitioner:
            () =>
            ({ message }) =>
              message.partition,
          produce: answering((partition) => (partition === 0 ? MESSAGE_TOO_LARGE : 0)),
        })
        await producer.connect()

        const a = producer
          .send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
          .catch((e) => e)
        await jest.advanceTimersByTimeAsync(2)
        const b = producer
          .send({
            topic: TOPIC,
            messages: [
              { partition: 0, value: 'b0' },
              { partition: 1, value: 'b1' },
            ],
          })
          .catch((e) => e)
        await jest.advanceTimersByTimeAsync(4)

        expect(await settledWithin(a)).toMatchObject({ code: MESSAGE_TOO_LARGE })
        expect(await settledWithin(b)).toMatchObject({ code: MESSAGE_TOO_LARGE })
        expect(
          produced().map(({ partitions }) =>
            partitions.map(({ partition, values }) => [partition, values])
          )
        ).toEqual([
          [
            [0, ['a', 'b0']],
            [1, ['b1']],
          ],
        ])
      })
    })

    test('an idempotent: false producer sends its batches on the non-idempotent path: no producer id, no sequence', async () => {
      const { cluster, producer, produced } = createLingeringProducer({ idempotent: false })
      await producer.connect()

      const sends = ['a', 'b'].map((value) =>
        producer.send({ topic: TOPIC, messages: [{ value }] })
      )
      await settledWithin(Promise.all(sends), LINGER)

      expect(cluster.broker.initProducerId).not.toHaveBeenCalled()
      expect(produced()).toEqual([
        {
          acks: -1,
          producerId: -1,
          partitions: [{ partition: 0, firstSequence: undefined, values: ['a', 'b'] }],
        },
      ])
    })

    test.each([
      [
        'an idempotent producer keeps a due batch open while its partition’s batch before it is in flight',
        undefined,
        [['a']],
        [['a'], ['b', 'c']],
      ],
      [
        'an idempotent: false producer sends a due batch at once, pipelining the partition',
        false,
        [['a'], ['b'], ['c']],
        [['a'], ['b'], ['c']],
      ],
    ])('%s', async (_, idempotent, whileHeld, expected) => {
      const answers = []
      const { producer, produced } = createLingeringProducer({
        idempotent,
        produce: async (request) => {
          const answer = deferred()
          answers.push(answer.resolve)
          await answer.promise
          return accepted(request)
        },
      })
      await producer.connect()

      const sends = []
      for (const value of ['a', 'b', 'c']) {
        sends.push(producer.send({ topic: TOPIC, messages: [{ value }] }))
        await jest.advanceTimersByTimeAsync(LINGER + 1)
      }
      const values = () => produced().map(({ partitions }) => partitions[0].values)
      expect(values()).toEqual(whileHeld)

      answers.splice(0).forEach((answer) => answer())
      await jest.advanceTimersByTimeAsync(0)
      answers.splice(0).forEach((answer) => answer())

      await settledWithin(Promise.all(sends))
      expect(values()).toEqual(expected)
    })

    test('a send with a different acks sends the pending batch first and starts a new one', async () => {
      const { producer, produced } = createLingeringProducer({ idempotent: false })
      await producer.connect()

      producer.send({ topic: TOPIC, messages: [{ value: 'a' }], acks: 1 })
      await jest.advanceTimersByTimeAsync(0)
      producer.send({ topic: TOPIC, messages: [{ value: 'b' }], acks: -1 })
      await jest.advanceTimersByTimeAsync(0)
      expect(produced().map(({ acks, partitions }) => [acks, partitions[0].values])).toEqual([
        [1, ['a']],
      ])

      await jest.advanceTimersByTimeAsync(LINGER)

      expect(produced().map(({ acks, partitions }) => [acks, partitions[0].values])).toEqual([
        [1, ['a']],
        [-1, ['b']],
      ])
    })

    test('a lingering batch goes out numbered, under the producer id, before an acks: 1 send turns idempotence off', async () => {
      const { producer, produced } = createLingeringProducer()
      await producer.connect()

      const lingering = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
      await jest.advanceTimersByTimeAsync(0)
      const downgraded = producer.send({ topic: TOPIC, messages: [{ value: 'b' }], acks: 1 })
      await settledWithin(Promise.all([lingering, downgraded]), LINGER)

      expect(produced()).toEqual([
        {
          acks: -1,
          producerId: '1000',
          partitions: [{ partition: 0, firstSequence: 0, values: ['a'] }],
        },
        {
          acks: 1,
          producerId: -1,
          partitions: [{ partition: 0, firstSequence: undefined, values: ['b'] }],
        },
      ])
      expect(producer.isIdempotent()).toBe(false)
    })

    test('a send still being assigned partitions when idempotence turns off goes out without the producer id', async () => {
      const held = deferred()
      const { cluster, producer, produced } = createLingeringProducer()
      await producer.connect()
      cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => held.promise)

      const late = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })
      await jest.advanceTimersByTimeAsync(0)
      const downgraded = producer.send({ topic: TOPIC, messages: [{ value: 'b' }], acks: 1 })
      await jest.advanceTimersByTimeAsync(0)
      held.resolve()
      await settledWithin(Promise.all([late, downgraded]), LINGER)

      expect(
        produced().map(({ acks, producerId, partitions }) => [
          acks,
          producerId,
          partitions[0].values,
        ])
      ).toEqual([
        [-1, -1, ['a']],
        [1, -1, ['b']],
      ])
    })

    test.each([
      ['deliveryTimeout', { deliveryTimeout: 300 }],
      [
        'retry.maxTotalTime',
        {
          retry: {
            retries: 3,
            initialRetryTime: 10,
            maxRetryTime: 40,
            factor: 0,
            maxTotalTime: 300,
          },
        },
      ],
    ])(
      'an on-wire expiry, set by %s, releases a queued batch before its own deadline',
      async (_, deadline) => {
        const answer = deferred()
        const { producer, produced } = createLingeringProducer({
          ...deadline,
          produce: async (request) => {
            if (produced().length === 1) {
              await answer.promise
            }
            return accepted(request)
          },
        })
        await producer.connect()

        const first = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e)
        let held
        try {
          await jest.advanceTimersByTimeAsync(20)
          held = producer.send({ topic: TOPIC, messages: [{ value: 'b' }] }).catch((e) => e)
          const firstSettled = settledFlag(first)
          const heldSettled = settledFlag(held)
          await jest.advanceTimersByTimeAsync(279)
          expect(firstSettled()).toBe(false)
          expect(heldSettled()).toBe(false)
          await jest.advanceTimersByTimeAsync(1)
          expect(firstSettled()).toBe(true)
          expect(await first).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
          await jest.advanceTimersByTimeAsync(10)
          expect(Date.now()).toBeLessThan(320)
          expect(await settledWithin(held)).toEqual([expect.objectContaining({ partition: 0 })])
          expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a'], ['b']])
        } finally {
          answer.resolve()
          await jest.advanceTimersByTimeAsync(1000)
          await Promise.all([first, held])
        }
      }
    )

    test('a batch’s delivery deadline counts from its first send, not from when it went out', async () => {
      const { producer } = createLingeringProducer({
        linger: 20,
        deliveryTimeout: 50,
        retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
        produce: async () => {
          throw new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
        },
      })
      await producer.connect()

      const sent = producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e)
      const settled = settledFlag(sent)
      await jest.advanceTimersByTimeAsync(49)
      expect(settled()).toBe(false)
      await jest.advanceTimersByTimeAsync(1)

      expect(settled()).toBe(true)
      expect(await sent).toBeInstanceOf(KafkaJSDeliveryTimeout)
    })

    describe('inside a transaction', () => {
      const transactionalProducer = (options) =>
        createLingeringProducer({ transactionalId: 'transactional-a', linger: 60000, ...options })

      test('commit() sends the transaction’s lingering batches at once', async () => {
        const { cluster, producer, produced } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()
        const sent = transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        await settledWithin(transaction.commit())

        await expect(sent).resolves.toEqual([expect.objectContaining({ partition: 0 })])
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
        expect(cluster.broker.endTxn).toHaveBeenCalledWith(
          expect.objectContaining({ transactionResult: true })
        )
        expect(jest.getTimerCount()).toBe(0)
      })

      test('commit() right after sends it did not await waits for no linger', async () => {
        const { producer, produced } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()

        const sends = ['a', 'b'].map((value) =>
          transaction.send({ topic: TOPIC, messages: [{ value }] })
        )
        await settledWithin(transaction.commit())

        await expect(Promise.all(sends)).resolves.toHaveLength(2)
        expect(produced().flatMap(({ partitions }) => partitions[0].values)).toEqual(['a', 'b'])
        expect(jest.getTimerCount()).toBe(0)
      })

      test('abort() right after a send it did not await waits for no linger', async () => {
        const { producer } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()

        const sent = transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await settledWithin(transaction.abort())

        await expect(sent).resolves.toHaveLength(1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('abort() rejects the transaction’s lingering sends and produces nothing', async () => {
        const { cluster, producer } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()
        const sent = transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e)
        await jest.advanceTimersByTimeAsync(0)

        await settledWithin(transaction.abort())

        expect(await sent).toEqual(
          new KafkaJSTransactionAbortableError(
            'The transaction was aborted while this batch was waiting to be sent'
          )
        )
        expect(cluster.broker.produce).not.toHaveBeenCalled()
        expect(jest.getTimerCount()).toBe(0)
      })

      test.each([
        ['commit', true],
        ['abort', false],
      ])(
        '%s() waits for the one request two partitions sent a millisecond apart share',
        async (ending, transactionResult) => {
          const answered = deferred()
          const { cluster, producer, produced } = transactionalProducer({
            linger: 50,
            createPartitioner:
              () =>
              ({ message }) =>
                message.partition,
            produce: async (request) => {
              await answered.promise
              return accepted(request)
            },
          })
          await producer.connect()
          const transaction = await producer.transaction()

          const sendTo = (partition) =>
            transaction.send({ topic: TOPIC, messages: [{ partition, value: `p${partition}` }] })
          const sends = [sendTo(0)]
          await jest.advanceTimersByTimeAsync(1)
          sends.push(sendTo(1))
          await jest.advanceTimersByTimeAsync(49)
          await microtasks()

          expect(
            produced().map(({ partitions }) => partitions.map(({ partition }) => partition))
          ).toEqual([[0, 1]])
          const ended = settledFlag(transaction[ending]())
          await jest.advanceTimersByTimeAsync(0)
          expect(ended()).toBe(false)
          expect(cluster.broker.endTxn).not.toHaveBeenCalled()

          answered.resolve()
          await settledWithin(Promise.all(sends))
          await jest.advanceTimersByTimeAsync(0)

          expect(ended()).toBe(true)
          expect(cluster.broker.endTxn).toHaveBeenCalledWith(
            expect.objectContaining({ transactionResult })
          )
          expect(jest.getTimerCount()).toBe(0)
        }
      )

      test('a record the encoder cannot write fails its send and leaves the transaction committable', async () => {
        const { cluster, producer, produced } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()

        await expect(
          settledWithin(
            transaction.send({ topic: TOPIC, messages: [{ value: 'a', headers: { h: 1 } }] })
          )
        ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
        transaction.send({ topic: TOPIC, messages: [{ value: 'b' }] })
        await settledWithin(transaction.commit())

        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['b']])
        expect(cluster.broker.endTxn).toHaveBeenCalledWith(
          expect.objectContaining({ transactionResult: true })
        )
      })

      test('a send whose topic cannot be found arms the transaction, as without linger', async () => {
        const { cluster, producer } = transactionalProducer({
          retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1 },
        })
        await producer.connect()
        const transaction = await producer.transaction()
        cluster.addMultipleTargetTopics.mockRejectedValue(
          new KafkaJSProtocolError(createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION))
        )

        const error = await settledWithin(
          transaction.send({ topic: 'no-such-topic', messages: [{ value: 'a' }] }).catch((e) => e),
          10
        )

        expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(error.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        await expect(settledWithin(transaction.commit())).rejects.toBe(error)
      })

      describe('a send the delivery deadline ends before it is partitioned', () => {
        const RETRY_EVERY_100 = {
          retries: 100,
          initialRetryTime: 100,
          maxRetryTime: 100,
          factor: 0,
        }
        const RETRY_EVERY_10 = { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }

        /** A partitioner that throws a retriable error for 'broken-topic' and picks 0 otherwise. */
        const failingFor =
          () =>
          ({ topic }) => {
            if (topic === 'broken-topic') throw new KafkaJSError('no route yet')
            return 0
          }

        /**
         * Makes the second wait of a send to 'broken-topic' for its topic's metadata never end, until
         * the returned `release` is called.
         * @param {object} cluster
         * @param {number} [hangAt=2] which wait for 'broken-topic' hangs
         * @returns {{ release: () => void }}
         */
        const hangingWait = (cluster, hangAt = 2) => {
          const hung = deferred()
          let waits = 0
          cluster.addMultipleTargetTopics.mockImplementation(async (topics) => {
            topics.forEach((topic) => cluster.targetTopics.add(topic))
            if (topics.includes('broken-topic') && (waits += 1) === hangAt) await hung.promise
          })
          return { release: hung.resolve }
        }

        test.each([1, 2])(
          'a partitioner that keeps failing until the retries reach the deadline leaves the transaction committable at transaction version %i',
          async (level) => {
            const { cluster, producer, produced } = transactionalProducer({
              level,
              linger: 5,
              deliveryTimeout: 250,
              retry: RETRY_EVERY_100,
              createPartitioner: failingFor,
            })
            await producer.connect()
            const transaction = await producer.transaction()

            const error = await settledWithin(
              transaction
                .send({ topic: 'broken-topic', messages: [{ value: 'a' }] })
                .catch((e) => e),
              250
            )

            expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
            expect(error.cause).toBeInstanceOf(KafkaJSError)
            transaction.send({ topic: TOPIC, messages: [{ value: 'b' }] })
            await settledWithin(transaction.commit())

            expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['b']])
            expect(cluster.broker.endTxn).toHaveBeenCalledWith(
              expect.objectContaining({ transactionResult: true })
            )
          }
        )

        /**
         * Sends to 'broken-topic' with a 300 ms delivery deadline and a fixed 100 ms retry delay:
         * the partitioner fails at 0, 100 and 200 ms, and at 300 ms the send's deadline, armed
         * first, comes due together with the next retry. Checks that the deadline, not the retry,
         * ends the send, as an unsent timeout with no retry error around it.
         * @param {number} level the transaction version
         */
        const expireInBackoff = async (level) => {
          let failed = 0
          const { cluster, producer, produced } = transactionalProducer({
            level,
            linger: 5,
            deliveryTimeout: 300,
            retry: RETRY_EVERY_100,
            createPartitioner:
              () =>
              ({ topic }) => {
                if (topic !== 'broken-topic') return 0
                failed += 1
                throw new KafkaJSError('no route yet')
              },
          })
          await producer.connect()
          const transaction = await producer.transaction()

          const sent = transaction
            .send({ topic: 'broken-topic', messages: [{ value: 'a' }] })
            .catch((e) => e)
          const settled = settledFlag(sent)
          await jest.advanceTimersByTimeAsync(299)
          expect(failed).toBe(3)
          expect(settled()).toBe(false)
          const error = await settledWithin(sent, 1)
          await jest.advanceTimersByTimeAsync(0)

          expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
          expect(error.outcome).toBe('unsent')
          expect(error.cause).toBeUndefined()
          expect(failed).toBe(3)
          expect(produced()).toEqual([])
          return { cluster, producer, produced, transaction }
        }

        test.each([1, 2])(
          'the deadline ending a send in the backoff after its partitioner failed, before the next retry runs, leaves the transaction committable at transaction version %i',
          async (level) => {
            const { cluster, produced, transaction } = await expireInBackoff(level)

            transaction.send({ topic: TOPIC, messages: [{ value: 'b' }] })
            await settledWithin(transaction.commit())

            expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['b']])
            expect(cluster.broker.endTxn).toHaveBeenCalledWith(
              expect.objectContaining({ transactionResult: true })
            )
          }
        )

        test.each([1, 2])(
          'the deadline ending a send in the backoff after its partitioner failed leaves the transaction abortable: abort() ends it and the next transaction commits at transaction version %i',
          async (level) => {
            const { cluster, producer, produced, transaction } = await expireInBackoff(level)

            await settledWithin(transaction.send({ topic: TOPIC, messages: [{ value: 'b' }] }), 5)
            await expect(settledWithin(transaction.abort())).resolves.toBeUndefined()
            const second = await producer.transaction()
            await settledWithin(second.send({ topic: TOPIC, messages: [{ value: 'c' }] }), 5)
            await expect(settledWithin(second.commit())).resolves.toBeUndefined()

            expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['b'], ['c']])
            expect(
              cluster.broker.endTxn.mock.calls.map(([{ transactionResult }]) => transactionResult)
            ).toEqual([false, true])
          }
        )

        test.each([1, 2])(
          'the deadline ending a send that waits for its metadata again after its partitioner failed makes the transaction abort-only at transaction version %i',
          async (level) => {
            const { cluster, producer } = transactionalProducer({
              level,
              linger: 5,
              deliveryTimeout: 300,
              retry: RETRY_EVERY_10,
              createPartitioner: failingFor,
            })
            const { release } = hangingWait(cluster)
            await producer.connect()
            const transaction = await producer.transaction()

            const error = await settledWithin(
              transaction
                .send({ topic: 'broken-topic', messages: [{ value: 'a' }] })
                .catch((e) => e),
              300
            )

            expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
            await expect(settledWithin(transaction.commit())).rejects.toBeInstanceOf(
              KafkaJSTransactionAbortableError
            )
            expect(cluster.broker.endTxn).not.toHaveBeenCalled()
            release()
            await jest.advanceTimersByTimeAsync(0)
          }
        )

        test.each([1, 2])(
          'the deadline ending a send that never got its metadata still makes the transaction abort-only at transaction version %i',
          async (level) => {
            const { cluster, producer } = transactionalProducer({
              level,
              linger: 5,
              deliveryTimeout: 300,
              retry: RETRY_EVERY_10,
            })
            const { release } = hangingWait(cluster, 1)
            await producer.connect()
            const transaction = await producer.transaction()

            const error = await settledWithin(
              transaction
                .send({ topic: 'broken-topic', messages: [{ value: 'a' }] })
                .catch((e) => e),
              300
            )

            expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
            await expect(settledWithin(transaction.commit())).rejects.toBeInstanceOf(
              KafkaJSTransactionAbortableError
            )
            expect(cluster.broker.endTxn).not.toHaveBeenCalled()
            release()
            await jest.advanceTimersByTimeAsync(0)
          }
        )
      })

      test('after a commit settles, the next transaction’s sends linger again', async () => {
        const { producer, produced } = transactionalProducer()
        await producer.connect()
        const first = await producer.transaction()
        first.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await settledWithin(first.commit())

        const second = await producer.transaction()
        const lingering = second.send({ topic: TOPIC, messages: [{ value: 'b' }] })
        await jest.advanceTimersByTimeAsync(0)
        const values = () => produced().map(({ partitions }) => partitions[0].values)
        expect(values()).toEqual([['a']])

        await settledWithin(second.commit())
        await lingering
        expect(values()).toEqual([['a'], ['b']])
      })

      test('disconnect() also sends the transaction’s lingering batches', async () => {
        const { producer, produced } = transactionalProducer()
        await producer.connect()
        const transaction = await producer.transaction()
        const sent = transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
        await jest.advanceTimersByTimeAsync(0)

        await settledWithin(producer.disconnect())

        await expect(sent).resolves.toEqual([expect.objectContaining({ partition: 0 })])
        expect(produced().map(({ partitions }) => partitions[0].values)).toEqual([['a']])
        expect(jest.getTimerCount()).toBe(0)
      })

      test('disconnect() refuses the producer’s own sends while the transaction’s batches are delivered, and leaves no timer', async () => {
        const answered = deferred()
        const { cluster, producer } = transactionalProducer({
          produce: async (request) => {
            await answered.promise
            return accepted(request)
          },
        })
        await producer.connect()
        const transaction = await producer.transaction()
        const sent = transaction.send({ topic: TOPIC, messages: [{ value: 't' }] })
        await jest.advanceTimersByTimeAsync(0)

        const disconnected = settledFlag(producer.disconnect())
        await jest.advanceTimersByTimeAsync(0)
        const late = producer.send({ topic: TOPIC, messages: [{ value: 'late' }] }).catch((e) => e)

        expect(await settledWithin(late)).toEqual(
          new KafkaJSNonRetriableError(
            `The producer is disconnecting; therefore, it can't safely accept messages anymore`
          )
        )
        expect(disconnected()).toBe(false)

        answered.resolve()

        await expect(settledWithin(sent)).resolves.toHaveLength(1)
        await jest.advanceTimersByTimeAsync(0)
        expect(disconnected()).toBe(true)
        expect(cluster.disconnect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      })
    })

    describe('a Produce request the connection refused for its SASL session', () => {
      const SESSION_REFUSAL =
        'The SASL session was not valid when the request was due to be written; it was not sent'

      /**
       * Fails every later Produce request as the connection's session refusal does: before the
       * attempt's guard is called with a frame length, so nothing is recorded as written.
       * @param {object} cluster
       * @returns {void}
       */
      const refuseForTheSession = (cluster) =>
        cluster.broker.produce.mockImplementation(async () => {
          throw new KafkaJSError(SESSION_REFUSAL, { retriable: true })
        })

      test('with no retries left a session refusal fails the send', async () => {
        const { cluster, producer } = createLingeringProducer({
          idempotent: false,
          retry: { retries: 0 },
        })
        refuseForTheSession(cluster)
        await producer.connect()

        const error = await settledWithin(
          producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e),
          100
        )

        expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        expect(error.message).toContain(SESSION_REFUSAL)
        expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
      })

      test('a batch written once and then refused for the session reports unknown', async () => {
        const { cluster, producer } = createLingeringProducer({
          idempotent: false,
          deliveryTimeout: 300,
          retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
          produce: async () => {
            throw new KafkaJSError('The broker did not answer')
          },
        })
        const writesThenFails = cluster.broker.produce.getMockImplementation()
        refuseForTheSession(cluster)
        cluster.broker.produce.mockImplementationOnce(writesThenFails)
        await producer.connect()

        const error = await settledWithin(
          producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e),
          320
        )

        expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
        expect(cluster.broker.produce.mock.calls.length).toBeGreaterThan(1)
      })

      test('inside a transaction a session refusal that exhausts retries arms KafkaJSTransactionAbortableError', async () => {
        jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask'] })
        const { cluster, producer } = createLingeringProducer({
          transactionalId: 'transactional-a',
          linger: 0,
          retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
        })
        await producer.connect()
        const transaction = await producer.transaction()
        refuseForTheSession(cluster)

        const error = await settledWithin(
          transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] }).catch((e) => e),
          100
        )

        expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(error.cause).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        expect(cluster.broker.produce).toHaveBeenCalledTimes(2)
        await expect(settledWithin(transaction.commit())).rejects.toBe(error)
      })
    })
  })

  describe('on the wire', () => {
    test.each([
      ['an idempotent producer', undefined],
      ['an idempotent: false producer', false],
    ])(
      '%s sends a record with a BigInt timestamp and one without a timestamp in one batch, and both sends resolve',
      async (_, idempotent) => {
        const wire = createGroupApiBroker({
          versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 7 } },
          respond: () => acceptAll(wire.requests.at(-1).body),
        })
        wire.broker.nodeId = 0
        const cluster = createFakeCluster({ partitions: 1 })
        cluster.findBroker = jest.fn(async () => wire.broker)
        const producer = createProducer({
          cluster,
          logger: newLogger(),
          idempotent,
          linger: LINGER,
          batchSize: BATCH_SIZE,
        })
        await producer.connect()

        try {
          const results = await Promise.all([
            producer.send({ topic: TOPIC, messages: [{ value: 'a', timestamp: 1700000000000n }] }),
            producer.send({ topic: TOPIC, messages: [{ value: 'b' }] }),
          ])

          const produces = wire.requests.filter(({ apiKey }) => apiKey === apiKeys.Produce)
          expect(produces).toHaveLength(1)
          const [{ batch }] = partitionsOf(produces[0].body)
          const { records: written } = await RecordBatchDecoder(new Decoder(batch))
          expect(written.map(({ value }) => value.toString())).toEqual(['a', 'b'])
          expect(String(written[0].timestamp)).toBe('1700000000000')
          expect(results.map(([{ baseOffset }]) => baseOffset)).toEqual(['100', '101'])
        } finally {
          await withinRealTime(producer.disconnect())
          await wire.pool.destroy()
        }
      }
    )

    test('three sends inside the linger window reach the broker as one Produce request, their records in arrival order', async () => {
      const wire = createGroupApiBroker({
        versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 7 } },
        respond: () => acceptAll(wire.requests.at(-1).body),
      })
      const cluster = createFakeCluster({ partitions: 1 })
      cluster.findBroker = jest.fn(async () => wire.broker)
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        linger: LINGER,
        batchSize: BATCH_SIZE,
      })
      await producer.connect()

      try {
        const results = await Promise.all(
          ['a', 'b', 'c'].map((value) => producer.send({ topic: TOPIC, messages: [{ value }] }))
        )

        const produces = wire.requests.filter(({ apiKey }) => apiKey === apiKeys.Produce)
        expect(produces).toHaveLength(1)
        const [{ partition, batch }] = partitionsOf(produces[0].body)
        const {
          producerId,
          firstSequence,
          records: written,
        } = await RecordBatchDecoder(new Decoder(batch))
        expect({ partition, producerId, firstSequence }).toEqual({
          partition: 0,
          producerId: '1000',
          firstSequence: 0,
        })
        expect(written.map(({ value }) => value.toString())).toEqual(['a', 'b', 'c'])
        expect(results.map(([{ baseOffset }]) => baseOffset)).toEqual(['100', '101', '102'])
      } finally {
        await withinRealTime(producer.disconnect())
        await wire.pool.destroy()
      }
    })
  })
})

describe('Producer > the accumulator > the topics a send holds', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * An accumulator that counts the topics held and released, keeps each send its partitioner is
   * given, and keeps every batch it is handed until the case settles and retires it.
   */
  const holdingTopics = (admission = new Admission({ capacity: 1048576, maxBlockMs: 0 })) => {
    const held = []
    const released = []
    const batches = []
    const sends = []
    const accumulator = new Accumulator({
      linger: LINGER,
      batchSize: BATCH_SIZE,
      admission,
      partition: async (send) => {
        sends.push(send)
        return send.records.flatMap(({ topic, messages }) =>
          messages.map((message) => ({ topic, partition: 0, messages: [message] }))
        )
      },
      sendPartitioned: async ({ batches: handed }) => {
        batches.push(...handed)
      },
      pinner: {
        holdTopic: (topic) => held.push(topic),
        releaseTopic: (topic) => released.push(topic),
      },
    })
    return { accumulator, admission, held, released, batches, sends }
  }

  test('a send holds each topic that carries records and releases it once, at its last retirement', async () => {
    const { accumulator, held, released, batches, sends } = holdingTopics()

    const sent = accumulator.append({
      acks: -1,
      topicMessages: [
        { topic: TOPIC, messages: records(0, 'a') },
        { topic: 'empty-topic', messages: [] },
        { topic: OTHER_TOPIC, messages: records(0, 'b') },
        { topic: 'third-topic', messages: records(0, 'c') },
      ],
    })
    expect(held).toEqual([TOPIC, OTHER_TOPIC, 'third-topic'])

    await jest.advanceTimersByTimeAsync(LINGER)
    expect(batches).toHaveLength(3)
    for (const batch of batches) {
      batch.settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '0' }))
    }
    await settledWithin(sent)
    expect(released).toEqual([])

    batches[0].retire()
    batches[1].retire()
    expect(released).toEqual([])
    batches[2].retire()
    expect(released).toEqual([TOPIC, OTHER_TOPIC, 'third-topic'])
    batches.forEach((batch) => batch.retire())
    sends[0].release()
    expect(released).toHaveLength(3)
  })

  test('a send the buffer refuses releases what it held, once', async () => {
    const { accumulator, held, released, admission } = holdingTopics(
      new Admission({ capacity: 2048, maxBlockMs: 0 })
    )

    await expect(
      accumulator.append({
        acks: -1,
        topicMessages: [{ topic: TOPIC, messages: [{ value: 'v'.repeat(4096) }] }],
      })
    ).rejects.toMatchObject({ name: 'KafkaJSRecordTooLargeError' })

    expect(held).toEqual([TOPIC])
    expect(released).toEqual([TOPIC])
    expect(admission.reserved).toBe(0)
  })

  test('a send holds nothing when no holder is given', async () => {
    const accumulator = new Accumulator({
      linger: LINGER,
      batchSize: BATCH_SIZE,
      partition: byMessage,
      sendPartitioned: async ({ batches }) =>
        batches.map(({ topic, partition, settlers }) => {
          const metadata = { topicName: topic, partition, errorCode: 0, baseOffset: '0' }
          settlers.forEach((settler) => settler.resolve(metadata))
          batches.forEach((batch) => batch.retire())
          return metadata
        }),
    })

    const sent = append(accumulator, records(0, 'a'))

    await expect(settledWithin(sent, LINGER)).resolves.toHaveLength(1)
  })
})

describe('Producer > the accumulator > a send its partitioner refused', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * An accumulator with a 1000 ms delivery deadline whose assignment marks each send as refused by
   * its partitioner, as a failed partitioner call does, and returns only when the case says;
   * `expired` hands back `armed` for whatever it is given.
   */
  const refusing = () => {
    const assigned = deferred()
    const armed = new KafkaJSError('armed')
    const expired = jest.fn(() => armed)
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: BATCH_SIZE,
      deadline: 1000,
      expired,
      partition: (send) => {
        send.refused = true
        return assigned.promise
      },
      sendPartitioned: async () => [],
    })
    return { accumulator, assigned, armed, expired }
  }

  test('its delivery deadline rejects it with the timeout itself and calls no expired', async () => {
    const { accumulator, assigned, expired } = refusing()
    const sent = append(accumulator, records(0, 'a')).catch((e) => e)

    const error = await settledWithin(sent, 1000)

    expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
    expect(expired).not.toHaveBeenCalled()
    assigned.resolve([])
    await jest.advanceTimersByTimeAsync(0)
    expect(accumulator.unsettled()).toEqual([])
  })

  test('a forced close rejects it through expired, as every send waiting for its partitions', async () => {
    const { accumulator, assigned, armed, expired } = refusing()
    const sent = append(accumulator, records(0, 'a')).catch((e) => e)
    await jest.advanceTimersByTimeAsync(0)

    accumulator.forceClose()

    expect(await settledWithin(sent)).toBe(armed)
    expect(expired).toHaveBeenCalledTimes(1)
    expect(expired).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'KafkaJSProducerClosedError', outcome: 'unsent' })
    )
    assigned.resolve([])
    await jest.advanceTimersByTimeAsync(0)
    expect(accumulator.unsettled()).toEqual([])
  })
})

describe('Producer > the accumulator > the flush inventory', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * An accumulator over a buffer of `capacity` whose send path keeps every batch it is handed;
   * `partition` assigns at once unless the case holds it.
   */
  const inventory = ({
    capacity = 1048576,
    linger = 0,
    deadline,
    partition = byMessage,
    getGeneration,
  } = {}) => {
    const admission = new Admission({ capacity, maxBlockMs: 60000 })
    const batches = []
    const accumulator = new Accumulator({
      linger,
      batchSize: BATCH_SIZE,
      deadline,
      admission,
      partition,
      sendPartitioned: async ({ batches: handed }) => {
        batches.push(...handed)
      },
      getGeneration,
    })
    return { accumulator, admission, batches }
  }

  /** Records when each promise settles, in the order the settlements are observed. */
  const observe = (order, named) => {
    for (const [name, promise] of Object.entries(named)) {
      promise.then(
        () => order.push(name),
        () => order.push(name)
      )
    }
  }

  test('a snapshot settles after the promise of every send begun before it, whatever ends each one', async () => {
    const assigned = deferred()
    const { accumulator, admission, batches } = inventory({
      capacity: 8192,
      deadline: 1000,
      partition: async (send) =>
        send.records[0].topic === 'held' ? assigned.promise : byMessage(send),
    })
    const order = []
    const sends = {
      delivered: append(accumulator, records(0, 'a')),
      failed: append(accumulator, records(1, 'b')),
      expired: accumulator.append({
        acks: -1,
        topicMessages: [{ topic: 'held', messages: records(0, 'c') }],
      }),
      refused: accumulator.append({
        acks: -1,
        topicMessages: [{ topic: TOPIC, messages: [{ value: 'w'.repeat(2000) }] }],
      }),
    }
    Object.values(sends).forEach((promise) => promise.catch(() => {}))
    observe(order, sends)
    await jest.advanceTimersByTimeAsync(0)
    expect(admission.waitingBytes).toBeGreaterThan(0)
    const [snapshot] = accumulator.unsettled()
    snapshot.then(() => order.push('flush'))
    await jest.advanceTimersByTimeAsync(500)
    const later = append(accumulator, records(2, 'later'))
    const laterSettled = settledFlag(later.catch(() => {}))

    batches[0].settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '0' }))
    batches[1].settlers.forEach((settler) => settler.reject(new KafkaJSError('failed')))
    await jest.advanceTimersByTimeAsync(500)
    expect(order).toEqual(['delivered', 'failed', 'expired', 'refused', 'flush'])
    expect(laterSettled()).toBe(false)
    expect(accumulator.unsettled()).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(500)
    await expect(later).rejects.toMatchObject({ name: 'KafkaJSDeliveryTimeout' })
    assigned.resolve([])
    batches.splice(0).forEach((batch) => batch.retire())
    await jest.advanceTimersByTimeAsync(0)
    expect(accumulator.unsettled()).toEqual([])
    expect(admission.reserved).toBe(0)
  })

  test('a send whose records cannot join a batch leaves the inventory with its rejection', async () => {
    const { accumulator, admission } = inventory({
      partition: async () => [{ topic: TOPIC, partition: 0, messages: null }],
    })
    const sent = append(accumulator, records(0, 'a'))
    const [snapshot] = accumulator.unsettled()

    await expect(sent).rejects.toBeInstanceOf(TypeError)
    await snapshot
    expect(accumulator.unsettled()).toEqual([])
    expect(admission.reserved).toBe(0)
  })

  test('a later snapshot also waits for the sends an earlier one covers, and none is taken when nothing is unsettled', async () => {
    const { accumulator, batches } = inventory()
    expect(accumulator.unsettled()).toEqual([])
    const first = append(accumulator, records(0, 'a'))
    const [older] = accumulator.unsettled()
    const second = append(accumulator, records(1, 'b'))
    const [newer] = accumulator.unsettled()
    const order = []
    observe(order, { older, newer })
    await jest.advanceTimersByTimeAsync(0)

    batches[1].settlers.forEach((settler) => settler.resolve({ partition: 1, baseOffset: '0' }))
    await second
    await jest.advanceTimersByTimeAsync(0)
    expect(order).toEqual([])
    batches[0].settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '0' }))
    await first
    await jest.advanceTimersByTimeAsync(0)
    expect(order).toEqual(['older', 'newer'])
    expect(accumulator.unsettled()).toEqual([])
    batches.splice(0).forEach((batch) => batch.retire())
  })

  test("a forced close rejects every send waiting for its partitions or lingering as unsent, with or without a deadline, and keeps a held assignment's shell until it returns", async () => {
    const assigned = deferred()
    const { accumulator, admission } = inventory({
      linger: 60000,
      partition: async (send) =>
        send.records[0].topic === 'held' ? assigned.promise : byMessage(send),
    })
    const held = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: 'held', messages: records(0, 'a') }],
    })
    const lingering = append(accumulator, records(0, 'b'))
    const settled = settledFlag(Promise.allSettled([held, lingering]))
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toBe(1)
    const [snapshot] = accumulator.unsettled()

    accumulator.forceClose()
    await jest.advanceTimersByTimeAsync(0)
    expect(settled()).toBe(true)
    for (const sent of [held, lingering]) {
      await expect(sent).rejects.toMatchObject({
        name: 'KafkaJSProducerClosedError',
        outcome: 'unsent',
      })
    }
    await snapshot
    expect(jest.getTimerCount()).toBe(0)
    expect(admission.reserved).toBe(1536)
    assigned.resolve([{ topic: 'held', partition: 0, messages: [] }])
    await jest.advanceTimersByTimeAsync(0)
    expect(admission.reserved).toBe(0)
    expect(accumulator.unsettled()).toEqual([])
  })

  test('close settles when the last send waiting for its partitions expires, and its shell is freed once the assignment returns', async () => {
    const assigned = deferred()
    const { accumulator, admission } = inventory({
      deadline: 1000,
      partition: () => assigned.promise,
    })
    const held = append(accumulator, records(0, 'a'))
    held.catch(() => {})
    await jest.advanceTimersByTimeAsync(0)
    const closed = settledFlag(accumulator.close())

    await jest.advanceTimersByTimeAsync(999)
    expect(closed()).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    await expect(held).rejects.toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
    expect(closed()).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
    expect(admission.reserved).toBeGreaterThan(0)

    assigned.resolve([{ topic: TOPIC, partition: 0, messages: [] }])
    await jest.advanceTimersByTimeAsync(0)
    expect(admission.reserved).toBe(0)
    expect(accumulator.unsettled()).toEqual([])
  })

  test('a send granted room after a wait goes on, and one granted as the producer closes is refused unsent and gives the room back', async () => {
    const generation = new Generation()
    const { accumulator, admission, batches } = inventory({
      capacity: 4096,
      getGeneration: () => generation,
    })
    const value = (prefix) => [{ partition: 0, value: prefix.repeat(1500) }]
    /** Answers and retires the batch handed on first. */
    const answer = () => {
      const batch = batches.shift()
      batch.settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '0' }))
      batch.retire()
    }
    const first = append(accumulator, value('a'))
    const second = append(accumulator, value('b'))
    await jest.advanceTimersByTimeAsync(0)
    answer()
    await first
    await jest.advanceTimersByTimeAsync(0)
    const third = append(accumulator, value('c'))
    const settled = settledFlag(third.catch(() => {}))
    expect(admission.waitingBytes).toBeGreaterThan(0)

    answer()
    expect(admission.waitingBytes).toBe(0)
    generation.close(new KafkaJSError('closed'))
    await jest.advanceTimersByTimeAsync(0)
    expect(settled()).toBe(true)
    await expect(third).rejects.toMatchObject({
      name: 'KafkaJSProducerClosedError',
      outcome: 'unsent',
    })
    await second
    expect(admission.reserved).toBe(0)
    expect(accumulator.unsettled()).toEqual([])
  })

  test('a send granted room as the lifecycle closes with a fatal error rejects with that exact error and gives the room back', async () => {
    const generation = new Generation()
    const { accumulator, admission, batches } = inventory({
      capacity: 4096,
      getGeneration: () => generation,
    })
    const fatal = new KafkaJSApplicationRecoverableError('fenced', {
      cause: createErrorFromCode(47),
    })
    const value = (prefix) => [{ partition: 0, value: prefix.repeat(1500) }]
    /** Answers and retires the batch handed on first. */
    const answer = () => {
      const batch = batches.shift()
      batch.settlers.forEach((settler) => settler.resolve({ partition: 0, baseOffset: '0' }))
      batch.retire()
    }
    const first = append(accumulator, value('a'))
    const second = append(accumulator, value('b'))
    await jest.advanceTimersByTimeAsync(0)
    answer()
    await first
    await jest.advanceTimersByTimeAsync(0)
    const third = append(accumulator, value('c'))
    const settled = settledFlag(third.catch(() => {}))
    expect(admission.waitingBytes).toBeGreaterThan(0)

    answer()
    expect(admission.waitingBytes).toBe(0)
    generation.close(fatal)
    await jest.advanceTimersByTimeAsync(0)
    expect(settled()).toBe(true)
    await expect(third).rejects.toBe(fatal)
    await second
    expect(batches).toHaveLength(0)
    expect(admission.reserved).toBe(0)
    expect(accumulator.unsettled()).toEqual([])
  })
})

describe('Producer > the accumulator > a join publishes all of a send or nothing', () => {
  const {
    watch,
    flushAll,
    countersOf,
    quiescent,
    bufferedProducer,
  } = require('../../__tests__/fixtures/bufferHarness')
  const { wire } = require('../recordSize')
  const { KafkaJSProducerClosedError } = require('../../../errors')

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask', 'nextTick', 'setImmediate'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * An accumulator over a 1 MiB buffer and a lifecycle of its own, whose send path calls `handOff`
   * with the batches it is given, records their values, answers each at base offset 100 and
   * retires it.
   */
  const joining = ({
    linger = LINGER,
    batchSize = BATCH_SIZE,
    partition = byMessage,
    lingers,
    leaderOf,
    budget,
    getGeneration,
    handOff = () => {},
  } = {}) => {
    const admission = new Admission({ capacity: 1048576, maxBlockMs: 0 })
    const generation = new Generation()
    const requests = []
    const sendPartitioned = jest.fn(async ({ batches }) => {
      handOff(batches)
      requests.push(
        batches.map(({ topic, partition: id, messages }) => ({
          [`${topic}/${id}`]: messages.map(({ value }) => value),
        }))
      )
      for (const batch of batches) {
        const metadata = {
          topicName: batch.topic,
          partition: batch.partition,
          errorCode: 0,
          baseOffset: '100',
        }
        batch.settlers.forEach((settler) => settler.resolve(metadata))
        batch.retire()
      }
    })
    const accumulator = new Accumulator({
      linger,
      batchSize,
      partition,
      sendPartitioned,
      lingers,
      leaderOf,
      admission,
      budget,
      getGeneration: getGeneration ?? (() => generation),
    })
    return { accumulator, admission, generation, requests, sendPartitioned }
  }

  /** The metadata a send to partition `partition` of the topic resolves with. */
  const metadataOf = (partition, baseOffset) => ({
    topicName: TOPIC,
    partition,
    errorCode: 0,
    baseOffset,
  })

  test.each([
    [
      'lingers() throws for the second partition',
      (failure) => {
        let calls = 0
        return {
          lingers: () => {
            calls += 1
            if (calls === 3) throw failure
            return true
          },
        }
      },
    ],
    [
      'reading a record of the second partition throws',
      (failure) => ({
        partition: async (send) => {
          const entries = await byMessage(send)
          for (const { messages } of entries) {
            for (const message of messages) {
              if (message.value === 'poisoned') {
                Object.defineProperty(message, 'value', {
                  get() {
                    throw failure
                  },
                })
              }
            }
          }
          return entries
        },
      }),
    ],
  ])(
    'when %s, the send rejects and every batch, charge and the next send are as they were before it',
    async (_, failing) => {
      const failure = new Error('join failure')
      const { accumulator, admission, requests } = joining(failing(failure))
      const earlier = watch(append(accumulator, records(0, 'earlier')))
      await flushAll()
      const before = countersOf(admission)
      expect(accumulator.depthOf(TOPIC, 0)).toBe(1)

      const failed = watch(
        append(accumulator, [...records(0, 'failed'), { partition: 1, value: 'poisoned' }])
      )
      await flushAll()

      expect(failed.state).toBe('rejected')
      expect(failed.value).toBe(failure)
      expect(countersOf(admission)).toEqual(before)
      expect([accumulator.depthOf(TOPIC, 0), accumulator.depthOf(TOPIC, 1)]).toEqual([1, 0])
      const later = watch(append(accumulator, records(0, 'later')))
      await jest.advanceTimersByTimeAsync(LINGER)
      await flushAll()

      expect(requests).toEqual([[{ [`${TOPIC}/0`]: ['earlier-0', 'later-0'] }]])
      expect(earlier.value).toEqual([metadataOf(0, '100')])
      expect(later.value).toEqual([metadataOf(0, '101')])
      const closed = watch(accumulator.close())
      await flushAll()
      expect(closed.state).toBe('fulfilled')
      expect(countersOf(admission)).toEqual(quiescent(admission))
    }
  )

  test.each([
    ['a lingers()', 'closes the producer', 'lingers', false],
    ['a lingers()', 'closes the producer with a fatal error', 'lingers', true],
    ['a leaderOf()', 'closes the producer', 'leaderOf', false],
    ['a leaderOf()', 'closes the producer with a fatal error', 'leaderOf', true],
  ])(
    '%s that %s while a send joins sees none of it published, and the send is refused with nothing handed on, charged or left to fire',
    async (_, __, callback, fatal) => {
      const armed = new KafkaJSApplicationRecoverableError('fenced', {
        cause: createErrorFromCode(47),
      })
      /** Each call's view of the send's two partitions; the first call closes the producer. */
      const seen = []
      const close = () => {
        const { accumulator, generation } = fixture
        seen.push([accumulator.depthOf(TOPIC, 0), accumulator.depthOf(TOPIC, 1)])
        if (seen.length > 1) return
        if (fatal) {
          generation.close(armed)
          accumulator.forceClose(armed)
        } else {
          generation.close(
            new KafkaJSProducerClosedError('The producer was closed', { outcome: 'unknown' })
          )
          accumulator.forceClose()
        }
      }
      const fixture = joining({
        lingers:
          callback === 'lingers'
            ? () => {
                close()
                return true
              }
            : undefined,
        leaderOf:
          callback === 'leaderOf'
            ? () => {
                close()
                return 1
              }
            : undefined,
      })
      const { accumulator, admission, sendPartitioned } = fixture

      const sent = watch(append(accumulator, [...records(0, 'a'), ...records(1, 'b')]))
      await flushAll()

      expect(seen).toEqual([
        [0, 0],
        [0, 0],
      ])
      expect(sent.state).toBe('rejected')
      if (fatal) {
        expect(sent.value).toBe(armed)
      } else {
        expect(sent.value).toBeInstanceOf(KafkaJSProducerClosedError)
        expect(sent.value).toMatchObject({
          message: 'The producer was closed before the send joined a batch',
          outcome: 'unsent',
        })
      }
      expect(jest.getTimerCount()).toBe(0)
      await jest.advanceTimersByTimeAsync(LINGER)
      await flushAll()
      expect(sendPartitioned).not.toHaveBeenCalled()
      expect(countersOf(admission)).toEqual(quiescent(admission))
    }
  )

  test('lingers() and leaderOf() run before the lifecycle fence, and nothing runs between the fence and the last batch the send links', async () => {
    const events = []
    const lifecycle = {
      get closed() {
        events.push('fence')
        return false
      },
      error: null,
      check() {},
    }
    const { accumulator, admission } = joining({
      lingers: () => {
        events.push('lingers')
        return true
      },
      leaderOf: (topic, partition) => {
        events.push(`leaderOf ${partition}`)
        return 1
      },
      getGeneration: () => lifecycle,
      handOff: () => events.push('hand-off'),
    })
    const debit = admission.debit
    admission.debit = (bytes) => {
      events.push('link')
      debit.call(admission, bytes)
    }

    const sent = watch(append(accumulator, [...records(0, 'a'), ...records(1, 'b')]))
    await flushAll()

    expect(events).toEqual([
      'lingers',
      'lingers',
      'leaderOf 0',
      'leaderOf 1',
      'fence',
      'link',
      'link',
    ])
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(events.slice(7)).toEqual(['hand-off'])
    expect(sent.value).toEqual([metadataOf(0, '100'), metadataOf(1, '100')])
  })

  test('a batch the send displaces is handed on only after every batch of the send is linked', async () => {
    /** At each hand-off: its batches' values, then how many batches wait for partitions 0 and 1. */
    const seen = []
    const fixture = joining({
      handOff: (batches) =>
        seen.push([
          batches.map(({ messages }) => messages.map(({ value }) => value)),
          fixture.accumulator.depthOf(TOPIC, 0),
          fixture.accumulator.depthOf(TOPIC, 1),
        ]),
    })
    const { accumulator, requests } = fixture
    const earlier = watch(append(accumulator, records(0, 'earlier')))
    await flushAll()

    const sent = watch(append(accumulator, [...records(0, 'a'), ...records(1, 'b')], { acks: 1 }))
    await flushAll()

    expect(seen).toEqual([[[['earlier-0']], 2, 1]])
    expect(earlier.value).toEqual([metadataOf(0, '100')])
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(requests).toEqual([
      [{ [`${TOPIC}/0`]: ['earlier-0'] }],
      [{ [`${TOPIC}/0`]: ['a-0'] }, { [`${TOPIC}/1`]: ['b-0'] }],
    ])
    expect(sent.value).toEqual([metadataOf(0, '100'), metadataOf(1, '100')])
  })

  test('a hand-off that closes the producer at once finds every batch of the joining send linked, so the send is refused unsent and none of it is handed on', async () => {
    let closing = true
    const fixture = joining({
      handOff: () => {
        if (!closing) return
        closing = false
        fixture.generation.close(
          new KafkaJSProducerClosedError('The producer was closed', { outcome: 'unknown' })
        )
        fixture.accumulator.forceClose()
      },
    })
    const { accumulator, admission, requests } = fixture
    const earlier = watch(append(accumulator, records(0, 'earlier')))
    await flushAll()

    const sent = watch(append(accumulator, [...records(0, 'a'), ...records(1, 'b')], { acks: 1 }))
    await flushAll()

    expect(sent.state).toBe('rejected')
    expect(sent.value).toMatchObject({
      name: 'KafkaJSProducerClosedError',
      message: 'The producer was closed before sending',
      outcome: 'unsent',
    })
    expect(earlier.value).toEqual([metadataOf(0, '100')])
    expect([accumulator.depthOf(TOPIC, 0), accumulator.depthOf(TOPIC, 1)]).toEqual([0, 0])
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(requests).toEqual([[{ [`${TOPIC}/0`]: ['earlier-0'] }]])
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test("a send path that throws while a displaced batch is handed on rejects that batch's sends, and the joining send goes out on its linger", async () => {
    const { accumulator, admission, requests, sendPartitioned } = joining()
    const failure = new Error('send path failure')
    sendPartitioned.mockImplementationOnce(() => {
      throw failure
    })
    const earlier = watch(append(accumulator, records(0, 'earlier')))
    await flushAll()

    const joined = watch(append(accumulator, records(0, 'joining'), { acks: 1 }))
    await flushAll()

    expect(earlier.state).toBe('rejected')
    expect(earlier.value).toBe(failure)
    expect(joined.state).toBe('pending')
    expect(accumulator.depthOf(TOPIC, 0)).toBe(1)
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(sendPartitioned).toHaveBeenCalledTimes(2)
    expect(requests).toEqual([[{ [`${TOPIC}/0`]: ['joining-0'] }]])
    expect(joined.value).toEqual([metadataOf(0, '100')])
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('records keep their order in each partition across sends, through a displaced batch and runs cut at the chunk capacity', async () => {
    const capacity = 2 * wire({ value: 'a-0' })
    const { accumulator, admission, requests } = joining({
      budget: { maxRequestSize: 1000, chunkCapacity: () => capacity },
    })
    const first = watch(append(accumulator, records(0, 'e')))
    await flushAll()

    const second = watch(append(accumulator, records(0, 'r', 3)))
    await flushAll()
    expect(requests).toEqual([
      [{ [`${TOPIC}/0`]: ['e-0'] }],
      [{ [`${TOPIC}/0`]: ['r-0', 'r-1'] }, { [`${TOPIC}/0`]: ['r-2'] }],
    ])
    const third = watch(append(accumulator, [...records(0, 't'), ...records(1, 'u')]))
    const fourth = watch(append(accumulator, records(0, 'v')))
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()

    expect(requests.slice(2)).toEqual([
      [{ [`${TOPIC}/0`]: ['t-0', 'v-0'] }, { [`${TOPIC}/1`]: ['u-0'] }],
    ])
    expect([first.value, second.value, third.value, fourth.value]).toEqual([
      [metadataOf(0, '100')],
      [metadataOf(0, '100')],
      [metadataOf(0, '100'), metadataOf(1, '100')],
      [metadataOf(0, '101')],
    ])
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test.each([
    ["fits the first run's batch", BATCH_SIZE, [[{ [`${TOPIC}/0`]: ['a-0', 'b-0'] }]]],
    [
      "does not fit the first run's batch",
      5,
      [[{ [`${TOPIC}/0`]: ['a-0'] }], [{ [`${TOPIC}/0`]: ['b-0'] }]],
    ],
  ])(
    'a send with two runs for one partition whose second %s leaves one batch waiting, and reads its leader once',
    async (_, batchSize, expected) => {
      const leaderOf = jest.fn(() => 1)
      const { accumulator, admission, requests } = joining({ batchSize, leaderOf })

      const sent = watch(
        accumulator.append({
          acks: -1,
          timeout: 30000,
          compression: 0,
          topicMessages: [
            { topic: TOPIC, messages: records(0, 'a') },
            { topic: TOPIC, messages: records(0, 'b') },
          ],
        })
      )
      await flushAll()

      expect(leaderOf.mock.calls).toEqual([[TOPIC, 0]])
      expect(jest.getTimerCount()).toBe(1)
      await jest.advanceTimersByTimeAsync(LINGER)
      await flushAll()
      expect(requests).toEqual(expected)
      expect(sent.state).toBe('fulfilled')
      expect(countersOf(admission)).toEqual(quiescent(admission))
    }
  )

  test('a send that joins a due batch held behind its partition’s batch in flight reads no leader and leaves it held until that batch settles', async () => {
    const answer = deferred()
    const handed = []
    const sendPartitioned = async ({ batches }) => {
      handed.push(batches.map(({ messages }) => messages.map(({ value }) => value)))
      if (handed.length === 1) {
        await answer.promise
      }
      batches.forEach(({ partition, settlers }) =>
        settlers.forEach((settler) => settler.resolve({ partition, baseOffset: '0' }))
      )
    }
    const leaderOf = jest.fn(() => 1)
    const { accumulator } = createAccumulator({ sendPartitioned, holds: () => true, leaderOf })
    append(accumulator, records(0, 'x'))
    await jest.advanceTimersByTimeAsync(LINGER)
    append(accumulator, records(0, 'y'))
    await jest.advanceTimersByTimeAsync(LINGER)
    expect(handed).toEqual([[['x-0']]])

    const joined = watch(append(accumulator, records(0, 'z')))
    await flushAll()

    expect(leaderOf).toHaveBeenCalledTimes(2)
    expect(handed).toEqual([[['x-0']]])
    expect(jest.getTimerCount()).toBe(0)
    answer.resolve()
    await flushAll()
    expect(handed).toEqual([[['x-0']], [['y-0', 'z-0']]])
    expect(joined.value).toEqual([{ partition: 0, baseOffset: '1' }])
  })

  /**
   * A producer without idempotence over two partitions, with `batchSize: 1` and a custom
   * partitioner that sends `a` to partition 0 and `b` to partition 1, after `change(message)` ran
   * on `b`; its cluster counts the topic pins it holds.
   */
  const changingProducer = (change) => {
    const cluster = createFakeCluster({ partitions: 2 })
    const pins = new Map()
    cluster.holdTopic = (topic) => pins.set(topic, (pins.get(topic) ?? 0) + 1)
    cluster.releaseTopic = (topic) =>
      pins.get(topic) === 1 ? pins.delete(topic) : pins.set(topic, pins.get(topic) - 1)
    const fixture = bufferedProducer({
      cluster,
      idempotent: false,
      batchSize: 1,
      createPartitioner:
        () =>
        ({ message }) => {
          if (message.value !== 'b') return 0
          change(message)
          return 1
        },
    })
    return { ...fixture, pins }
  }

  test('through a producer, a timestamp getter a custom partitioner puts on the second of two partitions that throws rejects the send with its error, writes nothing and leaves nothing charged or pinned', async () => {
    const failure = new RangeError('timestamp read failed')
    const { producer, cluster, admission, pins } = changingProducer((message) =>
      Object.defineProperty(message, 'timestamp', {
        get() {
          throw failure
        },
      })
    )
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'a' }, { value: 'b' }] }))
    await flushAll()

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBe(failure)
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    await producer.disconnect()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('through a producer, a timestamp getter a custom partitioner puts on a record that disconnects the producer at once refuses the send unsent, writes nothing and leaves nothing charged or pinned', async () => {
    let disconnected = null
    const fixture = changingProducer((message) =>
      Object.defineProperty(message, 'timestamp', {
        get() {
          disconnected ??= fixture.producer.disconnect({ timeout: 0 })
          return 1
        },
      })
    )
    const { producer, cluster, admission, pins } = fixture
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'a' }, { value: 'b' }] }))
    await flushAll()

    expect(disconnected).toBeInstanceOf(Promise)
    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSProducerClosedError)
    expect(sent.value).toMatchObject({
      message: 'The producer was closed before the send joined a batch',
      outcome: 'unsent',
    })
    await disconnected
    await flushAll()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
  })
})
