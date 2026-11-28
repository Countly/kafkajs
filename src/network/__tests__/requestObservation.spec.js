const RequestQueue = require('../requestQueue')
const Connection = require('../connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const events = require('../instrumentationEvents')
const { ON_DECODED, acceptedIndex } = require('../../broker/privateOptions')

/** @returns {object} `acceptedIndex(decoded)` as plain data: accepted partitions by topic */
const indexed = (decoded) =>
  Object.fromEntries([...acceptedIndex(decoded)].map(([topic, cells]) => [topic, [...cells]]))
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger, createFakeSocketFactory, respondWithFrame } = require('testHelpers')

const V10_OK = require('../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v10-ok.json')
const V9_OK = require('../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v9-basic.json')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_ID = 'ffffffff-0506-0708-090a-0b0c0d0e0f10'

let nextCorrelation = 0
/** @returns {{ correlationId: number, resolve: jest.Mock, reject: jest.Mock }} a queue entry */
const entry = () => ({ correlationId: nextCorrelation++, resolve: jest.fn(), reject: jest.fn() })

const queues = []
/**
 * @param {object} [extra] RequestQueue options
 * @returns {{ queue: RequestQueue, logger: object }} a queue with one in-flight slot
 */
const createQueue = (extra = {}) => {
  const logger = { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
  const queue = new RequestQueue({
    maxInFlightRequests: 1,
    requestTimeout: 50,
    enforceRequestTimeout: false,
    clientId: 'client',
    broker: 'broker:9092',
    logger,
    ...extra,
  })
  queues.push(queue)
  return { queue, logger }
}

/**
 * @param {object} [options] extra `Connection` options
 * @returns {Promise<{ connection: Connection, state: object }>} a connected fake-socket connection
 */
const connected = async (options = {}) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: 'broker',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'client',
    connectionTimeout: 1000,
    requestTimeout: 30000,
    enforceRequestTimeout: false,
    ...options,
  })
  await connection.connect()
  return { connection, state }
}

/**
 * @param {object} response the response codec
 * @param {object} [extra] more protocol fields
 * @returns {object} a request that encodes nothing, answered by `response`
 */
const protocol = (response, extra = {}) => ({
  request: {
    apiKey: apiKeys.ApiVersions,
    apiVersion: 0,
    apiName: 'ApiVersions',
    encode: async () => new Encoder(),
  },
  response,
  ...extra,
})

const flush = () => new Promise((resolve) => setImmediate(resolve))

describe('Network > request observation', () => {
  afterEach(() => {
    for (const queue of queues.splice(0)) queue.destroy()
  })

  describe('RequestQueue enrollment', () => {
    test.each([
      [
        'the enqueue log',
        (logger) => {
          logger.debug.mockImplementation(() => {
            throw new Error('log failed')
          })
          return () => logger.debug.mockImplementation(() => {})
        },
      ],
      [
        'a queue-size listener',
        (logger, emitter) =>
          emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, () => {
            throw new Error('listener failed')
          }),
      ],
    ])(
      'a queued request survives a throw from %s and is sent once with its promise unsettled',
      (label, arm) => {
        const emitter = new InstrumentationEventEmitter()
        const { queue, logger } = createQueue({ instrumentationEmitter: emitter })
        const first = entry()
        queue.push({ entry: first, expectResponse: true, sendRequest: () => {} })
        const queued = entry()
        const sendQueued = jest.fn()
        const disarm = arm(logger, emitter)

        expect(() =>
          queue.push({ entry: queued, expectResponse: true, sendRequest: sendQueued })
        ).not.toThrow()
        expect(queue.pending.map((request) => request.entry)).toEqual([queued])
        expect(logger.error).toHaveBeenCalledWith(
          'A queue observer failed',
          expect.objectContaining({ broker: 'broker:9092' })
        )

        disarm()
        queue.fulfillRequest({ correlationId: first.correlationId, payload: null, size: 0 })
        expect(sendQueued).toHaveBeenCalledTimes(1)
        expect(queued.reject).not.toHaveBeenCalled()
        queue.fulfillRequest({ correlationId: queued.correlationId, payload: null, size: 0 })
        expect(queued.resolve).toHaveBeenCalledTimes(1)
        expect(queue.isIdle()).toBe(true)
      }
    )

    test('a request that expects no response completes even when its log throws', () => {
      const { queue, logger } = createQueue()
      logger.debug.mockImplementation(() => {
        throw new Error('log failed')
      })
      const request = entry()

      expect(() =>
        queue.push({ entry: request, expectResponse: false, sendRequest: () => {} })
      ).not.toThrow()
      expect(request.resolve).toHaveBeenCalledWith(
        expect.objectContaining({ size: 0, payload: null })
      )
      expect(request.reject).not.toHaveBeenCalled()
      expect(queue.isIdle()).toBe(true)
    })

    test('a throttle diagnostic that throws still records the throttle', () => {
      const { queue, logger } = createQueue()
      logger.debug.mockImplementation(() => {
        throw new Error('log failed')
      })
      const before = Date.now()

      expect(() => queue.maybeThrottle(500)).not.toThrow()
      expect(queue.throttledUntil).toBeGreaterThanOrEqual(before + 500)
    })
  })

  describe('RequestQueue socket write failure', () => {
    test('a write that throws at once rejects the request, propagates and leaves no in-flight entry', () => {
      const { queue } = createQueue()
      const failure = new Error('write failed')
      const request = entry()

      expect(() =>
        queue.push({
          entry: request,
          expectResponse: true,
          sendRequest: () => {
            throw failure
          },
        })
      ).toThrow(failure)
      expect(request.reject).toHaveBeenCalledWith(failure)
      expect(queue.inflight.size).toBe(0)
      expect(queue.isIdle()).toBe(true)
    })

    test('a write of a pending request that throws rejects only that request and frees its slot', () => {
      const { queue } = createQueue()
      const failure = new Error('write failed')
      const first = entry()
      const second = entry()
      queue.push({ entry: first, expectResponse: true, sendRequest: () => {} })
      queue.push({
        entry: second,
        expectResponse: true,
        sendRequest: () => {
          throw failure
        },
      })

      expect(() =>
        queue.fulfillRequest({ correlationId: first.correlationId, payload: null, size: 0 })
      ).toThrow(failure)
      expect(first.resolve).toHaveBeenCalledTimes(1)
      expect(second.reject).toHaveBeenCalledWith(failure)
      expect(queue.inflight.size).toBe(0)
      expect(queue.isIdle()).toBe(true)
    })

    test('a queued write that throws on the throttle timer settles the queue through its owner and escapes nothing', () => {
      jest.useFakeTimers()
      try {
        const onDispatchFailure = jest.fn()
        const { queue } = createQueue({ maxInFlightRequests: 2, onDispatchFailure })
        const failure = new Error('write failed')
        const first = entry()
        const second = entry()
        const sendSecond = jest.fn()
        queue.maybeThrottle(500)
        queue.push({
          entry: first,
          expectResponse: true,
          sendRequest: () => {
            throw failure
          },
        })
        queue.push({ entry: second, expectResponse: true, sendRequest: sendSecond })
        expect(queue.pending).toHaveLength(2)

        expect(() => jest.advanceTimersByTime(1000)).not.toThrow()
        expect(first.reject).toHaveBeenCalledWith(failure)
        expect(onDispatchFailure).toHaveBeenCalledTimes(1)
        expect(onDispatchFailure).toHaveBeenCalledWith(failure)
        expect(sendSecond).not.toHaveBeenCalled()
        expect(queue.inflight.size).toBe(0)
        expect(queue.throttleCheckTimeoutId).toBeNull()
      } finally {
        jest.useRealTimers()
      }
    })

    test('without an owner, a queued write that throws on the throttle timer rejects every queued request', () => {
      jest.useFakeTimers()
      try {
        const { queue } = createQueue({ maxInFlightRequests: 2 })
        const failure = new Error('write failed')
        const first = entry()
        const second = entry()
        queue.maybeThrottle(500)
        queue.push({
          entry: first,
          expectResponse: true,
          sendRequest: () => {
            throw failure
          },
        })
        queue.push({ entry: second, expectResponse: true, sendRequest: jest.fn() })

        expect(() => jest.advanceTimersByTime(1000)).not.toThrow()
        expect(first.reject).toHaveBeenCalledWith(failure)
        expect(second.reject).toHaveBeenCalledWith(failure)
        expect(queue.isIdle()).toBe(true)
        expect(queue.throttleCheckTimeoutId).toBeNull()
      } finally {
        jest.useRealTimers()
      }
    })

    test('a throwing dispatch diagnostic on the throttle timer escapes nothing and settles the queue through its owner', () => {
      jest.useFakeTimers()
      try {
        const onDispatchFailure = jest.fn()
        const { queue, logger } = createQueue({ maxInFlightRequests: 2, onDispatchFailure })
        const first = entry()
        const second = entry()
        queue.maybeThrottle(500)
        queue.push({ entry: first, expectResponse: true, sendRequest: jest.fn() })
        queue.push({ entry: second, expectResponse: true, sendRequest: jest.fn() })
        const failure = new Error('log failed')
        logger.debug.mockImplementation((message) => {
          if (message === 'Consumed pending request') throw failure
        })

        expect(() => jest.advanceTimersByTime(1000)).not.toThrow()
        expect(onDispatchFailure).toHaveBeenCalledWith(failure)
        expect(first.reject).toHaveBeenCalledWith(failure)
        expect(second.reject).toHaveBeenCalledWith(failure)
        expect(queue.isIdle()).toBe(true)
        expect(queue.throttleCheckTimeoutId).toBeNull()
      } finally {
        jest.useRealTimers()
      }
    })

    test('a connection whose queued write throws on the throttle timer ends its generation and settles every request', async () => {
      const { connection, state } = await connected()
      // The throttle timer is advanced, not slept: under load a real 30 ms could pass before the
      // queue is inspected.
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
      try {
        connection.requestQueue.maybeThrottle(30)
        const answer = { decode: async (payload) => payload, parse: async (payload) => payload }
        const first = connection.send(protocol(answer))
        const second = connection.send(protocol(answer))
        const settled = Promise.allSettled([first, second])
        await flush()
        expect(connection.requestQueue.pending).toHaveLength(2)
        const failure = new Error('write failed')
        state.socket.write = () => {
          throw failure
        }

        jest.advanceTimersByTime(30)
        const [one, two] = await settled
        expect(one).toEqual({ status: 'rejected', reason: failure })
        expect(two).toEqual({ status: 'rejected', reason: failure })
        expect(connection.isConnected()).toBe(false)
        expect(connection.requestQueue.isIdle()).toBe(true)
        expect(connection.requestQueue.throttleCheckTimeoutId).toBeNull()
      } finally {
        jest.useRealTimers()
      }
    })

    test('a correlation collision rejects the new request and keeps the existing one in flight', () => {
      const { queue } = createQueue({ maxInFlightRequests: 2 })
      const first = entry()
      queue.push({ entry: first, expectResponse: true, sendRequest: () => {} })
      const existing = queue.inflight.get(first.correlationId)
      const duplicate = { ...entry(), correlationId: first.correlationId }

      expect(() =>
        queue.push({ entry: duplicate, expectResponse: true, sendRequest: () => {} })
      ).toThrow('Correlation id already exists')
      expect(duplicate.reject).toHaveBeenCalledTimes(1)
      expect(queue.inflight.get(first.correlationId)).toBe(existing)
    })
  })

  describe('Connection decoded-reply observation', () => {
    test('observes the decoded reply before a response guard refuses the caller', async () => {
      const { connection, state } = await connected()
      const order = []
      const refusal = new Error('caller refused')
      const reply = connection.send(
        protocol(
          {
            decode: async (payload) => ({ body: payload.toString() }),
            parse: async (decoded) => {
              order.push('parse')
              return decoded
            },
          },
          {
            onDecoded: (decoded) => order.push(['decoded', decoded.body]),
            responseGuard: () => {
              order.push('guard')
              return refusal
            },
          }
        )
      )
      await flush()
      const [id] = connection.requestQueue.inflight.keys()
      respondWithFrame(state, id, 0, Buffer.from('ack'))

      await expect(reply).rejects.toBe(refusal)
      expect(order).toEqual([['decoded', 'ack'], 'guard'])
      await connection.disconnect({ drainTimeout: 0 })
    })

    test('a throwing response log neither hides a parsed reply nor masks a protocol error', async () => {
      const logger = newLogger()
      const { connection, state } = await connected({ logger })
      const realDebug = connection.logDebug
      const debug = jest.spyOn(connection, 'logDebug').mockImplementation((message, extra) => {
        if (message.startsWith('Response')) throw new Error('log failed')
        return realDebug(message, extra)
      })
      const logError = jest.spyOn(connection, 'logError').mockImplementation(() => {
        throw new Error('log failed')
      })
      const protocolError = new Error('protocol error')
      const ok = connection.send(
        protocol({ decode: async (payload) => payload, parse: async () => 'parsed' })
      )
      const failed = connection.send(
        protocol({
          decode: async (payload) => payload,
          parse: async () => {
            throw protocolError
          },
        })
      )
      await flush()
      const [first, second] = connection.requestQueue.inflight.keys()
      respondWithFrame(state, first, 0, Buffer.from('a'))
      respondWithFrame(state, second, 0, Buffer.from('b'))

      await expect(ok).resolves.toBe('parsed')
      await expect(failed).rejects.toBe(protocolError)
      debug.mockRestore()
      logError.mockRestore()
      await connection.disconnect({ drainTimeout: 0 })
    })
  })

  describe('Broker offsetCommit acceptance', () => {
    let harness
    afterEach(async () => {
      await harness?.pool.destroy()
      harness = null
    })

    const commit = (topic) => ({
      groupId: 'test-group',
      groupGenerationId: 5,
      memberId: 'test-member',
      topics: [{ ...topic, partitions: [{ partition: 0, offset: '10' }] }],
    })

    test('hands the decoded reply to the private observer once, and the named cell is accepted', async () => {
      harness = createGroupApiBroker({
        versions: { [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion: 9 } },
        respond: () => Buffer.from(V9_OK.hex, 'hex'),
      })
      const seen = []

      await harness.broker.offsetCommit(commit({ topic: 'test-topic' }), {
        [ON_DECODED]: (decoded) => seen.push(indexed(decoded)),
      })

      expect(seen).toEqual([{ 'test-topic': [0] }])
    })

    test('a v10 reply naming a topic id the request did not carry is refused before the observer sees it', async () => {
      const hex = V10_OK.hex.replace(TOPIC_ID.replace(/-/g, ''), OTHER_ID.replace(/-/g, ''))
      harness = createGroupApiBroker({
        versions: { [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion: 10 } },
        respond: () => Buffer.from(hex, 'hex'),
      })
      const seen = []

      await expect(
        harness.broker.offsetCommit(commit({ topic: 'test-topic', topicId: TOPIC_ID }), {
          [ON_DECODED]: (decoded) => seen.push(indexed(decoded)),
        })
      ).rejects.toMatchObject({ retriable: true })

      expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([10])
      expect(seen).toEqual([])
    })

    test.each([
      [
        { responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }] },
        { 'test-topic': [0] },
      ],
      [
        { responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 7 }] }] },
        { 'test-topic': [] },
      ],
      [
        { responses: [{ topic: 'test-topic', partitions: [{ partition: 1, errorCode: 0 }] }] },
        { 'test-topic': [1] },
      ],
      [
        {
          responses: [
            { topic: null, topicId: OTHER_ID, partitions: [{ partition: 0, errorCode: 0 }] },
          ],
        },
        { null: [0] },
      ],
      [{ responses: [] }, {}],
      [
        {
          responses: [
            {
              topic: 'test-topic',
              partitions: [
                { partition: 0, errorCode: 0 },
                { partition: 0, errorCode: 12 },
              ],
            },
          ],
        },
        { 'test-topic': [] },
      ],
      [
        {
          responses: [
            {
              topic: 'test-topic',
              partitions: [
                { partition: 0, errorCode: 12 },
                { partition: 0, errorCode: 0 },
              ],
            },
          ],
        },
        { 'test-topic': [] },
      ],
      [
        {
          responses: [
            { topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] },
            { topic: 'test-topic', partitions: [{ partition: 0, errorCode: 12 }] },
          ],
        },
        { 'test-topic': [] },
      ],
      [
        {
          responses: [
            {
              topic: 'test-topic',
              partitions: [
                { partition: 0, errorCode: 0 },
                { partition: 0, errorCode: 0 },
              ],
            },
          ],
        },
        { 'test-topic': [0] },
      ],
    ])('acceptedIndex(%j) is %j', (decoded, accepted) => {
      expect(indexed(decoded)).toEqual(accepted)
    })
  })
})
