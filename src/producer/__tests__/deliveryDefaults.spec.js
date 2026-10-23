jest.mock('../../cluster', () => jest.fn())
jest.mock('../../retry', () => {
  const actual = jest.requireActual('../../retry')
  return Object.assign(
    jest.fn((...args) => actual(...args)),
    actual
  )
})

const Kafka = require('../../index')
const Cluster = require('../../cluster')
const createRetry = require('../../retry')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { LEVELS } = require('../../loggers')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSNonRetriableError,
} = require('../../errors')

const TOPIC = 'topic-a'
const MESSAGES = [{ key: 'key-0', value: 'value-0' }]
const REQUEST_TIMED_OUT = 7
const MAX_TIMER_DELAY = 2147483647

jest.setTimeout(10000)

/** A Kafka client over the fake cluster whose log lines are collected. */
const client = (options = {}, cluster = {}) => {
  const logs = []
  const clusters = []
  Cluster.mockImplementation((clusterOptions) => {
    clusters.push(createFakeCluster({ options: clusterOptions, ...cluster }))
    cleanupClusters.push(clusters[clusters.length - 1])
    return clusters[clusters.length - 1]
  })
  const kafka = new Kafka({
    brokers: ['localhost:9092'],
    logCreator: () => (entry) => logs.push(entry),
    logLevel: LEVELS.WARN,
    ...options,
  })
  return { kafka, logs, clusters }
}

const cleanupClusters = []
const producers = new Set()
const releases = []

/**
 * Holds a fake Produce answer until cleanup; rejecting it ends a red-source send without retry.
 * @returns {Promise<never>} pending answer
 */
const held = () =>
  new Promise((_, reject) => releases.push(() => reject(new KafkaJSNonRetriableError('released'))))

/**
 * Releases held calls and pending transaction retries, then closes producers after any assertion.
 * @returns {Promise<void>} all tracked producers disconnected
 */
const closeProducers = async () => {
  for (const cluster of cleanupClusters.splice(0)) cluster.broker.endTxn.mockResolvedValue({})
  const closing = [...producers].map((producer) => producer.disconnect())
  try {
    for (let pass = 0; pass < 3; pass++) {
      for (const release of releases.splice(0)) release()
      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY * 2 + 120000)
    }
  } finally {
    await Promise.all(closing)
    producers.clear()
  }
}

/**
 * A promise a test settles itself; cleanup releases it if the test did not.
 * @returns {{ promise: Promise<void>, release: () => void, reject: (error: Error) => void }}
 */
const gate = () => {
  const handle = {}
  handle.promise = new Promise((resolve, reject) => {
    handle.release = resolve
    handle.reject = reject
  })
  releases.push(() => handle.release())
  return handle
}

/** Whether a promise has settled, read without waiting on it. */
const settledFlag = (promise) => {
  let settled = false
  const flag = () => {
    settled = true
  }
  promise.then(flag, flag)
  return () => settled
}

describe('Producer > delivery defaults', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    createRetry.mockClear()
  })

  afterEach(async () => {
    try {
      await closeProducers()
    } finally {
      jest.useRealTimers()
    }
  })

  describe('the delivery deadline of a producer that sets none', () => {
    /** Sends to a producer whose answer never comes, and the time at which the send rejects. */
    const rejectionTime = async ({ kafkaOptions, producerOptions, limit }) => {
      const { kafka, logs } = client(kafkaOptions, { produce: held })
      const producer = kafka.producer({ idempotent: false, ...producerOptions })
      producers.add(producer)
      await producer.connect()
      const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      const done = settledFlag(sent)

      let elapsed = 0
      for (const step of [limit - 1, 1]) {
        await jest.advanceTimersByTimeAsync(step)
        elapsed += step
        if (done()) {
          return { elapsed, error: await sent, logs }
        }
      }
      return { elapsed, error: undefined, logs }
    }

    test('is 120 000 ms, counted from the send', async () => {
      const { elapsed, error } = await rejectionTime({
        kafkaOptions: { enforceRequestTimeout: false },
        limit: 120000,
      })

      expect(elapsed).toBe(120000)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    })

    test('is raised to linger + requestTimeout when that is larger, with one warning', async () => {
      const { elapsed, error, logs } = await rejectionTime({
        kafkaOptions: { requestTimeout: 100000, enforceRequestTimeout: false },
        producerOptions: { linger: 40000 },
        limit: 140000,
      })

      expect(elapsed).toBe(140000)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(
        logs.filter(({ log }) => /deliveryTimeout was not set/.test(log.message))
      ).toHaveLength(1)
    })

    test('is retry.maxTotalTime when that is set, with no floor', async () => {
      const { elapsed, error, logs } = await rejectionTime({
        kafkaOptions: { enforceRequestTimeout: false },
        producerOptions: { retry: { maxTotalTime: 5000 } },
        limit: 5000,
      })

      expect(elapsed).toBe(5000)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(logs.filter(({ log }) => /deliveryTimeout/.test(log.message))).toEqual([])
    })

    test('is retry.maxTotalTime of the client when the producer sets none', async () => {
      const { elapsed } = await rejectionTime({
        kafkaOptions: { retry: { maxTotalTime: 7000 }, enforceRequestTimeout: false },
        limit: 7000,
      })

      expect(elapsed).toBe(7000)
    })

    test('is an explicit deliveryTimeout over retry.maxTotalTime', async () => {
      const { elapsed } = await rejectionTime({
        kafkaOptions: { enforceRequestTimeout: false },
        producerOptions: { deliveryTimeout: 40000, retry: { maxTotalTime: 5000 } },
        limit: 40000,
      })

      expect(elapsed).toBe(40000)
    })

    test('may be longer than one timer delay, and expires at its time and not earlier', async () => {
      const { kafka } = client({ enforceRequestTimeout: false }, { produce: held })
      const producer = kafka.producer({ idempotent: false, linger: MAX_TIMER_DELAY })
      producers.add(producer)
      await producer.connect()
      const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      const done = settledFlag(sent)

      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY)
      expect(done()).toBe(false)
      await jest.advanceTimersByTimeAsync(29999)
      expect(done()).toBe(false)
      await jest.advanceTimersByTimeAsync(1)

      expect(done()).toBe(true)
      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      expect(await sent).toBeInstanceOf(KafkaJSDeliveryTimeout)
    })

    test('hands no retrier a time outside 1 to 2147483647 ms, and never an undefined option', async () => {
      const { kafka } = client({ enforceRequestTimeout: false }, { produce: held })
      const producer = kafka.producer({
        idempotent: false,
        linger: MAX_TIMER_DELAY,
        retry: { initialRetryTime: undefined, factor: undefined },
      })
      producers.add(producer)
      await producer.connect()
      const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY + 30000)
      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      await sent

      const options = createRetry.mock.calls.map(([found]) => found)
      expect(options.length).toBeGreaterThan(0)
      for (const found of options) {
        expect(Object.values(found).every((value) => value !== undefined)).toBe(true)
        if (found.maxTotalTime != null) {
          expect(found.maxTotalTime).toBeGreaterThanOrEqual(1)
          expect(found.maxTotalTime).toBeLessThanOrEqual(MAX_TIMER_DELAY)
        }
      }
    })

    test('past one timer delay is not cut short by a failed wait for the metadata', async () => {
      const { kafka, clusters } = client({
        requestTimeout: MAX_TIMER_DELAY,
        enforceRequestTimeout: false,
      })
      const producer = kafka.producer({
        idempotent: false,
        linger: 40000,
        retry: { initialRetryTime: 100, factor: 0 },
      })
      producers.add(producer)
      await producer.connect()
      const metadata = gate()
      clusters[0].refreshMetadataIfNecessary.mockImplementationOnce(() => metadata.promise)
      const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      const done = settledFlag(sent)

      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY - 5)
      metadata.reject(createErrorFromCode(REQUEST_TIMED_OUT))
      await jest.advanceTimersByTimeAsync(40004)
      expect(done()).toBe(false)
      await jest.advanceTimersByTimeAsync(1)

      expect(done()).toBe(true)
      const error = await sent
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      expect(clusters[0].broker.produce).not.toHaveBeenCalled()
    })
  })

  test('a held error refresh released after the public long deadline leaves no retry timer', async () => {
    const { kafka, clusters } = client({
      requestTimeout: MAX_TIMER_DELAY,
      enforceRequestTimeout: false,
    })
    const producer = kafka.producer({
      idempotent: false,
      linger: 40000,
      retry: { initialRetryTime: MAX_TIMER_DELAY, maxRetryTime: MAX_TIMER_DELAY, factor: 0 },
    })
    producers.add(producer)
    await producer.connect()
    const metadata = gate()
    clusters[0].refreshMetadataIfNecessary.mockRejectedValueOnce(createErrorFromCode(7))
    clusters[0].refreshMetadata.mockImplementationOnce(() => metadata.promise)
    const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
    await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY + 40000)
    const error = await sent
    expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(error.outcome).toBe('unsent')
    expect(clusters[0].refreshMetadata).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
    metadata.release()
    await jest.advanceTimersByTimeAsync(1)
    await producer.disconnect()
    producers.delete(producer)
    expect(jest.getTimerCount()).toBe(0)
  })

  describe('sends that finish partitioning out of order', () => {
    test('a lingering batch an older send joins goes out at the older send’s deadline', async () => {
      const { kafka, clusters } = client({ requestTimeout: 1000, enforceRequestTimeout: false })
      const producer = kafka.producer({
        idempotent: true,
        linger: 5000,
        retry: { maxTotalTime: 1000 },
      })
      producers.add(producer)
      await producer.connect()
      const metadata = gate()
      clusters[0].refreshMetadataIfNecessary.mockImplementationOnce(() => metadata.promise)
      const older = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      const olderDone = settledFlag(older)

      await jest.advanceTimersByTimeAsync(100)
      producer.send({ topic: TOPIC, messages: MESSAGES }).catch(() => {})
      await jest.advanceTimersByTimeAsync(400)
      metadata.release()
      await jest.advanceTimersByTimeAsync(500)

      expect(olderDone()).toBe(true)
      const error = await older
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      expect(clusters[0].broker.produce).not.toHaveBeenCalled()
    })
  })

  describe('the retries of a delivery', () => {
    /**
     * A producer whose every Produce is answered with a retriable error; the time of each attempt
     * is kept.
     */
    const failing = async ({ kafkaOptions, producerOptions }) => {
      const attempts = []
      const { kafka } = client(kafkaOptions, {
        produce: async () => {
          attempts.push(Date.now())
          throw createErrorFromCode(REQUEST_TIMED_OUT)
        },
      })
      const producer = kafka.producer({ idempotent: false, ...producerOptions })
      producers.add(producer)
      await producer.connect()
      const sent = producer.send({ topic: TOPIC, messages: MESSAGES }).catch((error) => error)
      return { attempts, sent }
    }

    const gaps = (attempts) => attempts.slice(1).map((time, index) => time - attempts[index])

    test('go on until the deadline, ten retriable answers and more, then reject with its cause', async () => {
      const { attempts, sent } = await failing({ producerOptions: { deliveryTimeout: 60000 } })

      await jest.advanceTimersByTimeAsync(60000)
      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      const error = await sent

      expect(attempts.length).toBeGreaterThan(10)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error).toMatchObject({ type: 'REQUEST_TIMED_OUT', code: REQUEST_TIMED_OUT })
    })

    test('start 100 ms apart, within the jitter, and never wait more than 1 000 ms', async () => {
      const { attempts, sent } = await failing({ producerOptions: { deliveryTimeout: 60000 } })

      await jest.advanceTimersByTimeAsync(60000)
      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      await sent

      expect(gaps(attempts)[0]).toBeGreaterThanOrEqual(80)
      expect(gaps(attempts)[0]).toBeLessThanOrEqual(120)
      expect(Math.max(...gaps(attempts))).toBeLessThanOrEqual(1000)
      expect(gaps(attempts).slice(-3)).toEqual([1000, 1000, 1000])
    })

    test.each([
      [
        'initialRetryTime',
        { initialRetryTime: 250 },
        (g) => [g[0] >= 240 && g[0] <= 260, Math.max(...g) === 1000],
      ],
      [
        'maxRetryTime',
        { maxRetryTime: 400 },
        (g) => [g[0] >= 80 && g[0] <= 120, Math.max(...g) === 400],
      ],
    ])(
      'an explicit %s is kept and leaves the other key to its default',
      async (_, retry, check) => {
        const { attempts, sent } = await failing({
          producerOptions: { deliveryTimeout: 60000, retry },
        })

        await jest.advanceTimersByTimeAsync(60000)
        expect(
          await Promise.race([
            sent.then(() => 'settled'),
            new Promise((resolve) => setImmediate(() => resolve('pending'))),
          ])
        ).toBe('settled')
        await sent

        expect(check(gaps(attempts))).toEqual([true, true])
      }
    )

    test('an explicit retry.retries stops the send after that many, whatever the deadline', async () => {
      const { attempts, sent } = await failing({ producerOptions: { retry: { retries: 3 } } })

      await jest.advanceTimersByTimeAsync(60000)

      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      expect(await sent).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(attempts).toHaveLength(4)
    })

    test('retries of the client are kept as the producer’s own', async () => {
      const { attempts, sent } = await failing({ kafkaOptions: { retry: { retries: 2 } } })

      await jest.advanceTimersByTimeAsync(60000)

      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      expect(await sent).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(attempts).toHaveLength(3)
    })

    test('an idempotent producer goes on the same way', async () => {
      const { attempts, sent } = await failing({
        producerOptions: { idempotent: true, deliveryTimeout: 40000 },
      })

      await jest.advanceTimersByTimeAsync(40000)

      expect(
        await Promise.race([
          sent.then(() => 'settled'),
          new Promise((resolve) => setImmediate(() => resolve('pending'))),
        ])
      ).toBe('settled')
      expect(await sent).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(attempts.length).toBeGreaterThan(10)
    })
  })

  describe('the requests of a transaction', () => {
    const features = (level) => ({
      finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
      finalizedFeaturesEpoch: 1,
    })

    test('keep the retrier’s defaults: a commit sends EndTxn once and then once per retry of the retrier', async () => {
      const { kafka, clusters } = client({}, { partitions: 2 })
      const producer = kafka.producer({ transactionalId: 'tx-a', idempotent: true })
      const { broker } = clusters[0]
      Object.assign(broker, features(2), { negotiatedVersion: () => 5 })
      broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
        producerId: '1000',
        producerEpoch: producerEpoch + 1,
      }))
      broker.endTxn.mockRejectedValue(createErrorFromCode(REQUEST_TIMED_OUT))
      producers.add(producer)
      await producer.connect()
      const transaction = await producer.transaction()
      const sending = transaction.send({
        topic: TOPIC,
        messages: [{ key: 'key-0', value: 'value-0', partition: 0 }],
      })
      await jest.advanceTimersByTimeAsync(10)
      await sending
      const committed = transaction.commit().catch((error) => error)

      await jest.advanceTimersByTimeAsync(60000)

      expect(broker.endTxn).toHaveBeenCalledTimes(createRetry.defaults.retries + 1)
      await committed
    })
  })
})
