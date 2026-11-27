const ProducerStateManager = require('..')
const { held } = require('../recordSize')
const { interleavings } = require('../../../../testHelpers/schedule')
const { createErrorFromCode } = require('../../../protocol/error')
const {
  KafkaJSConnectionClosedError,
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSProducerClosedError,
  KafkaJSRecordTooLargeError,
} = require('../../../errors')
const {
  bufferedProducer,
  collect,
  countersOf,
  deferred,
  flushAll,
  quiescent,
  succeeded,
  transactionalProducer,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'

/**
 * Makes the fake broker hold every Produce request until the test answers it: `answer(results?)`
 * succeeds (or returns `results`), `fail(error)` rejects, and `transmit()` runs the request's
 * pre-write guard as the connection would before writing it, rejecting with its refusal.
 */
const holdProduce = (cluster) => {
  const calls = []
  cluster.broker.produce.mockImplementation(
    (request) =>
      new Promise((resolve, reject) => {
        calls.push({
          request,
          answer: (results) => resolve(results ?? succeeded(request)),
          fail: reject,
          transmit: () => {
            const refusal = request.guard?.(64)
            if (refusal) reject(refusal)
            return refusal
          },
        })
      })
  )
  return calls
}

/** One partition's result with a Kafka error code. */
const failedResult = (code, partition = 0) => {
  const error = createErrorFromCode(code)
  error.topic = TOPIC
  error.partition = partition
  return { topicName: TOPIC, partition, errorCode: code, error, baseOffset: '-1' }
}

describe('Producer > the buffer > release at the terminal owner', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test.each([
    ['idempotent', { idempotent: true }],
    ['not idempotent', { idempotent: false }],
  ])('%s: a send is charged until its batch is answered, then released', async (_, options) => {
    const { producer, cluster, admission } = bufferedProducer(options)
    const calls = holdProduce(cluster)
    await producer.connect()
    const record = { value: 'v'.repeat(100) }

    const sent = watch(producer.send({ topic: TOPIC, messages: [record] }))
    expect(admission.reserved).toBe(held(record) + 1536)
    await flushAll()
    expect(calls).toHaveLength(1)
    expect(admission.debts).toBe(1024)
    calls[0].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('not idempotent, with acks 0: released when the request is written', async () => {
    const { producer, cluster, admission } = bufferedProducer({ idempotent: false })
    const calls = holdProduce(cluster)
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, acks: 0, messages: [{ value: 'v' }] }))
    await flushAll()
    expect(admission.reserved).toBeGreaterThan(0)
    calls[0].answer([])
    await flushAll()

    expect(sent.value).toEqual([])
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test.each([
    ['a retriable partition error', (call) => call.answer([failedResult(6)])],
    [
      'a connection error',
      (call) => call.fail(new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 })),
    ],
  ])(
    '%s keeps the reservation through the re-send until the retry is answered',
    async (_, firstAnswer) => {
      const { producer, cluster, admission } = bufferedProducer({
        deliveryTimeout: 60000,
        retry: { initialRetryTime: 10, maxRetryTime: 10 },
      })
      const calls = holdProduce(cluster)
      await producer.connect()
      const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
      await flushAll()
      const charged = countersOf(admission)

      firstAnswer(calls[0])
      await flushAll()
      expect(countersOf(admission)).toEqual(charged)
      await jest.advanceTimersByTimeAsync(20)
      await flushAll()
      expect(calls).toHaveLength(2)
      expect(countersOf(admission)).toEqual(charged)
      calls[1].answer()
      await flushAll()

      expect(sent.state).toBe('fulfilled')
      expect(countersOf(admission)).toEqual(quiescent(admission))
      await producer.disconnect()
    }
  )

  test('a non-retriable answer releases at the answer', async () => {
    const { producer, cluster, admission } = bufferedProducer()
    const calls = holdProduce(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()

    calls[0].answer([failedResult(87)])
    await flushAll()

    expect(sent.state).toBe('rejected')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test.each([
    ['idempotent', { idempotent: true }],
    ['not idempotent', { idempotent: false }],
  ])(
    '%s: a batch written and then expired keeps its reservation until its answer, then releases it',
    async (_, options) => {
      const { producer, cluster, admission } = bufferedProducer(options)
      const calls = holdProduce(cluster)
      await producer.connect()
      const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
      await flushAll()
      const charged = countersOf(admission)
      expect(calls[0].transmit()).toBeNull()

      await jest.advanceTimersByTimeAsync(1000)
      expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(sent.value.outcome).toBe('unknown')
      expect(countersOf(admission)).toEqual(charged)
      calls[0].answer()
      await flushAll()

      expect(countersOf(admission)).toEqual(quiescent(admission))
      await producer.disconnect()
    }
  )

  test('not idempotent: a batch written and then expired is released when its request fails, and one refused by the guard when its attempt ends', async () => {
    const { producer, cluster, admission } = bufferedProducer({ idempotent: false, partitions: 2 })
    const calls = holdProduce(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v', partition: 0 }] }))
    await flushAll()
    const charged = countersOf(admission)
    expect(calls[0].transmit()).toBeNull()

    await jest.advanceTimersByTimeAsync(1000)
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(sent.value.outcome).toBe('unknown')
    expect(countersOf(admission)).toEqual(charged)
    calls[0].fail(new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 }))
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))

    const refused = watch(producer.send({ topic: TOPIC, messages: [{ value: 'w', partition: 1 }] }))
    await flushAll()
    await jest.advanceTimersByTimeAsync(1000)
    expect(refused.value).toMatchObject({ outcome: 'unsent' })
    expect(calls[1].transmit()).toBeInstanceOf(Error)
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('an idempotent send path that refuses batches for their acks retires them', async () => {
    const createSendMessages = require('../../sendMessages')
    const sendMessages = createSendMessages({
      logger: { debug: () => {}, error: () => {} },
      cluster: {},
      partitioner: () => 0,
      eosManager: {},
      retrier: () => {},
      idempotence: { isEnabled: () => true },
    })
    const retire = jest.fn()

    await expect(
      sendMessages.sendPartitioned({ acks: 1, batches: [{ topic: TOPIC, partition: 0, retire }] })
    ).rejects.toThrow('An idempotent send must use acks -1')
    expect(retire).toHaveBeenCalledTimes(1)
  })

  describe('not idempotent, on the send path itself', () => {
    const createSendMessages = require('../../sendMessages')
    const createRetry = require('../../../retry')
    const { createFakeCluster } = require('../../__tests__/fixtures/fakeCluster')

    /** The plain send path over a fake cluster, with no retry. */
    const plainPath = (cluster) => {
      const retry = { retries: 0, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
      return createSendMessages({
        logger: { debug: () => {}, error: () => {}, warn: () => {} },
        cluster,
        partitioner: () => 0,
        eosManager: {
          isTransactional: () => false,
          getProducerId: () => '-1',
          getProducerEpoch: () => 0,
        },
        retrier: createRetry(retry),
        retry,
        deliveryTimeout: 1000,
        getConnectionStatus: () => 'connected',
      })
    }
    /** A batch whose settler rejects with `reject`, and whose detach and retire are counted. */
    const batchOf = (partition, reject = jest.fn(), createdAt) => ({
      topic: TOPIC,
      partition,
      messages: [{ value: 'v' }],
      createdAt,
      settlers: [{ resolve: jest.fn(), reject }],
      detach: jest.fn(),
      retire: jest.fn(),
    })

    test('batches handed over past their deadline reject unsent, detach and retire at once, with no request', async () => {
      const cluster = createFakeCluster({ partitions: 2 })
      const batches = [batchOf(0, jest.fn(), -2000), batchOf(1, jest.fn(), -1500)]

      const error = await plainPath(cluster)
        .sendPartitioned({ acks: 1, timeout: 1000, compression: 0, batches })
        .catch((e) => e)

      expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
      for (const batch of batches) {
        expect([batch.detach.mock.calls.length, batch.retire.mock.calls.length]).toEqual([1, 1])
      }
      expect(cluster.broker.produce).not.toHaveBeenCalled()
    })

    test('a call whose work fails on its own still settles every batch and retires each once', async () => {
      const cluster = createFakeCluster({
        partitions: 2,
        produce: async () => {
          throw new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 })
        },
      })
      const failing = jest.fn(() => {
        throw new Error('a settler that throws')
      })
      const batches = [batchOf(0, failing, 0), batchOf(1, jest.fn(), 0)]

      await plainPath(cluster)
        .sendPartitioned({ acks: 1, timeout: 1000, compression: 0, batches })
        .catch(() => {})
      await flushAll()

      expect(failing).toHaveBeenCalledTimes(1)
      expect(batches[1].settlers[0].reject).toHaveBeenCalledTimes(1)
      for (const batch of batches) expect(batch.retire).toHaveBeenCalledTimes(1)
    })
  })

  test('a batch whose attempt was never written keeps its reservation until the guard refuses that attempt', async () => {
    const { producer, cluster, admission } = bufferedProducer()
    const calls = holdProduce(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()
    const charged = countersOf(admission)

    await jest.advanceTimersByTimeAsync(1000)
    expect(sent.value).toMatchObject({ outcome: 'unsent' })
    expect(countersOf(admission)).toEqual(charged)
    expect(calls[0].transmit()).toBeInstanceOf(Error)
    await flushAll()

    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('a queued batch that expires before any request carries it is released at its expiry', async () => {
    const { producer, cluster, admission } = bufferedProducer()
    cluster.findLeaderForPartitions = () => ({})
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()
    expect(admission.reserved).toBeGreaterThan(0)
    await jest.advanceTimersByTimeAsync(1000)

    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test("not idempotent: a batch that expires while no request carries it gives back its records' share at its expiry, and its send's shell and its debit when the call's work ends", async () => {
    const { producer, cluster, admission } = bufferedProducer({ idempotent: false })
    const metadata = deferred()
    cluster.refreshMetadataIfNecessary
      .mockImplementationOnce(async () => {})
      .mockImplementation(() => metadata.promise)
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()
    expect(admission.reserved).toBeGreaterThan(0)
    await jest.advanceTimersByTimeAsync(1000)

    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(countersOf(admission)).toEqual({
      available: admission.capacity - 1536 - 1024,
      reserved: 1536,
      waitingBytes: 0,
      debts: 1024,
    })
    metadata.resolve()
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test.each([
    [
      'a held broker lookup',
      (cluster, gate) => {
        cluster.findBroker.mockImplementation(() => gate.promise)
      },
      () => {},
      (cluster) => cluster.broker,
    ],
    [
      'a held recovery after a retriable answer',
      (cluster, gate) => {
        // built here, not in the fake broker's call: an error keeps what its stack's frames hold
        const answer = [failedResult(6)]
        cluster.refreshMetadata.mockImplementation(() => gate.promise)
        cluster.broker.produce.mockImplementation(async () => answer)
      },
      () => {},
      () => undefined,
    ],
    [
      'the held side channels of a late connection failure',
      (cluster, gate) => {
        cluster.refreshMetadata.mockImplementation(() => gate.promise)
        return holdProduce(cluster)
      },
      (calls) => calls.forEach((call) => expect(call.transmit()).toBeNull()),
      () => undefined,
      (calls) => {
        const closed = new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 })
        calls.forEach((call) => call.fail(closed))
      },
    ],
  ])(
    'not idempotent: sends that expire behind %s drop their records at once and keep only their shells and batch debits charged until it returns',
    async (_, hold, beforeExpiry, opened, afterExpiry = () => {}) => {
      const { producer, cluster, admission } = bufferedProducer({ idempotent: false })
      await producer.connect()
      const gate = deferred()
      const calls = hold(cluster, gate)
      /** Starts the sends in a frame of their own, so this test keeps only weak references. */
      const sendAll = (count) => {
        const refs = []
        const sends = []
        for (let i = 0; i < count; i += 1) {
          const value = Buffer.alloc(16384, i)
          refs.push(new WeakRef(value))
          sends.push(watch(producer.send({ topic: TOPIC, messages: [{ value }] })))
        }
        return { refs, sends }
      }
      const { refs, sends } = sendAll(5)
      await flushAll()
      beforeExpiry(calls)

      await jest.advanceTimersByTimeAsync(1000)
      afterExpiry(calls)
      await flushAll()
      // the fake broker's own records of its calls are the test's, not the producer's
      calls?.splice(0)
      cluster.broker.produce.mockClear()
      await collect()

      expect(sends.every(({ value }) => value instanceof KafkaJSDeliveryTimeout)).toBe(true)
      expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
      // each shell is 1536 and the 16384 bytes of its Buffer, kept charged conservatively
      expect(countersOf(admission)).toEqual({
        available: admission.capacity - 5 * (1536 + 16384) - 5 * 1024,
        reserved: 5 * (1536 + 16384),
        waitingBytes: 0,
        debts: 5 * 1024,
      })
      gate.resolve(opened(cluster))
      await flushAll()
      expect(countersOf(admission)).toEqual(quiescent(admission))
      await producer.disconnect()
    }
  )

  test("the transaction's abort releases the batches it drops from the queue", async () => {
    const { producer, cluster, admission, transaction } = await transactionalProducer({
      deliveryTimeout: 60000,
    })
    cluster.findLeaderForPartitions = () => ({})
    const sent = watch(transaction.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()
    expect(admission.reserved).toBeGreaterThan(0)

    await transaction.abort()

    expect(sent.state).toBe('rejected')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('a fault in the send loop releases the queued batches at once and those on the wire at their answer', async () => {
    const { producer, cluster, admission } = bufferedProducer({ partitions: 2 })
    const calls = holdProduce(cluster)
    await producer.connect()
    const first = { value: 'on the wire', partition: 0 }
    const onWire = watch(producer.send({ topic: TOPIC, messages: [first] }))
    await flushAll()
    jest.spyOn(ProducerStateManager.prototype, 'drain').mockImplementationOnce(() => {
      throw new Error('a fault in the drain')
    })

    const queued = watch(producer.send({ topic: TOPIC, messages: [{ value: 'q', partition: 1 }] }))
    await flushAll()

    expect(queued.state).toBe('rejected')
    expect(onWire.state).toBe('rejected')
    expect(countersOf(admission)).toEqual({
      available: admission.capacity - (held(first) + 1536) - 1024,
      reserved: held(first) + 1536,
      waitingBytes: 0,
      debts: 1024,
    })
    calls[0].answer()
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test.each([
    [
      'a failed partition assignment',
      {
        createPartitioner: () => () => {
          throw new Error('the partitioner failed')
        },
      },
      () => {},
    ],
    [
      'a failed producer id request',
      {},
      (cluster) => cluster.broker.initProducerId.mockRejectedValue(createErrorFromCode(31)),
    ],
  ])('%s releases the send at once', async (_, options, fail) => {
    const { producer, cluster, admission } = bufferedProducer(options)
    fail(cluster)
    await producer.connect().catch(() => {})

    await expect(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] })).rejects.toThrow()

    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('sends that expire behind a held producer id request keep only their shells charged, and none of their records', async () => {
    // Each send's 1 MiB record needs a request above the default maxRequestSize.
    const { producer, cluster, admission } = bufferedProducer({
      capacity: 64 * 1048576,
      maxRequestSize: 2097152,
    })
    const initialized = deferred()
    cluster.broker.initProducerId.mockImplementation(() => initialized.promise)
    const connecting = producer.connect()
    await flushAll()
    /** Starts the sends in a frame of their own, so this test keeps only weak references. */
    const sendAll = (count) => {
      const refs = []
      const sends = []
      for (let i = 0; i < count; i += 1) {
        const value = Buffer.alloc(1048576, i)
        refs.push(new WeakRef(value))
        sends.push(watch(producer.send({ topic: TOPIC, messages: [{ value: `send-${i}` }] })))
        sends.push(watch(producer.send({ topic: TOPIC, messages: [{ value }] })))
      }
      return { refs, sends }
    }
    const { refs, sends } = sendAll(20)
    await flushAll()

    await jest.advanceTimersByTimeAsync(1000)
    await collect()

    expect(sends.every(({ value }) => value instanceof KafkaJSDeliveryTimeout)).toBe(true)
    expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
    expect(admission.reserved).toBe(20 * 1536 + 20 * (1536 + 1048576))
    initialized.resolve({ producerId: '1000', producerEpoch: 0 })
    await connecting
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('two disjoint 8 KiB views of one 16 KiB store in two partitions keep all 16 KiB charged until the last share retires', async () => {
    const { producer, cluster, admission } = bufferedProducer({
      partitions: 2,
      deliveryTimeout: 60000,
      retry: { initialRetryTime: 10, maxRetryTime: 10 },
    })
    const calls = holdProduce(cluster)
    await producer.connect()
    let store = Buffer.alloc(16384)
    const records = [
      { value: store.subarray(0, 8192), partition: 0 },
      { value: store.subarray(8192), partition: 1 },
    ]
    const shares = records.map((record) => held(record) - 8192)
    const sent = watch(producer.send({ topic: TOPIC, messages: records }))
    records.length = 0
    store = null
    await flushAll()

    const [{ request }] = calls
    calls[0].answer([
      ...succeeded(request).filter(({ partition }) => partition === 0),
      failedResult(6, 1),
    ])
    await flushAll()
    expect(admission.reserved).toBe(shares[1] + 1536 + 16384)
    await jest.advanceTimersByTimeAsync(20)
    calls[1].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test("a split send's chunks release their own shares at their answers, and its shell with the last one", async () => {
    const { producer, cluster, admission } = bufferedProducer({
      maxRequestSize: 2000,
      deliveryTimeout: 60000,
    })
    const calls = holdProduce(cluster)
    await producer.connect()
    // 2000 less the framing of the request (39), the topic (39), the partition (16) and the batch
    // header (61) leaves 1845 bytes of records: three records of 525 wire bytes a chunk
    const messages = Array.from({ length: 6 }, () => ({ value: 'x'.repeat(500) }))
    const share = 3 * (525 + 64)
    const sent = watch(producer.send({ topic: TOPIC, messages }))
    await flushAll()

    expect(calls).toHaveLength(1)
    expect(calls[0].request.topicData[0].partitions[0].messages).toHaveLength(3)
    expect([admission.reserved, admission.debts]).toEqual([1536 + 2 * share, 2 * 1024])
    calls[0].answer()
    await flushAll()
    expect(calls).toHaveLength(2)
    expect([admission.reserved, admission.debts]).toEqual([1536 + share, 1024])
    calls[1].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(sent.value).toEqual([expect.objectContaining({ partition: 0, baseOffset: '0' })])
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('Buffer-valued headers and copied views across partitions are released once each, and wholly', async () => {
    const { producer, cluster, admission } = bufferedProducer({
      partitions: 3,
      deliveryTimeout: 60000,
    })
    const calls = holdProduce(cluster)
    await producer.connect()
    const slab = Buffer.alloc(1048576)
    const shared = Buffer.alloc(4096)
    const records = [
      { value: slab.subarray(0, 10), headers: { a: shared.subarray(0, 100) }, partition: 0 },
      {
        value: 'v',
        headers: { b: [shared.subarray(100, 200), slab.subarray(10, 20)] },
        partition: 1,
      },
      { value: shared.subarray(200, 300), headers: { c: Buffer.alloc(5) }, partition: 2 },
    ]
    const expected = records.reduce((sum, record) => sum + held(record), 1536) + 4096 - 300
    const sent = watch(producer.send({ topic: TOPIC, messages: records }))
    await flushAll()

    // the 10-byte views of the 1 MiB slab are copied into stores of their own; the views of the
    // 4096-byte store keep its other 3796 bytes alive
    expect(admission.reserved).toBe(expected)
    calls[0].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('a graceful disconnect releases each batch as it settles', async () => {
    const { producer, cluster, admission } = bufferedProducer()
    const calls = holdProduce(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()

    const disconnected = watch(producer.disconnect())
    await flushAll()
    expect(disconnected.state).toBe('pending')
    expect(admission.reserved).toBeGreaterThan(0)
    calls[0].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(disconnected.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('once everything has settled, a send of the whole capacity is admitted in the same turn', async () => {
    const { producer, cluster, admission } = bufferedProducer({ capacity: 4096 })
    const calls = holdProduce(cluster)
    await producer.connect()
    const first = watch(producer.send({ topic: TOPIC, messages: [{ value: 'v' }] }))
    await flushAll()
    calls[0].answer()
    await flushAll()
    expect(first.state).toBe('fulfilled')

    const probe = { value: 'p'.repeat(2471) }
    expect(held(probe) + 1536).toBe(4096)
    const sent = watch(producer.send({ topic: TOPIC, messages: [probe] }))

    expect(admission.reserved).toBe(4096)
    await flushAll()
    calls[1].answer()
    await flushAll()
    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })
})

describe('Producer > the buffer > every order of answer, expiry, abort and disconnect', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  const STEPS = ['answer', 'expiry', 'abort', 'disconnect']

  test.each(interleavings(STEPS).map((order) => [order.map((index) => STEPS[index]).join(', ')]))(
    'a two-partition transactional send, then %s: the whole buffer comes back',
    async (order) => {
      const { producer, cluster, admission, transaction } = await transactionalProducer({
        partitions: 2,
        deliveryTimeout: 1000,
      })
      const calls = holdProduce(cluster)
      const sent = transaction
        .send({
          topic: TOPIC,
          messages: [
            { value: 'a', partition: 0 },
            { value: 'b', partition: 1 },
          ],
        })
        .catch(() => {})
      await flushAll()
      expect(admission.reserved).toBeGreaterThan(0)
      const ends = []
      const step = {
        answer: () => calls.splice(0).forEach((call) => call.answer()),
        expiry: () => jest.advanceTimersByTimeAsync(1000),
        abort: () => ends.push(transaction.abort().catch(() => {})),
        disconnect: () => ends.push(producer.disconnect().catch(() => {})),
      }

      for (const name of order.split(', ')) {
        await step[name]()
        await flushAll()
      }
      step.answer()
      await jest.advanceTimersByTimeAsync(1000)
      await Promise.all([sent, ...ends])

      expect(countersOf(admission)).toEqual(quiescent(admission))
      await producer.disconnect()
    }
  )
  test('post-guard children each clear their actual payload, debit a batch and keep the backing-store shell until the final owner retires', async () => {
    const Accumulator = require('../accumulator')
    const { Admission } = require('../admission')
    const admission = new Admission({ capacity: 1048576, maxBlockMs: 0 })
    const handed = []
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: 16384,
      admission,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async ({ batches }) => {
        handed.push(...batches)
      },
    })
    const sent = watch(
      accumulator.append({
        acks: -1,
        topicMessages: [
          {
            topic: TOPIC,
            messages: Array.from({ length: 4 }, () => ({ value: Buffer.alloc(8192) })),
          },
        ],
      })
    )
    await flushAll()
    const [parent] = handed
    const original = parent.messages
    const lateRetire = parent.retire
    const lateDetach = parent.detach
    const [head, tail] = Accumulator.cut(parent, 2)
    expect(original).toHaveLength(0)
    Object.assign(parent, tail)
    lateDetach()
    lateRetire()
    lateRetire()
    expect(tail.messages).toHaveLength(2)
    expect(admission.debts).toBe(2048)
    const [left, right] = Accumulator.cut(head, 1)
    expect(head.messages).toHaveLength(0)
    expect(admission.debts).toBe(3072)
    const before = admission.reserved
    left.detach()
    left.detach()
    expect(left.messages).toHaveLength(0)
    expect(admission.reserved).toBe(before - 90)
    left.settlers.forEach((settler) =>
      settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
    )
    left.retire()
    left.retire()
    head.retire()
    lateRetire()
    expect(admission.debts).toBe(2048)
    const failure = new Error('expired or aborted')
    for (const child of [right, tail]) {
      child.settlers.forEach((settler) => settler.reject(failure))
      child.settlers.forEach((settler) => settler.reject(failure))
      child.detach()
      child.detach()
    }
    await flushAll()
    expect(sent.value).toBe(failure)
    expect(right.messages).toHaveLength(0)
    expect(tail.messages).toHaveLength(0)
    expect(admission.reserved).toBe(1536 + 4 * 8192)
    right.retire()
    right.retire()
    expect(admission.reserved).toBe(1536 + 4 * 8192)
    expect(admission.debts).toBe(1024)
    tail.retire()
    tail.retire()
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test('plain post-guard child settlement behind held recovery clears every child array while shells and debits stay charged', async () => {
    const Accumulator = require('../accumulator')
    const cuts = []
    const cut = Accumulator.cut
    jest.spyOn(Accumulator, 'cut').mockImplementation((batch, at) => {
      const children = cut(batch, at)
      cuts.push(...children)
      return children
    })
    const { producer, cluster, admission } = bufferedProducer({
      idempotent: false,
      maxRequestSize: 4096,
    })
    const recovery = deferred()
    cluster.refreshMetadata.mockImplementation(() => recovery.promise)
    cluster.broker.produce.mockImplementation(async (request) => {
      const count = request.topicData
        .flatMap(({ partitions }) => partitions)
        .reduce((sum, { messages }) => sum + messages.length, 0)
      const refusal = request.guard(count > 1 ? 4097 : 64)
      if (refusal) throw refusal
      return [failedResult(6)]
    })
    await producer.connect()
    const sent = watch(
      producer.send({
        topic: TOPIC,
        messages: Array.from({ length: 4 }, () => ({ value: 'x'.repeat(500) })),
      })
    )
    await flushAll()
    expect(cuts).toHaveLength(4)
    expect(admission.debts).toBe(3072)
    await jest.advanceTimersByTimeAsync(1000)
    await flushAll()
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(cuts.every(({ messages }) => messages.length === 0)).toBe(true)
    expect(admission.reserved).toBe(1536)
    expect(admission.debts).toBe(3072)
    recovery.resolve()
    await flushAll()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })
})

describe('Producer > the buffer > a join that fails while planning', () => {
  const Accumulator = require('../accumulator')
  const { Admission } = require('../admission')
  const OTHER_TOPIC = 'topic-b'
  const LINGER = 5

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * An accumulator over a 64 KiB buffer whose request budget's `chunkCapacity` can be told to throw
   * on one call (`append` asks once per topic, the join once per partition). Each record is its own
   * entry, at the partition it names. The send path records what each call carried, then answers
   * and retires every batch. A `budget` given replaces that request budget.
   */
  const planningFailure = ({ batchSize, linger = 0, budget: given }) => {
    const admission = new Admission({ capacity: 65536, maxBlockMs: 0 })
    const failure = new RangeError('chunk capacity failed')
    const capacity = { calls: 0, failAt: 0 }
    const budget = given ?? {
      maxRequestSize: Infinity,
      chunkCapacity: () => {
        capacity.calls += 1
        if (capacity.calls === capacity.failAt) throw failure
        return Infinity
      },
    }
    const pins = new Map()
    const requests = []
    const sendPartitioned = jest.fn(async ({ acks, batches }) => {
      requests.push({
        acks,
        batches: batches.map(({ key, messages }) => ({
          key,
          values: messages.map(({ value }) => value),
        })),
      })
      for (const batch of batches) {
        const metadata = {
          topicName: batch.topic,
          partition: batch.partition,
          errorCode: 0,
          baseOffset: '0',
        }
        batch.settlers.forEach((settler) => settler.resolve(metadata))
        batch.retire()
      }
    })
    const accumulator = new Accumulator({
      linger,
      batchSize,
      admission,
      budget,
      partition: async ({ records }) =>
        records.flatMap(({ topic, messages }) =>
          messages.map((message) => ({ topic, partition: message.partition, messages: [message] }))
        ),
      sendPartitioned,
      pinner: {
        holdTopic: (topic) => pins.set(topic, (pins.get(topic) ?? 0) + 1),
        releaseTopic: (topic) =>
          pins.get(topic) === 1 ? pins.delete(topic) : pins.set(topic, pins.get(topic) - 1),
      },
    })
    /** Makes the `n`th `chunkCapacity` call from now throw; 0 for none. */
    const failCall = (n) => {
      capacity.calls = 0
      capacity.failAt = n
    }
    /** A watched send of `topicMessages`. */
    const send = (topicMessages, acks = -1) => watch(accumulator.append({ acks, topicMessages }))
    return { accumulator, admission, failure, failCall, pins, requests, send, sendPartitioned }
  }

  const twoPartitions = () => [
    {
      topic: TOPIC,
      messages: [
        { value: 'a', partition: 0 },
        { value: 'b', partition: 1 },
      ],
    },
  ]

  const twoTopics = () => [
    { topic: TOPIC, messages: [{ value: 'a', partition: 0 }] },
    { topic: OTHER_TOPIC, messages: [{ value: 'b', partition: 0 }] },
  ]

  test.each([
    ['one topic with two partitions, at the third capacity call', twoPartitions, 3],
    ['two topics with one partition each, at the fourth capacity call', twoTopics, 4],
  ])(
    '%s: a failure planning the second partition leaves no debt, reservation, waiting bytes or topic pin after the rejection and close',
    async (_, topicMessages, failAt) => {
      const { accumulator, admission, failure, failCall, pins, send, sendPartitioned } =
        planningFailure({ batchSize: 1 })

      failCall(failAt)
      const sent = send(topicMessages())
      await flushAll()
      expect(sent.state).toBe('rejected')
      expect(sent.value).toBe(failure)

      const closed = watch(accumulator.close())
      await flushAll()
      expect(closed.state).toBe('fulfilled')
      expect(sendPartitioned).not.toHaveBeenCalled()
      expect(countersOf(admission)).toEqual(quiescent(admission))
      expect([...pins]).toEqual([])
    }
  )

  test('the same failure repeated 100 times still leaves room for a good send', async () => {
    const { accumulator, admission, failure, failCall, pins, send } = planningFailure({
      batchSize: 1,
    })

    for (let i = 0; i < 100; i += 1) {
      failCall(3)
      const sent = send(twoPartitions())
      await flushAll()
      expect(sent.value).toBe(failure)
    }
    failCall(0)
    const good = send(twoPartitions())
    await flushAll()

    expect(good.state).toBe('fulfilled')
    expect(good.value).toEqual([
      { topicName: TOPIC, partition: 0, errorCode: 0, baseOffset: '0' },
      { topicName: TOPIC, partition: 1, errorCode: 0, baseOffset: '0' },
    ])
    const closed = watch(accumulator.close())
    await flushAll()
    expect(closed.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
  })

  test('a failed join leaves no pending batch, so a later send to that partition carries only its own records', async () => {
    const { accumulator, admission, failure, failCall, requests, send } = planningFailure({
      batchSize: 16384,
    })

    failCall(3)
    const failed = send(twoPartitions())
    await flushAll()
    expect(failed.value).toBe(failure)
    failCall(0)
    const good = send([{ topic: TOPIC, messages: [{ value: 'good', partition: 0 }] }])
    await flushAll()
    const closed = watch(accumulator.close())
    await flushAll()

    expect(requests).toEqual([{ acks: -1, batches: [{ key: `${TOPIC}/0`, values: ['good'] }] }])
    expect(good.value).toEqual([{ topicName: TOPIC, partition: 0, errorCode: 0, baseOffset: '0' }])
    expect(closed.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test("a failed join after an earlier send began the partition's batch leaves that earlier send's records and settlement unchanged", async () => {
    const { accumulator, admission, failure, failCall, requests, send } = planningFailure({
      batchSize: 16384,
      linger: LINGER,
    })

    failCall(0)
    const earlier = send([{ topic: TOPIC, messages: [{ value: 'earlier', partition: 0 }] }])
    await flushAll()
    expect(requests).toEqual([])
    failCall(3)
    const failed = send(twoPartitions())
    await flushAll()
    expect(failed.value).toBe(failure)
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()

    expect(requests).toEqual([{ acks: -1, batches: [{ key: `${TOPIC}/0`, values: ['earlier'] }] }])
    expect(earlier.state).toBe('fulfilled')
    expect(earlier.value).toEqual([
      { topicName: TOPIC, partition: 0, errorCode: 0, baseOffset: '0' },
    ])
    const closed = watch(accumulator.close())
    await flushAll()
    expect(closed.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
  })

  test("a synchronous throw from the send path while handing on an earlier batch rejects that batch's sends and leaves the joining send intact", async () => {
    const { accumulator, requests, send, sendPartitioned } = planningFailure({
      batchSize: 16384,
      linger: LINGER,
    })
    const handOff = new Error('send path failure')
    sendPartitioned.mockImplementationOnce(() => {
      throw handOff
    })

    const earlier = send([{ topic: TOPIC, messages: [{ value: 'earlier', partition: 0 }] }])
    await flushAll()
    const joining = send([{ topic: TOPIC, messages: [{ value: 'joining', partition: 0 }] }], 1)
    await flushAll()

    expect(sendPartitioned).toHaveBeenCalledTimes(1)
    expect(sendPartitioned.mock.calls[0][0].acks).toBe(-1)
    expect(earlier.state).toBe('rejected')
    expect(earlier.value).toBe(handOff)
    expect(joining.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()

    expect(requests).toEqual([{ acks: 1, batches: [{ key: `${TOPIC}/0`, values: ['joining'] }] }])
    expect(joining.value).toEqual([
      { topicName: TOPIC, partition: 0, errorCode: 0, baseOffset: '0' },
    ])
    const closed = watch(accumulator.close())
    await flushAll()
    expect(closed.state).toBe('fulfilled')
  })

  test('a synchronous throw from the send path while handing on an earlier batch retires that batch, leaving no debt, reservation or topic pin of it', async () => {
    const { accumulator, admission, pins, requests, send, sendPartitioned } = planningFailure({
      batchSize: 16384,
      linger: LINGER,
    })
    const handOff = new Error('send path failure')
    sendPartitioned.mockImplementationOnce(() => {
      throw handOff
    })

    const earlier = send([{ topic: TOPIC, messages: [{ value: 'earlier', partition: 0 }] }])
    await flushAll()
    expect(admission.debts).toBe(1024)
    expect(pins.get(TOPIC)).toBe(1)
    const joining = send([{ topic: TOPIC, messages: [{ value: 'joining', partition: 0 }] }], 1)
    await flushAll()

    expect(sendPartitioned).toHaveBeenCalledTimes(1)
    expect(earlier.state).toBe('rejected')
    expect(earlier.value).toBe(handOff)
    expect(joining.state).toBe('pending')
    expect(admission.debts).toBe(1024)
    expect(pins.get(TOPIC)).toBe(1)

    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(requests).toEqual([{ acks: 1, batches: [{ key: `${TOPIC}/0`, values: ['joining'] }] }])
    expect(joining.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    const closed = watch(accumulator.close())
    await flushAll()
    expect(closed.state).toBe('fulfilled')
  })

  test('a send path that takes an earlier batch and then rejects keeps its debt and topic pin until the send path retires it', async () => {
    const { admission, pins, send, sendPartitioned } = planningFailure({
      batchSize: 16384,
      linger: LINGER,
    })
    const handOff = new Error('send path failure')
    const taken = []
    sendPartitioned.mockImplementationOnce(async ({ batches }) => {
      taken.push(...batches)
      throw handOff
    })

    const earlier = send([{ topic: TOPIC, messages: [{ value: 'earlier', partition: 0 }] }])
    await flushAll()
    const joining = send([{ topic: TOPIC, messages: [{ value: 'joining', partition: 0 }] }], 1)
    await flushAll()

    expect(earlier.value).toBe(handOff)
    expect(taken).toHaveLength(1)
    expect(admission.debts).toBe(2048)
    expect(pins.get(TOPIC)).toBe(2)
    taken[0].retire()
    expect(admission.debts).toBe(1024)
    expect(pins.get(TOPIC)).toBe(1)

    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()
    expect(joining.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
  })

  test('a record above the chunk capacity at planning, though a larger capacity admitted it, is refused as too large with nothing handed on, charged or pinned', async () => {
    const budget = {
      maxRequestSize: 1000,
      chunkCapacity: jest.fn().mockReturnValueOnce(1000).mockReturnValue(24),
    }
    const { accumulator, admission, pins, send, sendPartitioned } = planningFailure({
      batchSize: 16384,
      budget,
    })

    const sent = send([
      {
        topic: TOPIC,
        messages: [
          { value: 'a', partition: 0 },
          { value: 'b', partition: 0 },
        ],
      },
    ])
    await flushAll()

    expect(budget.chunkCapacity).toHaveBeenCalledTimes(2)
    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(sent.value).toMatchObject({
      reason: 'maxRequestSize',
      scope: 'record',
      size: 25,
      limit: 24,
      topic: TOPIC,
    })
    expect(sendPartitioned).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    const closed = watch(accumulator.close())
    await flushAll()
    expect(closed.state).toBe('fulfilled')
    expect(sendPartitioned).not.toHaveBeenCalled()
  })
})

describe('Producer > the buffer > a custom partitioner that changes a record', () => {
  const { createFakeCluster } = require('../../__tests__/fixtures/fakeCluster')

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  /** How long a batch waits for more records in the tests that share one. */
  const LINGER = 5

  /**
   * A producer without idempotence over two partitions, with `batchSize: 1` and `partitioner` as
   * its custom partitioner, whose cluster counts the topic pins it holds. `options` override the
   * producer's.
   */
  const pinnedProducer = (partitioner, options = {}) => {
    const cluster = createFakeCluster({ partitions: 2 })
    const pins = new Map()
    cluster.holdTopic = (topic) => pins.set(topic, (pins.get(topic) ?? 0) + 1)
    cluster.releaseTopic = (topic) =>
      pins.get(topic) === 1 ? pins.delete(topic) : pins.set(topic, pins.get(topic) - 1)
    const fixture = bufferedProducer({
      cluster,
      idempotent: false,
      batchSize: 1,
      createPartitioner: () => partitioner,
      ...options,
    })
    return { ...fixture, pins }
  }

  /** Sends every record to partition 0, after `change(message)` ran on each but `a`. */
  const sameBatch =
    (change) =>
    ({ message }) => {
      if (message.value !== 'a') change(message)
      return 0
    }

  /** The values of the records a Produce request carries, in order. */
  const valuesOf = ({ topicData }) =>
    topicData.flatMap(({ partitions }) =>
      partitions.flatMap(({ messages }) => messages.map(({ value }) => value))
    )

  /**
   * Sends `a`, which waits `LINGER` ms in its batch, then `b` with a header, whose record the
   * partitioner changes with `change` and puts in the same batch.
   */
  const shareBatch = async (change) => {
    const fixture = pinnedProducer(sameBatch(change), { linger: LINGER, batchSize: 16384 })
    await fixture.producer.connect()
    const earlier = watch(fixture.producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }))
    await flushAll()
    const changed = watch(
      fixture.producer.send({ topic: TOPIC, messages: [{ value: 'b', headers: { h: 'ok' } }] })
    )
    await flushAll()
    return { ...fixture, earlier, changed }
  }

  /**
   * Lets the earlier send's batch go out, and checks that it carried that send alone and that
   * nothing stays charged or pinned.
   */
  const expectEarlierWrittenAlone = async ({ producer, cluster, admission, pins, earlier }) => {
    expect(earlier.state).toBe('pending')
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(LINGER)
    await flushAll()

    expect(earlier.state).toBe('fulfilled')
    expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
    expect(valuesOf(cluster.broker.produce.mock.calls[0][0])).toEqual(['a'])
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    await producer.disconnect()
  }

  /** Sends `a` to partition 0 and `b` to partition 1, after `change(message)` ran on `b`. */
  const byValue =
    (change) =>
    ({ message }) => {
      if (message.value !== 'b') return 0
      change(message)
      return 1
    }

  test('a timestamp getter that throws on the second of two partitions with batchSize 1 rejects the send with its error, writes nothing and leaves nothing charged or pinned', async () => {
    const failure = new RangeError('timestamp read failed')
    const { producer, cluster, admission, pins } = pinnedProducer(
      byValue((message) =>
        Object.defineProperty(message, 'timestamp', {
          get() {
            throw failure
          },
        })
      )
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
    expect([...pins]).toEqual([])
  })

  test('a timestamp getter that disconnects the producer at once while the send joins refuses the send unsent, writes nothing and leaves nothing charged or pinned', async () => {
    let disconnected = null
    const { producer, cluster, admission, pins } = pinnedProducer(
      byValue((message) =>
        Object.defineProperty(message, 'timestamp', {
          get() {
            disconnected ??= producer.disconnect({ timeout: 0 })
            return 1
          },
        })
      )
    )
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'a' }, { value: 'b' }] }))
    await flushAll()

    expect(disconnected).toBeInstanceOf(Promise)
    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSProducerClosedError)
    expect(sent.value.outcome).toBe('unsent')
    await disconnected
    await flushAll()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
  })

  test.each([
    [
      'a header value replaced by a number',
      'headers',
      (message) => {
        message.headers.h = 42
      },
    ],
    [
      'headers replaced by null',
      'headers',
      (message) => {
        message.headers = null
      },
    ],
    [
      'a header value set to null',
      'headers',
      (message) => {
        message.headers.h = null
      },
    ],
    [
      'a header value set to undefined',
      'headers',
      (message) => {
        message.headers.h = undefined
      },
    ],
    [
      'a header value nested in two arrays',
      'headers',
      (message) => {
        message.headers.h = [['x']]
      },
    ],
    [
      'a key replaced by a number',
      'key',
      (message) => {
        message.key = 42
      },
    ],
    [
      'a value replaced by an object',
      'value',
      (message) => {
        message.value = {}
      },
    ],
    [
      'a timestamp whose conversion throws',
      'timestamp',
      (message) => {
        message.timestamp = {
          [Symbol.toPrimitive]() {
            throw new RangeError('timestamp conversion failed')
          },
        }
      },
    ],
    [
      'a negative timestamp',
      'timestamp',
      (message) => {
        message.timestamp = -1
      },
    ],
  ])(
    '%s by the partitioner refuses that send alone, and the earlier send sharing its batch is written without it',
    async (_, field, change) => {
      const fixture = await shareBatch(change)

      expect(fixture.changed.state).toBe('rejected')
      expect(fixture.changed.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(fixture.changed.value.message).toContain(
        `Invalid message for topic "${TOPIC}": its ${field} must be`
      )
      await expectEarlierWrittenAlone(fixture)
    }
  )

  test('a value the partitioner makes larger than its send was charged refuses that send alone, and the earlier send sharing its batch is written without it', async () => {
    const larger = 'x'.repeat(4096)
    const fixture = await shareBatch((message) => {
      message.value = larger
    })

    expect(fixture.changed.state).toBe('rejected')
    expect(fixture.changed.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(fixture.changed.value.message).toBe(
      `The records of a send changed after it was charged: they need ${
        1536 + held({ value: larger, headers: { h: 'ok' } })
      } bytes of bufferMemory, and the send was charged ${
        1536 + held({ value: 'b', headers: { h: 'ok' } })
      }`
    )
    await expectEarlierWrittenAlone(fixture)
  })

  test('a Buffer the partitioner puts in place of a longer string is charged while its request is in flight as its batch carries it, and released at the answer', async () => {
    const replacement = Buffer.alloc(4096, 'r')
    const { producer, cluster, admission, pins } = pinnedProducer(
      sameBatch((message) => {
        message.value = replacement
      }),
      { batchSize: 16384 }
    )
    const calls = holdProduce(cluster)
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'b'.repeat(8192) }] }))
    await flushAll()

    expect(calls).toHaveLength(1)
    expect(calls[0].request.topicData[0].partitions[0].messages[0].value).toBe(replacement)
    expect(admission.reserved).toBe(1536 + held({ value: replacement }))
    calls[0].answer()
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    await producer.disconnect()
  })

  test('a record whose value the partitioner removes is still sent, as a tombstone', async () => {
    const { producer, cluster, admission, pins } = pinnedProducer(
      sameBatch((message) => {
        delete message.value
      })
    )
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ key: 'k', value: 'b' }] }))
    await flushAll()

    expect(sent.state).toBe('fulfilled')
    const [record] = cluster.broker.produce.mock.calls[0][0].topicData[0].partitions[0].messages
    expect(record.key).toBe('k')
    expect('value' in record).toBe(false)
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    await producer.disconnect()
  })

  test('a send option whose conversion disconnects the producer refuses the send unsent before any batch forms, and writes or charges nothing', async () => {
    let disconnected = null
    const { producer, cluster, admission, pins } = pinnedProducer(() => 0)
    const debit = jest.spyOn(admission, 'debit')
    const timeout = {
      [Symbol.toPrimitive]() {
        disconnected ??= producer.disconnect({ timeout: 0 })
        return 1000
      },
    }
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, timeout, messages: [{ value: 'a' }] }))
    await flushAll()

    expect(disconnected).toBeInstanceOf(Promise)
    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSProducerClosedError)
    expect(sent.value.outcome).toBe('unsent')
    expect(debit).not.toHaveBeenCalled()
    await disconnected
    await flushAll()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
  })

  test('a send option whose conversion throws rejects the send with that error, and writes or charges nothing', async () => {
    const failure = new RangeError('timeout conversion failed')
    const { producer, cluster, admission, pins } = pinnedProducer(() => 0)
    const debit = jest.spyOn(admission, 'debit')
    const timeout = {
      [Symbol.toPrimitive]() {
        throw failure
      },
    }
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, timeout, messages: [{ value: 'a' }] }))
    await flushAll()

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBe(failure)
    expect(debit).not.toHaveBeenCalled()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    expect([...pins]).toEqual([])
    await producer.disconnect()
  })
})
