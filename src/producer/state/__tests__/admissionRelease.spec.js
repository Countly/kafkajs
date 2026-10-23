const ProducerStateManager = require('..')
const { held } = require('../recordSize')
const { interleavings } = require('../../../../testHelpers/schedule')
const { createErrorFromCode } = require('../../../protocol/error')
const { KafkaJSConnectionClosedError, KafkaJSDeliveryTimeout } = require('../../../errors')
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
