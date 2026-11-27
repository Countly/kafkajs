const setupPool = require('../../../testHelpers/setupPool')
const Broker = require('../../broker')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../protocol/headers')
const retry = require('../../retry')
const createSendMessages = require('../sendMessages')
const createProducer = require('../index')
const createEosManager = require('../eosManager')
const ProducerStateManager = require('../state')
const Accumulator = require('../state/accumulator')
const { partitionResults } = require('../../protocol/requests/produce/partitionResults')
const { protocol: produceProtocol } = require('../../protocol/requests/produce')
const { createErrorFromCode } = require('../../protocol/error')
const { Codecs, Types } = require('../../protocol/message/compression')
const { createLogger, LEVELS } = require('../../loggers')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { deferred, flushAll, transactionalProducer, watch } = require('./fixtures/bufferHarness')
const {
  KafkaJSBrokerNotFound,
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
} = require('../../errors')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const TOPIC = 'test-topic'
const NODE_ID = 0
const NOT_ENOUGH_REPLICAS = 19
const INVALID_RECORD = 87

/** A v3/v4-shaped ApiVersions body advertising the given Produce maxVersion. */
const apiVersionsBody = (produceMaxVersion) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder()
        .writeInt16(apiKeys.Produce)
        .writeInt16(0)
        .writeInt16(produceMaxVersion)
        .writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * Builds a real `Broker` wired to a real `Connection` over the fake socket from `testHelpers`.
 * `answer` receives each outgoing request's header and returns the response body to play back;
 * the response header version is derived per-request from `responseHeaderVersion`, so a Produce
 * request negotiated at v9 gets a flexible (v1) response header while v8 gets the classic one.
 */
const createFakeBroker = (answer) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })

  let answered = 0
  const pump = async () => {
    while (answered < state.writes.length) {
      const header = readRequestHeader(state.writes[answered])
      answered += 1
      const headerVersion = responseHeaderVersion(header.apiKey, header.apiVersion)
      respondWithFrame(state, header.correlationId, headerVersion, answer(header))
    }
  }

  const connectionPool = setupPool({
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    generation: 0,
    throwIfRetired: () => {},
    connectionTimeout: 1000,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      await connection.connect()
      return connection
    },
    getConnectionByRequest: async () => {
      await connection.connect()
      return connection
    },
    send: async (protocolRequest) => {
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  })

  return { broker: new Broker({ connectionPool, logger: newLogger(), nodeId: NODE_ID }), state }
}

/**
 * Minimal `Cluster` stub: every partition of `TOPIC` is led by the one fake broker. `topicId`
 * always answers `undefined` - these fixtures exercise the pre-KIP-516 (name-keyed) versions, so
 * every case stays on that path regardless of what the broker would otherwise negotiate.
 */
const createCluster = (broker, partitionMetadata) => ({
  addMultipleTargetTopics: async () => {},
  refreshMetadataIfNecessary: async () => {},
  refreshMetadata: async () => {},
  findTopicPartitionMetadata: () => partitionMetadata,
  findLeaderForPartitions: (topic, partitions) => ({ [NODE_ID]: partitions.map(Number) }),
  findBroker: async () => broker,
  topicId: () => undefined,
  targetTopics: new Set(),
  isConnected: () => true,
})

const createEosManagerStub = () => ({
  getProducerId: () => -1,
  getProducerEpoch: () => -1,
  getSequence: () => 0,
  getTransactionalId: () => undefined,
  updateSequence: () => {},
  isTransactional: () => false,
  addPartitionsToTransaction: async () => {},
})

/** Routes each message to the partition explicitly set on it, bypassing any real partitioner. */
const partitionerByMessage = ({ message }) => message.partition

/**
 * One send as the producer makes it: its records copied at the call, joined to a batch by the
 * accumulator, assigned to partitions once, then sent.
 * @param {ReturnType<typeof createSendMessages>} sender
 * @param {{ topic: string, messages: object[] }[]} topicMessages
 */
const send = (sender, topicMessages) =>
  new Accumulator({
    linger: 0,
    batchSize: 16384,
    partition: (pending) => sender.partition(pending),
    sendPartitioned: (request) => sender.sendPartitioned(request),
  }).append({ acks: -1, timeout: 30000, topicMessages: Accumulator.snapshot(topicMessages) })

describe('Producer > sendMessages > recordErrors (KIP-467, real codec over a fake broker)', () => {
  test('a v9 (flexible) failure resolves recordErrors[].record through the per-partition grouping', async () => {
    const messages = [
      { key: 'a', value: '1', partition: 0 },
      { key: 'b', value: '2', partition: 0 },
      { key: 'c', value: '3', partition: 0 },
    ]

    const answer = (header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(9)
        : Buffer.from(
            require('../../protocol/requests/produce/fixtures/v9_response_record_errors.json')
          )

    const { broker, state } = createFakeBroker(answer)
    await broker.connect()

    const cluster = createCluster(broker, [{ partitionId: 0, leader: NODE_ID }])
    const sendMessages = createSendMessages({
      logger: newLogger(),
      cluster,
      partitioner: partitionerByMessage,
      eosManager: createEosManagerStub(),
      retrier: retry({ retries: 0 }),
    })

    const error = await send(sendMessages, [{ topic: TOPIC, messages }]).catch((e) => e)

    // Pins the version this case is actually exercising, rather than leaving it implicit in the
    // fixture choice: this is the flexible (v9) request/response path, not just "some" version.
    expect(readRequestHeader(state.writes[1]).apiVersion).toEqual(9)

    expect(error).toBeInstanceOf(Error)
    expect(error.recordErrors).toEqual([
      {
        batchIndex: 1,
        record: { key: Buffer.from('b'), value: Buffer.from('2'), headers: {} },
        errorMessage: 'record too large',
      },
    ])
    expect(error.recordErrors[0].record).not.toBe(messages[1])
  })

  test("a v8 (classic) failure on a non-first partition resolves through that partition's own grouping", async () => {
    // Six messages split three-to-a-partition; the fixture's failing partition is 1 with
    // batchIndex 0, so the correct record is the *fourth* message overall. A mapping that indexed
    // the caller's flat message array (instead of the per-partition grouping) would return the
    // first message here instead, which is the only reason this case exists.
    const messages = Array.from({ length: 6 }, (_, i) => ({
      key: `k${i}`,
      value: `v${i}`,
      partition: i < 3 ? 0 : 1,
    }))

    const answer = (header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(8)
        : Buffer.from(
            require('../../protocol/requests/produce/fixtures/v8_response_record_errors_partition1.json')
          )

    const { broker, state } = createFakeBroker(answer)
    await broker.connect()

    const cluster = createCluster(broker, [
      { partitionId: 0, leader: NODE_ID },
      { partitionId: 1, leader: NODE_ID },
    ])
    const sendMessages = createSendMessages({
      logger: newLogger(),
      cluster,
      partitioner: partitionerByMessage,
      eosManager: createEosManagerStub(),
      retrier: retry({ retries: 0 }),
    })

    const error = await send(sendMessages, [{ topic: TOPIC, messages }]).catch((e) => e)

    // Pins the version this case is actually exercising: the classic (v8) request/response path.
    expect(readRequestHeader(state.writes[1]).apiVersion).toEqual(8)

    expect(error.partition).toEqual(1)
    expect(error.recordErrors).toEqual([
      {
        batchIndex: 0,
        record: { key: Buffer.from('k3'), value: Buffer.from('v3'), headers: {} },
        errorMessage: 'record too large',
      },
    ])
    expect(error.recordErrors[0].record).not.toBe(messages[3])
  })
})

/** @returns {{ key: string, value: string, partition: number }} the record named `name` */
const message = (name, partition = 0) => ({ key: `key-${name}`, value: `value-${name}`, partition })

/** @returns {object} the `record` a projected entry carries for `message(name)` */
const recordOf = (name) => ({
  key: Buffer.from(`key-${name}`),
  value: Buffer.from(`value-${name}`),
  headers: {},
})

/**
 * A partition error as a Produce answer decodes it, with one rejected record per index.
 * @param {number} code
 * @param {number[]|undefined} indices undefined for an answer without record errors
 * @param {number} [partition=0]
 * @returns {KafkaJSProtocolError}
 */
const answerError = (code, indices, partition = 0) =>
  partitionResults({
    topics: [
      {
        topicName: TOPIC,
        partitions: [
          {
            partition,
            errorCode: code,
            errorMessage: 'refused',
            baseOffset: '-1',
            logAppendTime: '-1',
            logStartOffset: '0',
            ...(indices === undefined
              ? {}
              : {
                  recordErrors: indices.map((batchIndex) => ({
                    batchIndex,
                    batchIndexErrorMessage: `bad ${batchIndex}`,
                  })),
                }),
          },
        ],
      },
    ],
  })[0].error

/**
 * The per-partition answer to a Produce request: `errors[partition]` for a partition it names,
 * success for every other.
 * @param {{ topicData: { topic: string, partitions: { partition: number }[] }[] }} request
 * @param {{ [partition: number]: Error }} [errors]
 * @returns {object[]}
 */
const resultsOf = ({ topicData }, errors = {}) =>
  topicData.flatMap(({ topic, partitions }) =>
    partitions.map(({ partition }) => {
      const error = errors[partition] ?? null
      return {
        topicName: topic,
        partition,
        errorCode: error === null ? 0 : error.code,
        error,
        baseOffset: error === null ? '0' : '-1',
        logAppendTime: '-1',
        logStartOffset: '0',
      }
    })
  )

/** Asserts that no link of each error's `cause` chain carries a `record`. */
const expectRaw = (...errors) => {
  for (const error of errors) {
    const seen = new Set()
    for (let link = error; link != null && !seen.has(link); link = link.cause) {
      seen.add(link)
      for (const entry of link.recordErrors ?? []) {
        expect(Object.hasOwn(entry, 'record')).toBe(false)
      }
    }
  }
}

/** @returns {Error[]} the error and its `cause` links, outermost first */
const chainOf = (error) => {
  const chain = []
  for (let link = error; link != null && !chain.includes(link); link = link.cause) chain.push(link)
  return chain
}

/** @type {WeakMap<object, number[][]>} per fake cluster, what `countRecords` noted */
const noted = new WeakMap()

/**
 * Makes the cluster hand the producer its broker with a `produce` that notes the record count of
 * each batch of a request when it is called, then answers with `cluster.broker.produce`: a batch's
 * record list is emptied once it settles.
 * @param {object} cluster a fake cluster
 * @returns {void}
 */
const countRecords = (cluster) => {
  const counts = []
  noted.set(cluster, counts)
  const { broker } = cluster
  const counting = {
    ...broker,
    produce: (request) => {
      counts.push(
        request.topicData
          .flatMap(({ partitions }) => partitions.map(({ messages }) => messages.length))
          .sort((x, y) => x - y)
      )
      return broker.produce(request)
    },
  }
  cluster.findBroker = jest.fn(async () => counting)
}

/**
 * @returns {number[][]} the record count of each batch of each Produce request the fake broker
 *   received, ascending within a request
 */
const recordCounts = (cluster) => noted.get(cluster)

/** An explicitly idempotent producer refuses `retries: 0`; a terminal answer needs no retry. */
const ONE_RETRY = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

const PATHS = [
  ['idempotent', true, ONE_RETRY],
  ['plain', false, { retries: 0 }],
]

describe('Producer > the record errors of a shared batch', () => {
  const producers = []

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['queueMicrotask', 'nextTick', 'setImmediate'] })
  })

  afterEach(async () => {
    try {
      await jest.runAllTimersAsync()
      await Promise.all(producers.splice(0).map((producer) => producer.disconnect()))
    } finally {
      jest.useRealTimers()
      jest.restoreAllMocks()
    }
  })

  /**
   * A connected producer over the fake cluster, `linger: 20`, logging WARN and above to `logs`.
   * @param {object} options `createProducer` options, plus `partitions`
   */
  const start = async ({ partitions = 1, ...options }) => {
    const cluster = createFakeCluster({ partitions })
    countRecords(cluster)
    const logs = []
    const producer = createProducer({
      cluster,
      logger: createLogger({ level: LEVELS.WARN, logCreator: () => (entry) => logs.push(entry) }),
      linger: 20,
      deliveryTimeout: 60000,
      retry: { retries: 0 },
      ...options,
    })
    producers.push(producer)
    await producer.connect()
    return { producer, cluster, logs }
  }

  /** Sends each record on its own and lets `linger` join them into one batch per partition. */
  const sendAll = async (producer, records, options = {}) => {
    const sends = records.map((record) =>
      watch(producer.send({ topic: TOPIC, messages: [record], ...options }))
    )
    await jest.advanceTimersByTimeAsync(20)
    await flushAll()
    return sends
  }

  describe.each(PATHS)('%s', (_, idempotent, retry) => {
    test('two coalesced sends, the broker rejects one record of the first: only the first gets it, each its own error', async () => {
      const { producer, cluster } = await start({ idempotent, retry })
      const answer = answerError(INVALID_RECORD, [0])
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer })
      )

      const [a, b] = await sendAll(producer, [message('a'), message('b')])

      expect(recordCounts(cluster)).toEqual([[2]])
      expect(a.state).toBe('rejected')
      expect(b.state).toBe('rejected')
      expect(a.value).not.toBe(b.value)
      expect(a.value).not.toBe(answer)
      expect(b.value).not.toBe(answer)
      expect(a.value).toBeInstanceOf(KafkaJSProtocolError)
      expect(a.value).toMatchObject({ code: INVALID_RECORD, topic: TOPIC, partition: 0 })
      expect(a.value.recordErrors).toEqual([
        { batchIndex: 0, errorMessage: 'bad 0', record: recordOf('a') },
      ])
      expect(a.value.recordErrors[0]).not.toHaveProperty('messageIndex')
      expect(b.value.recordErrors).toEqual([{ batchIndex: 0, errorMessage: 'bad 0' }])
      expect(b.value.recordErrors[0]).toBe(answer.recordErrors[0])
      expect(b.value.recordErrors).not.toBe(answer.recordErrors)
      expectRaw(answer)
    })

    test('one error object answering two partitions of a response is ambiguous: both sends get it raw', async () => {
      const { producer, cluster } = await start({ idempotent, retry, partitions: 2 })
      const answer = answerError(INVALID_RECORD, [0])
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer, 1: answer })
      )

      const [a, c] = await sendAll(producer, [message('a', 0), message('c', 1)])

      expect(recordCounts(cluster)).toEqual([[1, 1]])
      expect(a.value).toBe(answer)
      expect(c.value).toBe(answer)
      expectRaw(answer)
    })
  })

  test('idempotent: a response that fails one partition, leaves one out and acknowledges one settles each on its own', async () => {
    const { producer, cluster } = await start({
      idempotent: true,
      partitions: 3,
      retry: { retries: 3, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
    })
    const answer = answerError(INVALID_RECORD, [0])
    cluster.broker.produce
      .mockImplementationOnce(async (request) =>
        resultsOf(request, { 0: answer }).filter(({ partition }) => partition !== 1)
      )
      .mockImplementation(async (request) => resultsOf(request))

    const [a, c, d] = await sendAll(producer, [message('a', 0), message('c', 1), message('d', 2)])
    await jest.advanceTimersByTimeAsync(20)
    await flushAll()

    expect(recordCounts(cluster)).toEqual([[1, 1, 1], [1]])
    expect(a.value.recordErrors).toEqual([
      { batchIndex: 0, errorMessage: 'bad 0', record: recordOf('a') },
    ])
    expect(c.state).toBe('fulfilled')
    expect(d.state).toBe('fulfilled')
    expectRaw(answer)
  })

  test('a projected record carries its fields as Buffers of one exact store, its timestamp as a string, and no custom timestamp', async () => {
    const { producer, cluster } = await start({ idempotent: false })
    const valueOf = jest.fn(() => 1700000000002)
    const answer = answerError(INVALID_RECORD, [0, 1, 2])
    let readsAtAnswer
    cluster.broker.produce.mockImplementation(async (request) => {
      readsAtAnswer = valueOf.mock.calls.length
      return resultsOf(request, { 0: answer })
    })

    const sent = watch(
      producer.send({
        topic: TOPIC,
        messages: [
          { value: 'v0', timestamp: 1700000000000 },
          { key: 'k1', value: null, timestamp: 1700000000001n },
          {
            key: Buffer.from('key2'),
            value: 'v2',
            headers: { a: 'x', b: ['y', Buffer.from('z')] },
            timestamp: { valueOf },
          },
        ],
      })
    )
    await jest.advanceTimersByTimeAsync(20)
    await flushAll()

    const records = sent.value.recordErrors.map(({ record }) => record)
    expect(records).toEqual([
      { key: null, value: Buffer.from('v0'), headers: {}, timestamp: '1700000000000' },
      { key: Buffer.from('k1'), value: null, headers: {}, timestamp: '1700000000001' },
      {
        key: Buffer.from('key2'),
        value: Buffer.from('v2'),
        headers: { a: Buffer.from('x'), b: [Buffer.from('y'), Buffer.from('z')] },
      },
    ])
    expect(records[2]).not.toHaveProperty('timestamp')
    const store = records[0].value.buffer
    expect(store.byteLength).toBe(13)
    for (const view of [
      records[1].key,
      records[2].key,
      records[2].headers.a,
      ...records[2].headers.b,
    ]) {
      expect(view.buffer).toBe(store)
    }
    expect(valueOf.mock.calls.length).toBe(readsAtAnswer)
    expectRaw(answer)
  })

  test('two owners whose records viewed one caller store get stores of their own: writing one changes nothing else', async () => {
    const { producer, cluster } = await start({ idempotent: false })
    const store = Buffer.from(Array.from({ length: 32 }, (_, i) => i))
    const original = Buffer.from(store)
    const answer = answerError(INVALID_RECORD, [0, 1])
    let sent
    cluster.broker.produce.mockImplementation(async (request) => {
      sent = [...request.topicData[0].partitions[0].messages]
      return resultsOf(request, { 0: answer })
    })

    const [a, b] = await sendAll(producer, [
      {
        key: store.subarray(0, 4),
        value: store.subarray(4, 12),
        headers: { h: store.subarray(12, 16) },
      },
      {
        key: store.subarray(16, 20),
        value: store.subarray(20, 28),
        headers: { h: [store.subarray(28, 32)] },
      },
    ])

    const first = a.value.recordErrors[0].record
    const second = b.value.recordErrors[1].record
    expect(a.value.recordErrors[1]).toBe(answer.recordErrors[1])
    expect(b.value.recordErrors[0]).toBe(answer.recordErrors[0])
    expect(first.value.buffer).not.toBe(second.value.buffer)
    expect(first.value.buffer).not.toBe(store.buffer)
    expect(first.key.buffer).toBe(first.value.buffer)
    expect(first.headers.h.buffer).toBe(first.value.buffer)
    expect(first.value.buffer.byteLength).toBe(16)
    expect(second.key.buffer).toBe(second.value.buffer)
    expect(second.headers.h[0].buffer).toBe(second.value.buffer)
    expect(second.value.buffer.byteLength).toBe(16)
    expect(first).toEqual({
      key: original.subarray(0, 4),
      value: original.subarray(4, 12),
      headers: { h: original.subarray(12, 16) },
    })

    new Uint8Array(first.value.buffer).fill(0xee)

    expect(second).toEqual({
      key: original.subarray(16, 20),
      value: original.subarray(20, 28),
      headers: { h: [original.subarray(28, 32)] },
    })
    expect(store).toEqual(original)
    expect(Buffer.concat([sent[0].key, sent[0].value, sent[0].headers.h])).toEqual(
      original.subarray(0, 16)
    )
    expectRaw(answer)
  })

  test('S = 50 coalesced sends and R = 50 reported indices make 50 owned entries and share 50 raw ones', async () => {
    const { producer, cluster } = await start({ idempotent: false })
    const count = 50
    const indices = Array.from({ length: count }, (_, i) => i)
    const answer = answerError(INVALID_RECORD, indices)
    cluster.broker.produce.mockImplementation(async (request) => resultsOf(request, { 0: answer }))

    const sends = await sendAll(
      producer,
      indices.map((i) => message(i))
    )

    expect(recordCounts(cluster)).toEqual([[count]])
    const errors = sends.map(({ value }) => value)
    expect(new Set(errors).size).toBe(count)
    expect(new Set(errors.map(({ recordErrors }) => recordErrors)).size).toBe(count)
    const entries = new Set()
    const stores = new Set()
    errors.forEach(({ recordErrors }, i) => {
      expect(recordErrors).toHaveLength(count)
      recordErrors.forEach((entry, j) => {
        entries.add(entry)
        if (j !== i) expect(entry).toBe(answer.recordErrors[j])
      })
      expect(recordErrors[i].record).toEqual(recordOf(i))
      stores.add(recordErrors[i].record.value.buffer)
    })
    expect(entries.size).toBe(2 * count)
    expect(stores.size).toBe(count)
  })

  test.each([[-1], [2], [1.5]])(
    'a record error at index %p names no record: every send gets the answer raw, and one warning names topic, partition and index',
    async (index) => {
      const { producer, cluster, logs } = await start({ idempotent: false })
      const answer = answerError(INVALID_RECORD, [index])
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer })
      )

      const [a, b] = await sendAll(producer, [message('a'), message('b')])

      expect(a.value).toBe(answer)
      expect(b.value).toBe(answer)
      expectRaw(answer)
      expect(logs.filter(({ log }) => /names no record/.test(log.message))).toEqual([
        expect.objectContaining({
          log: expect.objectContaining({ topic: TOPIC, partition: 0, batchIndex: index }),
        }),
      ])
    }
  )

  test('the allocator failing while records are copied still settles every send, with the answer itself', async () => {
    const { producer, cluster } = await start({ idempotent: false })
    const answer = answerError(INVALID_RECORD, [0])
    let allocate
    cluster.broker.produce.mockImplementation(async (request) => {
      allocate = jest.spyOn(Buffer, 'allocUnsafeSlow').mockImplementationOnce(() => {
        throw new RangeError('Array buffer allocation failed')
      })
      return resultsOf(request, { 0: answer })
    })

    const [a, b] = await sendAll(producer, [message('a'), message('b')])

    expect(allocate).toHaveBeenCalledTimes(1)
    expect(a.value).toBe(answer)
    expect(b.value).toBe(answer)
    expectRaw(answer)
  })

  test('a lookup error tracked by identity is never copied; one below the answer stays the same object in each copy', async () => {
    const { producer, cluster } = await start({ idempotent: false })
    const entries = Object.freeze([Object.freeze({ batchIndex: 0, errorMessage: 'bad' })])
    const marked = createEosManager.markLookupFailure(
      Object.assign(new KafkaJSBrokerNotFound('no broker', { retriable: false }), {
        topic: TOPIC,
        partition: 0,
        recordErrors: entries,
      })
    )
    const below = createEosManager.markLookupFailure(new KafkaJSBrokerNotFound('no broker'))
    const answer = answerError(INVALID_RECORD, [0])
    answer.cause = below
    cluster.broker.produce
      .mockImplementationOnce(async (request) => resultsOf(request, { 0: marked }))
      .mockImplementation(async (request) => resultsOf(request, { 0: answer }))

    const [a, b] = await sendAll(producer, [message('a'), message('b')])
    const [c, d] = await sendAll(producer, [message('c'), message('d')])

    expect(a.value).toBe(marked)
    expect(b.value).toBe(marked)
    expect(createEosManager.isDiscoveryExhausted(a.value)).toBe(true)
    expect(marked.recordErrors).toBe(entries)
    expect(c.value).not.toBe(answer)
    expect(c.value.recordErrors[0].record).toEqual(recordOf('c'))
    expect(c.value.cause).toBe(below)
    expect(d.value.cause).toBe(below)
    expect(createEosManager.isDiscoveryExhausted(c.value)).toBe(true)
    expectRaw(answer)
  })

  describe('a codec that throws one error object for two batches', () => {
    let saved
    beforeEach(() => {
      saved = Codecs[Types.Snappy]
    })
    afterEach(() => {
      Codecs[Types.Snappy] = saved
    })

    /**
     * A fake cluster whose broker builds each Produce request with the real request builder and
     * encodes it, so the registered codec runs, then acknowledges every partition.
     */
    const encodingCluster = () =>
      createFakeCluster({
        produce: async (request) => {
          const { request: built } = produceProtocol({ version: 7 })(request)
          await built.encode()
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

    const thrownError = () =>
      Object.assign(new KafkaJSNonRetriableError('codec refused'), {
        topic: TOPIC,
        partition: 0,
        recordErrors: Object.freeze([Object.freeze({ batchIndex: 0, errorMessage: 'bad' })]),
      })

    test('plain: thrown for a second batch while the first is still outstanding, it reaches both sends unchanged', async () => {
      const cluster = encodingCluster()
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        idempotent: false,
        linger: 0,
        retry: { retries: 0 },
      })
      producers.push(producer)
      await producer.connect()
      const thrown = thrownError()
      const entries = thrown.recordErrors
      const gate = deferred()
      let compressions = 0
      Codecs[Types.Snappy] = () => ({
        compress: async () => {
          compressions += 1
          if (compressions === 1) await gate.promise
          throw thrown
        },
        decompress: async () => Buffer.alloc(0),
      })

      const a = watch(
        producer.send({ topic: TOPIC, compression: Types.Snappy, messages: [message('a')] })
      )
      await flushAll()
      const b = watch(
        producer.send({ topic: TOPIC, compression: Types.Snappy, messages: [message('b')] })
      )
      await flushAll()

      expect(cluster.broker.produce).toHaveBeenCalledTimes(2)
      expect(b.state).toBe('rejected')
      expect(a.state).toBe('pending')

      gate.resolve()
      await flushAll()

      expect(a.value).toBe(thrown)
      expect(b.value).toBe(thrown)
      expect(thrown.recordErrors).toBe(entries)
      expectRaw(thrown)
    })

    test('idempotent: the thrown object reaches the send unchanged', async () => {
      const cluster = encodingCluster()
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        idempotent: true,
        linger: 0,
        retry: ONE_RETRY,
      })
      producers.push(producer)
      await producer.connect()
      const thrown = thrownError()
      const entries = thrown.recordErrors
      Codecs[Types.Snappy] = () => ({
        compress: async () => {
          throw thrown
        },
        decompress: async () => Buffer.alloc(0),
      })

      const a = watch(
        producer.send({ topic: TOPIC, compression: Types.Snappy, messages: [message('a')] })
      )
      await flushAll()

      expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
      expect(a.value).toBe(thrown)
      expect(thrown.recordErrors).toBe(entries)
      expectRaw(thrown)
    })
  })

  describe('plain', () => {
    test('a thrown error naming partition 0 settles the batches of partitions 0 and 1 with no record', async () => {
      const { producer, cluster } = await start({ idempotent: false, partitions: 2 })
      const entries = Object.freeze([Object.freeze({ batchIndex: 0, errorMessage: 'bad' })])
      const thrown = Object.assign(new KafkaJSNonRetriableError('refused'), {
        topic: TOPIC,
        partition: 0,
        recordErrors: entries,
      })
      cluster.broker.produce.mockImplementation(async () => {
        throw thrown
      })

      const [a, c] = await sendAll(producer, [message('a', 0), message('c', 1)])

      expect(recordCounts(cluster)).toEqual([[1, 1]])
      expect(a.value).toBe(thrown)
      expect(c.value).toBe(thrown)
      expect(thrown.recordErrors).toBe(entries)
      expectRaw(thrown)
    })

    test('a retriable answer whose retries run out rejects every send of the batch with no record', async () => {
      const { producer, cluster } = await start({
        idempotent: false,
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
      const answers = []
      cluster.broker.produce.mockImplementation(async (request) => {
        answers.push(answerError(NOT_ENOUGH_REPLICAS, [0]))
        return resultsOf(request, { 0: answers[answers.length - 1] })
      })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()

      expect(answers).toHaveLength(3)
      expect(a.value).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(b.value).toBe(a.value)
      expectRaw(a.value, ...answers)
    })

    test('a retriable answer, then the deadline passing during the backoff, rejects with a raw timeout', async () => {
      const { producer, cluster } = await start({
        idempotent: false,
        deliveryTimeout: 150,
        retry: { retries: 5, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0 },
      })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer })
      )

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(150)
      await flushAll()

      expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
      expect(a.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(a.value.cause).toBe(answer)
      expect(b.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expectRaw(a.value, b.value, answer)
    })

    test('retries running out while a cut batch has both halves queued reject both with no record', async () => {
      const { producer, cluster } = await start({
        idempotent: false,
        retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
      const answers = []
      cluster.broker.produce
        .mockImplementationOnce(async (request) => {
          throw request.guard(10 ** 9)
        })
        .mockImplementation(async (request) => {
          answers.push(answerError(NOT_ENOUGH_REPLICAS, [0]))
          return resultsOf(request, { 0: answers[answers.length - 1] })
        })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()

      expect(recordCounts(cluster)).toEqual([[2], [1], [1]])
      expect(a.value).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(b.value).toBe(a.value)
      expectRaw(a.value, ...answers)
    })
  })

  describe('idempotent', () => {
    const RETRY = { retries: 5, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }

    test('a retriable answer, then a drain fault throwing the identical object, rejects every send with it raw', async () => {
      const { producer, cluster, logs } = await start({ idempotent: true, retry: RETRY })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      let armed = false
      cluster.touchTargetTopic = jest.fn(() => {
        if (armed) {
          armed = false
          throw answer
        }
      })
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer })
      )
      const projections = jest.spyOn(Accumulator, 'projectionsFor')

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      expect(a.state).toBe('pending')
      armed = true
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()

      expect(cluster.touchTargetTopic).toHaveBeenCalledWith(TOPIC)
      expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
      expect(
        logs.filter(({ log }) => /The idempotent send loop failed/.test(log.message))
      ).toHaveLength(1)
      expect(a.value).toBe(answer)
      expect(b.value).toBe(answer)
      expect(projections).not.toHaveBeenCalled()
      expectRaw(answer)
    })

    test('a retriable answer, then its next request rejected as a whole with the identical object, rejects every send with it raw', async () => {
      const { producer, cluster } = await start({ idempotent: true, retry: RETRY })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      const fail = ProducerStateManager.prototype.fail
      jest
        .spyOn(ProducerStateManager.prototype, 'fail')
        .mockImplementation(function (batch, error, result) {
          if (result === undefined) {
            this.sequencer.fail(batch)
            return 'FAIL_BATCH'
          }
          return fail.call(this, batch, error, result)
        })
      cluster.broker.produce
        .mockImplementationOnce(async (request) => resultsOf(request, { 0: answer }))
        .mockImplementation(async () => {
          throw answer
        })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()

      expect(cluster.broker.produce).toHaveBeenCalledTimes(2)
      expect(a.value).toBe(answer)
      expect(b.value).toBe(answer)
      expectRaw(answer)
    })

    test('a retriable answer, then a thrown error, then the deadline: the timeout is around the thrown error, raw', async () => {
      const { producer, cluster } = await start({
        idempotent: true,
        deliveryTimeout: 150,
        retry: { retries: 5, initialRetryTime: 100, maxRetryTime: 100, factor: 0 },
      })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      const thrown = new KafkaJSError('connection lost')
      cluster.broker.produce
        .mockImplementationOnce(async (request) => resultsOf(request, { 0: answer }))
        .mockImplementationOnce(async () => {
          throw thrown
        })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(130)
      await flushAll()

      expect(cluster.broker.produce).toHaveBeenCalledTimes(2)
      expect(a.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(a.value.cause).toBe(thrown)
      expect(b.value).toBe(a.value)
      expectRaw(a.value, answer)
    })

    test('a retriable answer, then its unwritten retry cut as too large, then the deadline: both halves time out raw', async () => {
      const { producer, cluster } = await start({
        idempotent: true,
        deliveryTimeout: 150,
        retry: RETRY,
      })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0, 1])
      const held = deferred()
      cluster.broker.produce
        .mockImplementationOnce(async (request) => resultsOf(request, { 0: answer }))
        .mockImplementationOnce(async (request) => {
          throw request.guard(10 ** 9)
        })
        .mockImplementation(async (request) => {
          await held.promise
          return resultsOf(request)
        })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      try {
        await jest.advanceTimersByTimeAsync(130)
        await flushAll()

        expect(recordCounts(cluster)).toEqual([[2], [2], [1]])
        expect(a.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(b.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(a.value).not.toBe(b.value)
        expect(a.value.cause).toBe(answer)
        expect(b.value.cause).toBe(answer)
        expectRaw(a.value, b.value, answer)
      } finally {
        held.resolve()
        await flushAll()
      }
    })

    test("a retriable answer, then the deadline: the owner's timeout copy carries its record; the other partition's timeout is raw", async () => {
      const { producer, cluster } = await start({
        idempotent: true,
        partitions: 2,
        deliveryTimeout: 150,
        retry: { retries: 5, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0 },
      })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      const other = answerError(NOT_ENOUGH_REPLICAS, undefined, 1)
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer, 1: other })
      )

      const [a, b, c] = await sendAll(producer, [message('a', 0), message('b', 0), message('c', 1)])
      await jest.advanceTimersByTimeAsync(130)
      await flushAll()

      expect(recordCounts(cluster)).toEqual([[1, 2]])
      expect(a.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(b.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(a.value).not.toBe(b.value)
      expect(a.value.cause).not.toBe(answer)
      expect(a.value.cause).toBeInstanceOf(KafkaJSProtocolError)
      expect(a.value.cause.recordErrors).toEqual([
        { batchIndex: 0, errorMessage: 'bad 0', record: recordOf('a') },
      ])
      expect(b.value.cause).not.toBe(answer)
      expect(b.value.cause.recordErrors[0]).toBe(answer.recordErrors[0])
      expect(c.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(c.value.cause).toBe(other)
      expectRaw(answer)
    })

    test('a retriable answer whose retries run out is never projected', async () => {
      const { producer, cluster } = await start({
        idempotent: true,
        retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
      const answers = []
      cluster.broker.produce.mockImplementation(async (request) => {
        answers.push(answerError(NOT_ENOUGH_REPLICAS, [0]))
        return resultsOf(request, { 0: answers[answers.length - 1] })
      })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()

      expect(answers).toHaveLength(2)
      expect(a.value).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(b.value).toBe(a.value)
      expectRaw(a.value, ...answers)
    })

    test('one error object answering a retried batch and, later, another batch is ambiguous for good', async () => {
      const { producer, cluster } = await start({
        idempotent: true,
        partitions: 2,
        deliveryTimeout: 150,
        retry: { retries: 5, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0 },
      })
      const answer = answerError(NOT_ENOUGH_REPLICAS, [0])
      const fail = ProducerStateManager.prototype.fail
      jest
        .spyOn(ProducerStateManager.prototype, 'fail')
        .mockImplementation(function (batch, error, result) {
          if (batch.partition === 1) {
            this.sequencer.fail(batch)
            return 'FAIL_BATCH'
          }
          return fail.call(this, batch, error, result)
        })
      cluster.broker.produce.mockImplementation(async (request) =>
        resultsOf(request, { 0: answer, 1: answer })
      )

      const [a] = await sendAll(producer, [message('a', 0)])
      const [c] = await sendAll(producer, [message('c', 1)])

      expect(recordCounts(cluster)).toEqual([[1], [1]])
      expect(c.value).toBe(answer)

      await jest.advanceTimersByTimeAsync(130)
      await flushAll()

      expect(a.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(a.value.cause).toBe(answer)
      expectRaw(a.value, answer)
    })

    test('a batch cut after a refused unwritten attempt resolves each half’s record error against its own records', async () => {
      const { producer, cluster } = await start({ idempotent: true, retry: RETRY })
      const answers = []
      cluster.broker.produce
        .mockImplementationOnce(async (request) => {
          throw request.guard(10 ** 9)
        })
        .mockImplementation(async (request) => {
          answers.push(answerError(INVALID_RECORD, [0]))
          return resultsOf(request, { 0: answers[answers.length - 1] })
        })

      const [a, b] = await sendAll(producer, [message('a'), message('b')])
      await flushAll()

      expect(recordCounts(cluster)).toEqual([[2], [1], [1]])
      expect(a.value.recordErrors).toEqual([
        { batchIndex: 0, errorMessage: 'bad 0', record: recordOf('a') },
      ])
      expect(b.value.recordErrors).toEqual([
        { batchIndex: 0, errorMessage: 'bad 0', record: recordOf('b') },
      ])
      expectRaw(...answers)
    })
  })

  test('inside a transaction the batch’s sends get their own errors, and a later send and commit get the armed error itself, raw', async () => {
    const { producer, cluster, transaction } = await transactionalProducer({ linger: 20 })
    producers.push(producer)
    countRecords(cluster)
    const answer = answerError(INVALID_RECORD, [0])
    cluster.broker.produce.mockImplementation(async (request) => resultsOf(request, { 0: answer }))

    const a = watch(transaction.send({ topic: TOPIC, messages: [message('a')] }))
    const b = watch(transaction.send({ topic: TOPIC, messages: [message('b')] }))
    await jest.advanceTimersByTimeAsync(20)
    await flushAll()

    expect(recordCounts(cluster)).toEqual([[2]])
    expect(a.value).not.toBe(answer)
    expect(a.value).not.toBe(b.value)
    expect(a.value.recordErrors[0].record).toEqual(recordOf('a'))
    expect(b.value.recordErrors[0]).toBe(answer.recordErrors[0])

    const later = await transaction
      .send({ topic: TOPIC, messages: [message('c')] })
      .catch((error) => error)
    expect(later).toBeInstanceOf(Error)
    expect(chainOf(later)).toContain(answer)
    await expect(transaction.commit()).rejects.toBe(later)
    expectRaw(later, answer)
    await transaction.abort()
  })
})

describe('Accumulator.projectionsFor', () => {
  /**
   * Two sends of one record each, joined into one batch by a real accumulator whose send path
   * keeps the batch and settles nothing.
   * @returns {Promise<{ batch: object, sends: { state: string, value: unknown }[] }>}
   */
  const sharedBatch = async () => {
    const batches = []
    const accumulator = new Accumulator({
      linger: 1000,
      batchSize: 16384,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: (request) => {
        batches.push(...request.batches)
        return new Promise(() => {})
      },
    })
    const sends = ['a', 'b'].map((name) =>
      watch(
        accumulator.append({
          acks: -1,
          topicMessages: Accumulator.snapshot([{ topic: TOPIC, messages: [message(name)] }]),
        })
      )
    )
    await flushAll()
    accumulator.flush()
    await flushAll()
    expect(batches).toHaveLength(1)
    return { batch: batches[0], sends }
  }

  /** Settles every settler of `batch` as the send path does, and returns the projections. */
  const settleWith = async (batch, delivered, occurrence) => {
    const projected = Accumulator.projectionsFor(batch, delivered, occurrence)
    for (const settler of batch.settlers) settler.reject(projected?.get(settler) ?? delivered)
    await flushAll()
    return projected
  }

  /** An answer occurrence for `batch`, its error seen once. */
  const occurrenceFor = (answer, batch) => {
    const { seeAnswers, answerOccurrence } = require('../state/recordErrors')
    seeAnswers([{ error: answer }])
    return answerOccurrence(answer, batch)
  }

  test('a timeout with a locked cause around an answer with locked record errors: both copied with prototypes, symbols and attributes', async () => {
    const { batch, sends } = await sharedBatch()
    class CustomAnswer extends KafkaJSProtocolError {}
    const tag = Symbol('tag')
    const answer = new CustomAnswer(createErrorFromCode(INVALID_RECORD))
    const entries = Object.freeze([Object.freeze({ batchIndex: 0, errorMessage: 'bad' })])
    Object.defineProperty(answer, 'recordErrors', {
      value: entries,
      writable: false,
      enumerable: true,
      configurable: false,
    })
    answer[tag] = 'answer'
    const timeout = new KafkaJSDeliveryTimeout('late', { cause: answer, outcome: 'unknown' })
    Object.defineProperty(timeout, 'cause', {
      value: answer,
      writable: false,
      enumerable: false,
      configurable: false,
    })
    timeout[tag] = 'timeout'

    const projected = await settleWith(batch, timeout, occurrenceFor(answer, batch))

    const [first, second] = sends.map(({ value }) => value)
    expect(projected.size).toBe(2)
    expect(first).not.toBe(timeout)
    expect(second).not.toBe(first)
    for (const [copy, source] of [
      [first, timeout],
      [first.cause, answer],
      [second, timeout],
      [second.cause, answer],
    ]) {
      expect(copy).not.toBe(source)
      expect(Object.getPrototypeOf(copy)).toBe(Object.getPrototypeOf(source))
      expect(copy.stack).toBe(source.stack)
      for (const key of Reflect.ownKeys(source)) {
        if (key === 'cause' || key === 'recordErrors' || key === 'stack') continue
        expect(Object.getOwnPropertyDescriptor(copy, key)).toEqual(
          Object.getOwnPropertyDescriptor(source, key)
        )
      }
    }
    expect(first[tag]).toBe('timeout')
    expect(first.cause[tag]).toBe('answer')
    expect(Object.getOwnPropertyDescriptor(first, 'cause')).toEqual({
      value: first.cause,
      writable: false,
      enumerable: false,
      configurable: false,
    })
    expect(Object.getOwnPropertyDescriptor(first.cause, 'recordErrors')).toEqual({
      value: [{ batchIndex: 0, errorMessage: 'bad', record: recordOf('a') }],
      writable: false,
      enumerable: true,
      configurable: false,
    })
    expect(second.cause.recordErrors).toEqual([entries[0]])
    expect(second.cause.recordErrors[0]).toBe(entries[0])
    expect(second.cause.recordErrors).not.toBe(entries)
    expect(first.cause.cause).toBe(answer.cause)
    expect(timeout.cause).toBe(answer)
    expect(answer.recordErrors).toBe(entries)
  })

  test.each([
    [
      'a delivery timeout whose cause is an accessor',
      (answer, getter) => {
        const timeout = new KafkaJSDeliveryTimeout('late')
        Object.defineProperty(timeout, 'cause', { get: getter, configurable: true })
        return timeout
      },
    ],
    [
      'a delivery timeout with no cause of its own',
      (answer) => {
        const timeout = new KafkaJSDeliveryTimeout('late', { cause: answer })
        delete timeout.cause
        return timeout
      },
    ],
    [
      'a wrapper that is not a delivery timeout',
      (answer) => new KafkaJSError('wrapped', { cause: answer }),
    ],
  ])('%s gets no projection and runs no getter', async (_, wrap) => {
    const { batch, sends } = await sharedBatch()
    const answer = answerError(INVALID_RECORD, [0])
    const getter = jest.fn(() => answer)
    const delivered = wrap(answer, getter)

    const projected = await settleWith(batch, delivered, occurrenceFor(answer, batch))

    expect(projected).toBeNull()
    expect(getter).not.toHaveBeenCalled()
    expect(sends[0].value).toBe(delivered)
    expect(sends[1].value).toBe(delivered)
    expectRaw(answer)
  })

  test('an occurrence made before a cut projects onto neither half', async () => {
    const { batch } = await sharedBatch()
    const answer = answerError(INVALID_RECORD, [0])
    const occurrence = occurrenceFor(answer, batch)

    const [head, tail] = Accumulator.cut({ ...batch, settlers: batch.settlers.slice() }, 1)

    expect(Accumulator.projectionsFor({ ...batch, ...head }, answer, occurrence)).toBeNull()
    expect(Accumulator.projectionsFor({ ...batch, ...tail }, answer, occurrence)).toBeNull()
  })
})
