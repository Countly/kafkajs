const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const { itemRetry } = require('../itemRetry')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSConnectionError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
/** Builds a brokerless public admin. @param {object} [overrides] routes @param {object} [retryOptions] retry @returns {object} fixture @throws {Error} invalid fixture. */
const fixture = (overrides = {}, retryOptions = retry) => {
  const broker = { nodeId: 1 }
  const cluster = {
    addTargetTopic: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findBroker: jest.fn(async () => broker),
    findLeaderForPartitions: jest.fn((_topic, parts) => ({ 1: parts })),
    findGroupCoordinator: jest.fn(async () => broker),
    brokerPool: { brokers: { 1: broker } },
    ...overrides,
  }
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
  return { broker, cluster, admin: createAdmin({ logger, cluster, retry: retryOptions }) }
}
/** One transaction result. @param {string} transactionalId identity @param {number} [errorCode] code @returns {object} row. */
const row = (transactionalId, errorCode = 0) => ({
  transactionalId,
  errorCode,
  state: 'Ongoing',
  timeoutMs: 60000,
  startTimeMs: '9007199254740994',
  producerId: '9007199254740993',
  producerEpoch: 7,
  topics: [],
})

test('public transaction methods are functions', () => {
  const { admin } = fixture()
  for (const name of [
    'describeProducers',
    'describeTransactions',
    'listTransactions',
    'abortTransaction',
    'fenceProducers',
  ])
    expect(typeof admin[name]).toBe('function')
  expect(admin.forceTerminateTransaction).toBeUndefined()
})

test('local route failures retain the original typed error beside completed items', async () => {
  const original = new KafkaJSConnectionError('route gone', { retriable: true })
  const send = jest.fn(async (items) =>
    items.map((item) =>
      item.id === 'a' ? { ...item, errorCode: 0 } : { ...item, errorCode: -1, error: original }
    )
  )
  const result = await itemRetry(
    {
      items: [{ id: 'a' }, { id: 'b' }],
      keyOf: (x) => x.id,
      retry,
      retryable: (x) => x.errorCode === -1,
      callFailure: () => false,
      sanitize: (x) => x,
    },
    send
  )
  expect(result[1].error).toBe(original)
  expect(result.map((x) => x.errorCode)).toEqual([0, -1])
  expect(send.mock.calls[1][0]).toEqual([{ id: 'b' }])
})

test('transactions batch per coordinator and retry only the moved id', async () => {
  const f = fixture()
  const moved = { describeTransactions: jest.fn(async () => ({ transactions: [row('b')] })) }
  let movedId = false
  f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId, coordinatorType }) => {
    expect(coordinatorType).toBe(1)
    return groupId === 'b' && movedId ? moved : f.broker
  })
  f.broker.describeTransactions = jest.fn(async ({ transactionalIds }) => {
    movedId = true
    return { transactions: transactionalIds.map((id) => row(id, id === 'b' ? 16 : 0)) }
  })
  const result = await f.admin.describeTransactions({ transactionalIds: ['a', 'b'] })
  expect(result.map((x) => [x.transactionalId, x.errorCode])).toEqual([
    ['a', 0],
    ['b', 0],
  ])
  expect(f.broker.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['a', 'b'] })
  expect(moved.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['b'] })
  expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(3)
})

test('loading transactions retry without another coordinator lookup and retain int64 strings', async () => {
  const f = fixture()
  f.broker.describeTransactions = jest
    .fn()
    .mockResolvedValueOnce({ transactions: [row('a', 14), row('b', 53)] })
    .mockResolvedValueOnce({ transactions: [row('a')] })
  const result = await f.admin.describeTransactions({ transactionalIds: ['a', 'b'] })
  expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
  expect(f.broker.describeTransactions.mock.calls[1][0].transactionalIds).toEqual(['a'])
  expect(result[0].producerId).toBe('9007199254740993')
  expect(result[1].error.code).toBe(53)
})

test('a later route failure leaves completed coordinator results intact', async () => {
  const original = new KafkaJSConnectionError('gone')
  const f = fixture()
  f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
    if (groupId === 'b') throw original
    return f.broker
  })
  f.broker.describeTransactions = jest.fn(async () => ({ transactions: [row('a')] }))
  const result = await f.admin.describeTransactions({ transactionalIds: ['a', 'b'] })
  expect(result[0].errorCode).toBe(0)
  expect(result[1].errorCode).toBe(-1)
  expect(result[1].error).toBe(original)
  expect(f.broker.describeTransactions).toHaveBeenCalledTimes(1)
})

test('a relocation lookup failing with a protocol code keeps code -1 and the original error', async () => {
  const original = new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(15), {
    retryCount: 0,
    retryTime: 0,
  })
  const f = fixture()
  let lookups = 0
  f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
    if (groupId === 'b' && lookups++ > 0) throw original
    return f.broker
  })
  f.broker.describeTransactions = jest.fn(async () => ({
    transactions: [row('a'), row('b', 16)],
  }))
  const result = await f.admin.describeTransactions({ transactionalIds: ['a', 'b'] })
  expect(result.map((x) => x.errorCode)).toEqual([0, -1])
  expect(result[1].error).toBe(original)
  expect(f.broker.describeTransactions).toHaveBeenCalledTimes(1)
})

test.each(['missing', 'duplicate', 'foreign'])(
  'transaction response refuses %s identity',
  async (variant) => {
    const f = fixture()
    f.broker.describeTransactions = jest.fn(async () => ({
      transactions:
        variant === 'missing'
          ? []
          : variant === 'duplicate'
            ? [row('a'), row('a')]
            : [row('other')],
    }))
    await expect(f.admin.describeTransactions({ transactionalIds: ['a'] })).rejects.toBeInstanceOf(
      KafkaJSInvariantViolation
    )
  }
)

test('describe producer retries a moved leader and preserves completed partitions', async () => {
  const f = fixture()
  f.broker.describeProducers = jest
    .fn()
    .mockResolvedValueOnce({
      topics: [
        {
          topic: 'orders',
          partitions: [
            { partition: 0, errorCode: 0, activeProducers: [{ producerId: '9007199254740993' }] },
            { partition: 2, errorCode: 6, activeProducers: [] },
          ],
        },
      ],
    })
    .mockResolvedValueOnce({
      topics: [
        { topic: 'orders', partitions: [{ partition: 2, errorCode: 7, activeProducers: [] }] },
      ],
    })
  const result = await f.admin.describeProducers({
    topicPartitions: [{ topic: 'orders', partitions: [0, 2] }],
  })
  expect(result[0].partitions.map((x) => x.errorCode)).toEqual([0, 7])
  expect(f.broker.describeProducers.mock.calls[1][0]).toEqual({
    topics: [{ topic: 'orders', partitions: [2] }],
  })
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(2)
})

test('an explicitly selected replica reports code six without relocating', async () => {
  const f = fixture()
  f.broker.describeProducers = jest.fn(async () => ({
    topics: [
      { topic: 'orders', partitions: [{ partition: 0, errorCode: 6, activeProducers: [] }] },
    ],
  }))
  const result = await f.admin.describeProducers({
    topicPartitions: [{ topic: 'orders', partitions: [0] }],
    brokerId: 8,
  })
  expect(result[0].partitions[0].error.code).toBe(6)
  expect(f.cluster.findBroker).toHaveBeenCalledWith({ nodeId: 8 })
  expect(f.cluster.findLeaderForPartitions).not.toHaveBeenCalled()
  expect(f.cluster.addTargetTopic).not.toHaveBeenCalled()
  expect(f.broker.describeProducers).toHaveBeenCalledTimes(1)
})

test.each(['missing', 'duplicate', 'foreign', 'duplicateTopic'])(
  'producer response refuses %s identity',
  async (variant) => {
    const f = fixture()
    const topics = [
      { topic: 'orders', partitions: [{ partition: 0, errorCode: 0, activeProducers: [] }] },
    ]
    if (variant === 'missing') topics[0].partitions = []
    if (variant === 'duplicate') topics[0].partitions.push({ ...topics[0].partitions[0] })
    if (variant === 'foreign') topics[0].topic = 'other'
    if (variant === 'duplicateTopic') topics.push({ topic: 'orders', partitions: [] })
    f.broker.describeProducers = jest.fn(async () => ({ topics }))
    await expect(
      f.admin.describeProducers({ topicPartitions: [{ topic: 'orders', partitions: [0] }] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  }
)

test('list transactions visits every broker sequentially and returns unknown states', async () => {
  const calls = []
  const brokers = [1, 2, 3].map((id) => ({
    listTransactions: jest.fn(async (filters) => {
      calls.push(id)
      expect(filters.producerIds).toEqual(['9007199254740993'])
      expect(filters.transactionalIdPattern).toBe('(?P<name>tx).*')
      return {
        transactions: [{ transactionalId: 'tx-' + id, producerId: String(id), state: 'Ongoing' }],
        unknownStateFilters: ['FutureState'],
      }
    }),
  }))
  const f = fixture({
    brokerPool: { brokers: { 1: brokers[0], 2: brokers[1], 3: brokers[2] } },
    findBroker: jest.fn(async ({ nodeId }) => brokers[Number(nodeId) - 1]),
  })
  const result = await f.admin.listTransactions({
    producerIds: ['9007199254740993'],
    transactionalIdPattern: '(?P<name>tx).*',
  })
  expect(calls).toEqual([1, 2, 3])
  expect(result.transactions).toHaveLength(3)
  expect(result.unknownStateFilters).toEqual(['FutureState'])
})

test.each([15, 16, 31, 128])(
  'list transactions code %i fails the whole call without relocating',
  async (code) => {
    const f = fixture()
    f.broker.listTransactions = jest.fn(async () => {
      throw createErrorFromCode(code)
    })
    await expect(f.admin.listTransactions()).rejects.toMatchObject({ code })
    expect(f.broker.listTransactions).toHaveBeenCalledTimes(1)
    expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
  }
)

test('list transactions retries only coordinator loading', async () => {
  const f = fixture()
  f.broker.listTransactions = jest
    .fn()
    .mockRejectedValueOnce(createErrorFromCode(14))
    .mockResolvedValueOnce({ transactions: [], unknownStateFilters: [] })
  await expect(f.admin.listTransactions({ states: [], producerIds: [] })).resolves.toEqual({
    transactions: [],
    unknownStateFilters: [],
  })
  expect(f.broker.listTransactions).toHaveBeenCalledTimes(2)
})

test.each([
  ['describeProducers', null],
  ['describeProducers', { topicPartitions: [] }],
  ['describeProducers', { topicPartitions: [{ topic: 'orders', partitions: [0, 0] }] }],
  ['describeProducers', { topicPartitions: [{ topic: 'orders', partitions: [-1] }] }],
  [
    'describeProducers',
    { topicPartitions: [{ topic: 'orders', partitions: [0] }], brokerId: null },
  ],
  ['describeTransactions', { transactionalIds: [] }],
  ['describeTransactions', { transactionalIds: ['a', 'a'] }],
  ['describeTransactions', { transactionalIds: ['a\0b'] }],
  ['listTransactions', null],
  ['listTransactions', { producerIds: [Number.MAX_SAFE_INTEGER + 1] }],
  ['listTransactions', { producerIds: ['9223372036854775808'] }],
  ['listTransactions', { producerIds: ['01'] }],
  ['listTransactions', { producerIds: ['1e3'] }],
  ['listTransactions', { producerIds: ['-1'] }],
  ['listTransactions', { producerIds: [1, '1'] }],
  ['listTransactions', { producerIds: [1.5] }],
  ['listTransactions', { durationFilter: -1 }],
  ['listTransactions', { durationFilter: '10' }],
  ['listTransactions', { durationFilter: Infinity }],
  ['listTransactions', { durationFilter: 0.5 }],
  ['listTransactions', { transactionalIdPattern: /tx/ }],
  ['listTransactions', { transactionalIdPattern: null }],
])('%s rejects invalid input before routing %#', async (method, options) => {
  const f = fixture()
  await expect(f.admin[method](options)).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.findBroker).not.toHaveBeenCalled()
  expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
  expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
  expect(f.cluster.addTargetTopic).not.toHaveBeenCalled()
})

test('the original deadline retires a held lookup before any transaction RPC', async () => {
  jest.useFakeTimers()
  try {
    const f = fixture()
    let release
    f.cluster.findGroupCoordinator.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    f.broker.describeTransactions = jest.fn(async () => ({ transactions: [row('a')] }))
    const outcome = f.admin
      .describeTransactions({ transactionalIds: ['a'], apiTimeout: 5 })
      .catch((error) => error)
    await jest.advanceTimersByTimeAsync(5)
    expect((await outcome).name).toBe('KafkaJSTimeout')
    release(f.broker)
    await jest.advanceTimersByTimeAsync(0)
    expect(f.broker.describeTransactions).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  } finally {
    jest.useRealTimers()
  }
})

/** Builds the production Broker surface with an observed protocol sender. @param {object} versions offered ranges @returns {object} broker and frames @throws {Error} invalid ranges. */
const brokerFixture = (versions) => {
  const Broker = require('../../broker')
  const { lookup } = require('../../protocol/requests')
  const broker = Object.create(Broker.prototype)
  broker.versions = versions
  broker.lookupRequest = lookup(versions)
  const sender = Object.getOwnPropertySymbols(Broker.prototype).find(
    (key) => key.description === 'private:Broker:sendRequest'
  )
  broker[sender] = jest.fn(async (built) => built)
  return { broker, sent: broker[sender] }
}

test.each([
  [
    { 66: { minVersion: 0, maxVersion: 1 } },
    { transactionalIdPattern: 'tx.*', durationFilter: '-1' },
    'transactionalIdPattern needs v2',
    'v0-v1',
  ],
  [
    { 66: { minVersion: 0, maxVersion: 0 } },
    { transactionalIdPattern: null, durationFilter: '0' },
    'durationFilter needs v1',
    'v0-v0',
  ],
])(
  'meaningful list filters refuse unsupported ranges %#',
  async (offered, filters, needed, range) => {
    const { broker, sent } = brokerFixture(offered)
    await expect(
      broker.listTransactions({ states: [], producerIds: [], ...filters })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      message: expect.stringContaining(needed),
    })
    await expect(
      broker.listTransactions({ states: [], producerIds: [], ...filters })
    ).rejects.toThrow(range)
    expect(sent).not.toHaveBeenCalled()
  }
)

test.each([0, 1, 2])(
  'empty transaction filters remain valid at list version %i',
  async (version) => {
    const { broker } = brokerFixture({ 66: { minVersion: version, maxVersion: version } })
    const built = await broker.listTransactions({
      states: [],
      producerIds: [],
      durationFilter: '-1',
      transactionalIdPattern: '',
    })
    expect(built.request.apiVersion).toBe(version)
  }
)

test.each([
  ['describeProducers', 61, { topics: [] }],
  ['describeTransactions', 65, { transactionalIds: [] }],
  ['writeTxnMarkers', 27, { markers: [] }],
])('%s missing capability names both client and broker ranges', async (method, key, options) => {
  const { broker, sent } = brokerFixture({ [key]: { minVersion: 50, maxVersion: 51 } })
  await expect(broker[method](options)).rejects.toThrow('broker offers v50-v51')
  expect(sent).not.toHaveBeenCalled()
})

test('a failed RPC after another coordinator succeeded never resends that success', async () => {
  const f = fixture()
  const failed = {
    describeTransactions: jest.fn(async () => {
      throw new KafkaJSConnectionError('route lost')
    }),
  }
  f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) =>
    groupId === 'a' ? f.broker : failed
  )
  f.broker.describeTransactions = jest.fn(async () => ({ transactions: [row('a')] }))
  const result = await f.admin.describeTransactions({ transactionalIds: ['a', 'b'] })
  expect(result[0].errorCode).toBe(0)
  expect(result[1].errorCode).toBe(-1)
  expect(f.broker.describeTransactions).toHaveBeenCalledTimes(1)
  expect(failed.describeTransactions).toHaveBeenCalledTimes(2)
})

test('public diagnostics traverse negotiated byte codecs on the fake socket seam', async () => {
  const { Kafka } = require('../../../index')
  const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
  const cluster = createScriptedCluster({
    nodes: [{ id: 0, host: 'node0', port: 9092 }],
    profile: 'transactions',
  })
  producerScript(cluster, { topic: 'orders', partitions: 3 })
  cluster.answer('DescribeProducers', ({ topics }) => ({
    topics: topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map((partition) => ({
        partition,
        errorCode: 0,
        errorMessage: null,
        activeProducers: [
          {
            producerId: '9007199254740993',
            producerEpoch: 65537,
            lastSequence: 23,
            lastTimestamp: '-1',
            coordinatorEpoch: 9,
            currentTxnStartOffset: '-1',
          },
        ],
      })),
    })),
  }))
  cluster.answer('DescribeTransactions', ({ transactionalIds }) => ({
    transactions: transactionalIds.map((id) => row(id)),
  }))
  const admin = new Kafka(cluster.kafkaOptions({ retry, logLevel: 0 })).admin({ retry })
  try {
    await admin.connect()
    const result = await admin.describeTransactions({ transactionalIds: ['tx-a', 'tx-b'] })
    expect(result.map((x) => x.producerId)).toEqual(['9007199254740993', '9007199254740993'])
    const wire = cluster.requests().find((x) => x.api === 'DescribeTransactions')
    expect(wire.version).toBe(0)
    expect(wire.fields.transactionalIds).toEqual(['tx-a', 'tx-b'])
    const described = await admin.describeProducers({
      topicPartitions: [{ topic: 'orders', partitions: [0, 2] }],
    })
    expect(described[0].partitions[0].activeProducers[0].producerEpoch).toBe(65537)
    const listed = await admin.listTransactions({
      transactionalIdPattern: '^tx-.*$',
      durationFilter: 250,
    })
    expect(listed.unknownStateFilters).toEqual(['FutureState'])
    expect(
      cluster.requests().find((x) => x.api === 'ListTransactions').fields.transactionalIdPattern
    ).toBe('^tx-.*$')
  } finally {
    await admin.disconnect()
  }
  cluster.assertClean()
})

test.each([0, Number.MAX_SAFE_INTEGER, '9223372036854775807'])(
  'list accepts lossless producer-id boundary %s',
  async (producerId) => {
    const f = fixture()
    f.broker.listTransactions = jest.fn(async () => ({ transactions: [], unknownStateFilters: [] }))
    await f.admin.listTransactions({ producerIds: [producerId] })
    expect(f.broker.listTransactions.mock.calls[0][0].producerIds).toEqual([String(producerId)])
  }
)

test.each([0, Number.MAX_SAFE_INTEGER])(
  'list accepts duration boundary %s',
  async (durationFilter) => {
    const f = fixture()
    f.broker.listTransactions = jest.fn(async () => ({ transactions: [], unknownStateFilters: [] }))
    await f.admin.listTransactions({ durationFilter })
    expect(f.broker.listTransactions.mock.calls[0][0].durationFilter).toBe(String(durationFilter))
  }
)

test.each([0, 2147483647])(
  'explicit replica and partition boundary %i remains exact',
  async (value) => {
    const f = fixture()
    f.broker.describeProducers = jest.fn(async () => ({
      topics: [
        { topic: 'orders', partitions: [{ partition: value, errorCode: 0, activeProducers: [] }] },
      ],
    }))
    const result = await f.admin.describeProducers({
      topicPartitions: [{ topic: 'orders', partitions: [value] }],
      brokerId: value,
    })
    expect(result[0].partitions[0].partition).toBe(value)
    expect(f.cluster.findBroker).toHaveBeenCalledWith({ nodeId: value })
  }
)
