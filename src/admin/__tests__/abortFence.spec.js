const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const { createErrorFromCode } = require('../../protocol/error')
const apiKeys = require('../../protocol/requests/apiKeys')
const { attachResponseSource } = require('../../broker/failureSource')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
/** Makes one isolated public admin. @returns {object} fixture @throws {Error} invalid dependencies. */
const fixture = () => {
  const broker = {
    initProducerId: jest.fn(async () => ({ producerId: '9007199254740993', producerEpoch: 8 })),
  }
  const cluster = {
    addTargetTopic: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findLeaderForPartitions: jest.fn((_topic, parts) => ({ 1: parts })),
    findBroker: jest.fn(async () => broker),
    findGroupCoordinator: jest.fn(async () => broker),
  }
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
  return { broker, cluster, admin: createAdmin({ logger, cluster, retry }) }
}
const options = {
  topic: 'orders',
  partition: 2,
  producerId: '9007199254740993',
  producerEpoch: 7,
  coordinatorEpoch: 9,
}
/** A coordinator's InitProducerId reply error, noted as Connection#send notes it.
 * @param {number} code protocol error code @returns {Error} typed reply error
 */
const reply = (code) => {
  const error = createErrorFromCode(code)
  attachResponseSource(error, apiKeys.InitProducerId)
  return error
}
/** Creates one abort response. @param {number} [errorCode] code @returns {object} marker. */
const response = (errorCode = 0) => ({
  markers: [
    {
      producerId: options.producerId,
      topics: [{ topic: 'orders', partitions: [{ partition: 2, errorCode }] }],
    },
  ],
})

test('abort sends one false marker to the leader and resolves void', async () => {
  const f = fixture()
  f.broker.writeTxnMarkers = jest.fn(async () => response())
  await expect(f.admin.abortTransaction(options)).resolves.toBeUndefined()
  expect(f.broker.writeTxnMarkers).toHaveBeenCalledWith({
    markers: [
      {
        producerId: options.producerId,
        producerEpoch: 7,
        transactionResult: false,
        topics: [{ topic: 'orders', partitions: [2] }],
        coordinatorEpoch: 9,
        transactionVersion: 0,
      },
    ],
  })
})

test.each([47, 52, 31])('abort code %i is typed and terminal', async (code) => {
  const f = fixture()
  f.broker.writeTxnMarkers = jest.fn(async () => response(code))
  await expect(f.admin.abortTransaction(options)).rejects.toMatchObject({ code })
  expect(f.broker.writeTxnMarkers).toHaveBeenCalledTimes(1)
})

test.each([6, 7])('abort code %i retries and refreshes only for a moved leader', async (code) => {
  const f = fixture()
  f.broker.writeTxnMarkers = jest
    .fn()
    .mockResolvedValueOnce(response(code))
    .mockResolvedValueOnce(response())
  await expect(f.admin.abortTransaction(options)).resolves.toBeUndefined()
  expect(f.broker.writeTxnMarkers).toHaveBeenCalledTimes(2)
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(code === 6 ? 2 : 1)
})

test.each([
  'producer',
  'topic',
  'partition',
  'extraMarker',
  'extraTopic',
  'extraPartition',
  'empty',
  'code',
])('abort refuses %s identity or shape', async (variant) => {
  const f = fixture()
  const result = response()
  if (variant === 'producer') result.markers[0].producerId = '8'
  if (variant === 'topic') result.markers[0].topics[0].topic = 'other'
  if (variant === 'partition') result.markers[0].topics[0].partitions[0].partition = 1
  if (variant === 'extraMarker') result.markers.push(result.markers[0])
  if (variant === 'extraTopic') result.markers[0].topics.push(result.markers[0].topics[0])
  if (variant === 'extraPartition')
    result.markers[0].topics[0].partitions.push({ partition: 0, errorCode: 0 })
  if (variant === 'empty') result.markers = []
  if (variant === 'code') delete result.markers[0].topics[0].partitions[0].errorCode
  f.broker.writeTxnMarkers = jest.fn(async () => result)
  await expect(f.admin.abortTransaction(options)).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
})

test.each([14, 15, 16, 51])(
  'fence retries code %i without resending a successful id',
  async (code) => {
    const f = fixture()
    let pending = true
    f.broker.initProducerId.mockImplementation(
      async ({ transactionalId, transactionTimeout, ...rest }) => {
        expect(transactionTimeout).toBe(60000)
        expect(rest).toEqual({})
        if (transactionalId === 'b' && pending) {
          pending = false
          throw reply(code)
        }
        return { producerId: '9007199254740993', producerEpoch: 8 }
      }
    )
    const result = await f.admin.fenceProducers({ transactionalIds: ['a', 'b'] })
    expect(result.map((x) => x.errorCode)).toEqual([0, 0])
    expect(f.broker.initProducerId.mock.calls.map(([x]) => x.transactionalId)).toEqual([
      'a',
      'b',
      'b',
    ])
    expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes([15, 16].includes(code) ? 3 : 2)
  }
)

test('exhausted fencing retains completed ids and the last broker error', async () => {
  const f = fixture()
  f.broker.initProducerId.mockImplementation(async ({ transactionalId }) => {
    if (transactionalId === 'b') throw reply(51)
    return { producerId: '9007199254740993', producerEpoch: 8 }
  })
  const result = await f.admin.fenceProducers({ transactionalIds: ['a', 'b'] })
  expect(result.map((x) => x.errorCode)).toEqual([0, 51])
  expect(result[1].error.code).toBe(51)
  expect(result[1].producerId).toBeNull()
  expect(f.broker.initProducerId.mock.calls.map(([x]) => x.transactionalId)).toEqual([
    'a',
    'b',
    'b',
  ])
})

test.each(['coordinator lookup', 'InitProducerId call'])(
  'a protocol-coded local failure in the %s keeps code -1 and the original error',
  async (stage) => {
    const original = new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(15), {
      retryCount: 0,
      retryTime: 0,
    })
    const f = fixture()
    f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
      if (groupId === 'b' && stage === 'coordinator lookup') throw original
      return f.broker
    })
    f.broker.initProducerId.mockImplementation(async ({ transactionalId }) => {
      if (transactionalId === 'b') throw original
      return { producerId: '9007199254740993', producerEpoch: 8 }
    })
    const result = await f.admin.fenceProducers({ transactionalIds: ['a', 'b'] })
    expect(result.map((x) => x.errorCode)).toEqual([0, -1])
    expect(result[1].error).toBe(original)
    expect(result[1].producerId).toBeNull()
    expect(f.broker.initProducerId.mock.calls.map(([x]) => x.transactionalId)).toEqual(
      stage === 'coordinator lookup' ? ['a'] : ['a', 'b']
    )
  }
)

test('a protocol error noted for another request stays local, even from the InitProducerId call', async () => {
  const original = createErrorFromCode(15)
  attachResponseSource(original, apiKeys.ApiVersions)
  const f = fixture()
  f.broker.initProducerId.mockImplementation(async ({ transactionalId }) => {
    if (transactionalId === 'b') throw original
    return { producerId: '9007199254740993', producerEpoch: 8 }
  })
  const result = await f.admin.fenceProducers({ transactionalIds: ['a', 'b'] })
  expect(result.map((x) => x.errorCode)).toEqual([0, -1])
  expect(result[1].error).toBe(original)
  expect(f.broker.initProducerId.mock.calls.map(([x]) => x.transactionalId)).toEqual([
    'a',
    'b',
    'b',
  ])
})

/** A real coordinator Broker whose pool moved past its negotiated generation, so its InitProducerId
 * call re-runs ApiVersions and SASL setup first; only the setup requests are answered.
 * @param {object} answers ApiVersions and optional SaslHandshake response fields
 * @returns {object} broker, the API keys sent, the setup errors thrown and the pool's send spy
 * @throws {Error} invalid fixture
 */
const renegotiatingCoordinator = ({ apiVersions, handshake }) => {
  const Broker = require('../../broker')
  const Connection = require('../../network/connection')
  const { lookup } = require('../../protocol/requests')
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
  const table = {
    [apiKeys.ApiVersions]: { minVersion: 0, maxVersion: 4 },
    [apiKeys.InitProducerId]: { minVersion: 0, maxVersion: 5 },
    [apiKeys.SaslHandshake]: { minVersion: 0, maxVersion: 1 },
    [apiKeys.SaslAuthenticate]: { minVersion: 0, maxVersion: 2 },
  }
  const sent = []
  const thrown = []
  const attempt = {
    status: 'active',
    assertCurrent: () => {},
    refusal: () => null,
    guard: (work) => work,
    fail: () => {},
  }
  const connection = {
    setupAttempt: attempt,
    sasl: handshake ? { mechanism: 'plain', username: 'u', password: 'p' } : null,
    versions: table,
    logger,
    preparing: 0,
    connectAttempt: 1,
    socket: {},
    authEpisode: null,
    isConnected: () => true,
    isAuthenticated: () => false,
    connect: async () => {},
    finishSetup: () => {},
    markReady: () => {},
    authenticate(options) {
      return Connection.prototype.authenticate.call(this, options)
    },
    send: async ({ request, response }) => {
      sent.push(request.apiKey)
      try {
        return await response.parse(
          request.apiKey === apiKeys.ApiVersions ? apiVersions : handshake
        )
      } catch (error) {
        thrown.push(error)
        throw error
      }
    },
  }
  const pool = {
    host: 'node1',
    port: 9092,
    generation: 2,
    sasl: Boolean(handshake),
    setupPolicy: { timeoutCeiling: 1000, waitCeiling: 100 },
    pool: [connection],
    isConnected: () => true,
    isAuthenticated: () => false,
    authenticationBlackoutEntry: () => null,
    beginSetup: () => attempt,
    setVersions: () => {},
    throwIfRetired: () => {},
    getConnectionByRequest: async () => connection,
    send: jest.fn(async () => ({ producerId: '9007199254740993', producerEpoch: 8 })),
  }
  const broker = new Broker({ connectionPool: pool, logger, nodeId: 1, versions: table })
  broker.lookupRequest = lookup(table)
  broker.versionsGeneration = 1
  return { broker, sent, thrown, send: pool.send }
}

const apiVersionsRows = [
  { apiKey: apiKeys.InitProducerId, minVersion: 0, maxVersion: 5 },
  { apiKey: apiKeys.SaslHandshake, minVersion: 0, maxVersion: 1 },
  { apiKey: apiKeys.SaslAuthenticate, minVersion: 0, maxVersion: 2 },
]

test.each([
  ['ApiVersions code 31', { apiVersions: { errorCode: 31, apiVersions: [] } }, [18]],
  ['ApiVersions code 15', { apiVersions: { errorCode: 15, apiVersions: [] } }, [18]],
  [
    'SaslHandshake code 33',
    {
      apiVersions: { errorCode: 0, apiVersions: apiVersionsRows },
      handshake: { errorCode: 33, enabledMechanisms: ['SCRAM-SHA-512'] },
    },
    [18, 17],
  ],
])(
  'a fence call whose connection setup fails with %s keeps code -1 and the original error',
  async (_name, answers, keys) => {
    const coordinator = renegotiatingCoordinator(answers)
    const f = fixture()
    f.admin = createAdmin({
      logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
      cluster: f.cluster,
      retry: { ...retry, retries: 0 },
    })
    f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) =>
      groupId === 'b' ? coordinator.broker : f.broker
    )
    const result = await f.admin.fenceProducers({ transactionalIds: ['a', 'b'] })
    expect(result.map((x) => x.errorCode)).toEqual([0, -1])
    expect(coordinator.thrown).toHaveLength(1)
    expect(result[1].error).toBe(coordinator.thrown[0])
    expect(coordinator.sent).toEqual(keys)
    expect(coordinator.send).not.toHaveBeenCalled()
    expect(f.broker.initProducerId).toHaveBeenCalledTimes(1)
  }
)

test.each([31, 47, 52, 53, 90])('fence code %i is terminal item data', async (code) => {
  const f = fixture()
  f.broker.initProducerId.mockRejectedValue(reply(code))
  const result = await f.admin.fenceProducers({ transactionalIds: ['a'] })
  expect(result[0].error.code).toBe(code)
  expect(f.broker.initProducerId).toHaveBeenCalledTimes(1)
})

test.each([
  ['abortTransaction', { ...options, producerId: -1 }],
  ['abortTransaction', { ...options, producerId: 9007199254740992 }],
  ['abortTransaction', { ...options, producerEpoch: 32768 }],
  ['abortTransaction', { ...options, producerEpoch: null }],
  ['abortTransaction', { ...options, coordinatorEpoch: -1 }],
  ['abortTransaction', { ...options, partition: 2147483648 }],
  ['fenceProducers', { transactionalIds: [] }],
  ['fenceProducers', { transactionalIds: ['a', 'a'] }],
  ['fenceProducers', { transactionalIds: ['a'], transactionTimeout: 0 }],
  ['fenceProducers', { transactionalIds: ['a'], transactionTimeout: null }],
  ['fenceProducers', { transactionalIds: ['a'], transactionTimeout: '60000' }],
])('%s rejects invalid input before routing %#', async (method, input) => {
  const f = fixture()
  await expect(f.admin[method](input)).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.addTargetTopic).not.toHaveBeenCalled()
  expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
})

test('the existing InitProducerId builder keeps minus-one sentinels', async () => {
  const request = require('../../protocol/requests/initProducerId/v5/request')({
    transactionalId: 'a',
    transactionTimeout: 60000,
  })
  const fields = require('../../../testHelpers/wire/initProducerIdV5').readRequest(
    (await request.encode()).buffer
  )
  expect(fields).toEqual({
    transactionalId: 'a',
    transactionTimeout: 60000,
    producerId: '-1',
    producerEpoch: -1,
  })
})

test.each([1, 2])(
  'public abort and fence traverse marker version %i and unchanged producer-id byte codecs',
  async (version) => {
    const { Kafka } = require('../../../index')
    const {
      createScriptedCluster,
      producerScript,
    } = require('../../../testHelpers/scriptedCluster')
    const cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'transactions',
      advertise: { WriteTxnMarkers: [version, version] },
    })
    producerScript(cluster, { topic: 'orders', partitions: 3 })
    const admin = new Kafka(cluster.kafkaOptions({ retry, logLevel: 0 })).admin({ retry })
    try {
      await admin.connect()
      await expect(admin.abortTransaction(options)).resolves.toBeUndefined()
      const marker = cluster.requests().find((x) => x.api === 'WriteTxnMarkers')
      expect(marker.version).toBe(version)
      expect(marker.fields.markers[0]).toMatchObject({
        transactionResult: false,
        ...(version === 2 ? { transactionVersion: 0 } : {}),
        producerId: options.producerId,
      })
      const result = await admin.fenceProducers({ transactionalIds: ['tx-a'] })
      expect(result[0].errorCode).toBe(0)
      const init = cluster.requests().find((x) => x.api === 'InitProducerId')
      expect(init.fields).toMatchObject({
        transactionalId: 'tx-a',
        transactionTimeout: 60000,
        producerId: '-1',
        producerEpoch: -1,
      })
    } finally {
      await admin.disconnect()
    }
    cluster.assertClean()
  }
)

test('a genuine InitProducerId refusal on the fake socket stays the broker reply for its id', async () => {
  const { Kafka } = require('../../../index')
  const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
  const cluster = createScriptedCluster({
    nodes: [{ id: 0, host: 'node0', port: 9092 }],
    profile: 'transactions',
  })
  producerScript(cluster, { topic: 'orders', partitions: 3 })
  cluster.answer('InitProducerId', () => ({ errorCode: 53, producerId: '-1', producerEpoch: -1 }))
  const admin = new Kafka(cluster.kafkaOptions({ retry, logLevel: 0 })).admin({ retry })
  try {
    await admin.connect()
    const [result] = await admin.fenceProducers({ transactionalIds: ['tx-a'] })
    expect(result.errorCode).toBe(53)
    expect(result.error.code).toBe(53)
    expect(result.producerId).toBeNull()
    expect(cluster.requests().filter((x) => x.api === 'InitProducerId')).toHaveLength(1)
  } finally {
    await admin.disconnect()
  }
  cluster.assertClean()
})

test.each([1, 2147483647])(
  'fence preserves transaction timeout boundary %i',
  async (transactionTimeout) => {
    const f = fixture()
    await f.admin.fenceProducers({ transactionalIds: ['a'], transactionTimeout })
    expect(f.broker.initProducerId).toHaveBeenCalledWith({
      transactionalId: 'a',
      transactionTimeout,
    })
  }
)

test.each([
  [0, 0],
  [32767, 2147483647],
])('abort accepts epoch boundary %i/%i', async (producerEpoch, coordinatorEpoch) => {
  const f = fixture()
  f.broker.writeTxnMarkers = jest.fn(async () => response())
  await f.admin.abortTransaction({ ...options, producerEpoch, coordinatorEpoch })
  expect(f.broker.writeTxnMarkers.mock.calls[0][0].markers[0]).toMatchObject({
    producerEpoch,
    coordinatorEpoch,
  })
})
