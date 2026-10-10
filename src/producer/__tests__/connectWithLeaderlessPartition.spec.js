const createProducer = require('../index')
const Cluster = require('../../cluster')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { parse: parseMetadata } = require('../../protocol/requests/metadata/v13/response')
const { KafkaJSConnectionError, KafkaJSRequestTimeoutError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger, connectionOpts } = require('testHelpers')

const LEADER_NOT_AVAILABLE = 5
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
const RETRY = { retries: 1, initialRetryTime: 1, maxRetryTime: 5 }

/** Three nodes; node 3 coordinates every transactional id and every group. */
const NODES = [1, 2, 3].map((nodeId) => ({
  nodeId,
  host: `kafka${nodeId}`,
  port: 9092,
  rack: `rack-${nodeId}`,
}))
const [SEED, , COORDINATOR] = NODES
const addressOf = ({ host, port }) => `${host}:${port}`

const topicMetadata = (topic, partitionErrorCode = 0) => ({
  topicErrorCode: 0,
  topic,
  isInternal: false,
  partitionMetadata: [
    {
      partitionErrorCode,
      partitionId: 0,
      leader: partitionErrorCode === 0 ? 1 : -1,
      leaderEpoch: 0,
      replicas: [2, 3],
      isr: [],
      offlineReplicas: [],
    },
  ],
})

/**
 * A Metadata answer through the real response parse: the topics asked for are healthy, and a
 * request for every topic also carries `unrelated`, whose only partition has no leader.
 * @param {string[]} topics
 */
const answerMetadata = (topics) =>
  parseMetadata({
    throttleTime: 0,
    errorCode: 0,
    clusterId: 'fake',
    controllerId: SEED.nodeId,
    brokers: NODES,
    topicMetadata:
      topics.length > 0
        ? topics.map((topic) => topicMetadata(topic))
        : [topicMetadata('healthy'), topicMetadata('unrelated', LEADER_NOT_AVAILABLE)],
  })

/**
 * A real `Cluster` whose brokers are fakes: each answers at its address, records every request,
 * and refuses one while it is not connected. FindCoordinator names `control.coordinator`, and
 * throws `control.findCoordinatorFailure.error` while its `times` last.
 * @returns {{ cluster: Cluster, requests: object[], created: object[], control: object }}
 */
const createCluster = () => {
  const requests = []
  const created = []
  const control = { coordinator: COORDINATOR, findCoordinatorFailure: null }
  const cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retry: RETRY })
  const pool = cluster.brokerPool

  pool.buildConnectionPool = async ({ host = SEED.host, port = SEED.port, rack = null } = {}) => ({
    host,
    port,
    rack,
    destroy: async () => {},
  })

  pool.createBroker = ({ connectionPool, nodeId }) => {
    let connected = false
    const failure = () => control.findCoordinatorFailure
    const answer = async (api, request, response) => {
      if (!connected) {
        throw new KafkaJSConnectionError(`${api} sent to a broker that is not connected`)
      }

      requests.push({ api, address: addressOf(connectionPool), ...request })
      return response()
    }

    const broker = {
      nodeId,
      connectionPool,
      isConnected: () => connected,
      connect: async () => {
        connected = true
      },
      disconnect: jest.fn(async () => {
        connected = false
      }),
      metadata: (topics = []) => answer('Metadata', { topics }, () => answerMetadata(topics)),
      findGroupCoordinator: ({ groupId, coordinatorType }) =>
        answer('FindCoordinator', { groupId, coordinatorType }, () => {
          if (failure() && failure().times > 0) {
            failure().times -= 1
            throw failure().error
          }

          const { nodeId, host, port } = control.coordinator
          return { coordinator: { nodeId, host, port } }
        }),
      initProducerId: ({ transactionalId }) =>
        answer('InitProducerId', { transactionalId }, () => ({
          producerId: '1000',
          producerEpoch: 0,
        })),
    }
    created.push(broker)
    return broker
  }

  return { cluster, requests, created, control }
}

describe('Producer > connect while an unrelated partition has no leader', () => {
  let cluster, requests, control, producer

  beforeEach(() => {
    ;({ cluster, requests, control } = createCluster())
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    producer = null
    await cluster.disconnect()
  })

  test('the all-topics Metadata of this cluster fails, as a real one does', async () => {
    await cluster.connect()
    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      cause: expect.objectContaining({ type: 'LEADER_NOT_AVAILABLE' }),
    })
  })

  test.each([
    ['the default producer', {}],
    ['an explicitly idempotent producer', { idempotent: true }],
  ])(
    '%s connects: InitProducerId goes to the connected seed, with no Metadata',
    async (_, options) => {
      producer = createProducer({ cluster, logger: newLogger(), retry: RETRY, ...options })

      await expect(producer.connect()).resolves.toBeUndefined()

      expect(producer.isIdempotent()).toBe(true)
      expect(requests).toEqual([
        { api: 'InitProducerId', address: addressOf(SEED), transactionalId: null },
      ])
    }
  )

  test('a transactional producer connects, and its transaction initializes at the coordinator', async () => {
    producer = createProducer({
      cluster,
      logger: newLogger(),
      retry: RETRY,
      transactionalId: 'txn-a',
    })

    await expect(producer.connect()).resolves.toBeUndefined()
    const transaction = await producer.transaction()
    await transaction.abort()

    const coordinatorLookup = {
      api: 'FindCoordinator',
      groupId: 'txn-a',
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    }
    expect(requests).toEqual([
      { api: 'InitProducerId', address: addressOf(SEED), transactionalId: null },
      { ...coordinatorLookup, address: addressOf(SEED) },
      { api: 'InitProducerId', address: addressOf(COORDINATOR), transactionalId: 'txn-a' },
      { ...coordinatorLookup, address: addressOf(COORDINATOR) },
    ])
  })

  test('InitProducerId goes to a connected broker, not to the controller the metadata names', async () => {
    await cluster.connect()
    await cluster.addTargetTopic('healthy')
    const { brokers } = cluster.brokerPool
    await brokers[2].connect()
    await brokers[SEED.nodeId].disconnect()
    requests.length = 0

    producer = createProducer({ cluster, logger: newLogger(), retry: RETRY })
    await producer.connect()

    expect(requests).toEqual([
      { api: 'InitProducerId', address: addressOf(NODES[1]), transactionalId: null },
    ])
  })

  test('a coordinator already in the broker table is reached through its cached broker', async () => {
    await cluster.connect()
    await cluster.addTargetTopic('healthy')
    const cached = cluster.brokerPool.brokers[COORDINATOR.nodeId]

    const coordinator = await cluster.findGroupCoordinator({ groupId: 'group-a' })

    expect(coordinator).toBe(cached)
    expect(coordinator.connectionPool.rack).toEqual(COORDINATOR.rack)
  })

  test.each([
    ['before the first Metadata response, from the seed', async () => {}],
    ['from a cached broker', () => cluster.addTargetTopic('healthy')],
  ])(
    'a FindCoordinator failure %s surfaces as KafkaJSGroupCoordinatorNotFound',
    async (_, loadMetadata) => {
      await cluster.connect()
      await loadMetadata()
      control.findCoordinatorFailure = {
        error: createErrorFromCode(TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        times: Infinity,
      }

      await expect(
        cluster.findGroupCoordinator({
          groupId: 'txn-a',
          coordinatorType: COORDINATOR_TYPES.TRANSACTION,
        })
      ).rejects.toMatchObject({ name: 'KafkaJSGroupCoordinatorNotFound' })
    }
  )
})

describe('Cluster > findGroupCoordinator while the broker table is incomplete', () => {
  let cluster, requests, created, control

  beforeEach(() => {
    ;({ cluster, requests, created, control } = createCluster())
  })

  afterEach(async () => {
    await cluster.disconnect()
  })

  const findTransactionCoordinator = () =>
    cluster.findGroupCoordinator({
      groupId: 'txn-a',
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })

  /** @returns {string[]} the address of every connected broker object, one entry per object */
  const connectedAddresses = () =>
    created
      .filter((broker) => broker.isConnected())
      .map((broker) => addressOf(broker.connectionPool))

  test('a coordinator added by address keeps its object when a refresh names its rack', async () => {
    await cluster.connect()
    const coordinator = await findTransactionCoordinator()
    expect(coordinator.connectionPool.rack).toBeNull()

    await cluster.addTargetTopic('healthy')

    expect(cluster.brokerPool.brokers[COORDINATOR.nodeId]).toBe(coordinator)
    expect(coordinator.connectionPool.rack).toEqual(COORDINATOR.rack)
    expect(coordinator.disconnect).not.toHaveBeenCalled()

    await cluster.disconnect()
    expect(coordinator.disconnect).toHaveBeenCalledTimes(1)
  })

  test('a coordinator added by address keeps its object when a leader hint names its rack', async () => {
    await cluster.connect()
    const coordinator = await findTransactionCoordinator()

    await cluster.brokerPool.applyNodeEndpoints([COORDINATOR])

    expect(cluster.brokerPool.brokers[COORDINATOR.nodeId]).toBe(coordinator)
    expect(coordinator.connectionPool.rack).toEqual(COORDINATOR.rack)
    expect(coordinator.disconnect).not.toHaveBeenCalled()
  })

  test("a coordinator at the seed broker's address is the seed broker: one connection per address", async () => {
    control.coordinator = SEED
    await cluster.connect()

    const coordinator = await findTransactionCoordinator()

    expect(coordinator).toBe(cluster.brokerPool.seedBroker)
    expect(coordinator.nodeId).toEqual(SEED.nodeId)
    expect(cluster.brokerPool.brokers[SEED.nodeId]).toBe(coordinator)
    expect(connectedAddresses()).toEqual([addressOf(SEED)])

    await cluster.addTargetTopic('healthy')
    await Promise.all(Object.values(cluster.brokerPool.brokers).map((broker) => broker.connect()))

    const addresses = connectedAddresses()
    expect(addresses).toHaveLength(NODES.length)
    expect(new Set(addresses).size).toEqual(NODES.length)
  })

  test("a hint for an unknown node at the adopted seed's address does not adopt the seed again", async () => {
    await cluster.connect()
    await cluster.addTargetTopic('healthy')
    const { brokers, seedBroker } = cluster.brokerPool
    expect(brokers[SEED.nodeId]).toBe(seedBroker)

    await cluster.brokerPool.applyNodeEndpoints([{ ...SEED, nodeId: 4 }])

    const stray = cluster.brokerPool.brokers[4]
    expect(stray).not.toBe(seedBroker)
    expect(seedBroker.nodeId).toEqual(SEED.nodeId)

    await cluster.refreshMetadata()

    expect(cluster.brokerPool.brokers[4]).toBeUndefined()
    expect(stray.disconnect).toHaveBeenCalledTimes(1)
    expect(seedBroker.disconnect).not.toHaveBeenCalled()
    expect(seedBroker.isConnected()).toBe(true)
    expect(seedBroker.nodeId).toEqual(SEED.nodeId)
  })

  test.each([
    ['a connection error', () => new KafkaJSConnectionError('Connection closed')],
    ['a request timeout', () => new KafkaJSRequestTimeoutError('Request timed out')],
  ])('%s on the seed broker before the first Metadata response is retried', async (_, error) => {
    await cluster.connect()
    control.findCoordinatorFailure = { error: error(), times: 1 }

    const coordinator = await findTransactionCoordinator()

    expect(addressOf(coordinator.connectionPool)).toEqual(addressOf(COORDINATOR))
    expect(control.findCoordinatorFailure.times).toEqual(0)
  })

  test.each([
    ['with no cluster.connect()', async () => {}],
    [
      'after a rebootstrap',
      async () => {
        await cluster.connect()
        await cluster.brokerPool.rebootstrap('required')
      },
    ],
  ])('the seed broker is connected before FindCoordinator, %s', async (_, prepare) => {
    await prepare()
    requests.length = 0

    const coordinator = await findTransactionCoordinator()

    expect(cluster.brokerPool.seedBroker.isConnected()).toBe(true)
    expect(addressOf(coordinator.connectionPool)).toEqual(addressOf(COORDINATOR))
    expect(requests).toEqual([
      {
        api: 'FindCoordinator',
        address: addressOf(SEED),
        groupId: 'txn-a',
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      },
    ])
  })
})
