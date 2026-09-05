const Cluster = require('../index')
const BrokerPool = require('../brokerPool')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const consumerEvents = require('../../consumer/instrumentationEvents')
const producerEvents = require('../../producer/instrumentationEvents')
const adminEvents = require('../../admin/instrumentationEvents')
const { newLogger, connectionOpts } = require('testHelpers')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

const metadata = ({ topicId, leader, leaderEpoch }) => ({
  throttleTime: 0,
  clientSideThrottleTime: 0,
  brokers: [
    { nodeId: 1, host: '127.0.0.1', port: 9092, rack: null },
    { nodeId: 2, host: '127.0.0.1', port: 9095, rack: null },
  ],
  clusterId: 'c',
  controllerId: 1,
  topicMetadata: [
    {
      topicErrorCode: 0,
      topic: 'test-topic',
      topicId,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitionMetadata: [
        {
          partitionErrorCode: 0,
          partitionId: 0,
          leader,
          leaderEpoch,
          replicas: [1, 2],
          isr: [1, 2],
          offlineReplicas: [],
        },
      ],
    },
  ],
})

/**
 * Replaces the pool's broker discovery with a stub that answers `metadata()` from a queue, and
 * makes every broker look connected. Everything else in Cluster/BrokerPool is the real code.
 */
const stubBrokers = (cluster, responses) => {
  const broker = {
    nodeId: 1,
    allowAutoTopicCreation: true,
    isConnected: () => true,
    connect: async () => {},
    disconnect: async () => {},
    metadata: async () => responses.shift(),
    connectionPool: { host: '127.0.0.1', port: 9092, rack: null },
  }
  cluster.brokerPool.seedBroker = broker
  cluster.brokerPool.findConnectedBroker = async () => broker
  cluster.brokerPool.buildConnectionPool = async ({ host, port, rack }) => ({ host, port, rack })
  cluster.brokerPool.createBroker = ({ nodeId, connectionPool }) => ({
    ...broker,
    nodeId,
    connectionPool,
  })
  return broker
}

describe('Cluster > metadata cache wiring', () => {
  let cluster
  let emitter
  let events

  beforeEach(() => {
    emitter = new InstrumentationEventEmitter()
    events = []
    emitter.addListener(CLUSTER_TOPIC_RECREATED, (event) => events.push(event.payload))
    cluster = new Cluster({
      ...connectionOpts(),
      logger: newLogger(),
      instrumentationEmitter: emitter,
    })
  })

  test('the cache is constructed by Cluster and injected into BrokerPool', () => {
    expect(cluster.metadataCache).toBeTruthy()
    expect(cluster.brokerPool.metadataCache).toBe(cluster.metadataCache)
    expect(cluster.instrumentationEmitter).toBe(emitter)
  })

  test('a refresh feeds the cache, and a hint changes what findLeaderForPartitions returns', async () => {
    stubBrokers(cluster, [metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 })])
    await cluster.brokerPool.refreshMetadata(['test-topic'])

    expect(cluster.topicId('test-topic')).toEqual(ID_A)
    expect(cluster.metadataCache.leaderEpoch('test-topic', 0)).toEqual(5)
    expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 1: [0] })

    expect(
      cluster.metadataCache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 6 })
    ).toBe(true)
    expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 2: [0] })
  })

  test('a stale refresh racing an accepted hint does not send routing back to the old leader', async () => {
    stubBrokers(cluster, [
      metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 }),
      metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 }),
    ])
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    cluster.metadataCache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 6 })

    await cluster.brokerPool.refreshMetadata(['test-topic'])
    expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 2: [0] })
  })

  test('a recreation seen by a refresh emits TOPIC_RECREATED once', async () => {
    stubBrokers(cluster, [
      metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 }),
      metadata({ topicId: ID_B, leader: 1, leaderEpoch: 0 }),
    ])
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    await cluster.brokerPool.refreshMetadata(['test-topic'])

    expect(events).toEqual([{ topic: 'test-topic', previousTopicId: ID_A, topicId: ID_B }])
  })

  test('onTopicRecreated subscribes and its return value unsubscribes', async () => {
    const seen = []
    const unsubscribe = cluster.onTopicRecreated((event) => seen.push(event.payload.topic))

    stubBrokers(cluster, [
      metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 }),
      metadata({ topicId: ID_B, leader: 1, leaderEpoch: 0 }),
      metadata({ topicId: ID_A, leader: 1, leaderEpoch: 0 }),
    ])
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    expect(seen).toEqual(['test-topic'])

    unsubscribe()
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    expect(seen).toEqual(['test-topic'])
    // The beforeEach listener is a second, independent subscriber on the same emitter/event; it
    // must still have fired for both recreations (before and after unsubscribe), proving
    // unsubscribe removed only the caller's own listener.
    expect(events).toHaveLength(2)
  })

  test('cache.update runs before the awaited broker rebuild that follows the assignment', async () => {
    // The guard must be applied while nothing else has had a chance to read brokerPool.metadata.
    const responses = [metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 })]
    stubBrokers(cluster, responses)

    const order = []
    const update = cluster.metadataCache.update.bind(cluster.metadataCache)
    cluster.metadataCache.update = (m) => {
      order.push('update')
      return update(m)
    }
    const build = cluster.brokerPool.buildConnectionPool.bind(cluster.brokerPool)
    cluster.brokerPool.buildConnectionPool = async (d) => {
      order.push('buildConnectionPool')
      return build(d)
    }

    await cluster.brokerPool.refreshMetadata(['test-topic'])
    expect(order).toEqual(['update', 'buildConnectionPool'])
  })

  test('update() runs with no await between it and the metadata assignment', () => {
    // Source-level proof, not a timing race: `order[0] === 'update'` above only shows update()
    // runs before buildConnectionPool, which is reached through a *later* await in the same
    // function - any await inserted directly after the assignment would still pass that test.
    // This asserts the no-intervening-await guarantee directly, by inspecting the method body
    // between the two statements for a bare `await`.
    const source = BrokerPool.prototype.refreshMetadata.toString()
    const assignmentText = 'this.metadata = await broker.metadata(topics)'
    const assignmentIndex = source.indexOf(assignmentText)
    const updateIndex = source.indexOf('this.metadataCache.update(', assignmentIndex)

    expect(assignmentIndex).toBeGreaterThan(-1)
    expect(updateIndex).toBeGreaterThan(assignmentIndex)

    // Strip line comments first: the code itself explains the ordering in a `//` comment that
    // legitimately contains the word "await", which would otherwise false-positive this check.
    const between = source
      .slice(assignmentIndex + assignmentText.length, updateIndex)
      .replace(/\/\/[^\n]*/g, '')
    expect(between).not.toMatch(/\bawait\b/)
  })

  test('fetchTopicsOffset fills currentLeaderEpoch from the cache', async () => {
    stubBrokers(cluster, [metadata({ topicId: ID_A, leader: 1, leaderEpoch: 5 })])
    await cluster.brokerPool.refreshMetadata(['test-topic'])

    const requests = []
    cluster.findBroker = async () => ({
      listOffsets: async (request) => {
        requests.push(request)
        return { responses: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '0' }] }] }
      },
    })

    await cluster.fetchTopicsOffset([
      { topic: 'test-topic', partitions: [{ partition: 0 }], fromBeginning: true },
    ])

    expect(requests[0].topics[0].partitions[0]).toMatchObject({
      partition: 0,
      currentLeaderEpoch: 5,
    })
  })

  test('fetchTopicsOffset sends -1 when the metadata carried an unknown leader epoch', async () => {
    // The only reachable "unknown" here: a partition missing from the metadata is dropped by
    // findLeaderForPartitions (cluster/index.js:325-328) and never reaches addDefaultOffset, and
    // absent metadata throws KafkaJSTopicMetadataNotLoaded before that. So the case is a
    // partition the cluster does advertise whose decoded leaderEpoch is -1.
    stubBrokers(cluster, [metadata({ topicId: ID_A, leader: 1, leaderEpoch: -1 })])
    await cluster.brokerPool.refreshMetadata(['test-topic'])
    expect(cluster.metadataCache.leaderEpoch('test-topic', 0)).toEqual(-1)

    const requests = []
    cluster.findBroker = async () => ({
      listOffsets: async (request) => {
        requests.push(request)
        return { responses: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '0' }] }] }
      },
    })

    await cluster.fetchTopicsOffset([
      { topic: 'test-topic', partitions: [{ partition: 0 }], fromBeginning: true },
    ])

    expect(requests[0].topics[0].partitions[0]).toMatchObject({
      partition: 0,
      currentLeaderEpoch: -1,
    })
  })

  test('onTopicRecreated is a safe no-op unsubscribe when the cluster has no emitter', () => {
    const noEmitterCluster = new Cluster({ ...connectionOpts(), logger: newLogger() })

    expect(noEmitterCluster.instrumentationEmitter).toBeNull()
    const unsubscribe = noEmitterCluster.onTopicRecreated(() => {})
    expect(typeof unsubscribe).toBe('function')
    expect(() => unsubscribe()).not.toThrow()
  })
})

describe('TOPIC_RECREATED > client event mapping', () => {
  test.each([
    ['consumer', consumerEvents, 'consumer.cluster.topic_recreated'],
    ['producer', producerEvents, 'producer.cluster.topic_recreated'],
    ['admin', adminEvents, 'admin.cluster.topic_recreated'],
  ])(
    '%s.events.TOPIC_RECREATED unwraps to the cluster event',
    (_name, { events, unwrap }, expected) => {
      expect(events.TOPIC_RECREATED).toEqual(expected)
      expect(unwrap(events.TOPIC_RECREATED)).toEqual(CLUSTER_TOPIC_RECREATED)
    }
  )
})
