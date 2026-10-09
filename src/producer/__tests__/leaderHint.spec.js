const Cluster = require('../../cluster')
const createSendMessages = require('../sendMessages')
const createRetry = require('../../retry')
const createDefaultPartitioner = require('../partitioners/default')
const {
  decode: decodeProduceV11,
  parse: parseProduceV11Response,
} = require('../../protocol/requests/produce/v11/response')
const v10NotLeaderFixture = require('../../protocol/requests/produce/fixtures/v10_response_not_leader.json')
const { newLogger, connectionOpts } = require('../../../testHelpers')

const BROKERS_METADATA = [
  { nodeId: 1, host: '127.0.0.1', port: 9091, rack: null },
  { nodeId: 2, host: '127.0.0.1', port: 9092, rack: null },
]

/**
 * Builds a decoded Metadata response for one or more topics, using the two-broker list every
 * stub broker in this file advertises (127.0.0.1:9091 / :9092) - the addresses must line up
 * exactly, or `refreshMetadata`'s broker-map rebuild replaces the stub brokers through
 * `createBroker` and the object-identity assertions below stop meaning anything.
 */
const buildMetadataResponse = (topics) => {
  const response = {
    throttleTime: 0,
    brokers: BROKERS_METADATA,
    clusterId: 'test-cluster',
    controllerId: 1,
    topicMetadata: topics.map(({ topic, partitions }) => ({
      topicErrorCode: 0,
      topic,
      topicId: null,
      isInternal: false,
      partitionMetadata: partitions.map(({ partitionId, leader, leaderEpoch }) => ({
        partitionErrorCode: 0,
        partitionId,
        leader,
        leaderEpoch,
        replicas: [1, 2],
        isr: [1, 2],
        offlineReplicas: [],
      })),
    })),
  }
  metadataResponse.last = response
  return response
}

/** One topic ('test-topic'), one partition. Remembers the last response built on `.last`. */
const metadataResponse = ({ leader, leaderEpoch }) =>
  buildMetadataResponse([
    { topic: 'test-topic', partitions: [{ partitionId: 0, leader, leaderEpoch }] },
  ])
metadataResponse.last = null

/**
 * Two topics, each with one partition on a different leader: 'test-topic-2' on node 2 and
 * 'test-topic' on node 1 - the second name matches the committed fixture, so a failure on it
 * carries the fixture's real topic/partition.
 */
const twoLeadersMetadataResponse = () =>
  buildMetadataResponse([
    { topic: 'test-topic-2', partitions: [{ partitionId: 0, leader: 2, leaderEpoch: 5 }] },
    { topic: 'test-topic', partitions: [{ partitionId: 0, leader: 1, leaderEpoch: 5 }] },
  ])

const topicMessages = [{ topic: 'test-topic', messages: [{ key: 'k', value: 'v', partition: 0 }] }]

const topicMessagesTwoLeaders = [
  { topic: 'test-topic-2', messages: [{ key: 'a', value: 'va', partition: 0 }] },
  { topic: 'test-topic', messages: [{ key: 'b', value: 'vb', partition: 0 }] },
]

/**
 * A real `Cluster` with a real `MetadataCache`, primed with the given metadata, with only broker
 * discovery stubbed. `answer` decides what each produce request gets back; it is called with the
 * responding node id, the raw request and a running count of every produce call made so far.
 */
const createHintCluster = ({
  answer,
  metadataRecoveryStrategy,
  initialMetadata = metadataResponse({ leader: 1, leaderEpoch: 5 }),
} = {}) => {
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    metadataRecoveryStrategy,
  })
  const brokers = {}
  const produced = []

  // `metadata` is on every stub broker because sendMessages opens with
  // cluster.refreshMetadataIfNecessary() and its staleMetadata branch calls
  // cluster.refreshMetadata() - both land on BrokerPool#refreshMetadata, which calls
  // broker.metadata(topics). Without it the first attempt throws a TypeError, which the retrier
  // classifies as unrecoverable and rejects outright.
  for (const nodeId of [1, 2]) {
    brokers[nodeId] = {
      nodeId,
      allowAutoTopicCreation: true,
      isConnected: () => true,
      connect: async () => {},
      disconnect: jest.fn(async () => {}),
      connectionPool: {
        host: '127.0.0.1',
        port: 9090 + nodeId,
        rack: null,
        destroy: jest.fn(async () => {}),
      },
      replaceConnectionPool(connectionPool) {
        this.connectionPool = connectionPool
      },
      metadata: async () => metadataResponse.last,
      produce: async (request) => {
        produced.push({ nodeId, request })
        return answer({ nodeId, request, attempt: produced.length })
      },
    }
  }

  cluster.brokerPool.brokers = brokers
  cluster.brokerPool.versions = { 0: { minVersion: 0, maxVersion: 11 } }
  // Non-null so refreshMetadataIfNecessary only refreshes when the target topic is missing;
  // refreshMetadata() from the staleMetadata branch still runs, which is the point - the refresh
  // feeds the same response back through MetadataCache#update, exercising the stale-refresh
  // guard end to end rather than working around it.
  cluster.brokerPool.metadataExpireAt = Date.now() + 60000
  cluster.brokerPool.buildConnectionPool = jest.fn(async (destination) => ({
    ...destination,
    destroy: jest.fn(async () => {}),
  }))
  cluster.brokerPool.createBroker = jest.fn(({ nodeId, connectionPool, versions }) => ({
    nodeId,
    versions,
    connectionPool,
    allowAutoTopicCreation: true,
    isConnected: () => true,
    connect: async () => {},
    disconnect: async () => {},
    metadata: async () => metadataResponse.last,
    produce: async (request) => {
      produced.push({ nodeId, request })
      return answer({ nodeId, request, attempt: produced.length })
    },
  }))

  cluster.metadataCache.update(initialMetadata)
  cluster.brokerPool.metadata = metadataResponse.last

  return { cluster, brokers, produced }
}

/**
 * Decodes the committed v10 "not leader" fixture through the real v11 codec and returns the
 * rejection it throws, with `currentLeader`/`nodeEndpoints` overridden to the case's values (a
 * value left `undefined` keeps whatever the fixture itself carries).
 */
const parseProduceV11 = async (fixture, { leaderId, leaderEpoch, nodeEndpoints } = {}) => {
  const data = await decodeProduceV11(Buffer.from(fixture.data))

  try {
    await parseProduceV11Response(data)
  } catch (e) {
    if (leaderId !== undefined || leaderEpoch !== undefined) {
      e.currentLeader = { leaderId, leaderEpoch }
    }
    if (nodeEndpoints !== undefined) {
      e.nodeEndpoints = nodeEndpoints
    }
    return e
  }

  throw new Error('expected the fixture to decode into a rejection')
}

/** The error a v11 produce response with a leader hint parses into. */
const notLeaderWithHint = ({ leaderId = 2, leaderEpoch = 6, nodeEndpoints } = {}) =>
  parseProduceV11(v10NotLeaderFixture, { leaderId, leaderEpoch, nodeEndpoints })

/** The minimal EosManager surface `sendMessages` calls, for a non-transactional producer. */
const eosManagerStub = ({ updateSequence = jest.fn() } = {}) => ({
  isTransactional: () => false,
  getSequence: () => 0,
  updateSequence,
  getProducerId: () => -1,
  getProducerEpoch: () => -1,
})

const createSend = ({ cluster, eosManager = eosManagerStub(), retrier }) =>
  createSendMessages({
    logger: newLogger(),
    cluster,
    partitioner: createDefaultPartitioner(),
    eosManager,
    retrier: retrier ?? createRetry({ retries: 2, initialRetryTime: 1, maxRetryTime: 2 }),
  })

describe('Producer > KIP-951 leader hint', () => {
  test('an accepted hint changes the leader the next retrier attempt routes to', async () => {
    const { cluster, produced } = createHintCluster({
      answer: async ({ attempt }) => {
        if (attempt === 1) throw await notLeaderWithHint({ leaderId: 2, leaderEpoch: 6 })
        return { topics: [] }
      },
    })

    const send = createSend({ cluster })
    await send({ acks: -1, timeout: 30000, topicMessages })

    expect(produced.map(({ nodeId }) => nodeId)).toEqual([1, 2])
    expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 2: [0] })
  })

  test('a lower-epoch hint is ignored and the attempt routes unchanged', async () => {
    const { cluster, produced } = createHintCluster({
      answer: async ({ attempt }) => {
        if (attempt <= 2) throw await notLeaderWithHint({ leaderId: 2, leaderEpoch: 4 })
        return { topics: [] }
      },
    })

    const send = createSend({ cluster })
    await send({ acks: -1, timeout: 30000, topicMessages })

    expect(produced.map(({ nodeId }) => nodeId)).toEqual([1, 1, 1])
    expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 1: [0] })
  })

  test('applyNodeEndpoints replaces a broker whose host/port/rack moved, and preserves the blackout', async () => {
    const { cluster, brokers } = createHintCluster({ answer: async () => ({ topics: [] }) })
    const pool = cluster.brokerPool
    const before = brokers[2]
    pool.authenticationBlackout.set('127.0.0.1:9092', { until: Date.now() + 60000 })

    await pool.applyNodeEndpoints([{ nodeId: 2, host: '127.0.0.2', port: 9095, rack: 'rack-b' }])

    expect(pool.brokers[2]).not.toBe(before)
    expect(before.disconnect).toHaveBeenCalled()
    expect(pool.createBroker).toHaveBeenCalledWith(
      expect.objectContaining({ nodeId: 2, versions: pool.versions })
    )
    expect(pool.buildConnectionPool).toHaveBeenCalledWith({
      host: '127.0.0.2',
      port: 9095,
      rack: 'rack-b',
    })
    expect(pool.authenticationBlackout.get('127.0.0.1:9092')).toBeTruthy()
  })

  test('an endpoint identical to the cached one replaces nothing', async () => {
    const { cluster, brokers } = createHintCluster({ answer: async () => ({ topics: [] }) })
    const pool = cluster.brokerPool

    await pool.applyNodeEndpoints([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])

    expect(pool.brokers[2]).toBe(brokers[2])
    expect(pool.buildConnectionPool).not.toHaveBeenCalled()
  })

  test('a hint that moves the seed node updates its connection pool in place, keeping the Broker identity', async () => {
    // refreshMetadata can adopt the seed Broker into `this.brokers` under a real node id
    // (brokerPool.js's reduce: `assign(result, { [nodeId]: seedBroker })`); this reproduces that
    // state directly, since the harness's stub brokers never go through a real refresh.
    const { cluster, brokers } = createHintCluster({ answer: async () => ({ topics: [] }) })
    const pool = cluster.brokerPool
    const seed = brokers[1]
    pool.seedBroker = seed
    const oldConnectionPool = seed.connectionPool

    await pool.applyNodeEndpoints([{ nodeId: 1, host: '127.0.0.5', port: 9199, rack: 'rack-c' }])

    // Every other broker-pool method reads `this.seedBroker` directly - the object itself, and
    // its slot in `this.brokers`, must never change.
    expect(pool.brokers[1]).toBe(seed)
    expect(pool.seedBroker).toBe(seed)
    expect(seed.connectionPool).toMatchObject({ host: '127.0.0.5', port: 9199, rack: 'rack-c' })
    expect(oldConnectionPool.destroy).toHaveBeenCalled()
    expect(pool.createBroker).not.toHaveBeenCalled()
  })

  test('applyNodeEndpoints runs before the cache write - no window where routing names a node with no Broker', async () => {
    const { cluster } = createHintCluster({ answer: async () => ({ topics: [] }) })
    const order = []
    const build = cluster.brokerPool.buildConnectionPool
    cluster.brokerPool.buildConnectionPool = async (d) => {
      order.push('buildConnectionPool')
      return build(d)
    }
    const apply = cluster.metadataCache.applyLeaderHint.bind(cluster.metadataCache)
    cluster.metadataCache.applyLeaderHint = (...args) => {
      order.push('applyLeaderHint')
      return apply(...args)
    }

    await cluster.applyLeaderHint({
      topic: 'test-topic',
      partition: 0,
      leaderId: 2,
      leaderEpoch: 6,
      nodeEndpoints: [{ nodeId: 3, host: '127.0.0.3', port: 9098, rack: null }],
    })

    expect(order).toEqual(['buildConnectionPool', 'applyLeaderHint'])
    // Adding an unknown node id is allowed: node 3 was never in the table before this hint.
    expect(cluster.brokerPool.brokers[3]).toBeTruthy()
  })

  test('a hint for a partition the cache does not know is rejected, but only after the endpoint work already ran', async () => {
    // Cluster#applyLeaderHint's pre-check only sees the epoch, not whether the topic/partition is
    // known at all - that check lives inside MetadataCache#applyLeaderHint, which runs *after* the
    // endpoint work. A hint for an unknown partition therefore still pays for `applyNodeEndpoints`
    // even though it is ultimately rejected - the opposite of "a rejected hint costs nothing",
    // true only for the epoch-rejection case.
    const { cluster } = createHintCluster({ answer: async () => ({ topics: [] }) })

    await expect(
      cluster.applyLeaderHint({
        topic: 'unknown-topic',
        partition: 0,
        leaderId: 2,
        leaderEpoch: 6,
        nodeEndpoints: [{ nodeId: 2, host: '127.0.0.9', port: 9099, rack: null }],
      })
    ).resolves.toBe(false)

    expect(cluster.brokerPool.buildConnectionPool).toHaveBeenCalled()
  })

  test.each([[undefined], [[]]])(
    'nodeEndpoints %p is a no-op and the hint still applies',
    async (nodeEndpoints) => {
      const { cluster } = createHintCluster({ answer: async () => ({ topics: [] }) })

      await expect(
        cluster.applyLeaderHint({
          topic: 'test-topic',
          partition: 0,
          leaderId: 2,
          leaderEpoch: 6,
          nodeEndpoints,
        })
      ).resolves.toBe(true)

      expect(cluster.brokerPool.buildConnectionPool).not.toHaveBeenCalled()
      expect(cluster.findLeaderForPartitions('test-topic', [0])).toEqual({ 2: [0] })
    }
  )

  test('a hint with an unknown epoch is not short-circuited by the pre-check', async () => {
    // The two guards must agree at -1: the cache accepts against an unknown cached epoch, so the
    // pre-check must not reject before the endpoint work runs.
    const { cluster } = createHintCluster({ answer: async () => ({ topics: [] }) })
    cluster.metadataCache.update(metadataResponse({ leader: 1, leaderEpoch: -1 }))

    await expect(
      cluster.applyLeaderHint({
        topic: 'test-topic',
        partition: 0,
        leaderId: 2,
        leaderEpoch: -1,
        nodeEndpoints: [{ nodeId: 2, host: '127.0.0.9', port: 9099, rack: null }],
      })
    ).resolves.toBe(true)

    expect(cluster.brokerPool.buildConnectionPool).toHaveBeenCalled()
  })

  test.each([['rebootstrap'], ['none']])(
    'the hint path behaves identically under metadataRecoveryStrategy: %p',
    async (metadataRecoveryStrategy) => {
      const { cluster, produced } = createHintCluster({
        metadataRecoveryStrategy,
        answer: async ({ attempt }) => {
          if (attempt === 1) throw await notLeaderWithHint({ leaderId: 2, leaderEpoch: 6 })
          return { topics: [] }
        },
      })

      const send = createSend({ cluster })
      await send({ acks: -1, timeout: 30000, topicMessages })

      expect(produced.map(({ nodeId }) => nodeId)).toEqual([1, 2])
    }
  )

  test('no change to retry timing or to the per-broker sequence rollback', async () => {
    const run = async ({ withHint }) => {
      const { cluster, produced } = createHintCluster({
        answer: async ({ attempt }) => {
          if (attempt === 1) {
            const error = await notLeaderWithHint({ leaderId: 2, leaderEpoch: 6 })
            if (!withHint) {
              delete error.currentLeader
              delete error.nodeEndpoints
            }
            throw error
          }
          return { topics: [] }
        },
      })

      const updateSequence = jest.fn()
      const send = createSend({ cluster, eosManager: eosManagerStub({ updateSequence }) })
      await send({ acks: -1, timeout: 30000, topicMessages })
      return { updateSequence: updateSequence.mock.calls, produced }
    }

    const withHint = await run({ withHint: true })
    const without = await run({ withHint: false })

    expect(withHint.updateSequence).toEqual(without.updateSequence)
    expect(withHint.produced).toHaveLength(without.produced.length)
  })

  test('a Broker that applyNodeEndpoints replaced is produced to again, and no sequence is touched', async () => {
    // 'test-topic-2' (leader 2) succeeds before 'test-topic' (leader 1) fails with a hint that
    // hands node 2 a new address. Replacing node 2's Broker makes responsePerBroker treat it as
    // unsent, so the next attempt re-produces 'test-topic-2' alongside the legitimate retry of
    // 'test-topic' - a replaced broker is treated as unsent, an accepted widening of the
    // duplicate-on-retry window. This is asserted directly (node 2 is produced to twice) rather
    // than through a derived formula, so a future change cannot widen it further unnoticed.
    const { cluster, produced } = createHintCluster({
      initialMetadata: twoLeadersMetadataResponse(),
      answer: async ({ nodeId, attempt }) => {
        if (nodeId === 1 && attempt === 2) {
          throw await notLeaderWithHint({
            leaderId: 2,
            leaderEpoch: 6,
            nodeEndpoints: [{ nodeId: 2, host: '127.0.0.9', port: 9099, rack: null }],
          })
        }
        return { topics: [] }
      },
    })

    const updateSequence = jest.fn()
    const send = createSend({ cluster, eosManager: eosManagerStub({ updateSequence }) })
    await send({ acks: -1, timeout: 30000, topicMessages: topicMessagesTwoLeaders })

    expect(produced.map(({ nodeId }) => nodeId)).toEqual([2, 1, 2])
    // A non-idempotent request carries no sequence, so a retry has none to give back.
    expect(updateSequence).not.toHaveBeenCalled()
  })

  test('a rejecting applyLeaderHint does not replace the produce error or skip the staleMetadata fall-through', async () => {
    const { cluster, produced } = createHintCluster({
      answer: async ({ attempt }) => {
        if (attempt === 1) throw await notLeaderWithHint({ leaderId: 2, leaderEpoch: 6 })
        return { topics: [] }
      },
    })

    // Simulates any throw inside the hint path (a bad address, a rejecting disconnect, ...):
    // sendMessages' best-effort `.catch()` must swallow it locally, never touching the produce
    // error `e` the outer catch is already handling.
    cluster.applyLeaderHint = jest.fn(async () => {
      throw new Error('applyLeaderHint rejected')
    })
    const refreshMetadataSpy = jest.spyOn(cluster, 'refreshMetadata')

    const send = createSend({ cluster })
    await expect(send({ acks: -1, timeout: 30000, topicMessages })).resolves.toEqual([])

    expect(cluster.applyLeaderHint).toHaveBeenCalled()
    // The staleMetadata fall-through still ran - the rejection did not skip it.
    expect(refreshMetadataSpy).toHaveBeenCalled()
    // The hint was never actually applied, so routing stays on node 1; the retrier's own backoff
    // is what recovers, exactly as it would with no hint at all.
    expect(produced.map(({ nodeId }) => nodeId)).toEqual([1, 1])
  })
})
