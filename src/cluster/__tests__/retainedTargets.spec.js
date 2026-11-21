// Records every cluster a client builds, so a test can drive the cluster of a real producer,
// consumer or admin client.
const mockClusters = []
jest.mock('../index', () => {
  const Cluster = jest.requireActual('../index')
  return class RecordedCluster extends Cluster {
    constructor(options) {
      super(options)
      mockClusters.push(this)
    }
  }
})

const Cluster = require('../index')
const { Kafka } = require('../../../index')
const { protocol: metadataProtocol } = require('../../protocol/requests/metadata')
const { newLogger, connectionOpts } = require('testHelpers')

/** @returns {{ promise: Promise<void>, release: () => void }} a promise released by hand */
const latch = () => {
  let release
  const promise = new Promise((resolve) => (release = resolve))
  return { promise, release }
}

/**
 * Puts one broker double in front of a cluster. Every Metadata request is logged with its topics
 * and answered through the real v13 parse, after the promise `gates` holds for that request
 * number, if any. An empty request is answered with `all`.
 * @param {Cluster} cluster
 * @param {string[]} [all] the topics of the cluster
 * @returns {{ requests: string[][], gates: Map<number, Promise<void>> }} the log and the gates
 */
const front = (cluster, all = ['a', 'b']) => {
  const requests = []
  const gates = new Map()
  const broker = {
    nodeId: 0,
    connectionPool: { host: 'localhost', port: 9092 },
    isConnected: () => true,
    disconnect: async () => {},
    retire: async () => {},
    metadata: async (topics, options = {}) => {
      requests.push([...topics])
      await gates.get(requests.length)
      const named = topics.length > 0 ? topics : all
      return metadataProtocol({ version: 13 })({ topics, ...options }).response.parse({
        errorCode: 0,
        brokers: [{ nodeId: 0, host: 'localhost', port: 9092, rack: null }],
        controllerId: 0,
        topicMetadata: named.map((topic) => ({
          topic,
          topicId: null,
          topicErrorCode: 0,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 0,
              leaderEpoch: 1,
              replicas: [0],
              isr: [0],
              offlineReplicas: [],
            },
          ],
        })),
      })
    },
  }
  cluster.brokerPool.seedBroker = broker
  cluster.brokerPool.brokers = { 0: broker }
  cluster.brokerPool.findConnectedBroker = async () => broker
  return { requests, gates }
}

/** @param {Cluster} cluster @returns {string[]} the topics the installed metadata routes */
const routed = (cluster) => cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)

/**
 * Sends two refreshes straight to the broker pool, the first one answered last: a narrower one
 * observed by `onMetadata`, then a wider one.
 * @param {Cluster} cluster
 * @param {string[]} older the topics of the first request
 * @param {string[]} newer the topics of the second request
 * @returns {Promise<{ observed: string[][], requests: string[][] }>} what `onMetadata` saw
 */
const olderAnswersLast = async (cluster, older, newer) => {
  const { requests, gates } = front(cluster)
  const held = latch()
  gates.set(1, held.promise)
  const observed = []
  const first = cluster.brokerPool.refreshMetadata(older, {
    onMetadata: ({ topicMetadata }) => observed.push(topicMetadata.map(({ topic }) => topic)),
  })
  await new Promise(setImmediate)
  await cluster.brokerPool.refreshMetadata(newer)
  held.release()
  await first
  return { observed, requests }
}

describe('Cluster > a scoped answer keeps the routing of current targets', () => {
  let cluster
  beforeEach(() => {
    mockClusters.length = 0
  })
  afterEach(async () => {
    await cluster?.disconnect()
    cluster = null
  })

  test('with retainTargets, a scoped answer that lands last keeps the routing of every held topic, and drops a topic no call holds even while it is a target', async () => {
    cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retainTargets: true })
    cluster.holdTargetTopics(['a'])
    const releaseB = cluster.holdTargetTopics(['b'])
    cluster.targetTopics.add('a')
    cluster.targetTopics.add('b')

    const { observed, requests } = await olderAnswersLast(cluster, ['a'], ['a', 'b'])

    expect(requests).toEqual([['a'], ['a', 'b']])
    expect(routed(cluster).sort()).toEqual(['a', 'b'])
    expect(observed.map((topics) => topics.sort())).toEqual([['a', 'b']])
    expect(cluster.metadataCache.leader('b', 0)).toBe(0)

    releaseB()
    cluster.targetTopics.add('b')
    await cluster.brokerPool.refreshMetadata(['a'])
    expect(routed(cluster)).toEqual(['a'])
  })

  test('with retainTargets, an answer for every topic keeps the routing of a held topic it lacks, and drops an unheld one', async () => {
    cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retainTargets: true })
    front(cluster, ['b'])
    cluster.holdTargetTopics(['a'])
    cluster.targetTopics.add('a')
    cluster.targetTopics.add('c')
    await cluster.brokerPool.refreshMetadata(['a', 'c'])

    await cluster.brokerPool.refreshMetadata([])

    expect(routed(cluster).sort()).toEqual(['a', 'b'])
  })

  test('without retainTargets, the answer that lands last is installed and observed, as before', async () => {
    cluster = new Cluster({ ...connectionOpts(), logger: newLogger() })
    cluster.targetTopics.add('a')
    cluster.targetTopics.add('b')

    const { observed } = await olderAnswersLast(cluster, ['a'], ['a', 'b'])

    expect(routed(cluster)).toEqual(['a'])
    expect(observed).toEqual([['a']])
  })

  test('a producer keeps installing and observing the answer that lands last', async () => {
    new Kafka({ ...connectionOpts(), logLevel: 0 }).producer()
    ;[cluster] = mockClusters
    front(cluster)
    await cluster.addMultipleTargetTopics(['a', 'b'])

    const { observed } = await olderAnswersLast(cluster, ['a'], ['a', 'b'])

    expect(mockClusters).toHaveLength(1)
    expect(observed.map((topics) => topics.sort())).toEqual([['a', 'b']])
    expect(routed(cluster).sort()).toEqual(['a', 'b'])
  })

  test('a consumer installs the scoped answer that lands last as it is', async () => {
    new Kafka({ ...connectionOpts(), logLevel: 0 }).consumer({ groupId: 'g' })
    ;[cluster] = mockClusters
    cluster.targetTopics.add('a')
    cluster.targetTopics.add('b')

    const { observed } = await olderAnswersLast(cluster, ['a'], ['a', 'b'])

    expect(mockClusters).toHaveLength(1)
    expect(observed).toEqual([['a']])
    expect(routed(cluster)).toEqual(['a'])
  })

  test('a pattern consumer keeps installing and evaluating the answer that lands last', async () => {
    new Kafka({ ...connectionOpts(), logLevel: 0 }).consumer({ groupId: 'g' })
    ;[cluster] = mockClusters

    const { observed } = await olderAnswersLast(cluster, [], ['a'])

    expect(mockClusters).toHaveLength(1)
    expect(observed).toEqual([['a', 'b']])
    expect(routed(cluster)).toEqual(['a', 'b'])
  })

  test('an admin client keeps the routing of its held targets', async () => {
    new Kafka({ ...connectionOpts(), logLevel: 0 }).admin()
    ;[cluster] = mockClusters
    cluster.holdTargetTopics(['a', 'b'])
    cluster.targetTopics.add('a')
    cluster.targetTopics.add('b')

    await olderAnswersLast(cluster, ['a'], ['a', 'b'])

    expect(mockClusters).toHaveLength(1)
    expect(routed(cluster).sort()).toEqual(['a', 'b'])
  })

  describe('joining a refresh while admin calls hold targets', () => {
    test('a refresh begun before a topic was added is not joined by a call that needs that topic', async () => {
      cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retainTargets: true })
      const { requests, gates } = front(cluster)
      cluster.holdTargetTopics(['a'])
      cluster.targetTopics.add('a')
      const held = latch()
      gates.set(1, held.promise)
      const older = cluster.refreshMetadataIfNecessary()
      await new Promise(setImmediate)

      cluster.holdTargetTopics(['b'])
      cluster.targetTopics.add('b')
      await cluster.refreshMetadataIfNecessary()
      held.release()
      await older

      expect(requests).toEqual([['a'], ['a', 'b']])
      expect(routed(cluster).sort()).toEqual(['a', 'b'])
    })

    test('with no hold, a refresh is joined as before', async () => {
      cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retainTargets: true })
      const { requests, gates } = front(cluster)
      cluster.targetTopics.add('a')
      const held = latch()
      gates.set(1, held.promise)
      const older = cluster.refreshMetadata()
      await new Promise(setImmediate)

      cluster.targetTopics.add('b')
      const joined = cluster.refreshMetadata()
      held.release()
      await Promise.all([older, joined])

      expect(requests).toEqual([['a']])
    })
  })
})
