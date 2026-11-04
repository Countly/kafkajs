const Cluster = require('../../cluster')
const { createCluster } = require('testHelpers')
const { RoutingChanges, MAX_CHANGES } = require('./fixtures/routingChanges')

const TOPIC = 'moved-topic'
const PARTITIONS = [{ label: 'moved', topic: TOPIC, partition: 0 }]

/**
 * @param {number} leader
 * @param {number} epoch
 * @returns {object} a decoded Metadata answer naming one partition
 */
const answer = (leader, epoch) => ({
  brokers: [{ nodeId: 1 }, { nodeId: 2 }],
  topicMetadata: [
    {
      topic: TOPIC,
      topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      partitionMetadata: [{ partitionId: 0, leader, leaderEpoch: epoch, offlineReplicas: [] }],
    },
  ],
})

/**
 * Installs an answer the way the broker pool does: it becomes the routing, then goes through the cache.
 * @param {import('../../cluster')} cluster
 * @param {object} metadata
 * @returns {void}
 */
const install = (cluster, metadata) => {
  cluster.brokerPool.metadata = metadata
  cluster.metadataCache.update(metadata)
}

describe('Consumer > routing change observation', () => {
  let observer
  let at

  beforeEach(() => {
    at = 0
    observer = new RoutingChanges(PARTITIONS, () => (at += 10))
    observer.attach()
  })

  afterEach(() => observer.detach())

  it('records the time, the cached leader and epoch and the routed leader of each change', () => {
    const cluster = createCluster({}, ['localhost:9092'])
    cluster.onRoutingChange(() => {})

    install(cluster, answer(1, 5))
    install(cluster, answer(2, 4))

    const held = { label: 'moved', leader: 1, epoch: 5, routed: 1 }
    expect(observer.since(0)).toEqual({
      changes: [
        { at: 10, partitions: [held] },
        { at: 20, partitions: [held] },
      ],
      dropped: 0,
    })
    expect(observer.since(20).changes).toHaveLength(1)
  })

  it('records no leader and no route for a partition the cluster knows nothing of', () => {
    const cluster = createCluster({}, ['localhost:9092'])
    cluster.onRoutingChange(() => {})

    cluster.metadataCache.clear()

    expect(observer.changes).toEqual([
      { at: 10, partitions: [{ label: 'moved', leader: -1, epoch: -1, routed: null }] },
    ])
  })

  it('watches a cluster once however often it subscribes', () => {
    const cluster = createCluster({}, ['localhost:9092'])
    cluster.onRoutingChange(() => {})
    cluster.onRoutingChange(() => {})

    install(cluster, answer(1, 5))

    expect(observer.changes).toHaveLength(1)
  })

  it('keeps a bounded number of changes and counts the rest', () => {
    const cluster = createCluster({}, ['localhost:9092'])
    cluster.onRoutingChange(() => {})

    for (let epoch = 1; epoch <= MAX_CHANGES + 3; epoch++) install(cluster, answer(1, epoch))

    expect(observer.changes).toHaveLength(MAX_CHANGES)
    expect(observer.since(0).dropped).toBe(3)
  })

  it('stops watching and gives the cluster its own subscription back on detach', () => {
    const cluster = createCluster({}, ['localhost:9092'])
    cluster.onRoutingChange(() => {})
    expect(jest.isMockFunction(Cluster.prototype.onRoutingChange)).toBe(true)

    observer.detach()
    install(cluster, answer(1, 5))

    expect(observer.changes).toEqual([])
    expect(jest.isMockFunction(Cluster.prototype.onRoutingChange)).toBe(false)
  })
})
