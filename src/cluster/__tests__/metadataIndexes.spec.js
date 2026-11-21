const Cluster = require('../index')
const { KafkaJSError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger, connectionOpts } = require('testHelpers')

const answer = (names, partitions = 1) => ({
  throttleTime: 0,
  brokers: [{ nodeId: 1, host: '127.0.0.1', port: 9092, rack: null }],
  clusterId: 'c',
  controllerId: 1,
  topicMetadata: names.map((topic) => ({
    topicErrorCode: 0,
    topic,
    topicId: null,
    isInternal: false,
    partitionMetadata: Array.from({ length: partitions }, (_, partitionId) => ({
      partitionErrorCode: 0,
      partitionId,
      leader: 1,
      leaderEpoch: 0,
      replicas: [1],
      isr: [1],
      offlineReplicas: [],
    })),
  })),
})

/** Answers `metadata()` from a queue through a stub broker; the rest is the real code. */
const stubBrokers = (cluster, responses) => {
  const broker = {
    nodeId: 1,
    isConnected: () => true,
    connect: async () => {},
    disconnect: async () => {},
    metadata: async () => responses.shift(),
    connectionPool: { host: '127.0.0.1', port: 9092, rack: null },
  }
  const pool = cluster.brokerPool
  pool.seedBroker = broker
  pool.findConnectedBroker = async () => broker
  pool.buildConnectionPool = async ({ host, port, rack }) => ({ host, port, rack })
  pool.createBroker = ({ nodeId, connectionPool }) => ({ ...broker, nodeId, connectionPool })
  return broker
}

const newCluster = (options = {}) =>
  new Cluster({ ...connectionOpts(), logger: newLogger(), metadataMaxAge: 300000, ...options })

/** Whether a warm pool would refresh for these topics; the refresh itself is not sent. */
const wouldRefresh = async (pool, topics) => {
  const original = pool.refreshMetadata
  let refreshed = false
  pool.refreshMetadata = async () => {
    refreshed = true
  }
  try {
    await pool.refreshMetadataIfNecessary(topics)
  } finally {
    pool.refreshMetadata = original
  }
  return refreshed
}

/** The freshness predicate as it was before the name index: a scan per topic. */
const oldCovers = (pool, topics) =>
  topics.every(
    (topic) =>
      (pool.unavailableTopics?.has(topic) && Date.now() <= pool.isolatedRecheckAt) ||
      pool.metadata.topicMetadata.some((entry) => entry.topic === topic)
  )

/** `findLeaderForPartitions` as it was before the partition index. */
const oldFindLeaderForPartitions = (partitionMetadata, partitions) =>
  partitions.reduce((result, id) => {
    const partitionId = parseInt(id, 10)
    const metadata = partitionMetadata.find((p) => p.partitionId === partitionId)
    if (!metadata) return result
    if (metadata.leader === null || metadata.leader === undefined) {
      throw new KafkaJSError('Invalid partition metadata')
    }
    if (metadata.leader < 0) throw createErrorFromCode(5)
    const { leader } = metadata
    const current = result[leader] || []
    return { ...result, [leader]: [...current, partitionId] }
  }, {})

const outcome = (run) => {
  try {
    return { value: run() }
  } catch (error) {
    return { error: error.constructor.name }
  }
}

/** Seeded generator, so a failing shape can be replayed. */
const random = (seed) => () => {
  seed = (seed * 1103515245 + 12345) % 2147483648
  return seed / 2147483648
}

describe('Cluster > metadata indexes', () => {
  let cluster

  afterEach(async () => {
    cluster = null
  })

  describe('the freshness check', () => {
    test('decides as the per-topic scan did for generated answers and targets', async () => {
      const next = random(7)
      for (const size of [0, 1, 10, 100, 1000]) {
        for (let shape = 0; shape < 5; shape++) {
          cluster = newCluster({ onTopicMetadataStatus: () => {} })
          const pool = cluster.brokerPool
          const names = Array.from({ length: size }, (_, i) => `topic-${i}`)
          const rows = names.filter(() => next() < 0.8)
          if (rows.length && next() < 0.5) rows.push(rows[0])
          pool.metadata = answer(rows)
          pool.metadataExpireAt = Date.now() + 100000
          pool.isolatedRecheckAt = Date.now() + (next() < 0.5 ? 100000 : -1)
          for (const name of names) if (next() < 0.1) pool.unavailableTopics.add(name)
          const targets = [...names.filter(() => next() < 0.6), 'absent', '__proto__']
          for (const topics of [targets, targets.slice(0, -2), names.slice(0, 3), []]) {
            expect(await wouldRefresh(pool, topics)).toBe(!oldCovers(pool, topics))
          }
        }
      }
    })

    test('follows a plain refresh that replaces the answer', async () => {
      cluster = newCluster()
      const pool = cluster.brokerPool
      stubBrokers(cluster, [answer(['a']), answer(['b'])])
      await pool.refreshMetadata(['a'])
      expect(await wouldRefresh(pool, ['a'])).toBe(false)
      expect(await wouldRefresh(pool, ['b'])).toBe(true)

      await pool.refreshMetadata(['b'])
      expect(await wouldRefresh(pool, ['b'])).toBe(false)
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
      expect(cluster.findTopicPartitionMetadata('a')).toEqual([])
      expect(cluster.findTopicPartitionMetadata('b')).toHaveLength(1)
    })

    test('follows a producer refresh that merges the accepted topics into the retained answer', async () => {
      cluster = newCluster({ metadataMaxIdle: 300000 })
      const pool = cluster.brokerPool
      stubBrokers(cluster, [answer(['a']), answer(['b'])])
      await cluster.addTargetTopic('a')
      expect(await wouldRefresh(pool, ['a'])).toBe(false)
      expect(await wouldRefresh(pool, ['a', 'b'])).toBe(true)

      await cluster.addTargetTopic('b')
      expect(await wouldRefresh(pool, ['a', 'b'])).toBe(false)
      expect(cluster.findTopicPartitionMetadata('b')).toHaveLength(1)
    })

    test('follows a consumer refresh that isolates an established topic', async () => {
      const statuses = []
      cluster = newCluster({ onTopicMetadataStatus: (status) => statuses.push(status) })
      const pool = cluster.brokerPool
      const gone = answer(['a', 'b'])
      gone.topicMetadata[1].topicErrorCode = 3
      stubBrokers(cluster, [answer(['a', 'b']), gone])
      await cluster.addMultipleTargetTopics(['a', 'b'])
      expect(await wouldRefresh(pool, ['a', 'b'])).toBe(false)

      await pool.refreshMetadata(['a', 'b'])
      expect(statuses).toHaveLength(1)
      expect(cluster.findTopicPartitionMetadata('b')).toEqual([])
      pool.isolatedRecheckAt = Date.now() - 1
      expect(await wouldRefresh(pool, ['b'])).toBe(true)
      expect(await wouldRefresh(pool, ['a'])).toBe(false)
    })

    test('follows a forgotten topic', async () => {
      cluster = newCluster()
      const pool = cluster.brokerPool
      pool.metadata = answer(['a', 'b'])
      pool.metadataExpireAt = Date.now() + 100000
      expect(await wouldRefresh(pool, ['a', 'b'])).toBe(false)

      pool.forgetTopic('b')
      expect(await wouldRefresh(pool, ['b'])).toBe(true)
      expect(await wouldRefresh(pool, ['a'])).toBe(false)
      expect(cluster.findTopicPartitionMetadata('b')).toEqual([])
    })

    test('follows a rebootstrap that drops the answer', async () => {
      cluster = newCluster()
      const pool = cluster.brokerPool
      const seed = stubBrokers(cluster, [answer(['b'])])
      pool.metadata = answer(['a'])
      pool.metadataExpireAt = Date.now() + 100000
      expect(await wouldRefresh(pool, ['a'])).toBe(false)
      const fresh = { ...seed }
      pool.createSeedBroker = async () => {
        pool.seedBroker = fresh
        return fresh
      }
      pool.closeObsolete = () => {}

      await pool.rebootstrap('required')
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
      await pool.refreshMetadata(['b'])
      expect(await wouldRefresh(pool, ['b'])).toBe(false)
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
    })

    test('follows a disconnect that drops the answer', async () => {
      cluster = newCluster()
      const pool = cluster.brokerPool
      pool.metadata = answer(['a'])
      pool.metadataExpireAt = Date.now() + 100000
      expect(await wouldRefresh(pool, ['a'])).toBe(false)

      await pool.disconnect({ drainTimeout: 0 })
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
      pool.metadata = answer(['b'])
      pool.metadataExpireAt = Date.now() + 100000
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
      expect(await wouldRefresh(pool, ['b'])).toBe(false)
    })

    test('follows an answer assigned directly', async () => {
      cluster = newCluster()
      const pool = cluster.brokerPool
      pool.metadata = answer(['a'])
      pool.metadataExpireAt = Date.now() + 100000
      expect(await wouldRefresh(pool, ['a'])).toBe(false)

      pool.metadata = { ...pool.metadata, topicMetadata: answer(['b']).topicMetadata }
      expect(await wouldRefresh(pool, ['a'])).toBe(true)
      expect(await wouldRefresh(pool, ['b'])).toBe(false)
    })

    test('reads each row name a bounded number of times however often a warm client checks', async () => {
      const reads = async (size) => {
        cluster = newCluster({ metadataMaxIdle: 300000 })
        const pool = cluster.brokerPool
        const names = Array.from({ length: size }, (_, i) => `topic-${i}`)
        let count = 0
        pool.metadata = {
          brokers: [],
          topicMetadata: names.map((name) => ({
            get topic() {
              count++
              return name
            },
            partitionMetadata: [],
          })),
        }
        pool.metadataExpireAt = Date.now() + 100000
        for (const name of names) {
          cluster.targetTopics.add(name)
          cluster.touchTargetTopic(name)
        }
        pool.refreshMetadata = jest.fn()
        for (let i = 0; i < 20; i++) {
          await cluster.addMultipleTargetTopics([names[size - 1]])
          await cluster.refreshMetadataIfNecessary()
        }
        expect(pool.refreshMetadata).not.toHaveBeenCalled()
        return count
      }

      expect(await reads(100)).toBeLessThanOrEqual(2 * 100)
      expect(await reads(2000)).toBeLessThanOrEqual(2 * 2000)
    })
  })

  describe('findLeaderForPartitions', () => {
    const install = (partitionMetadata) => {
      cluster.brokerPool.metadata = {
        brokers: [],
        topicMetadata: [{ topic: 't', partitionMetadata }],
      }
    }

    beforeEach(() => {
      cluster = newCluster()
    })

    test('groups as the per-partition search did for generated partitions and requests', () => {
      const next = random(11)
      for (const size of [0, 1, 10, 50, 1000]) {
        for (let shape = 0; shape < 10; shape++) {
          const partitionMetadata = Array.from({ length: size }, (_, partitionId) => ({
            partitionId,
            leader: Math.floor(next() * 5),
          }))
          for (let i = partitionMetadata.length - 1; i > 0; i--) {
            const j = Math.floor(next() * (i + 1))
            ;[partitionMetadata[i], partitionMetadata[j]] = [
              partitionMetadata[j],
              partitionMetadata[i],
            ]
          }
          if (size && next() < 0.3) partitionMetadata.push({ partitionId: 0, leader: 9 })
          if (size && shape === 8) partitionMetadata[0].leader = -1
          if (size && shape === 9) partitionMetadata[0].leader = null
          install(partitionMetadata)
          const requested = Array.from({ length: Math.floor(next() * (size + 3)) }, () => {
            const id = Math.floor(next() * (size + 2))
            return next() < 0.2 ? String(id) : id
          })
          for (const partitions of [requested, requested.slice(0, 1), []]) {
            expect(outcome(() => cluster.findLeaderForPartitions('t', partitions))).toEqual(
              outcome(() => oldFindLeaderForPartitions(partitionMetadata, partitions))
            )
          }
        }
      }
    })

    test('follows a leader written onto the installed partition and a new answer', () => {
      install([
        { partitionId: 0, leader: 1 },
        { partitionId: 1, leader: 2 },
      ])
      expect(cluster.findLeaderForPartitions('t', [0, 1])).toEqual({ 1: [0], 2: [1] })

      cluster.brokerPool.metadata.topicMetadata[0].partitionMetadata[0].leader = 2
      expect(cluster.findLeaderForPartitions('t', [0, 1])).toEqual({ 2: [0, 1] })

      install([
        { partitionId: 0, leader: 3 },
        { partitionId: 2, leader: 3 },
      ])
      expect(cluster.findLeaderForPartitions('t', [0, 1, 2])).toEqual({ 3: [0, 2] })
    })

    test('reads each partition id a bounded number of times however often a route is looked up', () => {
      const reads = (size) => {
        let count = 0
        install(
          Array.from({ length: size }, (_, id) => ({
            get partitionId() {
              count++
              return id
            },
            leader: id % 3,
          }))
        )
        const all = Array.from({ length: size }, (_, id) => id)
        for (let i = 0; i < 20; i++) cluster.findLeaderForPartitions('t', all)
        return count
      }

      expect(reads(100)).toBeLessThanOrEqual(2 * 100)
      expect(reads(2000)).toBeLessThanOrEqual(2 * 2000)
    })
  })
})
