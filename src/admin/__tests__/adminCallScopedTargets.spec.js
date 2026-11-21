const Cluster = require('../../cluster')
const createAdmin = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { requests } = require('../../protocol/requests')
const { createErrorFromCode } = require('../../protocol/error')
const { connectionOpts, newLogger } = require('testHelpers')

const NODES = [1, 2].map((nodeId) => ({ nodeId, host: `kafka${nodeId}`, port: 9092, rack: null }))
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 2 }
const ID = (n) => `0000000${n}-0000-0000-0000-00000000000${n}`

/**
 * One partition of a Metadata answer.
 * @param {number} leader node id @param {number} partitionId partition index
 * @returns {object} the partition row
 */
const part = (leader, partitionId) => ({
  partitionErrorCode: 0,
  partitionId,
  leader,
  leaderEpoch: 3,
  replicas: [leader],
  isr: [leader],
  offlineReplicas: [],
})

/**
 * The admin's cluster, built as `kafka.admin()` builds it, over two broker doubles whose Metadata
 * answers go through the real v13 parse. `live` maps each existing topic to its partition leaders,
 * `ids` to its topic id and `refused` to the topic error code a named request gets for it. A
 * Metadata answer waits for `gate(topics, n)` when one is set; `n` counts the Metadata requests.
 * A ListOffsets request waits for `listHook(n)` when one is set; `n` counts those requests.
 * @param {object} [options] retry and apiTimeout of the admin
 * @returns {object} the cluster, the admin, the controls and the request log
 */
const build = ({ retry = RETRY, apiTimeout } = {}) => {
  const live = new Map([
    ['T', [1, 2]],
    ['U', [2]],
  ])
  const ids = new Map([
    ['T', ID(1)],
    ['U', ID(2)],
  ])
  const refused = new Map()
  const calls = []
  let gate = null
  let sent = 0
  let listHook = null
  let listed = 0
  const emitter = new InstrumentationEventEmitter()
  const recreated = []
  emitter.addListener(CLUSTER_TOPIC_RECREATED, ({ payload }) => recreated.push(payload))
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    retry,
    allowAutoTopicCreation: false,
    instrumentationEmitter: emitter,
    orderedMetadata: true,
  })
  const pool = cluster.brokerPool
  pool.buildConnectionPool = async ({ host = 'kafka1', port = 9092, rack = null } = {}) => ({
    host,
    port,
    rack,
    destroy: async () => {},
  })
  pool.createBroker = ({ connectionPool, nodeId }) => {
    let connected = false
    return {
      nodeId,
      connectionPool,
      isConnected: () => connected,
      connect: async () => {
        connected = true
      },
      disconnect: async () => {
        connected = false
      },
      retire: async () => {
        connected = false
      },
      metadata: async (topics = []) => {
        calls.push({ api: 'Metadata', topics: [...topics].sort() })
        const n = ++sent
        if (gate) await gate(topics, n)
        const named = topics.length > 0 ? topics : [...live.keys()]
        const { response } = requests.Metadata.protocol({ version: 13 })({ topics })
        return response.parse({
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode: 0,
          brokers: NODES,
          clusterId: 'c',
          controllerId: 1,
          topicMetadata: named.map((topic) => ({
            topicErrorCode: refused.get(topic) ?? (live.has(topic) ? 0 : 3),
            topic,
            topicId: live.has(topic) && !refused.has(topic) ? ids.get(topic) : null,
            isInternal: false,
            partitionMetadata: refused.has(topic) ? [] : (live.get(topic) ?? []).map(part),
          })),
        })
      },
      createTopics: async () => calls.push({ api: 'CreateTopics' }),
      deleteTopics: async ({ topics, topicIds }) => {
        calls.push({ api: 'DeleteTopics', topics, topicIds })
        for (const topic of topics ??
          [...ids].filter(([, id]) => topicIds.includes(id)).map(([t]) => t))
          live.delete(topic)
      },
      listOffsets: async ({ topics }) => {
        if (listHook) await listHook(++listed)
        calls.push({ api: 'ListOffsets', nodeId, topics: topics.map(({ topic }) => topic) })
        return {
          responses: topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition }) => ({ partition, offset: '5' })),
          })),
        }
      },
      deleteRecords: async () => {
        calls.push({ api: 'DeleteRecords', targets: [...cluster.targetTopics] })
      },
      describeProducers: async ({ topics }) => {
        calls.push({ api: 'DescribeProducers', targets: [...cluster.targetTopics] })
        return {
          topics: topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map((partition) => ({
              partition,
              errorCode: 0,
              activeProducers: [],
            })),
          })),
        }
      },
      writeTxnMarkers: async ({ markers: [marker] }) => {
        calls.push({ api: 'WriteTxnMarkers', targets: [...cluster.targetTopics] })
        const [{ topic, partitions }] = marker.topics
        return {
          markers: [
            {
              producerId: marker.producerId,
              topics: [{ topic, partitions: [{ partition: partitions[0], errorCode: 0 }] }],
            },
          ],
        }
      },
    }
  }
  const admin = createAdmin({
    cluster,
    logger: newLogger(),
    retry,
    instrumentationEmitter: emitter,
    ...(apiTimeout && { apiTimeout }),
  })
  const md = () => calls.filter((c) => c.api === 'Metadata')
  return {
    cluster,
    admin,
    live,
    ids,
    refused,
    calls,
    md,
    recreated,
    setGate: (g) => (gate = g),
    setListHook: (hook) => (listHook = hook),
  }
}

/** @param {Promise} promise @returns {Promise<{ ok: * }|{ error: Error }>} its outcome */
const outcome = (promise) =>
  promise.then(
    (ok) => ({ ok }),
    (error) => ({ error })
  )
/** @param {number} [ms] @returns {Promise<void>} after `ms` real milliseconds */
const tick = (ms = 5) => new Promise((resolve) => setTimeout(resolve, ms))
/** @returns {{ promise: Promise<void>, release: () => void }} a promise released by hand */
const latch = () => {
  let release
  const promise = new Promise((resolve) => (release = resolve))
  return { promise, release }
}
/**
 * Makes every if-necessary refresh go to the wire, as on a real network where at least a
 * millisecond passes between two requests of the admin (its metadataMaxAge is 0).
 * @param {Cluster} cluster
 */
const alwaysStale = (cluster) => {
  cluster.brokerPool.metadataMaxAge = -1
}
const T_OFFSETS = [
  { partition: 0, offset: '5', high: '5', low: '5' },
  { partition: 1, offset: '5', high: '5', low: '5' },
]
const U_OFFSETS = [{ partition: 0, offset: '5', high: '5', low: '5' }]
const CREATE = (topic) => ({ waitForLeaders: false, topics: [{ topic }] })

describe('Admin > each call targets its topics only while it runs', () => {
  let h
  afterEach(async () => {
    await h?.cluster.disconnect()
    h = null
  })

  describe('a topic deleted by another client', () => {
    test('fails only the calls that name it, and the admin targets nothing after each call', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.fetchTopicOffsets('T')
      expect([...h.cluster.targetTopics]).toEqual([])
      h.live.delete('T')

      const before = h.md().length
      await expect(h.admin.createTopics(CREATE('N'))).resolves.toBe(true)
      await expect(h.admin.fetchTopicOffsets('U')).resolves.toEqual(U_OFFSETS)
      expect([...h.cluster.targetTopics]).toEqual([])
      expect(
        h
          .md()
          .slice(before)
          .some(({ topics }) => topics.includes('T'))
      ).toBe(false)

      const named = await outcome(h.admin.fetchTopicOffsets('T'))
      expect(named.error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: { name: 'KafkaJSProtocolError', type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 },
      })
      h.cluster.findGroupCoordinator = async () => ({
        offsetFetch: async () => ({ responses: [] }),
      })
      await expect(
        h.admin.fetchOffsets({ groupId: 'g', topics: ['T', 'U'] })
      ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 })
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
    })

    test('a topic that became unauthorized fails the call that names it with code 29 after one request, and the next unrelated calls resolve', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.fetchTopicOffsets('T')
      h.refused.set('T', 29)
      const before = h.md().length

      await expect(h.admin.fetchTopicOffsets('T')).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'TOPIC_AUTHORIZATION_FAILED',
        code: 29,
      })
      expect(h.md().slice(before)).toEqual([{ api: 'Metadata', topics: ['T'] }])
      expect([...h.cluster.targetTopics]).toEqual([])
      await expect(h.admin.createTopics(CREATE('N'))).resolves.toBe(true)
      await expect(h.admin.fetchTopicOffsets('U')).resolves.toEqual(U_OFFSETS)
    })

    test('ten reads of distinct topics leave no target, and the next cluster-level refresh asks for every topic', async () => {
      h = build()
      const names = Array.from({ length: 10 }, (_, n) => `t${n}`)
      for (const name of names) h.live.set(name, [1])
      await h.admin.connect()
      for (const name of names) await h.admin.fetchTopicOffsets(name)
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])

      const before = h.md().length
      await h.admin.createTopics(CREATE('N'))
      expect(h.md().slice(before)).toEqual([{ api: 'Metadata', topics: [] }])
    })
  })

  describe('parallel reads (upstream #461)', () => {
    test('a first read that would join an older refresh of another topic gets its own complete answer', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.createTopics(CREATE('N'))
      alwaysStale(h.cluster)
      const held = latch()
      let uSeen = 0
      h.setGate(async (topics) => {
        if (topics.length === 1 && topics[0] === 'U' && ++uSeen === 2) await held.promise
      })

      const b = outcome(h.admin.fetchTopicOffsets('U'))
      await tick()
      const a = outcome(h.admin.fetchTopicOffsets('T'))
      await tick()
      held.release()

      expect(await a).toEqual({ ok: T_OFFSETS })
      expect(await b).toEqual({ ok: U_OFFSETS })
    })

    test('a later read whose older refresh answers while it routes still finds its topic', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.fetchTopicOffsets('T')
      await h.admin.fetchTopicOffsets('U')
      alwaysStale(h.cluster)
      const held = latch()
      let seen = 0
      h.setGate(async () => {
        if (++seen === 2) {
          setTimeout(held.release, 100)
          await held.promise
        }
      })
      const b = outcome(h.admin.fetchTopicOffsets('U'))
      await tick()
      h.setListHook(async (n) => {
        if (n === 1) {
          held.release()
          await tick(3)
        }
      })

      expect(await outcome(h.admin.fetchTopicOffsets('T'))).toEqual({ ok: T_OFFSETS })
      expect(await b).toEqual({ ok: U_OFFSETS })
    })

    test.each([['first'], ['later']])(
      'five reads started one after another in the %s round, their repeated refreshes answered newest first, each get their own offsets',
      async (round) => {
        h = build()
        const names = ['T', 'U', 'A', 'B', 'C']
        for (const name of ['A', 'B', 'C']) h.live.set(name, [1])
        await h.admin.connect()
        await h.admin.createTopics(CREATE('N'))
        if (round === 'later') await Promise.all(names.map((t) => h.admin.fetchTopicOffsets(t)))
        alwaysStale(h.cluster)
        const asked = new Set()
        const parked = []
        h.setGate(async (topics) => {
          const key = [...topics].sort().join()
          if (asked.has(key) || round === 'later') {
            const held = latch()
            parked.push(held)
            await held.promise
          }
          asked.add(key)
        })

        const calls = []
        for (const topic of names) {
          calls.push(outcome(h.admin.fetchTopicOffsets(topic)))
          await tick()
        }
        while (parked.length > 0) {
          parked.pop().release()
          await tick(2)
        }
        const results = await Promise.all(calls)

        expect(results).toEqual(names.map((t) => ({ ok: t === 'T' ? T_OFFSETS : U_OFFSETS })))
        expect([...h.cluster.targetTopics]).toEqual([])
      }
    )
  })

  describe('overlapping calls', () => {
    test('while a read holds a topic another client deleted, a cluster-level call fails with code 3, and resolves once the read has ended', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.createTopics(CREATE('N'))
      alwaysStale(h.cluster)
      const held = latch()
      let seen = 0
      h.setGate(async (topics) => {
        if (topics.includes('T') && ++seen === 2) {
          h.live.delete('T')
          await held.promise
        }
      })
      const x = outcome(h.admin.fetchTopicOffsets('T'))
      await tick()
      held.release()
      await tick()

      const during = await outcome(h.admin.createTopics(CREATE('N2')))
      expect(during.error).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 })
      expect((await x).error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' },
      })
      await expect(h.admin.createTopics(CREATE('N3'))).resolves.toBe(true)
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('an offset read overlapping a read of a deleted topic retries and resolves after that read released it', async () => {
      h = build({ retry: { retries: 6, initialRetryTime: 1, maxRetryTime: 2 } })
      await h.admin.connect()
      await h.admin.createTopics(CREATE('N'))
      alwaysStale(h.cluster)
      const held = latch()
      let seen = 0
      h.setGate(async (topics) => {
        if (topics.includes('T') && ++seen === 2) {
          h.live.delete('T')
          await held.promise
        }
      })
      const x = outcome(h.admin.fetchTopicOffsets('T'))
      await tick()
      held.release()

      expect(await outcome(h.admin.fetchTopicOffsets('U'))).toEqual({ ok: U_OFFSETS })
      expect((await x).error).toMatchObject({ cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' } })
    })

    test('two reads of one topic keep it a target until the second ends', async () => {
      h = build()
      await h.admin.connect()
      const held = latch()
      h.setListHook(async (n) => {
        if (n === 1) await held.promise
      })
      const first = h.admin.fetchTopicOffsets('T')
      await tick()

      await expect(h.admin.fetchTopicOffsets('T')).resolves.toEqual(T_OFFSETS)
      expect([...h.cluster.targetTopics]).toEqual(['T'])
      expect(h.cluster.targetHolds.get('T')).toBe(1)

      held.release()
      await expect(first).resolves.toEqual(T_OFFSETS)
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('deleteTopicRecords keeps its topic a target from the offsets read into the delete', async () => {
      h = build()
      await h.admin.connect()

      await h.admin.deleteTopicRecords({ topic: 'T', partitions: [{ partition: 0, offset: '3' }] })

      expect(h.calls.filter(({ api }) => api === 'DeleteRecords')).toEqual([
        { api: 'DeleteRecords', targets: ['T'] },
      ])
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('describeProducers and abortTransaction target their topics only while they run', async () => {
      h = build()
      await h.admin.connect()

      await h.admin.describeProducers({ topicPartitions: [{ topic: 'T', partitions: [0] }] })
      expect([...h.cluster.targetTopics]).toEqual([])
      await h.admin.abortTransaction({
        topic: 'U',
        partition: 0,
        producerId: '7',
        producerEpoch: 1,
        coordinatorEpoch: 1,
      })

      expect(
        h.calls.filter(({ targets }) => targets).map(({ api, targets }) => [api, targets])
      ).toEqual([
        ['DescribeProducers', ['T']],
        ['WriteTxnMarkers', ['U']],
      ])
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('deleteTopics by name and by id while a read holds the topic resolves, and the read then fails with code 3 itself', async () => {
      for (const selection of [{ topics: ['T'] }, { topicIds: [ID(1)] }]) {
        h = build()
        await h.admin.connect()
        alwaysStale(h.cluster)
        const held = latch()
        h.setListHook(async (n) => {
          if (n === 1) await held.promise
          if (!h.live.has('T')) throw createErrorFromCode(3)
        })
        const reader = outcome(h.admin.fetchTopicOffsets('T'))
        await tick()
        expect([...h.cluster.targetTopics]).toEqual(['T'])

        await expect(h.admin.deleteTopics(selection)).resolves.toBeUndefined()
        held.release()

        expect((await reader).error).toMatchObject({
          name: 'KafkaJSNumberOfRetriesExceeded',
          cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' },
        })
        expect([...h.cluster.targetTopics]).toEqual([])
        expect([...h.cluster.targetHolds]).toEqual([])
        await h.cluster.disconnect()
      }
    })
  })

  describe('every exit releases the hold', () => {
    test('success, a refused add, retry exhaustion and a failing batch all leave no hold', async () => {
      h = build()
      h.live.set('X', [1])
      await h.admin.connect()

      await h.admin.fetchTopicOffsetsByTimestamp('T', 0)
      await h.admin.fetchTopicOffsetsByTimestamp('U', { timestamp: 'earliest' })
      await h.admin.fetchOffsets({ groupId: 'g', topics: ['U'] }).catch(() => {})
      h.refused.set('X', 29)
      await outcome(h.admin.fetchTopicOffsets('X'))
      await outcome(h.admin.fetchTopicOffsets('missing'))
      await outcome(h.admin.fetchOffsets({ groupId: 'g', topics: ['missing', 'T'] }))
      await outcome(
        h.admin.describeProducers({ topicPartitions: [{ topic: 'missing', partitions: [0] }] })
      )
      await outcome(
        h.admin.abortTransaction({
          topic: 'missing',
          partition: 0,
          producerId: '7',
          producerEpoch: 1,
          coordinatorEpoch: 1,
        })
      )
      await outcome(
        h.admin.deleteTopicRecords({
          topic: 'missing',
          partitions: [{ partition: 0, offset: '1' }],
        })
      )

      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
    })

    test('past the API deadline the hold ends with the call, and the late answer of its refresh adds no target', async () => {
      h = build({ apiTimeout: 50 })
      await h.admin.connect()
      const held = latch()
      h.setGate(async (topics) => {
        if (topics.includes('T')) await held.promise
      })

      const call = outcome(h.admin.fetchTopicOffsets('T'))
      await tick(80)
      expect((await call).error).toMatchObject({ name: 'KafkaJSTimeout' })
      expect([...h.cluster.targetHolds]).toEqual([])
      expect([...h.cluster.targetTopics]).toEqual([])

      held.release()
      await tick(10)
      expect(h.md().filter(({ topics }) => topics.includes('T'))).toHaveLength(1)
      expect([...h.cluster.targetTopics]).toEqual([])
      await expect(h.admin.createTopics(CREATE('N'))).resolves.toBe(true)
      expect(h.md().slice(-1)).toEqual([{ api: 'Metadata', topics: [] }])
    })

    test('a forced disconnect during a call releases its hold, and a release taken before it does nothing afterwards', async () => {
      h = build()
      await h.admin.connect()
      h.setGate(async (topics) => {
        if (topics.includes('T')) await new Promise(() => {})
      })
      const call = outcome(h.admin.fetchTopicOffsets('T'))
      await tick()
      expect([...h.cluster.targetTopics]).toEqual(['T'])

      await h.admin.disconnect({ timeout: 0 })
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
      expect((await call).error).toMatchObject({ name: 'KafkaJSNonRetriableError' })

      h.setGate(null)
      await h.admin.connect()
      const old = h.cluster.holdTargetTopics(['U'])
      await h.cluster.disconnect()
      await h.admin.connect()
      const fresh = h.cluster.holdTargetTopics(['U'])
      h.cluster.targetTopics.add('U')
      old()
      old()
      expect([...h.cluster.targetTopics]).toEqual(['U'])
      fresh()
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('a hold of no topics and duplicate names count once each, and a release is one-shot', () => {
      h = build()
      const none = h.cluster.holdTargetTopics([])
      expect([...h.cluster.targetHolds]).toEqual([])
      none()

      const first = h.cluster.holdTargetTopics(['T', 'T'])
      const second = h.cluster.holdTargetTopics(['T'])
      h.cluster.targetTopics.add('T')
      expect(h.cluster.targetHolds.get('T')).toBe(2)
      first()
      first()
      expect([...h.cluster.targetTopics]).toEqual(['T'])
      second()
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
    })
  })

  describe('topic recreation', () => {
    test('is reported by a second read when no refresh came between, and not after a scoped refresh of another topic', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.fetchTopicOffsets('T')
      h.ids.set('T', ID(3))
      await h.admin.fetchTopicOffsets('T')
      expect(h.recreated).toEqual([expect.objectContaining({ topic: 'T' })])

      h.recreated.length = 0
      h.ids.set('T', ID(4))
      await h.admin.fetchTopicOffsets('U')
      await h.admin.fetchTopicOffsets('T')
      expect(h.recreated).toEqual([])
    })
  })
})
