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
 * Metadata answer waits for `gate(topics, n)` when one is set; `n` counts the Metadata requests;
 * a Map the gate returns replaces `live` for that answer. Its broker list is `nodes` as it was when the request was sent; its topics are read after the
 * gate. A ListOffsets request waits for `listHook(n, topics)` when one is set; `n` counts those
 * requests. Building a connection pool waits for `poolHook(host)` when one is set.
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
  let poolHook = null
  const nodes = [...NODES]
  const emitter = new InstrumentationEventEmitter()
  const recreated = []
  emitter.addListener(CLUSTER_TOPIC_RECREATED, ({ payload }) => recreated.push(payload))
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    retry,
    allowAutoTopicCreation: false,
    instrumentationEmitter: emitter,
    retainTargets: true,
  })
  const pool = cluster.brokerPool
  pool.buildConnectionPool = async ({ host = 'kafka1', port = 9092, rack = null } = {}) => {
    if (poolHook) await poolHook(host)
    return { host, port, rack, retire: () => {}, destroy: async () => {} }
  }
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
        const brokers = [...nodes]
        const view = (gate && (await gate(topics, n))) || live
        const named = topics.length > 0 ? topics : [...view.keys()]
        const { response } = requests.Metadata.protocol({ version: 13 })({ topics })
        return response.parse({
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode: 0,
          brokers,
          clusterId: 'c',
          controllerId: 1,
          topicMetadata: named.map((topic) => ({
            topicErrorCode: refused.get(topic) ?? (view.has(topic) ? 0 : 3),
            topic,
            topicId: view.has(topic) && !refused.has(topic) ? ids.get(topic) : null,
            isInternal: false,
            partitionMetadata: refused.has(topic) ? [] : (view.get(topic) ?? []).map(part),
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
        if (listHook) await listHook(++listed, topics)
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
    setPoolHook: (hook) => (poolHook = hook),
    nodes,
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
/** @param {Cluster} cluster @returns {string[]} the topics the installed metadata routes */
const routed = (cluster) => cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)
/** A producer-style lifecycle that never closes, as a fenced refresh carries one. */
const OPEN_FENCE = { closed: false, check: () => {}, guard: () => null, register: () => () => {} }
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
      const paused = latch()
      const resume = latch()
      h.setListHook(async (n, topics) => {
        if (n === 1) {
          paused.release()
          await resume.promise
        }
        if (topics.some(({ topic }) => !h.live.has(topic))) throw createErrorFromCode(3)
      })
      let ended = false
      const x = outcome(h.admin.fetchTopicOffsets('T')).finally(() => (ended = true))
      await paused.promise
      h.live.delete('T')

      const during = await outcome(h.admin.createTopics(CREATE('N2')))
      expect(ended).toBe(false)
      expect(h.cluster.targetHolds.get('T')).toBe(1)
      expect(during.error).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 })
      resume.release()
      expect((await x).error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' },
      })
      await expect(h.admin.createTopics(CREATE('N3'))).resolves.toBe(true)
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('an offset read overlapping a read of a deleted topic retries and resolves after that read released it', async () => {
      h = build()
      await h.admin.connect()
      const paused = latch()
      const resume = latch()
      h.setListHook(async (n) => {
        if (n === 1) {
          paused.release()
          await resume.promise
          throw createErrorFromCode(29)
        }
      })
      let ended = false
      const x = outcome(h.admin.fetchTopicOffsets('T')).finally(() => (ended = true))
      await paused.promise
      h.live.delete('T')
      const whileHeld = []
      h.setGate(async (topics) => {
        if (topics.includes('U'))
          whileHeld.push([[...topics].sort(), ended, h.cluster.targetHolds.get('T')])
        if (whileHeld.length === 1) resume.release()
      })

      expect(await outcome(h.admin.fetchTopicOffsets('U'))).toEqual({ ok: U_OFFSETS })
      expect(whileHeld[0]).toEqual([['T', 'U'], false, 1])
      expect(whileHeld.slice(-1)[0][0]).toEqual(['U'])
      expect((await x).error).toMatchObject({ type: 'TOPIC_AUTHORIZATION_FAILED' })
      expect([...h.cluster.targetTopics]).toEqual([])
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
        expect([...h.cluster.targetTopics]).toEqual([])
        expect(h.cluster.targetHolds.get('T')).toBe(1)
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

    test('the same admin deleting a topic between the two offset requests of a read that holds it fails that read with code 3', async () => {
      h = build()
      await h.admin.connect()
      alwaysStale(h.cluster)
      const paused = latch()
      const resume = latch()
      h.setListHook(async (n, topics) => {
        if (n === 1) {
          paused.release()
          await resume.promise
          return
        }
        if (topics.some(({ topic }) => !h.live.has(topic))) throw createErrorFromCode(3)
      })
      const reader = outcome(h.admin.fetchTopicOffsets('T'))
      await paused.promise

      await expect(h.admin.deleteTopics({ topics: ['T'] })).resolves.toBeUndefined()
      expect([...h.cluster.targetTopics]).toEqual([])
      expect(routed(h.cluster)).toContain('T')
      resume.release()

      const { error } = await reader
      expect(error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: { name: 'KafkaJSProtocolError', type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 },
      })
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
    })
  })

  describe('every metadata answer keeps the routing of the topics calls hold', () => {
    test.each([
      [
        'a named fetchTopicMetadata of another topic',
        (h) => h.admin.fetchTopicMetadata({ topics: ['U'] }),
      ],
      [
        'a named fetchTopicMetadata of the same topic',
        (h) => h.admin.fetchTopicMetadata({ topics: ['T'] }),
      ],
      [
        'a fenced refresh of another topic',
        (h) => h.cluster.brokerPool.refreshMetadata(['U'], { fence: OPEN_FENCE }),
      ],
    ])(
      '%s answered while a read routes its topic leaves that read its offsets',
      async (_, publish) => {
        h = build()
        await h.admin.connect()
        alwaysStale(h.cluster)
        const paused = latch()
        const resume = latch()
        h.setListHook(async (n) => {
          if (n === 1) {
            paused.release()
            await resume.promise
          }
        })
        const read = outcome(h.admin.fetchTopicOffsets('T'))
        await paused.promise

        await publish(h)
        expect(routed(h.cluster)).toContain('T')
        resume.release()

        expect(await read).toEqual({ ok: T_OFFSETS })
        expect([...h.cluster.targetTopics]).toEqual([])
      }
    )

    test('an older answer for every topic that lacks a held topic, landing while a read routes it, leaves that read its offsets', async () => {
      h = build()
      await h.admin.connect()
      alwaysStale(h.cluster)
      const allSent = latch()
      const allGo = latch()
      let armed = true
      h.setGate(async (topics) => {
        if (armed && topics.length === 0) {
          armed = false
          const before = new Map(h.live)
          before.delete('T')
          allSent.release()
          await allGo.promise
          return before
        }
      })
      const create = outcome(h.admin.createTopics(CREATE('N')))
      await allSent.promise
      const paused = latch()
      const resume = latch()
      h.setListHook(async (n) => {
        if (n === 1) {
          paused.release()
          await resume.promise
        }
      })
      const read = outcome(h.admin.fetchTopicOffsets('T'))
      await paused.promise

      allGo.release()
      expect(await create).toEqual({ ok: true })
      expect(routed(h.cluster)).toContain('T')
      resume.release()

      expect(await read).toEqual({ ok: T_OFFSETS })
      expect([...h.cluster.targetTopics]).toEqual([])
      expect([...h.cluster.targetHolds]).toEqual([])
    })

    test('an older refresh of another topic that retries after a newer answer leaves a routing read its offsets', async () => {
      h = build()
      await h.admin.connect()
      alwaysStale(h.cluster)
      const narrow = latch()
      const failNarrow = latch()
      const paused = latch()
      const resume = latch()
      let narrowSeen = 0
      h.setGate(async (topics) => {
        if (topics.length === 1 && topics[0] === 'U' && ++narrowSeen === 2) {
          narrow.release()
          await failNarrow.promise
          throw createErrorFromCode(5)
        }
      })
      let pausedOnce = false
      h.setListHook(async (n, topics) => {
        if (topics[0].topic === 'T' && !pausedOnce) {
          pausedOnce = true
          paused.release()
          await resume.promise
        }
      })
      const u = outcome(h.admin.fetchTopicOffsets('U'))
      await narrow.promise
      const t = outcome(h.admin.fetchTopicOffsets('T'))
      await paused.promise
      expect(routed(h.cluster).sort()).toEqual(['T', 'U'])

      failNarrow.release()
      expect(await u).toEqual({ ok: U_OFFSETS })
      expect(narrowSeen).toBe(3)
      expect(routed(h.cluster)).toContain('T')
      resume.release()

      expect(await t).toEqual({ ok: T_OFFSETS })
    })

    test('a read whose older answer lands while a newer answer is still building the broker of a moved leader gets its offsets', async () => {
      h = build()
      await h.admin.connect()
      await h.admin.fetchTopicOffsets('U')
      alwaysStale(h.cluster)
      const older = latch()
      const building = latch()
      const built = latch()
      const sent = latch()
      let tSeen = 0
      h.setGate(async (topics) => {
        if (topics.length === 1 && topics[0] === 'T' && ++tSeen === 2) {
          const before = new Map(h.live)
          sent.release()
          await older.promise
          return before
        }
      })
      const read = outcome(h.admin.fetchTopicOffsets('T'))
      await sent.promise

      h.nodes.push({ nodeId: 3, host: 'kafka3', port: 9092, rack: null })
      h.live.set('T', [3, 3])
      h.setPoolHook(async (host) => {
        if (host === 'kafka3') {
          building.release()
          await built.promise
        }
      })
      const newer = h.cluster.brokerPool.refreshMetadata(['T', 'U'])
      await building.promise
      older.release()
      await tick()
      built.release()

      expect(await read).toEqual({ ok: T_OFFSETS })
      await newer
      expect(h.calls.filter(({ api }) => api === 'ListOffsets').length).toBeGreaterThan(0)
    })
  })

  describe('a hold is released however its call ends', () => {
    test('a group-offset batch that fails fast keeps each started row its own hold until that row ends', async () => {
      h = build()
      await h.admin.connect()
      const queued = latch()
      const lock = h.cluster.mutatingTargetTopics
      const acquire = lock.acquire.bind(lock)
      let acquires = 0
      lock.acquire = () => (++acquires === 2 ? queued.promise.then(acquire) : acquire())
      const add = h.cluster.addTargetTopic.bind(h.cluster)
      h.cluster.addTargetTopic = (topic) =>
        add(topic).catch((e) => {
          if (topic === 'absent') queued.release()
          throw e
        })
      const heldWhileAdding = []
      h.setGate(async (topics) => {
        if (topics.includes('U')) heldWhileAdding.push(h.cluster.targetHolds.get('U'))
      })

      const call = await outcome(
        h.admin.fetchOffsetsForGroups({
          groups: [{ groupId: 'g', topics: [{ topic: 'absent' }, { topic: 'U' }] }],
        })
      )
      expect(call.error).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 })
      expect(acquires).toBe(2)
      for (let n = 0; n < 50 && heldWhileAdding.length === 0; n++) await tick(1)
      await tick()

      expect(heldWhileAdding[0]).toBe(1)
      expect([...h.cluster.targetHolds]).toEqual([])
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test('a group-offset read of topics without partitions holds each topic while it resolves it, and leaves no target', async () => {
      h = build()
      await h.admin.connect()
      const heldWhileAsked = []
      h.setGate(async (topics) => {
        heldWhileAsked.push([[...topics].sort(), [...h.cluster.targetHolds].sort()])
      })
      h.cluster.findGroupCoordinator = async () => ({
        nodeId: 1,
        lookupRequestWithMax: () => () => ({ request: { apiVersion: 9 } }),
        offsetFetch: async ({ groups }) => ({
          groups: groups.map(({ groupId, topics }) => ({
            groupId,
            errorCode: 0,
            topics: topics.map(({ topic, partitions }) => ({
              topic,
              partitions: partitions.map(({ partition }) => ({
                partition,
                committedOffset: '3',
                committedLeaderEpoch: -1,
                metadata: null,
                errorCode: 0,
              })),
            })),
          })),
        }),
      })

      const [result] = await h.admin.fetchOffsetsForGroups({
        groups: [{ groupId: 'g', topics: [{ topic: 'T' }, { topic: 'U' }] }],
      })

      expect(result.groupId).toBe('g')
      expect(heldWhileAsked.length).toBeGreaterThan(0)
      for (const [topics, holds] of heldWhileAsked) {
        expect(holds.map(([t]) => t)).toEqual(expect.arrayContaining(topics))
      }
      expect([...h.cluster.targetHolds]).toEqual([])
      expect([...h.cluster.targetTopics]).toEqual([])
    })

    test.each([
      [
        'fetchTopicOffsetsByTimestamp with a named spec',
        (a) => a.fetchTopicOffsetsByTimestamp('T', 'maxTimestamp'),
      ],
      [
        'fetchTopicOffsetsByTimestamp with a timestamp',
        (a) => a.fetchTopicOffsetsByTimestamp('T', 0),
      ],
      ['fetchTopicOffsets', (a) => a.fetchTopicOffsets('T')],
      [
        'abortTransaction',
        (a) =>
          a.abortTransaction({
            topic: 'T',
            partition: 0,
            producerId: '7',
            producerEpoch: 1,
            coordinatorEpoch: 1,
          }),
      ],
      [
        'deleteTopicRecords',
        (a) => a.deleteTopicRecords({ topic: 'T', partitions: [{ partition: 0, offset: '1' }] }),
      ],
    ])('%s with a retry setting its retrier refuses takes no hold', async (_, call) => {
      h = build()
      await h.admin.connect()
      const admin = createAdmin({
        cluster: h.cluster,
        logger: newLogger(),
        retry: { ...RETRY, maxTotalTime: 0 },
      })

      await expect(call(admin)).rejects.toMatchObject({
        name: 'KafkaJSInvalidConfigurationError',
        message: expect.stringContaining('retry.maxTotalTime'),
      })
      expect([...h.cluster.targetHolds]).toEqual([])
      expect([...h.cluster.targetTopics]).toEqual([])
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
