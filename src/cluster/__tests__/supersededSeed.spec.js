const { Kafka } = require('../../../index')
const Cluster = require('../index')
const BrokerPool = require('../brokerPool')
const { Schedule } = require('../../../testHelpers/schedule')
const {
  createScriptedCluster,
  groupScript,
  producerScript,
} = require('../../../testHelpers/scriptedCluster')
const { shareClient, until, flush } = require('../../../testHelpers/shareHarness')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { newLogger } = require('testHelpers')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const RETRY = { retries: 2, initialRetryTime: 5, maxRetryTime: 10, factor: 0 }
const TOPIC_A = '0a0a0a0a-0000-4000-8000-00000000000a'
const TOPIC_B = '0b0b0b0b-0000-4000-8000-00000000000b'
const CAUSES = ['rebootstrap', 'removeBroker']
const MESSAGES = {
  rebootstrap:
    'Metadata answer discarded: a rebootstrap replaced the seed broker while the request was in flight',
  removeBroker:
    'Metadata answer discarded: the removal of the failed seed broker replaced the seed broker while the request was in flight',
}

/**
 * @param {Promise} promise
 * @returns {Promise<{ ok: * }|{ error: Error }>} how it settled
 */
const outcome = (promise) =>
  promise.then(
    (ok) => ({ ok }),
    (error) => ({ error })
  )

/**
 * @param {BrokerPool} pool
 * @returns {Array<number>} the node ids the installed metadata lists that the broker table lacks
 */
const unreconciled = (pool) =>
  (pool.metadata?.brokers || []).map(({ nodeId }) => nodeId).filter((id) => !pool.hasNode(id))

/**
 * Replaces the pool's seed the way `how` names, and checks that it did.
 * @param {BrokerPool} pool
 * @param {'rebootstrap'|'removeBroker'} how
 * @returns {Promise<void>}
 */
const replaceSeed = async (pool, how) => {
  const seed = pool.seedBroker
  if (how === 'rebootstrap') await pool.rebootstrap('required')
  else pool.removeBroker({ host: seed.connectionPool.host, port: seed.connectionPool.port })
  expect(pool.seedBroker).not.toBe(seed)
}

/** @param {ReturnType<typeof createScriptedCluster>} cluster @returns {number} Metadata requests */
const metadataCount = (cluster) => cluster.tally('Metadata')

describe('a Metadata answer whose seed broker was replaced while the request was out', () => {
  let pools
  let returns
  let cleanups

  beforeEach(() => {
    pools = []
    returns = []
    cleanups = []
    const refresh = BrokerPool.prototype.refreshMetadataUnshared
    jest.spyOn(BrokerPool.prototype, 'refreshMetadataUnshared').mockImplementation(async function (
      ...args
    ) {
      if (!pools.includes(this)) pools.push(this)
      const result = await refresh.apply(this, args)
      returns.push({ installed: this.metadata !== null, unreconciled: unreconciled(this) })
      return result
    })
  })

  afterEach(async () => {
    for (const cleanup of cleanups.reverse()) await cleanup().catch(() => {})
    jest.restoreAllMocks()
  })

  /** Every refresh that returned left the metadata it installed reconciled into the table. */
  const expectEveryReturnReconciled = () => {
    expect(returns.length).toBeGreaterThan(0)
    expect(returns).toEqual(returns.map(() => ({ installed: true, unreconciled: [] })))
  }

  /**
   * A cluster over two scripted nodes whose only bootstrap broker is node 0, so a rebuilt seed is
   * always node 0's address.
   * @param {object} [options]
   * @param {object} [options.retry]
   * @returns {Promise<{ scripted: object, schedule: Schedule, cluster: Cluster, pool: BrokerPool }>}
   */
  const connectedCluster = async ({ retry = RETRY } = {}) => {
    const schedule = new Schedule()
    const scripted = producerScript(createScriptedCluster({ nodes: NODES, schedule }))
    const cluster = new Cluster({
      ...scripted.kafkaOptions({ brokers: ['node0:9092'] }),
      logger: newLogger(),
      retry,
    })
    cleanups.push(() => cluster.disconnect())
    await cluster.connect()
    await cluster.brokerPool.refreshMetadata([TOPIC])
    returns = []
    return { scripted, schedule, cluster, pool: cluster.brokerPool }
  }

  describe('the broker pool', () => {
    test.each(CAUSES)(
      'a late answer after %s is not installed and the attempt is asked again of the current seed',
      async (how) => {
        const { scripted, schedule, pool } = await connectedCluster()
        const before = metadataCount(scripted)
        const gate = schedule.hold({ api: 'Metadata' })
        const refresh = outcome(pool.refreshMetadata([TOPIC]))
        await gate.arrived()
        await replaceSeed(pool, how)
        const seed = pool.seedBroker
        gate.release()

        expect(await refresh).toEqual({ ok: undefined })
        expect(metadataCount(scripted) - before).toBe(2)
        expectEveryReturnReconciled()
        expect(pool.seedBroker).toBe(seed)
        expect(Object.keys(pool.brokers).sort()).toEqual(['0', '1'])
      }
    )

    test.each(CAUSES)(
      'with no retry left the refresh fails with a retriable error that says %s replaced the seed',
      async (how) => {
        const { scripted, schedule, pool } = await connectedCluster({
          retry: { ...RETRY, retries: 0 },
        })
        const before = metadataCount(scripted)
        const gate = schedule.hold({ api: 'Metadata' })
        const refresh = outcome(pool.refreshMetadata([TOPIC]))
        await gate.arrived()
        await replaceSeed(pool, how)
        gate.release()

        const { error } = await refresh
        expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
        expect(error.message).toBe(MESSAGES[how])
        expect(error.cause.name).toBe('KafkaJSConnectionError')
        expect(error.cause.retriable).toBe(true)
        expect(error.cause.message).toBe(MESSAGES[how])
        expect(metadataCount(scripted) - before).toBe(1)
        expect(pool.metadataExpireAt).toBeNull()
      }
    )

    test('a superseded attempt counts once: one retry is enough to recover from one replacement', async () => {
      const { scripted, schedule, pool } = await connectedCluster({
        retry: { ...RETRY, retries: 1 },
      })
      const before = metadataCount(scripted)
      const gate = schedule.hold({ api: 'Metadata' })
      const refresh = outcome(pool.refreshMetadata([TOPIC]))
      await gate.arrived()
      await replaceSeed(pool, 'rebootstrap')
      gate.release()

      expect(await refresh).toEqual({ ok: undefined })
      expect(metadataCount(scripted) - before).toBe(2)
      expectEveryReturnReconciled()
    })

    test('a rebootstrap before every answer ends at the retry budget with an error, not with an empty broker table', async () => {
      const { scripted, schedule, pool } = await connectedCluster()
      const before = metadataCount(scripted)
      for (let i = 0; i < 10; i++) {
        schedule.once({ api: 'Metadata' }, async () => {
          await pool.rebootstrap('required')
        })
      }

      const { error } = await outcome(pool.refreshMetadata([TOPIC]))
      expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.message).toBe(MESSAGES.rebootstrap)
      expect(metadataCount(scripted) - before).toBe(RETRY.retries + 1)
      expect(pool.metadata).toBeNull()
      expect(returns).toEqual([])
    })

    test.each(CAUSES)(
      'a seed replaced by %s while the answer is being reconciled restarts the attempt on the current seed',
      async (how) => {
        const schedule = new Schedule()
        const scripted = producerScript(createScriptedCluster({ nodes: NODES, schedule }))
        const cluster = new Cluster({
          ...scripted.kafkaOptions({ brokers: ['node0:9092'] }),
          logger: newLogger(),
          retry: RETRY,
        })
        cleanups.push(() => cluster.disconnect())
        await cluster.connect()
        const pool = cluster.brokerPool
        const build = pool.buildConnectionPool
        let replaced = false
        pool.buildConnectionPool = async function (destination, options) {
          if (destination && !replaced) {
            replaced = true
            await replaceSeed(pool, how)
          }
          return build.call(this, destination, options)
        }

        expect(await outcome(pool.refreshMetadata([TOPIC]))).toEqual({ ok: undefined })
        expect(replaced).toBe(true)
        expect(metadataCount(scripted)).toBe(2)
        expectEveryReturnReconciled()
      }
    )

    test.each(CAUSES)(
      'callers joined to one refresh whose seed %s replaced all get the retried result',
      async (how) => {
        const { scripted, schedule, pool } = await connectedCluster()
        const before = metadataCount(scripted)
        const gate = schedule.hold({ api: 'Metadata' })
        const seen = []
        const joined = [1, 2, 3].map(() =>
          outcome(pool.refreshMetadata([TOPIC]).then(() => seen.push(unreconciled(pool))))
        )
        await gate.arrived()
        await replaceSeed(pool, how)
        gate.release()

        expect(await Promise.all(joined)).toEqual([{ ok: 1 }, { ok: 2 }, { ok: 3 }])
        expect(seen).toEqual([[], [], []])
        expect(metadataCount(scripted) - before).toBe(2)
      }
    )

    test('a retried refresh leaves no flight to join and no timer behind', async () => {
      const { scripted, schedule, pool } = await connectedCluster()
      const timers = () =>
        // eslint-disable-next-line n/no-unsupported-features/node-builtins -- stable from Node 22.16 and 24.0; a development check
        process.getActiveResourcesInfo().filter((type) => type === 'Timeout').length
      const idle = timers()
      const asked = metadataCount(scripted)
      const gate = schedule.hold({ api: 'Metadata' })
      const refresh = outcome(pool.refreshMetadata([TOPIC]))
      await gate.arrived()
      await replaceSeed(pool, 'rebootstrap')
      gate.release()
      expect(await refresh).toEqual({ ok: undefined })
      expect(metadataCount(scripted) - asked).toBe(2)
      expect(timers()).toBeLessThanOrEqual(idle)

      const before = metadataCount(scripted)
      await pool.refreshMetadata([TOPIC])
      expect(metadataCount(scripted) - before).toBe(1)
    })
  })

  describe('every client', () => {
    /**
     * A Kafka client over two scripted nodes, node 0 the only bootstrap broker.
     * @param {(cluster: object) => object} script
     * @param {object} [options] Kafka options and the cluster profile
     * @returns {{ scripted: object, schedule: Schedule, kafka: Kafka }}
     */
    const client = (script, { profile, ...options } = {}) => {
      const schedule = new Schedule()
      const scripted = script(
        createScriptedCluster({ nodes: NODES, schedule, ...(profile && { profile }) })
      )
      const kafka = new Kafka(
        scripted.kafkaOptions({ brokers: ['node0:9092'], retry: RETRY, ...options })
      )
      return { scripted, schedule, kafka }
    }

    /**
     * Holds the next Metadata request `act` causes, replaces the seed while it is out, then lets
     * the answer arrive.
     * @param {object} setup `client()`'s result
     * @param {BrokerPool} pool
     * @param {'rebootstrap'|'removeBroker'} how
     * @param {() => Promise<*>} act
     * @returns {Promise<{ ok: * }|{ error: Error }>} how `act` settled
     */
    const supersede = async ({ schedule }, pool, how, act) => {
      returns = []
      pool.metadataExpireAt = 0
      const gate = schedule.hold({ api: 'Metadata' })
      const settled = outcome(act())
      await gate.arrived()
      await replaceSeed(pool, how)
      gate.release()
      return settled
    }

    test.each(CAUSES)('a producer with retries 0 sends after %s', async (how) => {
      const setup = client(producerScript)
      const producer = setup.kafka.producer({ idempotent: false, retry: { retries: 0 } })
      cleanups.push(() => producer.disconnect())
      await producer.connect()
      await producer.send({ topic: TOPIC, messages: [{ value: 'a', partition: 0 }] })
      const [pool] = pools

      const sent = await supersede(setup, pool, how, () =>
        producer.send({ topic: TOPIC, messages: [{ value: 'b', partition: 0 }] })
      )
      expect(sent.error).toBeUndefined()
      expect(sent.ok).toHaveLength(1)
      expectEveryReturnReconciled()
    })

    test.each(CAUSES)('a producer with default retries sends after %s', async (how) => {
      const setup = client(producerScript)
      const producer = setup.kafka.producer()
      cleanups.push(() => producer.disconnect())
      await producer.connect()
      await producer.send({ topic: TOPIC, messages: [{ value: 'a', partition: 0 }] })
      const [pool] = pools

      const sent = await supersede(setup, pool, how, () =>
        producer.send({ topic: TOPIC, messages: [{ value: 'b', partition: 0 }] })
      )
      expect(sent.error).toBeUndefined()
      expect(sent.ok).toHaveLength(1)
      expectEveryReturnReconciled()
    })

    test.each(CAUSES)('a transactional producer sends and commits after %s', async (how) => {
      const setup = client(producerScript, { profile: 'tv2' })
      const producer = setup.kafka.producer({ transactionalId: 'txn', maxInFlightRequests: 1 })
      cleanups.push(() => producer.disconnect())
      await producer.connect()
      const warm = await producer.transaction()
      await warm.send({ topic: TOPIC, messages: [{ value: 'a', partition: 0 }] })
      await warm.commit()
      const [pool] = pools
      const transaction = await producer.transaction()

      const sent = await supersede(setup, pool, how, () =>
        transaction.send({ topic: TOPIC, messages: [{ value: 'b', partition: 0 }] })
      )
      expect(sent.error).toBeUndefined()
      expect(sent.ok).toHaveLength(1)
      await transaction.commit()
      expect(setup.scripted.tally('EndTxn', 'answered')).toBe(2)
      expectEveryReturnReconciled()
    })

    test.each(CAUSES)('an admin call completes after %s', async (how) => {
      const setup = client(producerScript)
      const admin = setup.kafka.admin()
      cleanups.push(() => admin.disconnect())
      await admin.connect()
      await admin.fetchTopicMetadata({ topics: [TOPIC] })
      const [pool] = pools

      const read = await supersede(setup, pool, how, () =>
        admin.fetchTopicMetadata({ topics: [TOPIC] })
      )
      expect(read.error).toBeUndefined()
      expect(read.ok.topics.map(({ name }) => name)).toEqual([TOPIC])
      expectEveryReturnReconciled()
    })

    test('an admin call whose every attempt a rebootstrap supersedes ends at its own deadline', async () => {
      const setup = client(producerScript, {
        retry: { retries: 1000, initialRetryTime: 20, maxRetryTime: 20, factor: 0 },
      })
      const admin = setup.kafka.admin()
      cleanups.push(() => admin.disconnect())
      await admin.connect()
      await admin.fetchTopicMetadata({ topics: [TOPIC] })
      const [pool] = pools
      const before = metadataCount(setup.scripted)
      for (let i = 0; i < 1000; i++) {
        setup.schedule.once({ api: 'Metadata' }, async () => {
          await pool.rebootstrap('required')
        })
      }

      returns = []
      const started = Date.now()
      const { error } = await outcome(
        admin.fetchTopicMetadata({ topics: [TOPIC], apiTimeout: 300 })
      )
      expect(error.name).toBe('KafkaJSTimeout')
      expect(error.message).toMatch(/^fetchTopicMetadata did not complete within 300 ms/)
      expect(Date.now() - started).toBeLessThan(3000)
      expect(metadataCount(setup.scripted) - before).toBeGreaterThan(1)
      expect(returns).toEqual([])
    })

    test.each(CAUSES)(
      'a consumer keeps fetching after its fetch-loop refresh met %s',
      async (how) => {
        const brokerAnswerTimers = new Set()
        const setup = client((cluster) => groupScript(cluster, { batches: 0, brokerAnswerTimers }))
        const consumer = setup.kafka.consumer({ groupId: 'group', maxWaitTimeInMs: 5 })
        const crashes = []
        consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload.error))
        cleanups.push(async () => {
          await consumer.disconnect()
          for (const timer of brokerAnswerTimers) clearTimeout(timer)
        })
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
        await consumer.run({ eachMessage: async () => {} })
        await until(() => setup.scripted.tally('Fetch') > 0, 'the first fetch')
        const pool = pools.find((candidate) => candidate.metadata?.topicMetadata.length > 0)

        returns = []
        pool.metadataExpireAt = 0
        const gate = setup.schedule.hold({ api: 'Metadata' })
        await gate.arrived()
        await replaceSeed(pool, how)
        gate.release()
        await until(() => returns.length > 0, 'the refresh to return')
        const fetches = setup.scripted.tally('Fetch')
        await until(() => setup.scripted.tally('Fetch') > fetches + 2, 'fetching after the refresh')

        expect(crashes).toEqual([])
        expect(returns[0]).toEqual({ installed: true, unreconciled: [] })
      }
    )

    test.each(CAUSES)('a share consumer subscribes and fetches after %s', async (how) => {
      const h = shareClient({
        nodes: 2,
        interval: 20,
        kafka: { brokers: ['node0:9092'] },
        topics: {
          'topic-a': { topicId: TOPIC_A, partitions: [{ leader: 0, epoch: 1 }] },
          'topic-b': { topicId: TOPIC_B, partitions: [{ leader: 1, epoch: 1 }] },
        },
      })
      cleanups.push(() => h.close())
      await h.consumer.subscribe({ topics: ['topic-a'] })
      const [pool] = pools

      const subscribed = await supersede(h, pool, how, () =>
        h.consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
      )
      expect(subscribed).toEqual({ ok: undefined })
      expectEveryReturnReconciled()
      await h.consumer.run({ eachMessage: async () => {} })
      await until(() => h.frames(78).length > 0, 'a share fetch')
      await flush()
    })
  })
})
