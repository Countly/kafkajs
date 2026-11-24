const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const Cluster = require('../index')
const Generation = require('../../producer/generation')
const ConnectionPool = require('../../network/connectionPool')
const Subscription = require('../../consumer/subscription')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const { KafkaJSConnectionClosedError, KafkaJSConnectionError } = require('../../errors')
const { responseHeaderVersion } = require('../../protocol/headers')
const apiKeys = require('../../protocol/requests/apiKeys')
const { respondWithFrame } = require('../../../testHelpers/fakeSocket')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const metadataPair = require('../../../testHelpers/wire/metadataV13')
const { newLogger } = require('testHelpers')

/**
 * Pairs an observable broker double with a real connection pool, without opening sockets.
 * @param {number} nodeId table identity
 * @param {string} [host='old'] endpoint host
 * @param {number} [generation=3] initial summed pool generation
 * @returns {object} broker dispatch fixture
 */
const broker = (nodeId, host = 'old', generation = 3) => {
  const connectionPool = new ConnectionPool({ logger: newLogger(), host, port: 9092 })
  connectionPool.pool[0].generation = generation
  connectionPool.destroy = jest.fn(async () => {})
  const object = new Broker({ connectionPool, logger: newLogger(), nodeId })
  object.disconnect = jest.fn(async () => {})
  object.retire = jest.fn(async () => {})
  object.metadata = jest.fn(async () => ({ brokers: [], topicMetadata: [] }))
  object.isConnected = () => true

  return object
}
/** Builds the pool controller with explicit broker dispatch and real pool generations.
 * @returns {object} controller, seed and asynchronous builder
 */
const build = () => {
  let serial = 0
  const builder = {
    build: jest.fn(async ({ host = `seed-${++serial}`, port = 9092, rack } = {}) => {
      const pool = new ConnectionPool({ logger: newLogger(), host, port, rack })
      pool.destroy = jest.fn(async () => {})
      return pool
    }),
  }
  const pool = new BrokerPool({
    connectionPoolBuilder: builder,
    logger: newLogger(),
    retry: { retries: 0 },
    drainTimeout: 50,
  })
  pool.createBroker = jest.fn(({ nodeId, connectionPool }) =>
    Object.assign(broker(nodeId), { connectionPool })
  )
  const seed = broker(0, 'seed')
  pool.seedBroker = seed
  pool.brokers = { 0: seed }
  pool.findConnectedBroker = async () => seed
  return { pool, seed, builder }
}

/**
 * Bounds recovery independently of a deliberately unresolved obsolete drain.
 * @param {Promise<unknown>} work recovery operation
 * @returns {Promise<unknown>} the recovery result
 * @throws {Error} when recovery waits for the held obsolete drain
 */
const within = async (work) => {
  let timer
  try {
    return await Promise.race([
      work,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('replacement waited for obsolete drain')), 5000)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/** Lets every ready microtask and I/O callback run. */
const settled = () => new Promise(setImmediate)

/**
 * What a promise settled with, so a rejection is a value to look at.
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ value?: unknown, error?: Error }>}
 */
const outcome = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

/**
 * Makes every pool the builder produces wait for the test, newest last.
 * @param {{ build: jest.Mock }} builder the fixture's builder
 * @returns {Array<{ host: string, pool: ConnectionPool, release: () => void, fail: (error: Error) => void }>}
 *   the builds, in call order
 */
const holdBuilds = (builder) => {
  const builds = []
  builder.build.mockImplementation(
    ({ host, port, rack } = {}) =>
      new Promise((resolve, reject) => {
        const pool = new ConnectionPool({ logger: newLogger(), host, port, rack })
        pool.destroy = jest.fn(async () => {})
        builds.push({ host, pool, release: () => resolve(pool), fail: reject })
      })
  )
  return builds
}

/**
 * Runs two refreshes against the seed whose replies are both ready at once: the older publishes
 * first and the newer publishes while the older still waits for its first pool build. The older
 * build is then released, and after it the newer one.
 * @param {ReturnType<typeof build>} fixture
 * @param {object} older the first reply
 * @param {object} newer the second reply
 * @returns {Promise<Array<{ host: string, pool: ConnectionPool, release: () => void }>>} the builds
 */
const overlap = async ({ pool, seed, builder }, older, newer) => {
  const builds = holdBuilds(builder)
  seed.metadata.mockResolvedValueOnce(older).mockResolvedValueOnce(newer)
  const refreshing = Promise.all([
    pool.refreshMetadata([], { isCurrent: () => true }),
    pool.refreshMetadata([], { isCurrent: () => true }),
  ])
  await settled()
  expect(builds).toHaveLength(2)
  builds[0].release()
  await settled()
  builds[1].release()
  await refreshing
  return builds
}

const SEED_NODE = { nodeId: 0, host: 'seed', port: 9092 }

describe('Obsolete broker dispatch', () => {
  it('seed replacement installs before retirement and never awaits the drain', async () => {
    const { pool, seed } = build()
    seed.disconnect.mockImplementation(() => new Promise(() => {}))
    seed.retire.mockImplementation(() => {
      expect(pool.seedBroker).not.toBe(seed)
      return new Promise(() => {})
    })
    await within(pool.createSeedBroker())
    expect(seed.retire).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(pool.brokers[0]).toBeUndefined()
    expect(pool.seedBroker.connectionPool.host).toBe('seed-1')
  })

  it('rebootstrap deduplicates aliases and retires a broker added during the seed build', async () => {
    const { pool, seed, builder } = build()
    const old = broker(1)
    const replacement = broker(1, 'new')
    pool.brokers = { 0: seed, 1: old, 9: old }
    builder.build.mockImplementationOnce(async () => {
      pool.brokers[1] = replacement
      return { host: 'next-seed', port: 9092 }
    })
    old.retire.mockImplementation(() => new Promise(() => {}))
    await within(pool.rebootstrap('required'))
    expect(old.retire).toHaveBeenCalledTimes(1)
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(replacement.retire).toHaveBeenCalledTimes(1)
    expect(pool.brokers).toEqual({})
  })

  it('metadata replacement and removal install synchronously and do not await old drains', async () => {
    const { pool, seed } = build()
    const moved = broker(1)
    const unused = broker(2)
    moved.disconnect.mockImplementation(() => new Promise(() => {}))
    unused.disconnect.mockImplementation(() => new Promise(() => {}))
    pool.brokers = { 0: seed, 1: moved, 2: unused }
    seed.metadata.mockResolvedValue({
      brokers: [
        { nodeId: 0, host: 'seed', port: 9092 },
        { nodeId: 1, host: 'next', port: 9092 },
      ],
      topicMetadata: [],
    })
    moved.retire.mockImplementation(async () => {
      expect(pool.brokers[1]).not.toBe(moved)
    })
    await within(pool.refreshMetadata([]))
    expect(pool.brokers[1].connectionPool.host).toBe('next')
    expect(pool.brokers[2]).toBeUndefined()
    expect(moved.retire).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(unused.retire).toHaveBeenCalledWith({ drainTimeout: 50 })
  })

  it('metadata moves the shared seed pool and prunes only an unadvertised table alias', async () => {
    const { pool, seed } = build()
    const oldPool = seed.connectionPool
    pool.brokers[9] = seed
    seed.metadata.mockResolvedValue({
      brokers: [{ nodeId: 0, host: 'moved', port: 9092 }],
      topicMetadata: [],
    })
    await pool.refreshMetadata([])
    expect(pool.seedBroker).toBe(seed)
    expect(pool.brokers[0]).toBe(seed)
    expect(pool.brokers[9]).toBeUndefined()
    expect(seed.connectionPool.host).toBe('moved')
    expect(seed.connectionPool).not.toBe(oldPool)
    expect(oldPool.retired).toBe(true)
    expect(oldPool.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(seed.retire).not.toHaveBeenCalled()
  })

  it.each([
    ['the moved id first', [0, 3]],
    ['the seed-address id first', [3, 0]],
  ])(
    'metadata that gives the seed address to another id keeps both ids routed by address, %s',
    async (_, order) => {
      const { pool, seed } = build()
      const seedPool = seed.connectionPool
      const table = {
        0: { nodeId: 0, host: 'moved', port: 9092 },
        3: { nodeId: 3, host: 'seed', port: 9092 },
      }
      seed.metadata.mockResolvedValue({ brokers: order.map((id) => table[id]), topicMetadata: [] })
      for (let refresh = 0; refresh < 2; refresh++) {
        await pool.refreshMetadata([])
        expect(pool.seedBroker).toBe(seed)
        expect(pool.brokers[3]).toBe(seed)
        expect(seed.connectionPool).toBe(seedPool)
        expect(pool.brokers[0]).not.toBe(seed)
        expect(pool.brokers[0].connectionPool.host).toBe('moved')
      }
      expect(seedPool.retired).toBe(false)
      expect(seed.retire).not.toHaveBeenCalled()
    }
  )

  it('metadata cannot overwrite an entry installed while its replacement pool was being built', async () => {
    const { pool, seed, builder } = build()
    const old = broker(1)
    const replacement = broker(1, 'hint')
    const unused = new ConnectionPool({ logger: newLogger(), host: 'metadata', port: 9092 })
    unused.destroy = jest.fn(async () => {})
    pool.brokers[1] = old
    seed.metadata.mockResolvedValue({
      brokers: [
        { nodeId: 0, host: 'seed', port: 9092 },
        { nodeId: 1, host: 'metadata', port: 9092 },
      ],
      topicMetadata: [],
    })
    builder.build.mockImplementationOnce(async () => {
      pool.brokers[1] = replacement
      return unused
    })
    await pool.refreshMetadata([])
    expect(pool.brokers[1] === replacement).toBe(true)
    expect(unused.retired).toBe(true)
    expect(unused.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(old.retire).not.toHaveBeenCalled()
    expect(replacement.retire).not.toHaveBeenCalled()
  })

  it('an answer whose seed is replaced while its pools are built still installs every broker it lists', async () => {
    const { pool, seed, builder } = build()
    const old = broker(1)
    const peer = broker(2)
    pool.brokers = { 0: seed, 1: old, 2: peer }
    seed.metadata.mockResolvedValue({
      brokers: [
        { nodeId: 1, host: 'metadata', port: 9092 },
        { nodeId: 2, host: 'later', port: 9092 },
      ],
      topicMetadata: [],
    })
    builder.build.mockImplementationOnce(async ({ host, port, rack }) => {
      await pool.createSeedBroker()
      const built = new ConnectionPool({ logger: newLogger(), host, port, rack })
      built.destroy = jest.fn(async () => {})
      return built
    })
    const seen = []
    pool.metadataCache.onRoutingChange(() =>
      seen.push(Object.entries(pool.brokers).map(([id, b]) => `${id}:${b.connectionPool.host}`))
    )
    await pool.refreshMetadata([])
    expect(pool.seedBroker).not.toBe(seed)
    expect(Object.keys(pool.brokers)).toEqual(['1', '2'])
    expect(pool.brokers[1].connectionPool.host).toBe('metadata')
    expect(pool.brokers[2].connectionPool.host).toBe('later')
    expect(seen).toEqual([['1:metadata', '2:later']])
    expect(builder.build).toHaveBeenCalledTimes(3)
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(old.retire).toHaveBeenCalledTimes(1)
    expect(peer.retire).toHaveBeenCalledTimes(1)
  })

  it('a hint retires the old identity once and keeps the current entry across two replacements', async () => {
    const { pool } = build()
    const old = broker(1)
    pool.brokers[1] = old
    await pool.applyNodeEndpoints([{ nodeId: 1, host: 'next', port: 9092 }])
    const middle = pool.brokers[1]
    await pool.applyNodeEndpoints([{ nodeId: 1, host: 'last', port: 9092 }])
    const last = pool.brokers[1]
    expect(old.retire).toHaveBeenCalledTimes(1)
    expect(middle.retire).toHaveBeenCalledTimes(1)
    expect(last.connectionPool.host).toBe('last')
    pool.closeObsolete(old)
    expect(pool.brokers[1]).toBe(last)
    expect(old.retire).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['no host', { nodeId: 1, host: '', port: 9092 }],
    ['no port', { nodeId: 1, host: 'next' }],
    ['a fractional port', { nodeId: 1, host: 'next', port: 9092.5 }],
    ['a zero port', { nodeId: 1, host: 'next', port: 0 }],
    ['a negative port', { nodeId: 1, host: 'next', port: -1 }],
  ])('an endpoint with %s is skipped and the rest of the hint still applies', async (_, bad) => {
    const { pool, builder } = build()
    const old = broker(1)
    pool.brokers[1] = old

    await pool.applyNodeEndpoints([bad, { nodeId: 2, host: 'valid', port: 9093 }])

    expect(pool.brokers[1]).toBe(old)
    expect(old.retire).not.toHaveBeenCalled()
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(builder.build).toHaveBeenCalledWith(expect.objectContaining({ host: 'valid' }))
    expect(pool.brokers[2].connectionPool).toMatchObject({ host: 'valid', port: 9093 })
  })

  it('a shared seed hint keeps the Broker and retires its previous pool with the bound', async () => {
    const { pool, seed } = build()
    const oldPool = seed.connectionPool
    pool.brokers[9] = seed
    await pool.applyNodeEndpoints([{ nodeId: 0, host: 'moved', port: 9092 }])
    expect(pool.seedBroker).toBe(seed)
    expect(pool.brokers[0]).toBe(seed)
    expect(pool.brokers[9]).toBe(seed)
    expect(oldPool.retired).toBe(true)
    expect(oldPool.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(seed.retire).not.toHaveBeenCalled()
  })

  it('a reused seed continues the baseline and ignores failures before replacement connect', async () => {
    const { pool, seed } = build()
    const oldGeneration = seed.connectionPool.generation
    const source = { broker: seed, generation: oldGeneration }
    await pool.applyNodeEndpoints([{ nodeId: 0, host: 'moved', port: 9092 }])
    expect(seed.connectionPool.generation).toBe(oldGeneration)
    pool.removeBroker({ host: 'seed', port: 9092, source })
    expect(pool.seedBroker).toBe(seed)
    expect(pool.brokers[0]).toBe(seed)
    expect(seed.retire).not.toHaveBeenCalled()
    const nextGeneration = seed.connectionPool.generation
    await pool.applyNodeEndpoints([{ nodeId: 0, host: 'last', port: 9092 }])
    expect(seed.connectionPool.generation).toBe(nextGeneration)
  })

  it('a retired Broker refuses a late pool and keeps its attached pool', async () => {
    const { pool, seed, builder } = build()
    const current = new Broker({ connectionPool: seed.connectionPool, logger: newLogger() })
    const attached = current.connectionPool
    await current.retire({ drainTimeout: 0 })
    const replacement = await builder.build({ host: 'late', port: 9092 })
    pool.replacePool(current, replacement)
    expect(current.connectionPool).toBe(attached)
    expect(current.retired).toBe(true)
    expect(replacement.retired).toBe(true)
    expect(replacement.destroy).toHaveBeenCalledTimes(1)
    expect(attached.destroy).toHaveBeenCalledTimes(1)
  })

  it('a stale asynchronous hint cannot replace the installed broker and retires the unused pool', async () => {
    const { pool, builder } = build()
    const old = broker(1)
    const replacement = broker(1, 'new')
    const unusedPool = new ConnectionPool({ logger: newLogger(), host: 'stale', port: 9092 })
    unusedPool.destroy = jest.fn(async () => {})
    pool.brokers[1] = old
    builder.build.mockImplementationOnce(async () => {
      pool.brokers[1] = replacement
      return unusedPool
    })
    await pool.applyNodeEndpoints([{ nodeId: 1, host: 'stale', port: 9092 }])
    expect(pool.brokers[1]).toBe(replacement)
    expect(unusedPool.retired).toBe(true)
    expect(unusedPool.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(old.retire).not.toHaveBeenCalled()
  })

  it.each(['object', 'generation'])(
    'a stale sourced %s failure cannot fall back to the same address',
    (stale) => {
      const { pool } = build()
      const old = broker(1)
      const current = stale === 'object' ? broker(1) : old
      pool.brokers[1] = current
      const source = { broker: old, generation: 3 }
      if (stale === 'generation') current.connectionPool.pool[0].generation = 4
      pool.removeBroker({ host: 'old', port: 9092, source })
      expect(pool.brokers[1]).toBe(current)
      expect(current.retire).not.toHaveBeenCalled()
    }
  )

  it('a current source removes all aliases by identity while preserving unrelated same-address entries', () => {
    const { pool } = build()
    const old = broker(1)
    const peer = broker(2)
    pool.brokers = { 1: old, 2: peer, 9: old }
    pool.removeBroker({ host: 'old', port: 9092, source: { broker: old, generation: 3 } })
    expect(pool.brokers[1]).toBeUndefined()
    expect(pool.brokers[9]).toBeUndefined()
    expect(pool.brokers[2]).toBe(peer)
    expect(old.retire).toHaveBeenCalledTimes(1)
  })

  it('an unsourced failure keeps address removal and retires the removed object', () => {
    const { pool } = build()
    const old = broker(1)
    pool.brokers[1] = old
    pool.removeBroker({ host: 'old', port: 9092 })
    expect(pool.brokers[1]).toBeUndefined()
    expect(old.retire).toHaveBeenCalledWith({ drainTimeout: 50 })
  })

  it('a current source for a shared seed installs an alternate before retiring it', () => {
    const { pool, seed } = build()
    const alternate = broker(1, 'alternate')
    pool.brokers[1] = alternate
    pool.removeBroker({ host: 'seed', port: 9092, source: { broker: seed, generation: 3 } })
    expect(pool.seedBroker).toBe(alternate)
    expect(pool.brokers[0]).toBeUndefined()
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(pool.brokers[1]).toBe(alternate)
  })

  it('Cluster forwards the source object without loss and wires requestTimeout as the drain bound', async () => {
    const cluster = new Cluster({
      logger: newLogger(),
      brokers: ['localhost:9092'],
      requestTimeout: 37,
    })
    const source = { broker: {}, generation: 7 }
    const remove = jest.spyOn(cluster.brokerPool, 'removeBroker').mockImplementation(() => {})
    cluster.removeBroker({ host: 'node', port: 9092, source })
    expect(remove).toHaveBeenCalledWith({ host: 'node', port: 9092, source })
    expect(cluster.brokerPool.drainTimeout).toBe(37)
    const connections = await cluster.connectionPoolBuilder.build({ host: 'node', port: 9092 })
    await connections.destroy()
  })
})

describe('Overlapping metadata refreshes', () => {
  it('install the endpoint of the topology published last, and discard the older pool', async () => {
    const fixture = build()
    const [older, newer] = await overlap(
      fixture,
      { brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }], topicMetadata: [] },
      { brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }], topicMetadata: [] }
    )
    const { pool } = fixture
    expect(pool.metadata.brokers[1].host).toBe('second')
    expect(pool.brokers[1].connectionPool === newer.pool).toBe(true)
    expect(newer.pool.retired).toBe(false)
    expect(older.pool.retired).toBe(true)
    expect(older.pool.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
  })

  it('install the endpoint published last when the older reply finds the node already moved', async () => {
    const fixture = build()
    const previous = broker(1, 'initial')
    fixture.pool.brokers[1] = previous
    const [older, newer] = await overlap(
      fixture,
      { brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }], topicMetadata: [] },
      { brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }], topicMetadata: [] }
    )
    expect(fixture.pool.brokers[1].connectionPool === newer.pool).toBe(true)
    expect(older.pool.retired).toBe(true)
    expect(previous.retire).toHaveBeenCalledTimes(1)
  })

  it('never remove a broker the newer reply still lists', async () => {
    const fixture = build()
    const peer = broker(3, 'peer')
    fixture.pool.brokers[3] = peer
    const listedPeer = { nodeId: 3, host: 'peer', port: 9092 }
    await overlap(
      fixture,
      { brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }], topicMetadata: [] },
      {
        brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }, listedPeer],
        topicMetadata: [],
      }
    )
    expect(fixture.pool.brokers[3] === peer).toBe(true)
    expect(peer.retire).not.toHaveBeenCalled()
  })

  it('wake routing listeners once, after the newer answer installed its brokers, when the older build finishes while the newer is held', async () => {
    const fixture = build()
    const seen = []
    fixture.pool.metadataCache.onRoutingChange(() =>
      seen.push(fixture.pool.brokers[1]?.connectionPool.host ?? null)
    )
    const success = jest.spyOn(fixture.pool, 'onMetadataSuccess')
    await overlap(
      fixture,
      { brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }], topicMetadata: [] },
      { brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }], topicMetadata: [] }
    )
    expect(seen).toEqual(['second'])
    expect(success).toHaveBeenCalledTimes(1)
  })

  it('leave everything to the newer answer when its build finishes first, and report success for it alone', async () => {
    const { pool, seed, builder } = build()
    const builds = holdBuilds(builder)
    const seen = []
    pool.metadataCache.onRoutingChange(() =>
      seen.push(pool.brokers[1]?.connectionPool.host ?? null)
    )
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    seed.metadata
      .mockResolvedValueOnce({
        clusterId: 'first',
        brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }],
        topicMetadata: [],
      })
      .mockResolvedValueOnce({
        clusterId: 'second',
        brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }],
        topicMetadata: [],
      })
    const older = outcome(pool.refreshMetadata([], { isCurrent: () => true }))
    const newer = outcome(pool.refreshMetadata([], { isCurrent: () => true }))
    await settled()
    expect(builds).toHaveLength(2)
    expect(pool.metadata).toBeNull()

    builds[1].release()
    expect(await newer).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('second')
    expect(success).toHaveBeenCalledTimes(1)

    builds[0].release()
    expect(await older).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('second')
    expect(pool.brokers[1].connectionPool === builds[1].pool).toBe(true)
    expect(builds[0].pool.retired).toBe(true)
    expect(seen).toEqual(['second'])
    expect(success).toHaveBeenCalledTimes(1)
  })

  it('never write a rack over the one the newer reply set', async () => {
    const fixture = build()
    const peer = broker(2, 'peer')
    fixture.pool.brokers[2] = peer
    const peerNode = (rack) => ({ nodeId: 2, host: 'peer', port: 9092, rack })
    await overlap(
      fixture,
      {
        brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }, peerNode('older')],
        topicMetadata: [],
      },
      {
        brokers: [SEED_NODE, peerNode('newer'), { nodeId: 1, host: 'second', port: 9092 }],
        topicMetadata: [],
      }
    )
    expect(peer.connectionPool.rack).toBe('newer')
  })

  it('keep a broker discarded by the newer reply gone', async () => {
    const fixture = build()
    const peer = broker(3, 'peer')
    fixture.pool.brokers[3] = peer
    const listedPeer = { nodeId: 3, host: 'peer', port: 9092 }
    await overlap(
      fixture,
      {
        brokers: [SEED_NODE, { nodeId: 1, host: 'first', port: 9092 }, listedPeer],
        topicMetadata: [],
      },
      { brokers: [SEED_NODE, { nodeId: 1, host: 'second', port: 9092 }], topicMetadata: [] }
    )
    expect(fixture.pool.brokers[3]).toBeUndefined()
    expect(peer.retire).toHaveBeenCalledTimes(1)
  })

  it('leave the broker table on the endpoint the metadata names when both replies share a data event', async () => {
    const seam = createScriptedCluster({
      nodes: [
        { id: 0, host: 'seed', port: 9092 },
        { id: 1, host: 'first', port: 9092 },
        { id: 2, host: 'second', port: 9092 },
      ],
    })
    const cluster = new Cluster({
      ...seam.kafkaOptions({ requestTimeout: 100, enforceRequestTimeout: false }),
      logger: newLogger(),
      retry: { retries: 0 },
      metadataRecoveryStrategy: 'none',
      metadataMaxAge: 300000,
    })
    const built = jest.spyOn(cluster.connectionPoolBuilder, 'build')
    let gates = []
    try {
      await cluster.connect()
      gates = [seam.schedule.hold({ api: 'Metadata' }), seam.schedule.hold({ api: 'Metadata' })]
      const refreshing = Promise.all([
        cluster.brokerPool.refreshMetadata([], { isCurrent: () => true }),
        cluster.brokerPool.refreshMetadata([], { isCurrent: () => true }),
      ])
      const records = await Promise.all(gates.map((gate) => gate.arrived()))
      const frames = []
      records.forEach(({ correlationId }, index) => {
        const reply = {
          brokers: [
            { nodeId: 0, host: 'seed', port: 9092 },
            { nodeId: 1, host: index === 0 ? 'first' : 'second', port: 9092 },
          ],
          controllerId: 0,
          topics: [],
        }
        const header = responseHeaderVersion(apiKeys.Metadata, 13)
        const socket = { emit: (_, frame) => frames.push(frame) }
        respondWithFrame({ socket }, correlationId, header, metadataPair.buildResponse(reply))
      })
      seam.sockets()[0].emit('data', Buffer.concat(frames))
      await refreshing

      const { brokerPool } = cluster
      expect(brokerPool.metadata.brokers[1].host).toBe('second')
      expect(brokerPool.brokers[1].connectionPool.host).toBe('second')

      seam.down(1)
      const found = await cluster.findBroker({ nodeId: '1' })
      expect(found.connectionPool.host).toBe('second')

      const pools = await Promise.all(built.mock.results.map(({ value }) => value))
      const pool = (host) => pools.find((candidate) => candidate.host === host)
      expect(pool('first').retired).toBe(true)
      expect(pool('second').retired).toBe(false)
      seam.assertClean()
    } finally {
      await cluster.disconnect()
      for (const gate of gates) gate.release()
      for (const socket of seam.sockets()) socket.destroy()
    }
  })
})

describe('Public disconnect', () => {
  afterEach(() => jest.useRealTimers())

  it('drains the seed and its peers concurrently, so it waits one requestTimeout, not one per broker', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
    const seam = createScriptedCluster({
      nodes: [
        { id: 0, host: 'seed', port: 9092 },
        { id: 1, host: 'peer', port: 9092 },
      ],
    })
    const cluster = new Cluster({
      ...seam.kafkaOptions({ requestTimeout: 100, enforceRequestTimeout: false }),
      logger: newLogger(),
      retry: { retries: 0 },
    })
    let gates = []
    try {
      await cluster.connect()
      await cluster.refreshMetadata()
      const brokers = await Promise.all(['0', '1'].map((nodeId) => cluster.findBroker({ nodeId })))
      gates = brokers.map((_, node) => seam.schedule.hold({ api: 'Metadata', node }))
      const requests = brokers.map((held) => outcome(held.metadata([])))
      await Promise.all(gates.map((gate) => gate.arrived()))

      let disconnected = false
      const closing = cluster.disconnect().then(() => {
        disconnected = true
      })
      await jest.advanceTimersByTimeAsync(100)
      expect(disconnected).toBe(true)
      await closing

      for (const { error } of await Promise.all(requests)) {
        expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      }
      expect(seam.sockets().every((socket) => socket.destroyed)).toBe(true)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      for (const gate of gates) gate.release()
    }
  })

  it('disconnects a broker that is both the seed and in the table once', async () => {
    const { pool, seed } = build()
    const peer = broker(1)
    pool.brokers = { 0: seed, 5: seed, 1: peer }
    await pool.disconnect()
    expect(seed.disconnect).toHaveBeenCalledTimes(1)
    expect(peer.retire).toHaveBeenCalledTimes(1)
    expect(pool.brokers).toEqual({})
  })
})

const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

it.each(['coordinator lookup', 'cached broker', 'late endpoint'])(
  'leaves no orphan when %s overlaps another broker drain',
  async (kind) => {
    const seam = groupScript(
      createScriptedCluster({
        nodes: [
          { id: 0, host: 'node0', port: 9092 },
          { id: 1, host: 'node1', port: 9092 },
          { id: 2, host: 'node2', port: 9092 },
        ],
      }),
      { coordinator: 1 }
    )
    seam.answer('Metadata', () => ({
      brokers: seam.nodes
        .slice(0, 2)
        .map(({ id, ...node }) => ({ nodeId: id, ...node, rack: null })),
      topicMetadata: [],
    }))
    const cluster = new Cluster({
      ...seam.kafkaOptions({ requestTimeout: 1000 }),
      logger: newLogger(),
      retry: { retries: 0 },
    })
    let peer, newcomer, gate, heldRequest, closing
    try {
      await cluster.connect()
      await cluster.refreshMetadata()
      peer = await cluster.findBroker({ nodeId: 1 })
      const seed = cluster.brokerPool.seedBroker
      gate = seam.schedule.hold({ api: 'Metadata', node: 0 })
      heldRequest = seed.metadata([]).catch((error) => error)
      await gate.arrived()
      const disconnected = deferred()
      const disconnectPeer = peer.disconnect.bind(peer)
      peer.disconnect = async (...args) => {
        await disconnectPeer(...args)
        disconnected.resolve()
      }
      closing = cluster.disconnect()
      await disconnected.promise
      // Current code reconnects the captured peer through withBroker/findBroker here.
      // Refusing work while the seed drains is also a valid outcome.
      if (kind === 'coordinator lookup') {
        // Select the drained peer first; ordering must not depend on shuffle().
        const random = jest.spyOn(Math, 'random').mockReturnValue(0)
        try {
          await cluster.findGroupCoordinator({ groupId: 'probe-group' }).catch((error) => {
            expect(['KafkaJSBrokerNotFound', 'KafkaJSConnectionError']).toContain(
              (error.cause ?? error).name
            )
          })
        } finally {
          random.mockRestore()
        }
      } else if (kind === 'cached broker') {
        await peer.connect().catch((error) => {
          expect(error.name).toBe('KafkaJSConnectionError')
        })
      } else {
        await cluster.brokerPool.applyNodeEndpoints([
          { nodeId: 2, host: 'node2', port: 9092, rack: null },
        ])
        newcomer = await cluster.findBroker({ nodeId: 2 })
      }
      gate.release()
      gate = undefined
      await heldRequest
      await closing
      await cluster.disconnect()
      expect(seam.sockets().filter((socket) => !socket.destroyed)).toEqual([])
    } finally {
      gate?.release()
      await heldRequest
      await closing
      await cluster.disconnect()
      // Cleanup follows the assertion, so it cannot conceal an orphan.
      await peer?.disconnect()
      await newcomer?.disconnect()
    }
  }
)

describe('a closed producer lifecycle and an endpoint pool that finishes late', () => {
  const closed = new Error('the producer was closed')
  const HINT = {
    topic: 't',
    partition: 0,
    leaderId: 42,
    leaderEpoch: 2,
    nodeEndpoints: [{ nodeId: 42, host: 'late', port: 9092, rack: null }],
  }
  /** @returns {object} a fresh Metadata response, as the cache keeps what it is given */
  const metadata = () => ({
    throttleTime: 0,
    brokers: [],
    clusterId: 'cluster',
    controllerId: 0,
    topicMetadata: [
      {
        topicErrorCode: 0,
        topic: 't',
        topicId: null,
        isInternal: false,
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
      },
    ],
  })

  /**
   * An offline cluster with its seed built and t-0 cached at epoch 1, whose next pool build waits
   * for `release()`, and whose coordinator lookup answers node 42 at late:9092.
   * @returns {Promise<{ cluster: Cluster, pool: ConnectionPool, generation: Generation,
   *   release: () => void }>}
   */
  const lateCluster = async () => {
    const cluster = new Cluster({
      logger: newLogger(),
      clientId: 'client',
      brokers: ['seed:9092'],
      requestTimeout: 1000,
      retry: { retries: 0 },
      allowAutoTopicCreation: false,
      metadataMaxAge: 1000,
    })
    await cluster.brokerPool.createSeedBroker()
    cluster.metadataCache.update(metadata())
    cluster.findGroupCoordinatorMetadata = jest.fn(async () => ({
      coordinator: { nodeId: 42, host: 'late', port: 9092 },
    }))
    const pool = new ConnectionPool({ logger: newLogger(), host: 'late', port: 9092 })
    pool.retire = jest.fn()
    pool.destroy = jest.fn(async () => {})
    let release
    cluster.brokerPool.buildConnectionPool = jest.fn(
      () => new Promise((resolve) => (release = () => resolve(pool)))
    )
    return { cluster, pool, generation: new Generation(), release: () => release() }
  }

  it('a coordinator pool built after the close installs no broker and is retired unused', async () => {
    const { cluster, pool, generation, release } = await lateCluster()
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: 'group', fence: generation }))
      await settled()
      expect(cluster.brokerPool.buildConnectionPool).toHaveBeenCalledTimes(1)

      generation.close(closed)
      await cluster.disconnect({ drainTimeout: 0 })
      release()

      expect((await lookup).error).toBe(closed)
      expect(cluster.brokerPool.brokers[42]).toBeUndefined()
      expect(pool.retire).toHaveBeenCalledTimes(1)
      expect(pool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  it('a hint pool built after the close changes neither the broker table nor the cached epoch', async () => {
    const { cluster, pool, generation, release } = await lateCluster()
    try {
      const hint = outcome(cluster.applyLeaderHint(HINT, { fence: generation }))
      await settled()
      expect(cluster.brokerPool.buildConnectionPool).toHaveBeenCalledTimes(1)

      generation.close(closed)
      await cluster.disconnect({ drainTimeout: 0 })
      release()
      const { error } = await hint

      expect(cluster.leaderEpoch('t', 0)).toBe(1)
      expect(error).toBe(closed)
      expect(cluster.brokerPool.brokers[42]).toBeUndefined()
      expect(pool.retire).toHaveBeenCalledTimes(1)
      expect(pool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  it('a hint whose lifecycle closes as the endpoint work ends does not update the cached epoch', async () => {
    const { cluster, generation } = await lateCluster()
    const apply = cluster.brokerPool.applyNodeEndpoints.bind(cluster.brokerPool)
    cluster.brokerPool.applyNodeEndpoints = async (...args) => {
      await apply(...args)
      generation.close(closed)
    }
    try {
      const hint = await outcome(
        cluster.applyLeaderHint({ ...HINT, nodeEndpoints: [] }, { fence: generation })
      )

      expect(cluster.leaderEpoch('t', 0)).toBe(1)
      expect(hint.error).toBe(closed)
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  it('a hint on an open lifecycle still installs its broker and advances the epoch', async () => {
    const { cluster, pool, generation, release } = await lateCluster()
    try {
      const hint = outcome(cluster.applyLeaderHint(HINT, { fence: generation }))
      await settled()
      release()

      expect(await hint).toEqual({ value: true })
      expect(cluster.leaderEpoch('t', 0)).toBe(2)
      expect(cluster.brokerPool.brokers[42].connectionPool).toBe(pool)
      expect(pool.retire).not.toHaveBeenCalled()
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })
})

describe('Metadata answers across a rebootstrap', () => {
  /**
   * A Metadata answer of one cluster.
   * @param {string} clusterId which cluster answered
   * @param {object[]} [brokers] the brokers it lists
   * @param {string[]} [topics] the topics it lists, each with partition 0 led by node 0
   * @returns {object} a fresh decoded answer
   */
  const answer = (clusterId, brokers = [], topics = []) => ({
    throttleTime: 0,
    clusterId,
    controllerId: 0,
    brokers,
    topicMetadata: topics.map((topic) => ({
      topicErrorCode: 0,
      topic,
      topicId: null,
      isInternal: false,
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
  const OLD_PEER = { nodeId: 1, host: 'old-peer', port: 9092 }
  /** The seed the first rebootstrap builds, as the rebuilt cluster lists it. */
  const NEW_SEED = { nodeId: 0, host: 'seed-1', port: 9092 }
  const closed = () => new KafkaJSConnectionClosedError('Closed', { host: 'seed', port: 9092 })

  /**
   * Makes every Metadata request of `pool`, on its seed or on any broker built later, wait for the
   * test to answer it, and makes selection return the current seed, as it does once a rebootstrap
   * has emptied the broker table.
   * @param {BrokerPool} pool pool whose seed is a broker double
   * @returns {Array<{ resolve: Function, reject: Function }>} one entry per request in send order
   */
  const holdMetadata = (pool) => {
    const replies = []
    const held = () =>
      new Promise((resolve, reject) => {
        replies.push({ resolve, reject })
      })
    pool.seedBroker.metadata.mockImplementation(held)
    const create = pool.createBroker
    pool.createBroker = jest.fn((options) => {
      const created = create(options)
      created.metadata.mockImplementation(held)
      return created
    })
    pool.findConnectedBroker = jest.fn(async () => pool.seedBroker)
    return replies
  }

  /** @returns {object} the `build()` fixture with `replies`, see `holdMetadata` */
  const acrossRebootstrap = () => {
    const fixture = build()
    return { ...fixture, replies: holdMetadata(fixture.pool) }
  }

  /**
   * A Cluster whose pool is wired like `acrossRebootstrap`, so refreshes go through both of the
   * Cluster's shared refresh entry points.
   * @returns {{ cluster: Cluster, pool: BrokerPool, replies: object[] }}
   */
  const clusterAcrossRebootstrap = () => {
    const cluster = new Cluster({
      logger: newLogger(),
      clientId: 'client',
      brokers: ['seed:9092'],
      requestTimeout: 1000,
      retry: { retries: 0 },
      allowAutoTopicCreation: false,
    })
    const fixture = build()
    const pool = cluster.brokerPool
    pool.connectionPoolBuilder = fixture.builder
    pool.createBroker = fixture.pool.createBroker
    pool.seedBroker = fixture.seed
    pool.brokers = { 0: fixture.seed }
    return { cluster, pool, replies: holdMetadata(pool) }
  }

  /**
   * Runs one rebootstrap after the call's retrier has settled and before the call reads the
   * outcome, the last point at which a rebootstrap can overtake a refresh.
   * @param {BrokerPool} pool
   * @returns {void}
   */
  const rebootstrapAtSettlement = (pool) => {
    const retryFor = pool.retryFor.bind(pool)
    let done = false
    pool.retryFor = (fence) => {
      const retrier = retryFor(fence)
      return async (fn) => {
        try {
          return await retrier(fn)
        } finally {
          if (!done) {
            done = true
            await pool.rebootstrap('required')
          }
        }
      }
    }
  }

  it('a reply held across a rebootstrap installs nothing and the call installs the new seed answer', async () => {
    const { pool, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    expect(replies).toHaveLength(1)

    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    await settled()

    expect(pool.metadata).toBeNull()
    expect(pool.metadataExpireAt).toBeNull()
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    expect(pool.brokers).toEqual({})
    expect(replies).toHaveLength(2)
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.brokers).toEqual({ 0: pool.seedBroker })
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
  })

  it('a rebootstrap during broker selection asks the new seed instead of failing', async () => {
    const { pool, seed, replies } = acrossRebootstrap()
    pool.findConnectedBroker.mockImplementationOnce(async () => {
      await pool.rebootstrap('required')
      throw new KafkaJSConnectionError('Closed by the rebootstrap')
    })
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    expect(pool.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(replies).toHaveLength(1)
    expect(seed.metadata).not.toHaveBeenCalled()
    expect(pool.seedBroker).not.toBe(seed)
    expect(pool.seedBroker.metadata).toHaveBeenCalledTimes(1)
    replies[0].resolve(answer('new'))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
  })

  it('a connection closed by the rebootstrap starts no recovery clock, even when a second rebootstrap closes the retry', async () => {
    const { pool, replies } = acrossRebootstrap()
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    await pool.rebootstrap('required')
    replies[0].reject(closed())
    await settled()
    expect(failure).not.toHaveBeenCalled()
    expect(pool.metadataUnavailableSince).toBeNull()

    expect(replies).toHaveLength(2)
    await pool.rebootstrap('required')
    replies[1].reject(closed())
    const { error } = await refreshing
    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(failure).not.toHaveBeenCalled()
    expect(pool.metadataUnavailableSince).toBeNull()
    expect(replies).toHaveLength(2)
  })

  it('with retry: { retries: 0 } a refresh whose request a rebootstrap closed still installs once', async () => {
    const { pool, replies } = acrossRebootstrap()
    expect(pool.retryOptions).toEqual({ retries: 0 })
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    await pool.rebootstrap('required')
    replies[0].reject(closed())
    await settled()

    expect(replies).toHaveLength(2)
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
  })

  it('a rebootstrap while an answer pools are built installs nothing of it and asks the new seed again', async () => {
    const { pool, replies, builder } = acrossRebootstrap()
    const peerPool = new ConnectionPool({ logger: newLogger(), host: 'old-peer', port: 9092 })
    peerPool.destroy = jest.fn(async () => {})
    let releasePeer
    builder.build.mockImplementationOnce(
      () => new Promise((resolve) => (releasePeer = () => resolve(peerPool)))
    )
    const seen = []
    pool.metadataCache.onRoutingChange(() =>
      seen.push({
        cluster: pool.metadata?.clusterId ?? null,
        brokers: Object.keys(pool.brokers),
        endpoints: [...pool.setupMetadataEndpoints.keys()],
      })
    )
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER]))
    await settled()
    expect(pool.metadata).toBeNull()
    expect(pool.metadataExpireAt).toBeNull()
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(seen).toEqual([])
    const armed = pool.rebootstrapArmed

    await pool.rebootstrap('required')
    expect(seen).toEqual([{ cluster: null, brokers: [], endpoints: [] }])
    releasePeer()
    await settled()

    expect(pool.brokers[1]).toBeUndefined()
    expect(peerPool.retired).toBe(true)
    expect(pool.rebootstrapArmed).toBe(armed)
    expect(success).not.toHaveBeenCalled()
    expect(seen).toHaveLength(1)
    expect(replies).toHaveLength(2)
    replies[1].resolve(answer('new', [NEW_SEED]))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.brokers[1]).toBeUndefined()
    expect(success).toHaveBeenCalledTimes(1)
    expect(seen).toEqual([
      { cluster: null, brokers: [], endpoints: [] },
      { cluster: 'new', brokers: ['0'], endpoints: ['0'] },
    ])
  })

  it('setup endpoints of the old cluster are cleared by a rebootstrap', async () => {
    const { pool } = build()
    pool.setupMetadataEndpoints.set('1', 'old-peer:9092')
    await pool.rebootstrap('required')
    expect([...pool.setupMetadataEndpoints]).toEqual([])
  })

  it('a refresh started after a rebootstrap never joins one from before, and none resolves with nothing installed', async () => {
    const { pool, replies } = acrossRebootstrap()
    const unshared = jest.spyOn(pool, 'refreshMetadataUnshared')
    const installed = () => pool.metadata?.clusterId ?? null
    let firstSettled = false
    const first = outcome(pool.refreshMetadata([]).then(installed)).finally(() => {
      firstSettled = true
    })
    await settled()

    await pool.rebootstrap('required')
    const second = outcome(pool.refreshMetadata([]).then(installed))
    await settled()
    expect(unshared).toHaveBeenCalledTimes(2)
    expect(replies).toHaveLength(2)

    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    await settled()
    expect(replies).toHaveLength(3)
    replies[1].resolve(answer('new', [NEW_SEED]))
    expect(await second).toEqual({ value: 'new' })
    await settled()
    expect(firstSettled).toBe(false)

    replies[2].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await first).toEqual({ value: 'new' })
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
  })

  it('routing listeners run only after the answer brokers are installed', async () => {
    const { pool, seed } = build()
    seed.metadata.mockResolvedValue({
      brokers: [SEED_NODE, { nodeId: 1, host: 'next', port: 9092 }],
      topicMetadata: [],
    })
    const seen = []
    pool.metadataCache.onRoutingChange(() => seen.push(pool.brokers[1]?.connectionPool.host))
    await pool.refreshMetadata([])
    expect(seen).toEqual(['next'])
  })

  it('a refresh whose own rebootstrap selected the new seed installs that answer once', async () => {
    const { pool, replies } = acrossRebootstrap()
    pool.findConnectedBroker.mockImplementationOnce(async () => {
      await pool.rebootstrap('all-brokers-unavailable')
      return pool.seedBroker
    })
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    expect(replies).toHaveLength(1)
    replies[0].resolve(answer('new'))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(replies).toHaveLength(1)
    expect(pool.findConnectedBroker).toHaveBeenCalledTimes(1)
  })

  it('a caller refusal after a rebootstrap wins over asking the new seed', async () => {
    const { pool, replies } = acrossRebootstrap()
    const refusal = new Error('caller left')
    let refused = null
    const refreshing = outcome(pool.refreshMetadata([], { guard: () => refused }))
    await settled()

    await pool.rebootstrap('required')
    refused = refusal
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))

    expect((await refreshing).error).toBe(refusal)
    expect(replies).toHaveLength(1)
    expect(pool.metadata).toBeNull()
  })

  it('a caller refusal that arrives as the old answer is set aside wins over the reissue', async () => {
    const { pool, replies } = acrossRebootstrap()
    const refusal = new Error('caller left')
    let refused = null
    const refreshing = outcome(pool.refreshMetadata([], { guard: () => refused }))
    await settled()
    // The caller leaves during the reissue's broker selection: the old answer was judged with the
    // guard open, and no second request is sent.
    pool.findConnectedBroker.mockImplementationOnce(async () => {
      refused = refusal
      return pool.seedBroker
    })

    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))

    expect((await refreshing).error).toBe(refusal)
    expect(pool.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(replies).toHaveLength(1)
    expect(pool.metadata).toBeNull()
  })

  it('the one reissue spends no retry: ordinary failures before and after it share the call retry count', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    try {
      const { pool, replies } = acrossRebootstrap()
      pool.retryOptions = { retries: 1, initialRetryTime: 100, maxRetryTime: 100 }
      const leaderless = () =>
        Object.assign(new Error('Leader not available'), {
          type: 'LEADER_NOT_AVAILABLE',
          retriable: true,
        })
      const refreshing = outcome(pool.refreshMetadata([]))
      await settled()
      replies[0].reject(leaderless())
      await settled()
      expect(replies).toHaveLength(1)

      await jest.advanceTimersByTimeAsync(1000)
      expect(replies).toHaveLength(2)
      await pool.rebootstrap('required')
      replies[1].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
      await settled()
      expect(replies).toHaveLength(3)
      replies[2].reject(leaderless())

      const { error } = await refreshing
      expect(error?.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(error.retryCount).toBe(1)
      await jest.advanceTimersByTimeAsync(1000)
      expect(replies).toHaveLength(3)
      expect(pool.metadata).toBeNull()
    } finally {
      jest.useRealTimers()
    }
  })

  it('a rebootstrap after the last answer was installed and before the call settles rejects it as retriable', async () => {
    const { pool, seed, replies } = acrossRebootstrap()
    rebootstrapAtSettlement(pool)
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    replies[0].resolve(answer('old', [SEED_NODE], ['old-topic']))
    const { error } = await refreshing

    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    expect(success).not.toHaveBeenCalled()
    expect(failure).not.toHaveBeenCalled()
    expect(replies).toHaveLength(1)
  })

  it('a rebootstrap after the last request failed and before the call settles starts no recovery from that failure', async () => {
    const { pool, builder, replies } = acrossRebootstrap()
    rebootstrapAtSettlement(pool)
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    replies[0].reject(
      Object.assign(new Error('Rebootstrap required'), {
        type: 'REBOOTSTRAP_REQUIRED',
        retriable: false,
      })
    )
    const { error } = await refreshing

    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(failure).not.toHaveBeenCalled()
    // One seed was built, by the rebootstrap at settlement; the stale 129 started no second one.
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.metadataUnavailableSince).toBeNull()
  })

  it('a second rebootstrap during the reissue sets its late answer aside and rejects as retriable', async () => {
    const { pool, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    await settled()
    expect(replies).toHaveLength(2)

    await pool.rebootstrap('required')
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
    const { error } = await refreshing

    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(-1)
    expect(replies).toHaveLength(2)
  })

  it('a lifecycle closed while the reissue is outstanding rejects with its close error and installs nothing', async () => {
    const { pool, replies } = acrossRebootstrap()
    const generation = new Generation()
    const closedBy = new Error('producer closed')
    const refreshing = outcome(pool.refreshMetadata([], { fence: generation }))
    await settled()
    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    await settled()
    expect(replies).toHaveLength(2)

    generation.close(closedBy)
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))

    expect((await refreshing).error).toBe(closedBy)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(-1)
  })

  it('a disconnect while the reissue is outstanding resolves as before and installs nothing', async () => {
    const { pool, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    await settled()
    expect(replies).toHaveLength(2)

    await pool.disconnect()
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))

    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(-1)
  })

  it('an old seed connect that fails after a rebootstrap does not rotate away from the new seed', async () => {
    const { pool, seed, builder } = build()
    let fail
    seed.isConnected = () => false
    seed.connect = jest.fn(() => new Promise((resolve, reject) => (fail = reject)))
    const connecting = outcome(pool.connect())
    await settled()
    expect(seed.connect).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    const installed = pool.seedBroker
    fail(new KafkaJSConnectionError('Closed by the rebootstrap'))
    await connecting

    expect(pool.seedBroker).toBe(installed)
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(installed.retire).not.toHaveBeenCalled()
  })

  it('a node scan that a rebootstrap overtook does not run the all-brokers-unavailable rebootstrap', async () => {
    const { pool, builder } = build()
    delete pool.findConnectedBroker
    const old = broker(1)
    let fail
    old.isConnected = () => false
    old.connect = jest.fn(() => new Promise((resolve, reject) => (fail = reject)))
    pool.brokers = { 1: old }
    pool.rebootstrapArmed = true
    const finding = outcome(pool.findConnectedBroker())
    await settled()
    expect(old.connect).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    // An answer of the rebuilt cluster arms the trigger again.
    pool.rebootstrapArmed = true
    const installed = pool.seedBroker
    fail(new KafkaJSConnectionError('Closed by the rebootstrap'))

    expect((await finding).value).toBe(installed)
    expect(pool.seedBroker).toBe(installed)
    expect(builder.build).toHaveBeenCalledTimes(1)
  })

  it('an old answer ready as the new seed is installed is set aside, and the rebootstrap completes', async () => {
    const { pool, seed, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    expect(replies).toHaveLength(1)
    seed.retire.mockImplementation(async () => {
      replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    })

    await pool.rebootstrap('required')
    expect(pool.seedBroker).not.toBe(seed)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    await settled()
    expect(replies).toHaveLength(2)

    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
  })

  it.each(['refreshMetadata', 'refreshMetadataIfNecessary'])(
    'Cluster#%s: a pattern refresh begun before a rebootstrap is not joined by a later one and settles with the later one once displaced',
    async (method) => {
      const { cluster, pool, replies } = clusterAcrossRebootstrap()
      const subscription = new Subscription()
      subscription.add({ topics: [/^t-/], policy: { kind: 'latest' } })
      const observer = jest.fn()
      const refresh = () =>
        outcome(cluster[method]({ metadataScope: subscription, onMetadata: observer }))
      let firstSettled = false
      const first = refresh().finally(() => {
        firstSettled = true
      })
      await settled()
      expect(replies).toHaveLength(1)

      await pool.rebootstrap('required')
      const second = refresh()
      await settled()
      expect(replies).toHaveLength(2)

      replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['t-old']))
      await settled()
      expect(firstSettled).toBe(false)
      expect(pool.metadata).toBeNull()
      expect(pool.metadataCache.leader('t-old', 0)).toBe(-1)
      expect(observer).not.toHaveBeenCalled()

      replies[1].resolve(answer('new', [NEW_SEED], ['t-new']))
      expect(await second).toEqual({ value: undefined })
      expect(await first).toEqual({ value: undefined })
      expect(pool.metadata.clusterId).toBe('new')
      expect(pool.metadataCache.leader('t-new', 0)).toBe(0)
      expect(observer.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['new'])
      expect(replies).toHaveLength(2)
    }
  )

  it('a pattern refresh displaced across a rebootstrap fails with the later refresh failure', async () => {
    const { cluster, pool, replies } = clusterAcrossRebootstrap()
    const subscription = new Subscription()
    subscription.add({ topics: [/^t-/], policy: { kind: 'latest' } })
    const observer = jest.fn()
    const refresh = () =>
      outcome(cluster.refreshMetadata({ metadataScope: subscription, onMetadata: observer }))
    const first = refresh()
    await settled()
    await pool.rebootstrap('required')
    const second = refresh()
    await settled()
    expect(replies).toHaveLength(2)

    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['t-old']))
    await settled()
    const refused = Object.assign(new Error('Cluster authorization failed'), { retriable: false })
    replies[1].reject(refused)
    expect((await second).error).toBe(refused)
    expect((await first).error).toBe(refused)
    expect(observer).not.toHaveBeenCalled()
    expect(replies).toHaveLength(2)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('t-old', 0)).toBe(-1)
  })

  it('a pattern refresh displaced by one with another observer asks once more after it, so each observer sees one current answer', async () => {
    const { cluster, pool, replies } = clusterAcrossRebootstrap()
    const subscription = new Subscription()
    subscription.add({ topics: [/^t-/], policy: { kind: 'latest' } })
    const declaring = jest.fn()
    const discovering = jest.fn()
    const refresh = (onMetadata) =>
      outcome(cluster.refreshMetadata({ metadataScope: subscription, onMetadata }))
    const first = refresh(declaring)
    await settled()
    await pool.rebootstrap('required')
    const second = refresh(discovering)
    await settled()
    expect(replies).toHaveLength(2)

    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['t-old']))
    await settled()
    expect(replies).toHaveLength(2)
    replies[1].resolve(answer('new', [NEW_SEED], ['t-new']))
    expect(await second).toEqual({ value: undefined })
    await settled()
    expect(replies).toHaveLength(3)
    replies[2].resolve(answer('newer', [NEW_SEED], ['t-new', 't-later']))

    expect(await first).toEqual({ value: undefined })
    expect(discovering.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['new'])
    expect(declaring.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['newer'])
    expect(pool.metadataCache.leader('t-later', 0)).toBe(0)
  })

  it('an answer whose pool build fails installs nothing, wakes no routing listener and keeps the installed answer', async () => {
    const { pool, seed, builder } = build()
    pool.metadataMaxAge = 300000
    const installed = { clusterId: 'installed', brokers: [SEED_NODE], topicMetadata: [] }
    seed.metadata.mockResolvedValueOnce(installed)
    await pool.refreshMetadata([])
    const expireAt = pool.metadataExpireAt
    const seen = []
    pool.metadataCache.onRoutingChange(() => seen.push(pool.metadata?.clusterId ?? null))
    const failure = Object.assign(new Error('build failed'), { retriable: false })
    builder.build.mockImplementationOnce(async () => {
      throw failure
    })
    seed.metadata.mockResolvedValueOnce({
      clusterId: 'next',
      brokers: [SEED_NODE, { nodeId: 1, host: 'next', port: 9092 }],
      topicMetadata: [],
    })

    expect((await outcome(pool.refreshMetadata([], { isCurrent: () => true }))).error).toBe(failure)
    expect(pool.metadata).toBe(installed)
    expect(pool.metadataExpireAt).toBe(expireAt)
    expect(pool.brokers).toEqual({ 0: seed })
    expect(seen).toEqual([])
  })

  it('a caller refusal while the answer pools are built installs nothing and closes the pools unused', async () => {
    const { pool, seed, builder } = build()
    const builds = holdBuilds(builder)
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    const seen = []
    pool.metadataCache.onRoutingChange(() => seen.push(pool.metadata?.clusterId ?? null))
    const refusal = new Error('caller left')
    let refused = null
    seed.metadata.mockResolvedValueOnce({
      clusterId: 'next',
      brokers: [SEED_NODE, { nodeId: 1, host: 'next', port: 9092 }],
      topicMetadata: [],
    })
    const refreshing = outcome(pool.refreshMetadata([], { guard: () => refused }))
    await settled()
    expect(builds).toHaveLength(1)
    expect(pool.metadata).toBeNull()

    refused = refusal
    builds[0].release()
    expect((await refreshing).error).toBe(refusal)
    expect(pool.metadata).toBeNull()
    expect(pool.brokers).toEqual({ 0: seed })
    expect(builds[0].pool.retired).toBe(true)
    expect(seen).toEqual([])
    expect(success).not.toHaveBeenCalled()
  })

  it.each(['guard', 'lifecycle'])(
    'a %s refusal after the reissued answer is installed and before the call settles wins, and reports no success',
    async (kind) => {
      const { pool, replies } = acrossRebootstrap()
      const success = jest.spyOn(pool, 'onMetadataSuccess')
      const failure = jest.spyOn(pool, 'onMetadataFailure')
      const refusal = new Error('caller left')
      let refused = null
      const generation = new Generation()
      const retryFor = pool.retryFor.bind(pool)
      pool.retryFor = (fence) => {
        const retrier = retryFor(fence)
        return async (fn) => {
          try {
            return await retrier(fn)
          } finally {
            refused = refusal
            if (kind === 'lifecycle') generation.close(refusal)
          }
        }
      }
      const options = kind === 'guard' ? { guard: () => refused } : { fence: generation }
      const refreshing = outcome(pool.refreshMetadata([], options))
      await settled()
      await pool.rebootstrap('required')
      replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
      await settled()
      expect(replies).toHaveLength(2)
      replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))

      expect((await refreshing).error).toBe(refusal)
      expect(success).not.toHaveBeenCalled()
      expect(failure).not.toHaveBeenCalled()
    }
  )

  it('a node connection that completes after a rebootstrap is not returned by selection', async () => {
    const { pool } = build()
    delete pool.findConnectedBroker
    const old = broker(1)
    let connected
    old.isConnected = () => false
    old.connect = jest.fn(() => new Promise((resolve) => (connected = resolve)))
    pool.brokers = { 1: old }
    const finding = outcome(pool.findConnectedBroker())
    await settled()
    expect(old.connect).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    const installed = pool.seedBroker
    connected()

    expect((await finding).value).toBe(installed)
  })

  it.each([
    ['a closed connection', () => new KafkaJSConnectionError('Closed by the rebootstrap')],
    [
      'an illegal SASL state',
      () =>
        Object.assign(new Error('Illegal SASL state'), {
          type: 'ILLEGAL_SASL_STATE',
          retriable: true,
        }),
    ],
  ])(
    'a node connection that fails with %s after a rebootstrap is neither disconnected again nor rebuilt',
    async (_, failure) => {
      const { pool, builder } = build()
      delete pool.findConnectedBroker
      const old = broker(1)
      let fail
      old.isConnected = () => false
      old.connect = jest.fn(() => new Promise((resolve, reject) => (fail = reject)))
      old.retire.mockImplementation(() => new Promise(() => {}))
      old.disconnect.mockImplementation(() => new Promise(() => {}))
      pool.brokers = { 1: old }
      const finding = outcome(pool.findConnectedBroker())
      await settled()
      expect(old.connect).toHaveBeenCalledTimes(1)

      await pool.rebootstrap('required')
      const installed = pool.seedBroker
      fail(failure())
      await settled()

      expect(old.disconnect).not.toHaveBeenCalled()
      expect(builder.build).toHaveBeenCalledTimes(1)
      expect((await finding).value).toBe(installed)
    }
  )

  it('a seed connection that completes after a rebootstrap fulfils only once the new seed connected', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    try {
      const { pool, seed } = build()
      pool.retryOptions = { retries: 1, initialRetryTime: 100, maxRetryTime: 100 }
      pool.brokers = {}
      let connected
      seed.isConnected = () => false
      seed.versions = { old: true }
      seed.connect = jest.fn(() => new Promise((resolve) => (connected = resolve)))
      let connectingSettled = false
      const connecting = outcome(pool.connect()).finally(() => {
        connectingSettled = true
      })
      await settled()
      expect(seed.connect).toHaveBeenCalledTimes(1)

      const cold = broker(0, 'seed-1')
      cold.isConnected = () => false
      cold.versions = { new: true }
      cold.connect = jest.fn(async () => {})
      pool.createBroker.mockImplementationOnce(() => cold)
      await pool.rebootstrap('required')
      expect(pool.seedBroker).toBe(cold)
      connected()
      await settled()
      expect(connectingSettled).toBe(false)
      expect(pool.versions).toBeNull()

      await jest.advanceTimersByTimeAsync(1000)
      expect(await connecting).toEqual({ value: undefined })
      expect(cold.connect).toHaveBeenCalledTimes(1)
      expect(pool.versions).toEqual({ new: true })
    } finally {
      jest.useRealTimers()
    }
  })

  describe('a displaced pattern refresh', () => {
    /** @returns {Subscription} a pattern subscription */
    const patterns = () => {
      const subscription = new Subscription()
      subscription.add({ topics: [/^t-/], policy: { kind: 'latest' } })
      return subscription
    }

    it('stops waiting for the newer refresh when its own lifecycle closes', async () => {
      const { pool, replies } = acrossRebootstrap()
      const subscription = patterns()
      const observer = jest.fn()
      const generation = new Generation()
      const closedBy = new Error('run stopped')
      let firstSettled = false
      const first = outcome(
        pool.refreshMetadata([], {
          metadataScope: subscription,
          onMetadata: observer,
          fence: generation,
        })
      ).finally(() => {
        firstSettled = true
      })
      await settled()
      const second = outcome(
        pool.refreshMetadata([], { metadataScope: subscription, onMetadata: observer })
      )
      await settled()
      expect(replies).toHaveLength(2)

      replies[0].resolve(answer('first', [SEED_NODE], ['t-first']))
      await settled()
      expect(firstSettled).toBe(false)
      generation.close(closedBy)
      await settled()
      expect(firstSettled).toBe(true)
      expect((await first).error).toBe(closedBy)

      replies[1].resolve(answer('second', [SEED_NODE], ['t-second']))
      expect(await second).toEqual({ value: undefined })
      expect(observer.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['second'])
      expect(replies).toHaveLength(2)
    })

    it('asks once more when the newer refresh with another observer completed before its own answer arrived', async () => {
      const { pool, replies } = acrossRebootstrap()
      const subscription = patterns()
      const declaring = jest.fn()
      const discovering = jest.fn()
      const first = outcome(
        pool.refreshMetadata([], { metadataScope: subscription, onMetadata: declaring })
      )
      await settled()
      const second = outcome(
        pool.refreshMetadata([], { metadataScope: subscription, onMetadata: discovering })
      )
      await settled()
      expect(replies).toHaveLength(2)
      replies[1].resolve(answer('second', [SEED_NODE], ['t-second']))
      expect(await second).toEqual({ value: undefined })

      replies[0].resolve(answer('first', [SEED_NODE], ['t-first']))
      await settled()
      expect(replies).toHaveLength(3)
      replies[2].resolve(answer('third', [SEED_NODE], ['t-third']))
      expect(await first).toEqual({ value: undefined })
      expect(discovering.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['second'])
      expect(declaring.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['third'])
    })

    it('asks the rebuilt cluster itself when a rebootstrap cleared the answer of the newer refresh', async () => {
      const { pool, replies } = acrossRebootstrap()
      const subscription = patterns()
      const observer = jest.fn()
      const refresh = () =>
        outcome(pool.refreshMetadata([], { metadataScope: subscription, onMetadata: observer }))
      const first = refresh()
      await settled()
      const second = refresh()
      await settled()
      expect(replies).toHaveLength(2)
      replies[1].resolve(answer('second', [SEED_NODE], ['t-second']))
      expect(await second).toEqual({ value: undefined })

      await pool.rebootstrap('required')
      replies[0].resolve(answer('first', [SEED_NODE], ['t-first']))
      await settled()
      expect(replies).toHaveLength(3)
      replies[2].resolve(answer('new', [NEW_SEED], ['t-new']))
      expect(await first).toEqual({ value: undefined })
      expect(observer.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['second', 'new'])
      expect(pool.metadataCache.leader('t-new', 0)).toBe(0)
    })
  })

  describe('broker selection', () => {
    it('a node connection that a disconnect overtook is not returned by its lookup', async () => {
      const { pool } = build()
      delete pool.findConnectedBroker
      const peer = broker(1)
      let connected
      peer.isConnected = () => false
      peer.connect = jest.fn(() => new Promise((resolve) => (connected = resolve)))
      pool.brokers = { 0: pool.seedBroker, 1: peer }
      const finding = outcome(pool.findBroker({ nodeId: 1 }))
      await settled()
      expect(peer.connect).toHaveBeenCalledTimes(1)

      await pool.disconnect()
      connected()
      const { error } = await finding
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.retriable).toBe(true)
    })

    it('a seed selection that a disconnect overtook after its last check is not returned', async () => {
      const { pool, seed } = build()
      delete pool.findConnectedBroker
      pool.brokers = {}
      seed.isConnected = () => false
      seed.versions = { connected: true }
      seed.connect = jest.fn(async () => {})
      let versions = null
      let disconnecting = null
      // The seed connection publishes its versions right after its last check, so the disconnect
      // lands between that check and the return of the selection.
      Object.defineProperty(pool, 'versions', {
        configurable: true,
        get: () => versions,
        set: (value) => {
          versions = value
          if (value === seed.versions && !disconnecting) disconnecting = pool.disconnect()
        },
      })

      const { error } = await outcome(pool.findConnectedBroker())
      expect(disconnecting).not.toBeNull()
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.retriable).toBe(true)
      await disconnecting
    })

    it('a seed fallback whose connect finds a peer connected returns that peer, not the cold seed', async () => {
      const { pool, seed } = build()
      delete pool.findConnectedBroker
      const peer = broker(1)
      let up = false
      peer.isConnected = () => up
      // Another caller connects the peer while this attempt fails.
      peer.connect = jest.fn(async () => {
        up = true
        throw new KafkaJSConnectionError('Connection refused')
      })
      seed.isConnected = () => false
      seed.connect = jest.fn(async () => {})
      pool.brokers = { 1: peer }

      const { value } = await outcome(pool.findConnectedBroker())
      expect(value === peer).toBe(true)
      expect(seed.connect).not.toHaveBeenCalled()
    })

    it('a node whose illegal SASL state cleanup a rebootstrap overtook builds no replacement pool and fails retriably', async () => {
      const { pool, builder } = build()
      delete pool.findConnectedBroker
      const old = broker(1)
      let disconnected
      old.isConnected = () => false
      old.connect = jest.fn(async () => {
        throw Object.assign(new Error('Illegal SASL state'), {
          type: 'ILLEGAL_SASL_STATE',
          retriable: true,
        })
      })
      old.disconnect.mockImplementation(() => new Promise((resolve) => (disconnected = resolve)))
      pool.brokers = { 0: pool.seedBroker, 1: old }
      const finding = outcome(pool.findBroker({ nodeId: 1 }))
      await settled()
      expect(old.disconnect).toHaveBeenCalledTimes(1)

      await pool.rebootstrap('required')
      expect(builder.build).toHaveBeenCalledTimes(1)
      disconnected()
      const { error } = await finding
      expect(builder.build).toHaveBeenCalledTimes(1)
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.retriable).toBe(true)
    })

    it('a coordinator lookup whose broker selection was superseded asks again within its retries', async () => {
      jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
      try {
        const cluster = new Cluster({
          logger: newLogger(),
          clientId: 'client',
          brokers: ['seed:9092'],
          requestTimeout: 1000,
          retry: { retries: 1, initialRetryTime: 100, maxRetryTime: 100 },
        })
        const coordinator = broker(0, 'seed')
        cluster.brokerPool.brokers = { 0: coordinator }
        cluster.findGroupCoordinatorMetadata = jest.fn(async () => ({
          coordinator: { nodeId: 0, host: 'seed', port: 9092 },
        }))
        const superseded = new KafkaJSConnectionError(
          'The broker was replaced while it was selected',
          { code: BrokerPool.SELECTION_SUPERSEDED }
        )
        cluster.findBroker = jest
          .fn()
          .mockRejectedValueOnce(superseded)
          .mockResolvedValueOnce(coordinator)

        const finding = outcome(cluster.findGroupCoordinator({ groupId: 'group' }))
        await jest.advanceTimersByTimeAsync(1000)
        expect((await finding).value === coordinator).toBe(true)
        expect(cluster.findBroker).toHaveBeenCalledTimes(2)
      } finally {
        jest.useRealTimers()
      }
    })
  })
})

describe('One install step per metadata answer', () => {
  const PEER = { nodeId: 1, host: 'peer', port: 9092 }

  /**
   * One topic row: healthy with partition 0 led by the seed, or refused with no partitions.
   * @param {string} topic
   * @param {object} [options]
   * @param {number} [options.code=0] the topic error code
   * @param {string|null} [options.topicId=null]
   * @param {number} [options.leaderEpoch=1]
   * @returns {object} a decoded topic row
   */
  const row = (topic, { code = 0, topicId = null, leaderEpoch = 1 } = {}) => ({
    topicErrorCode: code,
    topic,
    topicId,
    isInternal: false,
    partitionMetadata: code
      ? []
      : [
          {
            partitionErrorCode: 0,
            partitionId: 0,
            leader: 0,
            leaderEpoch,
            replicas: [0],
            isr: [0],
            offlineReplicas: [],
          },
        ],
  })

  /**
   * A Cluster whose pool sends to the seed double of `build()` and builds pools with its builder.
   * @param {object} [options] Cluster options over the defaults
   * @returns {{ cluster: Cluster, pool: BrokerPool, seed: Broker, builder: object }}
   */
  const clusterWith = (options = {}) => {
    const cluster = new Cluster({
      logger: newLogger(),
      clientId: 'client',
      brokers: ['seed:9092'],
      requestTimeout: 1000,
      retry: { retries: 0 },
      allowAutoTopicCreation: false,
      ...options,
    })
    const fixture = build()
    const pool = cluster.brokerPool
    pool.connectionPoolBuilder = fixture.builder
    pool.createBroker = fixture.pool.createBroker
    pool.seedBroker = fixture.seed
    pool.brokers = { 0: fixture.seed }
    pool.findConnectedBroker = async () => fixture.seed
    return { cluster, pool, seed: fixture.seed, builder: fixture.builder }
  }

  /**
   * Records the broker table each routing notice sees.
   * @param {BrokerPool} pool
   * @returns {string[]} one `id:host` list per notice
   */
  const watchRouting = (pool) => {
    const seen = []
    pool.metadataCache.onRoutingChange(() =>
      seen.push(
        Object.entries(pool.brokers)
          .map(([id, entry]) => `${id}:${entry.connectionPool.host}`)
          .join(',')
      )
    )
    return seen
  }

  describe('producer targets', () => {
    it('a target forgotten and declared again while the answer pools are built is not installed from that answer', async () => {
      const { cluster, pool, seed, builder } = clusterWith({ metadataMaxIdle: 60000 })
      seed.metadata.mockResolvedValueOnce({ brokers: [SEED_NODE], topicMetadata: [row('orders')] })
      await cluster.addTargetTopic('orders')
      expect(cluster.findTopicPartitionMetadata('orders')).toHaveLength(1)

      const builds = holdBuilds(builder)
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE, PEER],
        topicMetadata: [row('orders', { leaderEpoch: 2 })],
      })
      const refreshing = outcome(cluster.refreshMetadata())
      await settled()
      expect(builds).toHaveLength(1)

      cluster.forgetTargetTopic('orders')
      cluster.targetTopics.add('orders')
      cluster.touchTargetTopic('orders')
      builds[0].release()

      expect(await refreshing).toEqual({ value: undefined })
      expect(pool.brokers[1].connectionPool.host).toBe('peer')
      expect(cluster.findTopicPartitionMetadata('orders')).toEqual([])
      expect(cluster.metadataCache.leaderEpoch('orders', 0)).toBe(-1)
    })

    it('an earlier target the answer refuses keeps its target and routing when the answer is not installed', async () => {
      const { cluster, pool, seed, builder } = clusterWith({ metadataMaxIdle: 60000 })
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders'), row('gone')],
      })
      await cluster.addMultipleTargetTopics(['orders', 'gone'])
      const builds = holdBuilds(builder)
      const seen = watchRouting(pool)
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE, PEER],
        topicMetadata: [row('orders'), row('gone', { code: 3 })],
      })
      const refusal = new Error('caller left')
      let refused = null
      const refreshing = outcome(cluster.refreshMetadata({ guard: () => refused }))
      await settled()
      expect(builds).toHaveLength(1)

      refused = refusal
      builds[0].release()
      expect((await refreshing).error).toBe(refusal)
      expect(cluster.targetTopics.has('gone')).toBe(true)
      expect(cluster.findTopicPartitionMetadata('gone')).toHaveLength(1)
      expect(seen).toEqual([])
    })

    it('an answer that drops an earlier target drops it at its install and wakes routing listeners once', async () => {
      const { cluster, pool, seed, builder } = clusterWith({ metadataMaxIdle: 60000 })
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders'), row('gone')],
      })
      await cluster.addMultipleTargetTopics(['orders', 'gone'])
      const builds = holdBuilds(builder)
      const seen = watchRouting(pool)
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE, PEER],
        topicMetadata: [row('orders'), row('gone', { code: 3 })],
      })
      const refreshing = outcome(cluster.refreshMetadata())
      await settled()
      expect(builds).toHaveLength(1)
      expect(cluster.targetTopics.has('gone')).toBe(true)

      builds[0].release()
      expect(await refreshing).toEqual({ value: undefined })
      expect(cluster.targetTopics.has('gone')).toBe(false)
      expect(cluster.findTopicPartitionMetadata('gone')).toEqual([])
      expect(cluster.findTopicPartitionMetadata('orders')).toHaveLength(1)
      expect(seen).toEqual(['0:seed,1:peer'])
    })

    it('a refused new target beside a droppable earlier one fails the add and drops nothing', async () => {
      const { cluster, seed } = clusterWith({ metadataMaxIdle: 60000 })
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders'), row('gone')],
      })
      await cluster.addMultipleTargetTopics(['orders', 'gone'])
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders'), row('gone', { code: 3 }), row('fresh', { code: 29 })],
      })

      const { error } = await outcome(cluster.addTargetTopic('fresh'))
      expect(error).toMatchObject({ type: 'TOPIC_AUTHORIZATION_FAILED' })
      expect(cluster.targetTopics.has('fresh')).toBe(false)
      expect(cluster.targetTopics.has('gone')).toBe(true)
      expect(cluster.findTopicPartitionMetadata('gone')).toHaveLength(1)
    })
  })

  describe('callbacks and the installed receipt', () => {
    const FIRST_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
    const SECOND_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'

    it('a topic status is reported once the answer brokers, metadata and freshness are installed', async () => {
      const snapshots = []
      let pool = null
      const fixture = clusterWith({
        onTopicMetadataStatus: ({ topic, status }) =>
          snapshots.push({
            topic,
            status,
            peer: pool.brokers[1]?.connectionPool.host ?? null,
            routed: pool.metadata.topicMetadata.map((entry) => entry.topic),
            fresh: pool.metadataExpireAt !== null,
          }),
      })
      const { cluster, seed, builder } = fixture
      pool = fixture.pool
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('healthy'), row('doomed')],
      })
      await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
      const builds = holdBuilds(builder)
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE, PEER],
        topicMetadata: [row('healthy'), row('doomed', { code: 3 })],
      })
      const refreshing = outcome(cluster.refreshMetadata())
      await settled()
      expect(builds).toHaveLength(1)
      expect(snapshots).toEqual([])

      builds[0].release()
      expect(await refreshing).toEqual({ value: undefined })
      expect(snapshots).toEqual([
        { topic: 'doomed', status: 'unavailable', peer: 'peer', routed: ['healthy'], fresh: true },
      ])
    })

    it('a topic-recreated listener that throws a non-Error neither fails the refresh nor stops its routing notice', async () => {
      const emitter = new InstrumentationEventEmitter()
      const thrown = null
      emitter.addListener(CLUSTER_TOPIC_RECREATED, () => {
        throw thrown
      })
      const { cluster, pool, seed } = clusterWith({ instrumentationEmitter: emitter })
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders', { topicId: FIRST_ID })],
      })
      await cluster.addTargetTopic('orders')
      const seen = watchRouting(pool)
      pool.metadataExpireAt = null
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders', { topicId: SECOND_ID })],
      })

      expect(await outcome(cluster.refreshMetadata())).toEqual({ value: undefined })
      expect(cluster.topicId('orders')).toBe(SECOND_ID)
      expect(pool.metadataExpireAt).not.toBeNull()
      expect(seen).toEqual(['0:seed'])
    })

    it('a topic-recreated listener that closes the refreshing lifecycle leaves the install complete and fresh, and routing listeners are still woken', async () => {
      const emitter = new InstrumentationEventEmitter()
      const generation = new Generation()
      const closedBy = new Error('producer closed')
      emitter.addListener(CLUSTER_TOPIC_RECREATED, () => generation.close(closedBy))
      const { cluster, pool, seed } = clusterWith({ instrumentationEmitter: emitter })
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders', { topicId: FIRST_ID })],
      })
      await cluster.addTargetTopic('orders')
      const seen = watchRouting(pool)
      pool.metadataExpireAt = null
      const observer = jest.fn()
      seed.metadata.mockResolvedValueOnce({
        brokers: [SEED_NODE],
        topicMetadata: [row('orders', { topicId: SECOND_ID })],
      })

      const { error } = await outcome(
        pool.refreshMetadata(['orders'], { fence: generation, onMetadata: observer })
      )
      expect(error).toBe(closedBy)
      expect(observer).not.toHaveBeenCalled()
      expect(cluster.topicId('orders')).toBe(SECOND_ID)
      expect(pool.metadataExpireAt).not.toBeNull()
      expect(seen).toEqual(['0:seed'])
    })

    it('an observer that throws fails its refresh only after the install is fresh and routing listeners were woken', async () => {
      const { pool, seed } = build()
      seed.metadata.mockResolvedValueOnce({ brokers: [SEED_NODE], topicMetadata: [] })
      const seen = []
      pool.metadataCache.onRoutingChange(() => seen.push(pool.metadataExpireAt !== null))
      const thrown = new Error('observer failed')

      const { error } = await outcome(
        pool.refreshMetadata([], {
          onMetadata: () => {
            throw thrown
          },
        })
      )
      expect(error).toBe(thrown)
      expect(seen).toEqual([true])
    })

    it('an answer a newer install replaced before its call settled reports no success of its own', async () => {
      const { pool, seed } = build()
      const success = jest.spyOn(pool, 'onMetadataSuccess')
      const retryFor = pool.retryFor.bind(pool)
      let first = true
      pool.retryFor = (fence) => {
        const retrier = retryFor(fence)
        return async (fn) => {
          try {
            return await retrier(fn)
          } finally {
            if (first) {
              first = false
              await pool.refreshMetadata([], { isCurrent: () => true })
            }
          }
        }
      }
      seed.metadata
        .mockResolvedValueOnce({ clusterId: 'older', brokers: [SEED_NODE], topicMetadata: [] })
        .mockResolvedValueOnce({ clusterId: 'newer', brokers: [SEED_NODE], topicMetadata: [] })

      expect(await outcome(pool.refreshMetadata([]))).toEqual({ value: undefined })
      expect(pool.metadata.clusterId).toBe('newer')
      expect(success).toHaveBeenCalledTimes(1)
    })

    it('an answer whose pool build fails does not stop a rebootstrap that was building its seed', async () => {
      const { pool, seed, builder } = build()
      const builds = holdBuilds(builder)
      const recovering = outcome(pool.rebootstrap('required'))
      await settled()
      expect(builds).toHaveLength(1)
      seed.metadata.mockResolvedValueOnce({ brokers: [SEED_NODE, PEER], topicMetadata: [] })
      const refreshing = outcome(pool.refreshMetadata([]))
      await settled()
      expect(builds).toHaveLength(2)

      const failure = Object.assign(new Error('build failed'), { retriable: false })
      builds[1].fail(failure)
      expect((await refreshing).error).toBe(failure)
      builds[0].release()
      expect(await recovering).toEqual({ value: undefined })
      expect(pool.seedBroker.connectionPool === builds[0].pool).toBe(true)
      expect(seed.retire).toHaveBeenCalledTimes(1)
    })

    it('an answer keeps the seed pool and endpoint an endpoint hint swapped in place while its pools were built', async () => {
      const { pool, seed, builder } = build()
      const builds = holdBuilds(builder)
      seed.metadata.mockResolvedValueOnce({ brokers: [SEED_NODE, PEER], topicMetadata: [] })
      const refreshing = outcome(pool.refreshMetadata([]))
      await settled()
      expect(builds).toHaveLength(1)

      const hinting = pool.applyNodeEndpoints([
        { nodeId: 0, host: 'hinted', port: 9092, rack: null },
      ])
      await settled()
      expect(builds).toHaveLength(2)
      builds[1].release()
      await hinting
      const hinted = seed.connectionPool
      expect(hinted.host).toBe('hinted')

      builds[0].release()
      expect(await refreshing).toEqual({ value: undefined })
      expect(pool.brokers[0] === seed).toBe(true)
      expect(seed.connectionPool === hinted).toBe(true)
      expect(hinted.retired).toBe(false)
      expect(pool.brokers[1].connectionPool.host).toBe('peer')
      expect(pool.setupMetadataEndpoints.get('0')).toBe('hinted:9092')
      expect(pool.setupMetadataEndpoints.get('1')).toBe('peer:9092')
    })
  })
})
