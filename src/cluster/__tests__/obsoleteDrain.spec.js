const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const Cluster = require('../index')
const Generation = require('../../producer/generation')
const ConnectionPool = require('../../network/connectionPool')
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
 * @returns {Array<{ host: string, pool: ConnectionPool, release: () => void }>} the builds, in call order
 */
const holdBuilds = (builder) => {
  const builds = []
  builder.build.mockImplementation(
    ({ host, port, rack }) =>
      new Promise((resolve) => {
        const pool = new ConnectionPool({ logger: newLogger(), host, port, rack })
        pool.destroy = jest.fn(async () => {})
        builds.push({ host, pool, release: () => resolve(pool) })
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

  it('metadata skips later builds after a concurrent seed replacement', async () => {
    const { pool, seed, builder } = build()
    const old = broker(1)
    const peer = broker(2)
    const unused = new ConnectionPool({ logger: newLogger(), host: 'metadata', port: 9092 })
    unused.destroy = jest.fn(async () => {})
    pool.brokers = { 0: seed, 1: old, 2: peer }
    seed.metadata.mockResolvedValue({
      brokers: [
        { nodeId: 1, host: 'metadata', port: 9092 },
        { nodeId: 2, host: 'later', port: 9092 },
      ],
      topicMetadata: [],
    })
    builder.build.mockImplementationOnce(async () => {
      await pool.createSeedBroker()
      return unused
    })
    await pool.refreshMetadata([])
    expect(pool.seedBroker).not.toBe(seed)
    expect(pool.brokers[1]).toBe(old)
    expect(pool.brokers[2]).toBe(peer)
    expect(builder.build).toHaveBeenCalledTimes(2)
    expect(unused.retired).toBe(true)
    expect(unused.destroy).toHaveBeenCalledWith({ drainTimeout: 50 })
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(old.retire).not.toHaveBeenCalled()
    expect(peer.retire).not.toHaveBeenCalled()
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

  it.each([
    ['a negative node id', -1],
    ['a fractional node id', 1.5],
  ])('an endpoint with %s installs no broker and builds no pool', async (_, nodeId) => {
    const { pool, builder } = build()

    await pool.applyNodeEndpoints([{ nodeId, host: 'next', port: 9092 }])

    expect(Object.keys(pool.brokers)).toEqual(['0'])
    expect(builder.build).not.toHaveBeenCalled()
  })

  it('an endpoint with a non-negative integer node id and a usable address is applied', async () => {
    const { pool, builder } = build()

    await pool.applyNodeEndpoints([{ nodeId: 3, host: 'next', port: 9092 }])

    expect(builder.build).toHaveBeenCalledWith(
      expect.objectContaining({ host: 'next', port: 9092 })
    )
    expect(pool.brokers[3].connectionPool).toMatchObject({ host: 'next', port: 9092 })
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
  /** @returns {Error} a REBOOTSTRAP_REQUIRED answer */
  const required = () =>
    Object.assign(new Error('Rebootstrap required'), {
      type: 'REBOOTSTRAP_REQUIRED',
      code: 129,
      retriable: false,
    })

  /**
   * Asserts the refusal of a refresh a rebootstrap overtook: retriable, and without a cause.
   * @param {Error} [error] what the refresh rejected with
   * @returns {void}
   */
  const expectOvertaken = (error) => {
    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(error.message).toBe('Metadata refresh was overtaken by a rebootstrap')
    expect(error.cause).toBeUndefined()
  }

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
   * @param {() => Promise<void>} [after] runs right after that rebootstrap
   * @returns {{ installed: object|null }} the metadata installed when the rebootstrap started
   */
  const rebootstrapAtSettlement = (pool, after = async () => {}) => {
    const retryFor = pool.retryFor.bind(pool)
    const seen = { installed: null }
    let done = false
    pool.retryFor = (fence) => {
      const retrier = retryFor(fence)
      return async (fn) => {
        try {
          return await retrier(fn)
        } finally {
          if (!done) {
            done = true
            seen.installed = pool.metadata
            await pool.rebootstrap('required')
            await after()
          }
        }
      }
    }
    return seen
  }

  it('a reply held across a rebootstrap installs nothing and the call rejects as retriable', async () => {
    const { pool, replies } = acrossRebootstrap()
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    expect(replies).toHaveLength(1)

    await pool.rebootstrap('required')
    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    const { error } = await refreshing

    expectOvertaken(error)
    expect(pool.metadata).toBeNull()
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    expect(pool.brokers).toEqual({})
    expect(replies).toHaveLength(1)
    expect(success).not.toHaveBeenCalled()
    expect(failure).not.toHaveBeenCalled()
  })

  it('a rebootstrap during broker selection rejects the refresh as retriable', async () => {
    const { pool, seed, replies } = acrossRebootstrap()
    pool.findConnectedBroker.mockImplementationOnce(async () => {
      await pool.rebootstrap('required')
      throw new KafkaJSConnectionError('Closed by the rebootstrap')
    })

    const { error } = await outcome(pool.refreshMetadata([]))

    expectOvertaken(error)
    expect(pool.findConnectedBroker).toHaveBeenCalledTimes(1)
    expect(replies).toHaveLength(0)
    expect(seed.metadata).not.toHaveBeenCalled()
    expect(pool.metadataUnavailableSince).toBeNull()
  })

  it('a connection closed by a rebootstrap starts no recovery clock and rejects as retriable', async () => {
    const { pool, replies } = acrossRebootstrap()
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    await pool.rebootstrap('required')
    replies[0].reject(closed())
    const { error } = await refreshing

    expectOvertaken(error)
    expect(failure).not.toHaveBeenCalled()
    expect(pool.metadataUnavailableSince).toBeNull()
    expect(replies).toHaveLength(1)
  })

  it('with retry: { retries: 0 } a refresh overtaken by a rebootstrap rejects as retriable', async () => {
    const { pool, replies } = acrossRebootstrap()
    expect(pool.retryOptions).toEqual({ retries: 0 })
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    await pool.rebootstrap('required')
    replies[0].reject(closed())
    const { error } = await refreshing

    expectOvertaken(error)
    expect(replies).toHaveLength(1)
    expect(pool.metadataUnavailableSince).toBeNull()
  })

  it("a rebootstrap while an answer's pools are built stops the build and rejects", async () => {
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
    expect(builder.build).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    // The rebootstrap's own wake sees the old cluster gone.
    expect(seen.at(-1)).toEqual({ cluster: null, brokers: [], endpoints: [] })
    releasePeer()
    const { error } = await refreshing

    expectOvertaken(error)
    expect(success).not.toHaveBeenCalled()
    expect(pool.brokers[1]).toBeUndefined()
    expect(peerPool.retired).toBe(true)
    expect(seen).toHaveLength(2)
  })

  it('setup endpoints of the old cluster are cleared by a rebootstrap', async () => {
    const { pool } = build()
    pool.setupMetadataEndpoints.set('1', 'old-peer:9092')
    await pool.rebootstrap('required')
    expect([...pool.setupMetadataEndpoints]).toEqual([])
  })

  it('a refresh started after a rebootstrap never joins one from before', async () => {
    const { pool, replies } = acrossRebootstrap()
    const unshared = jest.spyOn(pool, 'refreshMetadataUnshared')
    const first = outcome(pool.refreshMetadata([]))
    await settled()

    await pool.rebootstrap('required')
    const second = outcome(pool.refreshMetadata([]))
    await settled()
    expect(unshared).toHaveBeenCalledTimes(2)
    expect(replies).toHaveLength(2)

    replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
    expectOvertaken((await first).error)
    replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await second).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
    expect(pool.metadataCache.leader('old-topic', 0)).toBe(-1)
  })

  it('a retry whose selection a rebootstrap overtook after the first request sends nothing more and rejects as retriable', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
    try {
      const { pool, seed, replies } = acrossRebootstrap()
      pool.retryOptions = { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }
      const success = jest.spyOn(pool, 'onMetadataSuccess')
      const failure = jest.spyOn(pool, 'onMetadataFailure')
      const update = jest.spyOn(pool.metadataCache, 'update')
      const refreshing = outcome(pool.refreshMetadata([]))
      await settled()
      expect(replies).toHaveLength(1)

      let select
      pool.findConnectedBroker.mockImplementationOnce(
        () => new Promise((resolve) => (select = resolve))
      )
      replies[0].reject(
        Object.assign(new Error('Leader not available'), {
          type: 'LEADER_NOT_AVAILABLE',
          retriable: true,
        })
      )
      await jest.advanceTimersByTimeAsync(10)
      expect(pool.findConnectedBroker).toHaveBeenCalledTimes(2)

      await pool.rebootstrap('required')
      const installed = pool.seedBroker
      select(installed)
      await settled()

      expect(replies).toHaveLength(1)
      expect(seed.metadata).toHaveBeenCalledTimes(1)
      expect(installed.metadata).not.toHaveBeenCalled()
      expectOvertaken((await refreshing).error)
      expect(update).not.toHaveBeenCalled()
      expect(pool.metadata).toBeNull()
      expect(success).not.toHaveBeenCalled()
      expect(failure).not.toHaveBeenCalled()
    } finally {
      jest.useRealTimers()
    }
  })

  it('a refresh whose own rebootstrap selected the new seed installs that answer once', async () => {
    const { pool, replies } = acrossRebootstrap()
    const update = jest.spyOn(pool.metadataCache, 'update')
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
    expect(update).toHaveBeenCalledTimes(1)
    expect(update.mock.calls[0][0].clusterId).toBe('new')
    expect(replies).toHaveLength(1)
    expect(pool.findConnectedBroker).toHaveBeenCalledTimes(1)
  })

  it('a caller refusal after a rebootstrap wins over the overtaken error when the held reply settles', async () => {
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

  it('a rebootstrap after the last answer was installed and before the call settles rejects it as retriable', async () => {
    const { pool, seed, replies } = acrossRebootstrap()
    const seen = rebootstrapAtSettlement(pool)
    const success = jest.spyOn(pool, 'onMetadataSuccess')
    const failure = jest.spyOn(pool, 'onMetadataFailure')
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()

    replies[0].resolve(answer('old', [SEED_NODE], ['old-topic']))
    const { error } = await refreshing

    // The answer was installed before the rebootstrap that overtook the settlement.
    expect(seen.installed?.clusterId).toBe('old')
    expectOvertaken(error)
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

    replies[0].reject(required())
    const { error } = await refreshing

    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.retriable).toBe(true)
    expect(failure).not.toHaveBeenCalled()
    // One seed was built, by the rebootstrap at settlement; the stale 129 started no second one.
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.metadataUnavailableSince).toBeNull()
  })

  it('a disconnect after a rebootstrap still resolves an earlier refresh silently', async () => {
    // A held answer that arrives after both returns from the attempt and reaches the success path.
    const answered = acrossRebootstrap()
    const answeredHooks = [
      jest.spyOn(answered.pool, 'onMetadataSuccess'),
      jest.spyOn(answered.pool, 'onMetadataFailure'),
    ]
    const refreshing = outcome(answered.pool.refreshMetadata([]))
    await settled()
    await answered.pool.rebootstrap('required')
    await answered.pool.disconnect()
    answered.replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))

    expect(await refreshing).toEqual({ value: undefined })
    expect(answered.pool.metadata).toBeNull()
    expect(answered.pool.metadataCache.leader('old-topic', 0)).toBe(-1)
    for (const hook of answeredHooks) expect(hook).not.toHaveBeenCalled()

    // A selection that fails after both reaches the failure path.
    const failed = acrossRebootstrap()
    const failedHooks = [
      jest.spyOn(failed.pool, 'onMetadataSuccess'),
      jest.spyOn(failed.pool, 'onMetadataFailure'),
    ]
    failed.pool.findConnectedBroker.mockImplementationOnce(async () => {
      await failed.pool.rebootstrap('required')
      await failed.pool.disconnect()
      throw closed()
    })

    expect(await outcome(failed.pool.refreshMetadata([]))).toEqual({ value: undefined })
    expect(failed.pool.metadata).toBeNull()
    expect(failed.replies).toHaveLength(0)
    for (const hook of failedHooks) expect(hook).not.toHaveBeenCalled()
  })

  it('at settlement after a rebootstrap and a disconnect, a caller refusal rejects and otherwise the call resolves silently', async () => {
    const refusal = new Error('caller left')
    const settle = async (refuse) => {
      const { pool, replies } = acrossRebootstrap()
      const success = jest.spyOn(pool, 'onMetadataSuccess')
      let refused = null
      rebootstrapAtSettlement(pool, async () => {
        await pool.disconnect()
        if (refuse) refused = refusal
      })
      const refreshing = outcome(pool.refreshMetadata([], { guard: () => refused }))
      await settled()
      replies[0].resolve(answer('old', [SEED_NODE], ['old-topic']))
      const result = await refreshing
      expect(success).not.toHaveBeenCalled()
      return result
    }

    expect(await settle(true)).toEqual({ error: refusal })
    expect(await settle(false)).toEqual({ value: undefined })
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

  it('an old seed connect that succeeds after a rebootstrap does not write its versions', async () => {
    const { pool, seed, builder } = build()
    const stale = { 3: { minVersion: 0, maxVersion: 12 } }
    const fresh = { 3: { minVersion: 0, maxVersion: 13 } }
    pool.retryOptions = { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }
    const create = pool.createBroker
    pool.createBroker = jest.fn((options) =>
      Object.assign(create(options), { versions: fresh, connect: jest.fn(async () => {}) })
    )
    let succeed
    seed.isConnected = () => false
    seed.versions = stale
    seed.connect = jest.fn(() => new Promise((resolve) => (succeed = resolve)))
    const connecting = outcome(pool.connect())
    await settled()
    expect(seed.connect).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    const installed = pool.seedBroker
    succeed()

    expect(await connecting).toEqual({ value: undefined })
    expect(pool.versions).toBe(fresh)
    expect(installed.connect).toHaveBeenCalledTimes(1)
    expect(pool.seedBroker).toBe(installed)
    expect(builder.build).toHaveBeenCalledTimes(1)
  })

  it('a seed connect that fails after its seed was removed retries on a newly built seed without a rebootstrap', async () => {
    const { pool, seed, builder } = build()
    pool.retryOptions = { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }
    const create = pool.createBroker
    pool.createBroker = jest.fn((options) =>
      Object.assign(create(options), { connect: jest.fn(async () => {}) })
    )
    let fail
    seed.isConnected = () => false
    seed.connect = jest.fn(() => new Promise((resolve, reject) => (fail = reject)))
    const connecting = outcome(pool.connect())
    await settled()
    expect(seed.connect).toHaveBeenCalledTimes(1)

    pool.removeBroker({ host: 'seed', port: 9092 })
    expect(pool.seedBroker).toBeUndefined()
    fail(new KafkaJSConnectionError('Connection refused'))

    expect(await connecting).toEqual({ value: undefined })
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.createBroker).toHaveBeenCalledTimes(1)
    const rebuilt = pool.createBroker.mock.results[0].value
    expect(pool.seedBroker).toBe(rebuilt)
    expect(rebuilt.connect).toHaveBeenCalledTimes(1)
    expect(pool.recoveries).toBe(0)
  })

  it('a seed connect that succeeds after its seed was removed connects a newly built seed and writes its versions', async () => {
    const { pool, seed, builder } = build()
    const stale = { 3: { minVersion: 0, maxVersion: 12 } }
    const fresh = { 3: { minVersion: 0, maxVersion: 13 } }
    pool.retryOptions = { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }
    const create = pool.createBroker
    pool.createBroker = jest.fn((options) =>
      Object.assign(create(options), { versions: fresh, connect: jest.fn(async () => {}) })
    )
    let succeed
    seed.isConnected = () => false
    seed.versions = stale
    seed.connect = jest.fn(() => new Promise((resolve) => (succeed = resolve)))
    const connecting = outcome(pool.connect())
    await settled()
    expect(seed.connect).toHaveBeenCalledTimes(1)

    pool.removeBroker({ host: 'seed', port: 9092 })
    expect(pool.seedBroker).toBeUndefined()
    succeed()

    expect(await connecting).toEqual({ value: undefined })
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.createBroker).toHaveBeenCalledTimes(1)
    const rebuilt = pool.createBroker.mock.results[0].value
    expect(pool.seedBroker).toBe(rebuilt)
    expect(rebuilt.connect).toHaveBeenCalledTimes(1)
    expect(pool.versions).toBe(fresh)
    expect(pool.recoveries).toBe(0)
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

  it('a broker added to the table while the new seed is built is retired by the rebootstrap', async () => {
    const { pool, builder } = build()
    let releaseSeed
    builder.build.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releaseSeed = () =>
            resolve(new ConnectionPool({ logger: newLogger(), host: 'seed-1', port: 9092 }))
        })
    )
    const recovering = pool.rebootstrap('required')
    await settled()
    expect(builder.build).toHaveBeenCalledTimes(1)

    await pool.applyNodeEndpoints([{ nodeId: 7, host: 'late', port: 9092, rack: null }])
    const late = pool.brokers[7]
    expect(late.connectionPool.host).toBe('late')
    releaseSeed()
    await recovering

    expect(pool.brokers[7]).toBeUndefined()
    expect(late.retire).toHaveBeenCalledTimes(1)
    expect(late.retire).toHaveBeenCalledWith({ drainTimeout: 50 })
  })

  it('the rebootstrap wakes routing listeners only after the old brokers are detached', async () => {
    const { pool, seed } = build()
    const peer = broker(1, 'old-peer')
    pool.brokers = { 0: seed, 1: peer }
    pool.metadata = answer('old', [SEED_NODE, OLD_PEER])
    pool.setupMetadataEndpoints.set('1', 'old-peer:9092')
    const seen = []
    pool.metadataCache.onRoutingChange(() =>
      seen.push({
        brokers: Object.keys(pool.brokers),
        endpoints: [...pool.setupMetadataEndpoints.keys()],
        metadata: pool.metadata,
      })
    )

    await pool.rebootstrap('required')

    expect(seen).toEqual([{ brokers: [], endpoints: [], metadata: null }])
    expect(peer.retire).toHaveBeenCalledTimes(1)
  })

  it('a refresh episode overtaken before its work starts runs in the new generation and is joined by a later caller', async () => {
    const { pool, replies } = acrossRebootstrap()
    const unshared = jest.spyOn(pool, 'refreshMetadataUnshared')
    let installSeed
    pool.createSeedBroker = jest.fn(() => new Promise((resolve) => (installSeed = resolve)))
    const recovering = pool.rebootstrap('required')
    await settled()
    expect(pool.createSeedBroker).toHaveBeenCalledTimes(1)
    const next = pool.createBroker({
      connectionPool: new ConnectionPool({ logger: newLogger(), host: 'seed-1', port: 9092 }),
    })

    // Resolved before the call, so the rebootstrap commits in the microtask before the episode's
    // deferred start: the episode exists, its work has not started.
    pool.seedBroker = next
    installSeed(next)
    const first = outcome(pool.refreshMetadata([]))
    await recovering
    await settled()
    const second = outcome(pool.refreshMetadata([]))
    await settled()

    expect(unshared).toHaveBeenCalledTimes(1)
    expect(replies).toHaveLength(1)
    expect(next.metadata).toHaveBeenCalledTimes(1)
    replies[0].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await first).toEqual({ value: undefined })
    expect(await second).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
  })

  it("the call's own ApiVersions 129 recovery rejects with the 129 error", async () => {
    const { pool, seed, builder, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    const error = required()

    // The request's connection was answered 129: its broker recovers first, then fails the request.
    await pool.onApiVersionsFailure(error, { broker: seed, pool: seed.connectionPool })
    replies[0].reject(error)

    expect((await refreshing).error).toBe(error)
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.seedBroker).not.toBe(seed)
  })

  it("the call's own metadata 129 starts one rebootstrap and rejects with the 129", async () => {
    const { pool, seed, builder, replies } = acrossRebootstrap()
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    const error = required()

    replies[0].reject(error)

    expect((await refreshing).error).toBe(error)
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(pool.seedBroker).not.toBe(seed)
  })

  it("a cached node's ApiVersions 129 during selection reaches the refresh as the original error", async () => {
    const { pool, builder } = build()
    delete pool.findConnectedBroker
    const error = required()
    const node = broker(1, 'cached')
    node.isConnected = () => false
    node.connect = jest.fn(async () => {
      await pool.onApiVersionsFailure(error, { broker: node, pool: node.connectionPool })
      throw error
    })
    pool.brokers = { 1: node }
    const connect = jest.spyOn(pool, 'connect')

    expect((await outcome(pool.refreshMetadata([]))).error).toBe(error)
    expect(node.connect).toHaveBeenCalledTimes(1)
    expect(builder.build).toHaveBeenCalledTimes(1)
    expect(connect).not.toHaveBeenCalled()
  })

  it("a refresh whose selection ends on today's seed after an independent rebootstrap sends its request in the new generation", async () => {
    const { pool, replies } = acrossRebootstrap()
    delete pool.findConnectedBroker
    const old = broker(1, 'old-peer')
    let fail
    old.isConnected = () => false
    old.connect = jest.fn(() => new Promise((resolve, reject) => (fail = reject)))
    pool.brokers = { 1: old }
    const refreshing = outcome(pool.refreshMetadata([]))
    await settled()
    expect(old.connect).toHaveBeenCalledTimes(1)

    await pool.rebootstrap('required')
    const installed = pool.seedBroker
    fail(new KafkaJSConnectionError('Closed by the rebootstrap'))
    await settled()

    expect(replies).toHaveLength(1)
    expect(installed.metadata).toHaveBeenCalledTimes(1)
    replies[0].resolve(answer('new', [NEW_SEED], ['new-topic']))
    expect(await refreshing).toEqual({ value: undefined })
    expect(pool.metadata.clusterId).toBe('new')
    expect(pool.metadataCache.leader('new-topic', 0)).toBe(0)
  })

  it.each(['refreshMetadata', 'refreshMetadataIfNecessary'])(
    'Cluster#%s: a refresh begun before a rebootstrap is not joined by a later one',
    async (method) => {
      const { cluster, pool, replies } = clusterAcrossRebootstrap()
      const observer = jest.fn()
      const refresh = () => outcome(cluster[method]({ onMetadata: observer }))
      const first = refresh()
      await settled()
      expect(replies).toHaveLength(1)

      await pool.rebootstrap('required')
      const second = refresh()
      await settled()
      expect(replies).toHaveLength(2)

      replies[0].resolve(answer('old', [SEED_NODE, OLD_PEER], ['old-topic']))
      expectOvertaken((await first).error)
      expect(observer).not.toHaveBeenCalled()

      replies[1].resolve(answer('new', [NEW_SEED], ['new-topic']))
      expect(await second).toEqual({ value: undefined })
      expect(pool.metadata.clusterId).toBe('new')
      expect(observer.mock.calls.map(([metadata]) => metadata.clusterId)).toEqual(['new'])
    }
  )
})
