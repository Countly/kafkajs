const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const Cluster = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const { KafkaJSConnectionClosedError } = require('../../errors')
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
  const refreshing = Promise.all([pool.refreshMetadata([]), pool.refreshMetadata([])])
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

  it('rebootstrap deduplicates aliases and preserves a replacement installed during the build', async () => {
    const { pool, seed, builder } = build()
    const old = broker(1)
    const replacement = broker(1, 'new')
    pool.brokers = { 0: seed, 1: old, 9: old }
    builder.build.mockImplementationOnce(async () => {
      pool.brokers[1] = replacement
      return { host: 'next-seed', port: 9092 }
    })
    old.disconnect.mockImplementation(() => new Promise(() => {}))
    await within(pool.rebootstrap('required'))
    expect(old.retire).toHaveBeenCalledTimes(1)
    expect(seed.retire).toHaveBeenCalledTimes(1)
    expect(pool.brokers[1]).toBe(replacement)
    expect(pool.brokers[9]).toBeUndefined()
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
      const refreshing = Promise.all([cluster.metadata(), cluster.metadata()])
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
