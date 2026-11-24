const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const Cluster = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const { KafkaJSConnectionClosedError } = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')

/** @returns {{ promise: Promise<void>, resolve: () => void, reject: (e: Error) => void }} */
const deferred = () => {
  const d = {}
  d.promise = new Promise((resolve, reject) => Object.assign(d, { resolve, reject }))
  return d
}

/** Records how `p` settles, without awaiting it. */
const settled = (p) => {
  const s = { done: false }
  p.then(
    (value) => Object.assign(s, { done: true, value }),
    (error) => Object.assign(s, { done: true, error })
  )
  return s
}

/** Lets every ready microtask and I/O callback run. */
const flush = () => new Promise(setImmediate)

/**
 * A broker double over a real connection pool whose `disconnect` and `retire` record their
 * options and answer with the next scripted outcome, at once when none is scripted.
 * @param {number} nodeId
 * @returns {Broker & { calls: object[], next: Array<() => Promise<void>> }}
 */
const broker = (nodeId) => {
  const connectionPool = new ConnectionPool({ logger: newLogger(), host: `b${nodeId}`, port: 9092 })
  const object = new Broker({ connectionPool, logger: newLogger(), nodeId })
  object.calls = []
  object.next = []
  const answer = (kind) => (options) => {
    object.calls.push([kind, options])
    const step = object.next.shift()
    return step ? step() : Promise.resolve()
  }
  object.disconnect = jest.fn(answer('disconnect'))
  object.retire = jest.fn(answer('retire'))
  return object
}

/** A pool whose seed is broker 0 and whose table holds brokers 0, 1 and 2. */
const build = () => {
  const pool = new BrokerPool({
    connectionPoolBuilder: { build: jest.fn() },
    logger: newLogger(),
    retry: { retries: 0 },
    drainTimeout: 50,
  })
  const [seed, one, two] = [broker(0), broker(1), broker(2)]
  pool.seedBroker = seed
  pool.brokers = { 0: seed, 1: one, 2: two }
  return { pool, seed, one, two }
}

/**
 * The pool of `build()` able to refresh metadata and rebootstrap without a socket: the seed answers
 * every Metadata request with `answer`, and each pool the builder makes is a real pool whose
 * `destroy` resolves at once.
 * @param {object} [answer] decoded Metadata answer
 * @returns {ReturnType<typeof build>}
 */
const refreshable = (answer = { brokers: [], topicMetadata: [] }) => {
  const fixture = build()
  const { pool, seed } = fixture
  pool.connectionPoolBuilder.build.mockImplementation(
    async ({ host = 'next-seed', port = 9092, rack } = {}) => {
      const built = new ConnectionPool({ logger: newLogger(), host, port, rack })
      built.destroy = jest.fn(async () => {})
      return built
    }
  )
  pool.createBroker = jest.fn(({ nodeId, connectionPool }) =>
    Object.assign(broker(nodeId), { connectionPool })
  )
  pool.findConnectedBroker = async () => seed
  seed.isConnected = () => true
  seed.metadata = jest.fn(async () => answer)
  return fixture
}

/** Node 0 moved away from the seed's address, node 1 kept and node 2 no longer listed. */
const MOVED_SEED = {
  brokers: [
    { nodeId: 0, host: 'moved', port: 9092 },
    { nodeId: 1, host: 'b1', port: 9092 },
  ],
  topicMetadata: [],
}

describe('BrokerPool > a disconnect reaches the drains an earlier one started', () => {
  it('a zero-drain disconnect during a draining one dispatches again to the detached non-seed and settles only after its first drain has', async () => {
    const { pool, seed, one } = build()
    const drain = deferred()
    one.next.push(() => drain.promise)
    const first = settled(pool.disconnect({ preserveSetupState: true }))
    await flush()
    const second = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()

    expect(one.calls).toEqual([
      ['retire', { preserveSetupState: true }],
      ['retire', { drainTimeout: 0 }],
    ])
    expect(seed.calls).toEqual([
      ['disconnect', { preserveSetupState: true }],
      ['disconnect', { drainTimeout: 0 }],
    ])
    expect([first.done, second.done]).toEqual([false, false])

    drain.resolve()
    await flush()

    expect(first).toEqual({ done: true, value: undefined })
    expect(second).toEqual({ done: true, value: undefined })
  })

  it('a disconnect reaches a broker that a failure removal retired and is still draining', async () => {
    const { pool, two } = build()
    const drain = deferred()
    two.next.push(() => drain.promise)
    pool.removeBroker({ host: 'b2', port: 9092 })

    expect(two.calls).toEqual([['retire', { drainTimeout: 50 }]])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()

    expect(two.calls).toEqual([
      ['retire', { drainTimeout: 50 }],
      ['retire', { drainTimeout: 0 }],
    ])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()

    expect(close).toEqual({ done: true, value: undefined })
  })

  it('a first failed drain neither stops the dispatch to later brokers nor settles the disconnect before they have', async () => {
    const { pool, one, two } = build()
    const failure = new Error('drain failed')
    const held = deferred()
    one.retire.mockImplementationOnce(() => {
      throw failure
    })
    two.next.push(() => held.promise)
    const close = settled(pool.disconnect())
    await flush()

    expect(two.retire).toHaveBeenCalledTimes(1)
    expect(close.done).toBe(false)

    held.resolve()
    await flush()

    expect(close).toEqual({ done: true, error: failure })
  })

  it('a broker leaves the drain record once its drains settle, so the next disconnect does not dispatch to it (control)', async () => {
    const { pool, one } = build()
    await pool.disconnect()
    await flush()
    await pool.disconnect()

    expect(one.calls).toEqual([['retire', undefined]])
  })
})

describe('BrokerPool > a disconnect reaches the drains a replacement started', () => {
  it('a forced disconnect closes the socket an endpoint move left draining and rejects its held request', async () => {
    const seam = createScriptedCluster({
      nodes: [
        { id: 0, host: 'seed', port: 9092 },
        { id: 1, host: 'first', port: 9092 },
        { id: 2, host: 'second', port: 9092 },
      ],
    })
    let host = 'first'
    seam.answer('Metadata', () => ({
      brokers: [
        { nodeId: 0, host: 'seed', port: 9092, rack: null },
        { nodeId: 1, host, port: 9092, rack: null },
      ],
      controllerId: 0,
      topics: [],
    }))
    const cluster = new Cluster({
      ...seam.kafkaOptions({ requestTimeout: 3000, enforceRequestTimeout: false }),
      logger: newLogger(),
      retry: { retries: 0 },
      metadataRecoveryStrategy: 'none',
    })
    let gate
    try {
      await cluster.connect()
      await cluster.refreshMetadata()
      const moved = await cluster.findBroker({ nodeId: '1' })
      gate = seam.schedule.hold({ api: 'Metadata', node: 1 })
      const held = settled(moved.metadata([]))
      await gate.arrived()
      host = 'second'
      await cluster.refreshMetadata()
      expect(cluster.brokerPool.brokers[1]).not.toBe(moved)

      await cluster.disconnect({ drainTimeout: 0 })

      expect(seam.sockets().filter((socket) => !socket.destroyed)).toEqual([])
      await flush()
      expect(held.done).toBe(true)
      expect(held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
    } finally {
      gate?.release()
      await cluster.disconnect()
    }
  })

  it('a forced disconnect escalates the drain of a broker metadata no longer lists', async () => {
    const { pool, two } = refreshable({
      brokers: [
        { nodeId: 0, host: 'b0', port: 9092 },
        { nodeId: 1, host: 'b1', port: 9092 },
      ],
      topicMetadata: [],
    })
    const drain = deferred()
    two.next.push(() => drain.promise)
    await pool.refreshMetadata([])
    expect(two.calls).toEqual([['retire', { drainTimeout: 50 }]])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()

    expect(two.calls).toEqual([
      ['retire', { drainTimeout: 50 }],
      ['retire', { drainTimeout: 0 }],
    ])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()

    expect(close).toEqual({ done: true, value: undefined })
  })

  it('a forced disconnect escalates the drain of the old seed a rebootstrap retired', async () => {
    const { pool, seed } = refreshable()
    const drain = deferred()
    seed.next.push(() => drain.promise)
    await pool.rebootstrap('required')
    expect(pool.seedBroker).not.toBe(seed)
    expect(seed.calls).toEqual([['retire', { drainTimeout: 50 }]])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()

    expect(seed.calls).toEqual([
      ['retire', { drainTimeout: 50 }],
      ['retire', { drainTimeout: 0 }],
    ])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()

    expect(close).toEqual({ done: true, value: undefined })
  })

  it('a forced disconnect escalates the drain of a pool a replacement retired', async () => {
    const { pool, seed } = refreshable(MOVED_SEED)
    const oldPool = seed.connectionPool
    const drain = deferred()
    oldPool.destroy = jest.fn().mockReturnValueOnce(drain.promise).mockResolvedValue(undefined)
    await pool.refreshMetadata([])
    expect(seed.connectionPool).not.toBe(oldPool)
    expect(oldPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }]])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()

    expect(oldPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }], [{ drainTimeout: 0 }]])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()

    expect(close).toEqual({ done: true, value: undefined })
  })

  it('a rejected and a pending drain are both awaited, and the failure is thrown after both', async () => {
    const { pool, seed, two } = refreshable(MOVED_SEED)
    const failure = new Error('drain failed')
    const brokerDrain = deferred()
    const poolDrain = deferred()
    two.next.push(
      () => brokerDrain.promise,
      () => Promise.reject(failure)
    )
    seed.connectionPool.destroy = jest
      .fn()
      .mockReturnValueOnce(poolDrain.promise)
      .mockResolvedValue(undefined)
    await pool.refreshMetadata([])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()
    expect(close.done).toBe(false)

    brokerDrain.resolve()
    await flush()
    expect(close.done).toBe(false)

    poolDrain.resolve()
    await flush()
    expect(close).toEqual({ done: true, error: failure })
  })

  it('two overlapping disconnects both wait for the drains a replacement started', async () => {
    const { pool, seed, two } = refreshable(MOVED_SEED)
    const drain = deferred()
    two.next.push(() => drain.promise)
    seed.connectionPool.destroy = jest.fn(async () => {})
    await pool.refreshMetadata([])

    const forced = settled(pool.disconnect({ drainTimeout: 0 }))
    const plain = settled(pool.disconnect())
    await flush()
    expect([forced.done, plain.done]).toEqual([false, false])

    drain.resolve()
    await flush()

    expect(forced).toEqual({ done: true, value: undefined })
    expect(plain).toEqual({ done: true, value: undefined })
  })

  it('a synchronous throw from a pool destroy is tracked and rejects the disconnect after the other drains', async () => {
    const { pool, seed } = refreshable(MOVED_SEED)
    const failure = new Error('destroy threw')
    const drain = deferred()
    seed.connectionPool.destroy = jest
      .fn()
      .mockReturnValueOnce(drain.promise)
      .mockImplementationOnce(() => {
        throw failure
      })
    await pool.refreshMetadata([])

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()
    expect(close).toEqual({ done: true, error: failure })
  })

  it('a synchronous throw while retiring a replaced pool does not fail the refresh, still destroys the pool and keeps its drain joinable', async () => {
    const { pool, seed } = refreshable(MOVED_SEED)
    const oldPool = seed.connectionPool
    const failure = new Error('retire threw')
    const drain = deferred()
    oldPool.retire = jest.fn(() => {
      throw failure
    })
    oldPool.destroy = jest.fn().mockReturnValueOnce(drain.promise).mockResolvedValue(undefined)

    await pool.refreshMetadata([])

    expect(seed.connectionPool).not.toBe(oldPool)
    expect(oldPool.retire).toHaveBeenCalledTimes(1)
    expect(oldPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }]])
    expect(pool.drainingPools.has(oldPool)).toBe(true)

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()
    expect(oldPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }], [{ drainTimeout: 0 }]])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()
    // The background drain's retire failure is logged; only this call's own drains can reject it.
    expect(close).toEqual({ done: true, value: undefined })
    expect(pool.drainingPools.size).toBe(0)
  })

  it('a synchronous throw while fencing a removed broker still destroys its pool, and a disconnect waits for that destroy', async () => {
    const { pool } = build()
    const connectionPool = new ConnectionPool({ logger: newLogger(), host: 'b3', port: 9092 })
    const failure = new Error('retire threw')
    const drain = deferred()
    connectionPool.retire = jest.fn(() => {
      throw failure
    })
    connectionPool.destroy = jest
      .fn()
      .mockReturnValueOnce(drain.promise)
      .mockResolvedValue(undefined)
    const three = new Broker({ connectionPool, logger: newLogger(), nodeId: 3 })
    pool.brokers[3] = three

    pool.removeBroker({ host: 'b3', port: 9092 })
    await flush()

    expect(connectionPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }]])
    expect(pool.draining.has(three)).toBe(true)

    const close = settled(pool.disconnect({ drainTimeout: 0 }))
    await flush()
    expect(connectionPool.destroy.mock.calls).toEqual([
      [{ drainTimeout: 50 }],
      [{ drainTimeout: 0 }],
    ])
    expect(close.done).toBe(false)

    drain.resolve()
    await flush()
    // This call's own redispatch fenced again, so it reports the fencing failure after teardown.
    expect(close).toEqual({ done: true, error: failure })
    expect(pool.draining.size).toBe(0)
  })

  it('both drain records hold the drains while they run and are empty once they settle', async () => {
    const { pool, seed, two } = refreshable(MOVED_SEED)
    const oldPool = seed.connectionPool
    const brokerDrain = deferred()
    const poolDrain = deferred()
    two.next.push(() => brokerDrain.promise)
    oldPool.destroy = jest.fn().mockReturnValueOnce(poolDrain.promise).mockResolvedValue(undefined)
    await pool.refreshMetadata([])

    expect(pool.draining.has(two)).toBe(true)
    expect(pool.drainingPools.has(oldPool)).toBe(true)

    brokerDrain.resolve()
    poolDrain.resolve()
    await flush()

    expect(pool.draining.size).toBe(0)
    expect(pool.drainingPools.size).toBe(0)
  })
})
