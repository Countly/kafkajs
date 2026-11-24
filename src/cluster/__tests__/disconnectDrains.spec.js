const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const Cluster = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const { KafkaJSConnectionClosedError } = require('../../errors')
const { LEVELS } = require('../../loggers')
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

  it('a disconnect started synchronously from inside one broker drain dispatches to and waits for every broker the outer one captured', async () => {
    const { pool, one, two } = build()
    const held = deferred()
    let nested
    one.retire.mockImplementationOnce((options) => {
      one.calls.push(['retire', options])
      nested = settled(pool.disconnect({ drainTimeout: 0 }))
      return Promise.resolve()
    })
    two.next.push(() => held.promise)
    const outer = settled(pool.disconnect())
    await flush()

    expect(two.calls).toEqual([
      ['retire', { drainTimeout: 0 }],
      ['retire', undefined],
    ])
    expect([outer.done, nested.done]).toEqual([false, false])

    held.resolve()
    await flush()

    expect(outer).toEqual({ done: true, value: undefined })
    expect(nested).toEqual({ done: true, value: undefined })
    expect(pool.draining.size).toBe(0)
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

  it('two overlapping disconnects, one forced, both wait for a replaced pool still draining', async () => {
    const { pool, seed } = refreshable(MOVED_SEED)
    const oldPool = seed.connectionPool
    const drain = deferred()
    oldPool.destroy = jest.fn().mockReturnValueOnce(drain.promise).mockResolvedValue(undefined)
    await pool.refreshMetadata([])

    const forced = settled(pool.disconnect({ drainTimeout: 0 }))
    const plain = settled(pool.disconnect())
    await flush()

    expect(oldPool.destroy.mock.calls).toEqual([[{ drainTimeout: 50 }], [{ drainTimeout: 0 }], []])
    expect([forced.done, plain.done]).toEqual([false, false])

    drain.resolve()
    await flush()

    expect(forced).toEqual({ done: true, value: undefined })
    expect(plain).toEqual({ done: true, value: undefined })
    expect(pool.drainingPools.size).toBe(0)
  })

  it('a rebootstrap whose seed build a disconnect superseded neither opens nor installs its pool', async () => {
    const { pool, seed } = refreshable()
    const built = deferred()
    pool.connectionPoolBuilder.build.mockImplementationOnce(() => built.promise)
    const recovery = settled(pool.rebootstrap('required'))
    await flush()
    expect(pool.connectionPoolBuilder.build).toHaveBeenCalledTimes(1)

    await pool.disconnect({ drainTimeout: 0 })
    const unused = new ConnectionPool({ logger: newLogger(), host: 'next-seed', port: 9092 })
    const connects = unused.pool.map((connection) => jest.spyOn(connection, 'connect'))
    unused.destroy = jest.fn(async () => {})
    built.resolve(unused)
    await flush()

    expect(recovery.done).toBe(true)
    expect(recovery.error?.message).toBe('Seed broker build was superseded')
    expect(pool.seedBroker).toBe(seed)
    expect(unused.retired).toBe(true)
    expect(unused.destroy).toHaveBeenCalledTimes(1)
    for (const connect of connects) expect(connect).not.toHaveBeenCalled()
    expect(pool.drainingPools.size).toBe(0)
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

describe('BrokerPool > a disconnect closes the socket a replacement left draining', () => {
  const BOUND = 100

  /**
   * Each way a refresh or a rebootstrap leaves a socket draining: `node` carries the held request,
   * `sender` picks the broker that sends it, and `replace` retires that broker or its pool.
   */
  const REPLACEMENTS = {
    'an endpoint move': {
      node: 1,
      sender: (cluster) => cluster.findBroker({ nodeId: '1' }),
      replace: (cluster, listed) => {
        listed[1].host = 'second'
        return cluster.refreshMetadata()
      },
    },
    'a broker metadata no longer lists': {
      node: 1,
      sender: (cluster) => cluster.findBroker({ nodeId: '1' }),
      replace: (cluster, listed) => {
        listed.pop()
        return cluster.refreshMetadata()
      },
    },
    'a rebootstrap that replaced the seed': {
      node: 0,
      sender: (cluster) => cluster.brokerPool.seedBroker,
      replace: (cluster) => cluster.brokerPool.rebootstrap('required'),
    },
    'a seed address move that replaced the seed pool': {
      node: 0,
      sender: (cluster) => cluster.brokerPool.seedBroker,
      replace: (cluster, listed) => {
        listed[0].host = 'moved'
        return cluster.refreshMetadata()
      },
    },
  }

  afterEach(() => jest.useRealTimers())

  /**
   * Connects a client over scripted sockets, its bootstrap list pinned to the seed and requests
   * never timed out, holds one Metadata request on the scenario's node and runs the replacement,
   * all under fake time. Fills `context` as it goes, so the caller can clean up after a failure.
   * @param {object} context receives `seam`, `cluster`, `gate`, `held`, `socket`, `broker`, `pool`;
   *   its `logger`, when set, is the client's
   * @param {object} replacement one of `REPLACEMENTS`
   * @returns {Promise<void>}
   */
  const leaveDraining = async (context, { node, sender, replace }) => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
    const seam = createScriptedCluster({
      nodes: [
        { id: 0, host: 'seed', port: 9092 },
        { id: 1, host: 'first', port: 9092 },
        { id: 2, host: 'second', port: 9092 },
        { id: 3, host: 'moved', port: 9092 },
      ],
    })
    const listed = [
      { nodeId: 0, host: 'seed', port: 9092, rack: null },
      { nodeId: 1, host: 'first', port: 9092, rack: null },
    ]
    seam.answer('Metadata', () => ({
      brokers: listed.map((entry) => ({ ...entry })),
      controllerId: 0,
      topics: [],
    }))
    const cluster = new Cluster({
      ...seam.kafkaOptions({
        brokers: ['seed:9092'],
        requestTimeout: BOUND,
        enforceRequestTimeout: false,
      }),
      logger: context.logger ?? newLogger(),
      retry: { retries: 0 },
      metadataRecoveryStrategy: 'none',
    })
    Object.assign(context, { seam, cluster })
    await cluster.connect()
    await cluster.refreshMetadata()
    const broker = await sender(cluster)
    context.broker = broker
    context.pool = broker.connectionPool
    context.gate = seam.schedule.hold({ api: 'Metadata', node })
    context.held = settled(broker.metadata([]))
    const { node: id, generation } = await context.gate.arrived()
    context.socket = seam
      .sockets()
      .find((socket) => seam.socketKeys.get(socket) === `${id}/${generation}`)
    await replace(cluster, listed)
  }

  /** @param {object} context what `leaveDraining` filled @returns {Promise<void>} */
  const cleanUp = async ({ cluster, gate }) => {
    if (gate?.call) gate.release()
    await cluster?.disconnect({ drainTimeout: 0 })
  }

  /** @param {object} context @returns {boolean} whether the old broker or pool is still tracked */
  const tracked = ({ cluster, broker, pool }) =>
    cluster.brokerPool.draining.has(broker) || cluster.brokerPool.drainingPools.has(pool)

  describe.each(Object.keys(REPLACEMENTS))('after %s', (name) => {
    it('a forced disconnect closes the old socket at once and rejects its held request', async () => {
      const context = {}
      try {
        await leaveDraining(context, REPLACEMENTS[name])
        await jest.advanceTimersByTimeAsync(BOUND / 2)
        await flush()
        expect(tracked(context)).toBe(true)
        expect(context.socket.destroyed).toBe(false)

        await context.cluster.disconnect({ drainTimeout: 0 })

        expect(context.socket.destroyed).toBe(true)
        await flush()
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
      } finally {
        await cleanUp(context)
      }
    })

    it('a zero-bound disconnect started from a log sink during the teardown closes the old socket and settles without waiting for the bound', async () => {
      const context = {}
      let armed = false
      context.logger = newLogger({
        level: LEVELS.DEBUG,
        logCreator:
          () =>
          ({ log }) => {
            if (!armed || log.message !== 'Waiting for pending requests') return
            armed = false
            context.nested = settled(context.cluster.disconnect({ drainTimeout: 0 }))
          },
      })
      const { replace, ...replacement } = REPLACEMENTS[name]
      try {
        await leaveDraining(context, {
          ...replacement,
          replace: async (cluster, listed) => {
            armed = true
            const replacing = replace(cluster, listed)
            // A rebootstrap the nested disconnect superseded rejects; only the close is checked here.
            context.replaced = settled(replacing)
            await replacing.catch(() => {})
          },
        })
        await flush()

        expect(context.replaced.done).toBe(true)
        expect(context.nested).toEqual({ done: true, value: undefined })
        expect(context.socket.destroyed).toBe(true)
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
      } finally {
        await cleanUp(context)
      }
    })

    it('a default disconnect waits only for the rest of the retirement bound, then closes the old socket', async () => {
      const context = {}
      try {
        await leaveDraining(context, REPLACEMENTS[name])
        await jest.advanceTimersByTimeAsync(40)
        await flush()
        expect(tracked(context)).toBe(true)
        expect(context.socket.destroyed).toBe(false)

        let disconnected = false
        const closing = context.cluster.disconnect().then(() => {
          disconnected = true
        })
        await jest.advanceTimersByTimeAsync(BOUND - 40 - 1)
        await flush()

        expect(disconnected).toBe(false)
        expect(context.socket.destroyed).toBe(false)
        expect(context.held.done).toBe(false)

        await jest.advanceTimersByTimeAsync(1)
        await flush()

        expect(disconnected).toBe(true)
        await closing
        expect(context.socket.destroyed).toBe(true)
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
      } finally {
        await cleanUp(context)
      }
    })
  })

  describe('when tearing down the old socket throws', () => {
    const SEED_POOL_MOVE = REPLACEMENTS['a seed address move that replaced the seed pool']

    /**
     * Leaves the replaced seed pool draining its held request, with the old socket's `destroy`
     * recorded, then starts a zero-bound disconnect. The fake clock is never moved.
     * @param {object} context as for `leaveDraining`; receives `destroy` and `close`
     * @param {(socket: import('events').EventEmitter, destroy: Function) => void} breakSocket
     *   makes the old socket's teardown throw
     * @returns {Promise<void>}
     */
    const disconnectBroken = async (context, breakSocket) => {
      await leaveDraining(context, SEED_POOL_MOVE)
      await flush()
      expect(tracked(context)).toBe(true)
      expect(context.socket.destroyed).toBe(false)
      const destroy = context.socket.destroy
      context.destroy = jest.fn((...args) => destroy(...args))
      context.socket.destroy = context.destroy
      breakSocket(context.socket, destroy)

      context.close = settled(context.cluster.disconnect({ drainTimeout: 0 }))
      await flush()
    }

    /** @param {object} context @returns {Promise<void>} cleanup that cannot wait on the bound */
    const cleanUpBroken = async (context) => {
      await jest.advanceTimersByTimeAsync(BOUND)
      await cleanUp(context)
    }

    it('a zero-bound disconnect during a replaced pool drain destroys the old socket when detaching its listeners throws, and rejects with that first failure', async () => {
      const context = {}
      const detachFailure = new Error('listener removal failed')
      try {
        await disconnectBroken(context, (socket, destroy) => {
          socket.on('removeListener', (event) => {
            if (event === 'data') throw detachFailure
          })
          context.destroy.mockImplementation((...args) => {
            destroy(...args)
            throw new Error('destroy failed after the socket closed')
          })
        })

        expect(context.destroy).toHaveBeenCalledTimes(1)
        expect(context.socket.destroyed).toBe(true)
        expect(context.close.done).toBe(true)
        expect(context.close.error).toBe(detachFailure)
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
        expect(context.cluster.brokerPool.drainingPools.size).toBe(0)
      } finally {
        await cleanUpBroken(context)
      }
    })

    it('a zero-bound disconnect during a replaced pool drain whose destroy closes the socket and then throws null rejects with null and releases the earlier drain', async () => {
      const context = {}
      const none = null
      try {
        await disconnectBroken(context, (socket, destroy) => {
          context.destroy.mockImplementation((...args) => {
            destroy(...args)
            throw none
          })
        })

        expect(context.destroy).toHaveBeenCalledTimes(1)
        expect(context.socket.destroyed).toBe(true)
        expect(context.close).toStrictEqual({ done: true, error: null })
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
        expect(context.cluster.brokerPool.drainingPools.size).toBe(0)
      } finally {
        await cleanUpBroken(context)
      }
    })

    it('a zero-bound disconnect during a replaced pool drain whose listener removal destroys the socket and then throws destroys it once and rejects with that failure', async () => {
      const context = {}
      const detachFailure = new Error('listener removal failed after closing the socket')
      try {
        await disconnectBroken(context, (socket) => {
          let armed = true
          socket.on('removeListener', (event) => {
            if (!armed || event !== 'data') return
            armed = false
            socket.destroy()
            throw detachFailure
          })
        })

        expect(context.destroy).toHaveBeenCalledTimes(1)
        expect(context.socket.destroyed).toBe(true)
        expect(context.close.done).toBe(true)
        expect(context.close.error).toBe(detachFailure)
        expect(context.held.error).toBeInstanceOf(KafkaJSConnectionClosedError)
        expect(context.cluster.brokerPool.drainingPools.size).toBe(0)
      } finally {
        await cleanUpBroken(context)
      }
    })
  })
})

describe('BrokerPool > a close failure whose reason is falsy is still reported', () => {
  const nothing = undefined
  const CLOSE_FAILED = 'Closing a replaced broker connection failed'

  /** @returns {{ broker: Broker, connectionPool: ConnectionPool }} a real broker over a real pool */
  const retirable = () => {
    const connectionPool = new ConnectionPool({ logger: newLogger(), host: 'b3', port: 9092 })
    return {
      connectionPool,
      broker: new Broker({ connectionPool, logger: newLogger(), nodeId: 3 }),
    }
  }

  /**
   * The pool of `refreshable(MOVED_SEED)` with the replaced seed pool's warnings recorded.
   * @returns {{ pool: BrokerPool, oldPool: ConnectionPool, failures: () => object[] }} `failures`
   *   gives the fields of every warning that a replaced connection failed to close
   */
  const replacing = () => {
    const { pool, seed } = refreshable(MOVED_SEED)
    const warn = jest.spyOn(pool.logger, 'warn')
    const failures = () =>
      warn.mock.calls.filter(([message]) => message === CLOSE_FAILED).map(([, extra]) => extra)
    return { pool, oldPool: seed.connectionPool, failures }
  }

  it('a broker retirement whose fencing throws undefined rejects with undefined, though its destroy then fails too', async () => {
    const { connectionPool, broker } = retirable()
    connectionPool.retire = jest.fn(() => {
      throw nothing
    })
    connectionPool.destroy = jest.fn().mockRejectedValue(new Error('destroy failed'))

    await expect(broker.retire()).rejects.toBeUndefined()
    expect(connectionPool.destroy).toHaveBeenCalledTimes(1)
  })

  it('a broker retirement whose held destroy rejects with null rejects with null once it does', async () => {
    const { connectionPool, broker } = retirable()
    const drain = deferred()
    connectionPool.destroy = jest.fn(() => drain.promise)
    const retiring = settled(broker.retire())
    await flush()
    expect(retiring.done).toBe(false)

    drain.reject(null)
    await flush()

    expect(retiring).toStrictEqual({ done: true, error: null })
  })

  it('a replaced pool whose fencing throws undefined reports undefined, though its destroy then fails too', async () => {
    const { pool, oldPool, failures } = replacing()
    oldPool.retire = jest.fn(() => {
      throw nothing
    })
    oldPool.destroy = jest.fn().mockRejectedValue(new Error('destroy failed'))

    await pool.refreshMetadata([])
    await flush()

    expect(oldPool.destroy).toHaveBeenCalledTimes(1)
    expect(failures()).toEqual([{ error: 'undefined' }])
  })

  it('a replaced pool whose held destroy rejects with null reports null once it does', async () => {
    const { pool, oldPool, failures } = replacing()
    const drain = deferred()
    oldPool.destroy = jest.fn(() => drain.promise)
    await pool.refreshMetadata([])
    await flush()
    expect(failures()).toEqual([])
    expect(pool.drainingPools.has(oldPool)).toBe(true)

    drain.reject(null)
    await flush()

    expect(failures()).toEqual([{ error: 'null' }])
    expect(pool.drainingPools.size).toBe(0)
  })
})
