const BrokerPool = require('../brokerPool')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
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
