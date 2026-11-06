const ConnectionPool = require('../connectionPool')
const { newLogger, createFakeSocketFactory } = require('testHelpers')

/** API key, then the slot that serves it: Fetch, ShareFetch and ShareAcknowledge use slot 1. */
const ROUTES = [
  [1, 1],
  [76, 0],
  [77, 0],
  [78, 1],
  [79, 1],
  [90, 0],
  [91, 0],
  [92, 0],
  [12, 0],
  [-1, 0],
  [1000, 0],
]

/**
 * @param {object} [options] Pool overrides.
 * @returns {{pool: ConnectionPool, state: object}} A broker-free pool on in-process sockets.
 */
const fixture = (options = {}) => {
  const { factory, state } = createFakeSocketFactory()
  return {
    state,
    pool: new ConnectionPool({
      host: 'node0',
      port: 9092,
      clientId: 'test-client',
      logger: newLogger(),
      socketFactory: factory,
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      connectionsMaxIdle: 0,
      ...options,
    }),
  }
}

describe('share connection routing', () => {
  let pool
  afterEach(async () => {
    if (pool) await pool.destroy({ drainTimeout: 0 })
    pool = null
    jest.useRealTimers()
  })

  test.each(ROUTES)('routes API %s through slot %s', async (apiKey, slot) => {
    ;({ pool } = fixture())
    const connection = await pool.getConnectionByRequest({ request: { apiKey } })
    expect(connection).toBe(pool.pool[slot])
    expect(pool.pool[1 - slot].generation).toBe(0)
  })

  test('reads generation without opening a socket or summing its sibling', async () => {
    const f = fixture()
    pool = f.pool
    expect(pool.generationFor(78)).toBe(0)
    expect(f.state.sockets).toHaveLength(0)
    await pool.getConnection(0)
    const coordinator = pool.generationFor(76)
    expect(coordinator).toBeGreaterThan(0)
    expect(pool.generationFor(78)).toBe(0)
    await pool.getConnection(1)
    const data = pool.generationFor(78)
    expect(data).toBeGreaterThan(0)
    expect(pool.generationFor(76)).toBe(coordinator)
    expect(pool.generationFor(79)).toBe(data)
    expect(pool.generation).toBe(coordinator + data)
    await pool.pool[0].disconnect({ drainTimeout: 0 })
    await pool.getConnection(0)
    expect(pool.generationFor(76)).toBeGreaterThan(coordinator)
    expect(pool.generationFor(78)).toBe(data)
    const latestCoordinator = pool.generationFor(76)
    await pool.pool[1].disconnect({ drainTimeout: 0 })
    await pool.getConnection(1)
    expect(pool.generationFor(78)).toBeGreaterThan(data)
    expect(pool.generationFor(76)).toBe(latestCoordinator)
  })

  test('continues each established generation above a replaced pool baseline', async () => {
    const old = fixture().pool
    try {
      await old.getConnection(0)
      await old.getConnection(1)
      ;({ pool } = fixture())
      pool.continueGenerationFrom(old.generation)
      pool.continueGenerationFrom(0)
      await pool.getConnection(0)
      await pool.getConnection(1)
      expect(pool.generationFor(76)).toBeGreaterThan(old.generationFor(76))
      expect(pool.generationFor(78)).toBeGreaterThan(old.generationFor(78))
    } finally {
      await old.destroy({ drainTimeout: 0 })
    }
  })

  test('a refused disconnected request opens no socket and preserves its error identity', async () => {
    const f = fixture()
    pool = f.pool
    const refusal = new Error('closed operation')
    await expect(
      pool.getConnectionByRequest({ request: { apiKey: 78 } }, { guard: () => refusal })
    ).rejects.toBe(refusal)
    expect(f.state.sockets).toHaveLength(0)
    expect(pool.generationFor(78)).toBe(0)
    await pool.getConnectionByRequest({ request: { apiKey: 78 } }, { guard: () => null })
    expect(f.state.sockets).toHaveLength(1)
  })

  test('a retired pool cannot reconnect the data socket', async () => {
    const f = fixture()
    pool = f.pool
    pool.retire()
    await expect(pool.getConnectionByRequest({ request: { apiKey: 78 } })).rejects.toThrow(
      'replaced'
    )
    expect(f.state.sockets).toHaveLength(0)
  })

  test('a ready data socket reaps and reconnects without moving the coordinator generation', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'hrtime'] })
    ;({ pool } = fixture({ connectionsMaxIdle: 25 }))
    await pool.getConnection(1)
    const data = pool.generationFor(78)
    expect(pool.pool[1].isConnected()).toBe(true)
    await jest.advanceTimersByTimeAsync(26)
    expect(pool.pool[1].isConnected()).toBe(false)
    await pool.getConnection(1)
    expect(pool.generationFor(78)).toBeGreaterThan(data)
    expect(pool.generationFor(76)).toBe(0)
  })
})
