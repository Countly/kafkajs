const apiKeys = require('../protocol/requests/apiKeys')
const { connectionOpts, createFakeSocketFactory } = require('testHelpers')
const ConnectionPool = require('./connectionPool')

describe('ConnectionPool', () => {
  /** @type {ConnectionPool} */
  let pool

  beforeEach(() => {
    pool = new ConnectionPool(connectionOpts({ socketFactory: createFakeSocketFactory().factory }))
  })

  afterEach(async () => {
    pool && (await pool.destroy())
  })

  it('should connect on getConnect()', async () => {
    const connection = await pool.getConnection()
    expect(connection.isConnected()).toBe(true)

    expect(pool.isConnected()).toBe(true)
    await pool.destroy()
    expect(pool.isConnected()).toBe(false)

    expect(connection.isConnected()).toBe(false)
  })

  it('should return different connection for Fetch requests', async () => {
    const heartbeatConnection = await pool.getConnectionByRequest({
      request: { apiKey: apiKeys.Heartbeat },
    })
    const fetchConnection = await pool.getConnectionByRequest({
      request: { apiKey: apiKeys.Fetch },
    })
    expect(heartbeatConnection !== fetchConnection).toBe(true)
  })

  it('should reconnect connection if it disconnects', async () => {
    let connection = await pool.getConnection()

    expect(connection.isConnected()).toBe(true)
    await connection.disconnect()
    expect(connection.isConnected()).toBe(false)

    connection = await pool.getConnection()
    expect(connection.isConnected()).toBe(true)
  })
})

describe('ConnectionPool structural invariant', () => {
  it.each([1, 2, 3])('sums every connection generation for %i entries', (size) => {
    const pool = new ConnectionPool(connectionOpts())
    pool.pool = Array.from({ length: size }, (_, i) => ({ generation: i + 3 }))
    const sum = Array.from({ length: size }, (_, i) => i + 3).reduce((a, b) => a + b, 0)
    expect(pool.generation).toBe(sum)
    pool.continueGenerationFrom(7)
    expect(pool.generation).toBe(7 + sum)
  })

  it('routes every api only within its two connection slots', async () => {
    const pool = new ConnectionPool(connectionOpts())
    expect(pool.pool).toHaveLength(2)
    const get = jest.spyOn(pool, 'getConnection').mockResolvedValue({})
    for (const apiKey of [...Object.values(apiKeys), -1, 1000]) {
      await pool.getConnectionByRequest({ request: { apiKey } })
      expect(get.mock.calls.at(-1)[0]).toBe(apiKey === apiKeys.Fetch ? 1 : 0)
    }
    expect(get.mock.calls.every(([index]) => index >= 0 && index < pool.pool.length)).toBe(true)
  })
})
