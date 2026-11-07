const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const { newLogger, readRequestHeader } = require('testHelpers')
const { createScriptedCluster, shareScript } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const literal = require('../../../testHelpers/wire/expected')
const { requests } = require('../../protocol/requests')
const { KafkaJSConnectionClosedError } = require('../../errors')
const { flush, until } = require('../../../testHelpers/shareHarness')

/** Broker method, API name, API key and the one released version, for the three runtime pairs. */
const APIS = [
  ['shareGroupHeartbeat', 'ShareGroupHeartbeat', 76, 1],
  ['shareFetch', 'ShareFetch', 78, 2],
  ['shareAcknowledge', 'ShareAcknowledge', 79, 2],
]

/** Broker method, API name, API key and the one released version, for all seven share APIs. */
const ALL = [
  ['shareGroupHeartbeat', 'ShareGroupHeartbeat', 76, 1],
  ['shareGroupDescribe', 'ShareGroupDescribe', 77, 1],
  ['shareFetch', 'ShareFetch', 78, 2],
  ['shareAcknowledge', 'ShareAcknowledge', 79, 2],
  ['describeShareGroupOffsets', 'DescribeShareGroupOffsets', 90, 1],
  ['alterShareGroupOffsets', 'AlterShareGroupOffsets', 91, 0],
  ['deleteShareGroupOffsets', 'DeleteShareGroupOffsets', 92, 0],
]

/** The Broker's private common sender, which every share method calls. */
const SEND_REQUEST = Object.getOwnPropertySymbols(Broker.prototype).find((symbol) =>
  String(symbol).includes('sendRequest')
)

/**
 * @param {object} [options] Pool overrides.
 * @returns {Promise<object>} A connected Broker and its Pool on in-process sockets, with the
 *   scripted cluster and its schedule. Every answer carries no throttle.
 * @throws {Error} What `broker.connect()` throws, after the pool is destroyed.
 */
const fixture = async (options = {}) => {
  const schedule = new Schedule()
  const cluster = shareScript(
    createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'share',
      schedule,
    }),
    APIS.map(([method, api, , version]) => ({
      api,
      response: { ...literal[method + 'V' + version].response, throttleTime: 0 },
    }))
  )
  const pool = new ConnectionPool({
    host: 'node0',
    port: 9092,
    clientId: 'test-client',
    socketFactory: cluster.socketFactory,
    logger: newLogger(),
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 0,
    ...options,
  })
  const broker = new Broker({ connectionPool: pool, logger: newLogger() })
  try {
    await broker.connect()
  } catch (error) {
    await pool.destroy({ drainTimeout: 0 })
    throw error
  }
  return { broker, pool, cluster, schedule }
}

/**
 * Builds the complete request frame a client must write, from a literal body.
 * @param {number} key API key.
 * @param {number} version Wire version.
 * @param {number} correlationId Header correlation id.
 * @param {string} hex Independent body hex.
 * @returns {Buffer} The size-prefixed frame with the flexible (v2) request header.
 */
const frame = (key, version, correlationId, hex) => {
  const body = Buffer.from(hex, 'hex')
  const client = Buffer.from('test-client')
  const out = Buffer.alloc(15 + client.length + body.length)
  out.writeInt32BE(out.length - 4, 0)
  out.writeInt16BE(key, 4)
  out.writeInt16BE(version, 6)
  out.writeInt32BE(correlationId, 8)
  out.writeInt16BE(client.length, 12)
  client.copy(out, 14)
  out[14 + client.length] = 0
  body.copy(out, 15 + client.length)
  return out
}

describe('share broker requests', () => {
  let f
  afterEach(async () => {
    if (f) {
      await f.pool.destroy({ drainTimeout: 0 })
      f.cluster.assertClean()
      f = null
    }
    jest.restoreAllMocks()
  })

  test.each(APIS)(
    '%s writes its literal complete flexible frame',
    async (method, api, key, version) => {
      f = await fixture()
      const fields = literal[method + 'V' + version]
      await f.broker[method](fields.request)
      const written = f.cluster
        .sockets()
        .flatMap((socket) => socket.writes)
        .find((bytes) => readRequestHeader(bytes).apiKey === key)
      const header = readRequestHeader(written)
      expect(written).toEqual(frame(key, version, header.correlationId, fields.requestHex))
      expect(f.cluster.requests().filter((record) => record.api === api)).toHaveLength(1)
      expect(f.cluster.requests().find((record) => record.api === api).fields).toEqual(
        fields.request
      )
    }
  )

  test('provides all seven Broker methods without executing an absent admin pair', async () => {
    f = await fixture()
    for (const method of [
      'shareGroupHeartbeat',
      'shareGroupDescribe',
      'shareFetch',
      'shareAcknowledge',
      'describeShareGroupOffsets',
      'alterShareGroupOffsets',
      'deleteShareGroupOffsets',
    ]) {
      expect(typeof f.broker[method]).toBe('function')
    }
  })

  test.each([
    ['shareGroupHeartbeat', 'ShareGroupHeartbeat', 76],
    ['shareGroupDescribe', 'ShareGroupDescribe', 77],
    ['shareFetch', 'ShareFetch', 78],
    ['shareAcknowledge', 'ShareAcknowledge', 79],
    ['describeShareGroupOffsets', 'DescribeShareGroupOffsets', 90],
    ['alterShareGroupOffsets', 'AlterShareGroupOffsets', 91],
    ['deleteShareGroupOffsets', 'DeleteShareGroupOffsets', 92],
  ])(
    '%s asks for its own API key and writes nothing when the broker range does not overlap',
    async (method, apiName, apiKey) => {
      f = await fixture()
      f.broker.versions[apiKey] = { minVersion: 9, maxVersion: 9 }
      const before = f.cluster.requests().length
      await expect(f.broker[method]({})).rejects.toMatchObject({ apiKey, apiName })
      expect(f.cluster.requests()).toHaveLength(before)
    }
  )

  test.each(APIS)(
    '%s leaves unsupported-version 35 as caller data',
    async (method, api, key, version) => {
      f = await fixture()
      f.cluster.answer(api, () => ({
        errorCode: 35,
        errorMessage: 'not enabled',
        throttleTime: 0,
      }))
      const result = await f.broker[method](literal[method + 'V' + version].request)
      expect(result).toMatchObject({ errorCode: 35, errorMessage: 'not enabled' })
    }
  )

  test.each(ALL)('%s refuses an invalid guard before lookup and encoding', async (method) => {
    f = await fixture()
    const refusal = new Error('expired operation')
    const before = f.cluster.requests().length
    const lookup = jest.spyOn(f.broker, 'lookupRequest')
    await expect(f.broker[method]({}, { guard: () => refusal })).rejects.toBe(refusal)
    expect(lookup).not.toHaveBeenCalled()
    expect(f.cluster.requests()).toHaveLength(before)
  })

  test.each(ALL)(
    '%s returns an expired guard refusal before it reads its request',
    async (method) => {
      f = await fixture()
      const refusal = new Error('expired operation')
      const reads = jest.fn()
      const observed = {}
      for (const name of ['checkCrcs', 'groupId']) {
        Object.defineProperty(observed, name, { enumerable: true, get: reads })
      }
      for (const request of [null, observed]) {
        await expect(f.broker[method](request, { guard: () => refusal })).rejects.toBe(refusal)
      }
      expect(reads).not.toHaveBeenCalled()
    }
  )

  test.each(APIS)(
    '%s writes its frame when the guard allows the operation',
    async (method, api, key, version) => {
      f = await fixture()
      await f.broker[method](literal[method + 'V' + version].request, { guard: () => null })
      expect(f.cluster.requests().filter((record) => record.api === api)).toHaveLength(1)
    }
  )

  test.each(ALL)(
    '%s forwards its guard unchanged to the common sender and returns the sender answer',
    async (method, api, key, version) => {
      f = await fixture()
      const guard = () => null
      const answer = { errorCode: 0 }
      const send = jest.fn().mockResolvedValue(answer)
      f.broker[SEND_REQUEST] = send
      await expect(f.broker[method]({}, { guard })).resolves.toBe(answer)
      expect(send).toHaveBeenCalledTimes(1)
      const [protocolRequest, forwarded] = send.mock.calls[0]
      expect(forwarded).toBe(guard)
      expect(protocolRequest.request).toMatchObject({ apiKey: key, apiVersion: version })
      expect(f.cluster.requests().filter((record) => record.api === api)).toHaveLength(0)
    }
  )

  test('keeps the original data socket when a share heartbeat fails and reconnects', async () => {
    f = await fixture()
    await f.broker.shareFetch(literal.shareFetchV2.request)
    const data = f.pool.pool[1]
    const socket = data.socket
    const generation = f.pool.generationFor(78)
    const gate = f.schedule.hold({ api: 'ShareGroupHeartbeat' })
    const heartbeat = f.broker.shareGroupHeartbeat(literal.shareGroupHeartbeatV1.request)
    const observed = heartbeat.then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    try {
      await until(() => gate.call !== null, 'the written share heartbeat')
      f.pool.pool[0].socket.emit('end')
      const { error } = await observed
      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(error.message).toBe('Closed connection')
      await flush()
      expect(data.socket).toBe(socket)
      expect(socket.destroyed).toBe(false)
      expect(f.pool.generationFor(78)).toBe(generation)
      gate.release()
      await f.broker.shareGroupHeartbeat(literal.shareGroupHeartbeatV1.request)
      await f.broker.shareFetch(literal.shareFetchV2.request)
      expect(data.socket).toBe(socket)
      expect(f.pool.generationFor(78)).toBe(generation)
      expect(f.cluster.requests().filter(({ apiKey }) => apiKey === 76)).toHaveLength(2)
      expect(f.cluster.requests().filter(({ apiKey }) => apiKey === 78)).toHaveLength(2)
    } finally {
      if (gate.settle) gate.release()
    }
  })

  test('refuses a guard invalidated during a data socket reconnect', async () => {
    f = await fixture()
    const shareFetches = () => f.cluster.requests().filter(({ apiKey }) => apiKey === 78).length
    await f.broker.shareFetch(literal.shareFetchV2.request)
    const connection = f.pool.pool[1]
    await connection.disconnect({ drainTimeout: 0 })
    expect(connection.isConnected()).toBe(false)
    expect(shareFetches()).toBe(1)

    const refusal = new Error('closed during connect')
    let current = true
    const connect = connection.connect.bind(connection)
    const reconnect = jest.spyOn(connection, 'connect').mockImplementation(async (...args) => {
      await connect(...args)
      current = false
    })
    await expect(
      f.broker.shareFetch(literal.shareFetchV2.request, { guard: () => (current ? null : refusal) })
    ).rejects.toBe(refusal)
    expect(reconnect).toHaveBeenCalledTimes(1)
    expect(shareFetches()).toBe(1)
  })

  test('refuses a guard invalidated while encoding', async () => {
    f = await fixture()
    let current = true
    const refusal = new Error('closed during encode')
    const lookup = f.broker.lookupRequest.bind(f.broker)
    jest.spyOn(f.broker, 'lookupRequest').mockImplementation((key, definition) => {
      const build = lookup(key, definition)
      return (fields) => {
        const built = build(fields)
        const encode = built.request.encode
        built.request.encode = async () => {
          const value = await encode()
          current = false
          return value
        }
        return built
      }
    })
    await expect(
      f.broker.shareFetch(literal.shareFetchV2.request, { guard: () => (current ? null : refusal) })
    ).rejects.toBe(refusal)
    expect(f.cluster.requests().filter(({ apiKey }) => apiKey === 78)).toHaveLength(0)
  })

  test('rechecks a queued acknowledgement guard at transmission', async () => {
    f = await fixture({ maxInFlightRequests: 1 })
    const gate = f.schedule.hold({ api: 'ShareFetch' })
    const fetching = f.broker.shareFetch(literal.shareFetchV2.request)
    const fetchObserved = fetching.then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    await gate.arrived()
    let current = true
    const refusal = new Error('closed in queue')
    const queued = f.broker.shareAcknowledge(literal.shareAcknowledgeV2.request, {
      guard: () => (current ? null : refusal),
    })
    const queuedObserved = queued.then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    for (let turn = 0; turn < 100 && f.pool.pool[1].requestQueue.pending.length === 0; turn++)
      await new Promise(setImmediate)
    expect(f.pool.pool[1].requestQueue.pending.length).toBe(1)
    current = false
    gate.release()
    expect(await fetchObserved).toHaveProperty('value')
    expect(await queuedObserved).toEqual({ error: refusal })
    expect(f.cluster.requests().filter(({ apiKey }) => apiKey === 79)).toHaveLength(0)
  })

  test('preserves hints, exact acquired offsets and decode settings', async () => {
    f = await fixture()
    f.cluster.answer('ShareFetch', () => ({ ...literal.shareFetchV2.response, throttleTime: 0 }))
    const decode = jest.spyOn(requests.ShareFetch.protocol({ version: 2 })().response, 'decode')
    const result = await f.broker.shareFetch({
      ...literal.shareFetchV2.request,
      checkCrcs: false,
      maxDecompressedBatchBytes: 5120,
    })
    expect(decode).toHaveBeenCalledWith(expect.any(Buffer), {
      checkCrcs: false,
      maxDecompressedBatchBytes: 5120,
    })
    expect(result.responses[0].partitions[0].acquiredRecords).toEqual(
      literal.shareFetchV2.response.responses[0].partitions[0].acquiredRecords
    )
    expect(result.responses[0].partitions[0].messages[0].offset).toBe('9007199254740993')
    expect(result.responses[0].partitions[1]).toMatchObject({
      errorCode: 6,
      acknowledgeErrorCode: 121,
    })
    expect(result.responses[0].partitions[0].currentLeader).toEqual({
      leaderId: 2,
      leaderEpoch: 2147483647,
    })
    expect(result.responses[0].partitions[1].currentLeader).toEqual({
      leaderId: 3,
      leaderEpoch: 12,
    })
    expect(result.nodeEndpoints).toEqual(literal.shareFetchV2.response.nodeEndpoints)
  })

  test('decodes with checksums checked and no size ceiling when the call names no decode options', async () => {
    f = await fixture()
    const decode = jest.spyOn(requests.ShareFetch.protocol({ version: 2 })().response, 'decode')
    await f.broker.shareFetch(literal.shareFetchV2.request)
    expect(decode).toHaveBeenCalledTimes(1)
    const [bytes, options] = decode.mock.calls[0]
    expect(bytes).toBeInstanceOf(Buffer)
    expect(options.checkCrcs).toBe(true)
    expect(options.maxDecompressedBatchBytes).toBeUndefined()
  })

  test('keeps the decode options of one call out of the next call', async () => {
    f = await fixture()
    const decode = jest.spyOn(requests.ShareFetch.protocol({ version: 2 })().response, 'decode')
    const request = literal.shareFetchV2.request
    await f.broker.shareFetch({ ...request, checkCrcs: false, maxDecompressedBatchBytes: 5120 })
    await f.broker.shareFetch(request)
    await f.broker.shareFetch({ ...request, checkCrcs: false })
    expect(decode.mock.calls.map(([, options]) => options)).toEqual([
      { checkCrcs: false, maxDecompressedBatchBytes: 5120 },
      { checkCrcs: true },
      { checkCrcs: false },
    ])
  })

  test('refuses preview ShareFetch before a share frame is written', async () => {
    f = await fixture()
    f.broker.versions[78] = { minVersion: 1, maxVersion: 1 }
    await expect(f.broker.shareFetch(literal.shareFetchV2.request)).rejects.toMatchObject({
      apiKey: 78,
      apiName: 'ShareFetch',
    })
    expect(f.cluster.requests().filter(({ apiKey }) => apiKey === 78)).toHaveLength(0)
  })
})
