const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const { requests } = require('../../protocol/requests')
const { KafkaJSConnectionError } = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { createLogger } = require('../../loggers')
const Long = require('../../utils/long')
const logger = createLogger({ level: 0, logCreator: () => () => {} })
const fetchRequest = () =>
  requests.Fetch.protocol({ version: 18 })({
    maxWaitTime: 100,
    minBytes: 0,
    maxBytes: 1000,
    topics: [],
    sessionId: 0,
    sessionEpoch: -1,
    rackId: '',
    forgottenTopics: [],
  })
let seam, broker, gate

/** Builds a stack with a whole-SASL cap and one deterministic node. @param {object|null} [sasl] mechanism configuration @returns {void} */
const arrange = (sasl = null) => {
  const schedule = new Schedule()
  seam = createScriptedCluster({
    nodes: [{ id: 0, host: 'node', port: 9092 }],
    schedule,
    profile: 'sasl',
  })
  const pool = new ConnectionPool({
    host: 'node',
    port: 9092,
    logger,
    socketFactory: seam.socketFactory,
    connectionTimeout: 2000,
    authenticationTimeout: 400,
    requestTimeout: 5000,
    sasl,
  })
  broker = new Broker({
    connectionPool: pool,
    logger,
    authenticationTimeout: 400,
  })
}

beforeEach(() => {
  jest.useFakeTimers()
  arrange()
})
afterEach(async () => {
  await broker.disconnect({ drainTimeout: 0 })
  if (gate?.settle) gate.release(seam.apiVersionsAnswer())
  await jest.advanceTimersByTimeAsync(1)
  seam.assertClean()
  expect(seam.sockets().every((s) => s.destroyed)).toBe(true)
  expect(jest.getTimerCount()).toBe(0)
  gate = null
  jest.useRealTimers()
})

it.each(['direct Fetch', 'periodic reauthentication'])(
  'caps %s through the common authentication entry',
  async (mode) => {
    let held = false
    let release
    const promise = new Promise((resolve) => {
      release = resolve
    })
    const sasl = {
      mechanism: 'plain',
      authenticationProvider: () => ({
        authenticate: () => (held ? promise : Promise.resolve()),
      }),
    }
    arrange(sasl)
    const initial = broker.connect()
    await jest.advanceTimersByTimeAsync(10)
    await initial
    const acquiring = broker.connectionPool.getConnection(1)
    await jest.advanceTimersByTimeAsync(5)
    const connection = await acquiring
    if (mode === 'periodic reauthentication') {
      const authentication = connection.authenticate()
      await jest.advanceTimersByTimeAsync(5)
      await authentication
      connection.sessionLifetime = Long.fromValue(1)
    }
    held = true
    let error
    const sending = connection.send(fetchRequest()).catch((e) => {
      error = e
    })
    await jest.advanceTimersByTimeAsync(399)
    expect(error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    await sending
    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.message).toMatch(/SASL/)
    expect(connection.authenticatedAt).toBeNull()
    expect(connection.socket).toBeNull()
    release()
    await jest.advanceTimersByTimeAsync(1)
    expect(seam.requests().filter((r) => r.api === 'Fetch')).toHaveLength(0)
  }
)

it('does not wait for a healthy sibling Fetch when ApiVersions expires', async () => {
  gate = seam.schedule.hold({ api: 'ApiVersions' })
  let failure
  const setup = broker.connect().catch((error) => {
    failure = error
  })
  await jest.advanceTimersByTimeAsync(5)
  const acquiring = broker.connectionPool.getConnection(1)
  await jest.advanceTimersByTimeAsync(5)
  const sibling = await acquiring
  const heldFetch = seam.schedule.hold({ api: 'Fetch' })
  const fetch = sibling.send(fetchRequest())
  await jest.advanceTimersByTimeAsync(5)
  expect(heldFetch.call.api).toBe('Fetch')
  await jest.advanceTimersByTimeAsync(2000)
  await setup
  expect(failure).toBeInstanceOf(KafkaJSConnectionError)
  expect(sibling.socket.destroyed).toBe(false)
  heldFetch.release({ responses: [] })
  await jest.advanceTimersByTimeAsync(5)
  await fetch
})
