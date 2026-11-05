const BrokerPool = require('../brokerPool')
const builder = require('../connectionPoolBuilder')
const { ConnectionSetupPolicy } = require('../../network/connectionSetup')
const { KafkaJSConnectionError, KafkaJSProtocolError } = require('../../errors')
const { createScriptedCluster, NEGOTIATION_MS } = require('../../../testHelpers/scriptedCluster')
const { createLogger } = require('../../loggers')
const logger = createLogger({ level: 0, logCreator: () => () => {} })
let pool, seam, bootstrap

beforeEach(async () => {
  jest.useFakeTimers()
  seam = createScriptedCluster({
    nodes: [
      { id: 0, host: 'a', port: 9092 },
      { id: 1, host: 'b', port: 9092 },
    ],
  })
  bootstrap = ['a:9092']
  const connectionPoolBuilder = builder({
    brokers: () => bootstrap,
    logger,
    socketFactory: seam.socketFactory,
    requestTimeout: 100000,
    setupPolicy: ConnectionSetupPolicy.resolve({}, () => 0.5),
  })
  pool = new BrokerPool({
    connectionPoolBuilder,
    logger,
    retry: { retries: 0 },
    drainTimeout: 0,
  })
  await pool.createSeedBroker()
})
afterEach(async () => {
  await pool.disconnect({ drainTimeout: 0 })
  await jest.advanceTimersByTimeAsync(1)
  seam.assertClean()
  expect(seam.sockets().every((s) => s.destroyed)).toBe(true)
  expect(jest.getTimerCount()).toBe(0)
  jest.useRealTimers()
})

/** Counts two physical setups without ready, keeping the real owner and pool construction. @returns {void} */
const failTwice = async () => {
  seam.down(0)
  for (let n = 0; n < 2; n++) {
    const failed = pool.seedBroker.connect().catch((e) => e)
    await jest.advanceTimersByTimeAsync(n === 0 ? 1 : 50)
    expect(await failed).toBeInstanceOf(KafkaJSConnectionError)
  }
  seam.up(0)
}

it.each(['seed rotation', 'illegal SASL state', 'hint replacement', 'rebootstrap'])(
  'preserves endpoint revision through %s',
  async (path) => {
    await failTwice()
    const owner = pool.setupStates
    const current = pool.seedBroker
    pool.brokers[0] = current
    if (path === 'seed rotation') await pool.createSeedBroker()
    if (path === 'illegal SASL state') {
      jest.spyOn(current, 'connect').mockRejectedValueOnce(
        new KafkaJSProtocolError({
          type: 'ILLEGAL_SASL_STATE',
          code: 34,
          retriable: true,
          message: 'state',
        })
      )
      await expect(pool.connectBroker(current)).rejects.toThrow()
    }
    if (path === 'hint replacement') {
      await pool.applyNodeEndpoints([{ nodeId: 0, host: 'b', port: 9092, rack: null }])
      expect(owner.state('b:9092', 0).failures).toBe(0)
      await pool.applyNodeEndpoints([{ nodeId: 0, host: 'a', port: 9092, rack: null }])
    }
    if (path === 'rebootstrap') await pool.rebootstrap('required')
    expect(pool.seedBroker.connectionPool.setupStates).toBe(owner)
    expect(owner.state('a:9092', performance.now())).toMatchObject({ failures: 2, waitMs: 100 })
    const next = pool.seedBroker.connectionPool.beginSetup()
    expect(next.waitMs()).toBe(100)
    expect(next.timeout).toBe(30000)
    next.cancel()
  }
)

it('preserves crash cleanup state and clears explicit final disconnect state', async () => {
  await failTwice()
  await pool.disconnect({ preserveSetupState: true, drainTimeout: 0 })
  await pool.createSeedBroker()
  expect(pool.setupStates.state('a:9092', 0).failures).toBe(2)
  await pool.disconnect({ drainTimeout: 0 })
  await pool.createSeedBroker()
  expect(pool.setupStates.state('a:9092', 0).failures).toBe(0)
})

it('deletes state after a real ready and keeps a second endpoint independent', async () => {
  await failTwice()
  const second = await pool.buildConnectionPool({ host: 'b', port: 9092 })
  expect(second.setupStates).toBe(pool.setupStates)
  expect(second.setupStates.state('b:9092', 0).failures).toBe(0)
  const other = new (require('../../broker'))({ connectionPool: second, logger })
  const secondReady = other.connect()
  await jest.advanceTimersByTimeAsync(NEGOTIATION_MS)
  await secondReady
  expect(other.isConnected()).toBe(true)
  expect(pool.setupStates.state('a:9092', performance.now()).failures).toBe(2)
  const connect = pool.seedBroker.connect()
  await jest.advanceTimersByTimeAsync(110)
  await connect
  expect(pool.setupStates.entries.size).toBe(0)
  await second.destroy({ drainTimeout: 0 })
})

it('retains new failure state when reconnect reuses a disconnected seed', async () => {
  await pool.disconnect({ drainTimeout: 0 })
  const seed = pool.seedBroker
  seam.down(0)
  const failed = seed.connect().catch((e) => e)
  await jest.advanceTimersByTimeAsync(1)
  expect(await failed).toBeInstanceOf(KafkaJSConnectionError)
  expect(pool.setupStates.state('a:9092', performance.now()).failures).toBe(1)
  const next = seed.connectionPool.beginSetup()
  expect(next.timeout).toBe(20000)
  next.cancel()
})

it.each([false, true])(
  'consumer crash restart preserves setup policy and final disconnect clears it (close while deciding: %s)',
  async (closeWhileDeciding) => {
    jest.useRealTimers()
    const Runner = require('../../consumer/runner')
    const { CloseRig, NODE, gate, until } = require('../../consumer/__tests__/fixtures/closeRig')
    const Broker = require('../../broker')
    const decision = gate()
    const entered = gate()
    const start = jest.spyOn(Runner.prototype, 'start')
    const rig = await CloseRig.open({
      retry: {
        retries: 0,
        restartOnFailure: () => {
          entered.resolve()
          return decision.promise
        },
      },
    })
    const { Kafka } = require('../../../index')
    rig.kafka = new Kafka(
      rig.cluster.kafkaOptions({ connectionTimeout: undefined, retry: { retries: 0 } })
    )
    const lookup = rig.lookup()
    let auxiliary
    try {
      const { consumer } = await rig.started({
        consumer: {
          retry: {
            retries: 0,
            restartOnFailure: () => {
              entered.resolve()
              return decision.promise
            },
          },
        },
      })
      const owner = lookup.cluster.brokerPool
      const states = owner.setupStates
      auxiliary = await owner.buildConnectionPool({ host: NODE.host, port: NODE.port })
      rig.cluster.down(0)
      const failed = await new Broker({ connectionPool: auxiliary, logger })
        .connect()
        .catch((e) => e)
      expect(failed).toBeInstanceOf(KafkaJSConnectionError)
      expect(states.state(`${NODE.host}:${NODE.port}`, performance.now()).failures).toBe(1)
      const nextAttempt = auxiliary.beginSetup()
      expect(nextAttempt.timeout).toBeGreaterThanOrEqual(16000)
      expect(nextAttempt.timeout).toBeLessThanOrEqual(24000)
      nextAttempt.cancel()
      const crashing = start.mock.instances[0].onCrash(Object.assign(failed, { retryTime: 5 }))
      await entered.promise
      expect(states.state(`${NODE.host}:${NODE.port}`, performance.now()).failures).toBe(1)
      rig.cluster.up(0)
      if (closeWhileDeciding) await consumer.disconnect()
      decision.resolve(true)
      await crashing
      if (closeWhileDeciding) {
        await new Promise((resolve) => setTimeout(resolve, 20))
        expect(start).toHaveBeenCalledTimes(1)
      } else {
        await until(() => start.mock.calls.length === 2, 'the consumer restart')
      }
      await consumer.disconnect()
      expect(states.entries.size).toBe(0)
      expect(owner.setupStates).toBe(states)
    } finally {
      decision.resolve(false)
      rig.cluster.up(0)
      await auxiliary?.destroy({ drainTimeout: 0 })
      await rig.dispose()
      start.mockRestore()
      jest.useFakeTimers()
    }
  }
)

it('authoritative address replacement bounds retained failures', async () => {
  const connect = pool.seedBroker.connect()
  await jest.advanceTimersByTimeAsync(NEGOTIATION_MS)
  await connect
  const owner = pool.setupStates
  const removed = owner.begin('removed:9092')
  let metadataHost = '::1'
  let rack = null
  seam.answer('Metadata', () => ({
    brokers: [
      { nodeId: 0, host: 'a', port: 9092, rack: null },
      { nodeId: 1, host: metadataHost, port: 9092, rack },
    ],
    controllerId: 0,
    topics: [],
  }))
  for (let n = 0; n < 1000; n++) {
    metadataHost = n === 0 ? '::1' : `node-${n}`
    bootstrap = ['a:9092', `bootstrap-${n}:9092`]
    const unused = await pool.buildConnectionPool()
    await unused.destroy({ drainTimeout: 0 })
    const refresh = pool.refreshMetadata([])
    await jest.advanceTimersByTimeAsync(1)
    await refresh
    const address = `${metadataHost}:9092`
    const token = owner.begin(address)
    owner.failed(address, performance.now(), token)
    expect(owner.entries.size).toBeLessThanOrEqual(2)
    expect(owner.endpoints).toEqual(new Set(['a:9092', `bootstrap-${n}:9092`, address]))
  }
  rack = 'changed'
  const refresh = pool.refreshMetadata([])
  await jest.advanceTimersByTimeAsync(1)
  await refresh
  expect(owner.state(`${metadataHost}:9092`, performance.now()).failures).toBe(1)
  owner.finish('removed:9092', removed)
  expect(owner.entries.has('removed:9092')).toBe(false)
  const alias = owner.begin('alias:9092')
  expect(owner.state('alias:9092', performance.now()).failures).toBe(0)
  owner.finish('alias:9092', alias)
})

it('a ready node resets reconnect growth without delaying another node', async () => {
  const connection = pool.seedBroker.connectionPool.pool[0]
  const original = connection.socketFactory
  const starts = []
  connection.socketFactory = (options) => {
    starts.push(performance.now())
    return original(options)
  }
  seam.down(0)
  const first = pool.seedBroker.connect().catch((e) => e)
  await jest.advanceTimersByTimeAsync(1)
  expect(await first).toBeInstanceOf(KafkaJSConnectionError)
  const state = pool.setupStates.state('a:9092', performance.now())
  const second = pool.seedBroker.connect().catch((e) => e)
  await jest.advanceTimersByTimeAsync(state.waitMs - 1)
  expect(starts).toHaveLength(1)
  await jest.advanceTimersByTimeAsync(2)
  expect(await second).toBeInstanceOf(KafkaJSConnectionError)
  expect(starts[1]).toBe(state.retryAt)
  const after = pool.setupStates.state('a:9092', performance.now())
  expect(after.failures).toBe(2)
  const otherPool = await pool.buildConnectionPool({ host: 'b', port: 9092 })
  const other = new (require('../../broker'))({ connectionPool: otherPool, logger })
  try {
    const otherReady = other.connect()
    await jest.advanceTimersByTimeAsync(NEGOTIATION_MS)
    await otherReady
    expect(other.isConnected()).toBe(true)
    expect(starts).toHaveLength(2)
    seam.up(0)
    const ready = pool.seedBroker.connect()
    const remaining = pool.setupStates.state('a:9092', performance.now()).waitMs
    await jest.advanceTimersByTimeAsync(remaining - 1)
    expect(starts).toHaveLength(2)
    await jest.advanceTimersByTimeAsync(10)
    await ready
    expect(starts[2]).toBe(after.retryAt)
    expect(pool.setupStates.state('a:9092', performance.now()).failures).toBe(0)
    const next = pool.seedBroker.connectionPool.beginSetup()
    expect(next.timeout).toBe(10000)
    expect(next.waitMs()).toBe(0)
    next.cancel()
  } finally {
    await other.disconnect({ drainTimeout: 0 })
  }
})
