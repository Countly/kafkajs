const Broker = require('../index')
const { createConnectionPool, connectionOpts, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > reconnect re-negotiation', () => {
  let broker

  beforeEach(() => {
    broker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('re-runs ApiVersions after a reconnect and keeps working', async () => {
    await broker.connect()
    const firstVersions = broker.versions
    const firstGeneration = broker.versionsGeneration

    expect(firstVersions[18].maxVersion).toBeGreaterThanOrEqual(4)

    await broker.disconnect()
    await broker.connect()

    expect(broker.versionsGeneration).toBeGreaterThan(firstGeneration)
    expect(broker.versionsGeneration).toEqual(broker.connectionPool.generation)
    expect(broker.versions[18]).toEqual(firstVersions[18])

    const metadata = await retryProtocol('LEADER_NOT_AVAILABLE', async () => broker.metadata([]))
    expect(metadata.brokers.length).toBeGreaterThan(0)
  })

  test('measures the cost of the extra round trip', async () => {
    await broker.connect()
    await broker.disconnect()

    const start = process.hrtime.bigint()
    await broker.connect()
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6

    // Not an assertion on speed — recorded for the release notes. Keep the bound loose enough
    // that a slow CI box cannot fail it.
    expect(elapsedMs).toBeLessThan(5000)
    console.log(`reconnect with re-negotiation: ${elapsedMs.toFixed(1)}ms`)
  })
})
