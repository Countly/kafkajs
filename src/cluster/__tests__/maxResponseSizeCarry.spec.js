jest.mock('../../network/connectionPool')

const ConnectionPool = require('../../network/connectionPool')
const Cluster = require('../index')
const connectionPoolBuilder = require('../connectionPoolBuilder')
const { newLogger } = require('testHelpers')

describe('Cluster > maxResponseSize is handed to every connection pool', () => {
  beforeEach(() => ConnectionPool.mockClear())

  test('the builder passes it on', async () => {
    const builder = connectionPoolBuilder({
      brokers: ['localhost:9092'],
      clientId: 'test-client',
      logger: newLogger(),
      maxResponseSize: 4096,
    })

    await builder.build()

    expect(ConnectionPool).toHaveBeenCalledTimes(1)
    expect(ConnectionPool.mock.calls[0][0]).toMatchObject({ maxResponseSize: 4096 })
  })

  test('the cluster gives it to its builder', async () => {
    const cluster = new Cluster({
      brokers: ['localhost:9092'],
      clientId: 'test-client',
      logger: newLogger(),
      maxResponseSize: 8192,
    })

    await cluster.connectionPoolBuilder.build({ host: 'broker', port: 9093 })

    expect(ConnectionPool.mock.calls[0][0]).toMatchObject({
      host: 'broker',
      port: 9093,
      maxResponseSize: 8192,
    })
  })

  test('the cluster keeps the store of offset operations it was given', () => {
    const operations = new Map()
    const cluster = new Cluster({
      brokers: ['localhost:9092'],
      clientId: 'test-client',
      logger: newLogger(),
      operations,
    })

    expect(cluster.operationsByGroup).toBe(operations)
    expect(
      new Cluster({ brokers: ['localhost:9092'], clientId: 'c', logger: newLogger() })
        .operationsByGroup
    ).toBeInstanceOf(Map)
  })
})
