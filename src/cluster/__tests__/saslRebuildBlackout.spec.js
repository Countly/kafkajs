const BrokerPool = require('../brokerPool')
const connectionPoolBuilder = require('../connectionPoolBuilder')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

const NODES = [
  { id: 0, host: 'node-0', port: 9092 },
  { id: 1, host: 'node-1', port: 9093 },
]
const SASL = { mechanism: 'plain', username: 'u', password: 'p' }

/**
 * A pool rebuilt after ILLEGAL_SASL_STATE (`BrokerPool#connectBroker`) must keep the address's
 * authentication blackout. Node 0's broker starts its handshake; while it is in flight the seed,
 * rotated to node 0's address, fails authentication and so blacks the address out; the handshake
 * then answers ILLEGAL_SASL_STATE, the pool is rebuilt and retried, and the rebuilt pool refuses
 * to reconnect. The first seed is node 1, so node 0's broker is a broker object of its own.
 */
describe('Cluster > BrokerPool > an ILLEGAL_SASL_STATE rebuild inside an authentication blackout', () => {
  let cluster, brokerPool

  beforeEach(async () => {
    cluster = createScriptedCluster({ nodes: NODES, profile: 'sasl' })
    brokerPool = new BrokerPool({
      connectionPoolBuilder: connectionPoolBuilder({
        socketFactory: cluster.socketFactory,
        brokers: ['node-1:9093', 'node-0:9092'],
        sasl: SASL,
        clientId: 'blackout',
        requestTimeout: 5000,
        connectionTimeout: 1000,
        logger: newLogger(),
      }),
      logger: newLogger(),
      retry: { retries: 2, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
    })
    await brokerPool.connect()
    await brokerPool.refreshMetadata([])
  })

  afterEach(async () => {
    await brokerPool.disconnect()
    cluster.assertClean()
  })

  /** @returns {object[]} the SaslHandshake requests written so far */
  const handshakes = () => cluster.requests().filter(({ api }) => api === 'SaslHandshake')

  test('the rebuilt pool refuses to reconnect, and writes no handshake', async () => {
    const node = brokerPool.brokers[0]
    const firstPool = node.connectionPool
    const handshake = cluster.schedule.hold({ api: 'SaslHandshake' })
    const connecting = brokerPool.findBroker({ nodeId: 0 })
    connecting.catch(() => {})
    await handshake.arrived()

    cluster.schedule.once(
      { api: 'SaslAuthenticate' },
      { errorCode: 58, errorMessage: 'bad credentials', authBytes: null }
    )
    await brokerPool.createSeedBroker()
    await expect(brokerPool.seedBroker.connect()).rejects.toThrow(KafkaJSSASLAuthenticationError)
    const written = handshakes().length

    handshake.release({ errorCode: 34, enabledMechanisms: [] })

    await expect(connecting).rejects.toThrow(
      /SASL authentication recently failed for node-0:9092 \(bad credentials\)/
    )
    expect(node.connectionPool).not.toBe(firstPool)
    expect(node.connectionPool.authenticationBlackout).toBe(brokerPool.authenticationBlackout)
    expect(handshakes()).toHaveLength(written)
  })

  test('without a blackout the rebuilt pool connects again', async () => {
    const node = brokerPool.brokers[0]
    const firstPool = node.connectionPool
    cluster.schedule.once({ api: 'SaslHandshake' }, { errorCode: 34, enabledMechanisms: [] })

    const broker = await brokerPool.findBroker({ nodeId: 0 })

    expect(broker).toBe(node)
    expect(node.connectionPool).not.toBe(firstPool)
    expect(node.isConnected()).toBe(true)
  })
})
