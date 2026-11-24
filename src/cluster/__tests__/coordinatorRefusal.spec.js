jest.mock('../../utils/shuffle', () => (brokers) => brokers)

const Cluster = require('../index')
const { KafkaJSConnectionError, KafkaJSGroupCoordinatorNotFound } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { newLogger, connectionOpts } = require('testHelpers')

const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
const NOT_COORDINATOR = 16

/**
 * A real `Cluster` whose broker pool holds stub brokers, one per behaviour: `'down'` fails to
 * connect, a number answers FindCoordinator with that error code, and `'ok'` names itself as the
 * coordinator. With `seedOnly`, the pool holds no broker table and the first behaviour is the seed.
 * @param {Array<'down'|'ok'|number>} behaviours
 * @param {{ seedOnly?: boolean }} [options]
 * @returns {{ cluster: Cluster, thrown: Error[], outcomes: string[] }} the cluster, the errors
 *   the brokers' FindCoordinator answers threw, and broker outcomes in observed order
 */
const createCluster = (behaviours, { seedOnly = false } = {}) => {
  const cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retry: { retries: 0 } })
  const pool = cluster.brokerPool
  const thrown = []
  const outcomes = []
  const brokers = behaviours.map((behaviour, index) => {
    const nodeId = index + 1
    return {
      nodeId,
      behaviour,
      isConnected: () => false,
      connectionPool: { host: `kafka${nodeId}`, port: 9092, rack: null },
      findGroupCoordinator: async () => {
        if (behaviour === 'ok') {
          outcomes.push(`broker ${nodeId} answered with the coordinator`)
          return {
            host: `kafka${nodeId}`,
            coordinator: { nodeId, host: `kafka${nodeId}`, port: 9092 },
          }
        }
        const error = createErrorFromCode(behaviour)
        outcomes.push(`broker ${nodeId} answered ${error.code}`)
        thrown.push(error)
        throw error
      },
    }
  })

  pool.connectBroker = async (broker) => {
    if (broker.behaviour === 'down') {
      outcomes.push(`broker ${broker.nodeId} failed to connect`)
      throw new KafkaJSConnectionError(
        `Connection error: connect ECONNREFUSED kafka${broker.nodeId}`
      )
    }
  }
  if (seedOnly) {
    pool.brokers = {}
    pool.findConnectedBroker = async () => brokers[0]
  } else {
    pool.brokers = Object.fromEntries(brokers.map((broker) => [String(broker.nodeId), broker]))
  }
  return { cluster, thrown, outcomes }
}

/**
 * @param {Cluster} cluster
 * @returns {Promise<unknown>} what the lookup resolved with, or the error it rejected with
 */
const lookUp = (cluster) =>
  cluster
    .findGroupCoordinatorMetadata({
      groupId: 'txn-a',
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })
    .catch((error) => error)

describe('Cluster > a coordinator lookup refused by a broker', () => {
  test('a refusal from every broker is the cause of the not-found error', async () => {
    for (const { behaviours, seedOnly } of [
      {
        behaviours: [TRANSACTIONAL_ID_AUTHORIZATION_FAILED, TRANSACTIONAL_ID_AUTHORIZATION_FAILED],
      },
      { behaviours: [TRANSACTIONAL_ID_AUTHORIZATION_FAILED], seedOnly: true },
    ]) {
      const { cluster, thrown } = createCluster(behaviours, { seedOnly })

      const error = await lookUp(cluster)

      expect(thrown).toHaveLength(behaviours.length)
      expect(error).toBeInstanceOf(KafkaJSGroupCoordinatorNotFound)
      expect(error.cause).toBe(thrown[0])
      expect(error.cause).toMatchObject({ type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED', code: 53 })
      expect(error.lastError).toBe(thrown[thrown.length - 1])
    }
  })

  test("a refusal followed by another broker's connection failure still throws not-found with the refusal as cause", async () => {
    const { cluster, thrown, outcomes } = createCluster([
      TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
      'down',
    ])

    const error = await lookUp(cluster)

    expect(outcomes).toEqual(['broker 1 answered 53', 'broker 2 failed to connect'])
    expect(thrown).toHaveLength(1)
    expect(error).toBeInstanceOf(KafkaJSGroupCoordinatorNotFound)
    expect(error.cause).toBe(thrown[0])
  })

  test("a refusal followed by another broker's answer returns that coordinator", async () => {
    const { cluster, outcomes } = createCluster([TRANSACTIONAL_ID_AUTHORIZATION_FAILED, 'ok'])

    const found = await lookUp(cluster)

    expect(outcomes).toEqual(['broker 1 answered 53', 'broker 2 answered with the coordinator'])
    expect(found).toEqual({
      host: 'kafka2',
      coordinator: { nodeId: 2, host: 'kafka2', port: 9092 },
    })
  })

  test('only retriable failures leave no cause', async () => {
    const answered = createCluster([NOT_COORDINATOR, NOT_COORDINATOR])
    const notFound = await lookUp(answered.cluster)

    expect(notFound).toBeInstanceOf(KafkaJSGroupCoordinatorNotFound)
    expect(notFound.cause).toBeUndefined()
    expect(notFound.lastError).toBe(answered.thrown[1])

    const unreachable = createCluster([NOT_COORDINATOR, 'down'])
    const connectionError = await lookUp(unreachable.cluster)

    expect(connectionError).toBeInstanceOf(KafkaJSConnectionError)
  })
})
