jest.mock('../../utils/shuffle', () => (brokers) => brokers)

const Cluster = require('../index')
const { KafkaJSGroupCoordinatorNotFound } = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const findCoordinatorV6 = require('../../../testHelpers/wire/findCoordinatorV6')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger, connectionOpts } = require('testHelpers')

const GROUP = 'group-A'
const OTHER_NODE = { nodeId: 7, host: 'kafka7', port: 9092 }
const NO_NODE = { nodeId: -1, host: '', port: -1 }

describe('Cluster > a coordinator lookup answered with a coordinator no broker would name', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A real `Cluster` whose broker pool holds real brokers over real connections, tried in node
   * order, each answering FindCoordinator v6 with one coordinator entry. With `seedOnly`, the pool
   * holds no broker table and the first broker is the seed.
   * @param {object[]} answers one per broker: fields over a success for `GROUP` at `kafka<n>:9092`
   * @param {{ seedOnly?: boolean }} [options]
   * @returns {Cluster}
   */
  const createCluster = (answers, { seedOnly = false } = {}) => {
    const cluster = new Cluster({
      ...connectionOpts(),
      logger: newLogger(),
      retry: { retries: 0 },
    })
    const pool = cluster.brokerPool
    const brokers = answers.map((fields, index) => {
      const nodeId = index + 1
      const harness = createGroupApiBroker({
        versions: { [apiKeys.GroupCoordinator]: { minVersion: 0, maxVersion: 6 } },
        respond: () =>
          findCoordinatorV6.buildResponse({
            throttleTime: 0,
            coordinators: [
              {
                key: GROUP,
                nodeId,
                host: `kafka${nodeId}`,
                port: 9092,
                errorCode: 0,
                errorMessage: null,
                ...fields,
              },
            ],
          }),
      })
      harnesses.push(harness)
      return harness.broker
    })

    pool.connectBroker = async () => {}
    if (seedOnly) {
      pool.brokers = {}
      pool.findConnectedBroker = async () => brokers[0]
    } else {
      pool.brokers = Object.fromEntries(brokers.map((broker, index) => [String(index + 1), broker]))
    }
    return cluster
  }

  /**
   * @param {Cluster} cluster
   * @returns {Promise<unknown>} what the lookup resolved with, or the error it rejected with
   */
  const lookUp = (cluster) =>
    cluster
      .findGroupCoordinatorMetadata({ groupId: GROUP, coordinatorType: COORDINATOR_TYPES.GROUP })
      .catch((error) => error)

  /** @returns {number[]} how many FindCoordinator requests each broker was asked */
  const asked = () => harnesses.map(({ requests }) => requests.length)

  test("broker 1 answers a success for another key, broker 2 a valid one: the lookup returns broker 2's coordinator", async () => {
    const cluster = createCluster([{ key: 'group-B', ...OTHER_NODE }, {}])

    const found = await lookUp(cluster)

    expect(found.coordinator).toEqual({ nodeId: 2, host: 'kafka2', port: 9092 })
    expect(asked()).toEqual([1, 1])
  })

  test("broker 1 answers a success with no node, broker 2 a valid one: the lookup returns broker 2's coordinator", async () => {
    const cluster = createCluster([NO_NODE, {}])

    const found = await lookUp(cluster)

    expect(found.coordinator).toEqual({ nodeId: 2, host: 'kafka2', port: 9092 })
    expect(asked()).toEqual([1, 1])
  })

  test('a seed broker that answers for another key ends the lookup as not found, with that refusal as the last error', async () => {
    const cluster = createCluster([{ key: 'group-B', ...OTHER_NODE }], { seedOnly: true })

    const error = await lookUp(cluster)

    expect(error).toBeInstanceOf(KafkaJSGroupCoordinatorNotFound)
    expect(error.lastError).toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/^FindCoordinator response is for key .*group-A/),
    })
  })

  test("a correct answer from broker 1 is the lookup's result and broker 2 is not asked", async () => {
    const cluster = createCluster([{}, {}])

    const found = await lookUp(cluster)

    expect(found.coordinator).toEqual({ nodeId: 1, host: 'kafka1', port: 9092 })
    expect(asked()).toEqual([1, 0])
  })
})
