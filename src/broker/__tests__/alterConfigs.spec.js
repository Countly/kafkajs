const {
  createConnectionPool,
  connectionOpts,
  secureRandom,
  newLogger,
  waitFor,
  retryProtocol,
} = require('testHelpers')
const CONFIG_RESOURCE_TYPES = require('../../protocol/configResourceTypes')
const Broker = require('../index')

describe('Broker > alterConfigs', () => {
  let seedBroker, broker

  const getConfigEntries = response =>
    response.resources.find(r => r.resourceType === CONFIG_RESOURCE_TYPES.TOPIC).configEntries

  const getConfigValue = (configEntries, name) =>
    configEntries.find(c => c.configName === name).configValue

  beforeEach(async () => {
    seedBroker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find(b => b.nodeId === metadata.controllerId)

    broker = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`

    await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    const CONFIG_NAME = 'cleanup.policy'
    // KRaft commits CreateTopics on the controller and the broker's metadata image catches up
    // asynchronously, so a describeConfigs right after can still answer UNKNOWN_TOPIC_OR_PARTITION.
    const describeResponse = await retryProtocol('UNKNOWN_TOPIC_OR_PARTITION', () =>
      broker.describeConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: topicName1,
            configNames: [CONFIG_NAME],
          },
        ],
      })
    )

    let cleanupPolicy = getConfigValue(getConfigEntries(describeResponse), CONFIG_NAME)

    expect(cleanupPolicy).toEqual('delete')

    const response = await broker.alterConfigs({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configEntries: [
            {
              name: 'cleanup.policy',
              value: 'compact',
            },
          ],
        },
      ],
    })

    expect(response).toEqual({
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceName: topicName1,
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })

    // KRaft applies AlterConfigs through the controller, so the broker's own view of the topic
    // config catches up asynchronously.
    cleanupPolicy = await waitFor(
      async () => {
        const configResponse = await broker.describeConfigs({
          resources: [
            {
              type: CONFIG_RESOURCE_TYPES.TOPIC,
              name: topicName1,
              configNames: [CONFIG_NAME],
            },
          ],
        })

        const value = getConfigValue(getConfigEntries(configResponse), CONFIG_NAME)
        return value === 'compact' ? value : false
      },
      {
        ignoreTimeout: false,
        maxWait: 10000,
        timeoutMessage: 'Timed out waiting for cleanup.policy',
      }
    )

    expect(cleanupPolicy).toEqual('compact')
  })
})
