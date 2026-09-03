const {
  createConnectionPool,
  connectionOpts,
  secureRandom,
  newLogger,
  retryProtocol,
} = require('testHelpers')
const CONFIG_RESOURCE_TYPES = require('../../protocol/configResourceTypes')
const ConfigSource = require('../../protocol/configSource')
const Broker = require('../index')

const sortByConfigName = (array) => array.sort((a, b) => a.configName.localeCompare(b.configName))

describe('Broker > describeConfigs', () => {
  let seedBroker, broker

  beforeEach(async () => {
    seedBroker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find((b) => b.nodeId === metadata.controllerId)

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

    // KRaft commits CreateTopics on the controller and the broker's metadata image catches up
    // asynchronously, so a describeConfigs right after can still answer UNKNOWN_TOPIC_OR_PARTITION.
    const response = await retryProtocol('UNKNOWN_TOPIC_OR_PARTITION', () =>
      broker.describeConfigs({
        includeSynonyms: true,
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: topicName1,
            configNames: ['compression.type', 'retention.ms'],
          },
        ],
      })
    )

    expect(response).toEqual({
      resources: [
        {
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              isDefault: true,
              configSource: ConfigSource.DEFAULT_CONFIG,
              isSensitive: false,
              readOnly: false,
              configSynonyms: [
                {
                  configName: 'compression.type',
                  configValue: 'producer',
                  configSource: 5,
                },
              ],
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              isDefault: true,
              configSource: ConfigSource.DEFAULT_CONFIG,
              isSensitive: false,
              readOnly: false,
              configSynonyms: [],
            },
          ],
          errorCode: 0,
          // Kafka 4.x returns an empty error message rather than a null one for these responses.
          errorMessage: '',
          resourceName: topicName1,
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
        },
      ],
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
    })
  })

  describe('request without config names', () => {
    test('returns all config entries', async () => {
      await broker.connect()
      const topicName1 = `test-topic-${secureRandom()}`
      const topicName2 = `test-topic-${secureRandom()}`

      await broker.createTopics({
        topics: [{ topic: topicName1 }, { topic: topicName2 }],
      })

      // KRaft commits CreateTopics on the controller and the broker's metadata image catches up
      // asynchronously, so a describeConfigs right after can still answer UNKNOWN_TOPIC_OR_PARTITION.
      const response = await retryProtocol('UNKNOWN_TOPIC_OR_PARTITION', () =>
        broker.describeConfigs({
          resources: [
            {
              type: CONFIG_RESOURCE_TYPES.TOPIC,
              name: topicName1,
              configNames: [],
            },
          ],
        })
      )

      // Kafka 4.x topic defaults: message.format.version, message.downconversion.enable and
      // message.timestamp.difference.max.ms were removed in 4.0, and max.message.bytes now defaults
      // to 1048588. min.insync.replicas is asserted by value only: the apache/kafka image ships it
      // as a cluster-level dynamic default, so its source/isDefault are environment-dependent.
      const expectedConfigEntries = sortByConfigName([
        {
          configName: 'cleanup.policy',
          configValue: 'delete',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'compression.type',
          configValue: 'producer',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'delete.retention.ms',
          configValue: '86400000',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'file.delete.delay.ms',
          configValue: '60000',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'flush.messages',
          configValue: '9223372036854775807',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'flush.ms',
          configValue: '9223372036854775807',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'follower.replication.throttled.replicas',
          configValue: '',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'index.interval.bytes',
          configValue: '4096',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'leader.replication.throttled.replicas',
          configValue: '',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'max.compaction.lag.ms',
          configValue: '9223372036854775807',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'max.message.bytes',
          configValue: '1048588',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'message.timestamp.type',
          configValue: 'CreateTime',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'min.cleanable.dirty.ratio',
          configValue: '0.5',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'min.compaction.lag.ms',
          configValue: '0',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'min.insync.replicas',
          configValue: '1',
          isDefault: expect.any(Boolean),
          configSource: expect.any(Number),
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'preallocate',
          configValue: 'false',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'retention.bytes',
          configValue: '-1',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'retention.ms',
          configValue: '604800000',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'segment.bytes',
          configValue: '1073741824',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'segment.index.bytes',
          configValue: '10485760',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'segment.jitter.ms',
          configValue: '0',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'segment.ms',
          configValue: '604800000',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
        {
          configName: 'unclean.leader.election.enable',
          configValue: 'false',
          isDefault: true,
          configSource: ConfigSource.DEFAULT_CONFIG,
          isSensitive: false,
          readOnly: false,
          configSynonyms: [],
        },
      ])

      expect(response).toEqual(
        expect.objectContaining({
          resources: [
            expect.objectContaining({
              configEntries: expect.any(Array),
              errorCode: 0,
              // Kafka 4.x returns an empty error message rather than a null one for these responses.
              errorMessage: '',
              resourceName: topicName1,
              resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
            }),
          ],
          clientSideThrottleTime: expect.optional(0),
          throttleTime: 0,
        })
      )

      expect(sortByConfigName(response.resources[0].configEntries)).toEqual(
        expect.arrayContaining(expectedConfigEntries)
      )
    })
  })
})
