const createProducer = require('../../producer')
const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer', () => {
  let topicName, groupId, cluster, producer, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster = createCluster()
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  describe('describe group', () => {
    it('returns the group description', async () => {
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })

      consumer.run({ eachMessage: jest.fn() })
      await waitForConsumerToJoinGroup(consumer)

      // The 2.2.4 fields, unchanged in name and meaning, plus the additive ones the unified
      // describer produces: `describeGroup()` now routes through the same module
      // `admin.describeGroups` uses, so one shape describes a group of either protocol.
      await expect(consumer.describeGroup()).resolves.toEqual({
        errorCode: 0,
        errorMessage: null,
        groupId,
        members: [
          {
            clientHost: expect.any(String),
            clientId: expect.any(String),
            groupInstanceId: null,
            memberId: expect.any(String),
            memberAssignment: expect.anything(),
            memberMetadata: expect.anything(),
            assignment: [{ topic: topicName, partitions: [0] }],
            targetAssignment: null,
            memberEpoch: null,
            memberType: null,
            rackId: null,
            subscribedTopics: [],
            subscribedTopicRegex: null,
          },
        ],
        protocol: 'RoundRobinAssigner',
        protocolType: 'consumer',
        state: 'Stable',
        groupType: 'classic',
        groupEpoch: null,
        assignmentEpoch: null,
        assignorName: null,
        // The request does not ask for authorized operations, and the "not requested" sentinel is
        // reported as null - the same mapping `admin.describeGroups` has always applied.
        authorizedOperations: null,
      })
    })
  })
})
