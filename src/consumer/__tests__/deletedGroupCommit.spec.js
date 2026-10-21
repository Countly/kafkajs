const createConsumer = require('../index')
const createProducer = require('../../producer')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_REQUEST } = require('../../network/instrumentationEvents')

const {
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  secureRandom,
  waitFor,
  waitForConsumerToJoinGroup,
  waitForMessages,
} = require('testHelpers')

const GROUP_ID_NOT_FOUND = 69

/**
 * Live cover for the OffsetCommit v9 semantics change.
 *
 * Apache Kafka's `OffsetMetadataManager.validateOffsetCommit` answers `GROUP_ID_NOT_FOUND` (69)
 * from OffsetCommit v9, where v8 answered `ILLEGAL_GENERATION` (22), for a commit from a member
 * whose group no longer exists. The first case proves the broker really does that; the second
 * proves a classic consumer still rejoins rather than crashing when it happens.
 *
 * The group is not actually deleted under the running member: `DeleteGroups` refuses a group that
 * still has one (`NON_EMPTY_GROUP`, pinned by `src/admin/__tests__/deleteGroups.spec.js`), so the
 * production shape of this - a group dropped after `offsets.retention.minutes`, or deleted while
 * the consumers were briefly down - is reproduced by answering one commit the way that broker
 * would.
 *
 * @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java
 */
describe('Consumer > a commit answered GROUP_ID_NOT_FOUND', () => {
  let topicName, groupId, cluster, producer, consumer, requestEvents

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    const instrumentationEmitter = new InstrumentationEventEmitter()
    requestEvents = []
    instrumentationEmitter.addListener(NETWORK_REQUEST, ({ payload }) =>
      requestEvents.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )

    cluster = createCluster({ instrumentationEmitter })
    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  it('is what the broker answers an OffsetCommit for a group that does not exist', async () => {
    await cluster.connect()
    await cluster.addTargetTopic(topicName)

    const missingGroupId = `never-created-${secureRandom()}`
    const coordinator = await cluster.findGroupCoordinator({ groupId: missingGroupId })

    const caught = await coordinator
      .offsetCommit({
        groupId: missingGroupId,
        memberId: 'a-member-that-never-joined',
        groupGenerationId: 1,
        topics: [{ topic: topicName, partitions: [{ partition: 0, offset: '0' }] }],
      })
      .catch((e) => e)

    expect(caught.code).toEqual(GROUP_ID_NOT_FOUND)
    expect(caught.type).toEqual('GROUP_ID_NOT_FOUND')

    // The answer rests on the version branch, so the version that went out is asserted alongside
    // the code: at v8 this same request is answered 22.
    expect(requestEvents.filter(({ apiName }) => apiName === 'OffsetCommit')).toEqual([
      { apiName: 'OffsetCommit', apiVersion: 9 },
    ])

    await cluster.disconnect()
  })

  it('makes a running classic consumer rejoin rather than crash', async () => {
    let injected = false

    // Patched on the cluster rather than on one broker: the offset manager takes its coordinator
    // from here, and a coordinator re-resolved mid-test must carry the same answer.
    const findGroupCoordinator = cluster.findGroupCoordinator.bind(cluster)
    cluster.findGroupCoordinator = async (options) => {
      const coordinator = await findGroupCoordinator(options)

      if (!coordinator.offsetCommitPatched) {
        coordinator.offsetCommitPatched = true
        const offsetCommit = coordinator.offsetCommit.bind(coordinator)
        coordinator.offsetCommit = async (payload, options) => {
          if (!injected && payload.groupId === groupId) {
            injected = true
            throw new KafkaJSProtocolError(createErrorFromCode(GROUP_ID_NOT_FOUND))
          }

          return offsetCommit(payload, options)
        }
      }

      return coordinator
    }

    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    })

    const groupJoins = []
    const crashes = []
    consumer.on(consumer.events.GROUP_JOIN, (event) => groupJoins.push(event))
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))

    await Promise.all([consumer.connect(), producer.connect()])
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const consumed = []
    consumer.run({ eachMessage: async (event) => consumed.push(event) })
    await waitForConsumerToJoinGroup(consumer)

    const messages = Array(3)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(consumed, { number: messages.length })

    // The commit answered 69 takes the ILLEGAL_GENERATION verdict: discard, rejoin, keep running.
    await waitFor(() => injected && groupJoins.length > 1, { delay: 100, maxWait: 30000 })

    expect(crashes).toEqual([])
    expect(groupJoins.length).toBeGreaterThan(1)

    // And the consumer is still consuming afterwards.
    const more = [{ key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }]
    await producer.send({ acks: 1, topic: topicName, messages: more })
    await waitForMessages(consumed, { number: messages.length + 1 })

    expect(crashes).toEqual([])
  }, 60000)
})
