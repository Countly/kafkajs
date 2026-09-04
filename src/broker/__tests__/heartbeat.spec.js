const Broker = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')

const {
  secureRandom,
  createConnectionPool,
  newLogger,
  createTopic,
  retryProtocol,
} = require('testHelpers')

describe('Broker > Heartbeat', () => {
  let topicName, groupId, seedBroker, broker, groupCoordinator

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await seedBroker.connect()
    await createTopic({ topic: topicName })

    const metadata = await retryProtocol(
      'LEADER_NOT_AVAILABLE',
      async () => await seedBroker.metadata([topicName])
    )

    // Find leader of partition
    const partitionBroker = metadata.topicMetadata[0].partitionMetadata[0].leader
    const newBrokerData = metadata.brokers.find((b) => b.nodeId === partitionBroker)

    // Connect to the correct broker to produce message
    broker = new Broker({
      connectionPool: createConnectionPool(newBrokerData),
      logger: newLogger(),
    })
    await broker.connect()

    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () => await seedBroker.findGroupCoordinator({ groupId })
    )

    groupCoordinator = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })
    await groupCoordinator.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
    groupCoordinator && (await groupCoordinator.disconnect())
  })

  test('request', async () => {
    const { generationId, memberId } = await groupCoordinator.joinGroup({
      groupId,
      // Kafka 4.x honours rebalanceTimeout literally; omitting it encodes 0 and the coordinator
      // drops the member before SyncGroup (UNKNOWN_MEMBER_ID).
      sessionTimeout: 30000,
      rebalanceTimeout: 30000,
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
        },
      ],
    })

    const memberAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { [topicName]: [0] },
    })

    const groupAssignment = [{ memberId, memberAssignment }]
    await groupCoordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment,
    })

    const response = await groupCoordinator.heartbeat({
      groupId,
      groupGenerationId: generationId,
      memberId,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  // Heartbeat v3's response carries no group_instance_id to echo (throttle_time_ms + error_code
  // only), so a passing request/response round-trip alone would still pass if
  // Broker#heartbeat silently dropped the field. This is a smoke test for that round-trip; the
  // test below it is what actually proves groupInstanceId reached the wire.
  test('request with groupInstanceId (KIP-345 static membership)', async () => {
    const groupInstanceId = `static-${secureRandom()}`

    const { generationId, memberId } = await groupCoordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 30000,
      groupInstanceId,
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
        },
      ],
    })

    const memberAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { [topicName]: [0] },
    })

    const groupAssignment = [{ memberId, memberAssignment }]
    await groupCoordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId,
      groupAssignment,
    })

    const response = await groupCoordinator.heartbeat({
      groupId,
      groupGenerationId: generationId,
      memberId,
      groupInstanceId,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  test('groupInstanceId reaches the wire: a superseded member is fenced on Heartbeat', async () => {
    const groupInstanceId = `static-${secureRandom()}`
    const protocols = [
      {
        name: 'AssignerName',
        metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
      },
    ]

    const first = await groupCoordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 30000,
      groupInstanceId,
      groupProtocols: protocols,
    })

    // A second join under the *same* groupInstanceId replaces the coordinator's
    // group.instance.id -> member.id mapping (KIP-345); the coordinator can only know to do
    // this - and therefore only fence `first`'s heartbeat below - if groupInstanceId actually
    // reached the wire on the first joinGroup call. This is the negative assertion the
    // request/response round-trip above cannot provide.
    await groupCoordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 30000,
      groupInstanceId,
      groupProtocols: protocols,
    })

    await expect(
      groupCoordinator.heartbeat({
        groupId,
        groupGenerationId: first.generationId,
        memberId: first.memberId,
        groupInstanceId,
      })
    ).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID', code: 82 })
  })
})
