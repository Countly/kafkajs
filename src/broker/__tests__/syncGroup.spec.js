const Broker = require('../index')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')

describe('Broker > SyncGroup', () => {
  let topicName, groupId, seedBroker, groupCoordinator

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await seedBroker.connect()

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
    groupCoordinator && (await groupCoordinator.disconnect())
  })

  test('request', async () => {
    const { generationId, memberId } = await groupCoordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
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
    const response = await groupCoordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
      memberAssignment,
    })
  })

  // SyncGroup v3's response carries no group_instance_id to echo back (throttle_time_ms,
  // error_code, member_assignment only - see v3/response.js), and unlike Heartbeat, the
  // coordinator's fencing decision is made at JoinGroup time, not observable via a SyncGroup
  // call itself (a stale member's SyncGroup would fail on its now-invalid member/generation
  // regardless of what its own request's groupInstanceId carries). So there is no cheap
  // broker-observable negative that isolates "did *this* SyncGroup call put the field on the
  // wire" the way Heartbeat's fencing test does (see heartbeat.spec.js). This is a request/
  // response round-trip smoke test: it proves the call succeeds with groupInstanceId set, not
  // that dropping the field would fail it. Codec-level coverage that the field is only
  // destructured/encoded for v3 lives in the protocol layer (src/protocol/requests/syncGroup).
  test('request with groupInstanceId (KIP-345 static membership)', async () => {
    const groupInstanceId = `static-${secureRandom()}`

    const { generationId, memberId } = await groupCoordinator.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
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
    const response = await groupCoordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId,
      groupAssignment,
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
      memberAssignment,
    })
  })
})
