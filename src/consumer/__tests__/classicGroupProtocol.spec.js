const fs = require('fs')
const path = require('path')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { createFakeCoordinator } = require('./fixtures/fetchHarness')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const {
  events: { GROUP_JOIN },
} = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')

const FIXTURES = path.join(__dirname, 'fixtures', 'classicGroupProtocol')

// Every value here is pinned: a golden frame that contains a timestamp or a generated id records
// nothing. The metadata bytes come from the shipped v0 subscription encoder.
const GROUP_ID = 'golden-group'
const MEMBER_ID = 'golden-member-1'
const GROUP_INSTANCE_ID = 'golden-instance-1'
const GENERATION_ID = 7
const SESSION_TIMEOUT = 30000
const REBALANCE_TIMEOUT = 60000
const TOPICS = ['golden-topic-a', 'golden-topic-b']
const PROTOCOL_METADATA = MemberMetadata.encode({ version: 0, topics: TOPICS })
const MEMBER_ASSIGNMENT = MemberAssignment.encode({
  version: 0,
  assignment: { 'golden-topic-a': [0, 1], 'golden-topic-b': [0] },
})

const joinGroupResponse = () =>
  new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeInt32(GENERATION_ID)
    .writeString('RoundRobinAssigner')
    .writeString(MEMBER_ID)
    .writeString(MEMBER_ID)
    .writeArray([
      new Encoder()
        .writeString(MEMBER_ID)
        .writeString(GROUP_INSTANCE_ID)
        .writeBytes(PROTOCOL_METADATA),
    ]).buffer

const syncGroupResponse = () =>
  new Encoder().writeInt32(0).writeInt16(0).writeBytes(MEMBER_ASSIGNMENT).buffer

const emptyResponse = () => new Encoder().writeInt32(0).writeInt16(0).buffer

const leaveGroupResponse = () => new Encoder().writeInt32(0).writeInt16(0).writeArray([]).buffer

const answer = ({ apiKey }) => {
  switch (apiKey) {
    case apiKeys.JoinGroup:
      return joinGroupResponse()
    case apiKeys.SyncGroup:
      return syncGroupResponse()
    case apiKeys.Heartbeat:
      return emptyResponse()
    case apiKeys.LeaveGroup:
      return leaveGroupResponse()
    default:
      throw new Error(`unexpected apiKey ${apiKey}`)
  }
}

/**
 * Reads a committed frame fixture.
 * @param {string} name
 * @returns {{ apiKey: number, apiVersion: number, body: string }}
 */
const readFixture = (name) =>
  JSON.parse(fs.readFileSync(path.join(FIXTURES, `${name}.json`), 'utf8'))

/**
 * Drives the four classic group requests once each, in the order a real member makes them.
 * @returns {Promise<(apiKey: number) => { apiKey: number, apiVersion: number, body: Buffer }>}
 */
const recordFrames = async () => {
  const { broker, bodyOf } = await createFakeCoordinator(answer)

  const assigner = {
    name: 'RoundRobinAssigner',
    protocol: () => ({ name: 'RoundRobinAssigner', metadata: PROTOCOL_METADATA }),
    assign: async () => [{ memberId: MEMBER_ID, memberAssignment: MEMBER_ASSIGNMENT }],
  }

  const protocol = new ClassicGroupProtocol({
    cluster: {
      findGroupCoordinator: async () => broker,
      refreshMetadata: async () => {},
    },
    groupId: GROUP_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
    sessionTimeout: SESSION_TIMEOUT,
    rebalanceTimeout: REBALANCE_TIMEOUT,
    assigners: [assigner],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
  })

  // The recorded frames belong to a member that already has an id; a very first join would send
  // an empty one and produce different bytes.
  protocol.memberId = MEMBER_ID

  await protocol.rebalance({ topics: TOPICS, topicsSubscribed: TOPICS, beforeSync: () => {} })
  await protocol.heartbeat({ interval: 0 })
  await protocol.leave()

  return bodyOf
}

/**
 * A `ConsumerGroup` with a stub cluster, used to exercise the membership accessors and the
 * join/sync retry contract without a broker.
 * @param {number} retries
 * @returns {ConsumerGroup}
 */
const buildAccessorGroup = (retries) =>
  new ConsumerGroup({
    retry: { retries },
    cluster: {
      connect: async () => {},
      refreshMetadata: async () => {},
      refreshMetadataIfNecessary: async () => {},
      committedOffsets: () => ({}),
      findTopicPartitionMetadata: () => [],
    },
    groupId: 'accessor-group',
    topics: ['accessor-topic'],
    topicConfigurations: { 'accessor-topic': {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    groupInstanceId: 'accessor-instance',
    metadataMaxAge: 300000,
  })

describe('Consumer > classic group protocol > golden frames', () => {
  const CASES = [
    ['joinGroup_v5_request', apiKeys.JoinGroup, 5],
    ['syncGroup_v3_request', apiKeys.SyncGroup, 3],
    ['heartbeat_v3_request', apiKeys.Heartbeat, 3],
    ['leaveGroup_v3_request', apiKeys.LeaveGroup, 3],
  ]

  let bodyOf

  beforeAll(async () => {
    bodyOf = await recordFrames()
  })

  test.each(CASES)('%s is byte-identical to the recorded frame', (name, apiKey, apiVersion) => {
    const fixture = readFixture(name)
    const written = bodyOf(apiKey)

    expect(written.apiKey).toEqual(fixture.apiKey)
    expect(written.apiVersion).toEqual(fixture.apiVersion)
    expect(written.apiVersion).toEqual(apiVersion)
    expect(written.body.toString('hex')).toEqual(fixture.body)
  })

  it('negotiates the pinned versions rather than whatever the client implements', () => {
    // The negative control for the pinned ApiVersions table: if the table were ignored, a future
    // JoinGroup v6 in this repo would silently re-record every frame.
    expect(CASES.map(([, apiKey]) => bodyOf(apiKey).apiVersion)).toEqual([5, 3, 3, 3])
  })
})

describe('Consumer > classic group protocol > membership accessors', () => {
  const FIELDS = [
    ['memberId', 'member-9'],
    ['generationId', 42],
    ['leaderId', 'leader-9'],
    ['members', [{ memberId: 'member-9' }]],
    ['groupProtocol', 'RoundRobinAssigner'],
    ['groupInstanceId', null],
    ['coordinator', { heartbeat: () => {} }],
    ['lastRequest', 0],
  ]

  test.each(FIELDS)('%s reads and writes through to the protocol', (field, value) => {
    const group = buildAccessorGroup(0)

    group[field] = value

    expect(group.groupMembership[field]).toEqual(value)
    expect(group[field]).toEqual(value)
  })

  it('keeps warnIfStaticMembershipInactive callable on the group', () => {
    const group = buildAccessorGroup(0)
    group.coordinator = { negotiatedVersion: () => 4 }
    // The group's logger object is the protocol's logger object: it is not re-namespaced, so a
    // spy installed here observes what the protocol writes.
    const warn = jest.spyOn(group.logger, 'warn')

    group.warnIfStaticMembershipInactive()

    expect(warn).toHaveBeenCalledWith(
      'groupInstanceId is set but the broker negotiated JoinGroup v4 - static membership is inactive',
      expect.objectContaining({ negotiatedVersion: 4 })
    )
  })
})

describe('Consumer > classic group protocol > joinAndSync contract', () => {
  const rebalancing = () => {
    const error = new Error('rebalance in progress')
    error.type = 'REBALANCE_IN_PROGRESS'
    error.code = 27
    return error
  }

  const buildGroup = (retries) => {
    const group = buildAccessorGroup(retries)
    group.applyRebalanceResult = jest.fn(async () => ({
      assignment: [],
      added: [],
      revoked: [],
    }))
    group.assigned = () => []
    return group
  }

  it('retries the whole join+sync sequence on a rebalance error and returns the sync result', async () => {
    const group = buildGroup(2)
    const rebalance = jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValueOnce(rebalancing())
      .mockResolvedValueOnce({ assignment: { 'accessor-topic': [0] } })

    await expect(group.joinAndSync()).resolves.toEqual({
      assignment: { 'accessor-topic': [0] },
    })
    expect(rebalance).toHaveBeenCalledTimes(2)
  })

  it('clears the member id and retries on UNKNOWN_MEMBER_ID', async () => {
    const group = buildGroup(2)
    group.memberId = 'stale-member'
    const unknown = new Error('unknown member')
    unknown.type = 'UNKNOWN_MEMBER_ID'

    jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValueOnce(unknown)
      .mockResolvedValueOnce({ assignment: {} })

    await group.joinAndSync()

    // The remap ran before the retry: the stale id was dropped, so the second attempt joined
    // with an empty member id and took whatever the coordinator handed back.
    expect(group.memberId).toBeNull()
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(2)
  })

  it('bails without retrying on any other error', async () => {
    const group = buildGroup(5)
    jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValue(new Error('assigner not found'))

    await expect(group.joinAndSync()).rejects.toThrow('assigner not found')
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
  })

  it('applies the result and emits GROUP_JOIN once, in that order', async () => {
    const group = buildGroup(0)
    jest.spyOn(group.groupMembership, 'rebalance').mockResolvedValue({ assignment: {} })
    const calls = []
    group.applyRebalanceResult = jest.fn(async () => {
      calls.push('apply')
      return { assignment: [], added: [], revoked: [] }
    })
    // `InstrumentationEventEmitter` exposes only `emit` and `addListener` - there is no `on`,
    // and calling one is a TypeError, not a failing assertion.
    group.instrumentationEmitter.addListener(GROUP_JOIN, () => calls.push('group_join'))

    await group.joinAndSync()

    expect(calls).toEqual(['apply', 'group_join'])
  })
})
