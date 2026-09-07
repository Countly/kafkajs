const fs = require('fs')
const path = require('path')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { createFakeCoordinator } = require('./fixtures/fetchHarness')

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

  await broker.joinGroup({
    groupId: GROUP_ID,
    sessionTimeout: SESSION_TIMEOUT,
    rebalanceTimeout: REBALANCE_TIMEOUT,
    memberId: MEMBER_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
    groupProtocols: [{ name: 'RoundRobinAssigner', metadata: PROTOCOL_METADATA }],
  })

  await broker.syncGroup({
    groupId: GROUP_ID,
    generationId: GENERATION_ID,
    memberId: MEMBER_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
    groupAssignment: [{ memberId: MEMBER_ID, memberAssignment: MEMBER_ASSIGNMENT }],
    sessionTimeout: SESSION_TIMEOUT,
    rebalanceTimeout: REBALANCE_TIMEOUT,
  })

  await broker.heartbeat({
    groupId: GROUP_ID,
    groupGenerationId: GENERATION_ID,
    memberId: MEMBER_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
  })

  await broker.leaveGroup({
    groupId: GROUP_ID,
    memberId: MEMBER_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
  })

  return bodyOf
}

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
