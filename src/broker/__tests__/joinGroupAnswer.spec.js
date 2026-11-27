const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')

const MEMBER = 'member-1'
const PROTOCOL = 'RoundRobinAssigner'
const JOIN = {
  groupId: 'test-group',
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  memberId: MEMBER,
  groupProtocols: [{ name: PROTOCOL, metadata: Buffer.alloc(0) }],
}
const VERSIONS = [6, 8]

/**
 * @param {string|null} memberId
 * @returns {Encoder} one entry of a JoinGroup v6-v8 member list
 */
const encodeMember = (memberId) =>
  new Encoder()
    .writeUVarIntString(memberId)
    .writeUVarIntString(null)
    .writeUVarIntBytes(Buffer.alloc(0))
    .writeTaggedFields()

/**
 * A successful JoinGroup v6 to v8 answer. v7 and v8 carry the protocol type after the generation.
 * @param {number} version
 * @param {object} [fields]
 * @param {(string|null)[]|null} [fields.members] the listed member ids; null for the null array
 * @returns {Buffer} the response body
 */
const joinAnswer = (
  version,
  {
    generationId = 5,
    groupProtocol = PROTOCOL,
    leaderId = 'leader-1',
    memberId = MEMBER,
    members = [],
  } = {}
) => {
  const encoder = new Encoder().writeInt32(0).writeInt16(0).writeInt32(generationId)
  if (version >= 7) encoder.writeUVarIntString('consumer')
  return encoder
    .writeUVarIntString(groupProtocol)
    .writeUVarIntString(leaderId)
    .writeUVarIntString(memberId)
    .writeUVarIntArray(members === null ? null : members.map(encodeMember))
    .writeTaggedFields().buffer
}

describe('Broker > joinGroup answer checks', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * One JoinGroup answered with `fields`, through a real broker and connection.
   * @param {number} version
   * @param {object} fields
   * @returns {Promise<object>}
   */
  const join = (version, fields) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: version } },
      respond: () => joinAnswer(version, fields),
    })
    harnesses.push(harness)
    return harness.broker.joinGroup(JOIN)
  }

  /** @returns {number[]} the JoinGroup versions the latest broker was asked in */
  const versionsSent = () => harnesses[harnesses.length - 1].requests.map((r) => r.apiVersion)

  const refusal = {
    name: 'KafkaJSError',
    retriable: true,
    type: 'MALFORMED_GROUP_ANSWER',
    message: expect.stringMatching(/^JoinGroup response /),
  }

  test.each([
    ['an empty member id', { memberId: '' }],
    ['no member id', { memberId: null }],
    ['generation -1', { generationId: -1 }],
    ['a protocol this member did not offer', { groupProtocol: 'RangeAssigner' }],
    ['no protocol', { groupProtocol: null }],
  ])('a success with %s is refused retriably', async (_, fields) => {
    for (const version of VERSIONS) {
      await expect(join(version, fields)).rejects.toMatchObject(refusal)
      expect(versionsSent()).toEqual([version])
    }
  })

  test.each([
    ['no members', []],
    ['a null member list', null],
    ['a member with no id', [MEMBER, null]],
    ['a member with an empty id', [MEMBER, '']],
    ['a member listed twice', [MEMBER, 'member-2', 'member-2']],
    ['no entry for the leader', ['member-2']],
  ])('a leader answer with %s is refused retriably', async (_, members) => {
    for (const version of VERSIONS) {
      await expect(join(version, { leaderId: MEMBER, members })).rejects.toMatchObject(refusal)
    }
  })

  test('a follower answer with no members resolves: only the leader gets the member list', async () => {
    for (const version of VERSIONS) {
      await expect(join(version, {})).resolves.toMatchObject({
        memberId: MEMBER,
        leaderId: 'leader-1',
        generationId: 5,
        groupProtocol: PROTOCOL,
        members: [],
      })
    }
  })

  test('a leader answer listing itself and another member resolves', async () => {
    for (const version of VERSIONS) {
      const data = await join(version, { leaderId: MEMBER, members: ['member-2', MEMBER] })

      expect(data.members.map(({ memberId }) => memberId)).toEqual(['member-2', MEMBER])
    }
  })
})
