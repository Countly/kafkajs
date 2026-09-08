const { decode, parse } = require('./response')

const vector = () => require('../fixtures/java/describeGroupsResponse-v3-describedGroup.json')

/**
 * The vectors report member bytes as lower-case hex; the decoder reports Buffers.
 * @param {object} group a decoded group
 * @returns {object} the group with its members' byte fields as hex strings
 */
const asFixtureShape = (group) => ({
  ...group,
  members: group.members.map((member) => ({
    ...member,
    memberMetadata: member.memberMetadata.toString('hex'),
    memberAssignment: member.memberAssignment.toString('hex'),
  })),
})

describe('Protocol > Requests > DescribeGroups > v3', () => {
  test('decodes the describedGroup vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector()
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.groups.map(asFixtureShape)).toEqual(fields.groups)
  })

  test('reports authorizedOperations from v3, at the not-requested sentinel', async () => {
    // KIP-430: the broker fills the bitfield only when the request asked for it; -2147483648 is
    // the default it sends otherwise, and it is a value, not an absent field.
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0].authorizedOperations).toEqual(-2147483648)
  })

  test('has no member group instance id below v4', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0].members[0].groupInstanceId).toBeNull()
  })

  test('has no per-group error message below v6', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0].errorMessage).toBeNull()
  })

  test('keeps the KIP-219 throttle split, which no vector can show', async () => {
    // Every committed vector carries a zero throttle, so the split is pinned from a hand-built
    // frame: 250 ms of throttling and an empty groups array.
    const data = await decode(Buffer.from('000000fa00000000', 'hex'))

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 250, groups: [] })
  })

  test('shares v0s parse, which throws on any group-level error code', () => {
    expect(parse).toBe(require('../v0/response').parse)
  })
})
