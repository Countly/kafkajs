const { decode, parse } = require('./response')

const vector = (version) =>
  require(`../fixtures/java/describeGroupsResponse-v${version}-describedGroup.json`)

describe('Protocol > Requests > DescribeGroups > v4', () => {
  test('decodes the describedGroup vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector(4)
    const data = await decode(Buffer.from(hex, 'hex'))
    const [member] = data.groups[0].members

    expect(data.groups[0]).toMatchObject({
      errorCode: 0,
      errorMessage: null,
      groupId: 'test-group',
      state: 'Stable',
      protocolType: 'consumer',
      protocol: 'RoundRobinAssigner',
      authorizedOperations: -2147483648,
    })
    expect(member.memberMetadata.toString('hex')).toEqual(
      fields.groups[0].members[0].memberMetadata
    )
    expect(member.memberAssignment.toString('hex')).toEqual(
      fields.groups[0].members[0].memberAssignment
    )
  })

  test('decodes a member group instance id at v4, and null below it', async () => {
    // KIP-345's static membership id appears on members at v4. Below it the key is present and
    // null, so one row shape reads at every version.
    const atV4 = await decode(Buffer.from(vector(4).hex, 'hex'))
    const atV3 = await require('../v3/response').decode(Buffer.from(vector(3).hex, 'hex'))

    expect(atV4.groups[0].members[0].groupInstanceId).toEqual('test-instance')
    expect(atV3.groups[0].members[0].groupInstanceId).toBeNull()
  })

  test('the v4 body is longer than v3s by exactly the instance id it adds', async () => {
    const v3Bytes = Buffer.from(vector(3).hex, 'hex')
    const v4Bytes = Buffer.from(vector(4).hex, 'hex')

    // INT16 length prefix + "test-instance"
    expect(v4Bytes.length - v3Bytes.length).toEqual(2 + 'test-instance'.length)
  })

  test('shares v0s parse, which throws on any group-level error code', () => {
    expect(parse).toBe(require('../v0/response').parse)
  })
})
