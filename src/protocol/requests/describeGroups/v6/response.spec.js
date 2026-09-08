const { decode, parse } = require('./response')

const vector = (caseId = 'describedGroup') =>
  require(`../fixtures/java/describeGroupsResponse-v6-${caseId}.json`)

describe('Protocol > Requests > DescribeGroups > v6', () => {
  test('decodes the describedGroup vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector()
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
    expect(member).toMatchObject({
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      clientId: 'test-client',
      clientHost: '/127.0.0.1',
    })
    expect(member.memberMetadata.toString('hex')).toEqual(
      fields.groups[0].members[0].memberMetadata
    )
  })

  test('decodes the v6 error message beside the group error code', async () => {
    const { hex, fields } = vector('groupIdNotFound')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.groups[0].errorCode).toEqual(69)
    expect(data.groups[0].errorMessage).toEqual(fields.groups[0].errorMessage)
  })

  test('is one byte longer than v5 for a group with no message, which is where ErrorMessage sits', async () => {
    const v5Bytes = Buffer.from(
      require('../fixtures/java/describeGroupsResponse-v5-describedGroup.json').hex,
      'hex'
    )
    const v6Bytes = Buffer.from(vector().hex, 'hex')

    // The null compact string is the single 0x00 sentinel, and it is the only difference.
    expect(v6Bytes.length - v5Bytes.length).toEqual(1)
  })

  test('reports a missing group instead of throwing, and keeps the 2.2.4 row shape', async () => {
    // KIP-1043 made GROUP_ID_NOT_FOUND the answer for a group that is not there, and the caller
    // routes on it. Throwing would turn one absent group into a failed call for all of them, and
    // would change what `describeGroups(['does-not-exist'])` has always returned.
    const data = await decode(Buffer.from(vector('groupIdNotFound').hex, 'hex'))

    await expect(parse(data)).resolves.toEqual(data)
    expect(data.groups[0]).toMatchObject({
      errorCode: 69,
      state: 'Dead',
      members: [],
      protocol: '',
      protocolType: '',
    })
  })

  test('still throws for every other group-level code', async () => {
    // The exception is GROUP_ID_NOT_FOUND alone. `admin.describeGroups` and
    // `consumer.describeGroup` both wrap this call in a retrier and both depend on a retriable
    // coordinator code arriving as a throw; swallowing 14 would return a row instead of retrying,
    // on a public API, with no way for a caller to notice.
    await expect(parse({ groups: [{ errorCode: 14, groupId: 'g' }] })).rejects.toThrow()
    await expect(parse({ groups: [{ errorCode: 16, groupId: 'g' }] })).rejects.toThrow()
    await expect(
      parse({
        groups: [
          { errorCode: 0, groupId: 'a' },
          { errorCode: 14, groupId: 'b' },
        ],
      })
    ).rejects.toThrow()
  })

  test('resolves a batch that mixes a described group with a missing one', async () => {
    const data = {
      groups: [
        { errorCode: 0, groupId: 'a' },
        { errorCode: 69, groupId: 'b' },
      ],
    }

    await expect(parse(data)).resolves.toBe(data)
  })

  test('keeps the 2.2.4 key names', async () => {
    // `state` and `protocol`, not `groupState` and `protocolData`: these names are a compatibility
    // contract, and the wire names are only what the schema calls them.
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(Object.keys(data.groups[0])).toEqual(
      expect.arrayContaining([
        'errorCode',
        'groupId',
        'state',
        'protocolType',
        'protocol',
        'members',
      ])
    )
    expect(Object.keys(data.groups[0])).not.toContain('groupState')
    expect(Object.keys(data.groups[0])).not.toContain('protocolData')
  })
})
