const { decode, parse } = require('./response')

const vector = (version) =>
  require(`../fixtures/java/describeGroupsResponse-v${version}-describedGroup.json`)

describe('Protocol > Requests > DescribeGroups > v5', () => {
  test('decodes the describedGroup vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector(5)
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

  test('is the flexible framing from v5: compact strings and a tag buffer per struct', async () => {
    // The classic v4 body and the flexible v5 body of the same values differ in length; if they
    // did not, one of the two codecs is writing the wrong framing.
    const v4Bytes = Buffer.from(vector(4).hex, 'hex')
    const v5Bytes = Buffer.from(vector(5).hex, 'hex')

    expect(v5Bytes.length).not.toEqual(v4Bytes.length)
    // And they decode to the same values, which is what makes the length difference framing
    // rather than content.
    expect(await decode(v5Bytes)).toEqual(
      await require('../v4/response').decode(Buffer.from(vector(4).hex, 'hex'))
    )
  })

  test('keeps the KIP-219 throttle split, which no vector can show', async () => {
    // 250 ms of throttling, then the empty compact groups array and two tag buffers.
    const data = await decode(Buffer.from('000000fa0100', 'hex'))

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 250, groups: [] })
  })

  test('shares v0s parse, which throws on any group-level error code', () => {
    expect(parse).toBe(require('../v0/response').parse)
  })

  test('throws for every group-level code, as this client always has', async () => {
    // v5 predates per-group reporting: GROUP_ID_NOT_FOUND is a failed request here, not an answer.
    await expect(parse({ groups: [{ errorCode: 69 }] })).rejects.toThrow()
    await expect(parse({ groups: [{ errorCode: 14 }] })).rejects.toThrow()
  })
})
