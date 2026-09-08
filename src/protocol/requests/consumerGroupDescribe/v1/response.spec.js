const { decode, parse } = require('./response')

describe('Protocol > Requests > ConsumerGroupDescribe > v1', () => {
  test('decodes the memberType vector the Apache 4.3.1 codec produced', async () => {
    const {
      hex,
      fields,
    } = require('../fixtures/java/consumerGroupDescribeResponse-v1-memberType.json')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      groups: fields.groups,
    })
    expect(data.groups[0].members[0].memberType).toEqual(1)
  })

  test('decodes both assignments of a member, keyed by id and by name', async () => {
    const { hex } = require('../fixtures/java/consumerGroupDescribeResponse-v1-memberType.json')
    const [member] = (await decode(Buffer.from(hex, 'hex'))).groups[0].members

    expect(member.assignment.topicPartitions[0]).toEqual({
      topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      topicName: 'test-topic',
      partitions: [0, 2],
    })
    expect(member.targetAssignment).toEqual(member.assignment)
  })

  test('reports a group-level error instead of throwing for the whole batch', async () => {
    // MemberType lives inside the Member struct, so a group with no members has the same bytes at
    // both versions - which makes the v0 not-found golden a legal v1 frame to read here.
    const {
      hex,
      fields,
    } = require('../fixtures/java/consumerGroupDescribeResponse-v0-groupIdNotFound.json')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.groups[0].errorCode).toEqual(69)
    expect(data.groups[0].errorMessage).toEqual(fields.groups[0].errorMessage)
    expect(data.groups[0].members).toEqual([])
    await expect(parse(data)).resolves.toEqual(data)
  })

  test('a v0 decoder cannot read v1 bytes, because MemberType shifts everything after it', async () => {
    // Not "it throws or it differs, say which": the assertion is that the two decoders disagree,
    // which is true whichever way the desynchronised tag buffer lands. A v0 decoder that read v1
    // bytes to the same values would mean the version is decorative.
    const v1Bytes = Buffer.from(
      require('../fixtures/java/consumerGroupDescribeResponse-v1-memberType.json').hex,
      'hex'
    )
    const decodeV0 = require('../v0/response').decode

    const asV1 = await decode(v1Bytes)
    let asV0 = null
    let threw = null

    try {
      asV0 = await decodeV0(v1Bytes)
    } catch (e) {
      threw = e
    }

    expect(threw !== null || JSON.stringify(asV0) !== JSON.stringify(asV1)).toBe(true)
    expect(asV1.groups[0].members[0].memberType).toEqual(1)
  })
})
