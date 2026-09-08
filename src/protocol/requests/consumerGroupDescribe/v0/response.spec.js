const { decode, parse } = require('./response')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupDescribeResponse-v0-${caseId}.json`)

describe('Protocol > Requests > ConsumerGroupDescribe > v0', () => {
  test('decodes the described-group vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector('describedGroup')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      groups: fields.groups.map((group) => ({
        ...group,
        // v0 has no MemberType field at all; the key exists so the shape does not change
        // between versions, and it is null rather than -1 so "absent" and "unknown" stay apart.
        members: group.members.map((member) => ({ ...member, memberType: null })),
      })),
    })
  })

  test('reports a group-level error instead of throwing for the whole batch', async () => {
    // GROUP_ID_NOT_FOUND on this API is the "ask DescribeGroups instead" signal, not a failure.
    // A parse that threw would make that routing unreachable.
    const { hex, fields } = vector('groupIdNotFound')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.groups[0].errorCode).toEqual(69)
    expect(data.groups[0].errorMessage).toEqual(fields.groups[0].errorMessage)
    expect(data.groups[0].members).toEqual([])
    await expect(parse(data)).resolves.toEqual(data)
  })

  test('decodes both assignments of a member, keyed by id and by name', async () => {
    const data = await decode(Buffer.from(vector('describedGroup').hex, 'hex'))
    const [member] = data.groups[0].members

    expect(member.assignment.topicPartitions[0]).toEqual({
      topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      topicName: 'test-topic',
      partitions: [0, 2],
    })
    expect(member.targetAssignment).toEqual(member.assignment)
  })

  test('reports one failing group beside one succeeding group, in the same response', async () => {
    // Per-group errors are not only for the not-found signal: an authorization failure on one id
    // must not cost the caller the other id's answer. TOPIC_AUTHORIZATION_FAILED is the code the
    // coordinator sends when the member's subscription cannot be read back.
    const { hex, fields } = vector('mixedErrors')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.groups).toHaveLength(2)
    expect(data.groups[0]).toMatchObject({
      errorCode: 0,
      groupId: 'test-group',
      errorMessage: null,
    })
    expect(data.groups[1]).toMatchObject({
      errorCode: 29,
      groupId: 'other-group',
      errorMessage: fields.groups[1].errorMessage,
    })
    await expect(parse(data)).resolves.toEqual(data)
  })

  test('reports the broker throttle as clientSideThrottleTime and pins throttleTime at 0', async () => {
    // No committed vector carries a non-zero ThrottleTimeMs, so the KIP-219 split is pinned from
    // a hand-built frame: 250 ms of throttle, then an empty group array and an empty tag buffer.
    const data = await decode(Buffer.from('000000fa0100', 'hex'))

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 250, groups: [] })
  })
})
