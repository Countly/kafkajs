const {
  assertSameProtocol,
  assertJoinAnswer,
  malformedGroupAnswer,
  isMalformedGroupAnswer,
} = require('./groupProtocolCheck')
const { createErrorFromCode } = require('../error')

describe('Protocol > Requests > group protocol check', () => {
  test.each([
    ['equal values', 'consumer', 'consumer'],
    ['no value reported', null, 'consumer'],
    ['no value reported (undefined)', undefined, 'consumer'],
    ['no value sent', 'consumer', null],
    ['no value sent (undefined)', 'consumer', undefined],
    ['neither side set', null, null],
  ])('accepts %s', (_, received, sent) => {
    expect(() => assertSameProtocol(received, sent)).not.toThrow()
  })

  test.each([
    ['another protocol type', 'connect', 'consumer'],
    ['a different case', 'Consumer', 'consumer'],
    ['an empty string against a value', '', 'consumer'],
    ['a value against an empty string', 'consumer', ''],
  ])('raises INCONSISTENT_GROUP_PROTOCOL for %s, and it is not retriable', (_, received, sent) => {
    expect(() => assertSameProtocol(received, sent)).toThrow(
      expect.objectContaining({ type: 'INCONSISTENT_GROUP_PROTOCOL', code: 23, retriable: false })
    )
  })
})

describe('Protocol > Requests > JoinGroup answer check', () => {
  const OFFERED = [{ name: 'RoundRobinAssigner' }, { name: 'planner-\uD800' }]

  /**
   * @param {object} [fields] over a follower's answer
   * @returns {object} a decoded JoinGroup success
   */
  const answer = (fields = {}) => ({
    memberId: 'member-1',
    generationId: 0,
    groupProtocol: 'RoundRobinAssigner',
    leaderId: 'leader-1',
    members: [],
    ...fields,
  })
  const LEADER = { leaderId: 'member-1' }

  test.each([
    ['a follower answer', answer()],
    ['a follower answer, whose member list is not read', answer({ members: [{ memberId: null }] })],
    [
      'a leader answer listing every member once',
      answer({ ...LEADER, members: [{ memberId: 'member-1' }, { memberId: 'member-2' }] }),
    ],
    ['the echo of an offered name with a lone surrogate', answer({ groupProtocol: 'planner-�' })],
  ])('accepts %s', (_, decoded) => {
    expect(() => assertJoinAnswer(decoded, OFFERED)).not.toThrow()
  })

  test.each([
    ['no member id', answer({ memberId: null }), 'JoinGroup response has no member id'],
    ['an empty member id', answer({ memberId: '' }), 'JoinGroup response has no member id'],
    ['generation -1', answer({ generationId: -1 }), 'JoinGroup response names generation -1'],
    [
      'a protocol the member did not offer',
      answer({ groupProtocol: 'OtherAssigner' }),
      'JoinGroup response names protocol OtherAssigner, which this member did not offer',
    ],
    [
      'no protocol',
      answer({ groupProtocol: null }),
      'JoinGroup response names protocol null, which this member did not offer',
    ],
    [
      'a leader list with a member without an id',
      answer({ ...LEADER, members: [{ memberId: 'member-1' }, { memberId: '' }] }),
      'JoinGroup response to the leader lists a member with no id',
    ],
    [
      'a leader list naming a member twice',
      answer({ ...LEADER, members: [{ memberId: 'member-1' }, { memberId: 'member-1' }] }),
      'JoinGroup response to the leader lists member member-1 twice',
    ],
    [
      'a leader list without the leader',
      answer({ ...LEADER, members: [{ memberId: 'member-2' }] }),
      'JoinGroup response to the leader does not list the leader member-1',
    ],
  ])('refuses %s as a retriable MALFORMED_GROUP_ANSWER', (_, decoded, message) => {
    let refusal
    try {
      assertJoinAnswer(decoded, OFFERED)
    } catch (error) {
      refusal = error
    }

    expect(refusal).toMatchObject({ type: 'MALFORMED_GROUP_ANSWER', retriable: true, message })
    expect(isMalformedGroupAnswer(refusal)).toBe(true)
  })

  test('isMalformedGroupAnswer is false for a protocol error and for no error', () => {
    expect(isMalformedGroupAnswer(createErrorFromCode(23))).toBe(false)
    expect(isMalformedGroupAnswer(undefined)).toBe(false)
    expect(isMalformedGroupAnswer(malformedGroupAnswer('SyncGroup response'))).toBe(true)
  })
})
