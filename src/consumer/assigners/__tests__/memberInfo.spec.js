const { compareMembers } = require('../memberInfo')

const member = (memberId, groupInstanceId = null) => ({ memberId, groupInstanceId })

describe('Consumer > assigners > memberInfo', () => {
  it('compares by groupInstanceId when both have one', () => {
    expect(compareMembers(member('z', 'a'), member('a', 'b'))).toBeLessThan(0)
    expect(compareMembers(member('a', 'b'), member('z', 'a'))).toBeGreaterThan(0)
  })

  it('sorts a member with a groupInstanceId before one without', () => {
    expect(compareMembers(member('z', 'zzz'), member('a'))).toBeLessThan(0)
    expect(compareMembers(member('a'), member('z', 'zzz'))).toBeGreaterThan(0)
  })

  it('compares by memberId when neither has one', () => {
    expect(compareMembers(member('a'), member('b'))).toBeLessThan(0)
    expect(compareMembers(member('b'), member('a'))).toBeGreaterThan(0)
    expect(compareMembers(member('a'), member('a'))).toEqual(0)
  })

  it('treats an absent groupInstanceId the same as a null one', () => {
    expect(compareMembers({ memberId: 'a' }, { memberId: 'b' })).toBeLessThan(0)
    expect(compareMembers({ memberId: 'z', groupInstanceId: 'i' }, { memberId: 'a' })).toBeLessThan(
      0
    )
    expect(
      compareMembers({ memberId: 'a' }, { memberId: 'z', groupInstanceId: 'i' })
    ).toBeGreaterThan(0)
  })

  it('reports two members with the same groupInstanceId as equal', () => {
    expect(compareMembers(member('a', 'same'), member('z', 'same'))).toEqual(0)
  })

  it('produces a total order a sort can rely on', () => {
    const members = [member('c'), member('a', 'i2'), member('b'), member('d', 'i1')]

    expect([...members].sort(compareMembers).map(({ memberId }) => memberId)).toEqual([
      'd',
      'a',
      'b',
      'c',
    ])
  })
})
