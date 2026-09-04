const {
  isRebalancing,
  isStaleCoordinator,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
} = require('./errors')
const { createErrorFromCode } = require('./protocol/error')

describe('isRebalancing', () => {
  it.each(['REBALANCE_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP', 'ILLEGAL_GENERATION'])(
    'returns true for %s',
    (type) => expect(isRebalancing({ type })).toBe(true)
  )

  it('returns true for the coordinator-loading error (code 14)', () => {
    expect(isRebalancing({ type: 'GROUP_LOAD_IN_PROGRESS', code: 14 })).toBe(true)
  })

  it.each(['UNKNOWN_MEMBER_ID', 'NETWORK_EXCEPTION', 'BROKER_NOT_AVAILABLE'])(
    'returns false for %s',
    (type) => expect(isRebalancing({ type })).toBe(false)
  )

  it('returns false when the error has no type', () => {
    expect(isRebalancing({})).toBe(false)
  })
})

describe('isStaleCoordinator', () => {
  it.each([15, 16])('returns true for code %i regardless of the type string', (code) => {
    expect(isStaleCoordinator({ code })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isStaleCoordinator({ code: 14 })).toBe(false)
    expect(isStaleCoordinator({})).toBe(false)
  })
})

describe('isLostMember', () => {
  it.each([22, 25, 82])('returns true for code %i (member is LOST)', (code) => {
    expect(isLostMember({ code })).toBe(true)
  })

  it.each([27, 16, 14])('returns false for code %i (member is not lost)', (code) => {
    expect(isLostMember({ code })).toBe(false)
  })

  it('returns false when the error has no code', () => {
    expect(isLostMember({})).toBe(false)
  })
})

describe('isRevoked', () => {
  it.each([27, 16, 14])(
    'returns true for code %i (rebalance-class, member still valid)',
    (code) => {
      expect(isRevoked(createErrorFromCode(code))).toBe(true)
    }
  )

  it.each([22, 25, 82])('returns false for code %i (member is lost, not revoked)', (code) => {
    expect(isRevoked(createErrorFromCode(code))).toBe(false)
  })

  it('returns true for a type-only REBALANCE_IN_PROGRESS error with no code', () => {
    expect(isRevoked({ type: 'REBALANCE_IN_PROGRESS' })).toBe(true)
  })

  it('returns false when the error has no code or type', () => {
    expect(isRevoked({})).toBe(false)
  })
})

describe('isFencedInstance', () => {
  it('returns true for code 82', () => {
    expect(isFencedInstance({ code: 82 })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isFencedInstance({ code: 22 })).toBe(false)
    expect(isFencedInstance({})).toBe(false)
  })
})

describe('isUnknownMemberId', () => {
  it('returns true for code 25', () => {
    expect(isUnknownMemberId({ code: 25 })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isUnknownMemberId({ code: 22 })).toBe(false)
    expect(isUnknownMemberId({})).toBe(false)
  })
})
