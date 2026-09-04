const {
  isRebalancing,
  isStaleCoordinator,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
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

describe('error wrapping preserves the protocol type', () => {
  const protocolError = new KafkaJSProtocolError({
    type: 'REBALANCE_IN_PROGRESS',
    code: 27,
    retriable: false,
    message: 'The group is rebalancing, so a rejoin is needed',
  })

  it('KafkaJSError keeps type and code when wrapping a protocol error', () => {
    const wrapped = new KafkaJSError(protocolError)
    expect(wrapped.type).toEqual('REBALANCE_IN_PROGRESS')
    expect(wrapped.code).toEqual(27)
  })

  it('KafkaJSError does not invent a type when wrapping a string or a plain Error', () => {
    expect(new KafkaJSError('boom')).not.toHaveProperty('type')
    expect(new KafkaJSError(new Error('boom'))).not.toHaveProperty('type')
  })

  it('KafkaJSNumberOfRetriesExceeded keeps the original type and code', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(protocolError, {
      retryCount: 5,
      retryTime: 100,
    })
    expect(exceeded.type).toEqual('REBALANCE_IN_PROGRESS')
    expect(exceeded.code).toEqual(27)
    expect(exceeded.cause).toBe(protocolError)
  })

  it('KafkaJSNumberOfRetriesExceeded digs through a chain of wrappers', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(new KafkaJSError(protocolError), {
      retryCount: 5,
      retryTime: 100,
    })
    expect(exceeded.type).toEqual('REBALANCE_IN_PROGRESS')
  })

  it('KafkaJSNonRetriableError keeps type and code for a fenced static member (code 82)', () => {
    const fencedError = new KafkaJSProtocolError({
      type: 'FENCED_INSTANCE_ID',
      code: 82,
      retriable: false,
      message: 'The broker rejected this static consumer',
    })

    const wrapped = new KafkaJSNonRetriableError(fencedError)
    expect(wrapped.type).toEqual('FENCED_INSTANCE_ID')
    expect(wrapped.code).toEqual(82)
  })
})
