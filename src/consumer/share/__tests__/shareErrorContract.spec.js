const {
  MEMBERSHIP_ERRORS,
  membershipAction,
  lookupFailure,
  fatalMembershipError,
  createErrorFromCode,
  sessionAction,
  transportAction,
  fetchAction,
  acknowledgementAction,
  fatalFetchError,
} = require('../shareErrorContract')
const {
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSProtocolError,
  KafkaJSShareGroupsNotSupportedError,
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSRequestTimeoutError,
  KafkaJSNonRetriableError,
  KafkaJSInvariantViolation,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSLockTimeout,
  KafkaJSSASLAuthenticationError,
  KafkaJSInvalidConfigurationError,
} = require('../../../errors')

describe('share membership failure policy', () => {
  test.each([
    [14, 'coordinator'],
    [15, 'coordinator'],
    [16, 'coordinator'],
    [25, 'fence'],
    [110, 'fence'],
    [35, 'unsupported'],
    [30, 'fatal'],
    [29, 'fatal'],
    [81, 'fatal'],
    [42, 'fatal'],
    [69, 'groupNotFound'],
    [7, 'retry'],
    [3, 'retry'],
    [31, 'fatal'],
  ])('classifies code %s as %s', (code, action) => {
    expect(membershipAction(createErrorFromCode(code))).toBe(action)
  })
  test.each([
    ['a replaced connection', new KafkaJSConnectionError('This broker connection was replaced')],
    [
      'a refused connection with a system code',
      new KafkaJSConnectionError('Connection error', { code: 'ECONNREFUSED' }),
    ],
    ['a closed connection', new KafkaJSConnectionClosedError('Closed connection')],
    ['a request timeout', new KafkaJSRequestTimeoutError('Request timed out')],
  ])('forgets the coordinator after %s, which is not a broker answer', (_name, error) => {
    expect(membershipAction(error)).toBe('coordinator')
  })
  test.each([
    ['a non-retriable failure', new KafkaJSNonRetriableError('refused')],
    ['an invariant violation', new KafkaJSInvariantViolation('broken')],
  ])('keeps %s without a broker code fatal', (_name, error) => {
    expect(membershipAction(error)).toBe('fatal')
  })
  test('forgets the coordinator after a lock timeout, which a later attempt can pass', () => {
    expect(membershipAction(new KafkaJSLockTimeout('Timeout while acquiring lock'))).toBe(
      'coordinator'
    )
  })
  test('keeps the membership table read-only', () => {
    expect(Object.isFrozen(MEMBERSHIP_ERRORS)).toBe(true)
  })
  test('keeps unsupported inheritance and fatal code and cause', () => {
    expect(fatalMembershipError(createErrorFromCode(35))).toBeInstanceOf(
      KafkaJSServerDoesNotSupportApiKey
    )
    const error = createErrorFromCode(81)
    expect(fatalMembershipError(error)).toMatchObject({
      code: 81,
      retriable: false,
      cause: error,
    })
  })
  test('wraps code 69 as a plain non-retriable error that keeps its code and cause', () => {
    const error = createErrorFromCode(69)
    const fatal = fatalMembershipError(error)
    expect(fatal).toMatchObject({ code: 69, retriable: false, cause: error })
    expect(fatal).not.toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
  })
})

describe('share fetch and acknowledgement failure policy', () => {
  test('transport failure preserves the original cause and never confirms a mutation', () => {
    const error = new Error('request timed out')
    expect(transportAction(error)).toEqual({
      action: 'reset',
      result: 'session-lost',
      reason: 'connection',
      error,
    })
  })

  test.each([
    [78, 122, 'reset'],
    [78, 123, 'reset'],
    [78, 133, 'reset'],
    [79, 122, 'reset'],
    [79, 123, 'reset'],
    [79, 133, 'retry'],
    [78, -1, 'retry'],
    [79, -1, 'retry'],
    [78, 25, 'fence'],
    [79, 25, 'fence'],
    [78, 30, 'fatal'],
    [79, 30, 'fatal'],
    [78, 42, 'fatal'],
    [78, 7, 'retry'],
  ])('API %s code %s is %s', (api, code, action) => {
    expect(sessionAction(api, code).action).toBe(action)
  })

  test('a top-level unknown error such as a deleted topic is paced and never resets', () => {
    expect(sessionAction(78, -1)).toMatchObject({ action: 'retry', backoff: true })
    expect(sessionAction(78, 0)).toEqual({ action: 'ok', backoff: false, error: null })
  })

  test('a code 35 refusal carries the protocol error as cause', () => {
    for (const api of [78, 79]) {
      const { error } = sessionAction(api, 35)
      expect(error.cause).toBeInstanceOf(KafkaJSProtocolError)
      expect(error.cause).toMatchObject({ type: 'UNSUPPORTED_VERSION', code: 35 })
    }
    const heartbeat = createErrorFromCode(35)
    const fatal = fatalMembershipError(heartbeat)
    expect(fatal).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(fatal.cause).toBe(heartbeat)
  })

  test.each([78, 79])('runtime 35 on API %s names its actual API', (api) => {
    const policy = sessionAction(api, 35)
    expect(policy.action).toBe('fatal')
    expect(policy.error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(policy.error).toMatchObject({
      apiKey: api,
      apiName: api === 78 ? 'ShareFetch' : 'ShareAcknowledge',
      reason: 'refused-by-broker',
      retriable: false,
    })
  })

  test.each([
    [6, 'move'],
    [74, 'move'],
    [3, 'refresh'],
    [100, 'refresh'],
    [103, 'refresh'],
    [9, 'refresh'],
    [56, 'refresh'],
    [78, 'refresh'],
    [29, 'fatal'],
    [-1, 'retry'],
    [2, 'fatal'],
    [7, 'refresh'],
    [42, 'fatal'],
  ])('fetch code %s is %s', (code, action) => {
    expect(fetchAction(code).action).toBe(action)
  })

  test('corrupt records fail with a non-retriable protocol error despite the table flag', () => {
    expect(createErrorFromCode(2).retriable).toBe(true)
    const error = fetchAction(2).error
    expect(fatalFetchError(createErrorFromCode(2))).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({ code: 2, retriable: false })
  })

  test('a fatal authorization failure keeps its protocol identity and is not retriable', () => {
    expect(fetchAction(29).error).toMatchObject({ code: 29, retriable: false })
    expect(fetchAction(29).error).toBeInstanceOf(KafkaJSProtocolError)
  })

  test.each([
    [0, 'ok'],
    [121, 'rejected-by-broker'],
    [6, 'session-lost'],
    [74, 'session-lost'],
    [100, 'rejected-by-broker'],
    [133, 'rejected-by-broker'],
    [-1, 'rejected-by-broker'],
  ])('acknowledgement code %s settles %s', (code, result) => {
    expect(acknowledgementAction(code).result).toBe(result)
  })

  test('a rejected acknowledgement keeps the broker code as its cause and a confirmed one has none', () => {
    expect(acknowledgementAction(121).cause).toMatchObject({ code: 121 })
    expect(acknowledgementAction(0).cause).toBeUndefined()
  })
})

describe('share coordinator lookup failure policy', () => {
  const refused = () => new KafkaJSConnectionError('Connection error', { code: 'ECONNREFUSED' })
  const exhausted = (error) =>
    new KafkaJSNumberOfRetriesExceeded(error, { retryCount: 5, retryTime: 300 })
  const bailed = (error) => new KafkaJSNonRetriableError(error, { cause: error })

  test.each([
    ['retries exhausted on a refused connection', exhausted(refused())],
    ['a bailed refused connection', bailed(refused())],
    ['no coordinator found', new KafkaJSGroupCoordinatorNotFound('Failed to find coordinator')],
    ['retries exhausted on COORDINATOR_NOT_AVAILABLE', exhausted(createErrorFromCode(15))],
    ['retries exhausted on a request timeout', exhausted(new KafkaJSRequestTimeoutError('late'))],
    ['a lock timeout', new KafkaJSLockTimeout('Timeout while acquiring lock')],
    ['a retriable answer', createErrorFromCode(14)],
    ['an invariant violation', new KafkaJSInvariantViolation('broken')],
  ])('forgets the coordinator and retries after %s', (_name, error) => {
    expect(lookupFailure(error)).toEqual({ action: 'coordinator', error })
  })

  test.each([
    ['GROUP_AUTHORIZATION_FAILED', bailed(createErrorFromCode(30))],
    ['GROUP_AUTHORIZATION_FAILED unwrapped', createErrorFromCode(30)],
    ['INVALID_REQUEST', bailed(createErrorFromCode(42))],
    ['an unknown server error', bailed(createErrorFromCode(-1))],
    ['a SASL authentication failure', bailed(new KafkaJSSASLAuthenticationError('bad password'))],
    ['an invalid configuration', bailed(new KafkaJSInvalidConfigurationError('bad timeout'))],
  ])('ends the membership after %s, reporting the failure that decided it', (_name, error) => {
    const decided = lookupFailure(error)
    expect(decided.action).toBe('fatal')
    expect(decided.error).toBe(error.cause ?? error)
  })

  test('ends the membership when no coordinator was found because a broker refused the group', () => {
    const answer = createErrorFromCode(30)
    const notFound = new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
    notFound.lastError = answer
    expect(lookupFailure(bailed(notFound))).toEqual({ action: 'fatal', error: answer })
  })

  test('retries when no coordinator was found because the coordinator was unavailable', () => {
    const notFound = new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
    notFound.lastError = createErrorFromCode(15)
    const error = bailed(notFound)
    expect(lookupFailure(error)).toEqual({ action: 'coordinator', error })
  })

  test.each([
    ['an API the broker does not support', new KafkaJSServerDoesNotSupportApiKey('no API')],
    [
      'a broker that cannot run share groups',
      bailed(
        new KafkaJSShareGroupsNotSupportedError('Share groups refused', {
          apiKey: 76,
          apiName: 'ShareGroupHeartbeat',
          reason: 'api-not-advertised',
        })
      ),
    ],
  ])('reports %s as unsupported', (_name, error) => {
    const decided = lookupFailure(error)
    expect(decided.action).toBe('unsupported')
    expect(decided.error).toBe(error.cause ?? error)
  })
})
