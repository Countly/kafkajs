const {
  MEMBERSHIP_ERRORS,
  membershipAction,
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
