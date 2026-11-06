const {
  MEMBERSHIP_ERRORS,
  membershipAction,
  fatalMembershipError,
  createErrorFromCode,
} = require('../shareErrorContract')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../../errors')

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
