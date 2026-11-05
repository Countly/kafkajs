const { exact, validateTopics } = require('./responseIdentity')
const { KafkaJSInvariantViolation } = require('../../../errors')

/** Returns the invariant violation the call throws, and fails the test when it does not throw. */
const violation = (fn) => {
  try {
    fn()
  } catch (error) {
    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    return error
  }
  throw new Error('expected the call to throw')
}

describe('Protocol > Requests > OffsetFetch > response identity edges', () => {
  test('exact rejects rows that are not an array', () => {
    const error = violation(() => exact(undefined, null, 'topic', 'OffsetFetch topic'))

    expect(error.message).toContain('Missing OffsetFetch topic.')
  })

  test.each([
    ['an empty topic name', 'topic', [{ topic: '' }]],
    ['a topic name with a NUL', 'topic', [{ topic: 'a\0b' }]],
    ['a missing row', 'topic', [null]],
    ['a fractional partition', 'partition', [{ partition: 1.5 }]],
    ['a negative partition', 'partition', [{ partition: -1 }]],
    ['a partition above the int32 range', 'partition', [{ partition: 2147483648 }]],
  ])('exact rejects %s as an invalid identity', (_label, field, rows) => {
    const error = violation(() => exact(rows, null, field, 'OffsetFetch row'))

    expect(error.message).toContain('Invalid OffsetFetch row identity.')
  })

  test('validateTopics checks nothing in a group the broker answered with an error', () => {
    const requested = [{ topic: 't', partitions: [{ partition: 0 }] }]

    expect(validateTopics({ errorCode: 30, topics: undefined }, requested)).toBeUndefined()
  })

  test('validateTopics checks the topics of a group the broker answered successfully', () => {
    const requested = [{ topic: 't', partitions: [{ partition: 0 }] }]

    const error = violation(() => validateTopics({ errorCode: 0, topics: undefined }, requested))

    expect(error.message).toContain('Missing OffsetFetch topic.')
  })
})
