const { assertSameProtocol } = require('./groupProtocolCheck')

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
