const { errorCodes, createErrorFromCode } = require('./error')

describe('Protocol > error', () => {
  describe('#createErrorFromCode', () => {
    it('creates enhanced errors based on kafka error codes', () => {
      for (const errorCode of errorCodes) {
        const error = createErrorFromCode(errorCode.code)
        expect(error).toHaveProperty('message', errorCode.message)
        expect(error).toHaveProperty('type', errorCode.type)
        expect(error).toHaveProperty('code', errorCode.code)
        expect(error).toHaveProperty('retriable', errorCode.retriable)
      }
    })

    it('has a fallback error in case the error code is not supported', () => {
      const error = createErrorFromCode(123456789)
      expect(error).toBeTruthy()
      expect(error).toHaveProperty('type', 'KAFKAJS_UNKNOWN_ERROR_CODE')
      expect(error).toHaveProperty('code', -99)
      expect(error).toHaveProperty('retriable', false)
      expect(error).toHaveProperty('message', 'Unknown error code 123456789')
    })

    it('links to the corresponding FAQ entry if available', () => {
      const rebalanceInProgressCode = errorCodes.find(
        ({ type }) => type === 'REBALANCE_IN_PROGRESS'
      ).code
      const error = createErrorFromCode(rebalanceInProgressCode)
      expect(error).toHaveProperty(
        'helpUrl',
        'https://github.com/Countly/kafkajs/blob/master/docs/FAQ.md#what-does-it-mean-to-get-rebalance_in_progress-errors'
      )
    })
  })
})

describe('THROTTLING_QUOTA_EXCEEDED (89)', () => {
  it('is a known, retriable protocol error', () => {
    const error = createErrorFromCode(89)
    expect(error.type).toEqual('THROTTLING_QUOTA_EXCEEDED')
    expect(error.code).toEqual(89)
    expect(error.retriable).toBe(true)
  })
})

describe('error codes 90-133 (Kafka 4.3.1 Errors.java)', () => {
  test.each([
    [90, 'PRODUCER_FENCED', false],
    [100, 'UNKNOWN_TOPIC_ID', true],
    [103, 'INCONSISTENT_TOPIC_ID', true],
    [106, 'FETCH_SESSION_TOPIC_ID_ERROR', true],
    [128, 'INVALID_REGULAR_EXPRESSION', false],
    [129, 'REBOOTSTRAP_REQUIRED', false],
  ])('code %i is %s (retriable: %s)', (code, type, retriable) => {
    const error = createErrorFromCode(code)
    expect(error.type).toEqual(type)
    expect(error.code).toEqual(code)
    expect(error.retriable).toBe(retriable)
  })

  test('every code from -1 to 133 that Kafka defines is known', () => {
    const snapshot = require('../../scripts/compat/errors/4.3.1.json')
    const known = new Set(errorCodes.map((entry) => entry.code))
    const missing = snapshot.errors
      .filter((entry) => entry.code !== 0)
      .map((entry) => entry.code)
      .filter((code) => !known.has(code))

    expect(missing).toEqual([])
  })

  test('the KIP-848 consumer-protocol codes are present', () => {
    for (const code of [110, 111, 112, 113]) {
      expect(createErrorFromCode(code).type).not.toEqual('KAFKAJS_UNKNOWN_ERROR_CODE')
    }
  })
})
