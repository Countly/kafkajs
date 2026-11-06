const { toTopicsMetadata, withReadDeadline } = require('../describeTopicsByPartition')
const { currentDeadline } = require('../deadline')

const row = (topicAuthorizedOperations) => ({
  errorCode: 0,
  name: 'orders',
  topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
  isInternal: false,
  partitions: [],
  ...(topicAuthorizedOperations === undefined ? {} : { topicAuthorizedOperations }),
})
const maskOf = (rows, options) => toTopicsMetadata(rows, options).topics[0].authorizedOperations

describe('toTopicsMetadata authorization masks', () => {
  test('without options the broker mask is not projected', () => {
    expect(maskOf([row(8)])).toBeNull()
  })

  test('a supported mask is kept when the caller asks for it', () => {
    expect(maskOf([row(8)], { includeTopicAuthorizedOperations: true })).toBe(8)
  })

  test.each([
    ['a null mask', null],
    ['an absent mask', undefined],
    ['the signed minimum the broker sends for an unsupported mask', -2147483648],
  ])('%s stays null even when the caller asks for masks', (_label, mask) => {
    expect(maskOf([row(mask)], { includeTopicAuthorizedOperations: true })).toBeNull()
  })

  test('the first failing topic rejects with its protocol error', () => {
    expect(() => toTopicsMetadata([row(), { ...row(), errorCode: 3 }])).toThrow(
      'This server does not host this topic-partition'
    )
  })
})

describe('withReadDeadline', () => {
  test('called outside an admin invocation it opens one named fetchTopicMetadata', async () => {
    const seen = await withReadDeadline(async (read) => ({
      method: currentDeadline().method,
      timeoutMs: currentDeadline().timeoutMs,
      pages: read.pages,
    }))

    expect(seen).toEqual({ method: 'fetchTopicMetadata', timeoutMs: 60000, pages: null })
  })

  test('called outside an admin invocation it honours the given budget', async () => {
    const timeoutMs = await withReadDeadline(async () => currentDeadline().timeoutMs, 250)

    expect(timeoutMs).toBe(250)
  })
})
