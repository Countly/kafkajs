const { itemRetry } = require('../itemRetry')
const { runWithDeadline } = require('../deadline')
const { KafkaJSInvariantViolation } = require('../../errors')

const options = (overrides = {}) => ({
  items: [{ name: 'a' }, { name: 'b' }, { name: 'c' }],
  keyOf: (item) => item.name,
  retry: { retries: 1, initialRetryTime: 1, factor: 0 },
  retryable: (row) => row.errorCode === 14,
  callFailure: () => false,
  sanitize: (message) => message,
  ...overrides,
})
const row = (name, errorCode = 0) => ({ name, errorCode })

describe('admin item retry results', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  it('retries only unresolved rows and returns request order after reordered answers', async () => {
    const attempts = []
    const result = itemRetry(options(), async (items) => {
      attempts.push(items.map(({ name }) => name))
      return attempts.length === 1 ? [row('c', 14), row('b', 29), row('a')] : [row('c')]
    })
    await jest.runAllTimersAsync()
    const rows = await result
    expect(attempts).toEqual([['a', 'b', 'c'], ['c']])
    expect(rows.map(({ name, errorCode }) => [name, errorCode])).toEqual([
      ['a', 0],
      ['b', 29],
      ['c', 0],
    ])
    expect(rows[1].error.type).toBe('TOPIC_AUTHORIZATION_FAILED')
  })

  it.each([
    [row('a'), row('a'), row('c')],
    [row('a')],
    [row('a'), row('b'), row('foreign')],
    [row('a'), row('b'), row('c', '0')],
  ])('rejects malformed result identities %p', async (...rows) => {
    await expect(
      itemRetry(options({ retry: { retries: 0 } }), async () => rows)
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })

  it('keeps the last typed broker row on exhaustion', async () => {
    const rows = await itemRetry(options({ retry: { retries: 0 } }), async () => [
      row('a'),
      row('b'),
      { ...row('c', 14), errorMessage: 'loading' },
    ])
    expect(rows[2]).toMatchObject({ errorCode: 14, errorMessage: 'loading' })
    expect(rows[2].error.type).toBe('GROUP_LOAD_IN_PROGRESS')
  })

  it('assigns a later transport failure only to unresolved cells', async () => {
    const failure = new Error('transport closed')
    let attempts = 0
    const result = itemRetry(options(), async () => {
      if (++attempts === 1) return [row('a'), row('b', 29), row('c', 14)]
      throw failure
    })
    await jest.runAllTimersAsync()
    const rows = await result
    expect(rows[0].errorCode).toBe(0)
    expect(rows[1].errorCode).toBe(29)
    expect(rows[2]).toMatchObject({ errorCode: -1, error: failure })
  })

  it('rejects call-level capability failures with their original identity', async () => {
    const failure = new Error('unsupported capability')
    let refusal
    const result = itemRetry(options({ callFailure: (error) => error === failure }), async () => {
      throw failure
    }).catch((error) => (refusal = error))
    for (let n = 0; n < 20; n++) await Promise.resolve()
    expect(refusal).toBe(failure)
    await result
  }, 1000)

  it('rejects a held result at the API deadline and observes its late rejection', async () => {
    let reject
    const held = new Promise((resolve, no) => (reject = no))
    const result = runWithDeadline('items', 5, () => itemRetry(options(), () => held))
    const observed = result.catch((error) => error)
    await jest.advanceTimersByTimeAsync(5)
    expect(await observed).toMatchObject({ name: 'KafkaJSTimeout' })
    reject(new Error('late'))
    await Promise.resolve()
  })
})
