const { ShareRecordWalk } = require('../shareRecordWalk')
const { ShareAckLedger, lockDeadline } = require('../shareAckLedger')
const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../../errors')
const failure = (Class, message) => expect.objectContaining({ name: Class.name, message })
const OUTSIDE = 'Acknowledgement is outside its active handler acquisition'
const BAD_DEADLINE = 'Invalid acquisition lock deadline'
const make = (options = {}) => {
  const token = Symbol('acquisition')
  const walk = ShareRecordWalk.walk({
    acquiredRecords: [{ firstOffset: '1', lastOffset: '3', deliveryCount: 4 }],
    messages: [1, 2, 3].map((n) => ({ offset: String(n) })),
  })
  const ledger = new ShareAckLedger({
    topic: 'topic',
    topicId: '11111111-1111-4111-8111-111111111111',
    partition: 0,
    token,
    sentAt: 100,
    lockDuration: 1000,
    walk,
    ...options,
  })
  return { ledger, token, walk }
}
describe('deterministic acquisition locks', () => {
  test('uses pre-write time and blocks at the deadline without a timer', () => {
    const { ledger } = make()
    const timer = jest.spyOn(global, 'setTimeout')
    expect(lockDeadline(100, 1000)).toBe(1100)
    expect(ledger.canStart('1', 1099)).toBe(true)
    expect(ledger.canStart('1', 1100)).toBe(false)
    expect(ledger.dropExpired(['1', '2', '3'], 1100)).toEqual([
      { firstOffset: '1', lastOffset: '3' },
    ])
    expect(ledger.size).toBe(0)
    expect(timer).not.toHaveBeenCalled()
    timer.mockRestore()
  })
  test('half-lock checks flush all completed decisions at start and return, including after expiry', () => {
    const { ledger, token } = make()
    ledger.complete(['1', '2'], 'accept')
    expect(ledger.needsFlush(500)).toBe(false)
    expect(ledger.needsFlush(600)).toBe(false)
    expect(ledger.needsFlush(700)).toBe(true)
    expect(ledger.needsFlush(1500)).toBe(true)
    const { ticket } = ledger.prepare({ token })
    expect(ticket.acknowledgementBatches).toEqual([
      { firstOffset: '1', lastOffset: '2', acknowledgeTypes: [1] },
    ])
    expect(ledger.needsFlush(1500)).toBe(false)
    ledger.complete(['3'], 'release')
    expect(ledger.needsFlush(1500)).toBe(true)
    expect(ledger.settle(ticket, { token })[0].error).toBeNull()
    expect(ledger.needsFlush(1500)).toBe(true)
    const final = ledger.prepare({ token }).ticket
    expect(ledger.needsFlush(1500)).toBe(false)
    expect(ledger.settle(final, { token })[0].type).toBe('release')
    expect(ledger.size).toBe(0)
  })
  test('successful renewal moves only selected deadlines and preserves held state and count', () => {
    const { ledger, token, walk } = make()
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1'] })
    expect(ticket.isRenewAck).toBe(true)
    expect(ticket.acknowledgementBatches[0].acknowledgeTypes).toEqual([4])
    expect(ledger.settle(ticket, { token, sentAt: 900, lockDuration: 2000 })[0].type).toBe('renew')
    expect(walk.deliver.map((item) => item.deliveryCount)).toEqual([4, 4, 4])
    expect(ledger.canStart('1', 1100)).toBe(true)
    expect(ledger.canStart('2', 1100)).toBe(false)
    expect(ledger.canStart('1', 2899)).toBe(true)
    expect(ledger.canStart('1', 2900)).toBe(false)
    ledger.complete(['1'], 'accept')
    expect(ledger.needsFlush(1500)).toBe(false)
    expect(ledger.needsFlush(2000)).toBe(true)
    expect(ledger.size).toBe(3)
  })
  test('uses the earliest decided deadline when a renewal changes its duration', () => {
    const { ledger, token } = make({ lockDuration: 10000 })
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1'] })
    ledger.settle(ticket, { token, sentAt: 8100, lockDuration: 1000 })
    ledger.complete(['1', '2'], 'accept')
    expect(ledger.needsFlush(8200)).toBe(false)
    expect(ledger.needsFlush(8600)).toBe(false)
    expect(ledger.needsFlush(8601)).toBe(true)
  })
  test('failed renewal leaves deadlines and decisions unchanged and duplicate answers are ignored', () => {
    const { ledger, token } = make()
    ledger.decide('1', 'reject')
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1'] })
    const result = ledger.settle(ticket, {
      token,
      result: 'rejected-by-broker',
      cause: new Error('121'),
    })
    expect(result[0].error.reason).toBe('rejected-by-broker')
    expect(ledger.canStart('1', 1099)).toBe(true)
    expect(ledger.canStart('1', 1100)).toBe(false)
    expect(ledger.settle(ticket, { token, sentAt: 9999, lockDuration: 9000 })).toEqual([])
    expect(ledger.canStart('1', 1099)).toBe(true)
    expect(ledger.canStart('1', 1100)).toBe(false)
    ledger.complete(['1'], 'accept')
    expect(ledger.prepare({ token }).ticket.acknowledgementBatches[0].acknowledgeTypes).toEqual([3])
  })
  test('renewal without a duration retains the old duration and validates before mutation', () => {
    const { ledger, token } = make()
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1', '2'] })
    expect(() => ledger.settle(ticket, { token, sentAt: NaN, lockDuration: 1000 })).toThrow(
      failure(KafkaJSNonRetriableError, BAD_DEADLINE)
    )
    expect(ledger.canStart('1', 1100)).toBe(false)
    ledger.settle(ticket, { token, sentAt: 900 })
    expect(ledger.canStart('1', 1899)).toBe(true)
    expect(ledger.canStart('2', 1900)).toBe(false)
  })
  test('stale renewal produces no ticket and retirement clears pending renewals', () => {
    const { ledger, token } = make()
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1'] })
    const results = ledger.invalidate('session-lost')
    expect(results.map((x) => x.type)).toEqual(['renew', 'release'])
    expect(ledger.settle(ticket, { token, sentAt: 900, lockDuration: 2000 })).toEqual([])
    expect(ledger.prepare({ token: Symbol('other'), renewOffsets: ['1'] }).ticket).toBeNull()
    expect(ledger.size).toBe(0)
  })
  test('a stale token cannot prepare a renewal on a fresh ledger and retires it exactly once', () => {
    const { ledger, token } = make()
    const stale = Symbol('stale')
    const { ticket, results } = ledger.prepare({ token: stale, renewOffsets: ['1'] })
    expect(ticket).toBeNull()
    expect(results).toEqual([
      {
        firstOffset: '1',
        lastOffset: '3',
        type: 'release',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
    ])
    expect(results[0].error).toMatchObject({
      reason: 'session-lost',
      topic: 'topic',
      partition: 0,
      retriable: false,
      ranges: [{ firstOffset: '1', lastOffset: '3' }],
    })
    expect(ledger.size).toBe(0)
    expect(ledger.prepare({ token })).toEqual({ ticket: null, results: [] })
    expect(ledger.prepare({ token: stale, renewOffsets: ['1'] })).toEqual({
      ticket: null,
      results: [],
    })
    expect(ledger.invalidate('session-lost')).toEqual([])
    expect(() => ledger.canStart('1', 1099)).toThrow(failure(KafkaJSNonRetriableError, OUTSIDE))
  })
  test('a stale-token settlement fails the renewal and every held entry as session-lost, once', () => {
    const { ledger, token } = make()
    const { ticket } = ledger.prepare({ token, renewOffsets: ['1'] })
    const results = ledger.settle(ticket, {
      token: Symbol('stale'),
      sentAt: 900,
      lockDuration: 2000,
    })
    expect(results).toEqual([
      {
        firstOffset: '1',
        lastOffset: '1',
        type: 'renew',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
      {
        firstOffset: '1',
        lastOffset: '3',
        type: 'release',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
    ])
    expect(results[0].error).toMatchObject({
      reason: 'session-lost',
      ranges: [{ firstOffset: '1', lastOffset: '1' }],
    })
    expect(results[1].error).toMatchObject({
      reason: 'session-lost',
      ranges: [{ firstOffset: '1', lastOffset: '3' }],
    })
    expect(ledger.size).toBe(0)
    expect(ledger.settle(ticket, { token, sentAt: 900, lockDuration: 2000 })).toEqual([])
    expect(ledger.prepare({ token, renewOffsets: ['1'] })).toEqual({ ticket: null, results: [] })
    expect(() => ledger.canStart('1', 1099)).toThrow(failure(KafkaJSNonRetriableError, OUTSIDE))
  })
  test.each([
    [NaN, 1],
    [-1, 1],
    [0, -1],
    [0, 1.5],
    [0, 2147483648],
    [Infinity, 1],
  ])('rejects clock %p and duration %p', (time, duration) =>
    expect(() => lockDeadline(time, duration)).toThrow(
      failure(KafkaJSNonRetriableError, BAD_DEADLINE)
    )
  )
})
