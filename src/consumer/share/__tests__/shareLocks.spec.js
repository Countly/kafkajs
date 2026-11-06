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
    expect(walk.deliver.deliveryCounts).toEqual(new Uint16Array([4, 4, 4]))
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

describe('public share renewal contract', () => {
  const {
    acquire,
    ackAnswer,
    deferred,
    fetchAnswer,
    flush,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  let skew
  const real = performance.now.bind(performance)
  beforeEach(() => {
    skew = 0
    jest.spyOn(performance, 'now').mockImplementation(() => real() + skew)
  })
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.restoreAllMocks()
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} A subscribed client. */
  const start = async (options) => {
    h = shareClient(options)
    h.events = { acks: [], locks: [] }
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => h.events.acks.push(payload))
    h.consumer.on(h.consumer.events.LOCK_EXPIRED, ({ payload }) => h.events.locks.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  const lock = (ms, values) =>
    fetchAnswer([acquire(0, 0, values)], { acquisitionLockTimeoutMs: ms })

  test('starts just before a deadline and drops at the deadline', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    const gate = deferred()
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        await gate.promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    skew = 5000
    gate.resolve()
    await until(() => h.events.locks.some(({ stage }) => stage === 'before-handler'), 'the drop')
    await flush()
    expect(handled).toEqual(['0'])
    expect(h.events.locks.find(({ stage }) => stage === 'before-handler')).toEqual({
      topic: 'topic-a',
      partition: 0,
      offsets: ['1'],
      stage: 'before-handler',
    })
    await until(
      () =>
        h
          .frames(78)
          .some(({ fields }) =>
            fields.topics.some((t) => t.partitions.some((p) => p.acknowledgementBatches.length))
          ),
      'the acknowledgement of the started record'
    )
    const sent = h
      .frames(78)
      .flatMap(({ fields }) =>
        fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
      )
    expect(sent).toEqual([{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }])
  })

  test('still sends a current handler decision after the lock deadline', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    await h.consumer.run({
      eachMessage: async () => {
        skew = 5000
      },
    })
    await until(() => h.events.locks.length === 1, 'the late return event')
    expect(h.events.locks[0]).toEqual({
      topic: 'topic-a',
      partition: 0,
      offsets: ['0'],
      stage: 'after-handler',
    })
    await until(() => h.events.acks.length === 1, 'the settled acknowledgement')
    expect(h.events.acks[0]).toMatchObject({ type: 'accept', error: null })
  })

  test.each([
    [400, 0],
    [600, 1],
  ])(
    'flushes at handler boundaries below half the remaining duration (+%p ms, %p frames)',
    async (jump, frames) => {
      await start()
      h.script('ShareFetch', [lock(1000, ['a', 'b'])])
      const second = deferred()
      let calls = 0
      await h.consumer.run({
        eachMessage: async () => {
          calls++
          if (calls === 1) skew = jump
          else await second.promise
        },
      })
      await until(() => calls === 2, 'the second handler')
      await flush()
      expect(h.frames(79)).toHaveLength(frames)
      if (frames) {
        expect(h.frames(79)[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
          { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
        ])
      }
      second.resolve()
    }
  )

  test('renews only the selected subset through the session queue', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const durations = []
    await h.consumer.run({
      eachBatch: async ({ batch, renew }) => {
        durations.push(await renew([batch.messages[0], { offset: '2' }]))
      },
    })
    await until(() => durations.length === 1, 'the renewal')
    expect(durations).toEqual([2000])
    const [renewal] = h.frames(79)
    expect(renewal.fields.isRenewAck).toBe(true)
    expect(renewal.fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [4] },
      { firstOffset: '2', lastOffset: '2', acknowledgeTypes: [4] },
    ])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(2000)
    const renewed = h.events.acks.filter(({ type }) => type === 'renew')
    expect(renewed.map(({ firstOffset, lastOffset }) => [firstOffset, lastOffset])).toEqual([
      ['0', '0'],
      ['2', '2'],
    ])
    expect(renewed.every(({ error }) => error === null)).toBe(true)
  })

  test('leaves every deadline unchanged after a renewal refusal', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    h.script('ShareAcknowledge', [ackAnswer([{ partition: 0, code: 121 }])])
    const failures = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        await renew().catch((error) => failures.push(error))
      },
    })
    await until(() => failures.length === 1, 'the refused renewal')
    expect(failures[0]).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'rejected-by-broker',
      ranges: [{ firstOffset: '0', lastOffset: '0' }],
    })
    const renewed = h.events.acks.filter(({ type }) => type === 'renew')
    expect(renewed).toHaveLength(1)
    expect(renewed[0].error).toBe(failures[0])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(1000)
  })

  test('keeps successful renewals nonterminal', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 3000 }),
    ])
    const seen = []
    await h.consumer.run({
      eachMessage: async ({ message, renew }) => {
        seen.push(await renew(), await renew(), message.deliveryCount)
      },
    })
    await until(() => seen.length === 3, 'two renewals')
    expect(seen).toEqual([2000, 3000, 1])
    await until(() => h.events.acks.some(({ type }) => type === 'accept'), 'the terminal accept')
    expect(h.events.acks.filter(({ type }) => type === 'renew')).toHaveLength(2)
    expect(h.events.acks.filter(({ type }) => type === 'accept')).toHaveLength(1)
  })

  test('reports the last renewed duration when a later renewal keeps it', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 0 }),
    ])
    const seen = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        seen.push(await renew(), await renew())
      },
    })
    await until(() => seen.length === 2, 'two renewals')
    expect(seen).toEqual([2000, 2000])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(2000)
  })

  test('rejects invalid batch renewal selectors without traffic', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    const outcomes = []
    await h.consumer.run({
      eachBatch: async ({ batch, renew }) => {
        const [first] = batch.messages
        for (const selector of [[], null, 'a', [first, first], [{ ...first }], [{ offset: '9' }]]) {
          try {
            await renew(selector)
            outcomes.push('accepted')
          } catch (error) {
            outcomes.push(error.name)
          }
        }
      },
    })
    await until(() => outcomes.length === 6, 'six selector checks')
    expect(outcomes).toEqual(Array(6).fill('KafkaJSNonRetriableError'))
    expect(h.frames(79).filter(({ fields }) => fields.isRenewAck)).toHaveLength(0)
  })
})
