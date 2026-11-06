const RequestQueue = require('../../../network/requestQueue')
const { requests } = require('../../../protocol/requests')
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

describe('public share lock contract', () => {
  const {
    A,
    acquire,
    acquired,
    ackAnswer,
    deferred,
    fetchAnswer,
    flush,
    part,
    recordsHex,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  let now
  beforeEach(() => {
    now = 100
    jest.spyOn(performance, 'now').mockImplementation(() => now)
  })
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.restoreAllMocks()
  })
  const TWO_PARTITIONS = {
    'topic-a': {
      topicId: A,
      partitions: [
        { leader: 0, epoch: 1 },
        { leader: 0, epoch: 1 },
      ],
    },
  }
  /**
   * Starts a client whose clock is the test's `now` and records the events the specs read.
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} The subscribed client with `events.acks`, `events.locks` and
   *   `events.assignments`.
   */
  const start = async (options) => {
    h = shareClient({ interval: 5, ...options })
    h.events = { acks: [], locks: [], assignments: [] }
    const { consumer } = h
    consumer.on(consumer.events.ACKNOWLEDGE, ({ payload }) => h.events.acks.push(payload))
    consumer.on(consumer.events.LOCK_EXPIRED, ({ payload }) => h.events.locks.push(payload))
    consumer.on(consumer.events.ASSIGNMENT_CHANGED, ({ payload }) =>
      h.events.assignments.push(payload)
    )
    await consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /**
   * @param {number} ms Lock duration the answer reports.
   * @param {string[]} values One record value per offset, from offset 0 of partition 0.
   * @returns {object} A ShareFetch answer acquiring them.
   */
  const lock = (ms, values) =>
    fetchAnswer([acquire(0, 0, values)], { acquisitionLockTimeoutMs: ms })
  /** @param {object} frame A recorded share frame. @returns {object[]} Its acknowledgement batches. */
  const batchesOf = ({ fields }) =>
    fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
  /** @param {object} frame A recorded share frame. @returns {object[]} Its partitions with their batches. */
  const rowsOf = ({ fields }) =>
    fields.topics.flatMap((t) =>
      t.partitions.map((p) => ({ partition: p.partitionIndex, batches: p.acknowledgementBatches }))
    )
  /** @param {object[]} acks Acknowledge payloads. @returns {Array<Array>} Range, type and error reason of each. */
  const outcomes = (acks) =>
    acks.map(({ firstOffset, lastOffset, type, error }) => [
      firstOffset,
      lastOffset,
      type,
      error ? error.reason : null,
    ])
  const batch = (first, last, types) => ({
    firstOffset: String(first),
    lastOffset: String(last),
    acknowledgeTypes: types,
  })
  /**
   * Answers the next steady heartbeat with `assignment` at member epoch 2.
   * @param {object|null} assignment The answer's assignment.
   * @returns {Promise<void>}
   */
  const answerNextBeat = async (assignment) => {
    const beat = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    const call = await beat.arrived()
    beat.release({
      errorCode: 0,
      errorMessage: null,
      memberId: call.fields.memberId,
      memberEpoch: 2,
      heartbeatIntervalMs: 5,
      assignment,
    })
  }

  /**
   * Holds the first renewal request of the client at one stage of its write path until the test
   * lets it go, so a test can retire the run while the renewal is captured but not yet written.
   */
  class RenewHold {
    /**
     * @param {'encode'|'queue'} stage Where the renewal waits: inside its encoder, or just before
     *   the request queue it would be queued in.
     */
    constructor(stage) {
      this.gate = deferred()
      this.arrival = deferred()
      const { gate, arrival } = this
      let taken = false
      if (stage === 'encode') {
        const original = requests.ShareAcknowledge.protocol
        jest.spyOn(requests.ShareAcknowledge, 'protocol').mockImplementation((selection) => {
          const factory = original(selection)
          return (options) => {
            const built = factory(options)
            if (taken || options?.isRenewAck !== true) return built
            taken = true
            const { encode } = built.request
            return {
              ...built,
              request: {
                ...built.request,
                encode: async () => {
                  arrival.resolve()
                  await gate.promise
                  return encode()
                },
              },
            }
          }
        })
      } else {
        const original = RequestQueue.prototype.push
        jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
          if (taken || pushed.entry.apiKey !== 79) return original.call(this, pushed)
          taken = true
          arrival.resolve()
          gate.promise.then(() => {
            try {
              original.call(this, pushed)
            } catch (error) {
              pushed.entry.reject(error)
            }
          })
        })
      }
    }

    /** @returns {Promise<void>} Resolves once the renewal reached the held stage. */
    arrived() {
      return this.arrival.promise
    }

    /** @returns {void} Lets the renewal continue from its stage. */
    release() {
      this.gate.resolve()
    }
  }

  test('starts just before a deadline and drops at the deadline', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    const gates = [deferred(), deferred(), deferred()]
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        await gates[Number(message.offset)].promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    now = 1099
    gates[0].resolve()
    await until(
      () => handled.length === 2 && h.frames(79).length === 1,
      'the start at 1099 and its flush'
    )
    now = 1100
    gates[1].resolve()
    await until(
      () => h.events.locks.length === 2 && h.frames(79).length === 2,
      'the drop at 1100 and the flush of the second record'
    )
    await until(() => h.frames(78).length === 2, 'the next fetch')
    await flush()
    expect(handled).toEqual(['0', '1'])
    expect(h.events.locks).toEqual([
      { topic: 'topic-a', partition: 0, offsets: ['1'], stage: 'after-handler' },
      { topic: 'topic-a', partition: 0, offsets: ['2'], stage: 'before-handler' },
    ])
    expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 0, [1])], [batch(1, 1, [1])]])
    expect(outcomes(h.events.acks)).toEqual([
      ['0', '0', 'accept', null],
      ['1', '1', 'accept', null],
    ])
    expect(h.frames(78)[1].fields.topics).toEqual([])
  })

  test.each([
    ['the broker accepts it', 0, null],
    ['the broker answers 121', 121, 'rejected-by-broker'],
  ])(
    'still sends a current handler decision after the lock deadline when %s',
    async (_, code, reason) => {
      await start()
      h.script('ShareFetch', [lock(1000, ['a'])])
      if (code) h.script('ShareAcknowledge', [ackAnswer([{ partition: 0, code }])])
      const fetch = h.hold({ api: 'ShareFetch' })
      const returned = deferred()
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ message }) => {
          handled.push(message.offset)
          await returned.promise
        },
      })
      await fetch.arrived()
      now = 1099
      fetch.release()
      await until(() => handled.length === 1, 'the handler start at 1099')
      now = 1200
      returned.resolve()
      await until(() => h.events.acks.length === 1, 'the settled decision')
      expect(h.events.locks).toEqual([
        { topic: 'topic-a', partition: 0, offsets: ['0'], stage: 'after-handler' },
      ])
      expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 0, [1])]])
      expect(outcomes(h.events.acks)).toEqual([['0', '0', 'accept', reason]])
    }
  )

  test('releases the unstarted messages of a revoked partition and forgets it on the next fetch', async () => {
    await start({ topics: TWO_PARTITIONS })
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c']), part({ partition: 1 })])])
    const held = deferred()
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        await held.promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    await answerNextBeat({ topicPartitions: [{ topicId: A, partitions: [1] }] })
    await until(() => h.events.assignments.length === 2, 'the revocation')
    expect(h.events.assignments[1].removed).toEqual([{ topic: 'topic-a', partitions: [0] }])
    held.resolve()
    await until(() => h.frames(78).length === 2, 'the fetch that forgets the partition')
    const second = h.frames(78)[1].fields
    expect(rowsOf(h.frames(78)[1])).toEqual([{ partition: 0, batches: [batch(0, 2, [1, 2, 2])] }])
    expect(second.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(handled).toEqual(['0'])
    expect(h.frames(79)).toHaveLength(0)
    await until(() => h.events.acks.length === 2, 'the settled acknowledgements')
    expect(outcomes(h.events.acks)).toEqual([
      ['0', '0', 'accept', null],
      ['1', '2', 'release', null],
    ])
  })

  test('releases the unstarted messages of a stopped run in its final acknowledgement', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    const held = deferred()
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        await held.promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    const stopping = h.consumer.stop()
    held.resolve()
    await stopping
    expect(handled).toEqual(['0'])
    const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
    expect(finals).toHaveLength(1)
    expect(batchesOf(finals[0])).toEqual([batch(0, 2, [1, 2, 2])])
    expect(h.frames(79)).toHaveLength(1)
  })

  test.each([
    [400, 0],
    [500, 0],
    [501, 1],
    [600, 1],
  ])(
    'flushes at a handler return only below half the remaining lock (return at %p ms, %p flushes)',
    async (at, flushes) => {
      now = 0
      await start()
      h.script('ShareFetch', [lock(1000, ['a', 'b'])])
      const gates = [deferred(), deferred()]
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ message }) => {
          handled.push(message.offset)
          await gates[Number(message.offset)].promise
        },
      })
      await until(() => handled.length === 1, 'the first handler')
      now = at
      gates[0].resolve()
      await until(() => handled.length === 2, 'the second handler')
      await flush()
      expect(h.frames(79).map(batchesOf)).toEqual(flushes ? [[batch(0, 0, [1])]] : [])
      gates[1].resolve()
      await flush()
    }
  )

  test('flushes every decided entry of the acquisition together once the lock is half spent', async () => {
    now = 0
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c', 'd'])])
    const gates = [deferred(), deferred(), deferred(), deferred()]
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        await gates[Number(message.offset)].promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    now = 400
    gates[0].resolve()
    await until(() => handled.length === 2, 'the second handler')
    now = 500
    gates[1].resolve()
    await until(() => handled.length === 3, 'the third handler')
    await flush()
    expect(h.frames(79)).toHaveLength(0)
    now = 600
    gates[2].resolve()
    await until(() => handled.length === 4 && h.frames(79).length === 1, 'the flush at 600')
    expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 2, [1])]])
    gates[3].resolve()
    await until(() => h.frames(79).length === 2, 'the flush of the fourth record')
    expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 2, [1])], [batch(3, 3, [1])]])
  })

  test('flushes the automatic ranges of the next acquisition when its handler starts', async () => {
    now = 0
    await start({ topics: TWO_PARTITIONS })
    h.script('ShareFetch', [
      fetchAnswer(
        [
          acquire(0, 0, ['a']),
          part({ partition: 1, acquire: [acquired(0, 1)], records: recordsHex(0, ['b']) }),
        ],
        { acquisitionLockTimeoutMs: 1000 }
      ),
    ])
    const gates = [deferred(), deferred()]
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message, partition }) => {
        handled.push(`${partition}:${message.offset}`)
        await gates[partition].promise
      },
    })
    await until(() => handled.length === 1, 'the first partition handler')
    now = 600
    gates[0].resolve()
    await until(
      () => handled.length === 2 && h.frames(79).length === 2,
      'the second partition start and both flushes'
    )
    expect(handled).toEqual(['0:0', '1:0'])
    const byPartition = new Map(
      h
        .frames(79)
        .flatMap(rowsOf)
        .map(({ partition, batches }) => [partition, batches])
    )
    expect(byPartition.get(0)).toEqual([batch(0, 0, [1])])
    expect(byPartition.get(1)).toEqual([batch(1, 1, [0])])
    gates[1].resolve()
    await flush()
  })

  test('does not wait for a mid-acquisition flush in the dispatch loop', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const first = deferred()
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        if (message.offset === '0') await first.promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    now = 1000
    first.resolve()
    await gate.arrived()
    await until(() => handled.length === 3, 'the remaining handlers behind the held flush')
    await flush()
    expect(h.frames(79)).toHaveLength(1)
    expect(h.frames(78)).toHaveLength(1)
    gate.release()
    await until(() => h.frames(78).length === 2 && h.frames(79).length === 2, 'the queue to drain')
    const [firstFlush, secondFlush] = h.frames(79)
    const nextFetch = h.frames(78)[1]
    expect(batchesOf(firstFlush)).toEqual([batch(0, 0, [1])])
    expect(batchesOf(secondFlush)).toEqual([batch(1, 2, [1])])
    expect(secondFlush.order).toBeGreaterThan(firstFlush.answeredOrder)
    expect(nextFetch.order).toBeGreaterThan(secondFlush.answeredOrder)
    expect(nextFetch.fields.topics).toEqual([])
  })

  test('has no boundary flush inside a long eachBatch call', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    const inside = deferred()
    const done = deferred()
    await h.consumer.run({
      eachBatch: async () => {
        now = 1050
        inside.resolve()
        await done.promise
      },
    })
    await inside.promise
    await flush()
    expect(h.frames(79)).toHaveLength(0)
    done.resolve()
    await until(() => h.frames(79).length === 1, 'the flush at the return')
    expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 1, [1])]])
  })

  test('selects the flush by the earliest decided deadline when a sibling stays near expiry', async () => {
    now = 0
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const renewed = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        now = 400
        renewed.push(await renew([messages[0]]))
        now = 600
      },
    })
    await until(() => h.frames(79).length === 2, 'the renewal and the flush')
    expect(renewed).toEqual([2000])
    const [renewal, flushed] = h.frames(79)
    expect(renewal.fields.isRenewAck).toBe(true)
    expect(batchesOf(renewal)).toEqual([batch(0, 0, [4])])
    expect(flushed.fields.isRenewAck).toBe(false)
    expect(batchesOf(flushed)).toEqual([batch(0, 1, [1])])
    expect(h.events.locks).toEqual([])
  })

  test.each([
    [1400, 0],
    [1401, 1],
  ])(
    'flushes by the renewed deadline of the only decided entry (return at %p ms, %p flushes)',
    async (at, flushes) => {
      now = 0
      await start()
      h.script('ShareFetch', [lock(1000, ['a', 'b'])])
      h.script('ShareAcknowledge', [
        ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ])
      await h.consumer.run({
        eachMessage: async ({ message, renew }) => {
          if (message.offset !== '0') return
          now = 400
          await renew()
          now = at
        },
      })
      await until(
        () => h.events.locks.some(({ stage }) => stage === 'before-handler'),
        'the drop of the unrenewed sibling'
      )
      await until(() => h.frames(78).length === 2, 'the next fetch')
      const flushesSent = h.frames(79).filter(({ fields }) => !fields.isRenewAck)
      expect(flushesSent.map(batchesOf)).toEqual(flushes ? [[batch(0, 0, [1])]] : [])
      expect(batchesOf(h.frames(78)[1])).toEqual(flushes ? [] : [batch(0, 0, [1])])
      expect(h.events.locks).toEqual([
        { topic: 'topic-a', partition: 0, offsets: ['1'], stage: 'before-handler' },
      ])
    }
  )

  test.each([
    [2099, ['1']],
    [2100, ['0', '1', '2']],
  ])(
    'renews only the selected subset from its pre-write time (handler returns at %p ms)',
    async (returnAt, expired) => {
      await start()
      h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
      h.script('ShareAcknowledge', [
        ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const seen = { durations: [], counts: [] }
      await h.consumer.run({
        eachBatch: async ({ batch: { messages }, renew }) => {
          seen.durations.push(await renew([messages[0], messages[2]]))
          seen.counts.push(messages.map((message) => message.deliveryCount))
          now = returnAt
        },
      })
      await gate.arrived()
      expect(h.frames(78)).toHaveLength(1)
      now = 500
      gate.release()
      await until(() => h.events.locks.length === 1, 'the late return event')
      expect(seen).toEqual({ durations: [2000], counts: [[1, 1, 1]] })
      expect(h.events.locks).toEqual([
        { topic: 'topic-a', partition: 0, offsets: expired, stage: 'after-handler' },
      ])
      const [renewal] = h.frames(79)
      expect(renewal.fields.isRenewAck).toBe(true)
      expect(batchesOf(renewal)).toEqual([batch(0, 0, [4]), batch(2, 2, [4])])
      expect(h.consumer.acquisitionLockTimeoutMs()).toBe(2000)
      await until(() => h.events.acks.length === 3, 'the renewals and the terminal result')
      expect(outcomes(h.events.acks)).toEqual([
        ['0', '0', 'renew', null],
        ['2', '2', 'renew', null],
        ['0', '2', 'accept', null],
      ])
    }
  )

  test('serializes overlapping renewals and gives each its own write time', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 1000 }),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const results = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        const [m0, m1, m2] = messages
        results.push(...(await Promise.all([renew([m0, m1]), renew([m1, m2])])))
        now = 2399
      },
    })
    await gate.arrived()
    await flush()
    expect(h.frames(79)).toHaveLength(1)
    expect(batchesOf(h.frames(79)[0])).toEqual([batch(0, 1, [4])])
    now = 400
    gate.release()
    await until(() => h.events.locks.length === 1, 'the late return event')
    expect(results).toEqual([1000, 2000])
    expect(h.events.locks).toEqual([
      { topic: 'topic-a', partition: 0, offsets: ['0'], stage: 'after-handler' },
    ])
    await until(() => h.frames(79).length === 3, 'the terminal flush')
    expect(h.frames(79).map(batchesOf)).toEqual([
      [batch(0, 1, [4])],
      [batch(1, 2, [4])],
      [batch(0, 2, [1])],
    ])
    const [one, two, three] = h.frames(79)
    expect(two.order).toBeGreaterThan(one.answeredOrder)
    expect(h.frames(78).filter(({ order }) => order > one.order && order < three.order)).toEqual([])
    await until(() => h.events.acks.length === 3, 'the terminal result')
    await flush()
    expect(outcomes(h.events.acks)).toEqual([
      ['0', '1', 'renew', null],
      ['1', '2', 'renew', null],
      ['0', '2', 'accept', null],
    ])
    expect(
      h.events.acks
        .filter(({ type }) => type !== 'renew')
        .map(({ firstOffset, lastOffset }) => [firstOffset, lastOffset])
    ).toEqual([['0', '2']])
  })

  test('leaves every deadline unchanged after a renewal refusal', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    h.script('ShareAcknowledge', [ackAnswer([{ partition: 0, code: 121 }])])
    const failures = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        await renew([messages[0], messages[1]]).catch((error) => failures.push(error))
        now = 1100
      },
    })
    await until(() => h.events.locks.length === 1, 'the late return event')
    expect(failures).toHaveLength(1)
    expect(failures[0]).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'rejected-by-broker',
      ranges: [{ firstOffset: '0', lastOffset: '1' }],
    })
    expect(h.events.locks).toEqual([
      { topic: 'topic-a', partition: 0, offsets: ['0', '1', '2'], stage: 'after-handler' },
    ])
    const renewed = h.events.acks.filter(({ type }) => type === 'renew')
    expect(renewed).toHaveLength(1)
    expect(renewed[0].error).toBe(failures[0])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(1000)
  })

  test('updates only the offsets of a later successful renewal after a refusal', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b', 'c'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0, code: 121 }]),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const settled = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        const [m0, m1, m2] = messages
        settled.push(...(await Promise.allSettled([renew([m0, m1]), renew([m1, m2])])))
        now = 2399
      },
    })
    await gate.arrived()
    now = 400
    gate.release()
    await until(() => h.events.locks.length === 1, 'the late return event')
    expect(settled.map(({ status }) => status)).toEqual(['rejected', 'fulfilled'])
    expect(settled[0].reason).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'rejected-by-broker',
      ranges: [{ firstOffset: '0', lastOffset: '1' }],
    })
    expect(settled[1].value).toBe(2000)
    expect(h.events.locks).toEqual([
      { topic: 'topic-a', partition: 0, offsets: ['0'], stage: 'after-handler' },
    ])
    expect(outcomes(h.events.acks).slice(0, 2)).toEqual([
      ['0', '1', 'renew', 'rejected-by-broker'],
      ['1', '2', 'renew', null],
    ])
  })

  test('refuses a renewal queued behind a held request once its run is retired, and a new run renews', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const queued = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        queued.push(renew([messages[0]]), renew([messages[1]]))
      },
    })
    await gate.arrived()
    await flush()
    expect(h.frames(79)).toHaveLength(1)
    await h.consumer.disconnect({ timeout: 0 })
    await expect(queued[0]).rejects.toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'membership-lost',
      ranges: [{ firstOffset: '0', lastOffset: '0' }],
    })
    await expect(queued[1]).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message: 'Share run closed',
    })
    expect(h.frames(79)).toHaveLength(1)
    gate.release(ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 9999 }))
    await flush()
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(1000)

    await h.consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [lock(1000, ['c'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const durations = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        durations.push(await renew())
      },
    })
    await until(() => durations.length === 1, 'the renewal of the new run')
    expect(durations).toEqual([2000])
    const renewals = h.frames(79).filter(({ fields }) => fields.isRenewAck)
    expect(renewals).toHaveLength(2)
    expect(batchesOf(renewals[1])).toEqual([batch(0, 0, [4])])
  })

  test.each(['encode', 'queue'])(
    'refuses a renewal retired at its %s stage without writing it, and a new run renews',
    async (stage) => {
      await start()
      h.script('ShareFetch', [lock(1000, ['a'])])
      const hold = new RenewHold(stage)
      try {
        const renewals = []
        await h.consumer.run({
          eachMessage: async ({ renew }) => {
            renewals.push(
              renew().then(
                () => null,
                (error) => error
              )
            )
          },
        })
        await hold.arrived()
        expect(h.frames(79)).toEqual([])
        const closing = h.consumer.disconnect({ timeout: 0 })
        await flush()
        hold.release()
        await closing
        expect(await renewals[0]).toMatchObject({
          name: 'KafkaJSShareAcknowledgementError',
          reason: 'membership-lost',
          ranges: [{ firstOffset: '0', lastOffset: '0' }],
        })
        await flush()
        expect(h.frames(79)).toEqual([])
        expect(h.consumer.acquisitionLockTimeoutMs()).toBe(1000)

        await h.consumer.subscribe({ topics: ['topic-a'] })
        h.script('ShareFetch', [lock(1000, ['c'])])
        h.script('ShareAcknowledge', [
          ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
        ])
        const durations = []
        await h.consumer.run({
          eachMessage: async ({ renew }) => {
            durations.push(await renew())
          },
        })
        await until(() => durations.length === 1, 'the renewal of the new run')
        expect(durations).toEqual([2000])
        const written = h.frames(79).filter(({ fields }) => fields.isRenewAck)
        expect(written).toHaveLength(1)
        expect(batchesOf(written[0])).toEqual([batch(0, 0, [4])])
      } finally {
        hold.release()
      }
    }
  )

  test('keeps successful renewals nonterminal', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 3000 }),
    ])
    const seen = []
    let saved
    await h.consumer.run({
      eachMessage: async ({ message, renew, acknowledge }) => {
        saved = renew
        seen.push(await renew(), await renew(), message.deliveryCount)
        acknowledge('accept')
      },
    })
    await until(() => h.events.acks.length === 3, 'two renewals and the terminal accept')
    expect(seen).toEqual([2000, 3000, 1])
    expect(outcomes(h.events.acks)).toEqual([
      ['0', '0', 'renew', null],
      ['0', '0', 'renew', null],
      ['0', '0', 'accept', null],
    ])
    expect(h.frames(79).map(batchesOf)).toEqual([[batch(0, 0, [4])], [batch(0, 0, [4])]])
    expect(h.frames(79).map(({ fields }) => fields.isRenewAck)).toEqual([true, true])
    expect(batchesOf(h.frames(78)[1])).toEqual([batch(0, 0, [1])])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(3000)
    expect(() => saved()).toThrow('Handler call is no longer current')
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
    const outcomesSeen = []
    await h.consumer.run({
      eachBatch: async ({ batch: { messages }, renew }) => {
        const [first] = messages
        for (const selector of [[], null, 'a', [first, first], [{ ...first }], [{ offset: '9' }]]) {
          try {
            await renew(selector)
            outcomesSeen.push('accepted')
          } catch (error) {
            outcomesSeen.push(`${error.name}: ${error.message}`)
          }
        }
      },
    })
    await until(() => outcomesSeen.length === 6, 'six selector checks')
    expect(outcomesSeen).toEqual([
      'KafkaJSNonRetriableError: renew requires a non-empty array of local messages',
      'KafkaJSNonRetriableError: renew requires a non-empty array of local messages',
      'KafkaJSNonRetriableError: renew requires a non-empty array of local messages',
      'KafkaJSNonRetriableError: renew messages must be distinct',
      'KafkaJSNonRetriableError: Message does not belong to this handler call',
      'KafkaJSNonRetriableError: Message does not belong to this handler call',
    ])
    expect(h.frames(79).filter(({ fields }) => fields.isRenewAck)).toHaveLength(0)
  })

  test.each([
    ['eachBatch', [batch(0, 1, [4])]],
    ['eachMessage', [batch(0, 0, [4])]],
  ])('renews the whole call when %s omits the selector', async (handlerName, expectedBatches) => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a', 'b'])])
    const renewed = []
    await h.consumer.run({
      [handlerName]: async ({ renew }) => {
        if (renewed.length === 0) renewed.push(await renew())
      },
    })
    await until(() => renewed.length === 1, 'the renewal')
    expect(renewed).toEqual([1000])
    const [renewal] = h.frames(79)
    expect(renewal.fields.isRenewAck).toBe(true)
    expect(batchesOf(renewal)).toEqual(expectedBatches)
  })

  test('reports only the broker current successful duration after the run changed', async () => {
    await start()
    h.script('ShareFetch', [lock(1000, ['a'])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const old = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        old.push(renew())
      },
    })
    await gate.arrived()
    await h.consumer.disconnect({ timeout: 0 })
    await h.consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [lock(1000, ['b'])])
    const returned = deferred()
    await h.consumer.run({
      eachMessage: async () => {
        await returned.promise
      },
    })
    await until(() => h.consumer.acquisitionLockTimeoutMs() === 1000, 'the new run duration')
    gate.release(ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 9999 }))
    await flush()
    await expect(old[0]).rejects.toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'membership-lost',
    })
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(1000)
    now = 1099
    returned.resolve()
    await until(
      () => h.events.acks.some(({ type, error }) => type === 'accept' && error === null),
      'the new accept'
    )
    expect(h.events.locks).toEqual([])
  })
})
