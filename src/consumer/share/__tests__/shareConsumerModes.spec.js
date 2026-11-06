const { ShareHandlerCall } = require('../shareRunner')
const { countWithin } = require('../shareHandlerCall')
const { ShareAckLedger } = require('../shareAckLedger')
const { KafkaJSNonRetriableError } = require('../../../errors')

/**
 * @param {object} [options] Case overrides.
 * @param {string[]} [options.offsets] Offsets of the delivered records, ascending.
 * @returns {object} The actual ledger and one original invocation over all of its records.
 */
const setup = ({ mode = 'implicit', batch = true, stopping = false, offsets } = {}) => {
  const token = Symbol('acquisition')
  let current = true
  const records = (offsets ?? (batch ? ['0', '1'] : ['0'])).map((offset) => ({
    offset,
    value: Buffer.from(offset),
  }))
  const ledger = new ShareAckLedger({
    topic: 't',
    topicId: 'id',
    partition: 0,
    token,
    sentAt: 0,
    lockDuration: 1000,
    walk: {
      deliver: { records, deliveryCounts: new Uint16Array(records.length).fill(1) },
      gaps: [],
      reject: [],
      release: [],
    },
  })
  const renew = jest.fn(async () => 1000)
  const authority = {
    current: () => current,
    running: () => !stopping,
    stopping: () => stopping,
    renew,
  }
  const call = new ShareHandlerCall({ ledger, records, mode, batch, authority })
  return {
    token,
    messages: records,
    ledger,
    call,
    renew,
    authority,
    retire: () => {
      current = false
    },
  }
}
/** @param {object} context Original case. @returns {number[]} Terminal wire types. */
const types = ({ ledger, token }) =>
  ledger.prepare({ token }).ticket.acknowledgementBatches.flatMap((b) => b.acknowledgeTypes)

describe('Share consumer handler decisions', () => {
  test('implicit normal return accepts unacknowledged messages', () => {
    const c = setup()
    c.call.complete()
    expect(types(c)).toEqual([1])
  })
  test.each(['implicit', 'explicit'])(
    'throw releases only the undecided messages in %s mode',
    (mode) => {
      const c = setup({ mode })
      c.call.acknowledge(c.messages[0], 'reject')
      expect(c.call.complete(true)).toBeNull()
      expect(types(c)).toEqual([3, 2])
    }
  )
  test('explicit omission releases before returning the typed fatal verdict', () => {
    const c = setup({ mode: 'explicit' })
    c.call.acknowledge(c.messages[0])
    const error = c.call.complete()
    expect(error.reason).toBe('unacknowledged')
    expect(error.ranges).toEqual([{ firstOffset: '1', lastOffset: '1' }])
    expect(types(c)).toEqual([1, 2])
  })
  test('explicit omission coalesces adjacent omitted offsets into compact ranges', () => {
    const c = setup({ mode: 'explicit', offsets: ['0', '1', '2', '4'] })
    c.call.acknowledge(c.messages[1])
    expect(c.call.complete().ranges).toEqual([
      { firstOffset: '0', lastOffset: '0' },
      { firstOffset: '2', lastOffset: '2' },
      { firstOffset: '4', lastOffset: '4' },
    ])
    const d = setup({ mode: 'explicit', offsets: ['0', '1', '2', '4'] })
    d.call.acknowledge(d.messages[3])
    expect(d.call.complete().ranges).toEqual([{ firstOffset: '0', lastOffset: '2' }])
  })
  test('the last local decision wins and is ineligible before return', () => {
    const c = setup()
    c.call.acknowledge(c.messages[0], 'accept')
    c.call.acknowledge(c.messages[0], 'reject')
    expect(c.ledger.prepare({ token: c.token }).ticket).toBeNull()
    c.call.complete()
    expect(types(c)).toEqual([3, 1])
  })
  test('acknowledges a batch out of order and accepts offset-only selectors', () => {
    const c = setup({ mode: 'explicit' })
    c.call.acknowledge({ offset: '1' }, 'release')
    c.call.acknowledge(c.messages[0])
    expect(c.call.complete()).toBeNull()
    expect(types(c)).toEqual([1, 2])
  })
  test('late and foreign messages throw before ledger mutation', () => {
    const c = setup()
    expect(() => c.call.acknowledge({ ...c.messages[0] })).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.acknowledge({ offset: '2' })).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.acknowledge({ offset: 1 })).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.acknowledge(null)).toThrow(KafkaJSNonRetriableError)
    expect(c.ledger.prepare({ token: c.token }).ticket).toBeNull()
    c.call.complete()
    expect(() => c.call.acknowledge(c.messages[0])).toThrow(KafkaJSNonRetriableError)
  })
  test.each(['implicit', 'explicit'])(
    'stopped batch releases omission without crash in %s mode',
    (mode) => {
      const c = setup({ mode, stopping: true })
      c.call.acknowledge(c.messages[0], 'reject')
      expect(c.call.complete()).toBeNull()
      expect(types(c)).toEqual([3, 2])
    }
  )
  test('a current eachMessage normal return still accepts after stop', () => {
    const c = setup({ batch: false, stopping: true })
    expect(c.call.complete()).toBeNull()
    expect(types(c)).toEqual([1])
  })
  test('a fenced old return cannot decide or settle again', () => {
    const c = setup()
    c.retire()
    c.ledger.invalidate('membership-lost')
    expect(c.call.complete()).toBeNull()
    expect(c.ledger.size).toBe(0)
    expect(() => c.call.acknowledge(c.messages[0])).toThrow(KafkaJSNonRetriableError)
  })
  test('renew selects held offsets without making a terminal decision', async () => {
    const c = setup()
    await expect(c.call.renew([c.messages[1]])).resolves.toBe(1000)
    expect(c.renew).toHaveBeenCalledWith(['1'])
    expect(c.ledger.prepare({ token: c.token }).ticket).toBeNull()
    c.call.complete()
    expect(types(c)).toEqual([1])
  })
  test('renew without a selector renews every message of the call', async () => {
    const c = setup()
    await c.call.renew()
    expect(c.renew).toHaveBeenCalledWith(['0', '1'])
  })
  test('refuses an invalidated invocation before its ledger is retired', () => {
    const c = setup()
    c.retire()
    expect(() => c.call.acknowledge(c.messages[0])).toThrow('no longer current')
    expect(c.ledger.prepare({ token: c.token }).ticket).toBeNull()
  })
  test('rejects empty, duplicate and foreign renewal selections without queue work', () => {
    const c = setup()
    expect(() => c.call.renew([])).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.renew(null)).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.renew([c.messages[0], c.messages[0]])).toThrow(KafkaJSNonRetriableError)
    expect(() => c.call.renew([{ ...c.messages[0] }])).toThrow(KafkaJSNonRetriableError)
    expect(c.renew).not.toHaveBeenCalled()
  })
  test('finds messages by numeric offset order, not string order', () => {
    const c = setup({ mode: 'explicit', offsets: ['9', '10', '100', '9007199254740993'] })
    for (const message of [...c.messages].reverse()) c.call.acknowledge(message)
    expect(c.call.complete()).toBeNull()
    expect(
      c.ledger
        .prepare({ token: c.token })
        .ticket.acknowledgementBatches.map(({ firstOffset, lastOffset }) => [
          firstOffset,
          lastOffset,
        ])
    ).toEqual([
      ['9', '10'],
      ['100', '100'],
      ['9007199254740993', '9007199254740993'],
    ])
  })
  test('a call over a view of the acquisition cannot decide records outside it', () => {
    const c = setup({ offsets: ['0', '1', '2'], batch: false })
    const call = new ShareHandlerCall({
      ledger: c.ledger,
      records: c.messages,
      start: 1,
      end: 2,
      mode: 'implicit',
      batch: false,
      authority: c.authority,
    })
    expect(() => call.acknowledge(c.messages[0])).toThrow(KafkaJSNonRetriableError)
    expect(() => call.acknowledge({ offset: '2' })).toThrow(KafkaJSNonRetriableError)
    call.acknowledge(c.messages[1], 'reject')
    expect(call.complete()).toBeNull()
    expect(types(c)).toEqual([3])
  })
  test('two invocations never share callbacks: an old one stays dead beside a new one', () => {
    const c = setup({ batch: false })
    const first = c.call.messagePayload('t', 0)
    c.call.complete()
    const second = new ShareHandlerCall({
      ledger: c.ledger,
      records: c.messages,
      mode: 'implicit',
      batch: false,
      authority: c.authority,
    }).messagePayload('t', 0)
    expect(first.acknowledge).not.toBe(second.acknowledge)
    expect(() => first.acknowledge('reject')).toThrow(KafkaJSNonRetriableError)
    expect(c.ledger.prepare({ token: c.token }).ticket).not.toBeNull()
  })
  test('completes and observes a call without building an offset array', () => {
    for (const mode of ['implicit', 'explicit']) {
      const c = setup({ mode, offsets: ['0', '1', '2'] })
      const offsets = jest.spyOn(c.call, 'offsets')
      const byOffsets = jest.spyOn(c.ledger, 'complete')
      const byRange = jest.spyOn(c.ledger, 'completeRange')
      expect(c.call.expiredAt(500)).toBeNull()
      expect(c.call.expiredAt(1000)).toEqual(['0', '1', '2'])
      c.call.complete()
      expect(byRange).toHaveBeenCalledTimes(1)
      expect(byRange).toHaveBeenCalledWith(
        c.messages,
        0,
        3,
        mode === 'explicit' ? 'release' : 'accept'
      )
      expect(offsets).not.toHaveBeenCalled()
      expect(byOffsets).not.toHaveBeenCalled()
      expect(types(c)).toEqual([mode === 'explicit' ? 2 : 1])
    }
  })
  test('counts the delivered records inside a compact range by exact int64 offset', () => {
    const records = ['0', '1', '2', '9', '10', '100', '9007199254740993'].map((offset) => ({
      offset,
    }))
    expect(countWithin(records, '1', '9')).toBe(3)
    expect(countWithin(records, '3', '8')).toBe(0)
    expect(countWithin(records, '0', '9007199254740993')).toBe(7)
    expect(countWithin(records, '10', '100')).toBe(2)
    expect(countWithin(records, '11', '99')).toBe(0)
    expect(countWithin(records, '9007199254740992', '9007199254740994')).toBe(1)
    expect(countWithin(records, '9007199254740994', '9007199254740999')).toBe(0)
    expect(countWithin([], '0', '9')).toBe(0)
  })
  test('freezing a running call keeps only its explicit choices', () => {
    const c = setup({ offsets: ['0', '1', '2'] })
    c.call.acknowledge(c.messages[0], 'reject')
    c.call.acknowledge(c.messages[2], 'accept')
    c.call.freeze()
    expect(types(c)).toEqual([3, 1])
    expect(c.call.complete()).toBeNull()
    expect(() => c.call.acknowledge(c.messages[1])).toThrow(KafkaJSNonRetriableError)
  })
})

describe('public share handler contract', () => {
  const {
    A,
    acquire,
    acquired,
    ackAnswer,
    deferred,
    echo,
    fetchAnswer,
    flush,
    part,
    recordsHex,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  const {
    batch: batchBytes,
    record: recordBytes,
    controlBatch,
  } = require('../../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
  const FAKE_TIMERS = { doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] }
  const PACED = { retry: { initialRetryTime: 300, multiplier: 2, maxRetryTime: 1200, factor: 0 } }
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.useRealTimers()
    jest.restoreAllMocks()
  })
  /**
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} A subscribed client; `acks`, `errors` and `crashes` hold its
   *   acknowledge, handler_error and crash payloads, `timeline` their names in publication order
   *   and `crashed` settles at the first crash event.
   */
  const start = async (options) => {
    h = shareClient(options)
    h.acks = []
    h.errors = []
    h.crashes = []
    h.timeline = []
    h.crashed = deferred()
    const { consumer } = h
    consumer.on(consumer.events.ACKNOWLEDGE, ({ payload }) => {
      h.acks.push(payload)
      h.timeline.push('acknowledge')
    })
    consumer.on(consumer.events.HANDLER_ERROR, ({ payload }) => {
      h.errors.push(payload)
      h.timeline.push('handler_error')
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      h.crashes.push(payload)
      h.timeline.push('crash')
      h.crashed.resolve()
    })
    await consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} The acknowledgement batches each ShareFetch carried, in order. */
  const batches = () => h.frames(78).map(batchesOf)
  /** @param {object} frame A recorded share frame. @returns {object[]} Its acknowledgement batches. */
  const batchesOf = ({ fields }) =>
    fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
  /** @returns {object} One wire acknowledgement batch. */
  const wire = (first, last, types) => ({
    firstOffset: String(first),
    lastOffset: String(last),
    acknowledgeTypes: types,
  })
  /** @param {object[]} acks Acknowledge payloads. @returns {Array<Array>} Range, type and error reason of each. */
  const outcomes = (acks) =>
    acks.map(({ firstOffset, lastOffset, type, error }) => [
      firstOffset,
      lastOffset,
      type,
      error ? error.reason : null,
    ])
  const answer = (values, base = 0) => fetchAnswer([acquire(0, base, values)])
  /**
   * Answers the next steady heartbeat with the given fields.
   * @param {object} fields Fields laid over a successful answer.
   * @returns {Promise<void>}
   */
  const answerNextBeat = async (fields) => {
    const beat = h.hold({ api: 'ShareGroupHeartbeat', match: ({ fields: f }) => f.memberEpoch > 0 })
    const call = await beat.arrived()
    beat.release({
      errorCode: 0,
      errorMessage: null,
      memberId: call.fields.memberId,
      memberEpoch: 2,
      heartbeatIntervalMs: 5,
      assignment: null,
      ...fields,
    })
  }
  /**
   * Moves the fake clock in steps of at most 50 ms and lets the scripted sockets answer after each,
   * so a timer that one step fires can finish its work before the next step.
   * @param {number} ms Fake milliseconds.
   * @returns {Promise<void>}
   */
  const tick = async (ms) => {
    for (let left = ms; left > 0; left -= 50) {
      await jest.advanceTimersByTimeAsync(Math.min(50, left))
      await flush()
    }
  }

  test('accepts implicit message and batch normal returns', async () => {
    for (const handler of ['eachMessage', 'eachBatch']) {
      await start()
      h.script('ShareFetch', [answer(['a', 'b'])])
      const handled = []
      await h.consumer.run({
        [handler]: async (payload) => {
          handled.push(
            ...(payload.batch ? payload.batch.messages : [payload.message]).map((m) => m.offset)
          )
        },
      })
      await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
      expect(handled).toEqual(['0', '1'])
      expect(batches().find((b) => b.length > 0)).toEqual([wire(0, 1, [1])])
      await until(() => h.acks.length === 1, 'the settled acknowledgement')
      expect(h.acks[0]).toMatchObject({ type: 'accept', error: null })
      expect(h.frames(79)).toHaveLength(0)
      expect(h.errors).toHaveLength(0)
      expect(h.crashes).toHaveLength(0)
      await h.close()
      h = undefined
    }
  })

  test('keeps explicit choices in implicit mode', async () => {
    await start()
    h.script('ShareFetch', [answer(['a', 'b'])])
    const held = deferred()
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message, acknowledge }) => {
        handled.push(message.offset)
        if (message.offset === '0') {
          acknowledge('accept')
          acknowledge('reject')
          acknowledge('release')
          acknowledge('reject')
          await held.promise
        } else {
          acknowledge('release')
        }
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    await flush()
    expect(h.frames(79)).toHaveLength(0)
    expect(h.frames(78)).toHaveLength(1)
    held.resolve()
    await until(() => h.frames(78).length === 2, 'the next fetch')
    expect(handled).toEqual(['0', '1'])
    expect(batchesOf(h.frames(78)[1])).toEqual([wire(0, 1, [3, 2])])
    expect(h.crashes).toHaveLength(0)
  })

  test.each(['implicit', 'explicit'])(
    'releases undecided records after a throw in %s mode',
    async (acknowledgementMode) => {
      await start({ share: { acknowledgementMode } })
      h.script('ShareFetch', [answer(['a', 'b'])])
      const failure = new Error('handler failed')
      await h.consumer.run({
        eachBatch: async ({ batch, acknowledge }) => {
          acknowledge(batch.messages[0], 'reject')
          throw failure
        },
      })
      await until(() => h.frames(79).length === 1 && h.acks.length === 2, 'the release flush')
      expect(h.errors).toHaveLength(1)
      expect(h.errors[0].error).toBe(failure)
      expect(h.errors[0]).toEqual({
        topic: 'topic-a',
        partition: 0,
        offsets: ['0', '1'],
        error: failure,
      })
      expect(h.frames(79).map(batchesOf)).toEqual([[wire(0, 1, [3, 2])]])
      expect(outcomes(h.acks)).toEqual([
        ['0', '0', 'reject', null],
        ['1', '1', 'release', null],
      ])
      expect(h.crashes).toHaveLength(0)
    }
  )

  test('releases omission then crashes explicit mode without restart', async () => {
    const restartOnFailure = jest.fn(async () => true)
    await start({ share: { acknowledgementMode: 'explicit', retry: { restartOnFailure } } })
    h.script('ShareFetch', [answer(['a', 'b'])])
    await h.consumer.run({
      eachBatch: async ({ batch, acknowledge }) => acknowledge(batch.messages[0]),
    })
    await h.crashed.promise
    expect(h.crashes).toHaveLength(1)
    const { error, restart, groupId } = h.crashes[0]
    expect(error).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'unacknowledged',
      topic: 'topic-a',
      partition: 0,
      ranges: [{ firstOffset: '1', lastOffset: '1' }],
    })
    expect({ restart, groupId }).toEqual({ restart: false, groupId: 'group' })
    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    expect(restartOnFailure.mock.calls[0][0]).toBe(error)
    expect(h.errors).toHaveLength(0)
    expect(h.frames(79).map(batchesOf)).toEqual([[wire(0, 1, [1, 2])]])
    expect(outcomes(h.acks)).toEqual([
      ['0', '0', 'accept', null],
      ['1', '1', 'release', null],
    ])
    expect(h.timeline).toEqual(['acknowledge', 'acknowledge', 'crash'])
  })

  test('settles a fully acknowledged explicit batch without a crash', async () => {
    await start({ share: { acknowledgementMode: 'explicit' } })
    h.script('ShareFetch', [answer(['a', 'b', 'c'])])
    await h.consumer.run({
      eachBatch: async ({ batch, acknowledge }) => {
        for (const message of [...batch.messages].reverse()) acknowledge(message)
      },
    })
    await until(() => h.acks.length === 1, 'the settled acknowledgement')
    expect(h.acks[0]).toEqual({
      topic: 'topic-a',
      partition: 0,
      firstOffset: '0',
      lastOffset: '2',
      type: 'accept',
      error: null,
    })
    expect(h.crashes).toHaveLength(0)
    await until(() => h.frames(78).length >= 3, 'the node keeps fetching')
  })

  test('rejects late, foreign and invalid acknowledgements', async () => {
    await start({ share: { acknowledgementMode: 'explicit' } })
    const both = (deliveryCount) =>
      fetchAnswer([
        part({
          partition: 0,
          acquire: [acquired(0, 1, deliveryCount)],
          records: recordsHex(0, ['a', 'b']),
        }),
      ])
    h.script('ShareFetch', [both(1), both(2)])
    const attempt = (call) => {
      try {
        call()
        return 'accepted'
      } catch (error) {
        return `${error.name}: ${error.message}`
      }
    }
    const seen = { first: [], second: [] }
    let first
    await h.consumer.run({
      eachBatch: async ({ batch, acknowledge, renew }) => {
        const [m0, m1] = batch.messages
        if (!first) {
          first = { acknowledge, renew, messages: batch.messages }
          seen.first.push(
            attempt(() => acknowledge({ ...m0 })),
            attempt(() => acknowledge(m0, 'renew')),
            attempt(() => acknowledge({ offset: '9' })),
            attempt(() => acknowledge({ offset: '1' }, 'reject'))
          )
          acknowledge(m0)
        } else {
          seen.second.push(
            attempt(() => first.acknowledge(m0)),
            attempt(() => first.renew()),
            attempt(() => acknowledge(first.messages[0])),
            attempt(() => acknowledge({ offset: '0' }))
          )
          acknowledge(m1)
        }
      },
    })
    await until(() => seen.second.length === 4 && h.frames(78).length >= 3, 'both calls')
    expect(seen.first).toEqual([
      'KafkaJSNonRetriableError: Message does not belong to this handler call',
      'KafkaJSNonRetriableError: Invalid terminal acknowledgement',
      'KafkaJSNonRetriableError: Message does not belong to this handler call',
      'accepted',
    ])
    expect(seen.second).toEqual([
      'KafkaJSNonRetriableError: Handler call is no longer current',
      'KafkaJSNonRetriableError: Handler call is no longer current',
      'KafkaJSNonRetriableError: Message does not belong to this handler call',
      'accepted',
    ])
    expect(batches().slice(1, 3)).toEqual([[wire(0, 1, [1, 3])], [wire(0, 1, [1])]])
    expect(h.frames(79)).toHaveLength(0)
    expect(h.crashes).toHaveLength(0)
  })

  test('preserves last decisions until the handler returns', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const held = deferred()
    const renewed = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge, renew }) => {
        acknowledge('accept')
        acknowledge('release')
        acknowledge('reject')
        renewed.push(await renew())
        await held.promise
      },
    })
    await until(() => renewed.length === 1, 'the renewal')
    const beat = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    await beat.arrived()
    beat.release()
    await flush()
    expect(h.frames(79).map(({ fields }) => fields.isRenewAck)).toEqual([true])
    expect(h.frames(78)).toHaveLength(1)
    expect(h.acks.filter(({ type }) => type !== 'renew')).toEqual([])
    held.resolve()
    await until(() => h.frames(78).length === 2, 'the next fetch')
    expect(batchesOf(h.frames(78)[1])).toEqual([wire(0, 0, [3])])
  })

  test('lets a crash capture the last provisional choice through the closing authority', async () => {
    await start({ share: { acknowledgementMode: 'explicit' } })
    h.script('ShareFetch', [answer(['a'])])
    const held = deferred()
    const decided = deferred()
    await h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        acknowledge('accept')
        acknowledge('reject')
        decided.resolve()
        await held.promise
      },
    })
    await decided.promise
    await answerNextBeat({ errorCode: 30, memberEpoch: -1, heartbeatIntervalMs: 0 })
    await h.crashed.promise
    expect(h.crashes[0].restart).toBe(false)
    expect(h.frames(78)).toHaveLength(1)
    const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
    expect(finals).toHaveLength(1)
    expect(batchesOf(finals[0])).toEqual([wire(0, 0, [3])])
    expect(h.frames(79)).toHaveLength(1)
    held.resolve()
    await flush()
    expect(h.frames(79)).toHaveLength(1)
    expect(h.errors).toHaveLength(0)
  })

  test('releases an early stopped batch in either mode', async () => {
    for (const acknowledgementMode of ['implicit', 'explicit']) {
      await start({ share: { acknowledgementMode } })
      h.script('ShareFetch', [answer(['a', 'b', 'c'])])
      let stopping
      await h.consumer.run({
        eachBatch: async ({ batch, acknowledge, isRunning }) => {
          acknowledge(batch.messages[0], 'reject')
          stopping = h.consumer.stop()
          expect(isRunning()).toBe(false)
        },
      })
      await until(() => stopping !== undefined, 'the handler')
      await stopping
      expect(h.crashes).toHaveLength(0)
      const final = h.frames(79)[0]?.fields.topics[0].partitions[0].acknowledgementBatches
      expect(final).toEqual([wire(0, 2, [3, 2, 2])])
      await h.close()
      h = undefined
    }
  })

  test.each([
    ['a normal return', false],
    ['a throw', true],
  ])('ignores %s of a handler that was fenced before it returned', async (_, throws) => {
    await start()
    h.script('ShareFetch', [answer(['a']), answer(['b'], 5)])
    const held = deferred()
    const handled = []
    let saved
    await h.consumer.run({
      eachMessage: async ({ message, acknowledge }) => {
        handled.push(message.offset)
        if (message.offset !== '0') return
        saved = acknowledge
        await held.promise
        if (throws) throw new Error('late failure')
      },
    })
    await until(() => handled.length === 1, 'the first handler')
    const rejoin = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch === 0,
    })
    await answerNextBeat({ errorCode: 110, memberEpoch: -1, heartbeatIntervalMs: 0 })
    await rejoin.arrived()
    rejoin.release()
    await until(() => handled.length === 2, 'the replacement handler')
    const old = () => h.acks.filter(({ firstOffset }) => firstOffset === '0')
    expect(outcomes(old())).toEqual([['0', '0', 'release', 'membership-lost']])
    const frames79 = h.frames(79).length
    held.resolve()
    await flush()
    expect(outcomes(old())).toEqual([['0', '0', 'release', 'membership-lost']])
    expect(h.errors).toEqual([])
    expect(h.frames(79)).toHaveLength(frames79)
    expect(() => saved('reject')).toThrow('Handler call is no longer current')
    await until(
      () => h.acks.some(({ firstOffset, error }) => firstOffset === '5' && error === null),
      'the replacement acknowledgement'
    )
    expect(handled).toEqual(['0', '5'])
  })

  test.each([
    ['a normal return', false],
    ['a throw', true],
  ])('ignores %s of a handler that outlived a data session reset', async (_, throws) => {
    await start()
    h.script('ShareFetch', [answer(['a']), answer(['b'], 5)])
    h.script('ShareAcknowledge', [ackAnswer([], { errorCode: 122 })])
    const held = deferred()
    const reset = deferred()
    const handled = []
    const during = {}
    let saved
    await h.consumer.run({
      eachMessage: async ({ message, acknowledge, renew }) => {
        handled.push(message.offset)
        if (message.offset !== '0') return
        saved = acknowledge
        during.renewal = await renew().then(
          () => null,
          (error) => error
        )
        reset.resolve()
        await held.promise
        if (throws) throw new Error('late failure')
      },
    })
    await reset.promise
    await flush()
    expect(during.renewal).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'session-lost',
      ranges: [{ firstOffset: '0', lastOffset: '0' }],
    })
    const old = () => h.acks.filter(({ firstOffset }) => firstOffset === '0')
    const settled = outcomes(old())
    const frames79 = h.frames(79).length
    expect(handled).toEqual(['0'])
    held.resolve()
    await until(() => handled.length === 2, 'the replacement handler')
    await until(
      () => h.acks.some(({ firstOffset, error }) => firstOffset === '5' && error === null),
      'the replacement acknowledgement'
    )
    expect(outcomes(old())).toEqual(settled)
    expect(h.errors).toEqual([])
    expect(h.crashes).toEqual([])
    expect(h.frames(79)).toHaveLength(frames79)
    expect(
      h
        .frames(78)
        .map(({ fields }) => fields.shareSessionEpoch)
        .slice(0, 3)
    ).toEqual([0, 0, 1])
    expect(batches().slice(0, 2)).toEqual([[], []])
    expect(() => saved('reject')).toThrow('Handler call is no longer current')
    expect(handled).toEqual(['0', '5'])
  })

  test('paces consecutive handler failures and resets on success', async () => {
    jest.useFakeTimers(FAKE_TIMERS)
    await start({ share: PACED })
    const stamps = []
    const reader = (values, base) => () => {
      stamps.push(performance.now())
      return values ? answer(values, base) : undefined
    }
    h.script('ShareFetch', [
      reader(['a'], 0),
      reader(['b'], 1),
      reader(['c'], 2),
      reader(['d'], 3),
      reader(),
    ])
    let calls = 0
    await h.consumer.run({
      eachMessage: async () => {
        calls++
        if (calls !== 3) throw new Error('fail')
      },
    })
    await until(() => h.errors.length === 1, 'the first failure')
    const since = () => stamps.map((stamp) => stamp - stamps[0])
    await tick(299)
    expect(since()).toEqual([0])
    await tick(1)
    await until(() => h.errors.length === 2, 'the second failure')
    expect(since()).toEqual([0, 300])
    await tick(599)
    expect(since()).toEqual([0, 300])
    await tick(1)
    await until(() => h.errors.length === 3, 'the failure after the reset')
    expect(since()).toEqual([0, 300, 900, 900])
    await tick(299)
    expect(since()).toEqual([0, 300, 900, 900])
    await tick(1)
    await until(() => stamps.length === 5, 'the fifth fetching request')
    expect(since()).toEqual([0, 300, 900, 900, 1200])
    expect(h.errors).toHaveLength(3)
  })

  test('does not let progress on another node reset a failing node backoff', async () => {
    jest.useFakeTimers(FAKE_TIMERS)
    await start({
      nodes: 2,
      topics: {
        'topic-a': {
          topicId: A,
          partitions: [
            { leader: 0, epoch: 1 },
            { leader: 1, epoch: 1 },
          ],
        },
      },
      share: PACED,
    })
    const stamps = []
    const calls = [0, 0]
    const successes = []
    h.cluster.answer('ShareFetch', (fields, call) => {
      const nth = calls[call.node]++
      if (call.node === 0) {
        stamps.push(performance.now())
        return nth < 3 ? answer(['x'], nth) : echo('ShareFetch', fields)
      }
      return [1, 3].includes(nth)
        ? fetchAnswer([acquire(1, nth, ['y'])])
        : echo('ShareFetch', fields)
    })
    await h.consumer.run({
      eachMessage: async ({ partition, message }) => {
        if (partition === 0) throw new Error('fail')
        successes.push(message.offset)
      },
    })
    await until(() => h.errors.length === 1, 'the first failure of the failing node')
    const since = () => stamps.map((stamp) => stamp - stamps[0])
    await tick(299)
    await until(() => successes.length === 2, 'the progress of the other node')
    expect(successes).toEqual(['1', '3'])
    expect(since()).toEqual([0])
    await tick(1)
    await until(() => h.errors.length === 2, 'the second failure')
    expect(since()).toEqual([0, 300])
    await tick(599)
    expect(since()).toEqual([0, 300])
    await tick(1)
    await until(() => stamps.length === 3, 'the third fetching request')
    expect(since()).toEqual([0, 300, 900])
  })

  test('cancels the owned pacing wait when the run stops', async () => {
    jest.useFakeTimers(FAKE_TIMERS)
    await start({ share: PACED })
    h.script('ShareFetch', [answer(['a'])])
    await h.consumer.run({
      eachMessage: async () => {
        throw new Error('fail')
      },
    })
    await until(() => h.errors.length === 1, 'the failure')
    const stopped = performance.now()
    await h.consumer.stop()
    expect(performance.now()).toBe(stopped)
    expect(h.frames(78)).toHaveLength(1)
    expect(h.crashes).toHaveLength(0)
  })

  test('cancels the owned pacing wait when a fence ends the run and paces the replacement from the initial delay', async () => {
    jest.useFakeTimers(FAKE_TIMERS)
    await start({ share: PACED, interval: 100 })
    const stamps = []
    const reader = (values, base) => () => {
      stamps.push(performance.now())
      return values ? answer(values, base) : undefined
    }
    h.script('ShareFetch', [reader(['a'], 0), reader(['b'], 1), reader(['c'], 2), reader()])
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 110 }])
    await h.consumer.run({
      eachMessage: async () => {
        throw new Error('fail')
      },
    })
    await until(() => h.errors.length === 1, 'the first failure')
    const since = () => stamps.map((stamp) => stamp - stamps[0])
    await tick(399)
    expect(since()).toEqual([0])
    expect(h.errors).toHaveLength(1)
    await tick(1)
    await until(() => h.errors.length === 2, 'the failure of the replacement node')
    expect(since()).toEqual([0, 400])
    await tick(299)
    expect(since()).toEqual([0, 400])
    await tick(1)
    await until(() => stamps.length === 3, 'the third fetching request')
    expect(since()).toEqual([0, 400, 700])
    expect(h.crashes).toEqual([])
  })

  test('does not expose control records to handlers and acknowledges them as gaps', async () => {
    await start()
    const records = Buffer.concat([
      batchBytes({
        baseOffset: 0n,
        records: [recordBytes({ offsetDelta: 0, key: 'k0', value: 'data' })],
      }),
      controlBatch({ baseOffset: 1n }),
    ]).toString('hex')
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(0, 2)], records })]),
    ])
    const handled = []
    await h.consumer.run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(handled).toEqual(['0'])
    expect(batches().find((b) => b.length > 0)).toEqual([wire(0, 0, [1]), wire(1, 2, [0])])
  })

  test('does not expose gaps to handlers', async () => {
    await start()
    const records = recordsHex(0, ['data'])
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(0, 2)], records })]),
    ])
    const handled = []
    await h.consumer.run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(handled).toEqual(['0'])
    expect(batches().find((b) => b.length > 0)).toEqual([wire(0, 0, [1]), wire(1, 2, [0])])
  })

  test('keeps the prefix of a halted acquisition and releases the remainder compactly', async () => {
    await start()
    const good = batchBytes({
      baseOffset: 0n,
      records: [
        recordBytes({ offsetDelta: 0, key: 'k0', value: 'p' }),
        recordBytes({ offsetDelta: 1, key: 'k1', value: 'q' }),
      ],
    })
    const bad = batchBytes({
      baseOffset: 2n,
      records: [recordBytes({ offsetDelta: 0, key: 'k2', value: 'x' })],
      crc: 1,
    })
    h.script('ShareFetch', [
      fetchAnswer([
        part({
          partition: 0,
          acquire: [acquired(0, 999999)],
          records: Buffer.concat([good, bad]).toString('hex'),
        }),
      ]),
    ])
    const handled = []
    await h.consumer.run({
      eachMessage: async ({ message }) => handled.push(message.value.toString()),
    })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(handled).toEqual(['p', 'q'])
    expect(batches().find((b) => b.length > 0)).toEqual([wire(0, 1, [1]), wire(2, 999999, [2])])
    expect(h.crashes).toHaveLength(0)
  })

  test('acknowledges an incomplete trailing batch as one gap and starts no handler', async () => {
    await start()
    const incomplete = Buffer.alloc(13)
    incomplete.writeBigInt64BE(0n, 0)
    incomplete.writeInt32BE(100, 8)
    h.script('ShareFetch', [
      fetchAnswer([
        part({ partition: 0, acquire: [acquired(0, 0)], records: incomplete.toString('hex') }),
      ]),
    ])
    const handled = []
    await h.consumer.run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(handled).toEqual([])
    expect(batches().find((b) => b.length > 0)).toEqual([wire(0, 0, [0])])
  })

  test.each([
    ['undefined', undefined],
    ['null', null],
    ['a string', 'text'],
  ])('handles a non-Error throw of %s as a failure', async (_, thrown) => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    await h.consumer.run({
      eachMessage: async () => {
        throw thrown
      },
    })
    await until(() => h.errors.length === 1, 'the handler error')
    expect(h.errors[0].error).toBeInstanceOf(Error)
    expect(h.errors[0].error.cause).toBe(thrown)
    await until(() => h.acks.length === 1, 'the release settlement')
    expect(h.acks[0]).toMatchObject({ type: 'release', error: null })
  })

  test('keeps the original error and settles a release the broker refused with 121', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    h.script('ShareAcknowledge', [ackAnswer([{ partition: 0, code: 121 }])])
    const failure = new Error('handler failed')
    await h.consumer.run({
      eachMessage: async () => {
        throw failure
      },
    })
    await until(() => h.acks.length === 1, 'the refused release')
    await flush()
    expect(h.errors).toHaveLength(1)
    expect(h.errors[0].error).toBe(failure)
    expect(outcomes(h.acks)).toEqual([['0', '0', 'release', 'rejected-by-broker']])
    expect(h.acks[0].error).not.toBe(failure)
    expect(h.frames(79)).toHaveLength(1)
    expect(h.acks.some(({ type }) => type === 'accept')).toBe(false)
    expect(h.crashes).toHaveLength(0)
  })

  test('keeps the original error and settles a release whose answer was lost', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const failure = new Error('handler failed')
    await h.consumer.run({
      eachMessage: async () => {
        throw failure
      },
    })
    await gate.arrived()
    gate.fail(new Error('connection lost'))
    await until(() => h.acks.length === 1, 'the lost release')
    expect(h.errors).toHaveLength(1)
    expect(h.errors[0].error).toBe(failure)
    expect(outcomes(h.acks)).toEqual([['0', '0', 'release', 'session-lost']])
    expect(h.frames(79)).toHaveLength(1)
    expect(h.acks.some(({ type }) => type === 'accept')).toBe(false)
  })

  test('does not fetch while a returned handler renewal is pending and then resolves it', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const renewals = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        renewals.push(renew())
      },
    })
    await gate.arrived()
    const fetches = h.frames(78).length
    let settled = false
    renewals[0].then(
      () => (settled = true),
      () => (settled = true)
    )
    await flush()
    expect(settled).toBe(false)
    expect(h.frames(78)).toHaveLength(fetches)
    expect(h.frames(79)).toHaveLength(1)
    expect(h.frames(79)[0].fields.isRenewAck).toBe(true)
    gate.release()
    await expect(renewals[0]).resolves.toBe(2000)
    await until(() => h.frames(78).length > fetches, 'the next fetch after the renewal settled')
    expect(batchesOf(h.frames(78)[fetches])).toEqual([wire(0, 0, [1])])
    await until(() => h.acks.some(({ type }) => type === 'accept'), 'the one terminal decision')
    expect(outcomes(h.acks)).toEqual([
      ['0', '0', 'renew', null],
      ['0', '0', 'accept', null],
    ])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(2000)
  })

  test('does not fetch while a returned handler renewal is pending and then rejects its refusal', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    h.script('ShareAcknowledge', [ackAnswer([{ partition: 0, code: 121 }])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    const renewals = []
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        renewals.push(renew())
      },
    })
    await gate.arrived()
    const fetches = h.frames(78).length
    let settled = false
    renewals[0].then(
      () => (settled = true),
      () => (settled = true)
    )
    await flush()
    expect(settled).toBe(false)
    expect(h.frames(78)).toHaveLength(fetches)
    gate.release()
    await until(() => settled, 'the refused renewal')
    await flush()
    await expect(renewals[0]).rejects.toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'rejected-by-broker',
      ranges: [{ firstOffset: '0', lastOffset: '0' }],
    })
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(15000)
    await until(() => h.frames(78).length > fetches, 'the next fetch after the refusal settled')
    expect(batchesOf(h.frames(78)[fetches])).toEqual([wire(0, 0, [1])])
    await until(() => h.acks.some(({ type }) => type === 'accept'), 'the one terminal decision')
    expect(outcomes(h.acks)).toEqual([
      ['0', '0', 'renew', 'rejected-by-broker'],
      ['0', '0', 'accept', null],
    ])
  })

  test('builds no offset array for a successful call in either handler form', async () => {
    const offsets = jest.spyOn(ShareHandlerCall.prototype, 'offsets')
    const byOffsets = jest.spyOn(ShareAckLedger.prototype, 'complete')
    for (const handler of ['eachMessage', 'eachBatch']) {
      await start()
      offsets.mockClear()
      byOffsets.mockClear()
      h.script('ShareFetch', [answer(['a', 'b', 'c'])])
      let handled = 0
      await h.consumer.run({
        [handler]: async (payload) => {
          handled += payload.batch ? payload.batch.messages.length : 1
        },
      })
      await until(() => handled === 3 && h.acks.length >= 1, 'the settled acknowledgement')
      expect(h.acks[0]).toMatchObject({ firstOffset: '0', lastOffset: '2', type: 'accept' })
      expect(offsets).not.toHaveBeenCalled()
      expect(byOffsets).not.toHaveBeenCalled()
      await h.close()
      h = undefined
    }
  })
})
