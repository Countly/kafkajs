const { ShareHandlerCall } = require('../shareRunner')
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
    acquire,
    acquired,
    fetchAnswer,
    flush,
    part,
    recordsHex,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} Subscribed client with observers. */
  const start = async (options) => {
    h = shareClient(options)
    h.acks = []
    h.errors = []
    h.crashes = []
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => h.acks.push(payload))
    h.consumer.on(h.consumer.events.HANDLER_ERROR, ({ payload }) => h.errors.push(payload))
    h.consumer.on(h.consumer.events.CRASH, ({ payload }) => h.crashes.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} The acknowledgement batches each ShareFetch carried, in order. */
  const batches = () =>
    h
      .frames(78)
      .map(({ fields }) =>
        fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
      )
  const answer = (values, base = 0) => fetchAnswer([acquire(0, base, values)])

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
      expect(batches().find((b) => b.length > 0)).toEqual([
        { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1] },
      ])
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
    h.script('ShareFetch', [answer(['a'])])
    await h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        acknowledge('reject')
        acknowledge('release')
        acknowledge('reject')
      },
    })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(batches().find((b) => b.length > 0)).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
    ])
  })

  test('releases undecided records after a throw', async () => {
    for (const acknowledgementMode of ['implicit', 'explicit']) {
      await start({ share: { acknowledgementMode } })
      h.script('ShareFetch', [answer(['a', 'b'])])
      const failure = new Error('handler failed')
      await h.consumer.run({
        eachBatch: async ({ batch, acknowledge }) => {
          acknowledge(batch.messages[0], 'reject')
          throw failure
        },
      })
      await until(
        () => h.frames(79).length + batches().filter((b) => b.length).length > 0,
        'the release flush'
      )
      await until(() => h.errors.length === 1, 'the handler error')
      expect(h.errors[0].error).toBe(failure)
      expect(h.errors[0]).toMatchObject({ topic: 'topic-a', partition: 0, offsets: ['0', '1'] })
      const flush79 = h.frames(79)[0]?.fields.topics[0].partitions[0].acknowledgementBatches
      const sent = flush79 ?? batches().find((b) => b.length > 0)
      expect(sent).toEqual([{ firstOffset: '0', lastOffset: '1', acknowledgeTypes: [3, 2] }])
      await h.close()
      h = undefined
    }
  })

  test('releases omission then crashes explicit mode without restart', async () => {
    const restartOnFailure = jest.fn(async () => true)
    await start({ share: { acknowledgementMode: 'explicit', retry: { restartOnFailure } } })
    h.script('ShareFetch', [answer(['a', 'b'])])
    await h.consumer.run({
      eachBatch: async ({ batch, acknowledge }) => acknowledge(batch.messages[0]),
    })
    await until(() => h.crashes.length === 1, 'the crash event')
    const { error, restart } = h.crashes[0]
    expect(error).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'unacknowledged',
      ranges: [{ firstOffset: '1', lastOffset: '1' }],
    })
    expect(restart).toBe(false)
    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    expect(h.errors).toHaveLength(0)
    const sent = h.frames(79)[0]?.fields.topics[0].partitions[0].acknowledgementBatches
    expect(sent).toEqual([{ firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1, 2] }])
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

  test('rejects late and foreign acknowledgements', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    const saved = []
    await h.consumer.run({
      eachMessage: async ({ message, acknowledge, renew }) => {
        saved.push({ acknowledge, renew })
        expect(() => acknowledge('renew')).toThrow()
        expect(message.deliveryCount).toBe(1)
      },
    })
    await until(() => saved.length === 1 && batches().some((b) => b.length > 0), 'the return')
    expect(() => saved[0].acknowledge('reject')).toThrow('no longer current')
    expect(() => saved[0].renew()).toThrow('no longer current')
    expect(batches().find((b) => b.length > 0)).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
    ])
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
      expect(final).toEqual([{ firstOffset: '0', lastOffset: '2', acknowledgeTypes: [3, 2, 2] }])
      await h.close()
      h = undefined
    }
  })

  test('does not expose gaps or control records to handlers', async () => {
    await start()
    const records = recordsHex(0, ['data'])
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(0, 2)], records })]),
    ])
    const handled = []
    await h.consumer.run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => batches().some((b) => b.length > 0), 'a piggybacked acknowledgement')
    expect(handled).toEqual(['0'])
    expect(batches().find((b) => b.length > 0)).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      { firstOffset: '1', lastOffset: '2', acknowledgeTypes: [0] },
    ])
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

  test('paces consecutive handler failures and resets on success', async () => {
    await start({
      share: { retry: { initialRetryTime: 300, multiplier: 2, maxRetryTime: 1200, factor: 0 } },
    })
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
    await until(() => stamps.length === 5, 'five fetching requests')
    expect(h.errors).toHaveLength(3)
    expect(stamps[1] - stamps[0]).toBeGreaterThanOrEqual(299)
    expect(stamps[2] - stamps[1]).toBeGreaterThanOrEqual(599)
    expect(stamps[4] - stamps[3]).toBeGreaterThanOrEqual(299)
    expect(stamps[4] - stamps[3]).toBeLessThan(599)
  })

  test('does not fetch while a returned handler renewal is pending', async () => {
    await start()
    h.script('ShareFetch', [answer(['a'])])
    const gate = h.hold({ api: 'ShareAcknowledge' })
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        renew().catch(() => {})
      },
    })
    await gate.arrived()
    const fetches = h.frames(78).length
    await flush()
    expect(h.frames(78)).toHaveLength(fetches)
    expect(h.frames(79)).toHaveLength(1)
    gate.release()
    await until(() => h.frames(78).length > fetches, 'the next fetch after the renewal settled')
  })
})
