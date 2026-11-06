const { Schedule } = require('../../../../testHelpers/schedule')
const {
  A,
  acquire,
  acquired,
  ackAnswer,
  deferred,
  fetchAnswer,
  flush,
  frames,
  harness,
  part,
  topError,
} = require('../../../../testHelpers/shareHarness')
const { ConsumerOperation } = require('../../groupProtocol/interface')
const { KafkaJSShareAcknowledgementError } = require('../../../errors')
const { ShareSession } = require('../shareSession')

/** @returns {{session: ShareSession, onReset: jest.Mock, operation: ConsumerOperation}} A session on a real operation. */
const unit = () => {
  const onReset = jest.fn()
  const operation = new ConsumerOperation()
  return { session: new ShareSession({ operation, onReset }), onReset, operation }
}

/** @param {ShareSession} session Session. @param {number} [apiKey] API. @param {object} [extra] Fields. @returns {object} Original request descriptor. */
const request = (session, apiKey = 78, extra = {}) => ({
  token: session.token,
  epoch: session.epoch,
  apiKey,
  topics: [],
  ...extra,
})

/** @param {Promise<*>} promise A promise. @returns {Promise<{value?: *, error?: Error}>} What it settled with. */
const settled = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

describe('serialized share session object', () => {
  test('opens at zero advances only on answers wraps and closes', () => {
    const { session } = unit()
    const open = request(session)
    expect(session.epoch).toBe(0)
    session.apply(open, { errorCode: 0 })
    expect(session.epoch).toBe(1)
    session.apply(request(session), { errorCode: 0 })
    expect(session.epoch).toBe(2)
    session.epoch = 2147483647
    session.apply(request(session), { errorCode: 0 })
    expect(session.epoch).toBe(1)
    session.apply(request(session, 79, { epoch: -1 }), { errorCode: 0 })
    expect(session.closed).toBe(true)
    for (const errorCode of [122, 123, 133, 35, -1]) {
      const final = unit()
      final.session.epoch = 2
      const policy = final.session.apply(request(final.session, 79, { epoch: -1 }), { errorCode })
      expect(policy).toMatchObject({ action: 'closed', backoff: false })
      expect(final.session.closed).toBe(true)
      expect(final.session.refusal(final.session.token)).toBeInstanceOf(Error)
      expect(final.onReset).not.toHaveBeenCalled()
    }
  })

  test('a held answer prevents a second request and builders are lazy', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    const calls = []
    const first = session.enqueue(async () => {
      calls.push('first')
      await schedule.run({ api: 'first' }, () => null)
    })
    await gate.arrived()
    const second = session.enqueue(async () => {
      calls.push('second')
      return 2
    })
    expect(calls).toEqual(['first'])
    gate.release()
    await expect(first).resolves.toBeUndefined()
    await expect(second).resolves.toBe(2)
    expect(calls).toEqual(['first', 'second'])
  })

  test.each([122, 123, 133])(
    'fetch code %s resets and old queued work cannot resend',
    async (code) => {
      const { session, onReset } = unit()
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'active' })
      session.epoch = 4
      const old = session.token
      const active = session.enqueue(() => schedule.run({ api: 'active' }, () => null))
      await gate.arrived()
      const builder = jest.fn()
      const queued = session.enqueue(builder)
      let queuedError
      queued.catch((error) => {
        queuedError = error
      })
      try {
        const reply = session.apply(request(session), { errorCode: code })
        expect(reply.backoff).toBe(code === 133)
        expect(session.token).not.toBe(old)
        expect(session.epoch).toBe(0)
        expect(onReset.mock.calls[0][0]).toBe(old)
        expect(session.queue).toHaveLength(0)
        await Promise.resolve()
        expect(queuedError).toBeInstanceOf(Error)
        expect(builder).not.toHaveBeenCalled()
        expect(session.apply(request(session), { errorCode: code }).backoff).toBe(true)
        expect(() =>
          session.apply({ token: old, apiKey: 78, epoch: 4, topics: [] }, { errorCode: 0 })
        ).toThrow()
      } finally {
        gate.release()
        await Promise.allSettled([active, queued])
      }
    }
  )

  test('a reset rejects queued work before its callback can queue fresh work', async () => {
    const operation = new ConsumerOperation()
    let fresh
    const session = new ShareSession({
      operation,
      onReset: () => {
        fresh = session.enqueue(async () => 'fresh')
      },
    })
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'active' })
    const active = session.enqueue(() => schedule.run({ api: 'active' }, () => null))
    await gate.arrived()
    const stale = session.enqueue(async () => 'stale')
    session.reset('connection', new Error('lost'))
    await expect(stale).rejects.toThrow('lost')
    gate.release()
    await active
    await expect(fresh).resolves.toBe('fresh')
  })

  test('acknowledge 133 advances without resetting and unknown advances with retry', () => {
    const { session, onReset } = unit()
    session.epoch = 2
    const token = session.token
    expect(session.apply(request(session, 79), { errorCode: 133 }).action).toBe('retry')
    expect(session.epoch).toBe(3)
    expect(session.token).toBe(token)
    expect(session.apply(request(session), { errorCode: -1 }).action).toBe('retry')
    expect(session.epoch).toBe(4)
    expect(onReset).not.toHaveBeenCalled()
  })

  test('top-level unknown error -1 advances the epoch under backoff without a reset', () => {
    const { session, onReset } = unit()
    session.epoch = 5
    expect(session.apply(request(session), { errorCode: -1 })).toMatchObject({
      action: 'retry',
      backoff: true,
    })
    expect(session.epoch).toBe(6)
    expect(onReset).not.toHaveBeenCalled()
  })

  test('the first consecutive reset is immediate and a success clears the streak', () => {
    const { session } = unit()
    session.epoch = 3
    expect(session.apply(request(session), { errorCode: 122 }).backoff).toBe(false)
    expect(session.apply(request(session), { errorCode: 122 }).backoff).toBe(true)
    session.apply(request(session), { errorCode: 0 })
    session.epoch = 3
    expect(session.apply(request(session), { errorCode: 123 }).backoff).toBe(false)
  })

  test('models every listed partition then removes same-frame forgotten entries', () => {
    const { session } = unit()
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    session.apply(
      request(session, 78, {
        topics: [{ topicId: A, partitions: [{ partitionIndex: 1 }, { partitionIndex: 2 }] }],
        forgottenTopicsData: [{ topicId: A, partitions: [0, 1] }],
      }),
      { errorCode: 0 }
    )
    expect([...session.model.keys()]).toEqual([`${A}:2`])
  })

  test('connection loss refuses before generation changes and cannot adopt a fresh socket', () => {
    const { session } = unit()
    let connected = true
    const connection = { isConnected: () => connected }
    const pool = { generationFor: jest.fn(() => 8) }
    const broker = { connectionPool: pool }
    const token = session.token
    session.bind(token, { broker, pool, connection, generation: 8 })
    expect(session.refusal(token)).toBeNull()
    expect(session.connectionLost(token)).toBe(false)
    connected = false
    expect(session.refusal(token)).toBeInstanceOf(Error)
    expect(session.connectionLost(token)).toBe(true)
    connected = true
    broker.connectionPool = { generationFor: () => 8 }
    expect(session.refusal(token)).toBeInstanceOf(Error)
    broker.connectionPool = pool
    expect(() =>
      session.bind(token, { broker, pool, generation: 8, connection: { isConnected: () => true } })
    ).toThrow('replaced')
  })

  test('connection loss is not reported for a stale open or a retired operation', () => {
    const { session, operation } = unit()
    let connected = true
    const pool = { generationFor: () => 1 }
    const token = session.token
    session.bind(token, {
      broker: { connectionPool: pool },
      pool,
      connection: { isConnected: () => connected },
      generation: 1,
    })
    connected = false
    expect(session.connectionLost(token)).toBe(true)
    expect(session.connectionLost(Symbol('other'))).toBe(false)
    operation.retire(new Error('closed'))
    expect(session.connectionLost(token)).toBe(false)
  })

  test('transport reset invalidates old identity synchronously even with equal epoch values', () => {
    const { session, onReset } = unit()
    const token = session.token
    session.reset('connection', new Error('timeout'))
    expect(session.epoch).toBe(0)
    expect(session.refusal(token)).toBeInstanceOf(Error)
    expect(onReset).toHaveBeenCalledTimes(1)
    expect(onReset.mock.calls[0][1]).toBe('connection')
  })

  test('retirement rejects queued work and starts no lazy builder after a held answer', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    const first = session.enqueue(() => schedule.run({ api: 'first' }, () => null))
    await gate.arrived()
    const builder = jest.fn()
    const next = session.enqueue(builder)
    const error = new Error('closed')
    session.retire(error)
    await expect(next).rejects.toBe(error)
    gate.release()
    await expect(first).rejects.toBe(error)
    await Promise.resolve()
    expect(builder).not.toHaveBeenCalled()
    expect(session.queue).toHaveLength(0)
  })

  test('busy covers a running request and a queued one', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    expect(session.busy).toBe(false)
    const first = session.enqueue(() => schedule.run({ api: 'first' }, () => null))
    await gate.arrived()
    const second = session.enqueue(async () => null)
    expect(session.busy).toBe(true)
    gate.release()
    await Promise.all([first, second])
    expect(session.busy).toBe(false)
  })

  test('a captured final ends the session after the ordinary operation retired', () => {
    const { session, operation } = unit()
    session.epoch = 3
    operation.retire(new Error('run closed'))
    expect(() => session.apply(request(session, 79, { epoch: -1 }), { errorCode: 0 })).toThrow(
      'run closed'
    )
    expect(
      session.applyFinal(request(session, 79, { epoch: -1 }), { errorCode: 122 })
    ).toMatchObject({ action: 'closed', backoff: false })
    expect(session.closed).toBe(true)
  })
})

describe('share session socket integration', () => {
  let h
  afterEach(async () => {
    if (h) await h.close()
    h = null
    jest.restoreAllMocks()
  })

  test('serializes data and acknowledgement requests at the socket', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [{ topicId: A, partition: 0 }]
    h.script('ShareFetch')
    const gate = h.hold({ api: 'ShareFetch' })
    const send = () =>
      m.session.enqueue((open) => m.loop.exchange({ apiKey: 78, kind: 'fetch' }, open))
    const first = send()
    await gate.arrived()
    const second = send()
    const third = send()
    await flush()
    expect(frames(h.cluster)).toHaveLength(1)
    expect(frames(h.cluster)[0].fields).toMatchObject({
      shareSessionEpoch: 0,
      topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
    })
    gate.release()
    await Promise.all([first, second, third])
    expect(frames(h.cluster).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 2])
    m.session.epoch = 2147483647
    await send()
    await send()
    expect(frames(h.cluster).map(({ fields }) => fields.shareSessionEpoch)).toEqual([
      0, 1, 2, 2147483647, 1,
    ])
    await m.loop.finishNode()
    const last = frames(h.cluster).at(-1)
    expect(last).toMatchObject({ apiKey: 79, fields: { shareSessionEpoch: -1, isRenewAck: false } })
  })

  /** Registers an acquisition of partition 0 without a broker answer: one record, decided when `decide`. */
  const adopted = (m, offset, decide) => {
    const row = m.loop.adopt(
      { open: m.session.token, sentAt: h.clock.time },
      A,
      {
        partitionIndex: 0,
        acquiredRecords: [acquired(offset, offset)],
        messages: [{ offset: String(offset), value: Buffer.from('v') }],
      },
      15000
    )
    if (decide) row.ledger.complete([String(offset)], 'accept')
    row.pendingHandlers = false
    return row
  }

  test.each([
    ['flush', 1],
    ['renewal', 4],
  ])(
    'serializes a queued %s behind a held fetch with five requests allowed in flight',
    async (kind, acknowledgeType) => {
      h = await harness({ pool: { maxInFlightRequests: 5 } })
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      const queue = m.pool.pool[1].requestQueue
      expect(queue.maxInFlightRequests).toBe(5)
      const inflightAtAnswer = []
      const watch = (api) => () => {
        inflightAtAnswer.push([api, queue.inflight.size])
      }
      h.script('ShareFetch', [watch('ShareFetch'), watch('ShareFetch')])
      h.script('ShareAcknowledge', [watch('ShareAcknowledge')])
      const gate = h.hold({ api: 'ShareFetch' })
      const fetch = () =>
        m.session.enqueue((open) => m.loop.exchange({ apiKey: 78, kind: 'fetch' }, open))
      const first = fetch()
      await gate.arrived()
      const row = adopted(m, 77, kind === 'flush')
      const second =
        kind === 'flush'
          ? m.loop.acknowledge()
          : m.loop.acknowledge({ renew: [{ cohort: row, offsets: ['77'] }] })
      const third = fetch()
      await flush()
      const order = () =>
        frames(h.cluster).map(({ apiKey, fields }) => [apiKey, fields.shareSessionEpoch])
      expect(order()).toEqual([[78, 0]])
      expect(m.session.queue).toHaveLength(2)
      expect(queue.pending).toHaveLength(0)
      gate.release()
      await Promise.all([first, second, third])
      expect(order()).toEqual([
        [78, 0],
        [79, 1],
        [78, 2],
      ])
      const [acknowledgement] = frames(h.cluster, 79)
      expect(acknowledgement.fields).toMatchObject({ isRenewAck: kind === 'renewal' })
      expect(acknowledgement.fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '77', lastOffset: '77', acknowledgeTypes: [acknowledgeType] },
      ])
      expect(inflightAtAnswer).toEqual([
        ['ShareFetch', 1],
        ['ShareAcknowledge', 1],
        ['ShareFetch', 1],
      ])
    }
  )

  describe('session errors', () => {
    const retry = { initialRetryTime: 0, factor: 0, multiplier: 1, maxRetryTime: 0 }

    test.each([122, 123, 133])(
      'opening fetch code %s resets without tickets and reopens at epoch zero',
      async (code) => {
        h = await harness({ loop: { retry } })
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [topError(code)])
        const result = await m.loop.poll()
        expect(result).toMatchObject({ reset: true, backoff: code === 133 })
        expect(m.resets).toHaveLength(1)
        expect(m.session.epoch).toBe(0)
        if (code === 133) expect(m.sleeps).toEqual([250])
        else expect(m.sleeps).toEqual([])
        await m.loop.poll()
        const opening = frames(h.cluster, 78)
        expect(opening.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 0])
        expect(opening[1].fields.topics[0].partitions).toEqual([
          { partitionIndex: 0, acknowledgementBatches: [] },
        ])
      }
    )

    test.each([122, 123, 133])(
      'piggyback fetch code %s resets and settles the old acknowledgement session-lost once',
      async (code) => {
        h = await harness({ loop: { retry } })
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])]), topError(code)])
        await m.loop.poll()
        expect(m.session.epoch).toBe(1)
        const result = await m.loop.poll()
        expect(result.reset).toBe(true)
        const failures = m.results.filter((outcome) => outcome.error)
        expect(failures).toHaveLength(1)
        expect(failures[0]).toMatchObject({ firstOffset: '10', lastOffset: '11', type: 'accept' })
        expect(failures[0].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
        expect(failures[0].error.reason).toBe('session-lost')
        expect(m.results.filter((outcome) => !outcome.error)).toEqual([])
        await m.loop.poll()
        const last = frames(h.cluster, 78).at(-1).fields
        expect(last.shareSessionEpoch).toBe(0)
        expect(last.topics).toEqual([
          { topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
        ])
        expect(m.results.filter((outcome) => outcome.error)).toHaveLength(1)
      }
    )

    test.each([122, 123])(
      'acknowledge code %s resets and never replays the old decision',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
        h.script('ShareAcknowledge', [topError(code)])
        await m.loop.poll()
        const result = await m.loop.acknowledge()
        expect(result.reset).toBe(true)
        expect(m.resets).toHaveLength(1)
        const failures = m.results.filter((outcome) => outcome.error)
        expect(failures.map((outcome) => outcome.error.reason)).toEqual(['session-lost'])
        await m.loop.poll()
        const last = frames(h.cluster, 78).at(-1).fields
        expect(last.shareSessionEpoch).toBe(0)
        expect(last.topics[0].partitions[0].acknowledgementBatches).toEqual([])
      }
    )

    test('acknowledge code 133 advances and rejects its tickets without a reset', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      h.script('ShareAcknowledge', [topError(133)])
      await m.loop.poll()
      const open = m.session.token
      const result = await m.loop.acknowledge()
      expect(result).toMatchObject({ reset: false, backoff: true })
      expect(m.resets).toEqual([])
      expect(m.session.token).toBe(open)
      expect(m.session.epoch).toBe(2)
      const failures = m.results.filter((outcome) => outcome.error)
      expect(failures.map((outcome) => outcome.error.reason)).toEqual(['rejected-by-broker'])
    })

    test('the first reset is immediate, later ones are delayed, and 133 waits even with a zero retry time', async () => {
      h = await harness({ loop: { retry } })
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [topError(122), topError(122), topError(133)])
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: false })
      expect(m.sleeps).toEqual([])
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: true })
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: true })
      expect(m.sleeps).toEqual([50, 250])
    })

    test('a handler held across a reset gets no traffic afterwards', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      h.script('ShareAcknowledge', [topError(122)])
      let rows
      let started
      const running = new Promise((resolve) => {
        started = resolve
      })
      let proceed
      const held = new Promise((resolve) => {
        proceed = resolve
      })
      let late
      m.dispatch = async (cohorts) => {
        rows = cohorts
        started()
        await held
        try {
          rows[0].ledger.complete(['10', '11'], 'accept')
        } catch (error) {
          late = error
        }
      }
      const round = m.loop.poll()
      const outcome = round.then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      await running
      await expect(
        m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10', '11'] }] })
      ).rejects.toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(m.resets).toHaveLength(1)
      const before = frames(h.cluster).length
      proceed()
      expect((await outcome).error).toBeInstanceOf(Error)
      expect(late.message).toMatch(/outside its active handler acquisition/)
      expect(frames(h.cluster)).toHaveLength(before)
      expect(m.results.filter((result) => !result.error)).toEqual([])
    })
  })

  describe('written but unanswered mutations', () => {
    test.each([
      ['an acknowledgement request timeout', 79, 'timeout'],
      ['a piggybacked fetch request timeout', 78, 'timeout'],
      ['an acknowledgement in-flight disconnect', 79, 'disconnect'],
    ])('drops %s instead of resending it', async (label, apiKey, failure) => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a', 'b'])]),
        fetchAnswer([acquire(0, 10, ['a', 'b'])]),
      ])
      await m.loop.poll()
      const open = m.session.token
      const generation = m.pool.generationFor(78)
      const gate = h.hold({ api: apiKey === 79 ? 'ShareAcknowledge' : 'ShareFetch' })
      const call = settled(apiKey === 79 ? m.loop.acknowledge() : m.loop.poll())
      await gate.arrived()
      const written = frames(h.cluster, apiKey).at(-1)
      expect(written.fields.shareSessionEpoch).toBe(1)
      expect(written.fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '10', lastOffset: '11', acknowledgeTypes: [1] },
      ])
      const connection = m.pool.pool[1]
      if (failure === 'timeout') [...connection.requestQueue.inflight.values()][0].timeoutRequest()
      else await connection.disconnect({ drainTimeout: 0 })
      expect((await call).value).toMatchObject({ reset: true, backoff: true })
      expect(m.session.refusal(open)).toBeInstanceOf(Error)
      expect(m.pool.generationFor(78)).toBe(generation)
      expect(m.resets.map(({ reason }) => reason)).toEqual(['connection'])
      const failed = m.results.filter((outcome) => outcome.error)
      expect(failed).toHaveLength(1)
      expect(failed[0]).toMatchObject({ firstOffset: '10', lastOffset: '11', type: 'accept' })
      expect(failed[0].error.reason).toBe('session-lost')
      expect(m.results.filter((outcome) => !outcome.error)).toEqual([])

      await m.loop.poll()
      const reopened = frames(h.cluster, 78).at(-1).fields
      expect(reopened.shareSessionEpoch).toBe(0)
      expect(reopened.topics[0].partitions[0].acknowledgementBatches).toEqual([])
      expect(m.pool.generationFor(78)).toBeGreaterThan(generation)
      const replacement = m.session.token
      expect(replacement).not.toBe(open)
      expect(m.handled.map(({ offset }) => offset)).toEqual(['10', '11', '10', '11'])

      const settledBefore = m.results.length
      const modelBefore = [...m.session.model.keys()]
      const writtenBefore = frames(h.cluster).length
      gate.release(
        apiKey === 79 ? ackAnswer([{ partition: 0 }]) : fetchAnswer([part({ partition: 0 })])
      )
      await flush()
      expect(m.results).toHaveLength(settledBefore)
      expect(m.session.token).toBe(replacement)
      expect(m.session.epoch).toBe(1)
      expect([...m.session.model.keys()]).toEqual(modelBefore)
      expect(frames(h.cluster)).toHaveLength(writtenBefore)

      await m.loop.poll()
      expect(m.results.filter((outcome) => !outcome.error)).toEqual([
        expect.objectContaining({ firstOffset: '10', lastOffset: '11', type: 'accept' }),
      ])
      expect(m.results.filter((outcome) => outcome.error)).toHaveLength(1)
    })

    test('a disconnect before the generation counter moves refuses at once and resets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      const generation = m.pool.generationFor(78)
      await m.pool.pool[1].disconnect({ drainTimeout: 0 })
      expect(m.pool.generationFor(78)).toBe(generation)
      expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
      const written = frames(h.cluster).length
      const result = await m.loop.poll()
      expect(result.reset).toBe(true)
      expect(frames(h.cluster)).toHaveLength(written)
      expect(m.results.filter((outcome) => outcome.error).map((o) => o.error.reason)).toEqual([
        'session-lost',
      ])
      await m.loop.poll()
      const reopened = frames(h.cluster, 78).at(-1).fields
      expect(reopened.shareSessionEpoch).toBe(0)
      expect(reopened.topics[0].partitions[0].acknowledgementBatches).toEqual([])
    })
  })

  test('keeps the session across a heartbeat-socket reconnect only', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [{ topicId: A, partition: 0 }]
    await m.loop.poll()
    const data = m.pool.generationFor(78)
    const heartbeat = m.pool.pool[0].generation
    await m.pool.pool[0].disconnect({ drainTimeout: 0 })
    await m.pool.getConnection(0)
    expect(m.pool.pool[0].generation).toBeGreaterThan(heartbeat)
    expect(m.pool.generationFor(78)).toBe(data)
    await m.loop.poll()
    expect(m.resets).toEqual([])
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1])
    await m.pool.pool[1].disconnect({ drainTimeout: 0 })
    expect(m.pool.generationFor(78)).toBe(data)
    expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
    expect((await m.loop.poll()).reset).toBe(true)
    await m.loop.poll()
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 0])
    const replacement = h.newPool(m.node)
    m.broker.replaceConnectionPool(replacement)
    expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
    expect((await m.loop.poll()).reset).toBe(true)
    await m.loop.poll()
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([
      0, 1, 0, 0,
    ])
    expect(m.session.binding.pool).toBe(replacement)
  })

  describe('delayed old callbacks', () => {
    const hint = (leaderId, leaderEpoch) => ({ leaderId, leaderEpoch })

    /** Starts a replacement run on the same broker with the same member and wanted data and a fresh acquisition identity. */
    const replace = (m) => {
      const next = h.runOn({ node: m.node, pool: m.pool, broker: m.broker })
      next.want = [...m.want]
      next.loop.authority = { acquire: () => ({ owner: 'replacement' }) }
      return next
    }

    /** What the replacement shows once its records are handled and acknowledged: its own identity on both, one accepted range, no failure. */
    const REPLACEMENT_OUTCOME = {
      handled: [
        ['10', { owner: 'replacement' }],
        ['11', { owner: 'replacement' }],
      ],
      results: [['10', '11', 'accept', undefined]],
    }

    /** @returns {{handled: Array<[string, object]>, results: Array<Array<*>>}} The public record of a run: what it handled with which acquisition identity, and every settlement with its failure. */
    const publicOutcome = (run) => ({
      handled: run.handled.map(({ offset, token }) => [offset, token]),
      results: run.results.map(({ firstOffset, lastOffset, type, error }) => [
        firstOffset,
        lastOffset,
        type,
        error,
      ]),
    })

    /** What a run holds and has shown, so "nothing changed" is one comparison. */
    const state = (run) => ({
      results: run.results.length,
      handled: run.handled.length,
      moves: run.moves.length,
      epoch: run.session.epoch,
      token: run.session.token,
      model: [...run.session.model.keys()],
      sleeps: run.sleeps.length,
      waiters: run.operation.waiters.size,
      disposers: run.operation.disposers.size,
      timer: run.operation.timer,
      queued: run.session.queue.length,
      exchanges: run.loop.exchanges.size,
      cohorts: [...run.loop.cohorts.keys()],
      frames: frames(h.cluster).length,
    })

    /**
     * Retires `old` while its await is held, runs a replacement through the same offsets with its
     * own handler and ticket until its decisions are acknowledged, and returns it with its state.
     */
    const replaced = async (old) => {
      old.loop.authority = { acquire: () => ({ owner: 'old' }) }
      old.operation.retire(new Error('run closed'))
      const next = replace(old)
      await next.loop.poll()
      await next.loop.poll()
      expect(publicOutcome(next)).toEqual(REPLACEMENT_OUTCOME)
      return next
    }

    /** An answer that acquires offsets 10 and 11: what the replacement receives, and what a stale answer would carry again. */
    const acquiresTen = () => fetchAnswer([acquire(0, 10, ['x', 'y'])])

    /** Asserts the released old await changed nothing anywhere. */
    const unchanged = async (old, next, before, staleOutcome) => {
      expect((await staleOutcome).error.message).toBe('run closed')
      await flush()
      expect(state(next)).toEqual(before)
      expect(publicOutcome(next)).toEqual(REPLACEMENT_OUTCOME)
      expect(old.handled).toEqual([])
      expect(old.results).toEqual([])
      expect([...old.session.model.keys()]).toEqual([])
      expect(old.operation.waiters.size).toBe(0)
      expect(old.operation.disposers.size).toBe(0)
      expect(old.loop.exchanges.size).toBe(0)
      expect(old.loop.cohorts.size).toBe(0)
    }

    test('an old connection acquisition cannot touch a replacement run', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [acquiresTen()])
      const original = old.pool.getConnectionByRequest.bind(old.pool)
      const entered = deferred()
      const held = deferred()
      jest.spyOn(old.pool, 'getConnectionByRequest').mockImplementationOnce(async (...args) => {
        entered.resolve()
        await held.promise
        return original(...args)
      })
      const stale = settled(old.loop.poll())
      await entered.promise
      const next = await replaced(old)
      const before = state(next)
      held.resolve()
      await unchanged(old, next, before, stale)
    })

    test('an old transmission is refused at the socket after its run retired', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [acquiresTen()])
      const original = old.pool.send.bind(old.pool)
      const entered = deferred()
      const held = deferred()
      jest.spyOn(old.pool, 'send').mockImplementationOnce(async (...args) => {
        entered.resolve()
        await held.promise
        return original(...args)
      })
      const stale = settled(old.loop.poll())
      await entered.promise
      const next = await replaced(old)
      const before = state(next)
      held.resolve()
      await unchanged(old, next, before, stale)
    })

    test('an old answer applies nothing to a replacement run', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [acquiresTen()])
      const gate = h.hold({ api: 'ShareFetch' })
      const stale = settled(old.loop.poll())
      await gate.arrived()
      const next = await replaced(old)
      const before = state(next)
      gate.release(acquiresTen())
      await unchanged(old, next, before, stale)
    })

    test('an old leader-hint lookup installs no routing after its run retired', async () => {
      h = await harness({ nodes: 3 })
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      const real = await h.realCluster({
        'topic-a': { topicId: A, partitions: [{ leader: 0, epoch: 5 }] },
      })
      h.scriptNodes('ShareFetch', {
        0: [
          fetchAnswer([part({ partition: 0, code: 6, leader: hint(1, 9) })], {
            nodeEndpoints: [{ nodeId: 1, host: 'node1', port: 9092, rack: null }],
          }),
          acquiresTen(),
          fetchAnswer([part({ partition: 0, code: 6, leader: hint(2, 8) })], {
            nodeEndpoints: [{ nodeId: 2, host: 'node2', port: 9092, rack: null }],
          }),
        ],
      })
      const original = real.cluster.applyLeaderHint.bind(real.cluster)
      const entered = deferred()
      const held = deferred()
      jest.spyOn(real.cluster, 'applyLeaderHint').mockImplementationOnce(async (...args) => {
        entered.resolve()
        await held.promise
        return original(...args)
      })
      const stale = settled(old.loop.poll())
      await entered.promise
      const next = await replaced(old)
      const move = {
        topic: 'topic-a',
        topicId: A,
        partition: 0,
        leaderId: 2,
        reason: 'leader-moved',
      }
      expect(real.cluster.cachedLeader('topic-a', 0)).toBe(2)
      expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(8)
      expect(next.moves).toEqual([move])
      const before = state(next)
      held.resolve()
      expect((await stale).error.message).toBe('run closed')
      await flush()
      expect(real.cluster.cachedLeader('topic-a', 0)).toBe(2)
      expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(8)
      expect(old.moves).toEqual([])
      expect(next.moves).toEqual([move])
      expect(state(next)).toEqual(before)
      expect(publicOutcome(next)).toEqual(REPLACEMENT_OUTCOME)
      expect(real.refreshes()).toBe(0)
    })
  })
})
