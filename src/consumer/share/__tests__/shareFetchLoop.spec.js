const {
  batch,
  record,
} = require('../../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const { Schedule } = require('../../../../testHelpers/schedule')
const {
  A,
  B,
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
  until,
} = require('../../../../testHelpers/shareHarness')
const { ConsumerOperation } = require('../../groupProtocol/interface')
const {
  KafkaJSInvariantViolation,
  KafkaJSProtocolError,
  KafkaJSShareAcknowledgementError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../../errors')
const { ShareSession } = require('../shareSession')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareRecordWalk } = require('../shareRecordWalk')

/** @param {object} [options] Planner overrides. @returns {object} A planner with no transport and a fixed clock. */
const planner = (options = {}) => {
  const operation = new ConsumerOperation()
  jest.spyOn(operation, 'sleep').mockResolvedValue()
  const session = new ShareSession({ operation })
  const loop = new ShareFetchLoop({
    session,
    groupId: 'group',
    memberId: 'member',
    minBytes: 1,
    maxBytes: 1048576,
    maxRecords: 500,
    acquireMode: 'record_limit',
    now: () => 10000,
    ...options,
  })
  return { loop, session, operation }
}

/** @param {object} session Session. @param {number} partition Partition. @param {boolean} [renew] Renew ticket. @returns {object} Real compact ledger ticket. */
const prepared = (session, partition, renew = false) => {
  const walk = ShareRecordWalk.walk({
    acquiredRecords: [
      { firstOffset: '9007199254740993', lastOffset: '9007199254740993', deliveryCount: 1 },
    ],
    messages: [{ offset: '9007199254740993' }],
  })
  const ledger = new ShareAckLedger({
    topic: '__proto__',
    topicId: A,
    partition,
    token: session.token,
    sentAt: 100,
    lockDuration: 1000,
    walk,
  })
  if (!renew) ledger.complete(['9007199254740993'], 'accept')
  const { ticket } = ledger.prepare({
    token: session.token,
    ...(renew ? { renewOffsets: ['9007199254740993'] } : {}),
  })
  return { ledger, ticket }
}

describe('share node frame planning', () => {
  afterEach(() => jest.restoreAllMocks())

  test('a held handler prevents prefetch while an unawaited renewal stays ahead of the next fetch', async () => {
    const { loop, session } = planner()
    const schedule = new Schedule()
    const handlerGate = schedule.hold({ api: 'handler' })
    const renewGate = schedule.hold({ api: 'renew' })
    const calls = []
    const fetch = async () => {
      calls.push('fetch')
      return { reply: [], sentAt: 100, answeredAt: 105, empty: false }
    }
    const first = loop.round(fetch, () => schedule.run({ api: 'handler' }, () => null))
    await handlerGate.arrived()
    for (let i = 0; i < 6; i++) await Promise.resolve()
    const second = loop.round(fetch, async () => {})
    expect(second).toBe(first)
    expect(calls).toEqual(['fetch'])
    const renew = session.enqueue(async () => {
      calls.push('renew')
      await schedule.run({ api: 'renew' }, () => null)
    })
    await renewGate.arrived()
    handlerGate.release()
    await first
    const next = loop.round(fetch, async () => {})
    expect(calls).toEqual(['fetch', 'renew'])
    renewGate.release()
    await renew
    await next
    expect(calls).toEqual(['fetch', 'renew', 'fetch'])
  })

  test('the empty-round delay is owned by the original session operation', async () => {
    const { loop, operation } = planner()
    await loop.round(
      async () => ({ reply: [], sentAt: 100, answeredAt: 105, empty: true }),
      async () => {}
    )
    expect(operation.sleep).toHaveBeenCalledTimes(1)
    expect(operation.sleep).toHaveBeenCalledWith(45)
  })

  test('opening fields use 500 milliseconds and positive per-node record limits', () => {
    const { loop } = planner()
    expect(loop.buildFetch([{ topicId: A, partition: 0 }], [], new Set())).toEqual({
      groupId: 'group',
      memberId: 'member',
      shareSessionEpoch: 0,
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1048576,
      maxRecords: 500,
      batchSize: 500,
      shareAcquireMode: 1,
      isRenewAck: false,
      topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
      forgottenTopicsData: [],
    })
  })

  test('piggyback forgets a revoked acknowledgement-only partition in its very first frame', () => {
    const { loop, session } = planner()
    session.epoch = 3
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    const ack = prepared(session, 1)
    const body = loop.buildFetch([{ topicId: B, partition: 2 }], [ack], new Set())
    expect(body.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 1,
            acknowledgementBatches: [
              {
                firstOffset: '9007199254740993',
                lastOffset: '9007199254740993',
                acknowledgeTypes: [1],
              },
            ],
          },
        ],
      },
      { topicId: B, partitions: [{ partitionIndex: 2, acknowledgementBatches: [] }] },
    ])
    expect(body.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0, 1] }])
    session.apply(
      { ...body, epoch: body.shareSessionEpoch, token: session.token, apiKey: 78 },
      { errorCode: 0 }
    )
    expect([...session.model.keys()]).toEqual([`${B}:2`])
    expect(loop.buildFetch([{ topicId: B, partition: 2 }], [], new Set()).topics).toEqual([])
  })

  test.each(['flush', 'renew'])(
    'first fetch after %s forgets old listed partitions without acknowledgements',
    (kind) => {
      const { loop, session } = planner()
      session.epoch = 2
      const acknowledgement = loop.buildAcknowledge([prepared(session, 1, kind === 'renew')])
      expect(acknowledgement.isRenewAck).toBe(kind === 'renew')
      session.apply(
        {
          token: session.token,
          epoch: acknowledgement.shareSessionEpoch,
          apiKey: 79,
          topics: acknowledgement.topics,
        },
        { errorCode: 0 }
      )
      expect([...session.model.keys()]).toEqual([`${A}:1`])
      const body = loop.buildFetch([{ topicId: B, partition: 2 }], [], new Set())
      expect(body.forgottenTopicsData).toEqual([{ topicId: A, partitions: [1] }])
      expect(body.topics).toEqual([
        { topicId: B, partitions: [{ partitionIndex: 2, acknowledgementBatches: [] }] },
      ])
    }
  )

  test('held unwanted partitions are retained until the last decision is eligible', () => {
    const { loop, session } = planner()
    session.epoch = 2
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    expect(loop.buildFetch([], [], new Set([`${A}:0`])).forgottenTopicsData).toEqual([])
    expect(loop.buildFetch([], [], new Set()).forgottenTopicsData).toEqual([
      { topicId: A, partitions: [0] },
    ])
  })

  test('opening never carries acknowledgements and finals skip an unopened node', () => {
    const { loop, session } = planner()
    expect(() => loop.buildFetch([], [prepared(session, 0)], new Set())).toThrow('opening')
    expect(loop.buildAcknowledge([], true)).toBeNull()
    expect(() => loop.buildAcknowledge([])).toThrow('established')
  })

  test('a duplicate acquisition of one partition is refused before any frame exists', () => {
    const { loop, session } = planner()
    session.epoch = 2
    expect(() =>
      loop.buildFetch([], [prepared(session, 0), prepared(session, 0)], new Set())
    ).toThrow('One acquisition per partition per request is required')
  })

  test('flush renewal and final use acknowledgement frames with no fetch or forgotten fields', () => {
    const { loop, session } = planner()
    session.epoch = 1
    const renew = loop.buildAcknowledge([prepared(session, 0, true)])
    expect(renew.isRenewAck).toBe(true)
    expect(renew.topics[0].partitions[0].acknowledgementBatches[0].acknowledgeTypes).toEqual([4])
    const terminal = prepared(session, 1)
    const final = loop.buildAcknowledge([terminal], true)
    expect(final.shareSessionEpoch).toBe(-1)
    expect(final.isRenewAck).toBe(false)
    for (const body of [renew, final]) {
      expect(body).not.toHaveProperty('maxRecords')
      expect(body).not.toHaveProperty('maxWaitMs')
      expect(body).not.toHaveProperty('forgottenTopicsData')
    }
    expect(() => loop.buildAcknowledge([prepared(session, 3, true)], true)).toThrow('renew')
    expect(() => loop.buildFetch([], [prepared(session, 2, true)], new Set())).toThrow(
      'ShareAcknowledge'
    )
  })

  test.each([0, 1, 500])(
    'empty answers are paced with maxWaitTimeInMs %s and minBytes zero',
    (maxWaitTimeInMs) => {
      const { loop } = planner({ maxWaitTimeInMs, minBytes: 0 })
      expect(loop.buildFetch([], [], new Set()).maxWaitMs).toBe(maxWaitTimeInMs)
      expect(loop.buildFetch([], [], new Set()).minBytes).toBe(0)
      expect(loop.emptyDelay(100, 105, true)).toBe(45)
      expect(loop.emptyDelay(100, 150, true)).toBe(0)
      expect(loop.emptyDelay(100, 175, true)).toBe(0)
      expect(loop.emptyDelay(100, 105, false)).toBe(0)
    }
  )

  test.each(['record_limit', 'batch_optimized'])(
    'mode %s keeps limits per node without borrowing another node capacity',
    (acquireMode) => {
      const { loop } = planner({ acquireMode, maxRecords: 7 })
      const body = loop.buildFetch([], [], new Set())
      expect(body.maxRecords).toBe(7)
      expect(body.batchSize).toBe(7)
      expect(body.shareAcquireMode).toBe(acquireMode === 'record_limit' ? 1 : 0)
    }
  )

  test.each([{ maxRecords: 0 }, { maxBytes: 0 }])(
    'zero fetching limits are refused: %j',
    (options) => {
      const { loop } = planner(options)
      expect(() => loop.buildFetch([], [], new Set())).toThrow('positive')
    }
  )
})

/** An independent model of the broker's session set: an opening request replaces it, later ones add Topics and drop Forgotten. */
const sessionSet = () => {
  const held = new Set()
  return {
    apply({ shareSessionEpoch, topics, forgottenTopicsData }) {
      if (shareSessionEpoch === 0) held.clear()
      for (const topic of topics)
        for (const partition of topic.partitions)
          held.add(`${topic.topicId}:${partition.partitionIndex}`)
      for (const topic of forgottenTopicsData ?? [])
        for (const partition of topic.partitions) held.delete(`${topic.topicId}:${partition}`)
    },
    has: (topicId, partition) => held.has(`${topicId}:${partition}`),
    keys: () => [...held].sort(),
  }
}

/**
 * Registers an acquisition the node holds without a broker answer: one record, optionally decided.
 * @returns {object} The retained acquisition.
 */
const inject = (h, m, topicId, partition, offset, value, { decide = true } = {}) => {
  const row = m.loop.adopt(
    { open: m.session.token, sentAt: h.clock.time },
    topicId,
    {
      partitionIndex: partition,
      acquiredRecords: [acquired(offset, offset)],
      messages: [{ offset: String(offset), value: Buffer.from(value) }],
    },
    15000
  )
  if (decide) {
    row.ledger.complete([String(offset)], 'accept')
    row.pendingHandlers = false
  }
  return row
}

const fetchFrames = (h, node) =>
  frames(h.cluster, 78).filter(({ node: id }) => node === undefined || id === node)
const okResults = (m) => m.results.filter((outcome) => !outcome.error)
const failures = (m) => m.results.filter((outcome) => outcome.error)
const settled = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )
/** A retry configuration whose own delay is zero, so the pacing floors alone decide the wait. */
const NO_DELAY = { initialRetryTime: 0, factor: 0, multiplier: 1, maxRetryTime: 0 }
/** A logger that keeps every call, so a test reads what was logged and with which identity. */
const keepLog = () => {
  const lines = []
  const at = (level) => (message, extra) => lines.push({ level, message, ...extra })
  return { lines, error: at('error'), warn: at('warn'), info: at('info'), debug: at('debug') }
}
/**
 * @param {object} captured What `captureFinal` froze.
 * @param {() => Error|null} [guard] The closing predicate; it never refuses by default.
 * @returns {object} The `closing` option of `finishNode`, with the episode's own operation, which
 *   has no deadline, so it only ever ends by `finish`.
 */
const episode = (captured, guard = () => null) => ({
  guard,
  captured,
  operation: new ConsumerOperation(),
})
const SOCKET_EVENTS = ['data', 'end', 'error', 'close', 'timeout', 'connect']
/**
 * @param {object} socket A fake socket.
 * @returns {Record<string, number>} How many listeners it has for each event the client uses.
 */
const listeners = (socket) =>
  Object.fromEntries(SOCKET_EVENTS.map((name) => [name, socket.listenerCount(name)]))
/** What a closed data socket keeps: the client removes only these three listeners and keeps `error`, inert. */
const REMOVED_ON_CLOSE = { data: 0, end: 0, timeout: 0 }
const A0 = { topicId: A, partition: 0 }
const A1 = { topicId: A, partition: 1 }
const batchOf = (first, last, type) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: [type],
})

describe('share node socket integration', () => {
  let h
  afterEach(async () => {
    if (h) await h.close()
    h = null
    jest.restoreAllMocks()
  })

  test('waits for every node handler before fetching that node again', async () => {
    h = await harness({ nodes: 3 })
    const [n0, n1, n2] = h.members
    n0.want = [A0, A1]
    n1.want = [{ topicId: A, partition: 2 }]
    n2.want = [{ topicId: A, partition: 3 }]
    const values = (partition) => [`p${partition}-0`, `p${partition}-1`]
    h.scriptNodes('ShareFetch', {
      0: [fetchAnswer([acquire(0, 100, values(0)), acquire(1, 200, values(1))])],
      1: [fetchAnswer([acquire(2, 300, values(2))])],
      2: [fetchAnswer([acquire(3, 400, values(3))])],
    })
    const arrived = deferred()
    const held = deferred()
    n0.dispatch = async (rows) => {
      await Promise.all(
        rows.map(async (row) => {
          const { records } = row.walk.deliver
          records.forEach((message) =>
            n0.handled.push({ partition: row.partition, value: message.value.toString() })
          )
          if (row.partition === 0) {
            arrived.resolve()
            await held.promise
          }
          row.ledger.complete(
            records.map((message) => message.offset),
            'accept'
          )
        })
      )
    }
    const first0 = n0.loop.poll()
    const first1 = n1.loop.poll()
    const first2 = n2.loop.poll()
    await arrived.promise
    await Promise.all([first1, first2])
    await Promise.all([n1.loop.poll(), n2.loop.poll()])
    expect(n0.loop.poll()).toBe(first0)
    await flush()
    expect(fetchFrames(h, 0)).toHaveLength(1)
    expect(frames(h.cluster, 79)).toHaveLength(0)
    const acks = (frame) =>
      frame.fields.topics[0].partitions.map(({ partitionIndex, acknowledgementBatches }) => [
        partitionIndex,
        acknowledgementBatches,
      ])
    expect(acks(fetchFrames(h, 1)[1])).toEqual([[2, [batchOf(300, 301, 1)]]])
    expect(acks(fetchFrames(h, 2)[1])).toEqual([[3, [batchOf(400, 401, 1)]]])
    held.resolve()
    await first0
    await n0.loop.poll()
    expect(acks(fetchFrames(h, 0)[1])).toEqual([
      [0, [batchOf(100, 101, 1)]],
      [1, [batchOf(200, 201, 1)]],
    ])
    expect(frames(h.cluster, 79)).toHaveLength(0)
    const handled = [...n0.handled, ...n1.handled, ...n2.handled].map(({ value }) => value)
    expect(handled.sort()).toEqual(['p0-0', 'p0-1', 'p1-0', 'p1-1', 'p2-0', 'p2-1', 'p3-0', 'p3-1'])
  })

  test('forgets a revoked partition in the frame carrying its final acknowledgement', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    const model = sessionSet()
    h.script('ShareFetch', [
      (fields) => {
        model.apply(fields)
        return fetchAnswer([acquire(0, 9007199254740993n, ['x', 'y']), acquire(1, 500, ['z'])])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer([part({ partition: 0 }), part({ partition: 1 })])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer(model.has(A, 0) ? [acquire(0, 900, ['poison'])] : [])
      },
    ])
    await m.loop.poll()
    m.want = [A1]
    await m.loop.poll()
    await m.loop.poll()
    const [, piggyback, later] = fetchFrames(h)
    expect(piggyback.fields.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [batchOf('9007199254740993', '9007199254740994', 1)],
          },
          { partitionIndex: 1, acknowledgementBatches: [batchOf(500, 500, 1)] },
        ],
      },
    ])
    expect(piggyback.fields.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(later.fields.topics).toEqual([])
    expect(later.fields.forgottenTopicsData).toEqual([])
    expect(okResults(m).filter(({ partition }) => partition === 0)).toEqual([
      expect.objectContaining({
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
      }),
    ])
    expect(m.handled.map(({ value }) => value)).toEqual(['x', 'y', 'z'])
    expect(model.keys()).toEqual([`${A}:1`])
  })

  test.each(['topic-a', '__proto__'])(
    'forgets a newly listed acknowledgement-only partition immediately (%s)',
    async (name) => {
      h = await harness({
        names: new Map([
          [A, name],
          [B, 'topic-b'],
        ]),
      })
      const [m] = h.members
      m.want = [A1]
      await m.loop.poll()
      inject(h, m, A, 0, 40, 'a')
      inject(h, m, B, 0, 41, 'b')
      await m.loop.poll()
      const [, piggyback] = fetchFrames(h)
      expect(piggyback.fields.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(40, 40, 1)] }],
        },
        {
          topicId: B,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(41, 41, 1)] }],
        },
      ])
      expect(piggyback.fields.forgottenTopicsData).toEqual([
        { topicId: A, partitions: [0] },
        { topicId: B, partitions: [0] },
      ])
      expect(okResults(m).map(({ topicId, partition }) => [topicId, partition])).toEqual([
        [A, 0],
        [B, 0],
      ])
      expect([...m.session.model.keys()]).toEqual([`${A}:1`])
    }
  )

  test('forgets on the first fetch after a flush', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    const model = sessionSet()
    h.script('ShareFetch', [
      (fields) => {
        model.apply(fields)
        return fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer(
          model.has(A, 0)
            ? [acquire(0, 20, ['poison']), part({ partition: 1 })]
            : [part({ partition: 1 })]
        )
      },
    ])
    await m.loop.poll()
    m.want = [A1]
    await m.loop.acknowledge()
    expect(fetchFrames(h)).toHaveLength(1)
    const [flushed] = frames(h.cluster, 79)
    expect(flushed.fields).toEqual({
      groupId: 'group',
      memberId: 'member',
      shareSessionEpoch: 1,
      isRenewAck: false,
      topics: [
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 10, 1)] }],
        },
      ],
    })
    await m.loop.poll()
    const forget = fetchFrames(h)[1].fields
    expect(forget.shareSessionEpoch).toBe(2)
    expect(forget.topics).toEqual([])
    expect(forget.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(m.handled.map(({ value }) => value)).toEqual(['a'])
  })

  describe('renewal then terminal decision', () => {
    /** Starts a round whose handler holds record 30 of partition 0 until `proceed` is called. */
    const holdRound = async (m) => {
      const started = deferred()
      const proceed = deferred()
      const captured = {}
      m.dispatch = async (rows) => {
        captured.rows = rows
        started.resolve()
        await proceed.promise
        rows[0].ledger.complete(['30'], 'release')
      }
      const round = m.loop.poll()
      await started.promise
      return { captured, proceed: proceed.resolve, round }
    }

    test('forgets on the first eligible fetch after a renewal and terminal decision', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      const model = sessionSet()
      h.script('ShareFetch', [
        (fields) => {
          model.apply(fields)
          return fetchAnswer([acquire(0, 30, ['r']), part({ partition: 1 })])
        },
        (fields) => {
          model.apply(fields)
          return fetchAnswer(
            model.has(A, 0)
              ? [acquire(0, 70, ['poison']), part({ partition: 1 })]
              : [part({ partition: 0 }), part({ partition: 1 })]
          )
        },
      ])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const { captured, proceed, round } = await holdRound(m)
      const renewal = settled(
        m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['30'] }] })
      )
      await gate.arrived()
      m.want = [A1]
      expect(frames(h.cluster, 79)[0].fields).toMatchObject({
        shareSessionEpoch: 1,
        isRenewAck: true,
        topics: [
          {
            topicId: A,
            partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(30, 30, 4)] }],
          },
        ],
      })
      expect(fetchFrames(h)).toHaveLength(1)
      gate.release(ackAnswer([{ partition: 0 }]))
      expect((await renewal).value).toMatchObject({ lockDuration: 15000 })
      expect(fetchFrames(h)).toHaveLength(1)
      proceed()
      await round
      await m.loop.poll()
      const next = fetchFrames(h)[1].fields
      expect(next.shareSessionEpoch).toBe(2)
      expect(next.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(30, 30, 2)] }],
        },
      ])
      expect(next.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
      expect(model.keys()).toEqual([`${A}:1`])
      expect(okResults(m).map(({ type }) => type)).toEqual(['renew', 'release'])
    })

    test('flushing the terminal decision first lets the next fetch forget without acknowledgements', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 30, ['r']), part({ partition: 1 })])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const { captured, proceed, round } = await holdRound(m)
      const renewal = settled(
        m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['30'] }] })
      )
      await gate.arrived()
      m.want = [A1]
      gate.release(ackAnswer([{ partition: 0 }]))
      await renewal
      proceed()
      await round
      await m.loop.acknowledge()
      const terminal = frames(h.cluster, 79)[1].fields
      expect(terminal).toMatchObject({ shareSessionEpoch: 2, isRenewAck: false })
      expect(terminal.topics[0].partitions[0].acknowledgementBatches).toEqual([batchOf(30, 30, 2)])
      await m.loop.poll()
      const forget = fetchFrames(h)[1].fields
      expect(forget.topics).toEqual([])
      expect(forget.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    })
  })

  test('never forgets another held acquisition of the same partition', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })])])
    await m.loop.poll()
    const held = inject(h, m, A, 0, 20, 'b', { decide: false })
    m.want = [A1]
    await m.loop.poll()
    await m.loop.poll()
    const [, firstFrame, secondFrame] = fetchFrames(h)
    expect(firstFrame.fields.topics).toEqual([
      {
        topicId: A,
        partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 10, 1)] }],
      },
    ])
    expect(firstFrame.fields.forgottenTopicsData).toEqual([])
    expect(secondFrame.fields.topics).toEqual([])
    expect(secondFrame.fields.forgottenTopicsData).toEqual([])
    held.ledger.complete(['20'], 'accept')
    held.pendingHandlers = false
    await m.loop.poll()
    const last = fetchFrames(h)[3].fields
    expect(last.topics).toEqual([
      {
        topicId: A,
        partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(20, 20, 1)] }],
      },
    ])
    expect(last.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    const written = frames(h.cluster).length
    const first = inject(h, m, A, 0, 31, 'c')
    const second = inject(h, m, A, 0, 32, 'd')
    const tickets = [first, second].map((row) => ({
      ledger: row.ledger,
      ticket: row.ledger.prepare({ token: row.token }).ticket,
    }))
    expect(() => m.loop.buildFetch([A1], tickets, new Set())).toThrow(
      'One acquisition per partition per request is required'
    )
    expect(frames(h.cluster)).toHaveLength(written)
  })

  test.each([
    ['default wait and record limit', {}, { maxWaitMs: 500, minBytes: 1, shareAcquireMode: 1 }],
    [
      'zero wait and batch optimized',
      { maxWaitTimeInMs: 0, minBytes: 0, acquireMode: 'batch_optimized' },
      { maxWaitMs: 0, minBytes: 0, shareAcquireMode: 0 },
    ],
  ])(
    'uses acknowledgement requests for flush renewal and final (%s)',
    async (label, options, expected) => {
      h = await harness({ loop: options })
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 5, ['a', 'b', 'c'])]),
        fetchAnswer([acquire(0, 8, ['d'])]),
      ])
      await m.loop.poll()
      await m.loop.acknowledge()
      const started = deferred()
      const proceed = deferred()
      const captured = {}
      m.dispatch = async (rows) => {
        captured.rows = rows
        started.resolve()
        await proceed.promise
        rows[0].ledger.complete(['8'], 'accept')
      }
      const round = m.loop.poll()
      await started.promise
      await m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['8'] }] })
      proceed.resolve()
      await round
      await m.loop.finishNode()
      const fetches = fetchFrames(h).map(({ fields }) => fields)
      expect(fetches.map(({ shareSessionEpoch }) => shareSessionEpoch)).toEqual([0, 2])
      for (const fields of fetches) {
        expect(fields).toMatchObject({
          groupId: 'group',
          memberId: 'member',
          maxBytes: 1048576,
          maxRecords: 500,
          batchSize: 500,
          isRenewAck: false,
          ...expected,
        })
      }
      expect(fetches[0].topics).toEqual([
        { topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
      ])
      const acknowledgements = frames(h.cluster, 79).map(({ fields }) => fields)
      expect(acknowledgements.map(({ shareSessionEpoch }) => shareSessionEpoch)).toEqual([1, 3, -1])
      expect(acknowledgements.map(({ isRenewAck }) => isRenewAck)).toEqual([false, true, false])
      for (const fields of acknowledgements) {
        expect(Object.keys(fields).sort()).toEqual([
          'groupId',
          'isRenewAck',
          'memberId',
          'shareSessionEpoch',
          'topics',
        ])
      }
      expect(acknowledgements[0].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(5, 7, 1),
      ])
      expect(acknowledgements[1].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(8, 8, 4),
      ])
      expect(acknowledgements[2].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(8, 8, 1),
      ])
    }
  )

  describe('empty-round pacing', () => {
    test.each([
      ['default wait', { maxWaitTimeInMs: 500, minBytes: 1 }],
      ['zero wait and zero minimum bytes', { maxWaitTimeInMs: 0, minBytes: 0 }],
    ])('paces five-millisecond empty rounds at fifty milliseconds (%s)', async (label, options) => {
      h = await harness({ loop: options })
      const [m] = h.members
      m.want = [A0]
      m.holdSleeps()
      h.clock.time = 100
      h.script('ShareFetch', [
        () => {
          h.clock.time = 105
          return fetchAnswer([part({ partition: 0 })])
        },
        fetchAnswer([acquire(0, 10, ['x'])]),
      ])
      let ended = false
      const first = settled(m.loop.poll()).then((outcome) => {
        ended = true
        return outcome
      })
      expect((await m.nextTimer()).delay).toBe(45)
      expect(ended).toBe(false)
      expect(fetchFrames(h)).toHaveLength(1)

      h.clock.time = 149
      ;(await m.nextTimer()).release()
      expect((await m.nextTimer()).delay).toBe(1)
      expect(ended).toBe(false)
      expect(fetchFrames(h)).toHaveLength(1)

      h.clock.time = 150
      ;(await m.nextTimer()).release()
      expect((await first).value).toMatchObject({ sentAt: 100, answeredAt: 105, empty: true })
      expect(m.sleeps).toEqual([45, 1])
      expect(fetchFrames(h)).toHaveLength(1)
      const second = await m.loop.poll()
      expect(second).toMatchObject({ sentAt: 150, empty: false })
      expect(m.sleeps).toEqual([45, 1])
      expect(m.timers).toEqual([])
      expect(fetchFrames(h)).toHaveLength(2)
      expect(fetchFrames(h)[0].fields).toMatchObject({
        maxWaitMs: options.maxWaitTimeInMs,
        minBytes: options.minBytes,
      })
    })

    test('a retired node authority ends a paced round and no later timer delivery writes', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      m.holdSleeps()
      h.clock.time = 100
      h.script('ShareFetch', [
        () => {
          h.clock.time = 105
          return fetchAnswer([part({ partition: 0 })])
        },
      ])
      const round = settled(m.loop.poll())
      const timer = await m.nextTimer()
      expect(m.operation.waiters.size).toBe(1)
      m.operation.retire(new Error('run closed'))
      expect((await round).error.message).toBe('run closed')
      expect(m.operation.waiters.size).toBe(0)
      expect((await settled(m.loop.poll())).error.message).toBe('run closed')
      h.clock.time = 400
      timer.release()
      await flush()
      expect(fetchFrames(h)).toHaveLength(1)
    })

    test('a gap or an error answer is not an empty round', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([part({ partition: 0, acquire: [acquired(10, 10)] })]),
        fetchAnswer([part({ partition: 0, code: 9 })]),
      ])
      const gap = await m.loop.poll()
      expect(gap.empty).toBe(false)
      expect(m.sleeps).toEqual([])
      const error = await m.loop.poll()
      expect(error).toMatchObject({ empty: false, backoff: true })
      expect(m.sleeps).toHaveLength(1)
      expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
    })

    test('empty writes are at least fifty milliseconds apart so a five second window holds at most one hundred', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const writes = []
      for (let i = 0; i < 101; i++) writes.push((await m.loop.poll()).sentAt)
      const gaps = writes.slice(1).map((at, i) => at - writes[i])
      expect(gaps.every((gap) => gap >= 50)).toBe(true)
      expect(writes.filter((at) => at < writes[0] + 5000).length).toBeLessThanOrEqual(100)
    })

    test('session-limit resets are at least 250 milliseconds apart so a five second window holds at most twenty', async () => {
      h = await harness({
        loop: { retry: { initialRetryTime: 0, factor: 0, multiplier: 1, maxRetryTime: 0 } },
      })
      const [m] = h.members
      m.want = [A0]
      h.script(
        'ShareFetch',
        Array.from({ length: 21 }, () => topError(133))
      )
      const writes = []
      for (let i = 0; i < 21; i++) writes.push((await m.loop.poll()).sentAt)
      const gaps = writes.slice(1).map((at, i) => at - writes[i])
      expect(gaps.every((gap) => gap >= 250)).toBe(true)
      expect(writes.filter((at) => at < writes[0] + 5000).length).toBeLessThanOrEqual(20)
    })
  })

  test('limits record acquisitions independently per node', async () => {
    h = await harness({ nodes: 3, loop: { maxRecords: 7 } })
    const gates = h.members.map(() => deferred())
    const everyone = deferred()
    let held = 0
    h.members.forEach((m, i) => {
      m.want = [{ topicId: A, partition: i }]
      m.dispatch = async (rows) => {
        held += rows.reduce((sum, row) => sum + row.walk.deliver.records.length, 0)
        if (held === 21) everyone.resolve()
        await gates[i].promise
        for (const row of rows)
          row.ledger.complete(
            row.walk.deliver.records.map((message) => message.offset),
            'accept'
          )
      }
    })
    h.scriptNodes(
      'ShareFetch',
      Object.fromEntries(
        h.members.map((m, i) => [
          i,
          [
            fetchAnswer([
              acquire(
                i,
                100 * (i + 1),
                Array.from({ length: 7 }, (_, j) => `n${i}-${j}`)
              ),
            ]),
          ],
        ])
      )
    )
    const rounds = h.members.map((m) => m.loop.poll())
    await everyone.promise
    expect(held).toBe(21)
    for (const frame of fetchFrames(h)) {
      expect(frame.fields).toMatchObject({ maxRecords: 7, batchSize: 7, shareAcquireMode: 1 })
    }
    gates[0].resolve()
    await rounds[0]
    await h.members[0].loop.poll()
    expect(fetchFrames(h, 0)).toHaveLength(2)
    expect(fetchFrames(h, 0)[1].fields).toMatchObject({ maxRecords: 7, batchSize: 7 })
    expect(fetchFrames(h, 1)).toHaveLength(1)
    expect(fetchFrames(h, 2)).toHaveLength(1)
    gates[1].resolve()
    gates[2].resolve()
    await Promise.all(rounds)
  })

  test('does not truncate an aligned batch that exceeds the batch-optimized target', async () => {
    h = await harness({ loop: { acquireMode: 'batch_optimized', maxRecords: 7 } })
    const [m] = h.members
    m.want = [A0]
    const values = Array.from({ length: 8 }, (_, i) => `v${i}`)
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, values)])])
    await m.loop.poll()
    expect(fetchFrames(h)[0].fields).toMatchObject({
      maxRecords: 7,
      batchSize: 7,
      shareAcquireMode: 0,
    })
    expect(m.handled.map(({ value }) => value)).toEqual(values)
    await m.loop.poll()
    expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      batchOf(10, 17, 1),
    ])
  })

  describe('leader movement', () => {
    const endpoints = [{ nodeId: 2, host: 'node2', port: 9092, rack: null }]
    const hint = (leaderId, leaderEpoch) => ({ leaderId, leaderEpoch })
    /** Topic A with two partitions led by node 0 at epoch 5, as the scripted wire serves it. */
    const served = () => ({
      'topic-a': {
        topicId: A,
        partitions: [
          { leader: 0, epoch: 5 },
          { leader: 0, epoch: 5 },
        ],
      },
    })
    const moved = (code, leader, extra = {}) =>
      fetchAnswer([part({ partition: 0, code, leader }), part({ partition: 1 })], extra)

    test.each([6, 74])(
      'code %s uses the leader hint through the real cluster and retires the old node acquisition',
      async (code) => {
        h = await harness({ nodes: 3 })
        const [n0, , n2] = h.members
        n0.want = [A0, A1]
        const real = await h.realCluster(served())
        h.scriptNodes('ShareFetch', {
          0: [
            fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
            moved(code, hint(2, 8), { nodeEndpoints: endpoints }),
          ],
        })
        await n0.loop.poll()
        inject(h, n0, A, 0, 99, 'held', { decide: false })
        await n0.loop.poll()
        expect(real.cluster.cachedLeader('topic-a', 0)).toBe(2)
        expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(8)
        expect(real.cluster.cachedLeader('topic-a', 1)).toBe(0)
        expect(real.refreshes()).toBe(0)
        expect(n0.moves).toEqual([
          { topic: 'topic-a', topicId: A, partition: 0, leaderId: 2, reason: 'leader-moved' },
        ])
        expect(failures(n0)).toHaveLength(1)
        expect(failures(n0)[0].error.reason).toBe('session-lost')
        expect(failures(n0)[0]).toMatchObject({ firstOffset: '99', lastOffset: '99' })
        expect(okResults(n0).filter(({ partition }) => partition === 0)).toHaveLength(1)
        n0.want = [A1]
        n2.want = [A0]
        await n2.loop.poll()
        await n0.loop.poll()
        expect(fetchFrames(h, 2)[0].fields).toMatchObject({
          shareSessionEpoch: 0,
          topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
        })
        expect(fetchFrames(h, 0)[2].fields.forgottenTopicsData).toEqual([
          { topicId: A, partitions: [0] },
        ])
      }
    )

    test.each([
      ['no hint', hint(-1, -1)],
      ['a stale hint', hint(2, 5)],
    ])('falls back to one metadata refresh of the real cluster after %s', async (label, leader) => {
      h = await harness({ nodes: 3 })
      const [n0] = h.members
      n0.want = [A0]
      const real = await h.realCluster(served())
      h.scriptNodes('ShareFetch', {
        0: [
          fetchAnswer([part({ partition: 0 })]),
          fetchAnswer([part({ partition: 0, code: 6, leader })], { nodeEndpoints: endpoints }),
        ],
      })
      await n0.loop.poll()
      real.table.get('topic-a').partitions[0] = { leader: 2, epoch: 9 }
      await n0.loop.poll()
      expect(real.refreshes()).toBe(1)
      expect(real.cluster.cachedLeader('topic-a', 0)).toBe(2)
      expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(9)
      expect(n0.moves).toEqual([
        { topic: 'topic-a', topicId: A, partition: 0, leaderId: 2, reason: 'leader-moved' },
      ])
    })

    test('a session reset while the leader lookup awaits keeps the provider from installing routing', async () => {
      h = await harness({ nodes: 3 })
      const [n0] = h.members
      n0.want = [A0, A1]
      const real = await h.realCluster(served())
      h.scriptNodes('ShareFetch', {
        0: [
          fetchAnswer(
            [
              part({ partition: 0, code: 6, leader: hint(2, 8) }),
              acquire(1, 20, ['b']),
              part({ partition: 2, code: 6, leader: hint(1, 9) }),
            ],
            {
              nodeEndpoints: endpoints,
            }
          ),
        ],
      })
      h.script('ShareAcknowledge', [topError(122)])
      const original = real.cluster.applyLeaderHint.bind(real.cluster)
      const entered = deferred()
      const release = deferred()
      jest.spyOn(real.cluster, 'applyLeaderHint').mockImplementationOnce(async (...args) => {
        entered.resolve()
        await release.promise
        return original(...args)
      })
      const captured = {}
      n0.dispatch = async (rows) => {
        captured.rows = rows
      }
      const round = settled(n0.loop.poll())
      try {
        await entered.promise
        const renewal = await settled(
          n0.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['20'] }] })
        )
        expect(renewal.error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
        expect(renewal.error.reason).toBe('session-lost')
        expect(n0.resets).toHaveLength(1)
        release.resolve()
        expect(await round).toMatchObject({
          value: { reset: false, closed: false, backoff: true },
        })
        expect(real.cluster.cachedLeader('topic-a', 0)).toBe(0)
        expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(5)
        expect(n0.moves).toEqual([])
        expect(real.refreshes()).toBe(0)
        expect(real.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
        expect(n0.sleeps[0]).toBeGreaterThanOrEqual(50)
        await n0.loop.poll()
        const reopened = fetchFrames(h)[1]
        expect(reopened.fields.shareSessionEpoch).toBe(0)
        expect(
          reopened.fields.topics
            .flatMap((topic) => topic.partitions)
            .every(({ acknowledgementBatches }) => acknowledgementBatches.length === 0)
        ).toBe(true)
      } finally {
        release.resolve()
        await round
      }
    })
  })

  describe('topic identity and retriable partition errors', () => {
    const served = () => ({
      'topic-a': {
        topicId: A,
        partitions: [
          { leader: 0, epoch: 5 },
          { leader: 0, epoch: 5 },
        ],
      },
    })

    test.each([3, 100, 103, 9, 56, 78, 7])(
      'partition code %s refreshes the real cluster once and keeps the sibling delivery',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0, A1]
        const real = await h.realCluster(served())
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, code }), acquire(1, 10, ['sib'])]),
        ])
        const result = await m.loop.poll()
        expect(result.backoff).toBe(true)
        expect(real.refreshes()).toBe(1)
        expect(real.recreated).toEqual([])
        expect(m.handled.map(({ value }) => value)).toEqual(['sib'])
        expect(m.moves).toEqual([])
        expect(m.fatals).toEqual([])
        expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
      }
    )

    test('a recreated topic retires the old identity before the new one is installed', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const real = await h.realCluster(served())
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([part({ partition: 0, code: 100 })]),
      ])
      await m.loop.poll()
      inject(h, m, A, 0, 20, 'old', { decide: false })
      const order = []
      const observe = (step) =>
        order.push({
          step,
          cachedTopicId: real.cluster.topicId('topic-a'),
          model: [...m.session.model.keys()],
        })
      real.cluster.onTopicRecreated(() => observe('topic recreated'))
      const notified = m.loop.onResults
      m.loop.onResults = (results, row) => {
        if (results.some((outcome) => outcome.error)) observe('old identity retired')
        notified(results, row)
      }
      real.table.set('topic-a', {
        topicId: B,
        partitions: [
          { leader: 0, epoch: 1 },
          { leader: 0, epoch: 1 },
        ],
      })
      await m.loop.poll()
      expect(real.refreshes()).toBe(1)
      expect(real.recreated).toHaveLength(1)
      expect(order).toEqual([
        { step: 'topic recreated', cachedTopicId: B, model: [`${A}:0`] },
        { step: 'old identity retired', cachedTopicId: B, model: [`${A}:0`] },
      ])
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ firstOffset: '20', lastOffset: '20' })
      expect(failures(m)[0].error.reason).toBe('session-lost')
      expect(m.loop.cohorts.size).toBe(0)
      expect(fetchFrames(h)).toHaveLength(2)
      m.want = [{ topicId: B, partition: 0 }]
      await m.loop.poll()
      const next = fetchFrames(h)[2].fields
      expect(next.topics).toEqual([
        { topicId: B, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
      ])
      expect(next.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
      expect([...m.session.model.keys()]).toEqual([`${B}:0`])
    })

    test('a metadata refresh finished after retirement installs nothing and the replacement keeps its state', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const real = await h.realCluster(served())
      h.script('ShareFetch', [
        fetchAnswer([part({ partition: 0, code: 9 })]),
        fetchAnswer([part({ partition: 0, code: 9 })]),
      ])
      const gate = h.hold({ api: 'Metadata' })
      const round = settled(m.loop.poll())
      await gate.arrived()
      m.operation.retire(new Error('run closed'))
      expect((await round).error.message).toBe('run closed')
      const next = h.runOn({ node: m.node, pool: m.pool, broker: m.broker })
      next.want = [...m.want]
      await next.loop.poll()
      expect(real.refreshes()).toBe(2)
      const before = {
        leader: real.cluster.cachedLeader('topic-a', 0),
        epoch: real.cluster.leaderEpoch('topic-a', 0),
        frames: fetchFrames(h).length,
        sleeps: next.sleeps.length,
        model: [...next.session.model.keys()],
      }
      real.table.get('topic-a').partitions[0] = { leader: 2, epoch: 9 }
      gate.release()
      await flush()
      expect(real.cluster.cachedLeader('topic-a', 0)).toBe(before.leader)
      expect(real.cluster.leaderEpoch('topic-a', 0)).toBe(before.epoch)
      expect(before.leader).toBe(0)
      expect(m.moves).toEqual([])
      expect(next.moves).toEqual([])
      expect(fetchFrames(h)).toHaveLength(before.frames)
      expect(next.sleeps).toHaveLength(before.sleeps)
      expect([...next.session.model.keys()]).toEqual(before.model)
    })
  })

  describe('fatal and unknown failures', () => {
    test.each([29, 2, 42])(
      'partition code %s is fatal after its sibling acquisition was registered and released',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0, A1]
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, code }), acquire(1, 10, ['sib'])]),
        ])
        const failure = (await settled(m.loop.poll())).error
        expect(failure).toBeInstanceOf(KafkaJSProtocolError)
        expect(failure).toMatchObject({ code, retriable: false })
        expect(m.fatals).toEqual([failure])
        await flush()
        expect(fetchFrames(h)).toHaveLength(1)
        expect(m.handled).toEqual([])
        await m.loop.finishNode()
        const final = frames(h.cluster, 79)
        expect(final).toHaveLength(1)
        expect(final[0].fields).toMatchObject({ shareSessionEpoch: -1 })
        expect(final[0].fields.topics).toEqual([
          {
            topicId: A,
            partitions: [{ partitionIndex: 1, acknowledgementBatches: [batchOf(10, 10, 2)] }],
          },
        ])
        expect(okResults(m)).toEqual([expect.objectContaining({ type: 'release', partition: 1 })])
        expect(failures(m)).toEqual([])
      }
    )

    describe('a fatal partition error beside a registered sibling acquisition', () => {
      /** Fails partition 0 fatally in an answer that also acquires offset 10 of partition 1. */
      const fatalWithSibling = async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0, A1]
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, code }), acquire(1, 10, ['sib'])]),
        ])
        const failure = (await settled(m.loop.poll())).error
        expect(failure).toMatchObject({ code, retriable: false })
        expect(m.fatals).toEqual([failure])
        expect(m.handled).toEqual([])
        await flush()
        return m
      }
      const released = { partition: 1, firstOffset: '10', lastOffset: '10', type: 'release' }
      const retained = (m) => [m.loop.cohorts.size, m.loop.exchanges.size, m.session.queue.length]

      test.each([29, 2])('code %s settles the sibling once through retirement', async (code) => {
        const m = await fatalWithSibling(code)
        m.loop.dispose(new Error('node closed'))
        m.loop.dispose(new Error('node closed again'))
        expect(failures(m)).toEqual([expect.objectContaining(released)])
        expect(failures(m)[0].error.reason).toBe('session-lost')
        expect(okResults(m)).toEqual([])
        expect(retained(m)).toEqual([0, 0, 0])
        expect(frames(h.cluster, 79)).toHaveLength(0)
        expect(fetchFrames(h)).toHaveLength(1)
      })

      test.each([29, 2])(
        'code %s releases the sibling once through a captured close',
        async (code) => {
          const m = await fatalWithSibling(code)
          const captured = m.loop.captureFinal()
          expect(captured.prepared).toHaveLength(1)
          await m.loop.finishNode({ closing: episode(captured) })
          const finals = frames(h.cluster, 79)
          expect(finals).toHaveLength(1)
          expect(finals[0].fields.topics).toEqual([
            {
              topicId: A,
              partitions: [{ partitionIndex: 1, acknowledgementBatches: [batchOf(10, 10, 2)] }],
            },
          ])
          expect(okResults(m)).toEqual([expect.objectContaining(released)])
          expect(failures(m)).toEqual([])
          expect(retained(m)).toEqual([0, 0, 0])
        }
      )

      test.each([29, 2])(
        'code %s does not confirm the release when the close gets no answer',
        async (code) => {
          const m = await fatalWithSibling(code)
          const captured = m.loop.captureFinal()
          const gate = h.hold({ api: 'ShareAcknowledge' })
          const operation = new ConsumerOperation({ deadlineAt: 2000, now: () => h.clock.time })
          try {
            const closing = m.loop.finishNode({
              closing: { guard: operation.guard, captured, operation },
            })
            await gate.arrived()
            expect(failures(m)).toEqual([])
            expect(okResults(m)).toEqual([])
            h.clock.time = 2000
            expect(operation.guard()).toBeInstanceOf(Error)
            await closing
            expect(failures(m)).toEqual([expect.objectContaining(released)])
            expect(failures(m)[0].error.reason).toBe('session-lost')
            expect(okResults(m)).toEqual([])
            expect(retained(m)).toEqual([0, 0, 0])
            expect(frames(h.cluster, 79)).toHaveLength(1)
          } finally {
            operation.finish()
          }
        }
      )
    })

    test('a top-level deleted-topic -1 advances the epoch under backoff and never resets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(-1)])
      const result = await m.loop.poll()
      expect(result).toMatchObject({ backoff: true, reset: false })
      expect(m.resets).toEqual([])
      expect(m.session.epoch).toBe(1)
      expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.shareSessionEpoch).toBe(1)
    })

    test('an unknown partition error -1 is logged, paced and retried while siblings deliver', async () => {
      const log = keepLog()
      h = await harness({ loop: { logger: log, retry: NO_DELAY } })
      const [m] = h.members
      m.want = [A0, A1]
      const writes = []
      h.script('ShareFetch', [
        () => {
          writes.push(h.clock.time)
          return fetchAnswer([part({ partition: 0, code: -1 }), acquire(1, 10, ['sib'])])
        },
        () => {
          writes.push(h.clock.time)
        },
      ])
      const result = await m.loop.poll()
      expect(result.backoff).toBe(true)
      expect(m.handled.map(({ value }) => value)).toEqual(['sib'])
      expect(m.fatals).toEqual([])
      expect(log.lines.filter(({ level }) => level === 'error')).toEqual([
        {
          level: 'error',
          message: 'Share fetch partition failed',
          partition: 0,
          error: 'The server experienced an unexpected error when processing the request',
        },
      ])
      expect(m.sleeps).toEqual([50])
      await m.loop.poll()
      expect(writes).toEqual([1000, 1050])
      const retried = fetchFrames(h)[1].fields
      expect(retried.shareSessionEpoch).toBe(1)
      expect(retried.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 1, acknowledgementBatches: [batchOf(10, 10, 1)] }],
        },
      ])
      expect(retried.forgottenTopicsData).toEqual([])
    })

    test('runtime 35 names ShareFetch and ends the run without a retry frame', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(35)])
      const failure = (await settled(m.loop.poll())).error
      expect(failure).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
      expect(failure).toMatchObject({
        apiKey: 78,
        apiName: 'ShareFetch',
        reason: 'refused-by-broker',
      })
      expect(m.fatals).toEqual([failure])
      await flush()
      expect(fetchFrames(h)).toHaveLength(1)
    })

    test('runtime 30 is a fatal authorization failure', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(30)])
      const failure = (await settled(m.loop.poll())).error
      expect(failure).toMatchObject({ code: 30, retriable: false })
      expect(m.fatals).toEqual([failure])
    })

    test('runtime 25 goes to the membership with the issued token', async () => {
      const token = Symbol('membership')
      const membership = { onMembershipError: jest.fn() }
      h = await harness({ loop: { membership, authority: { membershipToken: token } } })
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(25)])
      await m.loop.poll()
      expect(membership.onMembershipError).toHaveBeenCalledTimes(1)
      const [error, issued] = membership.onMembershipError.mock.calls[0]
      expect(error).toMatchObject({ code: 25 })
      expect(issued).toBe(token)
    })
  })

  describe('acknowledgement fields', () => {
    test.each([
      ['ack 121', 121, 'rejected-by-broker'],
      ['ack 6', 6, 'session-lost'],
      ['ack 74', 74, 'session-lost'],
      ['ack 100', 100, 'rejected-by-broker'],
      ['ack 133', 133, 'rejected-by-broker'],
      ['an unlisted ack code', 7, 'rejected-by-broker'],
    ])('fetch success with %s settles %s without a session reset', async (label, ack, reason) => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([part({ partition: 0, ack })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.reason).toBe(reason)
      expect(failures(m)[0].error.cause).toMatchObject({ code: ack })
      expect(okResults(m)).toEqual([])
      expect(m.resets).toEqual([])
      expect(m.session.epoch).toBe(2)
    })

    test('a rejected acknowledgement does not stop the same answer from delivering a new acquisition', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([acquire(0, 20, ['new'], { ack: 121 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)[0].error.cause).toMatchObject({ code: 121 })
      expect(m.handled.map(({ value }) => value)).toEqual(['a', 'new'])
    })

    test('fetch error 6 with a confirmed acknowledgement confirms once and moves the leader', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.metadata.cachedLeader.mockReturnValue(2)
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })]),
        fetchAnswer([
          part({ partition: 0, code: 6, ack: 0, leader: { leaderId: 2, leaderEpoch: 8 } }),
          part({ partition: 1 }),
        ]),
      ])
      await m.loop.poll()
      inject(h, m, A, 0, 99, 'held', { decide: false })
      await m.loop.poll()
      expect(okResults(m).filter(({ partition }) => partition === 0)).toHaveLength(1)
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.reason).toBe('session-lost')
      expect(m.moves).toHaveLength(1)
    })

    test('a forgotten partition stays in the settlement set of its original request', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })]),
        fetchAnswer([part({ partition: 0, ack: 121 }), part({ partition: 1 })]),
      ])
      await m.loop.poll()
      m.want = [A1]
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.forgottenTopicsData).toEqual([
        { topicId: A, partitions: [0] },
      ])
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.cause).toMatchObject({ code: 121 })
    })

    test('an omitted acknowledgement partition fails its ticket without stranding sent work', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
        fetchAnswer([part({ partition: 1 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ partition: 0, firstOffset: '10', lastOffset: '10' })
      expect(failures(m)[0].error.reason).toBe('rejected-by-broker')
      expect(failures(m)[0].error.cause.message).toMatch(/omitted a partition/)
      expect(okResults(m)).toEqual([expect.objectContaining({ partition: 1, firstOffset: '20' })])
      await m.loop.poll()
      expect(fetchFrames(h)[2].fields.topics).toEqual([])
      expect(m.results).toHaveLength(2)
    })

    test('a repeated response partition fails that ticket and settles its siblings normally', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
        fetchAnswer([part({ partition: 0 }), part({ partition: 1 }), part({ partition: 1 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ partition: 1 })
      expect(failures(m)[0].error.cause.message).toMatch(/repeated a partition/)
      expect(okResults(m)).toEqual([expect.objectContaining({ partition: 0 })])
    })
  })

  describe('unawaited renewal', () => {
    /**
     * One acquisition whose handler starts a renewal, decides the record and returns without
     * awaiting it. Captures flush eligibility at renewal settlement, before the queued terminal
     * preparation clears the ledger's unsent-decision marker.
     * @param {object} answer The held renewal's response.
     * @returns {Promise<object>} The run, captured renewal state, queued fetch and prepare spy.
     */
    const renewing = async (answer) => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const prepare = jest.spyOn(ShareAckLedger.prototype, 'prepare')
      const captured = {}
      const onResults = m.loop.onResults
      m.loop.onResults = (results, row) => {
        if (results.some(({ type }) => type === 'renew'))
          captured.needsFlush = row.ledger.needsFlush(9000)
        onResults(results, row)
      }
      m.dispatch = async (rows) => {
        captured.row = rows[0]
        captured.renewal = settled(
          m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10'] }] })
        )
        await new Promise(setImmediate)
        rows[0].ledger.complete(['10'], 'accept')
      }
      await m.loop.poll()
      await gate.arrived()
      const next = m.loop.poll()
      await flush()
      expect(prepare).toHaveBeenCalledTimes(1)
      expect(fetchFrames(h)).toHaveLength(1)
      gate.release(answer)
      return { m, captured, next, prepare }
    }

    test('settles an unawaited renewal before preparing terminal work', async () => {
      const { m, captured, next, prepare } = await renewing(
        ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 20000 })
      )
      expect((await captured.renewal).value).toMatchObject({ lockDuration: 20000 })
      expect(m.results.filter((outcome) => outcome.type === 'accept')).toEqual([])
      expect(captured.needsFlush).toBe(false)
      expect(captured.row.walk.deliver.deliveryCounts[0]).toBe(1)
      await next
      expect(prepare).toHaveBeenCalledTimes(2)
      expect(fetchFrames(h)[1].fields).toMatchObject({ shareSessionEpoch: 2 })
      expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 10, 1),
      ])
    })

    test('a renewal rejected with 121 fails with the compact error and keeps the old deadline', async () => {
      const { m, captured, next } = await renewing(ackAnswer([{ partition: 0, code: 121 }]))
      const failure = (await captured.renewal).error
      expect(failure).toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(failure.reason).toBe('rejected-by-broker')
      expect(failure.cause).toMatchObject({ code: 121 })
      expect(captured.needsFlush).toBe(true)
      await next
      expect(
        m.results.filter((outcome) => outcome.type === 'accept' && !outcome.error)
      ).toHaveLength(1)
    })
  })

  describe('acknowledgement backoff', () => {
    test.each([
      ['code 133', topError(133)],
      ['an unknown error', topError(-1)],
    ])(
      'a flush answered with %s delays every later request of the node by the classified wait',
      async (label, answer) => {
        h = await harness({ loop: { retry: NO_DELAY } })
        const [m] = h.members
        m.want = [A0]
        m.holdSleeps()
        const writes = []
        h.script('ShareFetch', [
          fetchAnswer([acquire(0, 10, ['a', 'b'])]),
          () => {
            writes.push(['ShareFetch', h.clock.time])
            return fetchAnswer([acquire(0, 20, ['c'])])
          },
        ])
        h.script('ShareAcknowledge', [
          () => {
            writes.push(['ShareAcknowledge', h.clock.time])
            return answer
          },
        ])
        await m.loop.poll()
        const flushed = settled(m.loop.acknowledge())
        const timer = await m.nextTimer()
        expect(timer.delay).toBe(50)
        const next = settled(m.loop.poll())
        await flush()
        expect(fetchFrames(h)).toHaveLength(1)
        expect(frames(h.cluster, 79)).toHaveLength(1)
        h.clock.time += timer.delay
        timer.release()
        expect((await flushed).value).toMatchObject({ backoff: true, reset: false })
        expect((await next).value).toMatchObject({ empty: false })
        expect(writes).toEqual([
          ['ShareAcknowledge', 1000],
          ['ShareFetch', 1050],
        ])
        expect(failures(m).map(({ error }) => error.reason)).toEqual(['rejected-by-broker'])
        expect(fetchFrames(h)[1].fields.topics).toEqual([])
      }
    )

    test('retiring the node ends the backoff wait inside the FIFO slot and releases the close queued behind it', async () => {
      h = await harness({ loop: { retry: NO_DELAY } })
      const [m] = h.members
      m.want = [A0]
      m.holdSleeps()
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      h.script('ShareAcknowledge', [topError(133)])
      await m.loop.poll()
      const flushed = settled(m.loop.acknowledge())
      expect((await m.nextTimer()).delay).toBe(50)
      const closing = settled(m.loop.finishNode())
      await flush()
      expect(frames(h.cluster, 79)).toHaveLength(1)
      m.operation.retire(new Error('closing budget spent'))
      expect((await flushed).error.message).toBe('closing budget spent')
      expect((await closing).error.message).toBe('closing budget spent')
      expect(m.timers).toHaveLength(1)
      expect(frames(h.cluster, 79)).toHaveLength(1)
      expect([m.operation.waiters.size, m.operation.disposers.size]).toEqual([0, 0])
      expect([m.session.queue.length, m.loop.exchanges.size, m.loop.cohorts.size]).toEqual([
        0, 0, 0,
      ])
      expect(m.session.closed).toBe(true)
    })
  })

  describe('requests with nothing to send', () => {
    test('a node with nothing wanted and nothing modelled writes no fetch', async () => {
      h = await harness()
      const [m] = h.members
      m.want = []
      const result = await m.loop.poll()
      expect(result.empty).toBe(true)
      expect(frames(h.cluster)).toHaveLength(0)
      expect(m.session.epoch).toBe(0)
    })

    test('a flush with no decided acquisition writes no acknowledgement', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      await m.loop.poll()
      const result = await m.loop.acknowledge()
      expect(result.empty).toBe(true)
      expect(frames(h.cluster, 79)).toHaveLength(0)
      expect(m.session.epoch).toBe(1)
    })

    test('an acknowledgement on a session that never opened is refused locally', async () => {
      h = await harness()
      const [m] = h.members
      await expect(m.loop.acknowledge()).rejects.toThrow('established')
      expect(frames(h.cluster)).toHaveLength(0)
    })
  })

  describe('closing a node', () => {
    test('sends no final for a node that never opened', async () => {
      h = await harness()
      const [m] = h.members
      m.want = []
      await m.loop.finishNode()
      expect(frames(h.cluster)).toHaveLength(0)
      expect(m.session.closed).toBe(true)
      await expect(m.loop.poll()).rejects.toThrow('closing')
    })

    test('closes an established last-unwanted node once with its terminal tickets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      m.want = []
      await m.loop.finishNode()
      await m.loop.finishNode()
      expect(fetchFrames(h)).toHaveLength(1)
      const finals = frames(h.cluster, 79)
      expect(finals).toHaveLength(1)
      expect(finals[0].fields).toMatchObject({ shareSessionEpoch: -1, isRenewAck: false })
      expect(finals[0].fields.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 11, 1)] }],
        },
      ])
      expect(okResults(m)).toEqual([
        expect.objectContaining({ firstOffset: '10', lastOffset: '11', type: 'accept' }),
      ])
      await expect(m.loop.poll()).rejects.toThrow('closing')
      expect(frames(h.cluster)).toHaveLength(2)
    })

    test.each([
      [122, 'session-lost'],
      [123, 'session-lost'],
      [133, 'rejected-by-broker'],
      [35, 'rejected-by-broker'],
      [-1, 'rejected-by-broker'],
    ])(
      'a final answered with %s keeps the node terminal and fails only its tickets',
      async (code, reason) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
        h.script('ShareAcknowledge', [topError(code)])
        await m.loop.poll()
        m.want = []
        await m.loop.finishNode()
        expect(m.resets).toEqual([])
        expect(m.sleeps).toEqual([])
        expect(m.session.closed).toBe(true)
        expect(frames(h.cluster, 79)).toHaveLength(1)
        expect(fetchFrames(h)).toHaveLength(1)
        expect(failures(m)).toHaveLength(1)
        expect(failures(m)[0].error.reason).toBe(reason)
        expect(okResults(m)).toEqual([])
      }
    )

    test('an idle close waits for a pending renewal before its final', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const renewal = deferred()
      m.dispatch = async (rows) => {
        renewal.resolve(m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10'] }] }))
        await new Promise(setImmediate)
        rows[0].ledger.complete(['10'], 'accept')
      }
      await m.loop.poll()
      await gate.arrived()
      m.want = []
      const closing = m.loop.finishNode()
      await flush()
      expect(frames(h.cluster, 79)).toHaveLength(1)
      gate.release(ackAnswer([{ partition: 0 }]))
      await closing
      await renewal.promise
      const written = frames(h.cluster, 79).map(({ fields }) => fields)
      expect(written.map(({ isRenewAck }) => isRenewAck)).toEqual([true, false])
      expect(written[1].shareSessionEpoch).toBe(-1)
      expect(written[1].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 10, 1),
      ])
    })
  })

  test('a final that gets no answer fails its tickets session-lost without reopening the node', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0]
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
    await m.loop.poll()
    const gate = h.hold({ api: 'ShareAcknowledge' })
    m.want = []
    const closing = m.loop.finishNode()
    await gate.arrived()
    ;[...m.pool.pool[1].requestQueue.inflight.values()][0].timeoutRequest()
    await closing
    expect(frames(h.cluster, 79)).toHaveLength(1)
    expect(m.resets).toEqual([])
    expect(m.session.closed).toBe(true)
    expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
    expect(okResults(m)).toEqual([])
  })

  describe('captured closing', () => {
    /** An established node holding one decided record, 10 and 11, of partition 0. */
    const established = async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      return m
    }

    test('uses only the captured closing predicate for a preserved terminal subset', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      expect(captured.prepared).toHaveLength(1)
      m.operation.retire(new Error('run closed'))
      expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
      await m.loop.finishNode({ closing: episode(captured) })
      const finals = frames(h.cluster, 79)
      expect(finals).toHaveLength(1)
      expect(finals[0].fields).toMatchObject({ shareSessionEpoch: -1 })
      expect(finals[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 11, 1),
      ])
      expect(okResults(m)).toEqual([
        expect.objectContaining({ firstOffset: '10', lastOffset: '11' }),
      ])
      expect(failures(m)).toEqual([])
      expect(m.session.closed).toBe(true)
    })

    test('a refused closing predicate writes no frame, settles the tickets not sent and leaves nothing registered', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      const queue = m.pool.pool[1].requestQueue
      await m.loop.finishNode({ closing: episode(captured, () => new Error('revoked')) })
      expect(frames(h.cluster, 79)).toHaveLength(0)
      expect(failures(m).map(({ error }) => error.reason)).toEqual(['not-sent'])
      expect(okResults(m)).toEqual([])
      expect([m.loop.cohorts.size, m.loop.exchanges.size, m.session.queue.length]).toEqual([
        0, 0, 0,
      ])
      expect([queue.pending.length, queue.inflight.size]).toEqual([0, 0])
      expect([m.operation.waiters.size, m.operation.disposers.size]).toEqual([0, 0])
      expect(m.session.closed).toBe(true)
    })

    test('a preserved terminal subset leaves the ineligible acquisition to its own invalidation', async () => {
      const m = await established()
      m.want = [A0, A1]
      const held = inject(h, m, A, 1, 50, 'held', { decide: false })
      held.dispatched = true
      const captured = m.loop.captureFinal()
      const eligible = [
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 11, 1)] }],
        },
      ]
      expect(captured.prepared).toHaveLength(1)
      expect(captured.fields.topics).toEqual(eligible)
      m.operation.retire(new Error('run closed'))
      m.loop.dispose(new Error('node closed'), { preserve: captured })
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ partition: 1, firstOffset: '50', lastOffset: '50' })
      expect(failures(m)[0].error.reason).toBe('session-lost')
      expect(okResults(m)).toEqual([])
      await m.loop.finishNode({ closing: episode(captured) })
      const finals = frames(h.cluster, 79)
      expect(finals).toHaveLength(1)
      expect(finals[0].fields).toMatchObject({ shareSessionEpoch: -1, isRenewAck: false })
      expect(finals[0].fields.topics).toEqual(eligible)
      expect(okResults(m)).toEqual([
        expect.objectContaining({
          partition: 0,
          firstOffset: '10',
          lastOffset: '11',
          type: 'accept',
        }),
      ])
      expect(failures(m)).toHaveLength(1)
      expect([m.loop.cohorts.size, m.loop.exchanges.size]).toEqual([0, 0])
    })

    test('an unanswered captured final settles when its closing operation expires and its transport is closed', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const operation = new ConsumerOperation({ deadlineAt: 2000, now: () => h.clock.time })
      try {
        m.operation.retire(new Error('run closed'))
        const closing = m.loop.finishNode({
          closing: { guard: operation.guard, captured, operation },
        })
        await gate.arrived()
        expect(frames(h.cluster, 79)).toHaveLength(1)
        expect(m.loop.exchanges.size).toBe(1)
        expect(failures(m)).toEqual([])
        expect(okResults(m)).toEqual([])
        expect(m.pool.pool[1].isConnected()).toBe(true)
        h.clock.time = 2000
        expect(operation.guard()).toBeInstanceOf(Error)
        await closing
        expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
        expect(okResults(m)).toEqual([])
        await until(() => !m.pool.pool[1].isConnected(), 'the unanswered final transport to close')
        expect([m.loop.exchanges.size, m.loop.cohorts.size]).toEqual([0, 0])
        expect(m.session.closed).toBe(true)
      } finally {
        operation.finish()
      }
    })

    test('a captured close without its closing operation is refused before anything is written', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      const refusal = (
        await settled(m.loop.finishNode({ closing: { guard: () => null, captured } }))
      ).error
      expect(refusal).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(refusal.message).toMatch(/A captured close requires its closing operation/)
      expect(frames(h.cluster, 79)).toHaveLength(0)
      expect(failures(m).map(({ error }) => error.reason)).toEqual(['not-sent'])
      expect(okResults(m)).toEqual([])
      expect([m.loop.cohorts.size, m.loop.exchanges.size, m.session.queue.length]).toEqual([
        0, 0, 0,
      ])
      expect(m.session.closed).toBe(true)
    })

    test('an unanswered captured final written after the node was disposed settles on expiry and closes its own socket', async () => {
      const m = await established()
      const connection = m.pool.pool[1]
      const { socket, requestQueue: queue } = connection
      const baseline = listeners(socket)
      expect(baseline.data).toBeGreaterThan(0)
      const captured = m.loop.captureFinal()
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const operation = new ConsumerOperation({ deadlineAt: 2000, now: () => h.clock.time })
      try {
        m.loop.dispose(new Error('node closed'), { preserve: captured })
        const closing = m.loop.finishNode({
          closing: { guard: operation.guard, captured, operation },
        })
        await gate.arrived()
        expect(frames(h.cluster, 79)).toHaveLength(1)
        expect(m.loop.exchanges.size).toBe(1)
        expect(failures(m)).toEqual([])
        expect(connection.isConnected()).toBe(true)
        h.clock.time = 2000
        expect(operation.guard()).toBeInstanceOf(Error)
        await closing
        expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
        expect(okResults(m)).toEqual([])
        expect(m.loop.exchanges.size).toBe(0)
        await until(() => !connection.isConnected(), 'the unanswered final transport to close')
        expect(listeners(socket)).toEqual({ ...baseline, ...REMOVED_ON_CLOSE })
        expect([queue.pending.length, queue.inflight.size]).toEqual([0, 0])
        expect([operation.waiters.size, operation.disposers.size]).toEqual([0, 0])
        expect([m.session.queue.length, m.loop.cohorts.size]).toEqual([0, 0])
      } finally {
        operation.finish()
      }
    })

    test('a final answered after its predicate expired publishes no acceptance', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      const gate = h.hold({ api: 'ShareAcknowledge' })
      let expired = false
      const closing = m.loop.finishNode({
        closing: episode(captured, () => (expired ? new Error('closing expired') : null)),
      })
      await gate.arrived()
      expired = true
      gate.release(ackAnswer([{ partition: 0 }]))
      await closing
      expect(okResults(m)).toEqual([])
      expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
    })

    test('a forced close skips a node whose request is still running', async () => {
      const m = await established()
      const gate = h.hold({ api: 'ShareFetch' })
      const round = m.loop.poll()
      await gate.arrived()
      expect(m.loop.captureFinal()).toBeNull()
      gate.release()
      await round
    })
  })

  describe('resources', () => {
    test('disposing a node removes a throttled request that was never written, with its timer', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      await m.loop.poll()
      const connection = m.pool.pool[1]
      const queue = connection.requestQueue
      const baseline = {
        inflight: queue.inflight.size,
        listeners: listeners(connection.socket),
        heartbeat: listeners(m.pool.pool[0].socket),
        sockets: h.cluster.sockets().length,
        frames: frames(h.cluster).length,
      }
      try {
        queue.maybeThrottle(60000)
        const round = settled(m.loop.poll())
        await until(() => queue.pending.length === 1, 'the throttled request to be queued')
        expect(queue.throttleCheckTimeoutId).not.toBeNull()
        expect(frames(h.cluster)).toHaveLength(baseline.frames)
        m.loop.dispose(new Error('node closed'))
        expect((await round).error.message).toBe('node closed')
        expect(queue.pending).toHaveLength(0)
        expect(queue.throttleCheckTimeoutId).toBeNull()
        expect(queue.inflight.size).toBe(baseline.inflight)
        expect(listeners(connection.socket)).toEqual(baseline.listeners)
        expect(listeners(m.pool.pool[0].socket)).toEqual(baseline.heartbeat)
        expect([m.operation.waiters.size, m.operation.disposers.size]).toEqual([0, 0])
        expect([m.session.queue.length, m.loop.exchanges.size, m.loop.cohorts.size]).toEqual([
          0, 0, 0,
        ])
        queue.throttledUntil = -1
        queue.checkPendingRequests()
        await flush()
        expect(frames(h.cluster)).toHaveLength(baseline.frames)
        expect(h.cluster.sockets()).toHaveLength(baseline.sockets)
        expect(connection.isConnected()).toBe(true)
      } finally {
        queue.destroy()
      }
    })

    test('disposing a node closes only the data socket of a written request and removes its waits', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      await m.loop.poll()
      const connection = m.pool.pool[1]
      const queue = connection.requestQueue
      const dataSocket = connection.socket
      const dataBaseline = listeners(dataSocket)
      expect(dataBaseline.data).toBeGreaterThan(0)
      const heartbeat = listeners(m.pool.pool[0].socket)
      const sockets = h.cluster.sockets().length
      const gate = h.hold({ api: 'ShareFetch' })
      const round = settled(m.loop.poll())
      await gate.arrived()
      expect(queue.inflight.size).toBe(1)
      expect(m.operation.disposers.size).toBe(1)
      m.loop.dispose(new Error('node closed'))
      expect((await round).error.message).toBe('node closed')
      await until(() => !connection.isConnected(), 'the data socket to close')
      expect(listeners(dataSocket)).toEqual({ ...dataBaseline, ...REMOVED_ON_CLOSE })
      expect([queue.inflight.size, queue.pending.length]).toEqual([0, 0])
      expect([m.operation.waiters.size, m.operation.disposers.size]).toEqual([0, 0])
      expect([m.session.queue.length, m.loop.exchanges.size, m.loop.cohorts.size]).toEqual([
        0, 0, 0,
      ])
      expect(m.pool.pool[0].isConnected()).toBe(true)
      expect(listeners(m.pool.pool[0].socket)).toEqual(heartbeat)
      gate.release(fetchAnswer([part({ partition: 0 })]))
      await flush()
      expect(h.cluster.sockets()).toHaveLength(sockets)
      expect(m.results).toEqual([])
    })

    test('removes a pending backoff timer when the node is disposed', async () => {
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime'] })
      try {
        h = await harness({ loop: { retry: NO_DELAY } })
        const [m] = h.members
        m.want = [A0]
        m.operation.sleep.mockRestore()
        const sleep = jest.spyOn(m.operation, 'sleep')
        h.script('ShareFetch', [topError(-1)])
        const round = settled(m.loop.poll())
        await until(() => sleep.mock.calls.length === 1, 'the backoff sleep to start')
        expect(sleep).toHaveBeenCalledWith(50)
        expect(m.operation.disposers.size).toBe(1)
        const sleeping = jest.getTimerCount()
        expect(sleeping).toBeGreaterThanOrEqual(1)
        m.loop.dispose(new Error('node closed'))
        expect((await round).error.message).toBe('node closed')
        expect(jest.getTimerCount()).toBe(sleeping - 1)
        expect([m.operation.disposers.size, m.operation.waiters.size]).toEqual([0, 0])
        expect(m.operation.timer).toBeNull()
      } finally {
        jest.useRealTimers()
      }
    })

    test('forgetting one partition keeps only the other partition state', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])])])
      await m.loop.poll()
      m.want = [A1]
      await m.loop.poll()
      expect([...m.session.model.keys()]).toEqual([`${A}:1`])
      expect([...m.loop.cohorts.keys()]).toEqual([])
      expect(m.operation.waiters.size).toBe(0)
      expect(m.operation.disposers.size).toBe(0)
      expect(m.session.queue).toHaveLength(0)
      expect(m.loop.exchanges.size).toBe(0)
    })
  })

  describe('compact halted and incomplete records', () => {
    test.each([
      ['at the next offset', 102n],
      ['with unrelated coordinates', 5000n],
    ])(
      'releases compact halted remainder without guessing the interval of a corrupt batch %s',
      async (label, badBase) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0]
        const good = batch({
          baseOffset: 100n,
          records: [
            record({ offsetDelta: 0, key: 'k0', value: 'p' }),
            record({ offsetDelta: 1, key: 'k1', value: 'q' }),
          ],
        })
        const bad = batch({
          baseOffset: badBase,
          records: [record({ offsetDelta: 0, key: 'k2', value: 'x' })],
          crc: 1,
        })
        h.script('ShareFetch', [
          fetchAnswer([
            part({
              partition: 0,
              acquire: [acquired(100, 1000099)],
              records: Buffer.concat([good, bad]).toString('hex'),
            }),
          ]),
        ])
        const walks = []
        m.dispatch = async (rows) => {
          for (const row of rows) {
            walks.push(row.walk)
            row.ledger.complete(
              row.walk.deliver.records.map((message) => message.offset),
              'accept'
            )
          }
        }
        await m.loop.poll()
        expect(walks).toHaveLength(1)
        expect(walks[0].deliver.records.map(({ value }) => value.toString())).toEqual(['p', 'q'])
        expect(walks[0].reject).toEqual([])
        expect(walks[0].release).toEqual([
          expect.objectContaining({ firstOffset: '102', lastOffset: '1000099' }),
        ])
        await m.loop.poll()
        expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
          batchOf(100, 101, 1),
          batchOf(102, 1000099, 2),
        ])
        expect(
          okResults(m).map(({ firstOffset, lastOffset, type }) => [firstOffset, lastOffset, type])
        ).toEqual([
          ['100', '101', 'accept'],
          ['102', '1000099', 'release'],
        ])
        expect(failures(m)).toEqual([])
      }
    )

    test('turns an incomplete trailing batch into one gap acknowledgement', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const incomplete = Buffer.alloc(13)
      incomplete.writeBigInt64BE(0n, 0)
      incomplete.writeInt32BE(100, 8)
      h.script('ShareFetch', [
        fetchAnswer([
          part({ partition: 0, acquire: [acquired(0, 0)], records: incomplete.toString('hex') }),
        ]),
      ])
      await m.loop.poll()
      expect(m.handled).toEqual([])
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(0, 0, 0),
      ])
    })
  })
})
