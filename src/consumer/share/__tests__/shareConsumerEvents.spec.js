const diagnostics = require('node:diagnostics_channel')
const { ShareRunner } = require('../shareRunner')
const { events, wrap, unwrap } = require('../instrumentationEvents')
const channels = require('../../../instrumentation/channels')
const Emitter = require('../../../instrumentation/emitter')
const { ShareAckLedger } = require('../shareAckLedger')
const { KafkaJSNonRetriableError } = require('../../../errors')

/** @returns {object} Real share emitter, observer containment and log counter. */
const setup = () => {
  const logger = { error: jest.fn() }
  const emitter = new Emitter({ channels: channels.shareConsumer })
  return { logger, emitter, runner: new ShareRunner({ logger, instrumentationEmitter: emitter }) }
}

describe('Share consumer observer containment', () => {
  test('defines exactly eighteen names on a fourth diagnostics table', () => {
    expect(Object.keys(events)).toHaveLength(18)
    for (const name of Object.values(events)) {
      expect(channels.shareConsumer[unwrap(name)]).toBe(diagnostics.channel(`kafkajs.${name}`))
      expect(wrap(unwrap(name))).toBe(name)
    }
  })
  for (const name of Object.values(events)) {
    test.each(['throw', 'reject'])(`${name} contains an observer %s exactly once`, async (kind) => {
      const c = setup()
      const error = new Error('observer')
      const next = jest.fn()
      const remove = c.runner.on(name, () => {
        if (kind === 'throw') throw error
        return Promise.reject(error)
      })
      c.runner.on(name, next)
      c.emitter.emit(unwrap(name), { sentinel: 1 })
      await Promise.resolve()
      await Promise.resolve()
      expect(next).toHaveBeenCalledTimes(1)
      expect(next.mock.calls[0][0].type).toBe(name)
      expect(c.logger.error).toHaveBeenCalledTimes(1)
      remove()
      remove()
      c.emitter.emit(unwrap(name), { sentinel: 2 })
      await Promise.resolve()
      expect(c.logger.error).toHaveBeenCalledTimes(1)
    })
  }
  test('refuses an ordinary or unknown event synchronously', () => {
    const c = setup()
    expect(() => c.runner.on('consumer.fetch', () => {})).toThrow(KafkaJSNonRetriableError)
    expect(() => c.runner.on('unknown', () => {})).toThrow(KafkaJSNonRetriableError)
  })
  test('publishes a million-offset settlement as one range and contains a throwing reentrant observer', () => {
    const c = setup()
    const token = Symbol('acquisition')
    const received = []
    const ledger = new ShareAckLedger({
      topic: 't',
      topicId: 'id',
      partition: 0,
      token,
      sentAt: 0,
      lockDuration: 1000,
      walk: {
        deliver: { records: [], deliveryCounts: new Uint16Array(0) },
        gaps: [{ firstOffset: '9007199254740993', lastOffset: '9007199255740992' }],
        reject: [],
        release: [],
      },
    })
    const prepared = ledger.prepare({ token })
    expect(prepared.ticket.acknowledgementBatches).toHaveLength(1)
    c.runner.on(events.ACKNOWLEDGE, ({ payload }) => {
      received.push(payload)
      ledger.invalidate('membership-lost')
      throw new Error('observer')
    })
    const results = ledger.settle(prepared.ticket, { token })
    c.runner.publishResults({ topic: 't', partition: 0 }, results)
    expect(received).toHaveLength(1)
    expect(ledger.size).toBe(0)
    expect(Object.keys(received[0]).sort()).toEqual([
      'error',
      'firstOffset',
      'lastOffset',
      'partition',
      'topic',
      'type',
    ])
    expect(received[0]).toEqual({
      topic: 't',
      partition: 0,
      firstOffset: '9007199254740993',
      lastOffset: '9007199255740992',
      type: 'gap',
      error: null,
    })
    expect(c.logger.error).toHaveBeenCalledTimes(1)
    expect(ledger.settle(prepared.ticket, { token })).toEqual([])
  })
})

describe('public share event contract', () => {
  const {
    A,
    B,
    acquire,
    acquired,
    deferred,
    fetchAnswer,
    part,
    recordsHex,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  afterEach(async () => {
    jest.restoreAllMocks()
    await h?.close()
    h = undefined
  })
  /** @param {string[]} names Event constants to observe. @returns {Record<string, object[]>} Events by name. */
  const record = (names) => {
    const seen = {}
    for (const name of names) {
      seen[name] = []
      h.consumer.on(h.consumer.events[name], (event) => seen[name].push(event))
    }
    return seen
  }
  /** @param {object} frame A recorded share frame. @returns {object[]} Its acknowledgement batches. */
  const batchesOf = ({ fields }) =>
    fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))

  test('hands each listener fresh assignment copies that cannot reach the client state', async () => {
    h = shareClient({ interval: 5 })
    const seen = []
    h.consumer.on(h.consumer.events.ASSIGNMENT_CHANGED, ({ payload }) => {
      seen.push(JSON.parse(JSON.stringify(payload)))
      if (payload.assignment[0]) payload.assignment[0].partitions.push(99)
      payload.assignment.push({ topic: 'extra', partitions: [] })
      if (payload.added[0]) payload.added[0].topic = 'mutated'
    })
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({ eachMessage: async () => {} })
    expect(seen).toHaveLength(1)
    expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
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
      assignment: { topicPartitions: [] },
    })
    await until(() => seen.length === 2, 'the revocation event')
    expect(seen[1]).toMatchObject({
      memberEpoch: 2,
      assignment: [],
      added: [],
      removed: [{ topic: 'topic-a', partitions: [0] }],
    })
    expect(h.consumer.assignment()).toEqual([])
  })

  test('publishes no assignment event for answers that repeat the assignment in another form', async () => {
    h = shareClient({
      interval: 5,
      topics: {
        'topic-a': {
          topicId: A,
          partitions: [
            { leader: 0, epoch: 1 },
            { leader: 0, epoch: 1 },
          ],
        },
        'topic-b': { topicId: B, partitions: [{ leader: 0, epoch: 1 }] },
      },
    })
    const seen = record(['ASSIGNMENT_CHANGED', 'HEARTBEAT'])
    const epochs = () => seen.HEARTBEAT.map(({ payload }) => payload.memberEpoch)
    const beats = [0, 1, 2].map(() =>
      h.hold({ api: 'ShareGroupHeartbeat', match: ({ fields }) => fields.memberEpoch > 0 })
    )
    await h.consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
    await h.consumer.run({ eachMessage: async () => {} })
    expect(epochs()).toEqual([1])
    expect(seen.ASSIGNMENT_CHANGED.map(({ payload }) => payload.assignment)).toEqual([
      [
        { topic: 'topic-a', partitions: [0, 1] },
        { topic: 'topic-b', partitions: [0] },
      ],
    ])

    const answers = [
      [
        { topicId: B, partitions: [0] },
        { topicId: A, partitions: [1, 0, 1] },
      ],
      [
        { topicId: A, partitions: [0, 1] },
        { topicId: B, partitions: [0] },
      ],
      [
        { topicId: A, partitions: [1] },
        { topicId: B, partitions: [0] },
      ],
    ]
    for (const [index, topicPartitions] of answers.entries()) {
      const epoch = index + 2
      const call = await beats[index].arrived()
      beats[index].release({
        errorCode: 0,
        errorMessage: null,
        memberId: call.fields.memberId,
        memberEpoch: epoch,
        heartbeatIntervalMs: 5,
        assignment: { topicPartitions },
      })
      await until(() => epochs().length === index + 2, `the heartbeat of epoch ${epoch}`)
      expect(seen.ASSIGNMENT_CHANGED).toHaveLength(index === 2 ? 2 : 1)
    }

    expect(epochs()).toEqual([1, 2, 3, 4])
    expect(
      seen.ASSIGNMENT_CHANGED.slice(1).map(({ payload }) => ({
        memberEpoch: payload.memberEpoch,
        assignment: payload.assignment,
        added: payload.added,
        removed: payload.removed,
      }))
    ).toEqual([
      {
        memberEpoch: 4,
        assignment: [
          { topic: 'topic-a', partitions: [1] },
          { topic: 'topic-b', partitions: [0] },
        ],
        added: [],
        removed: [{ topic: 'topic-a', partitions: [0] }],
      },
    ])
    expect(h.consumer.assignment()).toEqual([
      { topic: 'topic-a', partitions: [1] },
      { topic: 'topic-b', partitions: [0] },
    ])
  })

  test.each(['stop', 'disconnect'])(
    'publishes every range of one settlement once when an acknowledge listener calls %s',
    async (closeCall) => {
      h = shareClient()
      const { consumer } = h
      const acks = []
      const stops = []
      const disconnects = []
      let closing
      consumer.on(consumer.events.STOP, () => stops.push(1))
      consumer.on(consumer.events.DISCONNECT, () => disconnects.push(1))
      consumer.on(consumer.events.ACKNOWLEDGE, ({ payload }) => {
        acks.push(payload)
        if (acks.length === 1) {
          closing = closeCall === 'stop' ? consumer.stop() : consumer.disconnect({ timeout: 0 })
        }
      })
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
      await consumer.subscribe({ topics: ['topic-a'] })
      await consumer.run({
        eachBatch: async ({ batch, acknowledge }) => {
          acknowledge(batch.messages[0])
          acknowledge(batch.messages[1], 'reject')
          acknowledge(batch.messages[2])
        },
      })
      await until(() => closing !== undefined, 'the reentrant close')
      await closing
      const rows = () =>
        acks.map(({ firstOffset, lastOffset, type, error }) => [
          firstOffset,
          lastOffset,
          type,
          error,
        ])
      expect(rows()).toEqual([
        ['0', '0', 'accept', null],
        ['1', '1', 'reject', null],
        ['2', '2', 'accept', null],
      ])
      expect(stops).toHaveLength(1)
      expect(disconnects).toHaveLength(closeCall === 'disconnect' ? 1 : 0)
      expect(consumer.assignment()).toEqual([])
      expect(h.frames(79).flatMap(batchesOf)).toEqual([])

      if (closeCall === 'disconnect') await consumer.subscribe({ topics: ['topic-a'] })
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['x'])])])
      const delivered = []
      await consumer.run({ eachMessage: async ({ message }) => delivered.push(message.offset) })
      await until(() => rows().length === 4, 'the acknowledgement of the new run')
      expect(delivered).toEqual(['10'])
      expect(rows()[3]).toEqual(['10', '10', 'accept', null])
      expect(acks).toHaveLength(4)
    }
  )

  test('publishes one membership session reset counting only the discarded delivered records', async () => {
    h = shareClient()
    const seen = record(['SESSION_RESET', 'ACKNOWLEDGE'])
    h.script('ShareFetch', [
      fetchAnswer([
        part({ partition: 0, acquire: [acquired(0, 5)], records: recordsHex(0, ['a', 'b', 'c']) }),
      ]),
    ])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    const settled = deferred()
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, () => {
      if (seen.ACKNOWLEDGE.length === 2) settled.resolve()
    })
    const second = deferred()
    const hold = deferred()
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        if (message.offset === '0') throw new Error('first fails')
        second.resolve()
        await hold.promise
      },
    })
    await Promise.all([settled.promise, second.promise])
    expect(seen.ACKNOWLEDGE.map(({ payload }) => payload)).toEqual([
      {
        topic: 'topic-a',
        partition: 0,
        firstOffset: '0',
        lastOffset: '0',
        type: 'release',
        error: null,
      },
      {
        topic: 'topic-a',
        partition: 0,
        firstOffset: '3',
        lastOffset: '5',
        type: 'gap',
        error: null,
      },
    ])
    expect(seen.SESSION_RESET).toHaveLength(0)
    await h.consumer.disconnect({ timeout: 0 })
    expect(seen.SESSION_RESET.map(({ type, payload }) => ({ type, ...payload }))).toEqual([
      {
        type: 'share_consumer.session_reset',
        nodeId: 0,
        reason: 'membership',
        recordsDropped: 1,
      },
    ])
    hold.resolve()
  })

  test('publishes one membership session reset for each discarded node session', async () => {
    h = shareClient({
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
    })
    const seen = record(['SESSION_RESET', 'FETCH'])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    const opened = deferred()
    h.consumer.on(h.consumer.events.FETCH, () => {
      if (new Set(seen.FETCH.map(({ payload }) => payload.nodeId)).size === 2) opened.resolve()
    })
    await h.consumer.run({ eachMessage: async () => {} })
    await opened.promise
    await h.consumer.disconnect({ timeout: 0 })
    expect(
      seen.SESSION_RESET.map(({ payload }) => payload).sort((a, b) => a.nodeId - b.nodeId)
    ).toEqual([
      { nodeId: 0, reason: 'membership', recordsDropped: 0 },
      { nodeId: 1, reason: 'membership', recordsDropped: 0 },
    ])
  })
})
