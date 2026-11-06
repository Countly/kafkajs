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
  test('publishes a million-offset settlement as one range despite reentrant stop intent', () => {
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
      c.runner.stopGeneration++
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
  /** @param {object} flow Handler overrides. @returns {Promise<string[]>} The delivered offsets after a stop. */
  const flow = async () => {
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b'])])])
    const delivered = []
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        delivered.push(message.offset)
        if (message.offset === '0') throw new Error('first fails')
      },
    })
    await until(() => delivered.length === 2, 'both deliveries')
    await until(() => h.frames(78).length >= 3, 'the acknowledgements')
    await h.consumer.stop()
    return delivered
  }

  test('contains sync and rejected observers at every real emit site', async () => {
    const names = [
      'CONNECT',
      'HEARTBEAT',
      'GROUP_JOIN',
      'ASSIGNMENT_CHANGED',
      'FETCH',
      'ACKNOWLEDGE',
      'HANDLER_ERROR',
      'STOP',
    ]
    h = shareClient()
    const errors = jest.spyOn(h.consumer.logger(), 'error')
    let invoked = 0
    for (const name of names) {
      h.consumer.on(h.consumer.events[name], () => {
        invoked++
        throw new Error(`sync ${name}`)
      })
      h.consumer.on(h.consumer.events[name], () => {
        invoked++
        return Promise.reject(new Error(`async ${name}`))
      })
    }
    const control = []
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => control.push(payload))
    const delivered = await flow()
    expect(delivered).toEqual(['0', '1'])
    await until(
      () =>
        errors.mock.calls.filter(([message]) => message.startsWith('Failed to execute listener'))
          .length === invoked,
      'one log per failing listener call'
    )
    expect(invoked).toBeGreaterThanOrEqual(names.length * 2)
    expect(control.length).toBeGreaterThan(0)
    expect(h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)).toHaveLength(1)
    expect(h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)).toHaveLength(1)
  })

  test('publishes the exact payload of each runtime event', async () => {
    h = shareClient()
    const seen = record([
      'CONNECT',
      'GROUP_JOIN',
      'ASSIGNMENT_CHANGED',
      'FETCH',
      'HANDLER_ERROR',
      'STOP',
      'DISCONNECT',
    ])
    await flow()
    await h.consumer.disconnect()
    const [assignment] = seen.ASSIGNMENT_CHANGED
    expect(Object.keys(assignment.payload).sort()).toEqual([
      'added',
      'assignment',
      'groupId',
      'memberEpoch',
      'memberId',
      'removed',
    ])
    expect(assignment.type).toBe('share_consumer.assignment_changed')
    expect(assignment.payload.added).toEqual([{ topic: 'topic-a', partitions: [0] }])
    expect(assignment.payload.removed).toEqual([])
    assignment.payload.assignment[0].partitions.push(99)
    const withRecords = seen.FETCH.find(({ payload }) => payload.numberOfRecords > 0)
    expect(Object.keys(withRecords.payload).sort()).toEqual([
      'acknowledgementsPiggybacked',
      'duration',
      'nodeId',
      'numberOfGaps',
      'numberOfRecords',
      'sessionEpoch',
    ])
    expect(withRecords.payload).toMatchObject({ nodeId: 0, numberOfRecords: 2, numberOfGaps: 0 })
    expect(seen.HANDLER_ERROR).toHaveLength(1)
    expect(Object.keys(seen.HANDLER_ERROR[0].payload).sort()).toEqual([
      'error',
      'offsets',
      'partition',
      'topic',
    ])
    expect(seen.HANDLER_ERROR[0].payload.error.message).toBe('first fails')
    for (const name of ['CONNECT', 'STOP', 'DISCONNECT']) {
      expect(seen[name]).toHaveLength(1)
      expect(seen[name][0].payload).toBeUndefined()
    }
    expect(seen.GROUP_JOIN).toHaveLength(1)
  })

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
