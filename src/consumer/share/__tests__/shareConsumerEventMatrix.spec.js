const diagnostics = require('node:diagnostics_channel')
const Emitter = require('../../../instrumentation/emitter')
const channels = require('../../../instrumentation/channels')
const { unwrap } = require('../instrumentationEvents')
const {
  A,
  B,
  acquire,
  fetchAnswer,
  flush,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const NODE = 'node0:9092'
const CLIENT = 'scripted-cluster'
const SHARE_APIS = { ShareGroupHeartbeat: [1, 1], ShareFetch: [2, 2], ShareAcknowledge: [2, 2] }

/** The literal public type of every event name the share consumer publishes. */
const TYPES = {
  HEARTBEAT: 'share_consumer.heartbeat',
  GROUP_JOIN: 'share_consumer.group_join',
  ASSIGNMENT_CHANGED: 'share_consumer.assignment_changed',
  FETCH: 'share_consumer.fetch',
  ACKNOWLEDGE: 'share_consumer.acknowledge',
  SESSION_RESET: 'share_consumer.session_reset',
  LOCK_EXPIRED: 'share_consumer.lock_expired',
  HANDLER_ERROR: 'share_consumer.handler_error',
  CONNECT: 'share_consumer.connect',
  DISCONNECT: 'share_consumer.disconnect',
  STOP: 'share_consumer.stop',
  CRASH: 'share_consumer.crash',
  REQUEST: 'share_consumer.network.request',
  REQUEST_TIMEOUT: 'share_consumer.network.request_timeout',
  REQUEST_QUEUE_SIZE: 'share_consumer.network.request_queue_size',
  AUTHENTICATION_FAILURE: 'share_consumer.network.authentication_failure',
  TOPIC_RECREATED: 'share_consumer.cluster.topic_recreated',
  REBOOTSTRAP: 'share_consumer.cluster.rebootstrap',
}

const FAKE_TIMERS = { doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] }
/** Every channel an ordinary client publishes on: a share event must never appear on one. */
const ORDINARY = new Set(
  [channels.producer, channels.consumer, channels.admin].flatMap((table) =>
    Object.values(table).map((channel) => channel.name)
  )
)
const SHARE_CHANNELS = Object.values(channels.shareConsumer).map((channel) => channel.name)

const any = (Class) => expect.any(Class)
/** @returns {object} The payload of a completed or timed out network request, timings unchecked. */
const networkPayload = (extra) => ({
  broker: NODE,
  clientId: CLIENT,
  correlationId: any(Number),
  createdAt: any(Number),
  sentAt: any(Number),
  pendingDuration: any(Number),
  ...extra,
})
/**
 * Moves the fake clock in steps of at most 50 ms and lets the scripted sockets answer after each.
 * @param {number} ms Fake milliseconds.
 * @returns {Promise<void>}
 */
const tick = async (ms) => {
  for (let left = ms; left > 0; left -= 50) {
    await jest.advanceTimersByTimeAsync(Math.min(50, left))
    await flush()
  }
}
/** @returns {object} One wire acknowledgement batch. */
const wire = (first, last, types) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: types,
})
/** @param {object} frame A recorded share frame. @returns {object[]} Its acknowledgement batches. */
const batchesOf = ({ fields }) =>
  fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
/** @returns {Promise<void>} Subscribes to topic-a and runs the handler. */
const subscribeAndRun = async (h, handler) => {
  await h.consumer.subscribe({ topics: ['topic-a'] })
  await h.consumer.run(handler)
}
const joinPayload = ({ h }) => ({
  groupId: 'group',
  memberId: h.frames(76)[0].fields.memberId,
  memberEpoch: 1,
})
const TWO_RECORDS = () => fetchAnswer([acquire(0, 0, ['a', 'b'])])

/**
 * One real operation per event name. `drive` performs it and returns the outcome the operation
 * must have whether or not a listener fails. `expected` lists the payloads of every event of that
 * name the bounded scenario publishes, in order; `count` is their number when it is known before
 * the operation ran, or null when the wire decides it. A `fake` scenario runs on a fake clock that
 * only `tick` moves, so no heartbeat, retry or timeout publishes behind the scenario's back.
 */
const SCENARIOS = [
  {
    name: 'CONNECT',
    count: 1,
    expected: () => [undefined],
    drive: async ({ h, done, seen }) => {
      await h.consumer.connect()
      await done()
      await h.consumer.connect()
      return { frames: h.frames().length, events: seen.length }
    },
    outcome: { frames: 0, events: 1 },
  },
  {
    name: 'DISCONNECT',
    count: 1,
    expected: () => [undefined],
    drive: async ({ h, done }) => {
      await h.consumer.connect()
      await h.consumer.disconnect()
      await done()
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [] },
  },
  {
    name: 'STOP',
    count: 1,
    expected: () => [undefined],
    drive: async ({ h, done }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await until(() => h.consumer.acquisitionLockTimeoutMs() !== null, 'the opened session')
      await h.consumer.stop()
      await done()
      return {
        finals: h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1).length,
        leaves: h.frames(76).filter(({ fields }) => fields.memberEpoch === -1).length,
      }
    },
    outcome: { finals: 1, leaves: 1 },
  },
  {
    name: 'HEARTBEAT',
    fake: true,
    count: 2,
    options: { interval: 100 },
    expected: ({ h }) => {
      const payload = { ...joinPayload({ h }), heartbeatIntervalMs: 100 }
      return [payload, payload]
    },
    drive: async ({ h, done }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await tick(100)
      await done()
      return { epochs: h.frames(76).map(({ fields }) => fields.memberEpoch) }
    },
    outcome: { epochs: [0, 1] },
  },
  {
    name: 'GROUP_JOIN',
    fake: true,
    count: 1,
    options: { interval: 100 },
    expected: ({ h }) => [joinPayload({ h })],
    drive: async ({ h, done, seen }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done()
      await tick(100)
      await until(() => h.frames(76).length === 2, 'the steady heartbeat')
      return { joins: seen.length, epochs: h.frames(76).map(({ fields }) => fields.memberEpoch) }
    },
    outcome: { joins: 1, epochs: [0, 1] },
  },
  {
    name: 'ASSIGNMENT_CHANGED',
    count: 1,
    expected: ({ h }) => [
      {
        ...joinPayload({ h }),
        assignment: [{ topic: 'topic-a', partitions: [0] }],
        added: [{ topic: 'topic-a', partitions: [0] }],
        removed: [],
      },
    ],
    drive: async ({ h, done }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done()
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [{ topic: 'topic-a', partitions: [0] }] },
  },
  {
    name: 'FETCH',
    count: 2,
    expected: () => [
      {
        nodeId: 0,
        sessionEpoch: 0,
        numberOfRecords: 2,
        numberOfGaps: 0,
        acknowledgementsPiggybacked: 0,
        duration: any(Number),
      },
      {
        nodeId: 0,
        sessionEpoch: 1,
        numberOfRecords: 0,
        numberOfGaps: 0,
        acknowledgementsPiggybacked: 1,
        duration: any(Number),
      },
    ],
    drive: async ({ h, done }) => {
      h.script('ShareFetch', [TWO_RECORDS()])
      const third = h.hold({
        api: 'ShareFetch',
        match: ({ fields }) => fields.shareSessionEpoch === 2,
      })
      const delivered = []
      await subscribeAndRun(h, {
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await done()
      await until(() => third.call !== null, 'the third fetch')
      return { delivered, piggybacked: batchesOf(h.frames(78)[1]) }
    },
    outcome: { delivered: ['0', '1'], piggybacked: [wire(0, 1, [1])] },
  },
  {
    name: 'ACKNOWLEDGE',
    count: 1,
    expected: () => [
      {
        topic: 'topic-a',
        partition: 0,
        firstOffset: '0',
        lastOffset: '1',
        type: 'accept',
        error: null,
      },
    ],
    drive: async ({ h, done }) => {
      h.script('ShareFetch', [TWO_RECORDS()])
      const third = h.hold({
        api: 'ShareFetch',
        match: ({ fields }) => fields.shareSessionEpoch === 2,
      })
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done()
      await until(() => third.call !== null, 'the third fetch')
      return { piggybacked: batchesOf(h.frames(78)[1]), flushes: h.frames(79).length }
    },
    outcome: { piggybacked: [wire(0, 1, [1])], flushes: 0 },
  },
  {
    name: 'SESSION_RESET',
    count: 1,
    expected: () => [{ nodeId: 0, reason: 'membership', recordsDropped: 0 }],
    drive: async ({ h, done }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await until(() => h.consumer.acquisitionLockTimeoutMs() !== null, 'the opened session')
      await h.consumer.disconnect({ timeout: 0 })
      await done()
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [] },
  },
  {
    name: 'LOCK_EXPIRED',
    count: 1,
    expected: () => [{ topic: 'topic-a', partition: 0, offsets: ['0'], stage: 'after-handler' }],
    drive: async ({ h, done }) => {
      let now = 100
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 0, ['a'])], { acquisitionLockTimeoutMs: 1000 }),
      ])
      await subscribeAndRun(h, {
        eachMessage: async () => {
          now = 1200
        },
      })
      await done()
      await until(() => h.frames(79).length === 1, 'the flush of the late return')
      return { flushed: h.frames(79).map(batchesOf) }
    },
    outcome: { flushed: [[wire(0, 0, [1])]] },
  },
  {
    name: 'HANDLER_ERROR',
    count: 1,
    expected: ({ failure }) => [{ topic: 'topic-a', partition: 0, offsets: ['0'], error: failure }],
    drive: async ({ h, done, failure }) => {
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
      await subscribeAndRun(h, {
        eachMessage: async () => {
          throw failure
        },
      })
      await done()
      await until(() => h.frames(79).length === 1, 'the release flush')
      return { released: h.frames(79).map(batchesOf) }
    },
    outcome: { released: [[wire(0, 0, [2])]] },
  },
  {
    name: 'CRASH',
    count: 1,
    options: { share: { acknowledgementMode: 'explicit' } },
    expected: () => [
      {
        error: expect.objectContaining({
          name: 'KafkaJSShareAcknowledgementError',
          reason: 'unacknowledged',
          topic: 'topic-a',
          partition: 0,
          ranges: [{ firstOffset: '1', lastOffset: '1' }],
        }),
        groupId: 'group',
        restart: false,
      },
    ],
    drive: async ({ h, done }) => {
      h.script('ShareFetch', [TWO_RECORDS()])
      await subscribeAndRun(h, {
        eachBatch: async ({ batch, acknowledge }) => acknowledge(batch.messages[0]),
      })
      await done()
      return {
        flushed: h.frames(79).map(batchesOf),
        epochs: h.frames(79).map(({ fields }) => fields.shareSessionEpoch),
      }
    },
    outcome: { flushed: [[wire(0, 1, [1, 2])], []], epochs: [1, -1] },
  },
  {
    name: 'REQUEST',
    count: null,
    expected: ({ h }) =>
      h.cluster.log.map(({ apiKey, version }) =>
        networkPayload({
          size: any(Number),
          duration: any(Number),
          apiName: any(String),
          apiKey,
          apiVersion: version,
        })
      ),
    drive: async ({ h }) => {
      await h.consumer.connect()
      return { firstApi: h.cluster.requests()[0].api }
    },
    outcome: { firstApi: 'ApiVersions' },
  },
  {
    name: 'REQUEST_QUEUE_SIZE',
    count: null,
    expected: ({ emitted }) =>
      emitted.map(() => ({ broker: NODE, clientId: CLIENT, queueSize: 0 })),
    drive: async ({ h }) => {
      await h.consumer.connect()
      await h.consumer.disconnect()
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [] },
  },
  {
    name: 'REQUEST_TIMEOUT',
    count: 1,
    options: { kafka: { requestTimeout: 100 } },
    expected: () => [networkPayload({ apiName: 'ShareFetch', apiKey: 78, apiVersion: 2 })],
    drive: async ({ h, done }) => {
      const held = h.hold({ api: 'ShareFetch' })
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await until(() => held.call !== null, 'the held fetch')
      await done()
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [{ topic: 'topic-a', partitions: [0] }] },
  },
  {
    name: 'AUTHENTICATION_FAILURE',
    count: 1,
    options: {
      profile: 'sasl',
      advertise: SHARE_APIS,
      kafka: { sasl: { mechanism: 'plain', username: 'user', password: 'secret' } },
    },
    expected: () => [
      {
        broker: NODE,
        mechanism: 'PLAIN',
        errorMessage: expect.stringContaining('SASL PLAIN authentication failed'),
      },
    ],
    drive: async ({ h, done }) => {
      h.cluster.answer('SaslHandshake', () => ({ enabledMechanisms: ['PLAIN'] }))
      h.cluster.answer('SaslAuthenticate', () => ({
        errorCode: 58,
        errorMessage: 'rejected',
        authBytes: Buffer.alloc(0),
      }))
      const rejection = await h.consumer.connect().catch((error) => error)
      await done()
      return { rejection: rejection.name }
    },
    outcome: { rejection: 'KafkaJSSASLAuthenticationError' },
  },
  {
    name: 'TOPIC_RECREATED',
    fake: true,
    count: 1,
    options: { interval: 100 },
    expected: () => [{ topic: 'topic-a', previousTopicId: A, topicId: B }],
    drive: async ({ h, done }) => {
      let reassigned = false
      h.consumer.on(h.consumer.events.ASSIGNMENT_CHANGED, ({ payload }) => {
        reassigned = payload.memberEpoch === 2 && payload.assignment.length === 1
      })
      await subscribeAndRun(h, { eachMessage: async () => {} })
      h.table.set('topic-a', { topicId: B, partitions: [{ leader: 0, epoch: 1 }] })
      const beat = h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch > 0,
      })
      await tick(100)
      await until(() => beat.call !== null, 'the held heartbeat')
      const call = beat.call
      beat.release({
        errorCode: 0,
        errorMessage: null,
        memberId: call.fields.memberId,
        memberEpoch: 2,
        heartbeatIntervalMs: 100,
        assignment: { topicPartitions: [{ topicId: B, partitions: [0] }] },
      })
      await done()
      await until(() => reassigned, 'the recreated assignment')
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [{ topic: 'topic-a', partitions: [0] }] },
  },
]

describe('share consumer event matrix', () => {
  let h
  const subscriptions = new Set()
  afterEach(async () => {
    for (const remove of subscriptions) remove()
    try {
      await h?.close()
    } finally {
      h = undefined
      jest.useRealTimers()
      jest.restoreAllMocks()
    }
  })

  /**
   * Performs the scenario's real operation with a failing listener of `kind` registered before an
   * observing one on the same event, and records what every layer published meanwhile: the
   * observer's events, the client emitter's own emissions of that event, and the diagnostics
   * channels' publications. The scenario is bounded: nothing but its own operation moves time.
   * @param {object} scenario One entry of SCENARIOS.
   * @param {'none'|'throw'|'reject'} kind What the failing listener does; `none` registers none.
   * @returns {Promise<object>} The observed events, the call order, the listener-failure logs, the
   *   client emitter's emitted payloads, the channel census and the outcome the operation had.
   */
  const run = async (scenario, kind) => {
    const { name, count, fake = false, options = {}, drive } = scenario
    if (fake) jest.useFakeTimers(FAKE_TIMERS)
    h = shareClient({ interval: 600000, ...options })
    const { consumer } = h
    const failure = new Error('handler failed')
    const logged = jest.spyOn(consumer.logger(), 'error')
    const internal = unwrap(TYPES[name])
    const emitted = []
    const emit = Emitter.prototype.emit
    jest.spyOn(Emitter.prototype, 'emit').mockImplementation(function (eventName, payload) {
      if (eventName === internal) emitted.push(payload)
      return emit.call(this, eventName, payload)
    })
    const publications = []
    const onPublish = (_, channelName) => publications.push(channelName)
    const watched = [...ORDINARY, ...SHARE_CHANNELS]
    const removers = watched.map((channelName) => {
      const remove = () => {
        diagnostics.unsubscribe(channelName, onPublish)
        subscriptions.delete(remove)
      }
      subscriptions.add(remove)
      diagnostics.subscribe(channelName, onPublish)
      return remove
    })
    const seen = []
    const trace = []
    const done = () => until(() => seen.length >= count, `the ${name} event`)
    if (kind !== 'none') {
      consumer.on(consumer.events[name], () => {
        trace.push('listener')
        if (kind === 'throw') throw new Error(`sync ${name}`)
        return Promise.reject(new Error(`async ${name}`))
      })
    }
    consumer.on(consumer.events[name], (event) => {
      trace.push('observer')
      seen.push({ type: event.type, payload: event.payload })
    })
    try {
      const outcome = await drive({ h, done, seen, failure, emitted })
      await flush()
      const logs = logged.mock.calls
        .filter(([message]) => message.startsWith('Failed to execute listener'))
        .map(([message, meta]) => [message, meta.eventName])
      return {
        seen,
        trace,
        logs,
        emitted,
        outcome,
        census: {
          ordinary: publications.filter((channelName) => ORDINARY.has(channelName)),
          own: publications.filter(
            (channelName) => channelName === `${channels.CHANNEL_PREFIX}${TYPES[name]}`
          ).length,
        },
        context: { h, failure, emitted },
      }
    } finally {
      for (const remove of removers) remove()
    }
  }

  test('publishes eighteen events under their literal names', () => {
    h = shareClient()
    expect(h.consumer.events).toEqual(TYPES)
    expect(Object.keys(h.consumer.events)).toHaveLength(18)
  })

  describe.each(SCENARIOS)('$name', (scenario) => {
    const { name, count, expected, outcome } = scenario
    const check = (result) => {
      const { seen, outcome: observed, context, emitted, census } = result
      const payloads = expected(context)
      if (count === null) expect(payloads.length).toBeGreaterThan(0)
      else expect(payloads).toHaveLength(count)
      expect(seen.map((event) => event.type)).toEqual(Array(payloads.length).fill(TYPES[name]))
      expect(seen.map((event) => event.payload)).toEqual(payloads)
      expect(emitted).toHaveLength(payloads.length)
      expect(census.own).toBe(payloads.length)
      expect(census.ordinary).toEqual([])
      expect(observed).toEqual(outcome)
    }

    test('publishes its exact complete sequence and outcome with no failing listener', async () => {
      const result = await run(scenario, 'none')
      check(result)
      expect(result.logs).toEqual([])
      expect(result.trace).toEqual(Array(result.seen.length).fill('observer'))
    })

    test.each([
      ['throw', 'sync'],
      ['reject', 'async'],
    ])(
      'contains a %s listener once per event without changing the operation',
      async (kind, word) => {
        const result = await run(scenario, kind)
        check(result)
        expect(result.logs).toEqual(
          Array(result.seen.length).fill([
            `Failed to execute listener: ${word} ${name}`,
            TYPES[name],
          ])
        )
        expect(result.trace).toEqual(
          Array(result.seen.length).fill(['listener', 'observer']).flat()
        )
      }
    )
  })
})
