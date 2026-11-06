const {
  A,
  B,
  acquire,
  deferred,
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
 * One real operation per event name. `drive` performs it and returns the outcome that the
 * operation must have whether or not a listener fails; `expected` lists the payloads of the first
 * `count` events, in order.
 */
const SCENARIOS = [
  {
    name: 'CONNECT',
    count: 1,
    expected: () => [undefined],
    drive: async ({ h, done, seen }) => {
      await h.consumer.connect()
      await done
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
      await done
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
      await done
      return {
        finals: h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1).length,
        leaves: h.frames(76).filter(({ fields }) => fields.memberEpoch === -1).length,
      }
    },
    outcome: { finals: 1, leaves: 1 },
  },
  {
    name: 'HEARTBEAT',
    count: 2,
    expected: ({ h }) => {
      const payload = { ...joinPayload({ h }), heartbeatIntervalMs: 5 }
      return [payload, payload]
    },
    drive: async ({ h, done }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done
      return { joins: h.frames(76).filter(({ fields }) => fields.memberEpoch === 0).length }
    },
    outcome: { joins: 1 },
  },
  {
    name: 'GROUP_JOIN',
    count: 1,
    expected: ({ h }) => [joinPayload({ h })],
    drive: async ({ h, done, seen }) => {
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done
      const beat = h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch > 0,
      })
      await beat.arrived()
      beat.release()
      await flush()
      return { joins: seen.length }
    },
    outcome: { joins: 1 },
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
      await done
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
      const delivered = []
      await subscribeAndRun(h, {
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await done
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
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await done
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
      await done
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
      await done
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
      await done
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
      await done
      return { flushed: h.frames(79).map(batchesOf) }
    },
    outcome: { flushed: [[wire(0, 1, [1, 2])]] },
  },
  {
    name: 'REQUEST',
    count: 1,
    expected: () => [
      networkPayload({
        size: any(Number),
        duration: any(Number),
        apiName: 'ApiVersions',
        apiKey: 18,
        apiVersion: any(Number),
      }),
    ],
    drive: async ({ h, done }) => {
      await h.consumer.connect()
      await done
      return { firstApi: h.cluster.requests()[0].api }
    },
    outcome: { firstApi: 'ApiVersions' },
  },
  {
    name: 'REQUEST_QUEUE_SIZE',
    count: 1,
    expected: () => [{ broker: NODE, clientId: CLIENT, queueSize: any(Number) }],
    drive: async ({ h, done }) => {
      await h.consumer.connect()
      await done
      return { firstApi: h.cluster.requests()[0].api }
    },
    outcome: { firstApi: 'ApiVersions' },
  },
  {
    name: 'REQUEST_TIMEOUT',
    count: 1,
    options: { kafka: { requestTimeout: 100 } },
    expected: () => [networkPayload({ apiName: 'ShareFetch', apiKey: 78, apiVersion: 2 })],
    drive: async ({ h, done }) => {
      const held = h.hold({ api: 'ShareFetch' })
      await subscribeAndRun(h, { eachMessage: async () => {} })
      await held.arrived()
      await done
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
      await done
      return { rejection: rejection.name }
    },
    outcome: { rejection: 'KafkaJSSASLAuthenticationError' },
  },
  {
    name: 'TOPIC_RECREATED',
    count: 1,
    options: { interval: 5 },
    expected: () => [{ topic: 'topic-a', previousTopicId: A, topicId: B }],
    drive: async ({ h, done }) => {
      const reassigned = deferred()
      let changes = 0
      h.consumer.on(h.consumer.events.ASSIGNMENT_CHANGED, () => {
        if (++changes === 2) reassigned.resolve()
      })
      await subscribeAndRun(h, { eachMessage: async () => {} })
      h.table.set('topic-a', { topicId: B, partitions: [{ leader: 0, epoch: 1 }] })
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
        assignment: { topicPartitions: [{ topicId: B, partitions: [0] }] },
      })
      await done
      await reassigned.promise
      return { assignment: h.consumer.assignment() }
    },
    outcome: { assignment: [{ topic: 'topic-a', partitions: [0] }] },
  },
]

describe('share consumer event matrix', () => {
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.restoreAllMocks()
  })

  /**
   * Performs the scenario's real operation with a failing listener of `kind` registered before an
   * observing one on the same event.
   * @param {object} scenario One entry of SCENARIOS.
   * @param {'none'|'throw'|'reject'} kind What the failing listener does; `none` registers none.
   * @returns {Promise<object>} The observed events, the call order, the listener-failure logs and
   *   the outcome the operation had.
   */
  const run = async (scenario, kind) => {
    const { name, count, options = {}, drive } = scenario
    h = shareClient({ interval: 5, ...options })
    const { consumer } = h
    const failure = new Error('handler failed')
    const logged = jest.spyOn(consumer.logger(), 'error')
    const seen = []
    const trace = []
    const done = deferred()
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
      if (seen.length >= count) done.resolve()
    })
    const outcome = await drive({ h, done: done.promise, seen, failure })
    await flush()
    const logs = logged.mock.calls
      .filter(([message]) => message.startsWith('Failed to execute listener'))
      .map(([message, meta]) => [message, meta.eventName])
    return { seen, trace, logs, outcome, context: { h, failure } }
  }

  test('publishes eighteen events under their literal names', () => {
    h = shareClient()
    expect(h.consumer.events).toEqual(TYPES)
    expect(Object.keys(h.consumer.events)).toHaveLength(18)
  })

  describe.each(SCENARIOS)('$name', (scenario) => {
    const { name, count, expected, outcome } = scenario
    const check = (result) => {
      const { seen, outcome: observed, context } = result
      expect(seen.length).toBeGreaterThanOrEqual(count)
      expect(seen.slice(0, count).map((event) => event.type)).toEqual(
        Array(count).fill(TYPES[name])
      )
      expect(seen.slice(0, count).map((event) => event.payload)).toEqual(expected(context))
      expect(observed).toEqual(outcome)
    }

    test('publishes its exact payloads and outcome with no failing listener', async () => {
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
        expect(result.logs).toHaveLength(result.seen.length)
        for (const log of result.logs)
          expect(log).toEqual([`Failed to execute listener: ${word} ${name}`, TYPES[name]])
        expect(result.trace).toEqual(
          Array(result.seen.length).fill(['listener', 'observer']).flat()
        )
      }
    )
  })
})
