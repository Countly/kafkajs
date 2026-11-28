const ConsumerGroup = require('../consumerGroup')
const { MemberAssignment } = require('../assignerProtocol')
const { KafkaJSError, KafkaJSTopicRecreatedError } = require('../../errors')
const {
  advanceUntil,
  deferred,
  settled,
  start,
  startClassic,
  teardown,
  forget,
  beats,
  fenced,
  inCauseChain,
  protocolError,
  recordingLogger,
} = require('./fixtures/scriptedConsumer')

const INSTANCE = 'instance-1'
const FENCED_INSTANCE_ID = 82
const REBALANCE_IN_PROGRESS = 27

beforeEach(() => jest.useFakeTimers({ doNotFake: ['queueMicrotask', 'nextTick', 'setImmediate'] }))
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** How each recorded step reads in a trail. */
const STEPS = {
  POLL: () => 'POLL_INTERVAL_EXCEEDED',
  commit: ({ tp, offset }) => `commit ${tp} ${offset}`,
  revoked: () => 'revoked callback',
  final: () => 'final commit',
  leave: ({ memberId, groupInstanceId }) => `leave ${memberId} ${groupInstanceId}`,
  STOP: () => 'STOP',
  DISCONNECT: () => 'DISCONNECT',
  CRASH: ({ restart }) => `CRASH restart ${restart}`,
}

/** @returns {string[]} the run's commits, leaves, close steps and events, in the order they happened */
const trail = (run) =>
  run.log.filter(({ kind }) => STEPS[kind]).map((entry) => STEPS[entry.kind](entry))

/** @returns {KafkaJSError} what a handler throws */
const failure = (retriable) => new KafkaJSError('the handler failed', { retriable })

/** @returns {KafkaJSTopicRecreatedError} the error a recreation stop reports */
const recreated = () =>
  new KafkaJSTopicRecreatedError('Topic topic-a was recreated', {
    topic: 'topic-a',
    previousTopicId: 'aAaAaAaAaAaAaAaAaAaAaA',
    topicId: 'zZzZzZzZzZzZzZzZzZzZzZ',
    groupId: 'loss-driver-group',
  })

/** The crash retry options of these tests: no retry in place, a 300 ms restart without jitter. */
const RETRY = { retries: 0, factor: 0, initialRetryTime: 300 }

/**
 * Starts a classic member of topic-a that resolves without committing (threshold 100), and
 * records each LeaveGroup, the start of the stop's final commit, the close callback and the
 * POLL_INTERVAL_EXCEEDED, DISCONNECT and CRASH events in `run.log`, beside the commits and STOP the
 * fixture records.
 * @param {object} [options] those of `startClassic`, and
 * @param {boolean} [options.instance] whether the member is static
 * @param {object} [options.retry] the consumer's retry options
 * @param {(payload: object, held: object, call: number) => Promise<void>} [options.eachMessage]
 *   called with the count of calls made so far, this one included
 * @param {() => Promise<void>} [options.closeCallback] what the revoked callback of a close awaits
 * @returns {Promise<object>} the run, with `delivered`: `partition|offset` of each handler call
 */
const begin = async ({
  instance = false,
  retry,
  config,
  eachMessage,
  closeCallback,
  ...options
} = {}) => {
  const ref = {}
  const delivered = []
  const run = await startClassic('classic', {
    autoCommitThreshold: 100,
    records: { 'topic-a|0': [40, 41] },
    trace: true,
    ...options,
    config: { ...(instance && { groupInstanceId: INSTANCE }), ...(retry && { retry }), ...config },
    eachMessage: async (payload, held) => {
      delivered.push(`${payload.partition}|${payload.message.offset}`)
      if (eachMessage) await eachMessage(payload, held, delivered.length)
    },
    ...(closeCallback && {
      hook: {
        onPartitionsRevoked: async ({ partitions }) => {
          if (partitions.length === 0) return
          ref.log.push({ kind: 'revoked' })
          await closeCallback()
        },
      },
    }),
  })
  const { consumer, coordinator, runner, log } = run
  ref.log = log
  coordinator.leaveGroup.mockImplementation(async ({ memberId, groupInstanceId }) => {
    log.push({ kind: 'leave', memberId, groupInstanceId })
  })
  const commitFinal = runner.commitFinalOffsetsContained.bind(runner)
  jest.spyOn(runner, 'commitFinalOffsetsContained').mockImplementation(async (scope) => {
    log.push({ kind: 'final' })
    return commitFinal(scope)
  })
  consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, ({ payload }) =>
    log.push({ kind: 'POLL', isStatic: payload.isStatic })
  )
  return Object.assign(run, { delivered })
}

/** @returns {number} when the run's CRASH was emitted, on the fake clock */
const crashedAt = (run) => run.log.find(({ kind }) => kind === 'CRASH').at

/**
 * Checks the restart delay to the millisecond: the cluster is not connected again one millisecond
 * before `delay` has passed since CRASH, and is connected at exactly `delay` when the run
 * restarts; a run that does not restart makes no connect within a further second. Call it within
 * `delay` ms of CRASH.
 * @param {object} run
 * @param {boolean} restart whether CRASH reported a restart
 * @param {number} [delay=300] the configured restart delay, in ms
 * @returns {Promise<void>}
 */
const expectRestart = async (run, restart, delay = 300) => {
  const connects = () => run.log.filter(({ kind }) => kind === 'connect')
  const before = connects().length
  const crashAt = crashedAt(run)
  const waited = Date.now() - crashAt
  expect(waited).toBeLessThan(delay)
  await jest.advanceTimersByTimeAsync(delay - 1 - waited)
  expect(connects()).toHaveLength(before)
  await jest.advanceTimersByTimeAsync(1)
  if (!restart) {
    await jest.advanceTimersByTimeAsync(1000)
    expect(connects()).toHaveLength(before)
    return
  }
  for (let turn = 0; turn < 20 && connects().length === before; turn++) {
    await jest.advanceTimersByTimeAsync(0)
  }
  expect(connects()).toHaveLength(before + 1)
  expect(connects()[before].at - crashAt).toBe(delay)
}

/** How each step of a crash during start reads in its trace, `run()` settlement included. */
const START_STEPS = {
  commit: () => 'commit',
  STOP: () => 'STOP',
  disconnect: () => 'transport disconnect',
  DISCONNECT: () => 'DISCONNECT',
  CRASH: ({ restart }) => `CRASH restart ${restart}`,
  RUN: () => 'run() settled',
}

/** @returns {string[]} what the run's single trace holds of a crash during start, in order */
const startupTrail = (run) =>
  run.log.filter(({ kind }) => START_STEPS[kind]).map((entry) => START_STEPS[entry.kind](entry))

/**
 * Parks the transport disconnect of the run's cleanup, so `run()` and the crash report are still
 * pending behind it.
 * @returns {{ prepare: Function, parked: Function, release: () => void }} the `startClassic`
 *   options that park it and wait until it is parked, and the release
 */
const parkTransport = () => {
  const gate = deferred()
  return {
    prepare: ({ state }) => {
      state.disconnecting = gate.promise
    },
    parked: ({ log }) => log.some(({ kind }) => kind === 'disconnect'),
    release: gate.resolve,
  }
}

/**
 * Disconnects a run whose handlers have all returned, then checks that it left no timer behind.
 * @returns {Promise<void>}
 */
const closeClean = async (run) => {
  const closing = settled(run.consumer.disconnect())
  await advanceUntil(() => closing.done, 2000)
  expect(closing).toMatchObject({ done: true, value: undefined })
  await jest.advanceTimersByTimeAsync(1000)
  expect(jest.getTimerCount()).toBe(0)
  forget()
}

/**
 * Starts the same scripted assignment through either handler API and any supported group path.
 * @param {string} protocol the group path
 * @param {string} handler the handler API
 * @param {Function} handle receives the payload, partition and first offset
 * @param {object} [options] assignment, records, retry options and fixture preparation
 * @returns {Promise<object>} the running consumer and its scripted broker
 */
const startRetryRun = (protocol, handler, handle, options = {}) => {
  const { retry: overrides, ...rest } = options
  const retry = { ...RETRY, retries: 1, restartOnFailure: async () => false, ...overrides }
  const handlers = {
    [handler]: (payload) =>
      handler === 'eachMessage'
        ? handle(payload, payload.partition, payload.message.offset)
        : handle(payload, payload.batch.partition, payload.batch.firstOffset()),
  }
  if (protocol === 'consumer') return start({ ...rest, retry, ...handlers })
  return startClassic('classic', {
    ...rest,
    ...handlers,
    config: {
      retry,
      maxPollInterval: 4000,
      heartbeatInterval: 500,
      ...(protocol === 'cooperative' && {
        partitionAssigners: [
          () => ({
            name: 'eager',
            version: 0,
            supportedProtocols: () => ['cooperative'],
            protocol: () => ({ name: 'eager', metadata: Buffer.alloc(0) }),
            assign: async () => [],
          }),
        ],
      }),
    },
  })
}

/** Routes two partitions to different scripted brokers without changing the Fetch response API. */
const splitBrokers = async ({ cluster }) => {
  const broker = await cluster.findBroker(0)
  const other = { fetch: jest.fn(broker.fetch.getMockImplementation()) }
  cluster.getNodeIds = () => [0, 1]
  cluster.findTopicPartitionMetadata = () => [
    { partitionId: 0, leader: 0 },
    { partitionId: 1, leader: 1 },
  ]
  cluster.findBroker.mockImplementation(async (nodeId) => (nodeId === 1 ? other : broker))
}

describe.each(['eager', 'cooperative', 'consumer'])(
  'Consumer > handler retries with %s',
  (protocol) => {
    it.each(['eachMessage', 'eachBatch'])(
      '%s exhausts one retry across two poll expiries',
      async (handler) => {
        const deliveries = []
        const order = []
        const error = failure(true)
        const run = await startRetryRun(protocol, handler, async () => {
          const held = deferred()
          deliveries.push(held)
          order.push('delivery')
          await held.promise
          throw error
        })
        const expiries = []
        run.consumer.on(run.consumer.events.POLL_INTERVAL_EXCEEDED, () => expiries.push(Date.now()))
        run.consumer.on(run.consumer.events.GROUP_JOIN, () => order.push('join'))
        run.consumer.on(run.consumer.events.CRASH, () => order.push('crash'))

        try {
          for (let attempt = 0; attempt < 2; attempt++) {
            await advanceUntil(() => expiries.length > attempt, 6000)
            expect(deliveries).toHaveLength(attempt + 1)
            await jest.advanceTimersByTimeAsync(50)
            deliveries[attempt].resolve()
          }
          await advanceUntil(() => run.crashes.length > 0)
          expect(order).toEqual(['delivery', 'join', 'delivery', 'join', 'crash'])
          expect(run.crashes[0].error).toMatchObject({
            name: 'KafkaJSNumberOfRetriesExceeded',
            retryCount: 1,
            cause: error,
          })
          expect(run.runner.partitionRetries.size).toBe(0)
        } finally {
          for (const held of deliveries) held.resolve()
        }
      },
      30000
    )

    it.each(['eachMessage', 'eachBatch'])(
      '%s charges every partition in an outage and never shortens the backoff',
      async (handler) => {
        const times = [[], []]
        const gates = [deferred(), deferred(), deferred()]
        const run = await startRetryRun(
          protocol,
          handler,
          async (_payload, partition) => {
            const attempt = times[partition].length
            times[partition].push(Date.now())
            await gates[attempt]?.promise
            throw failure(true)
          },
          {
            partitions: [0, 1],
            records: { 'topic-a|0': [40], 'topic-a|1': [40] },
            retry: { retries: 2, multiplier: 0.5 },
            run: { partitionsConsumedConcurrently: 2, autoCommit: false },
          }
        )
        try {
          for (let round = 0; round < gates.length; round++) {
            await advanceUntil(() => times.every((values) => values.length > round))
            expect(times.map((values) => values.length)).toEqual([round + 1, round + 1])
            gates[round].resolve()
          }
          await advanceUntil(() => run.crashes.length > 0)
          expect(run.crashes[0].error).toMatchObject({
            name: 'KafkaJSNumberOfRetriesExceeded',
            retryCount: 2,
          })
          for (const [first, second, third] of times) {
            expect(second - first).toBeGreaterThanOrEqual(300)
            expect(third - second).toBeGreaterThanOrEqual(second - first)
          }
          expect(times.map((values) => values.length)).toEqual([3, 3])
          expect(run.runner.partitionRetries.size).toBe(0)
        } finally {
          for (const gate of gates) gate.resolve()
        }
      },
      30000
    )
  }
)

describe('Consumer > partition handler retry state', () => {
  it.each([
    ['eager', 'join'],
    ['eager', 'backoff'],
    ['consumer', 'join'],
    ['consumer', 'backoff'],
  ])(
    '%s recovery expiry during %s crashes before another Fetch',
    async (protocol, phase) => {
      const held = deferred()
      const policy = jest.fn(async () => false)
      const handler = jest.fn(async () => {
        await held.promise
        throw failure(true)
      })
      const run = await startRetryRun(protocol, 'eachMessage', handler, {
        retry: { initialRetryTime: 120000, restartOnFailure: policy },
      })
      try {
        await advanceUntil(() => handler.mock.calls.length === 1)
        const fetches = () => run.log.filter(({ kind }) => kind === 'fetch').length
        const before = fetches()
        const retryJoin = jest.spyOn(run.runner.rebalanceCoordinator, 'retryJoin')
        const joins = jest.spyOn(run.group, 'joinAndSync').mockImplementation(async (round) => {
          if (phase === 'join') return round.wait(() => new Promise(() => {}))
          throw failure(true)
        })

        run.runner.rebalanceCoordinator.requestRejoin({
          kind: 'lost',
          reason: 'lost',
          error: protocol === 'consumer' ? fenced() : protocolError(22),
        })
        held.resolve()
        await advanceUntil(() => joins.mock.calls.length === 1)
        expect(joins).toHaveBeenCalledTimes(1)
        if (phase === 'backoff') {
          await advanceUntil(() => retryJoin.mock.calls.length > 0)
          expect(retryJoin).toHaveBeenCalledWith(expect.any(Function), expect.any(Object), {
            retryCount: 0,
            retryTime: 120000,
          })
        }
        expect(run.crashes).toEqual([])
        await jest.advanceTimersByTimeAsync(125000)
        expect(fetches()).toBe(before)
        expect(handler).toHaveBeenCalledTimes(1)
        expect(joins).toHaveBeenCalledTimes(1)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
        expect(policy).toHaveBeenCalledTimes(1)
      } finally {
        held.resolve()
      }
    },
    30000
  )

  it('drops retry state when cooperative rounds revoke and reacquire during backoff', async () => {
    const deliveries = jest.fn(() => {
      throw failure(true)
    })
    const run = await startRetryRun('cooperative', 'eachBatch', deliveries, {
      retry: { initialRetryTime: 2000 },
    })
    await advanceUntil(() => run.runner.partitionRetries.size === 1)
    const key = 'topic-a|0'
    const first = run.group.acquired.get(key).assignmentToken
    const accepted = []
    run.consumer.on(run.consumer.events.GROUP_JOIN, ({ payload }) => {
      accepted.push({
        partitions: payload.memberAssignment['topic-a'] ?? [],
        retries: run.runner.partitionRetries.size,
      })
    })
    run.coordinator.syncGroup.mockResolvedValueOnce({
      memberAssignment: MemberAssignment.encode({ version: 0, assignment: { 'topic-a': [] } }),
    })
    run.runner.rebalanceCoordinator.requestRejoin({
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: protocolError(REBALANCE_IN_PROGRESS),
    })
    await advanceUntil(() => accepted.length === 2, 1000)
    expect(accepted).toEqual([
      { partitions: [], retries: 0 },
      { partitions: [0], retries: 0 },
    ])
    expect(run.group.acquired.get(key).assignmentToken).not.toBe(first)
    expect(deliveries).toHaveBeenCalledTimes(1)
    await advanceUntil(() => run.crashes.length > 0, 6000)
    expect(deliveries).toHaveBeenCalledTimes(3)
    expect(run.crashes[0].error).toMatchObject({ retryCount: 1 })
  })

  it('retries an escaped join when a foreground loss takes over cooperative recovery', async () => {
    const held = deferred()
    const rejectJoin = deferred()
    const error = failure(false)
    const handler = jest.fn(async () => {
      await held.promise
      throw error
    })
    const run = await startRetryRun('cooperative', 'eachMessage', handler)
    const rebalancer = run.runner.rebalanceCoordinator
    try {
      await advanceUntil(() => handler.mock.calls.length === 1)
      const join = jest.fn(async () => {
        await rejectJoin.promise
        throw protocolError(7)
      })
      run.coordinator.joinGroup.mockImplementationOnce(join)
      rebalancer.requestRejoin({
        kind: 'revoked',
        reason: 'rebalance-in-progress',
        error: protocolError(REBALANCE_IN_PROGRESS),
      })
      await advanceUntil(() => join.mock.calls.length === 1)
      expect(join).toHaveBeenCalledTimes(1)
      rebalancer.requestRejoin({ kind: 'lost', reason: 'lost', error: protocolError(22) })
      held.resolve()
      await advanceUntil(() => rebalancer.recovery?.step)
      expect(rebalancer.recovery?.step).toMatchObject({ retryCount: 0, retryTime: 300 })
      rejectJoin.resolve()
      await advanceUntil(() => run.crashes.length === 1)
      expect(run.coordinator.joinGroup.mock.calls.length).toBeGreaterThanOrEqual(3)
      expect(handler).toHaveBeenCalledTimes(1)
      expect(run.crashes[0].error.cause).toBe(error)
    } finally {
      held.resolve()
      rejectJoin.resolve()
    }
  })

  it('clears a pending retry when a filtering-only batch resolves its offset', async () => {
    const handler = jest.fn(({ pause }) => {
      pause()
      throw failure(true)
    })
    const run = await startRetryRun('eager', 'eachMessage', handler)
    await advanceUntil(() => run.runner.partitionRetries.size === 1)
    const ended = jest.fn()
    run.consumer.on(run.consumer.events.END_BATCH_PROCESS, ended)
    run.state.controlOffset = 40
    run.consumer.resume([{ topic: 'topic-a', partitions: [0] }])
    await advanceUntil(() => ended.mock.calls.length > 0)
    expect(ended).toHaveBeenCalled()
    expect(handler).toHaveBeenCalledTimes(1)
    expect(run.runner.partitionRetries.size).toBe(0)
    await closeClean(run)
  })

  it('keeps the longest backoff when another partition starts failing', async () => {
    const times = [[], []]
    const run = await startRetryRun(
      'eager',
      'eachBatch',
      (_payload, partition) => {
        const calls = times[partition]
        calls.push(Date.now())
        if (partition === 0 ? calls.length <= 2 : calls.length === 3) throw failure(true)
      },
      {
        partitions: [0, 1],
        records: { 'topic-a|0': [40], 'topic-a|1': [40] },
        retry: { retries: 3, multiplier: 2 },
        run: {
          autoCommit: false,
          eachBatchAutoResolve: false,
          partitionsConsumedConcurrently: 2,
        },
      }
    )
    await advanceUntil(() => times.every((calls) => calls.length >= 4))
    const [first, second, third, fourth] = times[0]
    expect(second - first).toBeGreaterThanOrEqual(300)
    expect(third - second).toBeGreaterThanOrEqual(600)
    expect(fourth - third).toBeGreaterThanOrEqual(600)
    expect(run.crashes).toEqual([])
    await closeClean(run)
  })

  it.each(['eachMessage', 'eachBatch'])(
    '%s gets a fresh budget when delivery starts beyond the failed offset',
    async (handler) => {
      const offsets = []
      const run = await startRetryRun(
        'eager',
        handler,
        ({ pause }, _partition, offset) => {
          offsets.push(offset)
          if (offsets.length === 1) pause()
          throw failure(true)
        },
        { records: { 'topic-a|0': [40] } }
      )
      await advanceUntil(() => run.runner.partitionRetries.size === 1)
      run.state.queue['topic-a|0'] = [41]
      run.consumer.resume([{ topic: 'topic-a', partitions: [0] }])
      await advanceUntil(() => run.crashes.length > 0)
      expect(offsets).toEqual(['40', '41', '41'])
      expect(run.crashes[0].error).toMatchObject({ retryCount: 1 })
    }
  )

  it.each([
    ['seek', 'eachMessage'],
    ['seek', 'eachBatch'],
    ['offset reset', 'eachMessage'],
    ['offset reset', 'eachBatch'],
  ])(
    '%s during %s prevents the old failure from spending the new budget',
    async (change, handler) => {
      const held = deferred()
      const error = failure(true)
      const offsets = []
      const run = await startRetryRun(
        'eager',
        handler,
        async (_payload, _partition, offset) => {
          offsets.push(offset)
          if (offsets.length === 1) {
            await held.promise
            if (change === 'seek')
              run.consumer.seek({ topic: 'topic-a', partition: 0, offset: '41' })
          }
          throw error
        },
        { records: { 'topic-a|0': [40, 41] }, run: { autoCommit: false } }
      )
      try {
        await advanceUntil(() => offsets.length === 1)
        if (change === 'offset reset') {
          await run.group.recoverFromOffsetOutOfRange({ topic: 'topic-a', partition: 0 })
        }
        held.resolve()
        await advanceUntil(() => run.crashes.length > 0)
        expect(offsets).toEqual(change === 'seek' ? ['40', '41', '41'] : ['40', '40', '40'])
        expect(run.crashes[0].error).toMatchObject({ retryCount: 1, cause: error })
        expect(run.runner.partitionRetries.size).toBe(0)
      } finally {
        held.resolve()
      }
    }
  )

  it.each(['eachMessage', 'eachBatch'])(
    '%s clears its record after each accepted offset',
    async (handler) => {
      const calls = new Map()
      const records = Array.from({ length: 12 }, (_, index) => 40 + index)
      const run = await startRetryRun(
        'eager',
        handler,
        (_payload, _partition, offset) => {
          const count = calls.get(offset) ?? 0
          calls.set(offset, count + 1)
          if (count === 0) throw failure(true)
        },
        { records: { 'topic-a|0': records }, run: { autoCommit: false } }
      )
      await advanceUntil(() => calls.get('51') === 2, 10000)
      await advanceUntil(() => run.runner.partitionRetries.size === 0)
      expect([...calls.values()]).toEqual(records.map(() => 2))
      expect(run.runner.partitionRetries.size).toBe(0)
      expect(run.crashes).toEqual([])
      await closeClean(run)
    }
  )

  it('keeps the failed offset after a partial manual batch resolution', async () => {
    const ranges = []
    const run = await startRetryRun(
      'eager',
      'eachBatch',
      ({ batch, resolveOffset, pause }) => {
        ranges.push(batch.messages.map(({ offset }) => offset))
        if (ranges.length === 2) {
          resolveOffset('40')
          pause()
          return
        }
        throw failure(true)
      },
      {
        prepare: ({ state }) => {
          state.batchSize = 2
        },
        records: { 'topic-a|0': [40, 41] },
        run: { autoCommit: false, eachBatchAutoResolve: false },
      }
    )
    await advanceUntil(() => ranges.length === 2)
    expect(run.runner.partitionRetries.size).toBe(1)
    run.consumer.resume([{ topic: 'topic-a', partitions: [0] }])
    await advanceUntil(() => run.crashes.length > 0)
    expect(ranges).toEqual([['40', '41'], ['40', '41'], ['41']])
    expect(run.crashes[0].error).toMatchObject({ retryCount: 1 })
  })

  it.each([
    ['one broker', false],
    ['two brokers', true],
  ])('charges a concurrent failure on %s even when its sibling pauses', async (_kind, split) => {
    const calls = [0, 0]
    const gate = deferred()
    const error = failure(true)
    const run = await startRetryRun(
      'eager',
      'eachMessage',
      async ({ pause }, partition) => {
        calls[partition] += 1
        await gate.promise
        if (partition === 0) pause()
        throw error
      },
      {
        partitions: [0, 1],
        records: { 'topic-a|0': [40], 'topic-a|1': [40] },
        prepare: split ? splitBrokers : undefined,
        run: { partitionsConsumedConcurrently: 2, autoCommit: false },
      }
    )
    try {
      await advanceUntil(() => calls.every(Boolean))
      expect(calls).toEqual([1, 1])
      gate.resolve()
      await advanceUntil(() => run.runner.partitionRetries.size === 2)
      expect(run.runner.partitionRetries.size).toBe(2)
      await advanceUntil(() => run.crashes.length > 0)
      expect(calls).toEqual([1, 2])
      expect(run.crashes[0].error).toMatchObject({ retryCount: 1, cause: error })
    } finally {
      gate.resolve()
    }
  })

  it.each([
    ['one broker', 0, false],
    ['one broker', 1, false],
    ['two brokers', 0, true],
    ['two brokers', 1, true],
  ])(
    'reports a concurrent non-retriable failure on %s at partition %i',
    async (_kind, permanent, split) => {
      const calls = []
      const gate = deferred()
      const error = failure(false)
      const run = await startRetryRun(
        'eager',
        'eachBatch',
        async (_payload, partition) => {
          calls.push(partition)
          await gate.promise
          throw partition === permanent ? error : failure(true)
        },
        {
          partitions: [0, 1],
          records: { 'topic-a|0': [40], 'topic-a|1': [40] },
          prepare: split ? splitBrokers : undefined,
          run: { partitionsConsumedConcurrently: 2, autoCommit: false },
        }
      )
      try {
        await advanceUntil(() => calls.length === 2)
        expect(calls).toHaveLength(2)
        gate.resolve()
        await advanceUntil(() => run.crashes.length > 0)
        expect(calls).toHaveLength(2)
        expect(run.crashes[0].error).toMatchObject({
          name: 'KafkaJSNonRetriableError',
          cause: error,
        })
      } finally {
        gate.resolve()
      }
    }
  )

  it.each(['stop', 'disconnect'])(
    '%s cancels handler backoff and releases the record',
    async (method) => {
      const run = await startRetryRun('eager', 'eachMessage', () => {
        throw failure(true)
      })
      await advanceUntil(() => run.runner.cancelHandlerBackoff !== null)
      expect(run.runner.partitionRetries.size).toBe(1)
      const closing = settled(run.consumer[method]())
      await advanceUntil(() => closing.done)
      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.runner.partitionRetries.size).toBe(0)
      expect(run.runner.cancelHandlerBackoff).toBeNull()
      expect(run.crashes).toEqual([])
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  /**
   * Starts an eager run whose handler pauses topic-a/0 and fails, and waits for the failure record.
   * @returns {Promise<{ run: object, token: symbol }>} the run and the failed assignment's token
   */
  const pausedFailure = async () => {
    const run = await startRetryRun('eager', 'eachMessage', ({ pause }) => {
      pause()
      throw failure(true)
    })
    await advanceUntil(() => run.runner.partitionRetries.size === 1)
    expect(run.consumer.paused()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    return { run, token: run.group.acquired.get('topic-a|0').assignmentToken }
  }

  it.each(['seek', 'truncate'])(
    '%s clears a failed partition while it stays paused',
    async (change) => {
      const { run, token } = await pausedFailure()
      if (change === 'seek') run.consumer.seek({ topic: 'topic-a', partition: 0, offset: '40' })
      if (change === 'truncate') {
        await run.group.applyValidationOutcome('topic-a', 0, '40', {
          action: 'truncated',
          divergingOffset: '39',
        })
      }
      expect(run.runner.partitionRetries.size).toBe(0)
      expect(run.group.acquired.get('topic-a|0').assignmentToken).not.toBe(token)
      expect(run.consumer.paused()).toEqual([{ topic: 'topic-a', partitions: [0] }])
      await closeClean(run)
    }
  )

  it('recreate clears the record of a paused failed partition, and the pause goes with the old topic', async () => {
    const { run, token } = await pausedFailure()
    run.group.invalidateTopic('topic-a')
    expect(run.runner.partitionRetries.size).toBe(0)
    expect(run.group.acquired.get('topic-a|0').assignmentToken).not.toBe(token)
    expect(run.consumer.paused()).toEqual([])
    await closeClean(run)
  })
})

describe('Consumer > how a classic run ends', () => {
  describe('one cause alone', () => {
    it.each([
      [
        'stop()',
        'a dynamic',
        true,
        ['final commit', 'commit topic-a|0 42', 'leave member-1 null', 'STOP'],
      ],
      ['stop()', 'a static', true, ['final commit', 'commit topic-a|0 42', 'STOP']],
      ['stop()', 'a dynamic', false, ['final commit', 'leave member-1 null', 'STOP']],
      [
        'disconnect()',
        'a dynamic',
        true,
        ['final commit', 'commit topic-a|0 42', 'leave member-1 null', 'STOP', 'DISCONNECT'],
      ],
      [
        'disconnect({ leaveGroup: true })',
        'a static',
        true,
        ['final commit', 'commit topic-a|0 42', 'leave member-1 instance-1', 'STOP', 'DISCONNECT'],
      ],
    ])(
      '%s of %s idle member with autoCommit %s and offset 42 resolved ends with this trail',
      async (cause, kind, autoCommit, expected) => {
        const run = await begin({ instance: kind === 'a static', run: { autoCommit } })
        await advanceUntil(() => run.delivered.length === 2)
        await jest.advanceTimersByTimeAsync(200)
        expect(trail(run)).toEqual([])

        const ending = settled(
          cause === 'stop()'
            ? run.consumer.stop()
            : run.consumer.disconnect(cause === 'disconnect()' ? undefined : { leaveGroup: true })
        )
        await advanceUntil(() => ending.done, 2000)

        expect(ending).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual(expected)
        expect(run.crashes).toEqual([])
        expect(run.group.memberId).toBeNull()
        await jest.advanceTimersByTimeAsync(1000)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it.each([
      [
        'a dynamic',
        true,
        [
          'commit topic-a|0 41',
          'final commit',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          'CRASH restart false',
        ],
      ],
      [
        'a static',
        true,
        ['commit topic-a|0 41', 'final commit', 'STOP', 'DISCONNECT', 'CRASH restart false'],
      ],
      [
        'a dynamic',
        false,
        ['final commit', 'leave member-1 null', 'STOP', 'DISCONNECT', 'CRASH restart false'],
      ],
    ])(
      'a handler error of %s member with autoCommit %s commits the progress before it, cleans up through the stages of stop() and disconnects before CRASH',
      async (kind, autoCommit, expected) => {
        const error = failure(false)
        const run = await begin({
          instance: kind === 'a static',
          run: { autoCommit },
          eachMessage: async ({ message }, held) => {
            if (message.offset !== '41') return
            await held.promise
            throw error
          },
        })
        await advanceUntil(() => run.delivered.length === 2)
        expect(trail(run)).toEqual([])
        run.held.resolve()
        await advanceUntil(() => run.crashes.length > 0)
        await jest.advanceTimersByTimeAsync(1000)

        expect(trail(run)).toEqual(expected)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.name).toBe('KafkaJSNonRetriableError')
        expect(run.crashes[0].error.cause).toBe(error)
        expect(run.cluster.disconnect.mock.calls).toEqual([[{ preserveSetupState: true }]])
        expect(run.cluster.connect).toHaveBeenCalledTimes(1)
        expect(run.group.memberId).toBeNull()
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it('a start failure before the run is running commits and leaves nothing, emits STOP and then DISCONNECT before CRASH, and restarts by the crash rules', async () => {
      const error = new KafkaJSError('the connection failed', { retriable: true })
      const connect = jest.spyOn(ConsumerGroup.prototype, 'connect').mockRejectedValueOnce(error)
      const transport = parkTransport()
      const run = await startClassic('classic', {
        config: { retry: RETRY },
        trace: true,
        prepare: transport.prepare,
        until: transport.parked,
      })

      await jest.advanceTimersByTimeAsync(1000)
      expect(startupTrail(run)).toEqual(['STOP', 'transport disconnect'])
      expect(run.started.done).toBe(false)
      expect(run.crashes).toEqual([])

      transport.release()
      await advanceUntil(() => run.started.done, 2000)

      expect(run.started).toMatchObject({ done: true, value: undefined })
      expect(startupTrail(run)).toEqual([
        'STOP',
        'transport disconnect',
        'DISCONNECT',
        'CRASH restart true',
        'run() settled',
      ])
      expect(run.crashes.map((crash) => [crash.error, crash.restart])).toEqual([[error, true]])
      expect(run.cluster.disconnect.mock.calls).toEqual([[{ preserveSetupState: true }]])
      expect(run.coordinator.joinGroup).not.toHaveBeenCalled()
      expect(run.coordinator.offsetCommit).not.toHaveBeenCalled()
      expect(run.coordinator.leaveGroup).not.toHaveBeenCalled()

      await expectRestart(run, true)
      expect(connect).toHaveBeenCalledTimes(2)
      await advanceUntil(() => run.coordinator.joinGroup.mock.calls.length > 0, 1000)
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
    }, 30000)
  })

  describe('two ways to stop, one after the other', () => {
    it.each([
      ['stop()', 'disconnect()'],
      ['disconnect()', 'stop()'],
    ])(
      '%s then %s share one stop: one final commit and one LeaveGroup, stop() settles at STOP and disconnect() after the cluster teardown',
      async (first, second) => {
        const run = await begin()
        await advanceUntil(() => run.delivered.length === 2)
        await jest.advanceTimersByTimeAsync(200)
        const transport = deferred()
        run.state.disconnecting = transport.promise
        const call = (name) =>
          settled(name === 'stop()' ? run.consumer.stop() : run.consumer.disconnect())
        const calls = { [first]: call(first) }
        calls[second] = call(second)
        await advanceUntil(() => calls['stop()'].done, 2000)
        await jest.advanceTimersByTimeAsync(100)

        expect(calls['stop()']).toMatchObject({ done: true, value: undefined })
        expect(calls['disconnect()'].done).toBe(false)
        expect(trail(run)).toEqual([
          'final commit',
          'commit topic-a|0 42',
          'leave member-1 null',
          'STOP',
        ])
        expect(run.cluster.disconnect.mock.calls).toEqual([[]])

        transport.resolve()
        await advanceUntil(() => calls['disconnect()'].done, 1000)

        expect(calls['disconnect()']).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual([
          'final commit',
          'commit topic-a|0 42',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
        ])
        await jest.advanceTimersByTimeAsync(1000)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it('a disconnect({ timeout }) that joins a disconnect() without one does not bound it: both wait for the running handler', async () => {
      const run = await begin({
        eachMessage: async ({ message }, held) => {
          if (message.offset === '41') await held.promise
        },
      })
      await advanceUntil(() => run.delivered.length === 2)
      const plain = settled(run.consumer.disconnect())
      const bounded = settled(run.consumer.disconnect({ timeout: 100 }))
      await jest.advanceTimersByTimeAsync(1000)

      expect([plain.done, bounded.done]).toEqual([false, false])
      expect(trail(run)).toEqual([])

      run.held.resolve()
      await advanceUntil(() => plain.done && bounded.done, 2000)

      expect(plain).toMatchObject({ done: true, value: undefined })
      expect(bounded).toMatchObject({ done: true, value: undefined })
      expect(trail(run)).toEqual([
        'final commit',
        'commit topic-a|0 42',
        'leave member-1 null',
        'STOP',
        'DISCONNECT',
      ])
      expect(run.cluster.disconnect.mock.calls).toEqual([[]])
      await jest.advanceTimersByTimeAsync(1000)
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)
  })

  describe('a handler error while a sibling handler of another partition is parked', () => {
    it.each([
      ['returns', ['final commit', 'commit topic-a|1 41', 'leave member-1 null']],
      ['fails too', ['final commit', 'leave member-1 null']],
    ])(
      'is reported only once the sibling %s, and the final commit carries what the sibling resolved',
      async (sibling, cleanup) => {
        const first = failure(false)
        const second = failure(false)
        const run = await begin({
          partitions: [0, 1],
          records: { 'topic-a|0': [40], 'topic-a|1': [40] },
          run: { partitionsConsumedConcurrently: 2 },
          eachMessage: async ({ partition }, held) => {
            if (partition === 0) throw first
            await held.promise
            if (sibling === 'fails too') throw second
          },
        })
        await advanceUntil(() => run.delivered.length === 2)
        await jest.advanceTimersByTimeAsync(5000)

        expect([...run.delivered].sort()).toEqual(['0|40', '1|40'])
        expect(trail(run)).toEqual([])
        expect(run.crashes).toEqual([])

        run.held.resolve()
        await advanceUntil(() => run.crashes.length > 0)
        await jest.advanceTimersByTimeAsync(1000)

        expect(trail(run)).toEqual([...cleanup, 'STOP', 'DISCONNECT', 'CRASH restart false'])
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.cause).toBe(first)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )
  })

  describe('the restart after a handler error', () => {
    const POLICIES = {
      absent: undefined,
      'returning true': async () => true,
      'returning false': async () => false,
      rejecting: async () => {
        throw new Error('the policy failed')
      },
    }
    const CLEANUP = [
      'commit topic-a|0 41',
      'final commit',
      'leave member-1 null',
      'STOP',
      'DISCONNECT',
    ]

    it.each([
      ['retriable', 'absent', true, 'KafkaJSNumberOfRetriesExceeded'],
      ['retriable', 'returning true', true, 'KafkaJSNumberOfRetriesExceeded'],
      ['retriable', 'returning false', false, 'KafkaJSNumberOfRetriesExceeded'],
      ['retriable', 'rejecting', true, 'KafkaJSNumberOfRetriesExceeded'],
      ['non-retriable', 'absent', false, 'KafkaJSNonRetriableError'],
      ['non-retriable', 'returning true', true, 'KafkaJSNonRetriableError'],
      ['non-retriable', 'returning false', false, 'KafkaJSNonRetriableError'],
    ])(
      'a %s error with restartOnFailure %s reports restart %s, asks the callback after cleanup and restarts 300 ms after CRASH only then',
      async (retriability, policyName, restart, wrapper) => {
        const error = failure(retriability === 'retriable')
        const seen = {}
        const policy = POLICIES[policyName] && jest.fn(POLICIES[policyName])
        const run = await begin({
          retry: {
            ...RETRY,
            ...(policy && {
              restartOnFailure: async (crash) => {
                seen.atPolicy = trail(seen.run)
                return policy(crash)
              },
            }),
          },
          eachMessage: async ({ message }, held, call) => {
            if (message.offset !== '41' || call !== 2) return
            await held.promise
            throw error
          },
        })
        seen.run = run
        await advanceUntil(() => run.delivered.length === 2)
        expect(trail(run)).toEqual([])
        run.held.resolve()
        await advanceUntil(() => run.crashes.length > 0)

        expect(trail(run)).toEqual([...CLEANUP, `CRASH restart ${restart}`])
        expect(run.crashes[0].error.name).toBe(wrapper)
        expect(run.crashes[0].error.cause).toBe(error)
        if (policy) {
          expect(policy).toHaveBeenCalledTimes(1)
          expect(seen.atPolicy).toEqual(CLEANUP)
        }

        await expectRestart(run, restart)
        if (restart) {
          await advanceUntil(() => run.delivered.length === 3, 2000)
          expect(run.delivered).toEqual(['0|40', '0|41', '0|41'])
          await closeClean(run)
        } else {
          expect(jest.getTimerCount()).toBe(0)
        }
      },
      30000
    )

    it('a crash error that carries its own retryTime restarts after it, not after initialRetryTime', async () => {
      const run = await begin({ retry: RETRY })
      await advanceUntil(() => run.delivered.length === 2)
      const error = Object.assign(new KafkaJSError('a transient failure', { retriable: true }), {
        retryTime: 700,
      })

      run.group.stopRun(error)
      await advanceUntil(() => run.crashes.length > 0)

      expect(trail(run)).toEqual(['STOP', 'DISCONNECT', 'CRASH restart true'])
      expect(run.crashes[0].error).toBe(error)
      await expectRestart(run, true, 700)
      await advanceUntil(() => run.coordinator.joinGroup.mock.calls.length > 1, 1000)
      await closeClean(run)
    }, 30000)
  })

  describe('a handler error after another end began while the handler ran', () => {
    it.each([
      ['stop()', ['commit topic-a|0 41', 'final commit', 'leave member-1 null', 'STOP'], [], false],
      [
        'disconnect()',
        ['commit topic-a|0 41', 'final commit', 'leave member-1 null', 'STOP', 'DISCONNECT'],
        [[]],
        false,
      ],
      ['disconnect({ timeout: 500 })', ['STOP', 'DISCONNECT'], [[{ drainTimeout: 0 }]], true],
    ])(
      'suppresses the handler failure while %s is in progress',
      async (cause, cleanup, transport, timed) => {
        const error = failure(false)
        const policy = jest.fn(async () => true)
        const run = await begin({
          retry: { ...RETRY, restartOnFailure: policy },
          eachMessage: async ({ message }, held) => {
            if (message.offset !== '41') return
            await held.promise
            throw error
          },
        })
        await advanceUntil(() => run.delivered.length === 2)
        const ending = settled(
          cause === 'stop()'
            ? run.consumer.stop()
            : run.consumer.disconnect(cause === 'disconnect()' ? undefined : { timeout: 500 })
        )
        await jest.advanceTimersByTimeAsync(1000)

        if (timed) {
          expect(ending).toMatchObject({ done: true, value: undefined })
          expect(trail(run)).toEqual(cleanup)
        } else {
          expect(ending.done).toBe(false)
          expect(trail(run)).toEqual([])
        }

        run.held.resolve()
        await advanceUntil(() => ending.done, 2000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(ending).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual(cleanup)
        expect(run.crashes).toEqual([])
        expect(policy).not.toHaveBeenCalled()

        expect(run.cluster.disconnect.mock.calls).toEqual(transport)
        expect(run.cluster.connect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )
  })

  describe("another cause while a crash's close callback runs", () => {
    it.each([
      [
        'nothing else',
        [
          'commit topic-a|0 41',
          'revoked callback',
          'final commit',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          'CRASH restart true',
        ],
        [[{ preserveSetupState: true }]],
      ],
      [
        'stop()',
        [
          'commit topic-a|0 41',
          'revoked callback',
          'final commit',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          'CRASH restart false',
        ],
        [[{ preserveSetupState: true }]],
      ],
      [
        'disconnect()',
        [
          'commit topic-a|0 41',
          'revoked callback',
          'final commit',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          'CRASH restart false',
        ],
        [[]],
      ],
      [
        'disconnect({ timeout: 500 })',
        ['commit topic-a|0 41', 'revoked callback', 'STOP', 'DISCONNECT', 'CRASH restart false'],
        [[{ drainTimeout: 0 }]],
      ],
      [
        'a topic recreation',
        ['commit topic-a|0 41', 'revoked callback', 'STOP', 'DISCONNECT', 'CRASH restart true'],
        [[{ preserveSetupState: true }]],
      ],
    ])(
      'with %s arriving, a retriable handler error ends with this trail and reports the handler error',
      async (second, expected, transport) => {
        const error = failure(true)
        const callback = deferred()
        const run = await begin({
          retry: RETRY,
          closeCallback: () => callback.promise,
          eachMessage: async ({ message }, held, call) => {
            if (message.offset !== '41' || call !== 2) return
            await held.promise
            throw error
          },
        })
        await advanceUntil(() => run.delivered.length === 2)
        expect(trail(run)).toEqual([])
        run.held.resolve()
        await advanceUntil(() => trail(run).includes('revoked callback'))
        expect(trail(run)).toEqual(['commit topic-a|0 41', 'revoked callback'])

        let ending = null
        if (second === 'stop()') ending = settled(run.consumer.stop())
        if (second === 'disconnect()') ending = settled(run.consumer.disconnect())
        if (second === 'disconnect({ timeout: 500 })')
          ending = settled(run.consumer.disconnect({ timeout: 500 }))
        if (second === 'a topic recreation') run.group.stopRun(recreated())
        await advanceUntil(() => run.crashes.length > 0, 1000)
        callback.resolve()
        await advanceUntil(() => run.crashes.length > 0 && (ending === null || ending.done), 2000)

        expect(trail(run)).toEqual(expected)
        if (ending) expect(ending).toMatchObject({ done: true, value: undefined })
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.name).toBe('KafkaJSNumberOfRetriesExceeded')
        expect(run.crashes[0].error.cause).toBe(error)
        expect(run.cluster.disconnect.mock.calls).toEqual(transport)

        const restart = run.crashes[0].restart
        await expectRestart(run, restart)
        if (restart) {
          await advanceUntil(() => run.coordinator.joinGroup.mock.calls.length > 1, 2000)
          await closeClean(run)
        } else {
          expect(jest.getTimerCount()).toBe(0)
        }
      },
      30000
    )
  })

  describe('a poll-interval expiry while the handler is stuck', () => {
    /**
     * Starts a member whose first handler call parks on `held` and then runs `settle`, and waits
     * for the expiry.
     * @param {object} [options]
     * @param {object} [options.retry] the consumer's retry options
     * @param {(payload: object, run: object) => Promise<void>} [options.settle] what the first
     *   handler call does once `held` resolves
     * @param {(run: object) => void} [options.beforeExpiry] runs once the member is started
     */
    const expire = async ({ instance = false, retry, settle, beforeExpiry } = {}) => {
      const ref = {}
      const run = await begin({
        instance,
        retry,
        config: { maxPollInterval: 4000, heartbeatInterval: 500 },
        records: { 'topic-a|0': [40] },
        eachMessage: async (payload, held, call) => {
          if (call !== 1) return
          await held.promise
          await settle?.(payload, ref.run)
        },
      })
      ref.run = run
      beforeExpiry?.(run)
      await advanceUntil(() => trail(run).includes('POLL_INTERVAL_EXCEEDED'), 10000)
      return run
    }

    it.each([
      ['a dynamic', false, ['POLL_INTERVAL_EXCEEDED', 'leave member-1 null']],
      ['a static', true, ['POLL_INTERVAL_EXCEEDED']],
    ])(
      'alone, %s member leaves at once as this row says, commits nothing and rejoins once the handler returns',
      async (_kind, instance, expected) => {
        const run = await expire({ instance })
        await jest.advanceTimersByTimeAsync(1000)

        expect(trail(run)).toEqual(expected)
        expect(run.log.find(({ kind }) => kind === 'POLL').isStatic).toBe(instance)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)

        run.held.resolve()
        await advanceUntil(() => run.delivered.length === 2, 5000)
        await jest.advanceTimersByTimeAsync(200)

        expect(trail(run)).toEqual(expected)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
        expect(run.delivered).toEqual(['0|40', '0|40'])
        expect(run.crashes).toEqual([])
        await closeClean(run)
      },
      30000
    )

    it.each([
      ['a non-retriable', 'KafkaJSNonRetriableError', () => failure(false)],
      ['a TypeError', 'KafkaJSNonRetriableError', () => new TypeError('the handler failed')],
      ['a retriable', 'KafkaJSNumberOfRetriesExceeded', () => failure(true)],
      ['a plain Error', 'KafkaJSNumberOfRetriesExceeded', () => new Error('the handler failed')],
    ])(
      'then %s failure of that handler is reported after the rejoin as %s: one CRASH, restartOnFailure asked once, no commit and a second LeaveGroup from the crash cleanup',
      async (_kind, wrapper, make) => {
        const error = make()
        const policy = jest.fn(async () => false)
        const run = await expire({
          retry: { ...RETRY, restartOnFailure: policy },
          settle: async () => {
            throw error
          },
        })
        await jest.advanceTimersByTimeAsync(1000)
        expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])
        expect(run.crashes).toEqual([])

        run.held.resolve()
        await advanceUntil(() => run.crashes.length > 0, 5000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(trail(run)).toEqual([
          'POLL_INTERVAL_EXCEEDED',
          'leave member-1 null',
          'final commit',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          'CRASH restart false',
        ])
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
        expect(run.delivered).toEqual(['0|40'])
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.name).toBe(wrapper)
        expect(run.crashes[0].error.cause).toBe(error)
        expect(policy).toHaveBeenCalledTimes(1)
        expect(policy.mock.calls[0][0]).toBe(run.crashes[0].error)
        expect(run.started).toMatchObject({ done: true, value: undefined })
        expect(run.cluster.connect).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it('then a retriable failure each time the handler overruns spends one retry per rejoin, and CRASH reports KafkaJSNumberOfRetriesExceeded once the retries run out', async () => {
      const error = failure(true)
      const policy = jest.fn(async () => false)
      const run = await begin({
        config: { maxPollInterval: 4000, heartbeatInterval: 500 },
        retry: { ...RETRY, retries: 1, restartOnFailure: policy },
        records: { 'topic-a|0': [40] },
        eachMessage: async (_payload, held) => {
          await held.promise
          await new Promise((resolve) => setTimeout(resolve, 6000))
          throw error
        },
      })
      expect(trail(run)).toEqual([])
      run.held.resolve()
      await advanceUntil(() => run.delivered.length === 2, 15000)

      expect(run.crashes).toEqual([])
      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)

      await advanceUntil(() => run.crashes.length > 0, 15000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(trail(run)).toEqual([
        'POLL_INTERVAL_EXCEEDED',
        'leave member-1 null',
        'POLL_INTERVAL_EXCEEDED',
        'leave member-1 null',
        'final commit',
        'leave member-1 null',
        'STOP',
        'DISCONNECT',
        'CRASH restart false',
      ])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(run.delivered).toEqual(['0|40', '0|40'])
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(run.crashes[0].error.cause).toBe(error)
      expect(run.crashes[0].error.retryCount).toBe(1)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(policy.mock.calls[0][0]).toBe(run.crashes[0].error)
      expect(jest.getTimerCount()).toBe(0)
    }, 60000)

    it('then a failure of that handler rethrowing the refusal of its own commit stays part of the rejoin: no CRASH, and the record is redelivered', async () => {
      let refused = null
      const policy = jest.fn(async () => false)
      const run = await expire({
        retry: { ...RETRY, restartOnFailure: policy },
        settle: async ({ topic, partition }, started) => {
          try {
            await started.consumer.commitOffsets([{ topic, partition, offset: '41' }])
          } catch (error) {
            refused = error
            throw error
          }
        },
      })
      await jest.advanceTimersByTimeAsync(1000)
      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])

      run.held.resolve()
      await advanceUntil(() => run.delivered.length === 2, 5000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(refused?.name).toBe('KafkaJSCommitFailedError')
      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
      expect(run.delivered).toEqual(['0|40', '0|40'])
      expect(run.crashes).toEqual([])
      expect(policy).not.toHaveBeenCalled()
      await closeClean(run)
    }, 30000)

    /**
     * Parks the expiry's LeaveGroup at its coordinator lookup, before its write.
     * @returns {{ parked: () => boolean, release: () => void }}
     */
    const parkPollLeave = (run) => {
      const membership = run.group.groupMembership
      const live = membership.liveCoordinator.bind(membership)
      const gate = deferred()
      const state = { armed: false, parked: false }
      run.consumer.on(run.consumer.events.POLL_INTERVAL_EXCEEDED, () => {
        state.armed = true
      })
      jest.spyOn(membership, 'liveCoordinator').mockImplementation(async (operation) => {
        if (state.armed && !state.parked) {
          state.parked = true
          await gate.promise
        }
        return live(operation)
      })
      return { parked: () => state.parked, release: () => gate.resolve() }
    }

    it.each([
      [
        'stop()',
        'after',
        ['POLL_INTERVAL_EXCEEDED', 'leave member-1 null', 'final commit', 'STOP'],
        1,
      ],
      [
        'stop()',
        'before',
        [
          'POLL_INTERVAL_EXCEEDED',
          'final commit',
          'leave member-1 null',
          'STOP',
          'leave member-1 null',
        ],
        2,
      ],
      [
        'disconnect({ timeout: 500 })',
        'after',
        ['POLL_INTERVAL_EXCEEDED', 'leave member-1 null', 'STOP', 'DISCONNECT'],
        1,
      ],
      [
        'disconnect({ timeout: 500 })',
        'before',
        ['POLL_INTERVAL_EXCEEDED', 'STOP', 'DISCONNECT'],
        0,
      ],
    ])(
      'then %s, arriving %s the expiry LeaveGroup is written, ends with this trail and that many LeaveGroups',
      async (cause, when, expected, leaves) => {
        let park = null
        const run = await expire({
          beforeExpiry: (started) => {
            if (when === 'before') park = parkPollLeave(started)
          },
        })
        if (park) await advanceUntil(park.parked, 1000)
        await jest.advanceTimersByTimeAsync(200)
        expect(park?.parked() ?? true).toBe(true)
        expect(trail(run)).toEqual(
          when === 'before'
            ? ['POLL_INTERVAL_EXCEEDED']
            : ['POLL_INTERVAL_EXCEEDED', 'leave member-1 null']
        )

        const ending = settled(
          cause === 'stop()' ? run.consumer.stop() : run.consumer.disconnect({ timeout: 500 })
        )
        await jest.advanceTimersByTimeAsync(1000)
        run.held.resolve()
        await advanceUntil(() => ending.done, 2000)
        park?.release()
        await jest.advanceTimersByTimeAsync(1000)

        expect(ending).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual(expected)
        expect(run.coordinator.leaveGroup).toHaveBeenCalledTimes(leaves)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
        expect(run.crashes).toEqual([])
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it('then disconnect({ timeout: 500 }) with the expiry LeaveGroup written and its reply still held settles at once, and the late reply changes nothing', async () => {
      const reply = deferred()
      const holdReply = ({ coordinator, log }) =>
        coordinator.leaveGroup.mockImplementation(async ({ memberId, groupInstanceId }) => {
          log.push({ kind: 'leave', memberId, groupInstanceId })
          await reply.promise
        })
      const run = await expire({ beforeExpiry: holdReply })
      await jest.advanceTimersByTimeAsync(200)
      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])
      expect(run.coordinator.leaveGroup).toHaveBeenCalledTimes(1)

      const ending = settled(run.consumer.disconnect({ timeout: 500 }))
      await jest.advanceTimersByTimeAsync(1000)

      const ended = ['POLL_INTERVAL_EXCEEDED', 'leave member-1 null', 'STOP', 'DISCONNECT']
      expect(ending).toMatchObject({ done: true, value: undefined })
      expect(trail(run)).toEqual(ended)
      expect(run.cluster.disconnect.mock.calls).toEqual([[{ drainTimeout: 0 }]])

      reply.resolve()
      run.held.resolve()
      await jest.advanceTimersByTimeAsync(2000)

      expect(trail(run)).toEqual(ended)
      expect(run.coordinator.leaveGroup).toHaveBeenCalledTimes(1)
      expect(run.coordinator.leaveGroup.mock.calls[0][0]).toMatchObject({ memberId: 'member-1' })
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
      expect(run.log.filter(({ kind }) => kind === 'commit')).toEqual([])
      expect(run.crashes).toEqual([])
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)
  })

  describe('a FENCED_INSTANCE_ID heartbeat answer to a static member', () => {
    it('alone, while idle, drops the resolved offset, sends no LeaveGroup and reports CRASH with restart false although restartOnFailure says true', async () => {
      const policy = jest.fn(async () => true)
      const run = await begin({
        instance: true,
        config: { heartbeatInterval: 500 },
        retry: { ...RETRY, restartOnFailure: policy },
      })
      await advanceUntil(() => run.delivered.length === 2)
      await jest.advanceTimersByTimeAsync(200)
      run.coordinator.heartbeat.mockImplementation(async () => {
        throw protocolError(FENCED_INSTANCE_ID)
      })
      await advanceUntil(() => run.crashes.length > 0, 5000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(trail(run)).toEqual(['final commit', 'STOP', 'DISCONNECT', 'CRASH restart false'])
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error.code).toBe(FENCED_INSTANCE_ID)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(run.group.memberId).toBeNull()
      expect(run.cluster.connect).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)

    it('then stop() while the handler runs drops the verdict: the run ends with STOP alone, no commit and no CRASH', async () => {
      const policy = jest.fn(async () => true)
      const { logger, lines } = recordingLogger()
      const run = await begin({
        instance: true,
        config: { heartbeatInterval: 500, logger },
        retry: { ...RETRY, restartOnFailure: policy },
        eachMessage: async ({ message }, held) => {
          if (message.offset === '41') await held.promise
        },
      })
      await advanceUntil(() => run.delivered.length === 2)
      const sent = run.coordinator.heartbeat.mock.calls.length
      run.coordinator.heartbeat.mockImplementation(async () => {
        throw protocolError(FENCED_INSTANCE_ID)
      })
      await advanceUntil(() => run.coordinator.heartbeat.mock.calls.length > sent, 2000)
      await jest.advanceTimersByTimeAsync(100)
      expect(run.coordinator.heartbeat.mock.calls.length).toBeGreaterThan(sent)
      expect(trail(run)).toEqual([])

      const stopping = settled(run.consumer.stop())
      await jest.advanceTimersByTimeAsync(1000)
      expect(stopping.done).toBe(false)

      run.held.resolve()
      await advanceUntil(() => stopping.done, 2000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(trail(run)).toEqual(['final commit', 'STOP'])
      expect(run.crashes).toEqual([])
      expect(policy).not.toHaveBeenCalled()
      expect(lines.filter((line) => line.startsWith('Crash: '))).toEqual([])
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)

    it.each([
      ['disconnect()', ['final commit', 'STOP', 'DISCONNECT'], null],
      ['disconnect({ timeout: 500 })', ['STOP', 'DISCONNECT'], null],
      [
        'a failure of that handler',
        ['final commit', 'STOP', 'DISCONNECT', 'CRASH restart false'],
        'the fenced answer',
      ],
      ['a topic recreation', ['STOP', 'DISCONNECT', 'CRASH restart false'], 'the recreation'],
    ])(
      'queued while the handler runs, then %s, ends with this trail and no commit',
      async (second, expected, reported) => {
        const policy = jest.fn(async () => true)
        const failed = failure(false)
        const run = await begin({
          instance: true,
          config: { heartbeatInterval: 500 },
          retry: { ...RETRY, restartOnFailure: policy },
          eachMessage: async ({ message }, held) => {
            if (message.offset !== '41') return
            await held.promise
            if (second === 'a failure of that handler') throw failed
          },
        })
        await advanceUntil(() => run.delivered.length === 2)
        const sent = run.coordinator.heartbeat.mock.calls.length
        run.coordinator.heartbeat.mockImplementation(async () => {
          throw protocolError(FENCED_INSTANCE_ID)
        })
        await advanceUntil(() => run.coordinator.heartbeat.mock.calls.length > sent, 2000)
        await jest.advanceTimersByTimeAsync(100)
        expect(run.coordinator.heartbeat.mock.calls.length).toBeGreaterThan(sent)
        expect(trail(run)).toEqual([])

        const error = recreated()
        let ending = null
        if (second === 'disconnect()') ending = settled(run.consumer.disconnect())
        if (second === 'disconnect({ timeout: 500 })')
          ending = settled(run.consumer.disconnect({ timeout: 500 }))
        if (second === 'a topic recreation') run.group.stopRun(error)
        await jest.advanceTimersByTimeAsync(1000)
        if (second === 'disconnect({ timeout: 500 })') {
          expect(ending).toMatchObject({ done: true, value: undefined })
          expect(trail(run)).toEqual(expected)
        } else {
          expect(trail(run)).toEqual([])
        }

        run.held.resolve()
        await advanceUntil(
          () => (ending === null || ending.done) && (reported === null || run.crashes.length > 0),
          2000
        )
        await jest.advanceTimersByTimeAsync(1000)

        if (ending) expect(ending).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual(expected)
        expect(run.log.filter(({ kind }) => kind === 'commit')).toEqual([])
        expect(run.crashes).toHaveLength(reported === null ? 0 : 1)
        if (reported === 'the fenced answer') {
          expect(run.crashes[0].error.code).toBe(FENCED_INSTANCE_ID)
          expect(inCauseChain(run.crashes[0].error, failed)).toBe(false)
        }
        if (reported === 'the recreation') expect(run.crashes[0].error).toBe(error)
        expect(policy).toHaveBeenCalledTimes(reported === null ? 0 : 1)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )
  })

  describe('a REBALANCE_IN_PROGRESS heartbeat answer while the handler runs', () => {
    const ILLEGAL_GENERATION = 22
    const TOPIC_AUTHORIZATION_FAILED = 29

    /** How each step of a traced rejoin reads. */
    const REJOIN_STEPS = {
      join: ({ memberId }) => `join ${memberId}`,
      GROUP_JOIN: ({ memberId }) => `GROUP_JOIN ${memberId}`,
      leave: STEPS.leave,
      policy: () => 'restartOnFailure',
      CRASH: STEPS.CRASH,
    }

    /** @returns {string[]} the rejoins, accepted joins, leaves, restart decision and CRASH, in order */
    const rejoinTrail = (run) =>
      run.log
        .filter(({ kind }) => REJOIN_STEPS[kind])
        .map((entry) => REJOIN_STEPS[entry.kind](entry))

    /** @returns {number} when the `index`-th traced step of `kind` happened, on the fake clock */
    const stepAt = (run, kind, index = 0) =>
      run.log.filter((entry) => entry.kind === kind)[index].at

    /**
     * Records each later JoinGroup and each accepted join (`GROUP_JOIN`) in `run.log`, and answers
     * the n-th later JoinGroup with the member id `member-(n + 1)` once the returned gate opens, so
     * a LeaveGroup names the membership it leaves.
     * @returns {{ promise: Promise<void>, resolve: () => void }} the gate later JoinGroups wait on
     */
    const traceRejoins = (run) => {
      const gate = deferred()
      const answer = run.coordinator.joinGroup.getMockImplementation()
      let rejoins = 0
      run.coordinator.joinGroup.mockImplementation(async (...args) => {
        rejoins += 1
        const memberId = `member-${rejoins + 1}`
        run.log.push({ kind: 'join', memberId, at: Date.now() })
        await gate.promise
        return { ...(await answer(...args)), memberId }
      })
      run.consumer.on(run.consumer.events.GROUP_JOIN, ({ payload }) =>
        run.log.push({ kind: 'GROUP_JOIN', memberId: payload.memberId, at: Date.now() })
      )
      return gate
    }

    /** Answers the member's next heartbeat with `code` and waits until it was sent. */
    const answerNextHeartbeat = async (run, code) => {
      const sent = run.coordinator.heartbeat.mock.calls.length
      run.coordinator.heartbeat.mockImplementationOnce(async () => {
        throw protocolError(code)
      })
      await advanceUntil(() => run.coordinator.heartbeat.mock.calls.length > sent, 2000)
      await jest.advanceTimersByTimeAsync(100)
      expect(run.coordinator.heartbeat.mock.calls.length).toBeGreaterThan(sent)
    }

    /**
     * Starts an eager member of topic-a/0, serving offset 40, whose eachMessage runs
     * `handle(held, call)`, and waits for the first call.
     * @returns {Promise<object>} the run of `begin`
     */
    const viaEachMessage = async ({ retry, handle }) => {
      const run = await begin({
        config: { heartbeatInterval: 500 },
        retry,
        records: { 'topic-a|0': [40] },
        eachMessage: (_payload, held, call) => handle(held, call),
      })
      await advanceUntil(() => run.delivered.length === 1)
      return run
    }

    /**
     * Starts the same member with an eachBatch handler, records what `begin` records for a close
     * (LeaveGroup, DISCONNECT, CRASH), and waits for the first call.
     * @returns {Promise<object>} the run, with `delivered`: `partition|firstOffset` of each call
     */
    const viaEachBatch = async ({ retry, handle }) => {
      const delivered = []
      const run = await startClassic('classic', {
        autoCommitThreshold: 100,
        records: { 'topic-a|0': [40] },
        config: { heartbeatInterval: 500, retry },
        eachBatch: async ({ batch }, held) => {
          delivered.push(`${batch.partition}|${batch.firstOffset()}`)
          await handle(held, delivered.length)
        },
      })
      run.coordinator.leaveGroup.mockImplementation(async ({ memberId, groupInstanceId }) => {
        run.log.push({ kind: 'leave', memberId, groupInstanceId })
      })
      run.consumer.on(run.consumer.events.DISCONNECT, () => run.log.push({ kind: 'DISCONNECT' }))
      run.consumer.on(run.consumer.events.CRASH, ({ payload }) =>
        run.log.push({ kind: 'CRASH', restart: payload.restart })
      )
      await advanceUntil(() => delivered.length === 1)
      return Object.assign(run, { delivered })
    }

    it.each([
      ['eachMessage', viaEachMessage],
      ['eachBatch', viaEachBatch],
    ])(
      'then a non-retriable failure of that %s handler is reported only once the eager rejoin is accepted: one CRASH carries it, nothing is redelivered and the crash cleanup leaves the new membership',
      async (_kind, startRun) => {
        const error = failure(false)
        const policy = jest.fn(async () => {
          run.log.push({ kind: 'policy' })
          return false
        })
        const run = await startRun({
          retry: { ...RETRY, restartOnFailure: policy },
          handle: async (held) => {
            await held.promise
            throw error
          },
        })
        await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
        expect(trail(run)).toEqual([])
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
        const rejoin = traceRejoins(run)

        run.held.resolve()
        await advanceUntil(() => run.coordinator.joinGroup.mock.calls.length === 2, 2000)
        await jest.advanceTimersByTimeAsync(500)
        expect(rejoinTrail(run)).toEqual(['join member-2'])
        expect(run.crashes).toEqual([])
        expect(policy).not.toHaveBeenCalled()

        rejoin.resolve()
        await advanceUntil(() => run.crashes.length > 0, 2000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(rejoinTrail(run)).toEqual([
          'join member-2',
          'GROUP_JOIN member-2',
          'leave member-2 null',
          'restartOnFailure',
          'CRASH restart false',
        ])
        expect(trail(run).slice(-3)).toEqual(['STOP', 'DISCONNECT', 'CRASH restart false'])
        expect(run.delivered).toEqual(['0|40'])
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.name).toBe('KafkaJSNonRetriableError')
        expect(run.crashes[0].error.cause).toBe(error)
        expect(policy).toHaveBeenCalledTimes(1)
        expect(policy.mock.calls[0][0]).toBe(run.crashes[0].error)
        expect(jest.getTimerCount()).toBe(0)
      },
      30000
    )

    it('then a retriable failure of that handler spends the one retry, and a second REBALANCE_IN_PROGRESS answered during the retry backoff does not refill it: the redelivery after the backoff fails and CRASH reports KafkaJSNumberOfRetriesExceeded', async () => {
      const error = failure(true)
      const policy = jest.fn(async () => false)
      const deliveredAt = []
      const run = await viaEachMessage({
        retry: { retries: 1, factor: 0, initialRetryTime: 2000, restartOnFailure: policy },
        handle: async (held, call) => {
          deliveredAt.push(Date.now())
          if (call === 1) await held.promise
          throw error
        },
      })
      await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
      traceRejoins(run).resolve()

      run.held.resolve()
      await advanceUntil(() => rejoinTrail(run).includes('GROUP_JOIN member-2'), 2000)
      expect(run.crashes).toEqual([])

      await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
      expect(rejoinTrail(run)).toEqual(['join member-2', 'GROUP_JOIN member-2'])
      expect(run.delivered).toEqual(['0|40'])

      await advanceUntil(() => run.crashes.length > 0, 5000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(rejoinTrail(run)).toEqual([
        'join member-2',
        'GROUP_JOIN member-2',
        'join member-3',
        'GROUP_JOIN member-3',
        'leave member-3 null',
        'CRASH restart false',
      ])
      expect(stepAt(run, 'join', 1) - stepAt(run, 'GROUP_JOIN')).toBeGreaterThanOrEqual(2000)
      expect(deliveredAt[1]).toBeGreaterThanOrEqual(stepAt(run, 'GROUP_JOIN', 1))
      expect(run.delivered).toEqual(['0|40', '0|40'])
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(run.crashes[0].error.cause).toBe(error)
      expect(run.crashes[0].error.retryCount).toBe(1)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)

    it('keeps the failed record retry spent when a replayed prefix returns during another loss', async () => {
      const error = failure(true)
      const prefix = deferred()
      const run = await begin({
        config: { heartbeatInterval: 500 },
        retry: { ...RETRY, retries: 1, restartOnFailure: async () => false },
        run: { autoCommit: false },
        eachMessage: async ({ message }, held, call) => {
          if (call === 3) await prefix.promise
          if (message.offset !== '41') return
          if (call === 2) await held.promise
          throw error
        },
      })
      await advanceUntil(() => run.delivered.length === 2)
      traceRejoins(run).resolve()
      await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
      run.held.resolve()

      await advanceUntil(() => run.delivered.length === 3)
      expect(run.delivered).toEqual(['0|40', '0|41', '0|40'])
      await answerNextHeartbeat(run, ILLEGAL_GENERATION)
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
      prefix.resolve()
      await advanceUntil(() => run.crashes.length > 0)

      expect(run.delivered).toEqual(['0|40', '0|41', '0|40', '0|40', '0|41'])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: error,
        retryCount: 1,
      })
    }, 30000)

    it('keeps the failed partition retry spent when a sibling succeeds during another rebalance', async () => {
      const error = failure(true)
      const sibling = deferred()
      const run = await begin({
        config: { heartbeatInterval: 500 },
        retry: { ...RETRY, retries: 1, restartOnFailure: async () => false },
        partitions: [0, 1],
        records: { 'topic-a|0': [40] },
        run: { autoCommit: false, partitionsConsumedConcurrently: 2 },
        eachMessage: async ({ partition }, held, call) => {
          if (partition === 1) return sibling.promise
          if (call === 1) await held.promise
          throw error
        },
      })
      await advanceUntil(() => run.delivered.length === 1)
      traceRejoins(run).resolve()
      const failedPartition = [{ topic: 'topic-a', partitions: [0] }]
      run.consumer.pause(failedPartition)
      run.state.queue['topic-a|1'] = [40]
      run.held.resolve()
      await advanceUntil(() => run.delivered.includes('1|40'))
      expect(run.delivered).toEqual(['0|40', '1|40'])

      await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
      sibling.resolve()
      await advanceUntil(() => rejoinTrail(run).includes('GROUP_JOIN member-2'))
      run.consumer.resume(failedPartition)
      await advanceUntil(() => run.crashes.length > 0)

      expect(run.delivered.filter((entry) => entry === '0|40')).toHaveLength(2)
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: error,
        retryCount: 1,
      })
    }, 30000)

    it('starts a fresh Fetch retry budget after an idle rejoin', async () => {
      const policy = jest.fn(async () => false)
      const run = await begin({
        records: {},
        retry: { ...RETRY, retries: 1, restartOnFailure: policy },
      })
      traceRejoins(run).resolve()
      const broker = await run.cluster.findBroker(0)
      broker.fetch
        .mockRejectedValueOnce(protocolError(7))
        .mockRejectedValueOnce(protocolError(REBALANCE_IN_PROGRESS))
      await advanceUntil(() => rejoinTrail(run).includes('GROUP_JOIN member-2'))
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
      await jest.advanceTimersByTimeAsync(150)

      const again = jest.fn(async () => {
        throw protocolError(7)
      })
      broker.fetch.mockImplementationOnce(again)
      await advanceUntil(() => again.mock.calls.length > 0)
      expect(again).toHaveBeenCalledTimes(1)
      const fetches = () => run.log.filter(({ kind }) => kind === 'fetch').length
      const afterFailure = fetches()
      await advanceUntil(() => fetches() > afterFailure)
      expect(fetches()).toBeGreaterThan(afterFailure)
      expect(run.delivered).toEqual([])
      expect(run.crashes).toEqual([])
      expect(policy).not.toHaveBeenCalled()
      await closeClean(run)
    }, 30000)

    it.each([
      ['inside the join retrier', REBALANCE_IN_PROGRESS],
      ['after escaping the join retrier', 7],
    ])(
      'keeps the handler failure pending while a transient rejoin error is retried %s',
      async (_kind, code) => {
        const error = failure(false)
        const policy = jest.fn(async () => false)
        const run = await viaEachMessage({
          retry: { ...RETRY, retries: 1, restartOnFailure: policy },
          handle: async (held) => {
            await held.promise
            throw error
          },
        })
        await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
        const rejoin = traceRejoins(run)
        const refused = jest.fn(async () => {
          throw protocolError(code)
        })
        run.coordinator.joinGroup.mockImplementationOnce(refused)
        run.held.resolve()
        await advanceUntil(() => refused.mock.calls.length > 0)
        expect(refused).toHaveBeenCalledTimes(1)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
        await advanceUntil(() => rejoinTrail(run).includes('join member-2'))
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(3)
        expect(rejoinTrail(run)).toEqual(['join member-2'])
        expect(run.delivered).toEqual(['0|40'])
        expect(run.crashes).toEqual([])
        expect(policy).not.toHaveBeenCalled()

        rejoin.resolve()
        await advanceUntil(() => run.crashes.length > 0)
        expect(rejoinTrail(run)).toEqual([
          'join member-2',
          'GROUP_JOIN member-2',
          'leave member-2 null',
          'CRASH restart false',
        ])
        expect(run.delivered).toEqual(['0|40'])
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error.cause).toBe(error)
        expect(policy).toHaveBeenCalledTimes(1)
      },
      30000
    )

    /**
     * Starts an eager member whose first handler call, once released, commits offset 41 and
     * rethrows what the commit rejects with; releases it and holds that OffsetCommit until
     * `answer(how)` is called, `how` being what the coordinator double then does.
     * @param {Function} policy the consumer's `restartOnFailure`
     * @returns {Promise<object>} the run, with `answer(how)` and `refused()`: the commit's rejection
     */
    const commitHeld = async (policy) => {
      const ref = {}
      const reply = deferred()
      let refused = null
      const run = await viaEachMessage({
        retry: { ...RETRY, restartOnFailure: policy },
        handle: async (held, call) => {
          if (call !== 1) return
          await held.promise
          try {
            await ref.run.consumer.commitOffsets([{ topic: 'topic-a', partition: 0, offset: '41' }])
          } catch (error) {
            refused = error
            throw error
          }
        },
      })
      ref.run = run
      let respond = null
      const sent = run.coordinator.offsetCommit.mock.calls.length
      run.coordinator.offsetCommit.mockImplementationOnce(async () => {
        await reply.promise
        await respond()
      })
      run.held.resolve()
      await advanceUntil(() => run.coordinator.offsetCommit.mock.calls.length > sent, 1000)
      expect(run.coordinator.offsetCommit.mock.calls.length).toBeGreaterThan(sent)
      return Object.assign(run, {
        answer: (how) => {
          respond = how
          reply.resolve()
        },
        refused: () => refused,
      })
    }

    it("then a commit the handler awaits, answered TOPIC_AUTHORIZATION_FAILED after that REBALANCE_IN_PROGRESS, is the handler's own failure: rethrown, it is reported once the eager rejoin is accepted, though the commit was sent for the membership the rejoin replaced", async () => {
      const denied = protocolError(TOPIC_AUTHORIZATION_FAILED)
      const policy = jest.fn(async () => {
        run.log.push({ kind: 'policy' })
        return false
      })
      const run = await commitHeld(policy)
      await answerNextHeartbeat(run, REBALANCE_IN_PROGRESS)
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
      traceRejoins(run).resolve()

      run.answer(async () => {
        throw denied
      })
      await advanceUntil(() => run.crashes.length > 0, 2000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(run.refused()?.name).toBe('KafkaJSNonRetriableError')
      expect(inCauseChain(run.refused(), denied)).toBe(true)
      expect(rejoinTrail(run)).toEqual([
        'join member-2',
        'GROUP_JOIN member-2',
        'leave member-2 null',
        'restartOnFailure',
        'CRASH restart false',
      ])
      expect(run.delivered).toEqual(['0|40'])
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error.name).toBe('KafkaJSNonRetriableError')
      expect(inCauseChain(run.crashes[0].error, denied)).toBe(true)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)

    it('then a commit the handler awaits, refused because an ILLEGAL_GENERATION heartbeat answer took its membership meanwhile, stays part of the rejoin when rethrown: no CRASH, and the record is redelivered to the new membership', async () => {
      const policy = jest.fn(async () => false)
      const run = await commitHeld(policy)
      await answerNextHeartbeat(run, ILLEGAL_GENERATION)
      traceRejoins(run).resolve()

      run.answer(async () => {})
      await advanceUntil(() => run.delivered.length === 2, 2000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(run.refused()?.name).toBe('KafkaJSCommitFailedError')
      expect(rejoinTrail(run)).toEqual(['join member-2', 'GROUP_JOIN member-2'])
      expect(run.delivered).toEqual(['0|40', '0|40'])
      expect(run.crashes).toEqual([])
      expect(policy).not.toHaveBeenCalled()
      await closeClean(run)
    }, 30000)
  })

  describe('a background rejoin round of a cooperative member whose JoinGroup fails', () => {
    /** An assigner under the protocol name the JoinGroup double answers, cooperative only. */
    const cooperative = () => ({
      name: 'eager',
      version: 0,
      supportedProtocols: () => ['cooperative'],
      protocol: () => ({ name: 'eager', metadata: Buffer.alloc(0) }),
      assign: async () => [],
    })

    it.each([
      ['absent', false],
      ['returning true', true],
    ])(
      'with restartOnFailure %s cleans up as stop() does and reports the JoinGroup error itself, unwrapped, with restart %s',
      async (policyName, restart) => {
        const policy = policyName === 'absent' ? undefined : jest.fn(async () => true)
        const failed = failure(false)
        const run = await begin({
          config: { heartbeatInterval: 500, partitionAssigners: [cooperative] },
          retry: { ...RETRY, ...(policy && { restartOnFailure: policy }) },
        })
        await advanceUntil(() => run.delivered.length === 2)
        await jest.advanceTimersByTimeAsync(200)
        expect(run.group.rebalanceProtocol()).toBe('cooperative')
        expect(trail(run)).toEqual([])
        const rebalancing = jest.fn()
        run.consumer.on(run.consumer.events.REBALANCING, rebalancing)

        run.coordinator.joinGroup.mockImplementationOnce(async () => {
          throw failed
        })
        run.coordinator.heartbeat.mockImplementationOnce(async () => {
          throw protocolError(REBALANCE_IN_PROGRESS)
        })
        await advanceUntil(() => run.crashes.length > 0, 2000)

        expect(rebalancing).toHaveBeenCalledTimes(1)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
        expect(trail(run)).toEqual([
          'final commit',
          'commit topic-a|0 42',
          'leave member-1 null',
          'STOP',
          'DISCONNECT',
          `CRASH restart ${restart}`,
        ])
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].error).toBe(failed)
        expect(run.cluster.disconnect.mock.calls).toEqual([[{ preserveSetupState: true }]])
        if (policy) expect(policy).toHaveBeenCalledTimes(1)

        await expectRestart(run, restart)
        if (restart) {
          await advanceUntil(() => run.coordinator.joinGroup.mock.calls.length > 2, 2000)
          expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(3)
          await closeClean(run)
        } else {
          expect(jest.getTimerCount()).toBe(0)
        }
      },
      30000
    )
  })

  describe('a cooperative membership replacement during an explicit commit', () => {
    it.each([
      ['refuses an old membership reply without another rejoin', false],
      ['reports a saved authorization failure after the background join', true],
    ])(
      '%s',
      async (_title, independent) => {
        const ref = {}
        const reply = deferred()
        const commit = deferred()
        const policy = jest.fn(async () => false)
        const denied = protocolError(29)
        let refused
        const run = await begin({
          config: {
            heartbeatInterval: 500,
            partitionAssigners: [
              () => ({
                name: 'eager',
                version: 0,
                supportedProtocols: () => ['cooperative'],
                protocol: () => ({ name: 'eager', metadata: Buffer.alloc(0) }),
                assign: async () => [],
              }),
            ],
          },
          retry: { ...RETRY, restartOnFailure: policy },
          records: { 'topic-a|0': [40] },
          run: { autoCommit: false },
          eachMessage: async (_payload, held, call) => {
            if (call > 1) return
            await commit.promise
            try {
              await ref.run.consumer.commitOffsets([
                { topic: 'topic-a', partition: 0, offset: '41' },
              ])
            } catch (error) {
              refused = error
              await held.promise
              throw new TypeError('the handler rethrew the commit reply', { cause: error })
            }
          },
        })
        ref.run = run
        const commitEvents = jest.fn()
        const joined = jest.fn()
        const rebalancing = jest.fn()
        run.consumer.on(run.consumer.events.COMMIT_OFFSETS, commitEvents)
        run.consumer.on(run.consumer.events.GROUP_JOIN, joined)
        run.consumer.on(run.consumer.events.REBALANCING, rebalancing)
        run.coordinator.offsetCommit.mockImplementationOnce(async () => {
          if (independent) throw denied
          await reply.promise
          throw protocolError(22)
        })
        commit.resolve()
        await advanceUntil(() => run.coordinator.offsetCommit.mock.calls.length > 0)
        expect(run.coordinator.offsetCommit.mock.calls[0][0].groupGenerationId).toBe(1)
        if (independent) {
          await advanceUntil(() => refused != null)
          expect(inCauseChain(refused, denied)).toBe(true)
        }

        const join = run.coordinator.joinGroup.getMockImplementation()
        run.coordinator.joinGroup.mockImplementationOnce(async (...args) => ({
          ...(await join(...args)),
          generationId: 2,
        }))
        run.coordinator.heartbeat.mockRejectedValueOnce(protocolError(REBALANCE_IN_PROGRESS))
        await advanceUntil(() => joined.mock.calls.length > 0)
        expect(joined).toHaveBeenCalledTimes(1)
        expect(run.group.generationId).toBe(2)
        expect(run.crashes).toEqual([])
        expect(policy).not.toHaveBeenCalled()

        reply.resolve()
        await advanceUntil(() => refused != null)
        expect(refused?.name).toBe(
          independent ? 'KafkaJSNonRetriableError' : 'KafkaJSCommitFailedError'
        )
        run.held.resolve()
        if (independent) await advanceUntil(() => run.crashes.length > 0)
        await jest.advanceTimersByTimeAsync(1000)

        expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
        expect(commitEvents).not.toHaveBeenCalled()
        expect(rebalancing).toHaveBeenCalledTimes(1)
        expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
        expect(run.crashes).toHaveLength(Number(independent))
        expect(policy).toHaveBeenCalledTimes(Number(independent))
        if (independent) {
          expect(inCauseChain(run.crashes[0].error, denied)).toBe(true)
        } else {
          await closeClean(run)
        }
      },
      30000
    )
  })

  describe('other rows of a classic run', () => {
    it('a start crash after the run is running, when the first JoinGroup fails, emits STOP and DISCONNECT before CRASH, commits and leaves nothing, and run() resolves only after CRASH', async () => {
      const error = failure(false)
      let refuse = null
      const joined = { promise: new Promise((_resolve, reject) => (refuse = reject)) }
      const transport = parkTransport()
      const run = await startClassic('classic', {
        joined,
        config: { retry: RETRY },
        trace: true,
        prepare: transport.prepare,
      })
      expect(run.runner.running).toBe(true)

      refuse(error)
      await advanceUntil(() => transport.parked(run), 2000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(startupTrail(run)).toEqual(['STOP', 'transport disconnect'])
      expect(run.started.done).toBe(false)
      expect(run.crashes).toEqual([])

      transport.release()
      await advanceUntil(() => run.started.done, 2000)

      expect(run.started).toMatchObject({ done: true, value: undefined })
      expect(startupTrail(run)).toEqual([
        'STOP',
        'transport disconnect',
        'DISCONNECT',
        'CRASH restart false',
        'run() settled',
      ])
      expect(run.crashes).toHaveLength(1)
      expect(inCauseChain(run.crashes[0].error, error)).toBe(true)
      expect(run.cluster.disconnect.mock.calls).toEqual([[{ preserveSetupState: true }]])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(1)
      expect(run.coordinator.syncGroup).not.toHaveBeenCalled()
      expect(run.coordinator.leaveGroup).not.toHaveBeenCalled()
      await expectRestart(run, false)
      expect(jest.getTimerCount()).toBe(0)
    }, 30000)

    it('stop() whose close callback commits offset 42 itself: the callback commit comes before the final commit, which then sends nothing, and the member leaves after both', async () => {
      const ref = {}
      const run = await begin({
        closeCallback: () =>
          ref.consumer.commitOffsets([{ topic: 'topic-a', partition: 0, offset: '42' }]),
      })
      ref.consumer = run.consumer
      await advanceUntil(() => run.delivered.length === 2)
      await jest.advanceTimersByTimeAsync(200)

      const stopping = settled(run.consumer.stop())
      await advanceUntil(() => stopping.done, 2000)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(trail(run)).toEqual([
        'revoked callback',
        'commit topic-a|0 42',
        'final commit',
        'leave member-1 null',
        'STOP',
      ])
      expect(run.crashes).toEqual([])
    }, 30000)

    it('uncommittedOffsets() in eachBatch answers { topics: [] } before anything is resolved and again once the resolved offset is committed', async () => {
      const seen = []
      const run = await startClassic('classic', {
        autoCommitThreshold: 1,
        eachBatch: async ({
          batch,
          resolveOffset,
          uncommittedOffsets,
          commitOffsetsIfNecessary,
        }) => {
          seen.push(uncommittedOffsets())
          resolveOffset(batch.messages[0].offset)
          await commitOffsetsIfNecessary()
          seen.push(uncommittedOffsets())
        },
      })
      await advanceUntil(() => seen.length === 2)

      expect(seen).toEqual([{ topics: [] }, { topics: [] }])
      expect(run.log.filter(({ kind }) => kind === 'commit')).toMatchObject([
        { tp: 'topic-a|0', offset: '41' },
      ])
    }, 30000)
  })
})

describe('Consumer > how a consumer-protocol run ends', () => {
  /** @returns {number} how many entries of `kind` the run's trace holds */
  const count = (run, kind) => run.log.filter((entry) => entry.kind === kind).length

  /** @returns {number[]} the member epochs of the leave heartbeats the member sent */
  const leaves = (run) =>
    run.log.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch < 0).map(({ epoch }) => epoch)

  /**
   * Starts a member of topic-a/0 whose eachMessage runs `body`, records DISCONNECT beside the
   * fixture's STOP, and waits until the handler has been called once.
   * @returns {Promise<object>} the run, with `calls`: how often the handler was called
   */
  const begin = async (body, options = {}) => {
    const calls = []
    const run = await start({
      ...options,
      eachMessage: async (payload, held) => {
        calls.push(payload.message.offset)
        return body(payload, held, calls.length)
      },
    })
    run.consumer.on(run.consumer.events.DISCONNECT, () => run.log.push({ kind: 'DISCONNECT' }))
    await advanceUntil(() => calls.length === 1)
    return Object.assign(run, { calls })
  }

  /** Waits for the fatal answer the fixture arms, then until the fatal deadline has passed. */
  const forceClaim = async (run) => {
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await jest.advanceTimersByTimeAsync(run.faultAt() + 3999 - Date.now())
  }

  it('a fatal answer reserved while the handler runs, then a non-retriable failure of that handler before the deadline: CRASH reports the fatal answer with restart false and the failure is not reported', async () => {
    const failed = failure(false)
    const run = await begin(
      async (_payload, held) => {
        await held.promise
        throw failed
      },
      { retry: RETRY }
    )
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await jest.advanceTimersByTimeAsync(500)
    expect(run.crashes).toEqual([])

    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes.map(({ error, restart }) => [error.name, error.code, restart])).toEqual([
      ['KafkaJSNonRetriableError', 30, false],
    ])
    expect(inCauseChain(run.crashes[0].error, failed)).toBe(false)
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(count(run, 'commit')).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it('a fatal answer that arrives while a retriable handler failure waits for its retry: the retry takes the fatal answer and CRASH reports it with restart false, not the failure', async () => {
    const failed = failure(true)
    const run = await begin(
      async (_payload, _held, call) => {
        if (call === 1) throw failed
      },
      { retry: { retries: 1, factor: 0, initialRetryTime: 2000 } }
    )
    run.arm()
    await advanceUntil(() => run.faultAt() !== null, 1000)
    expect(run.crashes).toEqual([])

    await advanceUntil(() => run.crashes.length > 0, 3000)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes.map(({ error, restart }) => [error.name, error.code, restart])).toEqual([
      ['KafkaJSNonRetriableError', 30, false],
    ])
    expect(inCauseChain(run.crashes[0].error, failed)).toBe(false)
    expect(run.calls).toEqual(['40'])
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it('a stop() made while the forced claim delivers its lost callback settles at once without a second STOP; CRASH waits for the callback', async () => {
    const lost = deferred()
    const listener = jest.fn(() => lost.promise)
    const run = await begin((_payload, held) => held.promise, {
      hook: { onPartitionsLost: listener },
    })
    await forceClaim(run)
    expect(listener).not.toHaveBeenCalled()
    await advanceUntil(() => listener.mock.calls.length > 0, 100)

    const stopping = settled(run.consumer.stop())
    await jest.advanceTimersByTimeAsync(100)

    expect(stopping).toMatchObject({ done: true, value: undefined })
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(run.crashes).toEqual([])

    lost.resolve()
    await advanceUntil(() => run.crashes.length > 0, 1000)
    run.held.resolve()
    await jest.advanceTimersByTimeAsync(3000)

    expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])
    expect(listener).toHaveBeenCalledTimes(1)
    expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it.each([['the held handler fails'], ['a topic recreation arrives']])(
    'after the forced claim reported its CRASH, %s: nothing more is reported',
    async (second) => {
      const failed = failure(false)
      const run = await begin(async (_payload, held) => {
        await held.promise
        if (second === 'the held handler fails') throw failed
      })
      await forceClaim(run)
      await advanceUntil(() => run.crashes.length > 0, 100)
      expect(run.crashes.map(({ error, restart }) => [error.code, restart])).toEqual([[30, false]])

      if (second === 'a topic recreation arrives')
        run.coordinators[0].consumerGroup.stopRun(recreated())
      run.held.resolve()
      await jest.advanceTimersByTimeAsync(5000)

      expect(run.crashes).toHaveLength(1)
      expect([count(run, 'STOP'), count(run, 'DISCONNECT')]).toEqual([1, 1])
      expect(run.cluster.connect).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    },
    60000
  )

  it('a poll expiry, then a non-retriable failure of the stuck handler: the member rejoins first and then CRASH reports the failure, with a second leave from the crash cleanup', async () => {
    const failed = failure(false)
    const run = await begin(
      async (_payload, held, call) => {
        if (call !== 1) return
        await held.promise
        throw failed
      },
      { retry: RETRY }
    )
    const joins = beats(run, false)
    const joinsAtCrash = []
    run.consumer.on(run.consumer.events.CRASH, () => joinsAtCrash.push(beats(run, false)))
    await advanceUntil(() => leaves(run).length === 1, 10000)
    await jest.advanceTimersByTimeAsync(500)
    expect(run.crashes).toEqual([])

    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0, 5000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(joinsAtCrash).toEqual([joins + 1])
    expect(beats(run, false)).toBe(joins + 1)
    expect(
      run.crashes.map(({ error, restart }) => [error.name, error.cause === failed, restart])
    ).toEqual([['KafkaJSNonRetriableError', true, false]])
    expect(leaves(run)).toEqual([-1, -1])
    expect(run.calls).toEqual(['40'])
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it.each([
    ['a retriable', 'KafkaJSNumberOfRetriesExceeded', () => failure(true)],
    ['a TypeError', 'KafkaJSNonRetriableError', () => new TypeError('the handler failed')],
    ['a plain Error', 'KafkaJSNumberOfRetriesExceeded', () => new Error('the handler failed')],
  ])(
    'a poll expiry, then %s failure of the stuck handler: the member rejoins first and then CRASH reports it as %s, restartOnFailure is asked once, with a second leave from the crash cleanup',
    async (_kind, wrapper, make) => {
      const failed = make()
      const policy = jest.fn(async () => false)
      const run = await begin(
        async (_payload, held, call) => {
          if (call !== 1) return
          await held.promise
          throw failed
        },
        { retry: { ...RETRY, restartOnFailure: policy } }
      )
      const joins = beats(run, false)
      await advanceUntil(() => leaves(run).length === 1, 10000)
      await jest.advanceTimersByTimeAsync(500)
      expect(run.crashes).toEqual([])

      run.held.resolve()
      await advanceUntil(() => run.crashes.length > 0, 5000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(beats(run, false)).toBe(joins + 1)
      expect(
        run.crashes.map(({ error, restart }) => [error.name, error.cause === failed, restart])
      ).toEqual([[wrapper, true, false]])
      expect(policy).toHaveBeenCalledTimes(1)
      expect(policy.mock.calls[0][0]).toBe(run.crashes[0].error)
      expect(leaves(run)).toEqual([-1, -1])
      expect(run.calls).toEqual(['40'])
      expect(jest.getTimerCount()).toBe(0)
    },
    60000
  )

  it('a retriable failure each time the stuck handler overruns spends one retry per rejoin, and CRASH reports KafkaJSNumberOfRetriesExceeded once the retries run out', async () => {
    const failed = failure(true)
    const policy = jest.fn(async () => false)
    const run = await begin(
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 6000))
        throw failed
      },
      { retry: { ...RETRY, retries: 1, restartOnFailure: policy } }
    )
    const joins = beats(run, false)
    await advanceUntil(() => run.calls.length === 2, 15000)

    expect(run.crashes).toEqual([])
    expect(leaves(run)).toEqual([-1])
    expect(beats(run, false)).toBe(joins + 1)

    await advanceUntil(() => run.crashes.length > 0, 15000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(
      run.crashes.map(({ error, restart }) => [error.name, error.cause === failed, restart])
    ).toEqual([['KafkaJSNumberOfRetriesExceeded', true, false]])
    expect(run.crashes[0].error.retryCount).toBe(1)
    expect(policy).toHaveBeenCalledTimes(1)
    expect(beats(run, false)).toBe(joins + 2)
    expect(leaves(run)).toEqual([-1, -1, -1])
    expect(run.calls).toEqual(['40', '40'])
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it('a poll expiry, then a retriable failure of the stuck handler, then a fenced heartbeat answer during the retry backoff: the second loss does not refill the retries, so the redelivery after the backoff fails and CRASH reports KafkaJSNumberOfRetriesExceeded', async () => {
    const failed = failure(true)
    const policy = jest.fn(async () => false)
    const calledAt = []
    const run = await begin(
      async (_payload, held, call) => {
        calledAt.push(Date.now())
        if (call === 1) await held.promise
        throw failed
      },
      {
        fault: fenced,
        retry: { retries: 1, factor: 0, initialRetryTime: 2000, restartOnFailure: policy },
      }
    )
    const joins = beats(run, false)
    const accepted = []
    run.consumer.on(run.consumer.events.GROUP_JOIN, () => accepted.push(Date.now()))
    await advanceUntil(() => leaves(run).length === 1, 10000)
    run.held.resolve()
    await advanceUntil(() => accepted.length === 1, 5000)
    expect(accepted).toHaveLength(1)
    expect(run.crashes).toEqual([])

    run.arm()
    await advanceUntil(() => run.faultAt() !== null, 1500)
    expect(run.faultAt()).not.toBeNull()
    expect(run.calls).toEqual(['40'])
    expect(beats(run, false)).toBe(joins + 1)

    await advanceUntil(() => run.crashes.length > 0, 5000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(run.calls).toEqual(['40', '40'])
    expect(accepted).toHaveLength(2)
    expect(accepted[1] - accepted[0]).toBeGreaterThanOrEqual(2000)
    expect(calledAt[1]).toBeGreaterThanOrEqual(accepted[1])
    expect(beats(run, false)).toBe(joins + 2)
    expect(
      run.crashes.map(({ error, restart }) => [error.name, error.cause === failed, restart])
    ).toEqual([['KafkaJSNumberOfRetriesExceeded', true, false]])
    expect(run.crashes[0].error.retryCount).toBe(1)
    expect(policy).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  }, 60000)

  it.each([
    ['reports the refusal while its epoch is current', false],
    ['drops the saved refusal after replacing its epoch', true],
  ])(
    'a handler rethrowing STALE_MEMBER_EPOCH %s',
    async (_title, replace) => {
      const ref = {}
      const commit = deferred()
      const denied = protocolError(113)
      let refused
      const policy = jest.fn(async () => false)
      const run = await begin(
        async (_payload, held, call) => {
          if (call > 1) return
          await commit.promise
          try {
            await ref.run.consumer.commitOffsets([{ topic: 'topic-a', partition: 0, offset: '41' }])
          } catch (error) {
            refused = error
            await held.promise
            throw error
          }
        },
        { retry: { ...RETRY, restartOnFailure: policy }, fault: fenced }
      )
      ref.run = run
      const joined = jest.fn()
      const published = jest.fn()
      run.consumer.on(run.consumer.events.GROUP_JOIN, joined)
      run.consumer.on(run.consumer.events.COMMIT_OFFSETS, published)
      run.coordinator.offsetCommit.mockRejectedValueOnce(denied)
      commit.resolve()
      await advanceUntil(() => refused != null)
      expect(refused).toMatchObject({ name: 'KafkaJSNonRetriableError', retriable: false })
      expect(inCauseChain(refused, denied)).toBe(true)
      expect(run.coordinator.offsetCommit.mock.calls[0][0].groupGenerationId).toBe(1)

      if (replace) {
        run.arm()
        await advanceUntil(() => run.coordinators[0].hasVerdict())
        expect(run.coordinators[0].hasVerdict()).toBe(true)
        expect(run.crashes).toEqual([])
        expect(policy).not.toHaveBeenCalled()
      }
      run.held.resolve()
      if (replace) {
        await advanceUntil(() => run.calls.length === 2)
        expect(run.calls).toEqual(['40', '40'])
      } else {
        await advanceUntil(() => run.crashes.length > 0)
      }
      await jest.advanceTimersByTimeAsync(500)
      expect(joined).toHaveBeenCalledTimes(Number(replace))
      expect(run.crashes).toHaveLength(Number(!replace))
      expect(policy).toHaveBeenCalledTimes(Number(!replace))
      // The refused epoch-1 commit is never sent again; the redelivered record commits under epoch 2.
      expect(
        run.coordinator.offsetCommit.mock.calls.map(([{ groupGenerationId, topics }]) => [
          groupGenerationId,
          topics[0].partitions[0].offset,
        ])
      ).toEqual(
        replace
          ? [
              [1, '41'],
              [2, '41'],
            ]
          : [[1, '41']]
      )
      expect(
        published.mock.calls.map(([{ payload }]) => [
          payload.groupGenerationId,
          payload.topics[0].partitions[0].offset,
        ])
      ).toEqual(replace ? [[2, '41']] : [])
      if (replace) {
        await closeClean(run)
      } else {
        expect(inCauseChain(run.crashes[0].error, denied)).toBe(true)
      }
    },
    30000
  )

  it.each([
    ['reports an unrelated INVALID_REQUEST from another operation', false],
    ['folds the refusal from this member’s fenced OffsetCommit', true],
  ])(
    'a wrapped protocol error during recovery %s',
    async (_title, membership) => {
      const ref = {}
      const request = deferred()
      const protocol = protocolError(membership ? 110 : 42)
      const policy = jest.fn(async () => true)
      let wrapped
      const run = await begin(
        async (_payload, held, call) => {
          if (call > 1) return
          await request.promise
          try {
            if (membership) {
              await ref.run.consumer.commitOffsets([
                { topic: 'topic-a', partition: 0, offset: '41' },
              ])
            } else {
              await ref.run.cluster.fetchTopicsOffset([{ topic: 'topic-a', partitions: [0] }])
            }
          } catch (error) {
            wrapped = new Error('the operation failed', { cause: error })
            await held.promise
            throw wrapped
          }
        },
        { retry: { ...RETRY, restartOnFailure: policy }, fault: fenced }
      )
      ref.run = run
      const joined = jest.fn()
      run.consumer.on(run.consumer.events.GROUP_JOIN, joined)
      if (membership) {
        run.coordinator.offsetCommit.mockRejectedValueOnce(protocol)
      } else {
        run.cluster.fetchTopicsOffset.mockRejectedValueOnce(protocol)
      }
      request.resolve()
      await advanceUntil(() => wrapped != null)
      expect(inCauseChain(wrapped, protocol)).toBe(true)
      if (!membership) run.arm()
      await advanceUntil(() => run.coordinators[0].hasVerdict())
      expect(run.coordinators[0].hasVerdict()).toBe(true)
      expect(run.crashes).toEqual([])
      run.held.resolve()
      if (membership) {
        await advanceUntil(() => run.calls.length === 2)
        expect(run.calls).toEqual(['40', '40'])
      } else {
        await advanceUntil(() => run.crashes.length > 0)
      }
      expect(joined).toHaveBeenCalledTimes(1)
      expect(run.crashes).toHaveLength(Number(!membership))
      expect(policy).toHaveBeenCalledTimes(Number(!membership))
      if (membership) {
        await closeClean(run)
      } else {
        expect(run.crashes[0].error.name).toBe('KafkaJSNumberOfRetriesExceeded')
        expect(inCauseChain(run.crashes[0].error, wrapped)).toBe(true)
        expect(run.crashes[0].restart).toBe(true)
        await closeClean(run)
      }
    },
    30000
  )

  it('a poll expiry, then the stuck handler rethrowing the refusal of its own commit: the refusal stays part of the rejoin, nothing is sent or reported and the record is redelivered', async () => {
    const ref = {}
    let refused = null
    const policy = jest.fn(async () => false)
    const run = await begin(
      async ({ topic, partition }, held, call) => {
        if (call !== 1) return
        await held.promise
        try {
          await ref.run.consumer.commitOffsets([{ topic, partition, offset: '41' }])
        } catch (error) {
          refused = error
          throw error
        }
      },
      { retry: { ...RETRY, restartOnFailure: policy }, autoCommitThreshold: 100 }
    )
    ref.run = run
    const joins = beats(run, false)
    await advanceUntil(() => leaves(run).length === 1, 10000)
    await jest.advanceTimersByTimeAsync(500)

    run.held.resolve()
    await advanceUntil(() => run.calls.length === 2, 5000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(refused?.name).toBe('KafkaJSCommitFailedError')
    expect(count(run, 'commit')).toBe(0)
    expect(beats(run, false)).toBe(joins + 1)
    expect(leaves(run)).toEqual([-1])
    expect(run.calls).toEqual(['40', '40'])
    expect(run.crashes).toEqual([])
    expect(policy).not.toHaveBeenCalled()
  }, 60000)
})
