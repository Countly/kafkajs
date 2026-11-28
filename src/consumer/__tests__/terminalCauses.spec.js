const ConsumerGroup = require('../consumerGroup')
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
      [
        'stop()',
        ['commit topic-a|0 41', 'final commit', 'leave member-1 null', 'STOP', 'DISCONNECT'],
        [[{ preserveSetupState: true }]],
        'CRASH restart false',
      ],
      [
        'disconnect()',
        ['commit topic-a|0 41', 'final commit', 'leave member-1 null', 'STOP', 'DISCONNECT'],
        [[]],
        'CRASH restart false',
      ],
      ['disconnect({ timeout: 500 })', ['STOP', 'DISCONNECT'], [[{ drainTimeout: 0 }]], 'none'],
    ])(
      'after %s it ends with this trail, and restartOnFailure returning true restarts nothing',
      async (cause, cleanup, transport, crash) => {
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

        if (crash === 'none') {
          expect(ending).toMatchObject({ done: true, value: undefined })
          expect(trail(run)).toEqual(cleanup)
        } else {
          expect(ending.done).toBe(false)
          expect(trail(run)).toEqual([])
        }

        run.held.resolve()
        await advanceUntil(() => ending.done && (crash === 'none' || run.crashes.length > 0), 2000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(ending).toMatchObject({ done: true, value: undefined })
        expect(trail(run)).toEqual(crash === 'none' ? cleanup : [...cleanup, crash])
        expect(run.crashes.map(({ error: { cause } }) => cause)).toEqual(
          crash === 'none' ? [] : [error]
        )
        expect(policy).toHaveBeenCalledTimes(crash === 'none' ? 0 : 1)
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
     * Starts a member whose first handler call parks on `held` (and then throws `error`, if
     * given), and waits for the expiry.
     * @param {object} [options]
     * @param {(run: object) => void} [options.beforeExpiry] runs once the member is started
     */
    const expire = async ({ instance = false, error, beforeExpiry } = {}) => {
      const run = await begin({
        instance,
        config: { maxPollInterval: 4000, heartbeatInterval: 500 },
        records: { 'topic-a|0': [40] },
        eachMessage: async (_payload, held, call) => {
          if (call !== 1) return
          await held.promise
          if (error) throw error
        },
      })
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

    it('then a failure of that handler is taken by the rejoin: no CRASH, no commit, one LeaveGroup', async () => {
      const run = await expire({ error: failure(false) })
      await jest.advanceTimersByTimeAsync(1000)
      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])

      run.held.resolve()
      await advanceUntil(() => run.delivered.length === 2, 5000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(trail(run)).toEqual(['POLL_INTERVAL_EXCEEDED', 'leave member-1 null'])
      expect(run.coordinator.joinGroup).toHaveBeenCalledTimes(2)
      expect(run.delivered).toEqual(['0|40', '0|40'])
      expect(run.crashes).toEqual([])
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
})
