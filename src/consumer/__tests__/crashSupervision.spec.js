const { createLogger } = require('../../loggers')
const {
  GROUP_AUTHORIZATION_FAILED,
  fenced,
  groupAuthorization,
  protocolError,
  advanceUntil,
  deferred,
  settled,
  start,
  teardown,
  forget,
  beats,
  inCauseChain,
  recordingLogger,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

describe('Consumer > crash supervision', () => {
  describe('a log sink that throws on the membership-lost line', () => {
    const LOST_LINE = 'Membership lost, discarding uncommitted offsets and re-joining'

    /** Fences an established member, lets recovery run and returns the observed trace. */
    const fenceWith = async (logger) => {
      const delivered = []
      const run = await start({
        logger,
        fault: fenced,
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await advanceUntil(() => delivered.length === 1)
      run.arm()
      await advanceUntil(() => run.crashes.length > 0 || beats(run, false) > 1, 60000)
      run.state.queue['topic-a|0'] = [40, 41]
      await advanceUntil(() => delivered.length > 1)
      return {
        crashes: run.crashes.length,
        lost: run.log.filter((entry) => entry.kind === 'PARTITIONS_LOST').map((e) => e.partitions),
        zeroJoins: beats(run, false),
        delivered,
        commits: run.log.filter((entry) => entry.kind === 'commit').map((e) => [e.tp, e.offset]),
      }
    }

    it('recovers with a second epoch-zero join and resumed delivery, as with a working sink', async () => {
      const invoked = jest.fn()
      const throwing = createLogger({
        logCreator:
          () =>
          ({ log }) => {
            if (log.message !== LOST_LINE) return
            invoked()
            throw new Error('logging failed')
          },
      })
      const failing = await fenceWith(throwing)
      await teardown()
      jest.restoreAllMocks()
      const working = await fenceWith(createLogger({ logCreator: () => () => {} }))

      expect(invoked).toHaveBeenCalledTimes(1)
      expect(failing.crashes).toBe(0)
      expect(failing.zeroJoins).toBe(2)
      expect(failing.delivered).toEqual(['40', '41'])
      expect(failing).toEqual(working)
    }, 120000)
  })
  describe('a restartOnFailure policy failing with an unusual value', () => {
    const unreadable = () => {
      throw Object.defineProperty({}, 'message', {
        get() {
          throw new Error('unreadable')
        },
      })
    }
    const policies = [
      [
        'throws null synchronously',
        () => {
          throw null // eslint-disable-line no-throw-literal
        },
      ],
      ['rejects with null', () => Promise.reject(null)], // eslint-disable-line prefer-promise-reject-errors
      ['throws an object with an unreadable message', unreadable],
    ]

    it.each(policies)(
      'an ordinary handler crash whose policy %s: one CRASH with the original error, restart true, one delayed restart',
      async (_, failing) => {
        const policy = jest.fn(failing)
        const { logger, lines } = recordingLogger()
        const failure = Object.assign(new Error('handler failed'), { retriable: false })
        let thrown = false
        const run = await start({
          logger,
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          eachMessage: async () => {
            if (thrown) return
            thrown = true
            throw failure
          },
        })
        await advanceUntil(() => run.crashes.length > 0, 10000)
        const connects = run.cluster.connect.mock.calls.length
        const joins = beats(run, false)
        await jest.advanceTimersByTimeAsync(5000)

        expect(policy).toHaveBeenCalledTimes(1)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].restart).toBe(true)
        let cause = run.crashes[0].error
        while (cause !== failure && cause?.cause) cause = cause.cause
        expect(cause).toBe(failure)
        expect(run.cluster.connect.mock.calls.length).toBe(connects + 1)
        expect(beats(run, false)).toBe(joins + 1)
        expect(lines).not.toContain('Crash handler failed')
      },
      30000
    )

    it.each(policies)(
      'a fatal heartbeat at startup whose policy %s: one CRASH with restart false and no restart',
      async (_, failing) => {
        const policy = jest.fn(failing)
        const { logger, lines } = recordingLogger()
        const run = await start({
          logger,
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          initial: groupAuthorization,
        })
        await advanceUntil(() => run.crashes.length > 0, 10000)
        const connects = run.cluster.connect.mock.calls.length
        await jest.advanceTimersByTimeAsync(5000)

        expect(policy).toHaveBeenCalledTimes(1)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0]).toMatchObject({ restart: false })
        expect(run.crashes[0].error.code).toBe(GROUP_AUTHORIZATION_FAILED)
        expect(run.cluster.connect.mock.calls.length).toBe(connects)
        expect(beats(run, false)).toBe(1)
        expect(lines).not.toContain('Crash handler failed')
      },
      30000
    )
  })
  describe('a REBALANCING subscriber failing during a fatal membership loss', () => {
    /** Runs the fatal code-30 schedule with two REBALANCING subscribers, the first given. */
    const fatalWith = async (first) => {
      const policy = jest.fn(async () => true)
      const { logger, lines } = recordingLogger()
      const run = await start({
        logger,
        retry: { initialRetryTime: 300, restartOnFailure: policy },
      })
      const order = []
      run.consumer.on(run.consumer.events.REBALANCING, () => {
        order.push('first')
        return first()
      })
      run.consumer.on(run.consumer.events.REBALANCING, () => order.push('second'))
      run.arm()
      await advanceUntil(() => run.crashes.length > 0, 20000)
      await jest.advanceTimersByTimeAsync(5000)
      return {
        order,
        policy: policy.mock.calls.map(([error]) => error.code),
        crashes: run.crashes.map(({ error, restart }) => [error.code, restart]),
        diagnostics: lines.filter((line) => line.startsWith('Failed to execute listener')),
        trace: run.log.map(({ kind, epoch, partitions }) => [kind, epoch, partitions]),
      }
    }

    it.each([
      [
        'throws synchronously',
        () => {
          throw new Error('subscriber threw')
        },
      ],
      ['rejects', async () => Promise.reject(new Error('subscriber threw'))],
    ])(
      'a first subscriber that %s: the second runs, one diagnostic, and the run crashes as with a working subscriber',
      async (_, failing) => {
        const failed = await fatalWith(failing)
        await teardown()
        jest.restoreAllMocks()
        const working = await fatalWith(() => {})

        expect(failed.order).toEqual(['first', 'second'])
        expect(failed.diagnostics).toEqual(['Failed to execute listener: subscriber threw'])
        expect(failed.policy).toEqual([GROUP_AUTHORIZATION_FAILED])
        expect(failed.crashes).toEqual([[GROUP_AUTHORIZATION_FAILED, false]])
        expect(failed.trace).toEqual(working.trace)
        expect(working.diagnostics).toEqual([])
      },
      60000
    )
  })
  describe('a current consumer-group heartbeat answered with a fatal code the error table does not list', () => {
    /**
     * Answers an established member's next positive-epoch heartbeat with `make()` and waits for
     * the crash, then 60 s more. Returns the trace after the fault with times dropped.
     */
    const fatalAnswer = async (make, { policy = jest.fn(async () => true) } = {}) => {
      let answered = null
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        fault: () => (answered = make()),
      })
      run.arm()
      await advanceUntil(() => run.crashes.length > 0, 20000)
      const crashedAt = run.log.length
      await jest.advanceTimersByTimeAsync(60000)
      const fault = run.faultAt()
      return {
        answered,
        policy,
        crashes: run.crashes,
        after: run.log
          .filter((entry) => entry.at === undefined || entry.at >= fault)
          .map(({ kind, epoch }) => [kind, epoch]),
        sendsAfterCrash: run.log.slice(crashedAt).filter(({ kind }) => kind !== 'STOP'),
        connects: run.cluster.connect.mock.calls.length,
      }
    }

    it.each([
      ['GROUP_ID_NOT_FOUND (69)', 69],
      ['an unrecognised wire code (1042)', 1042],
      ['a code with a generic retriable flag (7)', 7],
    ])(
      '%s ends the run like GROUP_AUTHORIZATION_FAILED: one policy call, one CRASH with restart false, nothing sent after it',
      async (_, code) => {
        const observed = await fatalAnswer(() => protocolError(code))
        await teardown()
        jest.restoreAllMocks()
        const control = await fatalAnswer(() => protocolError(GROUP_AUTHORIZATION_FAILED))

        expect(observed.policy).toHaveBeenCalledTimes(1)
        expect(inCauseChain(observed.policy.mock.calls[0][0], observed.answered)).toBe(true)
        expect(observed.crashes).toHaveLength(1)
        expect(observed.crashes[0].restart).toBe(false)
        expect(inCauseChain(observed.crashes[0].error, observed.answered)).toBe(true)
        expect(observed.sendsAfterCrash).toEqual([])
        expect(observed.connects).toBe(1)
        expect(observed.after).toEqual(control.after)
      },
      120000
    )

    it('a policy clearing the public code and type of the answer still cannot restart the run', async () => {
      const policy = jest.fn(async (error) => {
        for (let e = error; e; e = e.cause) {
          e.code = undefined
          e.type = undefined
        }
        return true
      })
      const observed = await fatalAnswer(() => protocolError(69), { policy })

      expect(policy).toHaveBeenCalledTimes(1)
      expect(observed.crashes).toHaveLength(1)
      expect(observed.crashes[0].restart).toBe(false)
      expect(observed.connects).toBe(1)
    }, 120000)

    it.each([
      ['GROUP_ID_NOT_FOUND (69)', 69],
      ['an unrecognised wire code (1042)', 1042],
      ['REBALANCE_IN_PROGRESS (27)', 27],
      ['a code with a generic retriable flag (7)', 7],
    ])(
      '%s answering the first join ends the run after one join request with one CRASH and restart false',
      async (_, code) => {
        const policy = jest.fn(async () => true)
        let answered = null
        const run = await start({
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          initial: () => (answered = protocolError(code)),
          eachMessage: async () => {},
        })
        await advanceUntil(() => run.crashes.length > 0, 20000)
        await jest.advanceTimersByTimeAsync(60000)

        expect(beats(run, false)).toBe(1)
        expect(policy).toHaveBeenCalledTimes(1)
        expect(inCauseChain(policy.mock.calls[0][0], answered)).toBe(true)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].restart).toBe(false)
        expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
        expect(run.log.filter(({ kind }) => kind === 'fetch')).toEqual([])
        expect(run.cluster.connect).toHaveBeenCalledTimes(1)
      },
      120000
    )

    it('GROUP_ID_NOT_FOUND answering the leave heartbeat completes the disconnect without a CRASH', async () => {
      const run = await start({})
      const answer = run.state.answer
      run.state.answer = (req) =>
        req.memberEpoch < 0 ? Promise.reject(protocolError(69)) : answer(req)
      const closing = settled(run.consumer.disconnect())
      await advanceUntil(() => closing.done, 10000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.log.some(({ kind, epoch }) => kind === 'heartbeat' && epoch < 0)).toBe(true)
      expect(run.crashes).toEqual([])
      forget()
    })
  })
  describe('a fence and an independent non-retriable handler failure in the same fetch attempt', () => {
    /**
     * Holds the first delivery, fences the member, then releases the handler into `settle`.
     * Returns the observed trace after the fence was accepted.
     */
    const fenceThenRelease = async ({ api = 'eachMessage', settle, hook, wait = true } = {}) => {
      const policy = jest.fn(async () => false)
      const entered = jest.fn()
      let run = null
      const handler = async (payload, held) => {
        entered()
        if (entered.mock.calls.length > 1) return
        await held.promise
        return settle(payload, run)
      }
      run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        fault: fenced,
        [api]: handler,
        hook,
      })
      await advanceUntil(() => entered.mock.calls.length === 1)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      run.held.resolve()
      if (!wait) return { run, policy }
      await advanceUntil(() => run.crashes.length > 0 || beats(run, false) > 1, 20000)
      await jest.advanceTimersByTimeAsync(5000)
      return {
        run,
        policy,
        deliveries: entered.mock.calls.length,
        lost: run.log.filter(({ kind }) => kind === 'PARTITIONS_LOST').map((e) => e.partitions),
        zeroJoins: beats(run, false),
        commits: run.log.filter(({ kind }) => kind === 'commit'),
      }
    }

    it.each(['eachMessage', 'eachBatch'])(
      '%s: the loss is handled once, the member rejoins, then one CRASH carries the handler failure',
      async (api) => {
        const failure = Object.assign(new Error('handler failed'), { retriable: false })
        const observed = await fenceThenRelease({
          api,
          settle: () => {
            throw failure
          },
        })

        expect(observed.lost).toEqual([[{ topic: 'topic-a', partitions: [0] }]])
        expect(observed.commits).toEqual([])
        expect(observed.zeroJoins).toBe(2)
        expect(observed.policy).toHaveBeenCalledTimes(1)
        expect(inCauseChain(observed.policy.mock.calls[0][0], failure)).toBe(true)
        expect(observed.run.crashes).toHaveLength(1)
        expect(observed.run.crashes[0].restart).toBe(false)
        expect(inCauseChain(observed.run.crashes[0].error, failure)).toBe(true)
        expect(observed.deliveries).toBe(1)
      },
      60000
    )

    it('a handler rethrowing the refusal of its own commit recovers without a CRASH', async () => {
      let refused = null
      const observed = await fenceThenRelease({
        settle: async ({ topic, partition }, run) => {
          try {
            await run.consumer.commitOffsets([{ topic, partition, offset: '41' }])
          } catch (error) {
            refused = error
            throw error
          }
        },
      })

      expect(refused?.name).toBe('KafkaJSCommitFailedError')
      expect(observed.lost).toEqual([[{ topic: 'topic-a', partitions: [0] }]])
      expect(observed.zeroJoins).toBe(2)
      expect(observed.policy).not.toHaveBeenCalled()
      expect(observed.run.crashes).toEqual([])
    }, 60000)

    it('a stop requested while the loss callback is held leaves no late CRASH for the handler failure', async () => {
      const hookHeld = deferred()
      const lostEntered = jest.fn()
      const { run, policy } = await fenceThenRelease({
        wait: false,
        settle: () => {
          throw Object.assign(new Error('handler failed'), { retriable: false })
        },
        hook: {
          onPartitionsLost: async () => {
            lostEntered()
            await hookHeld.promise
          },
        },
      })
      await advanceUntil(() => lostEntered.mock.calls.length > 0, 1000)
      expect(lostEntered).toHaveBeenCalledTimes(1)
      const stopping = settled(run.consumer.stop())
      await jest.advanceTimersByTimeAsync(10)
      hookHeld.resolve()
      await advanceUntil(() => stopping.done, 10000)
      await jest.advanceTimersByTimeAsync(5000)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(policy).not.toHaveBeenCalled()
      expect(run.crashes).toEqual([])
    }, 60000)

    it('a loss callback that throws during the recovery wins over the handler failure', async () => {
      const hookFailure = new Error('lost hook failed')
      const handlerFailure = Object.assign(new Error('handler failed'), { retriable: false })
      const observed = await fenceThenRelease({
        settle: () => {
          throw handlerFailure
        },
        hook: {
          onPartitionsLost: async () => {
            throw hookFailure
          },
        },
      })

      expect(observed.run.crashes).toHaveLength(1)
      expect(observed.run.crashes[0].error.name).toBe('KafkaJSRebalanceListenerError')
      expect(inCauseChain(observed.run.crashes[0].error, hookFailure)).toBe(true)
      expect(inCauseChain(observed.run.crashes[0].error, handlerFailure)).toBe(false)
      expect(observed.policy).toHaveBeenCalledTimes(1)
    }, 60000)
  })
})
