const Runner = require('../runner')
const RebalanceHooks = require('../rebalanceHooks')
const { KafkaJSNonRetriableError } = require('../../errors')
const handlerContext = require('../handlerContext')
const { createPublicRig, gate, settle } = require('./fixtures/hookRound')
const makeRunner = () =>
  Object.assign(Object.create(Runner.prototype), {
    rebalanceHooks: { has: () => true },
    consumerGroup: { ownership: { lose: jest.fn() } },
  })
const round = () => ({ guard: () => null, wait: (work) => work(), retire: jest.fn() })
describe('hook quiescence and captured reentry', () => {
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })
  it('waits only for revoked handlers and unlinks its waiter', async () => {
    const runner = makeRunner()
    const one = { topic: 'orders', partition: 0 }
    const kept = { topic: 'orders', partition: 1 }
    runner.beginHookBatch(one)
    runner.beginHookBatch(kept)
    let done = false
    const work = runner
      .waitForPartitions([{ topic: 'orders', partitions: [0] }], {
        deadlineAt: performance.now() + 1000,
        identity: { round: round(), membership: 7 },
      })
      .then(() => {
        done = true
      })
    await Promise.resolve()
    expect(done).toBe(false)
    runner.endHookBatch(one)
    await work
    expect(runner.hookHandlers.get('orders|1')).toBe(1)
    expect(runner.hookWaiters.size).toBe(0)
    runner.endHookBatch(kept)
    expect(runner.hookHandlers.size).toBe(0)
  })
  it('times out a held handler without false quiescence', async () => {
    jest.useFakeTimers()
    jest
      .spyOn(require('node:perf_hooks').performance, 'now')
      .mockImplementation(() => performance.now())
    const baseline = jest.getTimerCount()
    const runner = makeRunner()
    const r = round()
    runner.beginHookBatch({ topic: 'orders', partition: 0 })
    const work = runner
      .waitForPartitions([{ topic: 'orders', partitions: [0] }], {
        deadlineAt: performance.now() + 20,
        identity: { round: r, membership: 7 },
      })
      .catch((e) => e)
    await jest.advanceTimersByTimeAsync(20)
    expect(await work).toMatchObject({ name: 'KafkaJSRebalanceListenerError', reason: 'timeout' })
    expect(r.retire).toHaveBeenCalled()
    expect(runner.consumerGroup.ownership.lose).toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(baseline)
    expect(runner.hookWaiters.size).toBe(0)
  })
  it('allocates no handler table without a revoked callback', () => {
    const runner = makeRunner()
    runner.rebalanceHooks = null
    runner.beginHookBatch({ topic: 'orders', partition: 0 })
    expect(runner.hookHandlers).toBeUndefined()
  })
  it.each(['seek', 'commit', 'pause', 'resume', 'stop', 'disconnect', 'unsubscribe'])(
    'refuses a late %s under the original invocation',
    async () => {
      const held = gate()
      const old = { run: {}, round: {}, membership: 7, acquisitions: [] }
      let attempt
      const mutate = jest.fn()
      const hooks = new RebalanceHooks(
        {
          onPartitionsRevoked: async () => {
            await held.promise
            const record = RebalanceHooks.current()
            attempt = Promise.resolve()
              .then(() => {
                const error = RebalanceHooks.refusal(record)
                if (error) throw error
                mutate()
              })
              .catch((e) => e)
          },
        },
        { isCurrent: () => true, onExpire: jest.fn() }
      )
      const work = hooks
        .invoke('onPartitionsRevoked', {}, { deadlineAt: performance.now() + 1000, identity: old })
        .catch((e) => e)
      await Promise.resolve()
      const error = new Error('old run retired')
      hooks.retire(old, error)
      expect(await work).toBe(error)
      held.release()
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      expect(await attempt).toBe(error)
      expect(mutate).not.toHaveBeenCalled()
    }
  )
  it('clears both contexts only for internal work', async () => {
    await handlerContext.run({ topic: 'orders', partition: 0 }, async () => {
      const hooks = new RebalanceHooks(
        {
          onPartitionsRevoked: () => {
            expect(handlerContext.current()).toBeDefined()
            handlerContext.run(null, () =>
              RebalanceHooks.outside(() => {
                expect(handlerContext.current()).toBeNull()
                expect(RebalanceHooks.current()).toBeUndefined()
              })
            )
            expect(RebalanceHooks.current()).toBeDefined()
          },
        },
        { isCurrent: () => true, onExpire: jest.fn() }
      )
      await hooks.invoke(
        'onPartitionsRevoked',
        {},
        {
          deadlineAt: performance.now() + 1000,
          identity: { run: {}, round: {}, membership: 7, acquisitions: [] },
        }
      )
    })
  })
  it.each(['before waiter construction', 'inside waiter construction'])(
    'observes expiry %s with the real round and removes owned work',
    (boundary) => {
      const { spawnSync } = require('node:child_process')
      const child = spawnSync(
        process.execPath,
        [
          '-e',
          `
const Runner = require(process.argv[1])
const ConsumerGroup = require(process.argv[2])
const { performance } = require('node:perf_hooks')
let now = 0
Object.defineProperty(performance, 'now', { value: () => now })
const timers = new Set()
const schedule = global.setTimeout
const cancel = global.clearTimeout
global.setTimeout = (work, delay, ...args) => {
  const timer = schedule(() => { timers.delete(timer); work(...args) }, delay)
  timers.add(timer)
  return timer
}
global.clearTimeout = (timer) => { timers.delete(timer); return cancel(timer) }
const unhandled = []
process.on('unhandledRejection', error => unhandled.push(error.name))
const group = Object.assign(Object.create(ConsumerGroup.prototype), {
  idleWait: { disposed: false }, rebalanceTimeout: 20,
  cluster: { brokerPool: { allowAutoTopicCreation: false } },
  isStopping: () => false,
})
const round = group.beginRebalanceRound()
const losses = []
const runner = Object.assign(Object.create(Runner.prototype), {
  rebalanceHooks: { has: () => true },
  consumerGroup: { ownership: { lose: (error, token) => losses.push({ error, token }) } },
})
const batch = { topic: 'orders', partition: 0 }
runner.beginHookBatch(batch)
const waiters = new Set()
if (process.argv[3] === 'before waiter construction') {
  Object.defineProperty(runner, 'hookWaiters', { get: () => { now = 20; return waiters } })
} else {
  waiters.add = function (waiter) { now = 20; return Set.prototype.add.call(this, waiter) }
  runner.hookWaiters = waiters
}
let hooks = 0, applies = 0
const observe = runner.waitForPartitions([{ topic: 'orders', partitions: [0] }], {
  deadlineAt: round.roundDeadlineAt, identity: { round, membership: 7 },
}).then(() => { hooks++; applies++; return null }, error => error)
;(async () => {
  const failure = await observe
  const observedTimers = timers.size
  const observedWaiters = waiters.size
  runner.endHookBatch(batch)
  round.retire(failure)
  await round.wait(() => { hooks++; applies++ }).catch(() => {})
  await new Promise(resolve => setImmediate(resolve))
  await new Promise(resolve => setImmediate(resolve))
  process.stdout.write(JSON.stringify({
    name: failure?.name, reason: failure?.reason, unhandled,
    observedTimers, observedWaiters, timers: timers.size, waiters: waiters.size,
    handlers: runner.hookHandlers.size, hooks, applies,
    losses: losses.length, membership: losses[0]?.token,
  }))
})().catch(error => { process.stderr.write(error.stack); process.exitCode = 1 })
`,
          require.resolve('../runner'),
          require.resolve('../consumerGroup'),
          boundary,
        ],
        { encoding: 'utf8', timeout: 5000, env: process.env }
      )
      expect(child.error).toBeUndefined()
      expect(child.status).toBe(0)
      expect(child.stderr).toBe('')
      expect(JSON.parse(child.stdout)).toEqual({
        name: 'KafkaJSRebalanceListenerError',
        reason: 'timeout',
        unhandled: [],
        observedTimers: 0,
        observedWaiters: 0,
        timers: 0,
        waiters: 0,
        handlers: 0,
        hooks: 0,
        applies: 0,
        losses: 1,
        membership: 7,
      })
    }
  )
})

describe('public re-entry from a rebalance callback', () => {
  let rig
  afterEach(async () => {
    await rig?.consumer.disconnect().catch(() => {})
    rig?.restore()
    rig = null
  })

  const TOPIC = 't'
  const groupOf = () => rig.spies.joinAndSync.mock.contexts[0]
  const offsets = [{ topic: TOPIC, partition: 0, offset: '3' }]
  /** The public calls a callback may attempt, each returning its settled outcome. */
  const attempts = {
    seek: async (consumer) => consumer.seek({ topic: TOPIC, partition: 0, offset: '4' }),
    pause: async (consumer) => consumer.pause([{ topic: TOPIC, partitions: [0] }]),
    resume: async (consumer) => consumer.resume([{ topic: TOPIC, partitions: [0] }]),
    commitOffsets: (consumer) => consumer.commitOffsets(offsets),
    stop: (consumer) => consumer.stop(),
    disconnect: (consumer) => consumer.disconnect(),
    subscribe: (consumer) => consumer.subscribe({ topic: TOPIC }),
    run: (consumer) => consumer.run({ eachMessage: () => {} }),
    unsubscribe: (consumer) => consumer.unsubscribe(),
    position: (consumer) => consumer.position({ topic: TOPIC, partition: 0 }),
    committed: (consumer) => consumer.committed([{ topic: TOPIC, partition: 0 }]),
  }
  const LIFECYCLE = ['stop', 'disconnect', 'unsubscribe']
  const READS = ['position', 'committed']
  const outcome = (call) =>
    Promise.resolve()
      .then(call)
      .then(
        () => 'accepted',
        (error) => error
      )

  /** Runs one initial round whose single callback records what each API does inside and after it. */
  const exercise = async (hook, mode) => {
    rig = createPublicRig({ cooperative: mode === 'cooperative' })
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    const inside = {}
    const after = {}
    const detached = []
    const listener = {
      [hook]: async () => {
        for (const [name, call] of Object.entries(attempts)) {
          if (LIFECYCLE.includes(name) || READS.includes(name)) continue
          inside[name] = await outcome(() => call(rig.consumer))
        }
        detached.push(
          new Promise((resolve) => setTimeout(resolve, 5)).then(async () => {
            for (const [name, call] of Object.entries(attempts)) {
              after[name] = await outcome(() => call(rig.consumer))
            }
          })
        )
      },
    }
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: listener })
    await Promise.all(detached)
    return { inside, after }
  }

  describe.each(['eager', 'cooperative'])('%s group', (mode) => {
    it.each(['onPartitionsRevoked', 'onPartitionsAssigned'])(
      '%s: a current callback may seek, pause, resume and commit, but not run or subscribe',
      async (hook) => {
        if (mode === 'cooperative' && hook === 'onPartitionsRevoked') return
        const { inside } = await exercise(hook, mode)
        for (const name of ['seek', 'pause', 'resume'])
          expect([hook, name, inside[name]]).toEqual([hook, name, 'accepted'])
        expect(inside.run).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(inside.subscribe).toBeInstanceOf(KafkaJSNonRetriableError)
      }
    )

    it.each(['onPartitionsRevoked', 'onPartitionsAssigned'])(
      '%s: a settled callback is refused before any effect, for every public call',
      async (hook) => {
        if (mode === 'cooperative' && hook === 'onPartitionsRevoked') return
        const { after } = await exercise(hook, mode)
        for (const [name, result] of Object.entries(after)) {
          expect(result).toBeInstanceOf(Error)
          expect([name, result.message]).toEqual([
            name,
            'The rebalance hook invocation is no longer current',
          ])
        }
        // none of the refused calls reached the group
        expect(rig.spies.seek).toHaveBeenCalledTimes(1)
        expect(rig.spies.pause).toHaveBeenCalledTimes(1)
        expect(rig.spies.resume).toHaveBeenCalledTimes(1)
        expect(rig.spies.leave).not.toHaveBeenCalled()
      }
    )
  })

  it('a lost callback is current but cannot commit', async () => {
    rig = createPublicRig()
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    let result
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({
      eachMessage: () => {},
      rebalanceListener: {
        onPartitionsAssigned: () => {},
        onPartitionsLost: async () => {
          result = await outcome(() => attempts.commitOffsets(rig.consumer))
        },
      },
    })
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3 })
    const group = groupOf()
    group.ownership.lose(new Error('membership lost'), group.ownership.token())
    const commitsBefore = rig.spies.commitOffsets.mock.calls.length
    await group.rebalanceCoordinator.runRound('lost', 'lost')
    expect(result).toBeInstanceOf(Error)
    expect(rig.spies.commitOffsets).toHaveBeenCalledTimes(commitsBefore)
  })

  it('a valid callback commits before release through its own membership', async () => {
    rig = createPublicRig()
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    let committed
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({
      eachMessage: () => {},
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          committed = await outcome(() => attempts.commitOffsets(rig.consumer))
        },
      },
    })
    expect(committed).toBe('accepted')
    expect(rig.spies.commitOffsets).toHaveBeenCalledTimes(1)
  })

  describe("a hook commit keeps the invocation's original acquisition cells", () => {
    const CHANGES = {
      acquisition: () => groupOf().acquired.set(`${TOPIC}|0`, { acquiredAt: 999, changedAt: 999 }),
      incarnation: () => {
        rig.cluster.topicId = () => 'recreated'
      },
      assignment: () => {
        groupOf().assignmentEpoch += 1
      },
    }
    /** The transmitting spy: it asks the commit's guard the way the sender does. */
    const transmit = () =>
      rig.spies.commitOffsets.mockImplementation(async (_offsets, { guard }) => {
        const refusal = guard()
        if (refusal) throw refusal
      })

    /** Runs a first round whose assigned callback commits `named` after `change` ran. */
    const commitFromAssigned = async (change, named = offsets) => {
      rig = createPublicRig()
      transmit()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
      let result
      await rig.consumer.subscribe({ topic: TOPIC })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            change?.()
            result = await outcome(() => rig.consumer.commitOffsets(named))
          },
        },
      })
      return result
    }

    it.each(Object.keys(CHANGES))(
      'refuses a commit of a partition whose %s changed under the same membership',
      async (kind) => {
        const result = await commitFromAssigned(CHANGES[kind])
        expect(result).toMatchObject({
          name: 'KafkaJSCommitFailedError',
          reason: 'stale-assignment',
        })
      }
    )

    it('accepts the same commit while every cell is unchanged', async () => {
      expect(await commitFromAssigned(null)).toBe('accepted')
      expect(rig.spies.commitOffsets).toHaveBeenCalledTimes(1)
    })

    it('leaves a partition the invocation did not name to the ordinary explicit-offset rules', async () => {
      const other = [{ topic: TOPIC, partition: 3, offset: '9' }]
      expect(await commitFromAssigned(CHANGES.acquisition, other)).toBe('accepted')
    })

    it('refuses when the cell changes between the call and the transmission', async () => {
      rig = createPublicRig()
      rig.spies.commitOffsets.mockImplementation(async (_offsets, { guard }) => {
        CHANGES.acquisition()
        const refusal = guard()
        if (refusal) throw refusal
      })
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
      let result
      await rig.consumer.subscribe({ topic: TOPIC })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            result = await outcome(() => rig.consumer.commitOffsets(offsets))
          },
        },
      })
      expect(result).toMatchObject({ name: 'KafkaJSCommitFailedError', reason: 'stale-assignment' })
    })

    it('still commits from a revoked callback for the partitions it is giving up', async () => {
      rig = createPublicRig()
      transmit()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
      let result
      await rig.consumer.subscribe({ topic: TOPIC })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsRevoked: async ({ partitions }) => {
            if (partitions.length) result = await outcome(() => rig.consumer.commitOffsets(offsets))
          },
        },
      })
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3 })
      await groupOf().rebalanceCoordinator.runRound('requested', 'revoked')
      expect(result).toBe('accepted')
    })
  })

  it.each(['stop', 'disconnect', 'unsubscribe'])(
    'a valid callback requests %s without awaiting its own round, and the member leaves once',
    async (api) => {
      rig = createPublicRig()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
      const settled = []
      await rig.consumer.subscribe({ topic: TOPIC })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            await attempts[api](rig.consumer)
            settled.push(api)
          },
        },
      })
      await new Promise((resolve) => setTimeout(resolve, 30))
      expect(settled).toEqual([api])
      expect(rig.spies.leave).toHaveBeenCalledTimes(1)
      if (api === 'disconnect') expect(rig.cluster.disconnect).toHaveBeenCalled()
      if (api === 'unsubscribe') expect(rig.consumer.subscription()).toEqual([])
    }
  )

  it.each(['stop', 'disconnect', 'unsubscribe'])(
    'a callback of a running consumer requests %s and settles while the cleanup is still running',
    async (api) => {
      rig = createPublicRig()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
      const leaving = gate()
      rig.spies.leave.mockImplementation(() => leaving.promise)
      let armed = false
      const settled = []
      await rig.consumer.subscribe({ topic: TOPIC })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            if (!armed) return
            await attempts[api](rig.consumer)
            settled.push(api)
          },
        },
      })
      armed = true
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3 })
      await groupOf().rebalanceCoordinator.runRound('requested', 'revoked')
      await new Promise((resolve) => setTimeout(resolve, 30))

      expect(settled).toEqual([api])
      expect(rig.spies.leave).toHaveBeenCalledTimes(1)
      leaving.release()
    }
  )

  it('a manual consumer rejects a listener before any state changes and stays usable', async () => {
    rig = createPublicRig()
    await rig.consumer.assign([{ topic: TOPIC, partitions: [0] }])
    await expect(
      rig.consumer.run({ eachMessage: () => {}, rebalanceListener: {} })
    ).rejects.toThrow('rebalanceListener is unavailable with manual assignment')
    expect(rig.consumer.assignment()).toEqual([])
    await rig.consumer.run({ eachMessage: () => {} })
    expect(rig.consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  it('a callback of a retired run cannot touch the run that replaced it, whatever the equal values', async () => {
    rig = createPublicRig()
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    const held = gate()
    let late
    await rig.consumer.subscribe({ topic: TOPIC })
    const first = rig.consumer.run({
      eachMessage: () => {},
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          await held.promise
          late = {
            seek: await outcome(() => attempts.seek(rig.consumer)),
            pause: await outcome(() => attempts.pause(rig.consumer)),
            stop: await outcome(() => attempts.stop(rig.consumer)),
          }
        },
      },
    })
    await settle()
    await rig.consumer.disconnect()
    await first
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: {} })
    const seeks = rig.spies.seek.mock.calls.length
    const leaves = rig.spies.leave.mock.calls.length
    held.release()
    await settle()
    for (const result of Object.values(late ?? {})) expect(result).toBeInstanceOf(Error)
    expect(rig.spies.seek).toHaveBeenCalledTimes(seeks)
    expect(rig.spies.pause).not.toHaveBeenCalled()
    expect(rig.spies.leave).toHaveBeenCalledTimes(leaves)
  })
})
