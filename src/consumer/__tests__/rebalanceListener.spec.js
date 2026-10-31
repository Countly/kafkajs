const RebalanceHooks = require('../rebalanceHooks')
const Batch = require('../batch')
const {
  createHookRig,
  createPublicRig,
  recordingListener,
  gate,
  settle,
} = require('./fixtures/hookRound')
const { KafkaJSRebalanceListenerError, KafkaJSNonRetriableError } = require('../../errors')
const identity = () => ({
  run: {},
  round: {},
  membership: 7,
  acquisitions: [{ topic: 'orders', partition: 0, acquiredAt: 1, topicId: 'new' }],
})
const payload = { groupId: 'group', memberId: 'member', generationId: 1, partitions: [] }
const owner = (listener, options = {}) =>
  new RebalanceHooks(RebalanceHooks.validate(listener, 'classic'), {
    isCurrent: () => true,
    onExpire: jest.fn(),
    ...options,
  })

describe('awaited rebalance callbacks', () => {
  afterEach(() => jest.useRealTimers())
  it.each(['onPartitionsRevoked', 'onPartitionsAssigned', 'onPartitionsLost'])(
    'awaits %s under immutable identity',
    async (name) => {
      const held = gate()
      let entered
      const id = identity()
      const hooks = owner({
        [name]: async (actual) => {
          expect(actual).toEqual(payload)
          entered = RebalanceHooks.current()
          await held.promise
        },
      })
      let done = false
      const work = hooks
        .invoke(name, payload, { deadlineAt: performance.now() + 1000, identity: id })
        .then(() => {
          done = true
        })
      await Promise.resolve()
      await Promise.resolve()
      expect(done).toBe(false)
      expect(Object.isFrozen(entered.context)).toBe(true)
      expect(Object.isFrozen(entered.context.acquisitions[0])).toBe(true)
      expect(entered.context.run).toBe(id.run)
      expect(entered.context.membership).toBe(id.membership)
      held.release()
      await work
      expect(RebalanceHooks.refusal(entered)).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(hooks.pending.size).toBe(0)
    }
  )
  it('calls lost or its revoked fallback exactly once', async () => {
    const revoked = jest.fn()
    const lost = jest.fn()
    for (const listener of [
      { onPartitionsRevoked: revoked },
      { onPartitionsRevoked: revoked, onPartitionsLost: lost },
    ]) {
      await owner(listener).invoke('onPartitionsLost', payload, {
        deadlineAt: performance.now() + 1000,
        identity: identity(),
      })
    }
    expect(revoked).toHaveBeenCalledTimes(1)
    expect(lost).toHaveBeenCalledTimes(1)
  })
  it.each([null, 'listener', [], { onPartitionsRevoked: 1 }, { onPartitionsAssigned: undefined }])(
    'rejects invalid listener %p',
    (listener) => {
      expect(() => RebalanceHooks.validate(listener, 'classic')).toThrow(KafkaJSNonRetriableError)
    }
  )
  it('rejects manual and missing-group listeners and copies callbacks', () => {
    expect(RebalanceHooks.validate(undefined, 'manual', false)).toBeNull()
    expect(() => RebalanceHooks.validate({}, 'manual')).toThrow('manual')
    expect(() => RebalanceHooks.validate({}, 'classic', false)).toThrow(
      'Consumer groupId must be a non-empty string.'
    )
    const fn = jest.fn()
    const listener = { onPartitionsAssigned: fn }
    const copy = RebalanceHooks.validate(listener, 'classic')
    listener.onPartitionsAssigned = jest.fn()
    expect(copy.onPartitionsAssigned).toBe(fn)
  })
  const hostile = {
    toString() {
      throw Error('conversion')
    },
    valueOf() {
      throw Error('conversion')
    },
  }
  it.each([new Error('offset store'), 'store failed', null, undefined, hostile, Symbol('failed')])(
    'contains thrown value %p',
    async (thrownValue) => {
      const hooks = owner({
        onPartitionsAssigned: () => {
          throw thrownValue
        },
      })
      const error = await hooks
        .invoke('onPartitionsAssigned', payload, {
          deadlineAt: performance.now() + 1000,
          identity: identity(),
        })
        .catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSRebalanceListenerError)
      expect(error.reason).toBe('threw')
      expect(error.thrownValue).toBe(thrownValue)
      expect(error.cause).toBeInstanceOf(Error)
      if (thrownValue instanceof Error) expect(error.cause).toBe(thrownValue)
      if (thrownValue === hostile) expect(error.cause.message).toBe('<unreadable thrown value>')
      expect(hooks.pending.size).toBe(0)
    }
  )
  it('times out a hung callback and clears timers', async () => {
    jest.useFakeTimers()
    const baseline = jest.getTimerCount()
    let now = 0
    const expire = jest.fn()
    const hooks = owner(
      { onPartitionsAssigned: () => new Promise(() => {}) },
      { now: () => now, onExpire: expire }
    )
    const work = hooks
      .invoke('onPartitionsAssigned', payload, { deadlineAt: 20, identity: identity() })
      .catch((e) => e)
    await Promise.resolve()
    now = 20
    await jest.advanceTimersByTimeAsync(20)
    expect((await work)?.reason).toBe('timeout')
    expect(expire).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(baseline)
    expect(hooks.pending.size).toBe(0)
  })
  it('clears timers after both ordinary success and ordinary failure', async () => {
    jest.useFakeTimers()
    const baseline = jest.getTimerCount()
    for (const fails of [false, true]) {
      const hooks = owner(
        {
          onPartitionsAssigned: () => {
            if (fails) throw new Error('store')
          },
        },
        { now: () => 0 }
      )
      await hooks
        .invoke('onPartitionsAssigned', payload, { deadlineAt: 20, identity: identity() })
        .catch(() => {})
      expect(jest.getTimerCount()).toBe(baseline)
      expect(hooks.pending.size).toBe(0)
    }
  })
  it('checks actual time before a delayed timer permits mutation', async () => {
    let now = 0
    let failure
    const expire = jest.fn()
    const hooks = owner(
      {
        onPartitionsRevoked: () => {
          now = 50
          failure = RebalanceHooks.refusal()
        },
      },
      { now: () => now, onExpire: expire }
    )
    const error = await hooks
      .invoke('onPartitionsRevoked', payload, { deadlineAt: 50, identity: identity() })
      .catch((e) => e)
    expect(error).toBe(failure)
    expect(error?.reason).toBe('timeout')
    expect(expire).toHaveBeenCalledTimes(1)
  })
  it('starts no callback after the original deadline', async () => {
    const callback = jest.fn()
    const hooks = owner({ onPartitionsAssigned: callback }, { now: () => 10 })
    await expect(
      hooks.invoke('onPartitionsAssigned', payload, { deadlineAt: 10, identity: identity() })
    ).rejects.toMatchObject({ reason: 'timeout' })
    expect(callback).not.toHaveBeenCalled()
  })
  it('retires client waits and contains late user rejection', async () => {
    const held = gate()
    const id = identity()
    let record
    const hooks = owner({
      onPartitionsRevoked: async () => {
        record = RebalanceHooks.current()
        await held.promise
        throw Error('late failure')
      },
    })
    const work = hooks
      .invoke('onPartitionsRevoked', payload, {
        deadlineAt: performance.now() + 1000,
        identity: id,
      })
      .catch((e) => e)
    await Promise.resolve()
    const stopped = new Error('captured run stopped')
    hooks.retire({ run: id.run }, stopped)
    expect(await work).toBe(stopped)
    expect(RebalanceHooks.refusal(record)).toBe(stopped)
    held.release()
    await Promise.resolve()
    await Promise.resolve()
    expect(hooks.pending.size).toBe(0)
  })
  it('does not retire an equal-values replacement run', async () => {
    const held = gate()
    const old = identity()
    const next = identity()
    const hooks = owner({ onPartitionsAssigned: () => held.promise })
    const work = hooks.invoke('onPartitionsAssigned', payload, {
      deadlineAt: performance.now() + 1000,
      identity: next,
    })
    await Promise.resolve()
    hooks.retire({ run: old.run }, new Error('old run'))
    expect(hooks.pending.size).toBe(1)
    held.release()
    await work
  })
  it('clears hook context for internally scheduled cleanup', async () => {
    let inside
    const hooks = owner({
      onPartitionsRevoked: () => {
        expect(RebalanceHooks.current()).toBeDefined()
        inside = RebalanceHooks.outside(() => RebalanceHooks.current())
      },
    })
    await hooks.invoke('onPartitionsRevoked', payload, {
      deadlineAt: performance.now() + 1000,
      identity: identity(),
    })
    expect(inside).toBeUndefined()
  })
})

describe('awaited rebalance callbacks in a round', () => {
  let rig
  afterEach(async () => {
    await rig?.stop()
    rig = null
  })
  const eachHook = (log, hold = {}) => recordingListener(log, hold)
  const partitions = (...ids) => [{ topic: 'orders', partitions: ids }]

  it('an eager round awaits revoked with the empty assignment, then assigned, in order', async () => {
    const revokedGate = gate()
    const assignedGate = gate()
    rig = createHookRig({
      listener: (log) =>
        eachHook(log, {
          onPartitionsRevoked: () => revokedGate.promise,
          onPartitionsAssigned: () => assignedGate.promise,
        }),
    })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    let started = false
    const run = rig.start().then(() => {
      started = true
    })
    await settle()
    expect(rig.log).toEqual(['hook:onPartitionsRevoked'])
    revokedGate.release()
    await settle()
    expect(rig.log).toEqual([
      'hook:onPartitionsRevoked',
      'join',
      'event:PARTITIONS_ASSIGNED',
      'hook:onPartitionsAssigned',
    ])
    expect(started).toBe(false)
    assignedGate.release()
    await run
    expect(rig.log.at(-1)).toBe('event:GROUP_JOIN')
    expect(rig.onCrash).not.toHaveBeenCalled()
  })

  it('hook payloads carry the stage they run at and the event schema', async () => {
    const seen = []
    rig = createHookRig({
      listener: (log) =>
        eachHook(log, {
          onPartitionsRevoked: (payload) => seen.push(['revoked', payload]),
          onPartitionsAssigned: (payload) => seen.push(['assigned', payload]),
        }),
    })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    await rig.start()
    expect(seen).toEqual([
      ['revoked', { groupId: 'group', memberId: 'member-1', generationId: 1, partitions: [] }],
      [
        'assigned',
        { groupId: 'group', memberId: 'member-1', generationId: 2, partitions: partitions(0, 1) },
      ],
    ])
    const event = rig.emitted.find(({ name }) => name === 'PARTITIONS_ASSIGNED')
    expect(event.payload).toEqual(seen[1][1])
    expect(rig.emitted.some(({ name }) => name === 'PARTITIONS_REVOKED')).toBe(false)
  })

  it('a later eager round commits, then awaits revoked with the previous assignment before joining', async () => {
    rig = createHookRig({ listener: (log) => eachHook(log) })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    await rig.start()
    rig.log.length = 0
    rig.joins.push({ assignment: { orders: [1, 2] }, generationId: 3 })
    await rig.round('rebalance')
    expect(rig.log).toEqual([
      'commit',
      'hook:onPartitionsRevoked',
      'join',
      'event:PARTITIONS_REVOKED',
      'event:PARTITIONS_ASSIGNED',
      'hook:onPartitionsAssigned',
      'event:GROUP_JOIN',
    ])
  })

  it('a cooperative round revokes only the delta, in order, and assigned runs with an empty delta', async () => {
    const seen = []
    rig = createHookRig({
      mode: 'cooperative',
      listener: (log) =>
        eachHook(log, {
          onPartitionsRevoked: ({ partitions }) => seen.push(['revoked', partitions]),
          onPartitionsAssigned: ({ partitions }) => seen.push(['assigned', partitions]),
        }),
    })
    rig.joins.push({ assignment: { orders: [0, 1, 2] }, generationId: 2 })
    await rig.start()
    rig.log.length = 0
    seen.length = 0
    // A cooperative round that revoked something runs one follow-up round to confirm the result.
    rig.joins.push(
      { assignment: { orders: [0, 1, 3] }, generationId: 3 },
      { assignment: { orders: [0, 1, 3] }, generationId: 4 }
    )
    await rig.round('rebalance')
    expect(rig.log).toEqual([
      'join',
      'commit',
      'hook:onPartitionsRevoked',
      'event:PARTITIONS_REVOKED',
      'event:PARTITIONS_ASSIGNED',
      'hook:onPartitionsAssigned',
      'event:GROUP_JOIN',
      'join',
      'hook:onPartitionsAssigned',
      'event:GROUP_JOIN',
    ])
    expect(seen).toEqual([
      ['revoked', partitions(2)],
      ['assigned', partitions(3)],
      ['assigned', []],
    ])
  })

  it('added partitions stay held from admission until the assigned callback returns', async () => {
    const assignedGate = gate()
    let during
    rig = createHookRig({
      mode: 'cooperative',
      listener: (log) =>
        eachHook(log, {
          onPartitionsAssigned: async () => {
            during = rig.state.active()
            await assignedGate.promise
          },
        }),
    })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    const run = rig.start()
    await settle()
    expect(during).toEqual([{ topic: 'orders', partitions: [] }])
    expect(rig.state.isAssigned('orders', 0)).toBe(true)
    assignedGate.release()
    await run
    expect(rig.state.active()).toEqual([{ topic: 'orders', partitions: [0, 1] }])
    expect(rig.state.admissionHolds).toBeNull()
  })

  it('a revoked callback does not wait for a handler on a retained partition', async () => {
    rig = createHookRig({ mode: 'cooperative', listener: (log) => eachHook(log) })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    await rig.start()
    const stuck = gate()
    rig.runner.eachBatch = () => stuck.promise
    const batch = new Batch('orders', 5, {
      partition: 0,
      highWatermark: 6,
      messages: [{ offset: '5', key: null, value: 'v' }],
    })
    const running = rig.runner.handleBatch(batch)
    await settle()
    expect(rig.runner.hookHandlers?.get('orders|0')).toBe(1)
    rig.joins.push(
      { assignment: { orders: [0] }, generationId: 3 },
      { assignment: { orders: [0] }, generationId: 4 }
    )
    await rig.round('rebalance')
    expect(rig.log).toContain('hook:onPartitionsRevoked')
    stuck.release()
    await running
  })

  it.each([
    ['an explicit lost callback', { onPartitionsLost: true }, ['hook:onPartitionsLost']],
    [
      'the revoked callback as the fallback',
      { onPartitionsLost: false },
      ['hook:onPartitionsRevoked'],
    ],
  ])('a loss calls exactly one callback: %s', async (_, { onPartitionsLost }, expected) => {
    rig = createHookRig({
      listener: (log) => {
        const listener = eachHook(log)
        if (!onPartitionsLost) delete listener.onPartitionsLost
        return listener
      },
    })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    await rig.start()
    rig.log.length = 0
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 3 })
    await rig.round('lost', 'lost')
    expect(rig.log.filter((entry) => entry.startsWith('hook:'))).toEqual([
      ...expected,
      'hook:onPartitionsAssigned',
    ])
    expect(rig.log.indexOf('event:PARTITIONS_LOST')).toBeLessThan(rig.log.indexOf(expected[0]))
    expect(rig.log.indexOf(expected[0])).toBeLessThan(rig.log.indexOf('join'))
    expect(rig.log).not.toContain('commit')
  })

  it('a loss of an empty assignment calls neither lost nor its fallback', async () => {
    rig = createHookRig({ listener: (log) => eachHook(log) })
    await rig.start()
    rig.log.length = 0
    await rig.round('lost', 'lost')
    expect(rig.log.filter((entry) => entry.startsWith('hook:'))).toEqual([
      'hook:onPartitionsAssigned',
    ])
  })

  it('a run without a listener keeps its sequence and creates no callback owner', async () => {
    rig = createHookRig({ mode: 'eager' })
    rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
    await rig.start()
    rig.log.length = 0
    rig.joins.push({ assignment: { orders: [1] }, generationId: 3 })
    await rig.round('rebalance')
    expect(rig.group.rebalanceHooks).toBeNull()
    expect(rig.log).toEqual([
      'join',
      'event:PARTITIONS_REVOKED',
      'event:PARTITIONS_ASSIGNED',
      'event:GROUP_JOIN',
    ])
  })

  describe('ordinary failure', () => {
    const hostile = {
      toString() {
        throw new Error('conversion')
      },
    }
    const thrown = [
      new Error('offset store'),
      'store failed',
      null,
      undefined,
      hostile,
      Symbol('failed'),
      { code: 7 },
    ]
    const labels = ['Error', 'string', 'null', 'undefined', 'hostile', 'symbol', 'plain object']

    it.each(thrown.map((value, index) => [labels[index], value]))(
      'finishes the live round and ends the chain with the exact first error (%s)',
      async (_, value) => {
        const failure = value
        rig = createHookRig({
          listener: (log) =>
            eachHook(log, {
              onPartitionsRevoked: () => {
                throw failure
              },
              onPartitionsAssigned: () => {
                throw new Error('second failure')
              },
            }),
        })
        rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
        await rig.start()
        expect(rig.onCrash).toHaveBeenCalledTimes(1)
        const error = rig.onCrash.mock.calls[0]?.[0]
        expect(error).toBeInstanceOf(KafkaJSRebalanceListenerError)
        expect(error.hook).toBe('onPartitionsRevoked')
        expect(error.reason).toBe('threw')
        expect(error.thrownValue).toBe(failure)
        expect(error.cause).toBeInstanceOf(Error)
        if (failure instanceof Error) expect(error.cause).toBe(failure)
        // the assignment was applied, the assigned callback ran once and the join was not retried
        expect(rig.group.assigned()).toEqual(partitions(0, 1))
        expect(rig.log.filter((entry) => entry === 'hook:onPartitionsAssigned')).toHaveLength(1)
        expect(rig.log.filter((entry) => entry === 'join')).toHaveLength(1)
        expect(rig.state.admissionHolds).toBeNull()
      }
    )

    it('keeps the first of two different callback failures and never wraps it', async () => {
      const first = new Error('revoked failed')
      const second = new Error('assigned failed')
      const behaviors = {}
      rig = createHookRig({
        mode: 'cooperative',
        listener: (log) => eachHook(log, behaviors),
      })
      rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
      await rig.start()
      behaviors.onPartitionsRevoked = () => {
        throw first
      }
      behaviors.onPartitionsAssigned = () => {
        throw second
      }
      rig.joins.push({ assignment: { orders: [0, 2] }, generationId: 3 })
      const error = await rig.round('rebalance').catch((e) => e)
      expect(error?.name).toBe('KafkaJSRebalanceListenerError')
      expect(error?.cause).toBe(first)
      expect(error?.constructor.name).not.toBe('KafkaJSNumberOfRetriesExceeded')
    })

    it('a failing callback with the old events still fires them at their old points', async () => {
      rig = createHookRig({
        listener: (log) =>
          eachHook(log, {
            onPartitionsAssigned: () => {
              throw new Error('store')
            },
          }),
      })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 2 })
      await rig.start()
      expect(rig.log).toEqual([
        'hook:onPartitionsRevoked',
        'join',
        'event:PARTITIONS_ASSIGNED',
        'hook:onPartitionsAssigned',
        'event:GROUP_JOIN',
      ])
    })
  })

  describe('expiry', () => {
    it('a never-settling revoked callback ends an eager round with a typed timeout and no join', async () => {
      rig = createHookRig({
        rebalanceTimeout: 40,
        listener: (log) => eachHook(log, { onPartitionsRevoked: () => new Promise(() => {}) }),
      })
      await rig.start()
      const error = rig.onCrash.mock.calls[0]?.[0]
      expect(error).toBeInstanceOf(KafkaJSRebalanceListenerError)
      expect(error?.reason).toBe('timeout')
      expect(error?.hook).toBe('onPartitionsRevoked')
      expect(rig.log).not.toContain('join')
      expect(rig.group.rebalanceHooks?.pending.size).toBe(0)
      expect(rig.state.admissionHolds).toBeNull()
    })

    it('a never-settling assigned callback fails the round without publishing a join', async () => {
      rig = createHookRig({
        rebalanceTimeout: 40,
        listener: (log) => eachHook(log, { onPartitionsAssigned: () => new Promise(() => {}) }),
      })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 2 })
      await rig.start()
      const error = rig.onCrash.mock.calls[0]?.[0]
      expect(error?.reason).toBe('timeout')
      expect(error?.hook).toBe('onPartitionsAssigned')
      expect(rig.log).not.toContain('event:GROUP_JOIN')
      expect(rig.state.admissionHolds).toBeNull()
      expect(rig.group.rebalanceHooks?.pending.size).toBe(0)
    })

    it('a held pre-release commit ends an eager round at its deadline and refuses a late finish', async () => {
      rig = createHookRig({ rebalanceTimeout: 40, listener: (log) => eachHook(log) })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 2 })
      await rig.start()
      rig.log.length = 0
      const held = gate()
      const guards = []
      rig.group.commitOffsets.mockImplementation(async (_offsets, { guard }) => {
        guards.push(guard)
        await held.promise
      })
      let settled = false
      const round = rig.round('rebalance').then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      round.then(() => {
        settled = true
      })
      let atDeadline
      try {
        await new Promise((resolve) => setTimeout(resolve, 120))
        atDeadline = { entered: guards.length, settled, admission: rig.runner.admissionQuiescing }
      } finally {
        held.release()
      }
      const { error } = await round
      expect(atDeadline).toEqual({ entered: 1, settled: true, admission: false })
      expect(error?.type).toBe('JOIN_DEADLINE_EXCEEDED')
      expect(guards[0]()).toBe(error)
      expect(rig.log).not.toContain('hook:onPartitionsRevoked')
      expect(rig.log).not.toContain('join')
      expect(rig.state.admissionHolds).toBeNull()
    })

    it('an eligible pre-release commit inside the budget leaves the round unchanged', async () => {
      rig = createHookRig({ rebalanceTimeout: 5000, listener: (log) => eachHook(log) })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 2 })
      await rig.start()
      const guards = []
      rig.group.commitOffsets.mockImplementation(async (_offsets, { guard }) => {
        guards.push(guard)
      })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 3 })
      await rig.round('rebalance')
      expect(guards).toHaveLength(1)
      expect(rig.log).toContain('hook:onPartitionsRevoked')
      expect(rig.log).toContain('join')
      expect(rig.runner.admissionQuiescing).toBe(false)
    })

    it('a cooperative revoked callback that never settles leaves the old assignment and no hold', async () => {
      const behaviors = {}
      rig = createHookRig({
        mode: 'cooperative',
        rebalanceTimeout: 40,
        listener: (log) => eachHook(log, behaviors),
      })
      rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
      await rig.start()
      behaviors.onPartitionsRevoked = () => new Promise(() => {})
      rig.joins.push({ assignment: { orders: [0] }, generationId: 3 })
      const error = await rig.round('rebalance').catch((e) => e)
      expect(error?.reason).toBe('timeout')
      expect(rig.group.assigned()).toEqual(partitions(0, 1))
      expect(rig.state.admissionHolds).toBeNull()
      expect(rig.group.rebalanceHooks?.pending.size).toBe(0)
    })

    it('a handler that never returns times out a cooperative revoke without a false snapshot', async () => {
      rig = createHookRig({
        mode: 'cooperative',
        rebalanceTimeout: 40,
        listener: (log) => eachHook(log),
      })
      rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
      await rig.start()
      rig.runner.eachBatch = () => new Promise(() => {})
      rig.runner.handleBatch(
        new Batch('orders', 5, {
          partition: 1,
          highWatermark: 6,
          messages: [{ offset: '5', key: null, value: 'v' }],
        })
      )
      await settle()
      rig.log.length = 0
      rig.joins.push({ assignment: { orders: [0] }, generationId: 3 })
      const error = await rig.round('rebalance').catch((e) => e)
      expect(error?.reason).toBe('timeout')
      expect(rig.log).not.toContain('hook:onPartitionsRevoked')
      expect(rig.log).not.toContain('commit')
      expect(rig.group.assigned()).toEqual(partitions(0, 1))
      expect(rig.runner.hookWaiters.size).toBe(0)
      expect(rig.state.admissionHolds).toBeNull()
    })
  })

  describe('close', () => {
    it('calls revoked once for a healthy nonempty assignment, before leaving', async () => {
      rig = createHookRig({ listener: (log) => eachHook(log) })
      rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
      await rig.start()
      rig.log.length = 0
      await rig.stop()
      expect(rig.log).toEqual(['hook:onPartitionsRevoked', 'leave'])
    })

    it('calls no callback for an empty assignment', async () => {
      rig = createHookRig({ listener: (log) => eachHook(log) })
      await rig.start()
      rig.log.length = 0
      await rig.stop()
      expect(rig.log).toEqual(['leave'])
    })

    it('calls lost, not revoked, once the membership is lost', async () => {
      rig = createHookRig({ listener: (log) => eachHook(log) })
      rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
      await rig.start()
      rig.log.length = 0
      const { ownership } = rig.group
      ownership.lose(new Error('fenced'), ownership.token())
      await rig.stop()
      expect(rig.log).toEqual(['hook:onPartitionsLost', 'leave'])
    })

    it('logs a failing callback and still leaves', async () => {
      rig = createHookRig({
        listener: (log) =>
          eachHook(log, {
            onPartitionsRevoked: () => {
              throw new Error('store down')
            },
          }),
      })
      rig.joins.push({ assignment: { orders: [0] }, generationId: 2 })
      await rig.start()
      rig.log.length = 0
      await expect(rig.stop()).resolves.toBeUndefined()
      expect(rig.log).toEqual(['hook:onPartitionsRevoked', 'leave'])
    })

    describe('while a cooperative round is joining', () => {
      let seen
      const startCooperative = async () => {
        seen = []
        rig = createHookRig({
          mode: 'cooperative',
          listener: (log) =>
            eachHook(log, {
              onPartitionsRevoked: ({ partitions: held }) => seen.push(['revoked', held]),
              onPartitionsLost: ({ partitions: held }) => seen.push(['lost', held]),
            }),
        })
        rig.joins.push({ assignment: { orders: [0, 1] }, generationId: 2 })
        await rig.start()
        rig.log.length = 0
        seen.length = 0
      }
      const closeAround = async (assignment, afterStop) => {
        const joining = gate()
        rig.joins.push({ assignment, generationId: 3, gate: joining })
        const round = rig.round('enforced').catch(() => {})
        await settle()
        expect(rig.coordinator.isRebalancing()).toBe(true)
        const stopping = rig.stop()
        await settle()
        afterStop?.()
        joining.release()
        await Promise.all([round, stopping])
      }

      it('calls revoked, not lost, for a member whose membership is intact', async () => {
        await startCooperative()
        await closeAround({ orders: [0, 1] })
        expect(seen).toEqual([['revoked', partitions(0, 1)]])
        expect(rig.log).toContain('leave')
      })

      it('still calls lost when the membership is lost while the round runs', async () => {
        await startCooperative()
        await closeAround({ orders: [0, 1] }, () => {
          const { ownership } = rig.group
          ownership.lose(new Error('fenced'), ownership.token())
        })
        expect(seen.map(([kind]) => kind)).toEqual(['lost'])
      })
    })
  })
})

describe('consumer.run({ rebalanceListener })', () => {
  let rig
  afterEach(async () => {
    await rig?.consumer.disconnect().catch(() => {})
    rig?.restore()
    rig = null
  })

  it.each([
    ['null', null],
    ['a string', 'listener'],
    ['an array', []],
    ['a number', 5],
    ['a non-function revoked hook', { onPartitionsRevoked: 1 }],
    ['an explicitly undefined hook', { onPartitionsAssigned: undefined }],
    ['a non-function lost hook', { onPartitionsLost: 'lost' }],
  ])('rejects %s before any run state exists', async (_, rebalanceListener) => {
    rig = createPublicRig()
    await rig.consumer.subscribe({ topic: 't' })
    await expect(
      rig.consumer.run({ eachMessage: () => {}, rebalanceListener })
    ).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    expect(rig.spies.joinAndSync).not.toHaveBeenCalled()
    expect(rig.cluster.connect).not.toHaveBeenCalled()
    // the rejected call left the consumer usable
    rig.joins.push({ assignment: { t: [0] }, generationId: 2 })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: {} })
    expect(rig.spies.joinAndSync).toHaveBeenCalledTimes(1)
  })

  it('copies the callbacks once, so a later edit of the caller object changes nothing', async () => {
    rig = createPublicRig()
    const original = jest.fn()
    const listener = { onPartitionsAssigned: original }
    rig.joins.push({ assignment: { t: [0] }, generationId: 2 })
    await rig.consumer.subscribe({ topic: 't' })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: listener })
    listener.onPartitionsAssigned = jest.fn()
    const group = rig.spies.joinAndSync.mock.contexts[0]
    expect(group.rebalanceHooks?.listener.onPartitionsAssigned).toBe(original)
    expect(Object.isFrozen(group.rebalanceHooks?.listener)).toBe(true)
  })

  it('a crash restart reuses the run listener and a later run takes a new one', async () => {
    rig = createPublicRig()
    const firstListener = { onPartitionsAssigned: jest.fn() }
    rig.cluster.connect.mockRejectedValueOnce(Object.assign(new Error('down'), { retriable: true }))
    const groups = () => rig.spies.joinAndSync.mock.contexts
    rig.joins.push({ assignment: { t: [0] }, generationId: 2 })
    await rig.consumer.subscribe({ topic: 't' })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: firstListener })
    for (let i = 0; i < 100 && groups().length < 1; i++) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    expect(groups()).toHaveLength(1)
    expect(groups()[0].rebalanceHooks?.listener.onPartitionsAssigned).toBe(
      firstListener.onPartitionsAssigned
    )
    expect(rig.cluster.connect.mock.calls.length).toBeGreaterThanOrEqual(2)
    await rig.consumer.stop()
    const secondListener = { onPartitionsAssigned: jest.fn() }
    rig.joins.push({ assignment: { t: [0] }, generationId: 3 })
    await rig.consumer.run({ eachMessage: () => {}, rebalanceListener: secondListener })
    expect(groups().at(-1).rebalanceHooks?.listener.onPartitionsAssigned).toBe(
      secondListener.onPartitionsAssigned
    )
  })

  it('a cooperative disconnect right after enforceRebalance calls revoked, not lost', async () => {
    rig = createPublicRig({ cooperative: true, fetchLoop: true })
    const calls = []
    const joining = gate()
    rig.joins.push({ assignment: { t: [0, 1] }, generationId: 2 })
    await rig.consumer.subscribe({ topic: 't' })
    await rig.consumer.run({
      eachMessage: () => {},
      rebalanceListener: {
        onPartitionsRevoked: ({ partitions: held }) => calls.push(['revoked', held]),
        onPartitionsLost: ({ partitions: held }) => calls.push(['lost', held]),
      },
    })
    rig.joins.push({ assignment: { t: [0, 1] }, generationId: 3, gate: joining })
    await rig.consumer.enforceRebalance('listener-live')
    const closing = rig.consumer.disconnect()
    await settle()
    joining.release()
    await closing
    expect(calls).toEqual([['revoked', [{ topic: 't', partitions: [0, 1] }]]])
  })

  it.each(['eager', 'cooperative'])(
    'a failing callback in a requested %s round reaches the crash event as the exact typed error',
    async (mode) => {
      rig = createPublicRig({ cooperative: mode === 'cooperative', fetchLoop: true })
      const failure = new Error('external store down')
      let rounds = 0
      const crashes = []
      rig.consumer.on(rig.consumer.events.CRASH, ({ payload }) => crashes.push(payload))
      rig.joins.push({ assignment: { t: [0, 1] }, generationId: 2 })
      await rig.consumer.subscribe({ topic: 't' })
      await rig.consumer.run({
        eachMessage: () => {},
        rebalanceListener: {
          onPartitionsAssigned: () => {
            if (++rounds === 2) throw failure
          },
        },
      })
      rig.joins.push({ assignment: { t: [0, 1] }, generationId: 3 })
      await rig.consumer.enforceRebalance('store changed')
      for (let i = 0; i < 100 && crashes.length === 0; i++) {
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
      expect(crashes).toHaveLength(1)
      const [{ error, restart }] = crashes
      expect(error).toBeInstanceOf(KafkaJSRebalanceListenerError)
      expect(error.cause).toBe(failure)
      expect(error.thrownValue).toBe(failure)
      expect(restart).toBe(false)
      expect(rounds).toBe(2)
    }
  )
})
