const SubscriptionState = require('../subscriptionState')
const Runner = require('../runner')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}
const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve()
}
describe('rebalance admission holds', () => {
  it('excludes held work without exposing a pause or losing assignment', () => {
    const state = new SubscriptionState()
    const token = {}
    state.assign([{ topic: '__proto__', partitions: [0, 1] }])
    state.hold([{ topic: '__proto__', partitions: [0] }], token)
    expect(state.active()).toEqual([{ topic: '__proto__', partitions: [1] }])
    expect(state.paused()).toEqual([])
    expect(state.isAssigned('__proto__', 0)).toBe(true)
    expect(state.release(token)).toBe(true)
    expect(state.active()).toEqual([{ topic: '__proto__', partitions: [0, 1] }])
  })
  it('old release preserves overlapping holds and user pause', () => {
    const state = new SubscriptionState()
    const old = {}
    const next = {}
    state.assign([{ topic: 'orders', partitions: [0, 1] }])
    state.pause([{ topic: 'orders', partitions: [1] }])
    state.hold([{ topic: 'orders', partitions: [0] }], old)
    state.hold([{ topic: 'orders', partitions: [0] }], next)
    state.release(old)
    expect(state.release(old)).toBe(false)
    expect(state.active()[0].partitions).toEqual([])
    state.release(next)
    expect(state.active()[0].partitions).toEqual([0])
    expect(state.paused()[0].partitions).toEqual([1])
    expect(state.admissionHolds).toBeNull()
  })
  it('holds additions before publication and copies caller keys', () => {
    const state = new SubscriptionState()
    const token = {}
    const input = [{ topic: 'orders', partitions: [2] }]
    state.hold(input, token)
    input[0].partitions.push(3)
    state.assign([{ topic: 'orders', partitions: [2, 3] }])
    expect(state.active()[0].partitions).toEqual([3])
    state.release(token)
    expect(state.active()[0].partitions).toEqual([2, 3])
  })
  it('does not allocate holds for ordinary admission', () => {
    const state = new SubscriptionState()
    expect(state.isHeld('orders', 0)).toBe(false)
    expect(state.admissionHolds).toBeNull()
  })
})

describe('admission holds against running handlers', () => {
  const open = []
  afterEach(async () => {
    for (const { runner, held } of open.splice(0)) {
      held.release()
      await runner.stop()
    }
  })
  const round = () => ({ guard: () => null, wait: (work) => work(), retire: jest.fn() })
  const batchOf = (topic, partition, offset) =>
    new Batch(topic, offset, {
      partition,
      highWatermark: offset + 1,
      messages: [{ offset: String(offset), key: null, value: 'v' }],
    })

  /** A real Runner over a group double whose assignment state is a real SubscriptionState. */
  const rig = async (topic, handlerKind) => {
    const state = new SubscriptionState()
    state.assign([{ topic, partitions: [0, 1] }])
    const resolved = []
    const commits = []
    const entered = []
    const held = gate()
    const consumerGroup = {
      groupId: 'group',
      memberId: 'member',
      generationId: 1,
      groupMembership: {},
      assignmentEpoch: 0,
      attach: jest.fn(),
      kind: 'classic',
      subscriptionState: state,
      rebalanceHooks: { has: (name) => name === 'onPartitionsRevoked' },
      isAssigned: (t, p) => state.isAssigned(t, p),
      isPaused: (t, p) => state.isPaused(t, p),
      hasSeekOffset: () => false,
      resolveOffset: jest.fn(({ partition, offset }) => resolved.push([partition, offset])),
      commitOffsets: jest.fn(async (offsets) => commits.push(offsets)),
      commitOffsetsIfNecessary: jest.fn(),
      heartbeat: jest.fn(),
      heartbeatInterval: () => 3000,
      startIdleWait: jest.fn(),
      onStopRequested: jest.fn(),
      clearAssignerState: jest.fn(),
      leave: jest.fn(async () => {}),
      rebalanceProtocol: () => 'eager',
      leaderId: 'member',
      groupProtocol: 'RoundRobinAssigner',
      isLeader: () => true,
      connect: jest.fn(),
      joinAndSync: jest.fn(async () => ({ assignment: {} })),
      applyRebalanceResult: jest.fn(async () => ({ assignment: [], added: [], revoked: [] })),
      beginRebalanceRound: jest.fn(round),
      assigned: () => [],
      forgetOwnedPartitions: jest.fn(),
      clearStaleAssignmentPending: jest.fn(),
      needsReconciliation: () => false,
      invokeHook: jest.fn(async () => {}),
      hookIdentity: jest.fn(() => ({})),
      takeListenerFailure: jest.fn(),
      releaseHold: jest.fn(),
    }
    const handler = async (batch, helpers) => {
      entered.push([batch.partition, batch.firstOffset()])
      if (batch.partition !== 0 || batch.firstOffset() !== '4') return
      await held.promise
      if (helpers?.resolveOffset) {
        helpers.resolveOffset('4')
        await helpers.commitOffsetsIfNecessary({
          topics: [{ topic, partitions: [{ partition: 0, offset: '5' }] }],
        })
      }
    }
    const runner = new Runner({
      consumerGroup,
      onCrash: jest.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
      concurrency: 2,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      ...(handlerKind === 'eachBatch'
        ? { eachBatch: ({ batch, ...helpers }) => handler(batch, helpers) }
        : {
            eachMessage: ({ partition, message }) =>
              handler({ partition, firstOffset: () => message.offset }),
          }),
    })
    runner.scheduleFetchManager = jest.fn()
    await runner.start()
    open.push({ runner, held })
    return { runner, state, resolved, commits, entered, held, consumerGroup }
  }

  it.each(['orders', '__proto__'])(
    'a held key admits no queued batch but its running handler resolves and commits (%s)',
    async (topic) => {
      const { runner, state, resolved, commits, entered, held } = await rig(topic, 'eachBatch')
      const running = runner.handleBatch(batchOf(topic, 0, 4))
      await settle()
      expect(entered).toEqual([[0, '4']])

      const token = {}
      state.hold([{ topic, partitions: [0] }], token)
      await runner.handleBatch(batchOf(topic, 0, 5))
      await runner.handleBatch(batchOf(topic, 1, 7))
      expect(entered).toEqual([
        [0, '4'],
        [1, '7'],
      ])

      let quiescent = false
      const waiting = runner
        .waitForPartitions([{ topic, partitions: [0] }], {
          deadlineAt: performance.now() + 5000,
          identity: { round: round(), membership: 0 },
        })
        .then(() => {
          quiescent = true
        })
      await settle()
      expect(quiescent).toBe(false)

      held.release()
      await running
      await waiting
      expect(quiescent).toBe(true)
      expect(resolved).toContainEqual([0, '4'])
      expect(commits).toContainEqual({
        topics: [{ topic, partitions: [{ partition: 0, offset: '5' }] }],
      })
      expect(runner.hookHandlers.size).toBe(0)
      await runner.stop()
    }
  )

  it('applies the same admission rule to eachMessage and keeps a retained handler independent', async () => {
    const { runner, state, entered, held } = await rig('orders', 'eachMessage')
    const running = runner.handleBatch(batchOf('orders', 0, 4))
    await settle()
    state.hold([{ topic: 'orders', partitions: [0] }], {})
    await runner.handleBatch(batchOf('orders', 0, 5))
    expect(entered).toEqual([[0, '4']])

    let waited = false
    const keep = runner.waitForPartitions([{ topic: 'orders', partitions: [1] }], {
      deadlineAt: performance.now() + 5000,
      identity: { round: round(), membership: 0 },
    })
    await keep
    waited = true
    expect(waited).toBe(true)
    held.release()
    await running
    await runner.stop()
  })

  it('does not count batches for a listener without a revoked callback', async () => {
    const { runner, consumerGroup } = await rig('orders', 'eachBatch')
    consumerGroup.rebalanceHooks = { has: () => false }
    const quiet = new Runner({
      consumerGroup,
      onCrash: jest.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
      eachBatch: jest.fn(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
    })
    quiet.scheduleFetchManager = jest.fn()
    await quiet.start()
    await quiet.handleBatch(batchOf('orders', 1, 7))
    expect(quiet.hookHandlers).toBeUndefined()
    await quiet.stop()
    await runner.stop()
  })
})
