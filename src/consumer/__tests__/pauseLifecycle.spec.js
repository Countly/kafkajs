const SubscriptionState = require('../subscriptionState')
const { createFetchGroup, TOPIC } = require('./fixtures/fetchHarness')

describe('pause subscription lifetime', () => {
  it('keeps pauseAll and resumed exceptions across revoke and reassign', () => {
    const state = new SubscriptionState()
    state.assign([{ topic: 'orders', partitions: [0, 1] }])
    state.pause([{ topic: 'orders' }])
    state.resume([{ topic: 'orders', partitions: [1] }])
    state.update([])
    expect(state.paused()).toEqual([])
    state.update([{ topic: 'orders', partitions: [0, 1] }])
    expect(state.paused()).toEqual([{ topic: 'orders', partitions: [0] }])
    expect(state.active()).toEqual([{ topic: 'orders', partitions: [1] }])
  })
  it('retires one incarnation and preserves other topics', () => {
    const state = new SubscriptionState()
    state.assign([
      { topic: 'orders', partitions: [0] },
      { topic: 'audit', partitions: [0] },
    ])
    state.pause([{ topic: 'orders' }, { topic: 'audit' }])
    state.retireTopic('orders')
    expect(state.paused()).toEqual([{ topic: 'audit', partitions: [0] }])
  })
  it('retires pre-pause at the next subscription lifecycle and accepts prototype keys', () => {
    const state = new SubscriptionState()
    state.pause([{ topic: 'never-subscribed' }, { topic: '__proto__' }])
    expect(Object.keys(state.subscriptionStatesByTopic)).toHaveLength(2)
    state.retainTopics(['__proto__'])
    state.assign([{ topic: '__proto__', partitions: [0] }])
    expect(Object.keys(state.subscriptionStatesByTopic)).toEqual(['__proto__'])
    expect(state.paused()).toEqual([{ topic: '__proto__', partitions: [0] }])
    state.clearPauses()
    expect(state.paused()).toEqual([])
    expect(Object.keys(state.subscriptionStatesByTopic)).toEqual([])
  })
})

/** @returns {Promise<void>} lets continuations of resolved promises run */
const entered = async () => {
  for (let n = 0; n < 12; n++) await Promise.resolve()
}

/** @returns {number[]} the partitions the group would put in its next Fetch */
const fetched = (group) =>
  group
    .buildFetchRequests(group.subscriptionState.assigned())
    .flatMap(({ partitions }) => partitions.map(({ partition }) => partition))

describe('pause across a rebalance on the real apply paths', () => {
  let h
  afterEach(async () => {
    if (h) await h.broker.disconnect()
  })

  /** Applies an assignment of `partitions` of TOPIC and primes their committed offsets. */
  const apply = async (partitions, options) => {
    const assignment = partitions.length ? { [TOPIC]: partitions } : {}
    await h.group.applyRebalanceResult({ assignment }, options)
    Object.assign(h.group.offsetManager.committedOffsets(), {
      [TOPIC]: { 0: '10', 1: '20' },
    })
  }

  describe('pause survives an ordinary rebalance in each membership protocol', () => {
    it.each([
      ['classic eager', 'classic', { rebuildOffsets: true }],
      ['classic cooperative', 'classic', { rebuildOffsets: false }],
      ['consumer protocol', 'consumer', { rebuildOffsets: false }],
    ])('%s', async (_, groupProtocol, options) => {
      h = await createFetchGroup({ partitions: [0, 1], groupProtocol })
      h.group.partitionsPerSubscribedTopic = h.group.generatePartitionsPerSubscribedTopic()
      await apply([0, 1], options)
      h.group.pause([{ topic: TOPIC, partitions: [0] }])
      expect(fetched(h.group)).toEqual([1])

      await apply([], options)
      expect(h.group.paused()).toEqual([])
      await apply([0, 1], options)
      expect(h.group.paused()).toEqual([{ topic: TOPIC, partitions: [0] }])
      expect(fetched(h.group)).toEqual([1])

      h.group.resume([{ topic: TOPIC, partitions: [0] }])
      h.group.pause([{ topic: TOPIC }])
      h.group.resume([{ topic: TOPIC, partitions: [1] }])
      await apply([], options)
      await apply([0, 1], options)
      expect(h.group.paused()).toEqual([{ topic: TOPIC, partitions: [0] }])
      expect(fetched(h.group)).toEqual([1])
    })
  })

  it('an unassigned incarnation change retires pause', async () => {
    h = await createFetchGroup({ partitions: [0] })
    h.group.offsetManager = null
    h.group.subscriptionState.assign([])
    h.group.pause([
      { topic: TOPIC, partitions: [0] },
      { topic: 'audit', partitions: [0] },
    ])
    h.group.invalidateTopic(TOPIC)
    h.group.subscriptionState.assign([
      { topic: TOPIC, partitions: [0] },
      { topic: 'audit', partitions: [0] },
    ])
    expect(h.group.subscriptionState.isPaused(TOPIC, 0)).toBe(false)
    expect(h.group.subscriptionState.isPaused('audit', 0)).toBe(true)
  })

  describe('a run boundary', () => {
    beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] }))
    afterEach(() => {
      jest.restoreAllMocks()
      jest.clearAllTimers()
      jest.useRealTimers()
    })

    it('stop and a new run retire intent without reviving old waiters', async () => {
      h = await createFetchGroup({ partitions: [0] })
      const callbacks = []
      const original = global.setTimeout
      jest.spyOn(global, 'setTimeout').mockImplementation((callback, ms) => {
        callbacks.push(callback)
        return original(callback, ms)
      })
      const { group } = h
      group.pause([{ topic: TOPIC, partitions: [0] }, { topic: 'ahead' }])
      const old = group.idleWait
      const oldSettled = jest.fn()
      const oldWait = old.wait(5000).then(oldSettled)
      const oldCallbacks = [...callbacks]

      group.onStopRequested()
      await oldWait
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toEqual([])
      group.pause([{ topic: 'ahead' }])
      group.startIdleWait()
      expect(group.idleWait).not.toBe(old)
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toEqual([])

      const freshSettled = jest.fn()
      const freshWait = group.idleWait.wait(5000).then(freshSettled)
      oldCallbacks.forEach((callback) => callback())
      await entered()
      expect(oldSettled).toHaveBeenCalledTimes(1)
      expect(freshSettled).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(1)
      expect(group.subscriptionState.assigned()).toEqual([{ topic: TOPIC, partitions: [0] }])
      group.idleWait.dispose()
      await freshWait
      expect(jest.getTimerCount()).toBe(0)
    })
  })
})
