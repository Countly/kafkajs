const Runner = require('../runner')
const RebalanceHooks = require('../rebalanceHooks')
const { createPatternGroup } = require('./fixtures/patternGroup')
const { newLogger, waitFor } = require('testHelpers')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'
const ID_C = '21222324-2526-2728-292a-2b2c2d2e2f30'

/** @returns {{promise: Promise<void>, release: () => void}} a promise a test settles by hand */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/**
 * A pattern group with a real runner and coordinator, so a round runs its awaited callbacks.
 * @param {object} listener the public rebalance listener
 * @returns {object} the pattern harness plus `coordinator`
 */
const createRig = (listener) => {
  const harness = createPatternGroup({
    catalog: { 'orders-1': ID_A },
    group: { rebalanceListener: RebalanceHooks.validate(listener, 'classic') },
  })
  const { group } = harness
  const runner = new Runner({
    consumerGroup: group,
    onCrash: jest.fn(),
    instrumentationEmitter: group.instrumentationEmitter,
    logger: newLogger(),
    eachBatch: jest.fn(),
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    autoCommit: false,
  })
  runner.scheduleFetchManager = jest.fn()
  runner.heartbeatManager.ensureStarted = jest.fn()
  return { ...harness, coordinator: runner.rebalanceCoordinator }
}

describe('rebalance callbacks and the subscription revision', () => {
  test('a round keeps the revision it pinned while a callback runs, and the next round takes the newer one', async () => {
    const held = gate()
    const seen = []
    let calls = 0
    const rig = createRig({
      onPartitionsRevoked: async () => {
        seen.push({ call: ++calls, applied: rig.applied() })
        if (calls === 1) await held.promise
      },
    })
    await rig.refresh()
    await rig.round()
    expect(rig.applied()).toEqual(['orders-1'])

    rig.set('orders-2', ID_B)
    await rig.refresh()
    const joinsBefore = rig.rebalances.length
    const running = rig.coordinator.runRound('subscription-changed', 'revoked')
    await waitFor(() => calls === 1)

    rig.drop('orders-2')
    await rig.refresh()
    rig.set('orders-3', ID_C)
    await rig.refresh()
    expect(rig.subscription.desired.names).toEqual(['orders-1', 'orders-3'])
    held.release()
    await running

    expect(seen[0]).toEqual({ call: 1, applied: ['orders-1'] })
    expect(rig.rebalances.slice(joinsBefore)).toEqual([
      ['orders-1', 'orders-2'],
      ['orders-1', 'orders-3'],
    ])
    expect(seen[1]).toEqual({ call: 2, applied: ['orders-1', 'orders-2'] })
    expect(rig.applied()).toEqual(['orders-1', 'orders-3'])
    expect(rig.subscription.needsApply()).toBe(false)
  })
})
