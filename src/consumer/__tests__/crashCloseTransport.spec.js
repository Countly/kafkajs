const { KafkaJSNonRetriableError } = require('../../errors')
const { startTwoNode } = require('./fixtures/twoNodeRig')
const { gate, pause, until } = require('./fixtures/closeRig')

jest.setTimeout(20000)

/** A topic only the test asks about, so its held request is the one held. */
const HELD_TOPIC = 'held-topic'

/** Records how `p` settles, without awaiting it. */
const settled = (p) => {
  const s = { done: false }
  p.then(
    (value) => Object.assign(s, { done: true, value }),
    (error) => Object.assign(s, { done: true, error })
  )
  return s
}

describe('Consumer > a close joining crash cleanup on a real broker pool', () => {
  let rig = null
  afterEach(async () => {
    await rig?.consumer.disconnect({ timeout: 0 }).catch(() => {})
    rig = null
  })

  /**
   * Holds a Metadata request of the test's own on the coordinator's connection (node 1, not the
   * seed), then lets the handler fail without retry, so crash cleanup starts a cluster disconnect
   * that drains it.
   */
  const crashWhileDraining = async ({ policy = async () => false } = {}) => {
    const fail = gate()
    rig = await startTwoNode({
      eachMessage: async () => {
        await fail.promise
        throw new KafkaJSNonRetriableError('handler failed')
      },
      retry: { restartOnFailure: policy },
    })
    const { scripted, cluster } = rig
    const coordinator = await cluster.findBroker({ nodeId: '1' })
    const held = scripted.schedule.hold({
      api: 'Metadata',
      node: 1,
      match: (call) => JSON.stringify(call.fields).includes(HELD_TOPIC),
    })
    const request = settled(coordinator.metadata([HELD_TOPIC]))
    await held.arrived()
    const disconnect = jest.spyOn(cluster, 'disconnect')
    fail.resolve()
    await until(() => disconnect.mock.calls.length === 1, 'crash cleanup disconnecting the cluster')
    await pause(20)
    return { ...rig, coordinator, held, request, disconnect }
  }

  it('a zero-timeout disconnect made while crash cleanup drains a detached non-seed broker resolves only after that broker socket closed and its request settled, with the broker reply still held', async () => {
    const { consumer, coordinator, held, request, disconnect, events } = await crashWhileDraining()

    expect(request.done).toBe(false)
    expect(coordinator.isConnected()).toBe(true)

    await consumer.disconnect({ timeout: 0 })

    expect(request.done).toBe(true)
    expect(request.error).toMatchObject({ name: 'KafkaJSConnectionClosedError' })
    expect(coordinator.isConnected()).toBe(false)
    expect(held.call).not.toBeNull()
    expect(disconnect.mock.calls).toEqual([[{ preserveSetupState: true }], [{ drainTimeout: 0 }]])
    expect(events.filter((name) => name === 'DISCONNECT')).toHaveLength(2)

    await until(() => events.includes('CRASH'), 'the crash report')
    expect(events.filter((name) => name === 'CRASH')).toHaveLength(1)
  })

  it('a zero-timeout disconnect joining crash cleanup settles while restartOnFailure is still deciding; CRASH follows once it decides', async () => {
    const decision = gate()
    const policy = jest.fn(() => decision.promise)
    const { consumer, request, events } = await crashWhileDraining({ policy })

    await consumer.disconnect({ timeout: 0 })

    expect(request.done).toBe(true)
    await until(() => policy.mock.calls.length === 1, 'the crash policy call')
    expect(events).not.toContain('CRASH')

    decision.resolve(false)
    await until(() => events.includes('CRASH'), 'the crash report')
    expect(events.filter((name) => name === 'CRASH')).toHaveLength(1)
  })
})
