const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const IdleWait = require('../idleWait')
const RebalanceHooks = require('../rebalanceHooks')
const handlerContext = require('../handlerContext')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger } = require('testHelpers')
const { createPublicRig, gate, settle } = require('./fixtures/hookRound')

const TOPIC = 't'

describe('consumer.enforceRebalance', () => {
  let rig
  afterEach(async () => {
    await rig?.consumer.disconnect().catch(() => {})
    rig?.restore()
    rig = null
  })

  const start = async (options = {}) => {
    rig = createPublicRig({ fetchLoop: true, ...options })
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    const reasons = []
    rig.consumer.on(rig.consumer.events.REBALANCING, ({ payload }) => reasons.push(payload.reason))
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({ eachMessage: () => {} })
    return reasons
  }
  const joins = () => rig.spies.joinAndSync.mock.calls.length
  const until = async (condition) => {
    for (let i = 0; i < 200 && !condition(); i++) await new Promise((r) => setTimeout(r, 5))
    expect(condition()).toBe(true)
  }

  describe.each(['eager', 'cooperative'])('%s classic group', (mode) => {
    it('is accepted before the round ends and adds exactly one JoinGroup with the reason', async () => {
      const reasons = await start({ cooperative: mode === 'cooperative' })
      expect(joins()).toBe(1)
      const syncGate = gate()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3, gate: syncGate })
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 4 })
      await rig.consumer.enforceRebalance('external offsets changed')
      await until(() => joins() === 2)
      expect(reasons).toEqual(['external offsets changed'])
      syncGate.release()
      await settle()
      await new Promise((resolve) => setTimeout(resolve, 30))
      expect(joins()).toBe(2)
    })

    it('uses the default reason and is a no-op while a round runs', async () => {
      const reasons = await start({ cooperative: mode === 'cooperative' })
      const syncGate = gate()
      rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3, gate: syncGate })
      await rig.consumer.enforceRebalance()
      await until(() => joins() === 2)
      await rig.consumer.enforceRebalance('second request')
      await rig.consumer.enforceRebalance('third request')
      syncGate.release()
      await new Promise((resolve) => setTimeout(resolve, 40))
      expect(reasons).toEqual(['enforced-rebalance'])
      expect(joins()).toBe(2)
    })
  })

  it.each([5, null, {}, ['x']])('rejects the reason %p without changing state', async (reason) => {
    await start()
    const before = joins()
    await expect(rig.consumer.enforceRebalance(reason)).rejects.toBeInstanceOf(
      KafkaJSNonRetriableError
    )
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(joins()).toBe(before)
  })

  it('rejects a consumer that is not running, naming the reason', async () => {
    rig = createPublicRig()
    await expect(rig.consumer.enforceRebalance('why')).rejects.toThrow(/why/)
    expect(joins()).toBe(0)
  })

  it('rejects a manual consumer, running or not, naming the reason', async () => {
    rig = createPublicRig({ fetchLoop: true })
    await rig.consumer.assign([{ topic: TOPIC, partitions: [0] }])
    await expect(rig.consumer.enforceRebalance('why')).rejects.toThrow(/manual assignment.*why/)
    await rig.consumer.run({ eachMessage: () => {} })
    await expect(rig.consumer.enforceRebalance('why')).rejects.toThrow(/manual assignment.*why/)
    expect(rig.spies.joinAndSync).not.toHaveBeenCalled()
  })

  it('rejects the consumer protocol, naming the reason, and sends no membership traffic', async () => {
    rig = createPublicRig({ groupProtocol: 'consumer' })
    await expect(rig.consumer.enforceRebalance('why')).rejects.toThrow(
      /groupProtocol 'consumer'.*why/
    )
    expect(joins()).toBe(0)
  })

  it('an expired callback cannot request a rebalance', async () => {
    rig = createPublicRig({ fetchLoop: true })
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    let late
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({
      eachMessage: () => {},
      rebalanceListener: {
        onPartitionsAssigned: () => {
          setTimeout(() => {
            rig.consumer.enforceRebalance('late').then(
              () => (late = 'accepted'),
              (error) => (late = error)
            )
          }, 5)
        },
      },
    })
    await new Promise((resolve) => setTimeout(resolve, 40))
    expect(late).toBeInstanceOf(Error)
    expect(joins()).toBe(1)
  })

  it('a rebalance requested from a handler runs its observers outside the handler store', async () => {
    rig = createPublicRig({ cooperative: true, fetchLoop: true })
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 2 })
    const stores = []
    rig.consumer.on(rig.consumer.events.GROUP_JOIN, () => {
      stores.push([RebalanceHooks.current(), handlerContext.current() ?? null])
    })
    await rig.consumer.subscribe({ topic: TOPIC })
    await rig.consumer.run({ eachMessage: () => {} })
    const identity = { topic: TOPIC, partition: 0, assignmentEpoch: null, acquireClock: 0 }
    rig.joins.push({ assignment: { [TOPIC]: [0, 1] }, generationId: 3 })
    await handlerContext.run(identity, () => rig.consumer.enforceRebalance('from handler'))
    await until(() => stores.length >= 2)
    expect(stores.every(([hook, handler]) => hook === undefined && handler === null)).toBe(true)
  })

  it('a custom assigner rebuilds its subscription, user data included, on every join', async () => {
    let version = 0
    const joinGroup = jest.fn(async () => ({
      memberId: 'member',
      generationId: 1,
      leaderId: 'member',
      members: [],
      groupProtocol: 'custom',
      protocolType: 'consumer',
    }))
    const assigner = {
      name: 'custom',
      protocol: () => ({ name: 'custom', metadata: Buffer.from(`user-data-${++version}`) }),
    }
    const protocol = new ClassicGroupProtocol({
      cluster: {
        findGroupCoordinator: async () => ({ isConnected: () => true, joinGroup, nodeId: 0 }),
        refreshMetadata: async () => {},
        findTopicPartitionMetadata: () => [],
      },
      groupId: 'group',
      groupInstanceId: null,
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      assigners: [assigner],
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    const group = Object.create(ConsumerGroup.prototype)
    Object.assign(group, {
      idleWait: new IdleWait(),
      rebalanceTimeout: 60000,
      cluster: { allowAutoTopicCreation: true },
      isStopping: () => false,
    })
    const round = group.beginRebalanceRound()
    await protocol.join({ topicsSubscribed: [TOPIC], round })
    await protocol.join({ topicsSubscribed: [TOPIC], round })
    round.retire()
    expect(
      joinGroup.mock.calls.map(([{ groupProtocols }]) => String(groupProtocols[0].metadata))
    ).toEqual(['user-data-1', 'user-data-2'])
  })
})
