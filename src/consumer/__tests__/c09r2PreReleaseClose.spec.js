const ConsumerGroup = require('../consumerGroup')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const GroupOwnership = require('../groupOwnership')
const OffsetManager = require('../offsetManager')
const Runner = require('../runner')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { ConsumerOperation } = require('../groupProtocol/interface')
const { KafkaJSDeliveryTimeout, KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')

jest.setTimeout(15000)

/** @returns {{promise: Promise<any>, resolve: Function, reject: Function}} a manually settled promise */
const gate = () => {
  const held = {}
  held.promise = new Promise((resolve, reject) => {
    held.resolve = resolve
    held.reject = reject
  })
  return held
}

const flush = () => new Promise((resolve) => setImmediate(resolve))

/**
 * The real leave chain of a consumer-protocol member: protocol leave, its pre-release commit, the
 * real ConsumerGroup#commitBeforeRelease, a real OffsetManager and its sender, over a counted
 * coordinator whose lookup and OffsetCommit answer can be held.
 * @param {object} [options]
 * @param {boolean} [options.cached=false] whether the group's coordinator is connected, so no
 *   lookup is made; otherwise the lookup is held until released
 * @returns {object} the collaborators and counters
 */
const rig = ({ cached = false } = {}) => {
  const lookup = gate()
  const commitAnswer = gate()
  const ownership = new GroupOwnership()
  const emitter = new InstrumentationEventEmitter()
  const committedEvents = jest.fn()
  emitter.addListener(COMMIT_OFFSETS, committedEvents)
  const live = {
    isConnected: () => true,
    offsetCommit: jest.fn(() => commitAnswer.promise),
  }
  const stale = { isConnected: () => cached, offsetCommit: live.offsetCommit }
  const cluster = {
    committedOffsets: () => cache,
    topicId: () => undefined,
    findGroupCoordinator: jest.fn(() => lookup.promise),
  }
  const cache = { t: { 0: '0' } }
  const manager = new OffsetManager({
    cluster,
    coordinator: cached ? live : stale,
    memberAssignment: { t: [0] },
    autoCommit: true,
    topicConfigurations: { t: {} },
    instrumentationEmitter: emitter,
    groupId: 'g',
    generationId: 8,
    memberId: 'm',
    ownership,
    membershipProtocol: 'consumer',
    logger: newLogger(),
  })
  manager.committedOffsets().t[0] = '0'
  manager.resolveOffset({ topic: 't', partition: 0, offset: '0' })

  const runner = new Runner({
    logger: newLogger(),
    consumerGroup: {
      groupId: 'g',
      attach: () => {},
      getNodeIds: () => [],
      heartbeatInterval: () => 3000,
      onStopRequested: () => {},
      retireFetchSessions: () => {},
    },
    instrumentationEmitter: emitter,
    concurrency: 1,
    eachBatch: async () => {},
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    onCrash: () => {},
  })
  runner.running = false
  runner.stopRequested = true

  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    autoCommit: true,
    offsetManager: manager,
    rebalanceTimeout: 60000,
    logger: newLogger(),
    rebalanceCoordinator: { runner },
  })
  Object.defineProperties(group, {
    generationId: { value: 8 },
    memberId: { value: 'm' },
    coordinator: { value: cached ? live : stale },
  })

  const heartbeat = jest.fn(async () => ({ memberEpoch: -1, heartbeatIntervalMs: 100 }))
  const protocol = new ConsumerGroupProtocol({
    cluster: {},
    groupId: 'g',
    rebalanceTimeout: 60000,
    ownership,
    instrumentationEmitter: emitter,
    logger: newLogger(),
  })
  protocol.state = 'STABLE'
  protocol.memberEpoch = 8
  protocol.coordinator = { consumerGroupHeartbeat: heartbeat }
  protocol.ownedPartitions = [{ topic: 't', partitions: [0] }]
  const emitPartitions = jest.fn()
  protocol.attach({
    heartbeatManager: { requestTick: jest.fn() },
    rebalanceCoordinator: { emitPartitions, requestRejoin: jest.fn() },
    commitBeforeRelease: (partitions, options) => group.commitBeforeRelease(partitions, options),
  })

  return {
    cache,
    committedEvents,
    commitAnswer,
    emitPartitions,
    group,
    heartbeat,
    live,
    lookup,
    manager,
    ownership,
    protocol,
    runner,
    cluster,
  }
}

/** Starts the leave under the operation, the way `Runner#stop` does. */
const leaveUnder = (h, operation) =>
  operation.wait(() => h.protocol.leave({ operation })).catch((error) => error)

const timeout = () => new KafkaJSDeliveryTimeout('close deadline')

describe('a consumer-protocol final commit under a close that expires', () => {
  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('probe: a retired runner and operation leave no late cache write behind', async () => {
    const h = rig({ cached: true })
    const op = new ConsumerOperation()
    const committing = op.wait(() => h.group.commitBeforeRelease([{ topic: 't', partitions: [0] }]))
    committing.catch(() => {})
    await flush()
    expect(h.live.offsetCommit).toHaveBeenCalledTimes(1)
    h.runner.retire(timeout())
    op.retire(timeout())
    await flush()
    h.commitAnswer.resolve({})
    await flush()
    expect(h.cache.t[0]).toBe('0')
    expect(h.committedEvents).not.toHaveBeenCalled()
  })

  it('a held coordinator lookup sends no late frame', async () => {
    const h = rig()
    const op = new ConsumerOperation()
    const leaving = leaveUnder(h, op)
    await flush()
    expect(h.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    const reason = timeout()
    op.retire(reason)
    expect(await leaving).toBe(reason)
    h.lookup.resolve(h.live)
    await flush()
    expect(h.live.offsetCommit).not.toHaveBeenCalled()
    expect(h.heartbeat).not.toHaveBeenCalled()
    expect(h.emitPartitions).not.toHaveBeenCalled()
    expect(h.protocol.memberEpoch).toBe(8)
    expect(h.protocol.state).toBe('PREPARE_LEAVING')
    expect(h.manager.coordinator).not.toBe(h.live)
  })

  it('a written response publishes no late cache, debt, event, revocation or leave', async () => {
    const h = rig({ cached: true })
    const op = new ConsumerOperation()
    const leaving = leaveUnder(h, op)
    await flush()
    expect(h.live.offsetCommit).toHaveBeenCalledTimes(1)
    const reason = timeout()
    op.retire(reason)
    expect(await leaving).toBe(reason)
    h.commitAnswer.resolve({})
    await flush()
    await flush()
    expect(h.cache.t[0]).toBe('0')
    expect(h.committedEvents).not.toHaveBeenCalled()
    expect(h.manager.operations.size()).toBe(0)
    expect(h.emitPartitions).not.toHaveBeenCalled()
    expect(h.heartbeat).not.toHaveBeenCalled()
    expect(h.protocol.memberEpoch).toBe(8)
    expect(h.protocol.state).toBe('PREPARE_LEAVING')
  })

  it('a late loss answer of the old token cannot poison its replacement', async () => {
    const h = rig({ cached: true })
    const op = new ConsumerOperation()
    const leaving = leaveUnder(h, op)
    await flush()
    const reason = timeout()
    op.retire(reason)
    await leaving
    h.ownership.renew()
    h.commitAnswer.reject(new KafkaJSProtocolError(createErrorFromCode(25)))
    await flush()
    await flush()
    expect(h.ownership.refusal(h.ownership.token())).toBeNull()
    expect(h.emitPartitions).not.toHaveBeenCalled()
    expect(h.heartbeat).not.toHaveBeenCalled()
  })

  it('a replacement membership commits normally after the old close expired', async () => {
    const h = rig({ cached: true })
    const op = new ConsumerOperation()
    const leaving = leaveUnder(h, op)
    await flush()
    op.retire(timeout())
    await leaving
    h.ownership.renew()
    const replacement = new ConsumerOperation()
    h.live.offsetCommit.mockResolvedValueOnce({})
    await replacement.wait(() =>
      h.manager.commitOffsets({}, { guard: replacement.guard, fence: replacement })
    )
    expect(h.cache.t[0]).toBe('1')
    h.commitAnswer.resolve({})
    await flush()
    expect(h.cache.t[0]).toBe('1')
    replacement.finish()
  })

  it('the control makes one commit attempt, reports the revoke and then leaves', async () => {
    const h = rig({ cached: true })
    h.commitAnswer.resolve({})
    const op = new ConsumerOperation({ deadlineAt: performance.now() + 5000 })
    await op.wait(() => h.protocol.leave({ operation: op }))
    expect(h.live.offsetCommit).toHaveBeenCalledTimes(1)
    expect(h.cache.t[0]).toBe('1')
    expect(h.committedEvents).toHaveBeenCalledTimes(1)
    expect(h.emitPartitions).toHaveBeenCalledTimes(1)
    expect(h.heartbeat).toHaveBeenCalledTimes(1)
    expect(h.heartbeat.mock.calls[0][0].memberEpoch).toBe(-1)
    expect(h.protocol.state).toBe('UNSUBSCRIBED')
    op.finish()
  })

  it('a leave without any operation keeps its single commit attempt', async () => {
    const h = rig({ cached: true })
    h.commitAnswer.resolve({})
    await h.protocol.leave()
    expect(h.live.offsetCommit).toHaveBeenCalledTimes(1)
    expect(h.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('a commit failure inside the deadline is contained and the member still leaves', async () => {
    const h = rig({ cached: true })
    h.commitAnswer.reject(new KafkaJSProtocolError(createErrorFromCode(14)))
    const op = new ConsumerOperation({ deadlineAt: performance.now() + 5000 })
    await op.wait(() => h.protocol.leave({ operation: op }))
    expect(h.heartbeat).toHaveBeenCalledTimes(1)
    expect(h.protocol.state).toBe('UNSUBSCRIBED')
    op.finish()
  })
})
