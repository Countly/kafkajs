const ConsumerGroup = require('../../consumerGroup')
const Runner = require('../../runner')
const RebalanceHooks = require('../../rebalanceHooks')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { events } = require('../../instrumentationEvents')
const { COOPERATIVE, EAGER } = require('../../groupProtocol/interface')
const { newLogger } = require('testHelpers')

/** @returns {{ promise: Promise<void>, release: () => void }} a promise a test settles by hand */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/** Lets pending promise continuations run. @returns {Promise<void>} */
const settle = async () => {
  await new Promise((resolve) => setImmediate(resolve))
}

/**
 * A real `Runner`, `RebalanceCoordinator` and `ConsumerGroup` over a stubbed cluster: the only
 * scripted parts are the JoinGroup/SyncGroup exchange, the fetch loop and the commit transport, so
 * every assertion is about the shipped round, apply and callback wiring.
 * @param {object} [options]
 * @param {'eager'|'cooperative'} [options.mode='eager'] the rebalance protocol
 * @param {object|((log: string[]) => object)} [options.listener] public listener, or a factory given
 *   the rig's log, validated like `run()` does; none for a plain run
 * @param {number} [options.rebalanceTimeout=60000] the round budget in milliseconds
 * @param {boolean} [options.autoCommit=true] whether the runner commits automatically
 * @returns {object} the rig: `group`, `runner`, `coordinator`, `state`, `log`, `emitted`, `onCrash`,
 *   `joins`, `start()`, `round(reason, kind)` and `stop()`
 */
const createHookRig = ({
  mode = 'eager',
  listener,
  rebalanceTimeout = 60000,
  autoCommit = true,
} = {}) => {
  const log = []
  const emitted = []
  const joins = []
  const topic = 'orders'
  const cluster = {
    connect: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    topicId: () => 'topic-id',
    findTopicPartitionMetadata: () =>
      Array.from({ length: 4 }, (_, partitionId) => ({ partitionId })),
    findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true })),
    getNodeIds: () => [0],
  }
  const instrumentationEmitter = new InstrumentationEventEmitter()
  for (const name of [
    'PARTITIONS_REVOKED',
    'PARTITIONS_ASSIGNED',
    'PARTITIONS_LOST',
    'GROUP_JOIN',
  ]) {
    instrumentationEmitter.addListener(events[name], (event) => {
      log.push(`event:${name}`)
      emitted.push({ name, payload: event.payload })
    })
  }

  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'group',
    topics: [topic],
    topicConfigurations: { [topic]: {} },
    logger: newLogger(),
    instrumentationEmitter,
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    rebalanceProtocol: mode === 'cooperative' ? COOPERATIVE : EAGER,
    metadataMaxAge: 300000,
    rebalanceListener:
      listener === undefined
        ? null
        : RebalanceHooks.validate(
            typeof listener === 'function' ? listener(log) : listener,
            'classic'
          ),
  })
  group.memberId = 'member-1'
  group.generationId = 1
  group.coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    offsetCommit: jest.fn(async () => {}),
  }
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
  group.joinAndSync = jest.fn(async () => {
    log.push('join')
    const step = joins.shift() || { assignment: {} }
    if (step.gate) await step.gate.promise
    if (step.generationId) group.generationId = step.generationId
    return { assignment: step.assignment }
  })
  group.commitOffsets = jest.fn(async () => {
    log.push('commit')
  })
  group.commitBeforeRelease = jest.fn(async () => {
    log.push('commit')
  })
  group.leave = jest.fn(async () => {
    log.push('leave')
  })

  const onCrash = jest.fn()
  const runner = new Runner({
    consumerGroup: group,
    onCrash,
    instrumentationEmitter,
    logger: newLogger(),
    eachBatch: jest.fn(),
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    autoCommit,
  })
  runner.scheduleFetchManager = jest.fn()
  runner.heartbeatManager.ensureStarted = jest.fn()
  const { rebalanceCoordinator: coordinator } = runner

  return {
    topic,
    group,
    runner,
    coordinator,
    state: group.subscriptionState,
    log,
    emitted,
    joins,
    onCrash,
    /** Starts the run and its initial round. @returns {Promise<void>} */
    start: () => runner.start(),
    /** Runs one more round through the coordinator. @returns {Promise<void>} */
    round: (reason, kind = 'revoked') => coordinator.runRound(reason, kind),
    /** Stops the runner and its timers. @returns {Promise<void>} */
    stop: async () => {
      runner.running = true
      await runner.stop()
    },
  }
}

/**
 * A listener whose callbacks append `hook:<name>` to the log, then run the optional behavior.
 * @param {string[]} log the rig's shared log
 * @param {Record<string, (payload: object) => unknown>} [behaviors] per-callback bodies
 * @returns {object} a public listener
 */
const recordingListener = (log, behaviors = {}) =>
  Object.fromEntries(
    ['onPartitionsRevoked', 'onPartitionsAssigned', 'onPartitionsLost'].map((name) => [
      name,
      async (payload) => {
        log.push(`hook:${name}`)
        return behaviors[name]?.(payload)
      },
    ])
  )

/**
 * A public consumer over a stubbed cluster whose rounds are scripted: `JoinGroup` answers from a
 * queue, no heartbeat or fetch loop runs, and the group calls a test cares about are spies. The
 * consumer is the real factory, so every public refusal and lifecycle rule is the shipped one.
 * @param {object} [options]
 * @param {boolean} [options.cooperative=false] selects the cooperative rebalance protocol
 * @param {boolean} [options.fetchLoop=false] runs the real fetch loop over empty fetch rounds
 * @param {string} [options.groupProtocol] the group protocol option, classic by default
 * @returns {object} `consumer`, `joins`, `spies` (by group method name) and `restore()`
 */
const createPublicRig = ({ cooperative = false, fetchLoop = false, groupProtocol } = {}) => {
  const createConsumer = require('../../index')
  const OffsetOperations = require('../../offsetManager/offsetOperations')
  const HeartbeatManager = require('../../heartbeatManager')
  const { roundRobin, cooperativeSticky } = require('../../assigners')
  const joins = []
  const cluster = {
    onTopicRecreated: () => () => {},
    offsetOperations: () => new OffsetOperations(),
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    stageMetadata: jest.fn(async ({ topics }) => ({
      topicMetadata: topics.map((topic) => ({
        topic,
        partitionMetadata: Array.from({ length: 4 }, (_, partitionId) => ({ partitionId })),
      })),
    })),
    resetTargetTopics: jest.fn(async (_names, { publish } = {}) => {
      publish?.()
      return true
    }),
    getNodeIds: () => [0],
    topicId: () => 'topic-id',
    committedOffsets: () => ({}),
    allowAutoTopicCreation: false,
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    findTopicPartitionMetadata: () =>
      Array.from({ length: 4 }, (_, partitionId) => ({ partitionId })),
  }
  const spies = {}
  const spy = (target, name, implementation) => {
    spies[name] = jest.spyOn(target, name)
    if (implementation) spies[name].mockImplementation(implementation)
    return spies[name]
  }
  spy(ConsumerGroup.prototype, 'seek')
  spy(ConsumerGroup.prototype, 'pause')
  spy(ConsumerGroup.prototype, 'resume')
  spy(ConsumerGroup.prototype, 'leave', async () => {})
  spy(ConsumerGroup.prototype, 'commitOffsets', async () => {})
  spy(ConsumerGroup.prototype, 'joinAndSync', async function () {
    const step = joins.shift() || { assignment: {} }
    if (step.gate) await step.gate.promise
    this.memberId = 'member-1'
    this.generationId = step.generationId || 1
    this.coordinator = {
      isConnected: () => true,
      nodeId: 0,
      offsetFetch: async () => ({ responses: [] }),
      offsetCommit: async () => {},
    }
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
    return { assignment: step.assignment }
  })
  spy(HeartbeatManager.prototype, 'ensureStarted', () => {})
  if (fetchLoop) {
    spy(ConsumerGroup.prototype, 'fetch', async () => {
      await new Promise((resolve) => setTimeout(resolve, 5))
      return []
    })
    spy(ConsumerGroup.prototype, 'heartbeat', async () => {})
  } else {
    spy(Runner.prototype, 'scheduleFetchManager', () => {})
  }
  const consumer = createConsumer({
    cluster,
    groupId: 'group',
    logger: newLogger(),
    ...(!groupProtocol && { partitionAssigners: [cooperative ? cooperativeSticky : roundRobin] }),
    retry: { retries: 0 },
    ...(groupProtocol && { groupProtocol }),
  })
  return {
    consumer,
    cluster,
    joins,
    spies,
    /** Undoes every spy. @returns {void} */
    restore: () => Object.values(spies).forEach((s) => s.mockRestore()),
  }
}

module.exports = { createHookRig, createPublicRig, recordingListener, gate, settle }
