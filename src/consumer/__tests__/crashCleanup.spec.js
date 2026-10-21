const createConsumer = require('../index')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { newLogger, waitFor } = require('testHelpers')
const {
  createScheduledOffsetManager,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')

const TOPIC = 'test-topic'
const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

/**
 * A stub Cluster with a real InstrumentationEventEmitter, so `onTopicRecreated` and the listener
 * bookkeeping under test are the real code paths.
 */
const createCluster = ({ topicIds = {} } = {}) => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  return {
    instrumentationEmitter,
    committedOffsetsByGroup: new Map(),
    operationsByGroup: new Map(),
    connect: async () => {},
    disconnect: async () => {},
    metadata: async () => ({ topicMetadata: [] }),
    addMultipleTargetTopics: async () => {},
    refreshMetadata: async () => {},
    refreshMetadataIfNecessary: async () => {},
    // Backed by the map, exactly as Cluster#committedOffsets is, so a test can assert that a
    // topic key was (or was not) written into the group's shared offsets.
    committedOffsets({ groupId }) {
      if (!this.committedOffsetsByGroup.has(groupId)) {
        this.committedOffsetsByGroup.set(groupId, {})
      }
      return this.committedOffsetsByGroup.get(groupId)
    },
    // Backed by the map, exactly as Cluster#offsetOperations is.
    offsetOperations({ groupId }) {
      if (!this.operationsByGroup.has(groupId)) {
        this.operationsByGroup.set(groupId, new OffsetOperations())
      }
      return this.operationsByGroup.get(groupId)
    },
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    topicId: (topic) => topicIds[topic],
    onTopicRecreated: (handler) =>
      instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler),
    listenerCount: () => instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED),
    emitRecreated: (payload) => instrumentationEmitter.emit(CLUSTER_TOPIC_RECREATED, payload),
  }
}

const build = (cluster, options = {}) =>
  createConsumer({
    cluster,
    groupId: 'test-group',
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    metadataMaxAge: 300000,
    retry: { retries: 0 },
    ...options,
  })

/** @returns {object} a deferred promise and its resolver for one scheduled boundary. */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}
/** @returns {Error} a retriable terminal runner error with an explicit restart delay. */
const crashError = () =>
  Object.assign(new Error('temporary failure'), { retriable: true, retryTime: 5 })

/**
 * Starts the real Consumer and ConsumerGroup with only Runner startup/stop stubbed.
 * @param {object} [retry={}] explicit retry/decision overrides
 * @returns {Promise<object>} client, group-owning runner, cluster and startup spy
 */
const started = async (retry = {}) => {
  const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
  jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
  const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
  const consumer = build(cluster, {
    retry: { retries: 0, initialRetryTime: 5, restartOnFailure: async () => true, ...retry },
  })
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({ eachMessage: async () => {}, autoCommit: false })
  return { start, cluster, consumer, runner: start.mock.instances[0] }
}

describe('Crash cleanup lifetime', () => {
  afterEach(() => jest.restoreAllMocks())

  test('explicit disconnect during crash cluster cleanup cancels the pending restart', async () => {
    const { start, cluster, consumer, runner } = await started()
    cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
    let arrived, release
    const entered = new Promise((resolve) => {
      arrived = resolve
    })
    const held = new Promise((resolve) => {
      release = resolve
    })
    let count = 0
    cluster.disconnect = async () => {
      if (++count === 1) {
        arrived()
        await held
      }
    }
    const crashes = []
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))
    try {
      const crashing = runner.onCrash(crashError())
      await entered
      await consumer.disconnect()
      expect(runner.consumerGroup.obligations.size()).toBe(0)
      release()
      await crashing
      await new Promise((resolve) => setTimeout(resolve, 40))
      expect({ starts: start.mock.calls.length, restart: crashes[0].restart }).toEqual({
        starts: 1,
        restart: false,
      })
      expect(cluster.listenerCount()).toBe(0)
    } finally {
      release()
      await consumer.disconnect()
    }
  })

  test('recreation during the cleanup gap survives and resolves 0 instead of committed 99', async () => {
    const deciding = gate()
    const entered = gate()
    const { start, cluster, consumer, runner } = await started({
      restartOnFailure: () => {
        entered.release()
        return deciding.promise
      },
    })
    const crashing = runner.onCrash(crashError())
    try {
      await entered.promise
      expect(runner.consumerGroup.operations.attached.size).toBe(0)
      cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      deciding.release(true)
      await crashing
      await waitFor(() => start.mock.calls.length === 2, { delay: 5, maxWait: 2000 })
      const restarted = start.mock.instances[1].consumerGroup
      const after = createScheduledOffsetManager({
        obligations: restarted.obligations,
        operations: restarted.operations,
        topicId: ID_B,
        committed: '99',
        listOffset: '0',
      })
      await after.manager.resolveOffsets()
      expect(after.next()).toBe('0')
      expect(cluster.listenerCount()).toBe(1)
    } finally {
      deciding.release(false)
      await crashing
      await consumer.disconnect()
      expect(cluster.listenerCount()).toBe(0)
    }
  })

  test('a stop before cluster cleanup also cancels the restart', async () => {
    const { start, consumer, runner } = await started()
    const stopping = gate()
    const entered = gate()
    Runner.prototype.stop.mockImplementationOnce(async () => {
      entered.release()
      await stopping.promise
    })
    const crashes = []
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))
    const crashing = runner.onCrash(crashError())
    try {
      await entered.promise
      const disconnecting = consumer.disconnect()
      stopping.release()
      await disconnecting
      await crashing
      await new Promise((resolve) => setTimeout(resolve, 40))
      expect({ starts: start.mock.calls.length, restart: crashes[0].restart }).toEqual({
        starts: 1,
        restart: false,
      })
    } finally {
      stopping.release()
      await crashing
      await consumer.disconnect()
    }
  })

  test('disconnect during the user decision clears resets and cannot rearm the listener', async () => {
    const deciding = gate()
    const entered = gate()
    const { start, cluster, consumer, runner } = await started({
      restartOnFailure: () => {
        entered.release()
        return deciding.promise
      },
    })
    const crashing = runner.onCrash(crashError())
    try {
      await entered.promise
      await consumer.disconnect()
      cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      deciding.release(true)
      await crashing
      await new Promise((resolve) => setTimeout(resolve, 40))
      expect(start).toHaveBeenCalledTimes(1)
      expect(cluster.listenerCount()).toBe(0)
      expect(runner.consumerGroup.obligations.size()).toBe(0)
    } finally {
      deciding.release(false)
      await crashing
      await consumer.disconnect()
    }
  })

  test('cleanup throwing still reports one crash and preserves cancellation', async () => {
    const { start, cluster, consumer, runner } = await started({
      restartOnFailure: async () => false,
    })
    cluster.disconnect = async () => {
      throw new Error('cleanup failed')
    }
    const crashes = []
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))
    await runner.onCrash(crashError())
    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(false)
    expect(start).toHaveBeenCalledTimes(1)
    expect(cluster.listenerCount()).toBe(1)
    await expect(consumer.disconnect()).rejects.toThrow('cleanup failed')
    expect(cluster.listenerCount()).toBe(0)
  })

  test('repeated crashes retain one listener and terminal disconnect removes it', async () => {
    const { start, cluster, consumer } = await started()
    try {
      for (let count = 1; count <= 3; count++) {
        await start.mock.instances[count - 1].onCrash(crashError())
        await waitFor(() => start.mock.calls.length === count + 1, { delay: 5, maxWait: 2000 })
        expect(cluster.listenerCount()).toBe(1)
      }
    } finally {
      await consumer.disconnect()
    }
    expect(cluster.listenerCount()).toBe(0)
  })

  test('repeated public stops during cleanup stay terminal', async () => {
    const deciding = gate()
    const entered = gate()
    const { start, cluster, consumer, runner } = await started({
      restartOnFailure: () => {
        entered.release()
        return deciding.promise
      },
    })
    const crashing = runner.onCrash(crashError())
    try {
      await entered.promise
      await consumer.disconnect()
      await consumer.disconnect()
      deciding.release(true)
      await crashing
      await new Promise((resolve) => setTimeout(resolve, 40))
      expect(start).toHaveBeenCalledTimes(1)
      expect(cluster.listenerCount()).toBe(0)
    } finally {
      deciding.release(false)
      await crashing
      await consumer.disconnect()
    }
  })

  test('public reuse after a terminal disconnect reacquires one subscription', async () => {
    const { cluster, consumer } = await started({ restartOnFailure: async () => false })
    await consumer.disconnect()
    expect(cluster.listenerCount()).toBe(0)
    await consumer.connect()
    await consumer.connect()
    expect(cluster.listenerCount()).toBe(1)
    await consumer.disconnect()
    expect(cluster.listenerCount()).toBe(0)
  })
})
