const createConsumer = require('../index')
const { disposeConsumer } = require('../disposal')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'

/**
 * A stub cluster with a real instrumentation emitter, so the listener bookkeeping under test is the
 * real code path, and a `disconnect` that records whether anything shut the shared cluster down.
 */
const createCluster = () => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const operations = new OffsetOperations()
  return {
    instrumentationEmitter,
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    metadata: async () => ({ topicMetadata: [] }),
    addMultipleTargetTopics: async () => {},
    refreshMetadata: async () => {},
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => ({}),
    offsetOperations: () => operations,
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    topicId: () => undefined,
    onTopicRecreated: (handler) =>
      instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler),
    listenerCount: () => instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED),
  }
}

const build = (cluster) =>
  createConsumer({
    cluster,
    groupId: 'test-group',
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    metadataMaxAge: 300000,
    retry: { retries: 0 },
  })

describe('Consumer > disposal of a consumer that borrows its creator cluster', () => {
  afterEach(() => {
    jest.restoreAllMocks()
  })

  test('an idle consumer gives up its cluster listener and leaves the cluster connected', async () => {
    const cluster = createCluster()
    const consumer = build(cluster)
    expect(cluster.listenerCount()).toBe(1)

    await disposeConsumer(consumer)

    expect(cluster.listenerCount()).toBe(0)
    expect(cluster.disconnect).not.toHaveBeenCalled()
  })

  test('a running consumer is stopped once, and the cluster stays connected', async () => {
    const cluster = createCluster()
    jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
    const stop = jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
    const consumer = build(cluster)
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })

    await disposeConsumer(consumer)

    expect(stop).toHaveBeenCalledTimes(1)
    expect(cluster.listenerCount()).toBe(0)
    expect(cluster.disconnect).not.toHaveBeenCalled()
  })

  test('a consumer still starting is stopped only once its start has settled', async () => {
    const cluster = createCluster()
    let settleStart
    jest.spyOn(Runner.prototype, 'start').mockReturnValue(new Promise((r) => (settleStart = r)))
    const stop = jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
    const consumer = build(cluster)
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    const running = consumer.run({ eachMessage: async () => {} })

    const disposed = disposeConsumer(consumer)
    await new Promise((resolve) => setImmediate(resolve))
    expect(stop).not.toHaveBeenCalled()

    settleStart()
    await Promise.all([running, disposed])

    expect(stop).toHaveBeenCalledTimes(1)
    expect(cluster.listenerCount()).toBe(0)
  })

  test('a start that fails does not keep the disposal from finishing', async () => {
    const cluster = createCluster()
    jest.spyOn(Runner.prototype, 'start').mockRejectedValue(new Error('start failed'))
    jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
    const consumer = build(cluster)
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    const running = consumer.run({ eachMessage: async () => {} })

    await disposeConsumer(consumer)

    await expect(running).rejects.toThrow('start failed')
    expect(cluster.listenerCount()).toBe(0)
  })

  test('a second disposal changes nothing', async () => {
    const cluster = createCluster()
    const consumer = build(cluster)

    await disposeConsumer(consumer)
    await disposeConsumer(consumer)

    expect(cluster.listenerCount()).toBe(0)
    expect(cluster.disconnect).not.toHaveBeenCalled()
  })

  test('a failed stop still releases the listener, and the failure reaches the caller', async () => {
    const cluster = createCluster()
    jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
    jest.spyOn(Runner.prototype, 'stop').mockRejectedValue(new Error('stop failed'))
    const consumer = build(cluster)
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })

    await expect(disposeConsumer(consumer)).rejects.toThrow('stop failed')

    expect(cluster.listenerCount()).toBe(0)
    expect(cluster.disconnect).not.toHaveBeenCalled()
  })

  test('disconnect() is unchanged: it disconnects the cluster and releases the listener', async () => {
    const cluster = createCluster()
    const consumer = build(cluster)

    await consumer.disconnect()

    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(cluster.listenerCount()).toBe(0)
  })

  test('an object that is not a consumer is ignored', async () => {
    await expect(disposeConsumer({})).resolves.toBeUndefined()
  })
})
