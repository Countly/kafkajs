const createConsumer = require('../index')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const ResetObligations = require('../offsetManager/resetObligations')
const { disposeConsumer } = require('../disposal')
const { newLogger, waitFor } = require('testHelpers')
const { KafkaJSNonRetriableError } = require('../../errors')

/** @returns {{promise: Promise<any>, resolve: Function, reject: Function}} a manually settled promise */
const gate = () => {
  const held = {}
  held.promise = new Promise((resolve, reject) => {
    held.resolve = resolve
    held.reject = reject
  })
  return held
}

/** @returns {Promise<void>} resolves after the pending microtasks and one macrotask turn */
const flush = () => new Promise((resolve) => setImmediate(resolve))

/**
 * A cluster with just what a consumer without a broker needs, recording its listener registrations.
 * @param {object} [overrides]
 * @returns {object} the fake, with `listeners` (the live topic-recreation handlers) and `removes`
 */
const fakeCluster = (overrides = {}) => {
  const listeners = new Set()
  const removes = []
  const operations = new OffsetOperations()
  jest.spyOn(operations, 'touchTopic')
  return {
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    resetTargetTopics: jest.fn(async () => true),
    removeBroker: jest.fn(),
    isConnected: () => true,
    addMultipleTargetTopics: jest.fn(async () => {}),
    useConsumerMetadataScope: jest.fn(),
    releaseConsumerMetadataScope: jest.fn(),
    onTopicRecreated: jest.fn((handler) => {
      listeners.add(handler)
      const remove = jest.fn(() => listeners.delete(handler))
      removes.push(remove)
      return remove
    }),
    offsetOperations: () => operations,
    retry: {},
    listeners,
    removes,
    operations,
    ...overrides,
  }
}

jest.setTimeout(15000)

const build = (cluster, options = {}) =>
  createConsumer({ cluster, groupId: 'g', logger: newLogger(), partitionAssigners: [], ...options })

describe('a public connect owns its lifetime', () => {
  it('publishes no CONNECT after a forced close retired it', async () => {
    const held = gate()
    const cluster = fakeCluster({ connect: jest.fn(() => held.promise) })
    const consumer = build(cluster)
    const events = []
    consumer.on(consumer.events.CONNECT, () => events.push('CONNECT'))
    const connecting = consumer.connect().catch((error) => error)
    await consumer.disconnect({ timeout: 0 })
    held.resolve()
    const outcome = await connecting
    await flush()
    expect(events).toEqual([])
    expect(outcome.type).toBe('CONSUMER_STOPPING')
  })

  it('refuses the retired connect while a replacement connect publishes once', async () => {
    const first = gate()
    const cluster = fakeCluster({ connect: jest.fn().mockReturnValueOnce(first.promise) })
    const consumer = build(cluster)
    const events = []
    consumer.on(consumer.events.CONNECT, () => events.push('CONNECT'))
    const stale = consumer.connect().catch((error) => error)
    await consumer.disconnect({ timeout: 0 })
    await consumer.connect()
    first.resolve()
    await stale
    await flush()
    expect(events).toEqual(['CONNECT'])
    await consumer.disconnect()
  })

  it('hands the connect fence to the cluster so its retries end with the close', async () => {
    const held = gate()
    const cluster = fakeCluster({ connect: jest.fn(() => held.promise) })
    const consumer = build(cluster)
    const connecting = consumer.connect().catch((error) => error)
    const { fence } = cluster.connect.mock.calls[0][0]
    expect(fence.closed).toBe(false)
    await consumer.disconnect({ timeout: 0 })
    expect(fence.closed).toBe(true)
    held.resolve()
    await connecting
  })

  it('still publishes CONNECT for an ordinary connect', async () => {
    const consumer = build(fakeCluster())
    const events = []
    consumer.on(consumer.events.CONNECT, () => events.push('CONNECT'))
    await consumer.connect()
    await flush()
    expect(events).toEqual(['CONNECT'])
    await consumer.disconnect()
  })
})

describe('an old close settling late releases only what it owned', () => {
  afterEach(() => jest.restoreAllMocks())

  /**
   * Holds the first cluster disconnect and starts a replacement connect meanwhile. The replacement
   * waits for the old close, so it acquires its lifetime only after the close released its own.
   * @param {object} cluster the fake cluster
   * @param {object} consumer the consumer under test
   * @returns {Promise<void>} resolves once the old close settled and the replacement connected
   */
  const closeAroundReplacement = async (cluster, consumer) => {
    const hold = gate()
    cluster.disconnect.mockImplementationOnce(() => hold.promise)
    const closing = consumer.disconnect()
    await flush()
    const connects = cluster.connect.mock.calls.length
    const replacing = consumer.connect()
    await flush()
    expect(cluster.connect).toHaveBeenCalledTimes(connects)
    hold.resolve()
    await Promise.all([closing, replacing])
    expect(cluster.connect).toHaveBeenCalledTimes(connects + 1)
  }

  it('keeps the replacement topic-recreation listener registered', async () => {
    const cluster = fakeCluster()
    const consumer = build(cluster)
    await consumer.subscribe({ topic: 't' })
    await consumer.connect()
    await closeAroundReplacement(cluster, consumer)
    expect(cluster.removes).toHaveLength(2)
    expect(cluster.removes[0]).toHaveBeenCalledTimes(1)
    expect(cluster.removes[1]).not.toHaveBeenCalled()
    for (const handler of [...cluster.listeners]) handler({ payload: { topic: 't', topicId: 'x' } })
    expect(cluster.operations.touchTopic).toHaveBeenCalledTimes(1)
  })

  it('keeps the debt the replacement owes after the old close cleared its own', async () => {
    const clear = jest.spyOn(ResetObligations.prototype, 'clear')
    const owe = jest.spyOn(ResetObligations.prototype, 'owe')
    const cluster = fakeCluster()
    const consumer = build(cluster)
    await consumer.subscribe({ topic: 't' })
    await consumer.connect()
    await closeAroundReplacement(cluster, consumer)
    for (const handler of [...cluster.listeners]) handler({ payload: { topic: 't', topicId: 'x' } })
    expect(owe).toHaveBeenCalledTimes(1)
    const debtor = owe.mock.contexts[0]
    expect(debtor.size()).toBe(1)
    expect(clear).toHaveBeenCalledTimes(1)
    expect(clear.mock.invocationCallOrder[0]).toBeLessThan(owe.mock.invocationCallOrder[0])
  })

  it('registers the discovery scope for the replacement after the old close released its own', async () => {
    const cluster = fakeCluster()
    const consumer = build(cluster)
    await consumer.subscribe({ topics: [/^t-.*/] })
    await consumer.connect()
    await closeAroundReplacement(cluster, consumer)
    expect(cluster.releaseConsumerMetadataScope).toHaveBeenCalledTimes(1)
    const [release] = cluster.releaseConsumerMetadataScope.mock.invocationCallOrder
    const uses = cluster.useConsumerMetadataScope.mock.invocationCallOrder
    expect(uses[uses.length - 1]).toBeGreaterThan(release)
  })

  it('does release its own discovery scope and listener when nothing replaced it', async () => {
    const cluster = fakeCluster()
    const consumer = build(cluster)
    await consumer.subscribe({ topics: [/^t-.*/] })
    await consumer.connect()
    await consumer.disconnect()
    expect(cluster.releaseConsumerMetadataScope).toHaveBeenCalledTimes(1)
    expect(cluster.listeners.size).toBe(0)
  })
})

describe('public calls belong to the lifetime that started them', () => {
  it('a forced close retires its own call and leaves the replacement call running', async () => {
    const lookups = [gate(), gate()]
    const coordinator = { offsetFetch: jest.fn(async () => ({ responses: [] })) }
    const cluster = fakeCluster({
      findGroupCoordinator: jest.fn(() => lookups.shift().promise),
    })
    const teardown = gate()
    cluster.disconnect.mockImplementationOnce(() => teardown.promise)
    const consumer = build(cluster)
    await consumer.connect()
    const second = lookups[1]
    const oldCall = consumer.committed([{ topic: 't', partition: 0 }]).catch((error) => error)
    await flush()
    const closing = consumer.disconnect({ timeout: 40 })
    const replacing = consumer.connect()
    expect(await oldCall).toBeInstanceOf(Error)
    teardown.resolve()
    await Promise.all([closing, replacing])
    const newCall = consumer.committed([{ topic: 't', partition: 0 }])
    let settled = false
    newCall.then(
      () => (settled = true),
      () => (settled = true)
    )
    await flush()
    expect(settled).toBe(false)
    coordinator.offsetFetch.mockResolvedValue({
      responses: [{ topic: 't', partitions: [{ partition: 0, offset: '3', metadata: '' }] }],
    })
    second.resolve(coordinator)
    await expect(newCall).resolves.toEqual([
      { topic: 't', partition: 0, offset: '3', metadata: '' },
    ])
    await consumer.disconnect()
  })
})

describe('crash restart ownership', () => {
  const crashing = (extra = {}) => {
    const cluster = fakeCluster()
    const crashes = []
    const consumer = build(cluster, {
      retry: { retries: 0, initialRetryTime: 20, ...extra },
    })
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))
    cluster.connect.mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('fatal misconfiguration')
    })
    return { cluster, consumer, crashes }
  }

  it('a restart policy answering true after a forced close arms no restart', async () => {
    const decision = gate()
    const { cluster, consumer, crashes } = crashing({ restartOnFailure: () => decision.promise })
    await consumer.subscribe({ topic: 't' })
    const running = consumer.run({ eachMessage: async () => {} })
    await waitFor(() => cluster.connect.mock.calls.length === 1, { delay: 5, maxWait: 2000 })
    await flush()
    await consumer.disconnect({ timeout: 0 })
    decision.resolve(true)
    await running
    await flush()
    await new Promise((resolve) => setTimeout(resolve, 80))
    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(false)
    expect(cluster.connect).toHaveBeenCalledTimes(1)
  })

  it('a restart connect held past a forced close resurrects nothing', async () => {
    const { cluster, consumer, crashes } = crashing({ restartOnFailure: async () => true })
    const held = gate()
    const started = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topic: 't' })
    await consumer.run({ eachMessage: async () => {} })
    expect(crashes[0].restart).toBe(true)
    cluster.connect.mockImplementation(() => held.promise)
    await waitFor(() => cluster.connect.mock.calls.length === 2, { delay: 5, maxWait: 2000 })
    await consumer.disconnect({ timeout: 0 })
    held.resolve()
    await flush()
    await new Promise((resolve) => setTimeout(resolve, 80))
    expect(crashes).toHaveLength(1)
    expect(cluster.connect).toHaveBeenCalledTimes(2)
    expect(started).toHaveBeenCalledTimes(2)
    expect(cluster.removeBroker).not.toHaveBeenCalled()
    started.mockRestore()
  })

  it('a crash of a closed run is ignored and cannot stop its replacement', async () => {
    const { cluster, consumer, crashes } = crashing({ restartOnFailure: async () => true })
    const failing = gate()
    const replacing = gate()
    cluster.connect
      .mockImplementationOnce(() => failing.promise)
      .mockImplementationOnce(() => replacing.promise)
    const latch = jest.spyOn(Runner.prototype, 'latchStop')
    await consumer.subscribe({ topic: 't' })
    const first = consumer.run({ eachMessage: async () => {} })
    await flush()
    await consumer.disconnect({ timeout: 0 })
    const second = consumer.run({ eachMessage: async () => {} })
    await flush()
    const stopsBefore = latch.mock.calls.length
    const disconnectsBefore = cluster.disconnect.mock.calls.length
    failing.reject(new KafkaJSNonRetriableError('late failure of the closed run'))
    await first
    await flush()
    expect(crashes).toHaveLength(0)
    expect(latch).toHaveBeenCalledTimes(stopsBefore)
    expect(cluster.disconnect).toHaveBeenCalledTimes(disconnectsBefore)
    replacing.resolve()
    await second
    await consumer.disconnect({ timeout: 0 })
    latch.mockRestore()
  })

  it('a disconnect cancels a pending restart timer', async () => {
    jest.useFakeTimers()
    try {
      const { cluster, consumer, crashes } = crashing({
        restartOnFailure: async () => true,
        initialRetryTime: 5000,
      })
      await consumer.subscribe({ topic: 't' })
      const running = consumer.run({ eachMessage: async () => {} })
      await jest.advanceTimersByTimeAsync(10)
      await running
      expect(crashes[0].restart).toBe(true)
      await consumer.disconnect()
      expect(jest.getTimerCount()).toBe(0)
      await jest.advanceTimersByTimeAsync(10000)
      expect(cluster.connect).toHaveBeenCalledTimes(1)
    } finally {
      jest.useRealTimers()
    }
  })
})

describe('a borrowed consumer disposed while its start is pending', () => {
  afterEach(() => jest.restoreAllMocks())

  it('requests the stop before it waits for the start, and leaves the cluster connected', async () => {
    const held = gate()
    const cluster = fakeCluster({ connect: jest.fn(() => held.promise) })
    const consumer = build(cluster)
    const latch = jest.spyOn(Runner.prototype, 'latchStop')
    await consumer.subscribe({ topic: 't' })
    const running = consumer.run({ eachMessage: async () => {} })
    await flush()
    const disposing = disposeConsumer(consumer)
    await flush()
    expect(latch).toHaveBeenCalledTimes(1)
    held.resolve()
    await disposing
    await running
    expect(cluster.disconnect).not.toHaveBeenCalled()
    expect(cluster.listeners.size).toBe(0)
  })
})
