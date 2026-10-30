const Cluster = require('../index')

beforeEach(() => {
  expect(typeof Cluster.prototype.groupMetadata).toBe('function')
})

/** @param {object} overrides @returns {{cluster: object, broker: object, other: object}} isolated snapshot owner */
const fixture = (overrides = {}) => {
  const broker = {
    isConnected: () => true,
    connect: jest.fn(async () => {}),
    metadata: jest.fn(async () => ({ topicMetadata: [] })),
    ...overrides,
  }
  const other = {
    isConnected: () => true,
    connect: jest.fn(),
    metadata: jest.fn(),
  }
  const cluster = Object.create(Cluster.prototype)
  cluster.brokerPool = {
    brokers: { 1: broker, 2: other },
    metadata: { sentinel: true },
    metadataCache: { sentinel: true },
    seedBroker: other,
  }
  cluster.targetTopics = new Set(['A'])
  return { cluster, broker, other }
}

describe('bounded group snapshot', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.restoreAllMocks()
    jest.clearAllTimers()
    jest.useRealTimers()
  })
  it('uses one broker once without touching targets or cached metadata', async () => {
    const { cluster, broker, other } = fixture()
    const cache = cluster.brokerPool.metadataCache
    const metadata = cluster.brokerPool.metadata
    await cluster.groupMetadata({
      topics: ['B', 'C'],
      allowAutoTopicCreation: false,
      deadlineAt: performance.now() + 100,
      guard: () => null,
    })
    expect(broker.metadata).toHaveBeenCalledTimes(1)
    expect(broker.metadata.mock.calls[0][0]).toEqual(['B', 'C'])
    expect(broker.metadata.mock.calls[0][1]).toMatchObject({
      allowAutoTopicCreation: false,
      lenient: true,
    })
    expect(other.connect).not.toHaveBeenCalled()
    expect(other.metadata).not.toHaveBeenCalled()
    expect([...cluster.targetTopics]).toEqual(['A'])
    expect(cluster.brokerPool.metadata).toBe(metadata)
    expect(cluster.brokerPool.metadataCache).toBe(cache)
    expect(jest.getTimerCount()).toBe(0)
  })
  it('preserves an authentication failure without trying another broker', async () => {
    const original = new Error('authentication failed')
    const { cluster, other } = fixture({
      connect: jest.fn(async () => {
        throw original
      }),
    })
    await expect(
      cluster.groupMetadata({
        topics: ['B'],
        allowAutoTopicCreation: false,
        deadlineAt: performance.now() + 100,
        guard: () => null,
      })
    ).rejects.toBe(original)
    expect(other.connect).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })
  it('settles at the deadline and refuses a late acquisition continuation', async () => {
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    const { cluster, broker } = fixture({ connect: jest.fn(() => held) })
    const pending = cluster
      .groupMetadata({
        topics: ['B'],
        allowAutoTopicCreation: false,
        deadlineAt: performance.now() + 100,
        guard: () => null,
      })
      .catch((error) => error)
    await jest.advanceTimersByTimeAsync(100)
    expect(await pending).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    release()
    await Promise.resolve()
    await Promise.resolve()
    expect(broker.metadata).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })
  it('checks the actual clock before Metadata even when timer callbacks are withheld', async () => {
    let now = 0
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    const { cluster, broker } = fixture({ connect: jest.fn(() => held) })
    const pending = cluster
      .groupMetadata({
        topics: ['B'],
        allowAutoTopicCreation: false,
        deadlineAt: 100,
        guard: () => null,
      })
      .catch((error) => error)
    now = 101
    release()
    expect(await pending).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    expect(broker.metadata).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })
})
