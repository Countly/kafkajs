const Protocol = require('../groupProtocol/consumer')
const Subscription = require('../subscription')
const Cluster = require('../../cluster')
const MetadataCache = require('../../cluster/metadataCache')
const Emitter = require('../../instrumentation/emitter')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSProtocolError } = require('../../errors')
const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER = '21222324-2526-2728-292a-2b2c2d2e2f30'
const OLD = '31323334-3536-3738-393a-3b3c3d3e3f40'

/** @returns {object} no-output logger with independently countable calls */
const logger = () => ({
  namespace() {
    return this
  },
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
})
/** @returns {{promise: Promise<object>, resolve: Function, reject: Function}} manually released response */
const gate = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}
/** @param {string} id topic incarnation @returns {object} decoded Metadata answer */
const metadata = (id = ID) => ({
  brokers: [{ nodeId: 1 }],
  topicMetadata: [
    {
      topic: 'named',
      topicId: id,
      partitionMetadata: [{ partitionId: 0, leader: 1, leaderEpoch: 8 }],
    },
  ],
})
/** @returns {Promise<void>} drains only the finite discovery continuation chain */
const settle = async () => {
  for (let i = 0; i < 30; i++) await Promise.resolve()
}
/** @param {object} cluster metadata source @returns {object} stable real protocol with an unresolved target */
const protocol = (cluster) => {
  const p = new Protocol({
    cluster,
    groupId: 'discovery',
    rebalanceTimeout: 30000,
    metadataMaxAge: 1000,
    logger: logger(),
    instrumentationEmitter: new Emitter(),
  })
  p.state = 'STABLE'
  p.memberEpoch = 7
  p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
  p.rebalanceCoordinator = { requestRejoin: jest.fn() }
  return p
}
/** @param {Function} answer Metadata responder @param {number} version negotiated Metadata version
 * @returns {object} real cache and cluster methods without sockets or timer work */
const build = (answer, version = 13) => {
  const log = logger()
  const emitter = new Emitter()
  const cache = new MetadataCache({ logger: log, instrumentationEmitter: emitter })
  const old = metadata(OLD)
  cache.update(old)
  const update = jest.spyOn(cache, 'updateTopicsById')
  const broker = { metadata: jest.fn(answer), negotiatedVersion: () => version }
  const cluster = Object.assign(Object.create(Cluster.prototype), {
    logger: log,
    metadataCache: cache,
    brokerPool: { metadata: old, withBroker: async (fn) => fn({ broker }) },
    refreshMetadata: jest.fn(async () => {}),
    topicName: (id) => cache.topicName(id),
  })
  const p = protocol(cluster)
  return { p, cluster, broker, cache, update, old }
}

describe('Consumer > target discovery', () => {
  afterEach(() => jest.restoreAllMocks())
  it('resolves a just-converged subset immediately when both change flags are false', async () => {
    const h = build(async () => metadata())
    h.p.state = 'RECONCILING'
    h.p.evaluateTarget({ targetChanged: false })
    await settle()
    expect(h.broker.metadata).toHaveBeenCalledTimes(1)
    expect(h.cache.topicName(ID)).toBe('named')
    expect(h.p.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
  })
  it('limits fast misses but retries forever at metadata age, with one request in flight', async () => {
    let now = 10000
    jest.spyOn(Date, 'now').mockImplementation(() => now)
    const pending = gate()
    const h = build(async () => pending.promise)
    h.p.evaluateTarget({ targetChanged: false })
    await settle()
    for (let i = 0; i < 20; i++) h.p.evaluateTarget({ targetChanged: false })
    await settle()
    expect(h.broker.metadata).toHaveBeenCalledTimes(1)
    pending.resolve({ brokers: [], topicMetadata: [] })
    await settle()
    await h.p.refreshMetadataForTopicIds([ID])
    expect(h.p.unresolvedAttempts.get(ID)).toBe(2)
    await h.p.refreshMetadataForTopicIds([ID])
    expect(h.broker.metadata).toHaveBeenCalledTimes(2)
    for (let i = 0; i < 5; i++) {
      now += 999
      h.p.evaluateTarget({ targetChanged: false })
      await settle()
      expect(h.broker.metadata).toHaveBeenCalledTimes(2 + i)
      now += 1
      h.p.evaluateTarget({ targetChanged: false })
      await settle()
      expect(h.broker.metadata).toHaveBeenCalledTimes(3 + i)
      expect(h.p.unresolvedAttempts.get(ID)).toBe(2)
    }
    h.broker.metadata.mockResolvedValue(metadata())
    now += 1000
    h.p.evaluateTarget({ targetChanged: false })
    await settle()
    expect(h.p.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
  })
  test.each(['clear', 'fence', 'leave', 'rejoin'])(
    'rejects a bulk answer after %s at equal ids and epochs',
    async (retirement) => {
      const held = gate()
      const h = build(async () => held.promise)
      h.p.unresolvedTopicIds.add(ID)
      const oldEpoch = h.p.target.localEpoch
      const started = h.p.refreshMetadataForTopicIds([ID])
      await settle()
      if (retirement === 'fence')
        h.p.transitionToFenced(new KafkaJSProtocolError(createErrorFromCode(110)))
      else if (retirement === 'leave') await h.p.leaveForPollExpiry()
      else if (retirement === 'rejoin') h.p.resetGeneration()
      else h.p.clearTarget()
      h.p.state = 'STABLE'
      h.p.memberEpoch = 7
      h.p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
      expect(h.p.target.localEpoch).toBe(oldEpoch)
      h.p.unresolvedTopicIds.add(ID)
      const before = JSON.stringify([...h.cache.topics])
      held.resolve(metadata())
      await started
      await settle()
      expect(h.update).not.toHaveBeenCalled()
      expect(JSON.stringify([...h.cache.topics])).toBe(before)
      expect(h.cluster.brokerPool.metadata).toBe(h.old)
      expect(h.old.topicMetadata[0].partitionMetadata[0]).toMatchObject({
        leader: 1,
        leaderEpoch: 8,
      })
      expect(h.cache.topicName(ID)).toBeUndefined()
      expect(h.p.unresolvedAttempts.size).toBe(0)
      expect([...h.p.unresolvedTopicIds]).toEqual([ID])
      expect(h.p.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
      expect(h.broker.metadata).toHaveBeenCalledTimes(1)
    }
  )
  test.each(['bulk', 'per-id fallback'])(
    'a retired metadata scope stops a held %s answer from any effect while a current twin resolves',
    async (route) => {
      const held = gate()
      const h = build(async () => metadata())
      h.p.subscription = new Subscription()
      h.p.unresolvedTopicIds.add(ID)
      h.p.unresolvedTopicIds.add(OTHER)
      if (route === 'bulk') {
        h.broker.metadata.mockImplementationOnce(() => held.promise)
      } else {
        h.broker.metadata.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(100)))
        h.broker.metadata.mockImplementationOnce(() => held.promise)
      }
      const started = h.p.refreshMetadataForTopicIds([ID, OTHER])
      await settle()
      const sent = h.broker.metadata.mock.calls.length
      h.p.subscription.retireMetadata()
      const before = JSON.stringify([...h.cache.topics])
      held.resolve(metadata())
      await started
      await settle()
      expect(h.update).not.toHaveBeenCalled()
      expect(JSON.stringify([...h.cache.topics])).toBe(before)
      expect(h.cluster.brokerPool.metadata).toBe(h.old)
      expect(h.broker.metadata).toHaveBeenCalledTimes(sent)
      expect(h.p.unresolvedAttempts.size).toBe(0)
      expect(h.p.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()

      h.broker.metadata.mockResolvedValue(metadata())
      h.p.unresolvedTopicIds.add(ID)
      h.p.state = 'RECONCILING'
      h.p.evaluateTarget({ targetChanged: false })
      await settle()
      expect(h.update).toHaveBeenCalledTimes(1)
      expect(h.cache.topicName(ID)).toBe('named')
      expect(h.p.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
    }
  )
  it('keeps metadata effects retired after stale bulk and per-id replies', async () => {
    const bulk = gate()
    const single = gate()
    const h = build(async () => metadata())
    const renewed = () => {
      h.p.state = 'STABLE'
      h.p.memberEpoch = 7
      h.p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
      h.p.unresolvedTopicIds.add(ID)
    }
    h.p.unresolvedTopicIds.add(ID)
    const epoch = h.p.target.localEpoch
    h.broker.metadata.mockImplementationOnce(() => bulk.promise)
    const first = h.p.refreshMetadataForTopicIds([ID])
    await settle()
    await h.p.leaveForPollExpiry()
    renewed()
    expect(h.p.target.localEpoch).toBe(epoch)
    bulk.resolve(metadata())
    await first
    await settle()
    h.broker.metadata.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(100)))
    h.broker.metadata.mockImplementationOnce(() => single.promise)
    const second = h.p.refreshMetadataForTopicIds([ID, OTHER])
    await settle()
    const sent = h.broker.metadata.mock.calls.length
    h.p.transitionToFenced(new KafkaJSProtocolError(createErrorFromCode(110)))
    renewed()
    single.resolve(metadata())
    await second
    await settle()
    expect(h.update).not.toHaveBeenCalled()
    expect(h.cache.topicName(ID)).toBeUndefined()
    expect(h.cluster.brokerPool.metadata).toBe(h.old)
    expect(h.broker.metadata).toHaveBeenCalledTimes(sent)
    expect(h.p.unresolvedAttempts.size).toBe(0)
    expect(h.p.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
    h.broker.metadata.mockResolvedValue(metadata())
    h.p.state = 'RECONCILING'
    h.p.evaluateTarget({ targetChanged: false })
    await settle()
    expect(h.update).toHaveBeenCalledTimes(1)
    expect(h.cache.topicName(ID)).toBe('named')
    expect(h.p.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
  })
  it('does not install a held per-id answer or ask the next id after retirement', async () => {
    const held = gate()
    const h = build(async () => metadata())
    h.broker.metadata.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(100)))
    h.broker.metadata.mockImplementationOnce(() => held.promise)
    h.p.unresolvedTopicIds.add(ID)
    h.p.unresolvedTopicIds.add(OTHER)
    const started = h.p.refreshMetadataForTopicIds([ID, OTHER])
    await settle()
    expect(h.broker.metadata).toHaveBeenCalledTimes(2)
    h.p.clearTarget()
    h.p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
    held.resolve(metadata())
    await started
    expect(h.update).not.toHaveBeenCalled()
    expect(h.broker.metadata).toHaveBeenCalledTimes(2)
    expect(h.p.unresolvedAttempts.size).toBe(0)
  })
  it('does not start per-id fallbacks from a retired bulk rejection', async () => {
    const held = gate()
    const h = build(() => held.promise)
    const started = h.p.refreshMetadataForTopicIds([ID, OTHER])
    await settle()
    h.p.clearTarget()
    held.reject(new KafkaJSProtocolError(createErrorFromCode(100)))
    await started
    expect(h.update).not.toHaveBeenCalled()
    expect(h.broker.metadata).toHaveBeenCalledTimes(1)
  })
  it('guards old-version full-name responses without installing global routing', async () => {
    const held = gate()
    const h = build(() => held.promise, 11)
    const started = h.p.refreshMetadataForTopicIds([ID])
    await settle()
    h.p.clearTarget()
    held.resolve(metadata())
    await started
    expect(h.update).not.toHaveBeenCalled()
    expect(h.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(h.cluster.brokerPool.metadata).toBe(h.old)
  })
  it('keeps ordinary unguarded old-version refresh behavior', async () => {
    const h = build(async () => metadata(), 11)
    await h.cluster.refreshMetadataForTopicIds([ID])
    expect(h.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(h.broker.metadata).not.toHaveBeenCalled()
  })
  it('keeps a current fallback useful when one id fails', async () => {
    const h = build(async () => metadata())
    h.broker.metadata.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(100)))
    h.broker.metadata.mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(100)))
    const outcome = await h.cluster.refreshMetadataForTopicIds([OTHER, ID], {
      isCurrent: () => true,
    })
    expect(outcome).toEqual({ resolved: [ID], unresolved: [OTHER] })
    expect(h.broker.metadata).toHaveBeenCalledTimes(3)
    expect(h.update).toHaveBeenCalledTimes(1)
  })
  it('retires the background re-read across an equal-valued replacement target', async () => {
    const held = gate()
    const h = build(() => held.promise)
    h.p.evaluateTarget({ targetChanged: false })
    await settle()
    const epoch = h.p.target.localEpoch
    h.p.clearTarget()
    h.p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
    expect(h.p.target.localEpoch).toBe(epoch)
    const reread = jest.spyOn(h.p, 'evaluateTarget')
    held.resolve(metadata())
    await settle()
    expect(reread).not.toHaveBeenCalled()
    expect(h.update).not.toHaveBeenCalled()
    expect(h.p.unresolvedAttempts.size).toBe(0)
  })
  it('keeps ordinary current bulk install and guarded by-name install useful', async () => {
    for (const version of [11, 13]) {
      const h = build(async () => metadata(), version)
      await h.p.refreshMetadataForTopicIds([ID])
      expect(h.cache.topicName(ID)).toBe('named')
      expect(h.update).toHaveBeenCalledTimes(1)
      expect(h.cluster.brokerPool.metadata).toBe(h.old)
    }
  })
  it('does not publish counters from an obsolete resolution even if its helper returns ids', async () => {
    const held = gate()
    const cluster = {
      topicName: () => undefined,
      refreshMetadataForTopicIds: jest.fn(() => held.promise),
    }
    const p = protocol(cluster)
    const started = p.refreshMetadataForTopicIds([ID])
    await settle()
    p.clearTarget()
    p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
    held.resolve({ resolved: [], unresolved: [ID] })
    await started
    expect(p.unresolvedAttempts.size).toBe(0)
  })
})
