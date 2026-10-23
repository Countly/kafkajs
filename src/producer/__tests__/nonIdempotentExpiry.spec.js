const { Kafka } = require('../../../index')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const Generation = require('../generation')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

describe('Non-idempotent producer delivery expiry', () => {
  let producer, cluster, gate, pending, observation

  beforeEach(async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    const schedule = new Schedule()
    gate = schedule.hold({ api: 'Produce' })
    cluster = producerScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        schedule,
      })
    )
    producer = new Kafka({
      ...cluster.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({ idempotent: false, linger: 0, deliveryTimeout: 1000 })
    await producer.connect()
    let state = 'pending'
    let error
    pending = producer
      .send({ topic: TOPIC, messages: [{ value: 'deadline', timestamp: 1000 }] })
      .then(
        () => {
          state = 'fulfilled'
        },
        (failure) => {
          if (!(failure instanceof KafkaJSDeliveryTimeout)) throw failure
          error = failure
          state = 'rejected'
        }
      )
    for (let i = 0; i < 100 && !gate.call; i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(gate.call).not.toBeNull()
    await gate.arrived()
    cluster.mark('answer held')
    await jest.advanceTimersByTimeAsync(1000)
    const atDeadline = state
    await jest.advanceTimersByTimeAsync(4000)
    observation = { atDeadline, atFiveSeconds: state, error: error?.name, outcome: error?.outcome }
    gate.release()
    await pending
    for (
      let i = 0;
      i < 100 &&
      cluster.timeline().filter(({ api, kind }) => api === 'Produce' && kind === 'response')
        .length === 0;
      i += 1
    ) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    observation.afterLateSuccess = state
    await new Promise((resolve) => setImmediate(resolve))
    expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
    WireOrder.of(cluster).assertSequencesInOrder()
  })

  afterEach(async () => {
    if (gate?.settle) gate.release()
    if (pending) await pending
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    jest.useRealTimers()
  })

  it('settles an on-wire send at its delivery deadline and ignores a late success', () => {
    expect(observation).toEqual({
      atDeadline: 'rejected',
      atFiveSeconds: 'rejected',
      error: 'KafkaJSDeliveryTimeout',
      outcome: 'unknown',
      afterLateSuccess: 'rejected',
    })
  })

  it('applies a late leader hint without resending a settled batch', async () => {
    const Cluster = require('../../cluster')
    const applied = jest.spyOn(Cluster.prototype, 'applyLeaderHint')
    const schedule = new Schedule()
    const late = schedule.hold({ api: 'Produce' })
    const wire = producerScript(
      createScriptedCluster({
        nodes: [
          { id: 0, host: 'node0', port: 9092 },
          { id: 1, host: 'node1', port: 9093 },
        ],
        profile: 'tv1',
        schedule,
      })
    )
    wire.answer('Metadata', () => ({
      brokers: [
        { nodeId: 0, host: 'node0', port: 9092, rack: null },
        { nodeId: 1, host: 'node1', port: 9093, rack: null },
      ],
      controllerId: 0,
      topics: [
        {
          name: TOPIC,
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          errorCode: 0,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 0,
              leaderEpoch: 1,
              replicas: [0],
              isr: [0],
              offlineReplicas: [],
            },
          ],
        },
      ],
    }))
    const client = new Kafka({
      ...wire.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({ idempotent: false, linger: 0, deliveryTimeout: 1000 })
    let error, task
    const pump = async (condition) => {
      for (let i = 0; i < 200 && !condition(); i += 1) {
        await jest.advanceTimersByTimeAsync(1)
        await new Promise((resolve) => setImmediate(resolve))
      }
      expect(condition()).toBe(true)
    }
    try {
      await client.connect()
      task = client.send({ topic: TOPIC, messages: [{ value: 'late hint' }] }).catch((failure) => {
        error = failure
      })
      await pump(() => late.call !== null)
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
      const metadataBefore = wire.requests().filter(({ api }) => api === 'Metadata').length
      const responsesBefore = wire
        .timeline()
        .filter(({ api, kind }) => api === 'Metadata' && kind === 'response').length
      late.release({
        responses: [
          {
            topic: TOPIC,
            partitions: [
              {
                partition: 0,
                errorCode: 6,
                baseOffset: '-1',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: { leaderId: 1, leaderEpoch: 2 },
              },
            ],
          },
        ],
        nodeEndpoints: [{ nodeId: 1, host: 'node1', port: 9093, rack: null }],
      })
      await pump(
        () =>
          applied.mock.calls.length > 0 &&
          wire.requests().filter(({ api }) => api === 'Metadata').length > metadataBefore &&
          wire.timeline().filter(({ api, kind }) => api === 'Metadata' && kind === 'response')
            .length > responsesBefore
      )
      expect(applied).toHaveBeenCalledWith(
        {
          topic: TOPIC,
          partition: 0,
          leaderId: 1,
          leaderEpoch: 2,
          nodeEndpoints: [{ nodeId: 1, host: 'node1', port: 9093, rack: null }],
        },
        { fence: expect.any(Generation) }
      )
      expect(await applied.mock.results[0].value).toBe(true)
      await task
      await jest.advanceTimersByTimeAsync(1000)
      expect(wire.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      if (late.settle) late.release()
      await jest.advanceTimersByTimeAsync(2000)
      if (task) await task
      await client.disconnect()
      wire.assertClean()
      applied.mockRestore()
    }
  })

  it('runs a complete held-answer scenario through the public producer', () => {
    const produce = cluster.timeline().filter(({ api }) => api === 'Produce')
    expect(produce.map(({ kind }) => kind)).toEqual(['request', 'response'])
  })
})

describe('a plain request with different delivery deadlines', () => {
  const createSendMessages = require('../sendMessages')
  const createRetry = require('../../retry')
  const { createFakeCluster } = require('./fixtures/fakeCluster')
  const { newLogger } = require('testHelpers')
  let release, work
  beforeEach(() => jest.useFakeTimers({ now: 500 }))
  afterEach(async () => {
    release?.()
    await jest.advanceTimersByTimeAsync(2000)
    if (work) await work
    jest.useRealTimers()
    release = work = null
  })

  /** One plain owner with two independent creation timestamps; no helper supplies an expiry. */
  const setup = (produce, retries = 3, createdAt = [0, 500]) => {
    const cluster = createFakeCluster({
      partitions: 2,
      produce,
      options: {
        requestTimeout: 1000,
        enforceRequestTimeout: false,
        linger: 0,
        deliveryTimeout: 1000,
      },
    })
    const retry = { retries, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
    const sender = createSendMessages({
      logger: newLogger(),
      cluster,
      partitioner: () => 0,
      eosManager: {
        isTransactional: () => false,
        getProducerId: () => '-1',
        getProducerEpoch: () => 0,
      },
      retrier: createRetry(retry),
      retry,
      deliveryTimeout: 1000,
      getConnectionStatus: () => 'connected',
    })
    const settlers = [0, 1].map(() => ({ resolve: jest.fn(), reject: jest.fn() }))
    const batches = [0, 1].map((partition) => ({
      topic: TOPIC,
      partition,
      messages: [{ value: String(partition) }],
      createdAt: createdAt[partition],
      settlers: [settlers[partition]],
    }))
    let error
    work = sender
      .sendPartitioned({ acks: 1, timeout: 1000, compression: 0, batches })
      .catch((failure) => {
        error = failure
      })
    return { cluster, settlers, error: () => error }
  }
  const success = ({ topicData }) => ({
    topics: topicData.map(({ topic, partitions }) => ({
      topicName: topic,
      partitions: partitions.map(({ partition }) => ({
        partition,
        errorCode: 0,
        baseOffset: '0',
        logAppendTime: '-1',
        logStartOffset: '0',
      })),
    })),
  })

  it('settles each written batch at its own deadline while the common answer stays held', async () => {
    const harness = setup(async (request) => {
      const refused = request.guard?.(100)
      if (refused) throw refused
      await new Promise((resolve) => {
        release = resolve
      })
      return success(request)
    })
    await jest.advanceTimersByTimeAsync(0)
    expect(release).toBeDefined()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(500)
    expect(harness.settlers[0].reject).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'unknown' })
    )
    expect(harness.settlers[1].reject).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(500)
    expect(harness.settlers[1].reject).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'unknown' })
    )
    expect(harness.error()).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(jest.getTimerCount()).toBe(0)
    release()
    await jest.advanceTimersByTimeAsync(0)
    expect(harness.cluster.broker.produce).toHaveBeenCalledTimes(1)
    for (const settler of harness.settlers) {
      expect(settler.reject).toHaveBeenCalledTimes(1)
      expect(settler.resolve).not.toHaveBeenCalled()
    }
  })

  it('refuses the whole unwritten attempt and resubmits only the live batch without a retry', async () => {
    const writes = []
    let calls = 0
    const harness = setup(async (request) => {
      calls += 1
      if (calls === 1)
        await new Promise((resolve) => {
          release = resolve
        })
      const refused = request.guard?.(100)
      if (refused) throw refused
      writes.push(
        request.topicData.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
      )
      return success(request)
    }, 0)
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(500)
    expect(harness.settlers[0].reject).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'unsent' })
    )
    release()
    await jest.advanceTimersByTimeAsync(0)
    await work
    expect(writes).toEqual([[1]])
    expect(harness.settlers[1].resolve).toHaveBeenCalledTimes(1)
    expect(harness.settlers[1].reject).not.toHaveBeenCalled()
    expect(harness.cluster.broker.produce).toHaveBeenCalledTimes(2)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('expires all equal-deadline members in one callback', async () => {
    const harness = setup(
      async (request) => {
        const refused = request.guard?.(100)
        if (refused) throw refused
        await new Promise((resolve) => {
          release = resolve
        })
        return success(request)
      },
      3,
      [0, 0]
    )
    await jest.advanceTimersByTimeAsync(0)
    expect(release).toBeDefined()
    const timersAtWrite = jest.getTimerCount()
    jest.advanceTimersByTime(500)
    for (const settler of harness.settlers) {
      expect(settler.reject).toHaveBeenCalledTimes(1)
      expect(settler.reject).toHaveBeenCalledWith(expect.objectContaining({ outcome: 'unknown' }))
      expect(settler.resolve).not.toHaveBeenCalled()
    }
    expect(Date.now()).toBe(1000)
    expect(timersAtWrite).toBe(1)
    expect(jest.getTimerCount()).toBe(0)
    release()
    await jest.advanceTimersByTimeAsync(0)
    await work
    expect(harness.cluster.broker.produce).toHaveBeenCalledTimes(1)
    for (const settler of harness.settlers) expect(settler.reject).toHaveBeenCalledTimes(1)
  })
})
describe.each(['tv1', 'tv2'])('plain late failure identity profile %s', (profile) => {
  const Broker = require('../../broker')
  const Cluster = require('../../cluster')
  const StateManager = require('../state')
  const { failureSourceOf } = require('../../broker/failureSource')
  const { KafkaJSConnectionClosedError } = require('../../errors')
  const { newLogger } = require('testHelpers')

  /**
   * Runs ready continuations until a test-body invariant holds, without a wall-clock timeout.
   * @param {() => boolean} condition required state
   * @returns {Promise<void>} resolves after the invariant holds
   * @throws {Error} assertion failure when 200 event-loop turns do not reach the state
   */
  const pump = async (condition) => {
    for (let i = 0; i < 200 && !condition(); i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise(setImmediate)
    }
    expect(condition()).toBe(true)
  }

  beforeEach(() => jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  it.each([
    'current',
    'reconnect',
    'sibling',
    'replacement pool baseline',
    'replacement pool connected',
    'replacement broker',
  ])('forwards the original source and protects %s after public expiry', async (kind) => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Produce' })
    const wire = producerScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile,
        schedule,
      })
    )
    const client = new Kafka({
      ...wire.kafkaOptions(),
      enforceRequestTimeout: false,
      requestTimeout: 1000,
    }).producer({ idempotent: false, linger: 0, deliveryTimeout: 1000, retry: { retries: 0 } })
    let source, actual, owner, task, replacement, removed
    let releaseFailure
    const paused = new Promise((resolve) => {
      releaseFailure = resolve
    })
    const settlements = jest.fn()
    const fail = jest.spyOn(StateManager.prototype, 'fail')
    const produce = Broker.prototype.produce
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      try {
        return await produce.call(this, request)
      } catch (error) {
        actual = error
        source = failureSourceOf(error)
        await paused
        throw error
      }
    })
    const findBroker = Cluster.prototype.findBroker
    jest.spyOn(Cluster.prototype, 'findBroker').mockImplementation(function (...args) {
      owner = this
      return findBroker.apply(this, args)
    })
    const removeBroker = Cluster.prototype.removeBroker
    const remove = jest
      .spyOn(Cluster.prototype, 'removeBroker')
      .mockImplementation(function (args) {
        owner = this
        removeBroker.call(this, args)
        removed = {
          brokers: Object.values(this.brokerPool.brokers),
          seed: this.brokerPool.seedBroker,
          retired: source.broker.retired,
          generation: source.broker.connectionPool.generation,
          generationBase: source.broker.connectionPool.generationBase,
        }
      })
    try {
      await client.connect()
      task = client.send({ topic: TOPIC, messages: [{ value: 'late failure' }] }).then(
        (value) => settlements({ value }),
        (error) => settlements({ error })
      )
      await pump(() => gate.call !== null)
      await jest.advanceTimersByTimeAsync(1000)
      expect(settlements).toHaveBeenCalledTimes(1)
      const timeout = settlements.mock.calls[0][0].error
      expect(timeout).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
      owner.brokerPool.brokers[0].connectionPool.pool[0].socket.emit('end')
      gate.release()
      await pump(() => source !== undefined)
      expect(actual).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(source.broker.connectionPool.generation).toBe(source.generation)
      const old = source.broker
      const pool = old.connectionPool
      if (kind === 'reconnect') await old.connect()
      if (kind === 'sibling') await pool.pool[1].connect()
      if (kind.startsWith('replacement pool') || kind === 'replacement broker') {
        const ConnectionPool = require('../../network/connectionPool')
        const { connectionOpts } = require('testHelpers')
        const next = new ConnectionPool(
          connectionOpts({
            host: 'node0',
            port: 9092,
            socketFactory: wire.socketFactory,
            enforceRequestTimeout: false,
          })
        )
        if (kind === 'replacement broker') {
          replacement = new Broker({ connectionPool: next, logger: newLogger(), nodeId: 0 })
          await replacement.connect()
        } else {
          expect(old.replaceConnectionPool(next)).toBe(true)
          expect(next.generationBase).toBe(source.generation)
          if (kind === 'replacement pool connected') await old.connect()
        }
      }
      expect(owner).toBeDefined()
      const brokerPool = owner.brokerPool
      brokerPool.brokers.alias = old
      if (kind === 'replacement broker') {
        for (const key of Object.keys(brokerPool.brokers)) {
          if (brokerPool.brokers[key] === old) brokerPool.brokers[key] = replacement
        }
        if (brokerPool.seedBroker === old) brokerPool.seedBroker = replacement
      }
      releaseFailure()
      await pump(() => remove.mock.calls.length === 1)
      expect(remove).toHaveBeenCalledWith({ host: 'node0', port: 9092, source })
      expect(remove.mock.calls[0][0].source).toBe(source)
      expect(failureSourceOf(actual)).toBe(source)
      expect(fail).not.toHaveBeenCalled()
      expect(settlements).toHaveBeenCalledTimes(1)
      expect(settlements.mock.calls[0][0].error).toBe(timeout)
      if (kind === 'current') {
        expect(removed.brokers).not.toContain(old)
        expect(removed.seed).not.toBe(old)
        expect(removed.retired).toBe(true)
      } else {
        expect(removed.brokers).toContain(replacement || old)
        expect(removed.retired).toBe(false)
      }
      if (kind === 'replacement pool baseline') {
        expect(removed.generation).toBe(source.generation)
        expect(removed.generationBase).toBe(source.generation)
      }
      if (['reconnect', 'sibling', 'replacement pool connected'].includes(kind)) {
        expect(removed.generation).toBeGreaterThan(source.generation)
      }
      if (replacement) expect(replacement.retired).toBe(false)
      await pump(() => wire.requests().every(({ answeredOrder }) => answeredOrder !== null))
      await jest.advanceTimersByTimeAsync(2000)
      expect(wire.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
      await task
    } finally {
      releaseFailure()
      if (gate.settle) gate.release()
      await jest.advanceTimersByTimeAsync(2000)
      if (task) await task
      await client.disconnect()
      if (source) await source.broker.disconnect()
      if (replacement) await replacement.disconnect()
      wire.assertClean()
    }
  })
})
