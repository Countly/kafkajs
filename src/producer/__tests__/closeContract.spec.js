const createRetry = require('../../retry')
const Cluster = require('../../cluster')
const Generation = require('../generation')
const {
  bufferedProducer,
  deferred,
  succeeded,
  flushAll,
  watch,
} = require('./fixtures/bufferHarness')
const {
  KafkaJSConnectionError,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../../errors')
const { Kafka } = require('../../../index')
const { newLogger } = require('testHelpers')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

describe('producer close cancellation', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test('completed generation registrations are removed before close', () => {
    const generation = new Generation()
    const finished = jest.fn()
    const active = jest.fn()
    const unregister = generation.register(finished)
    unregister()
    unregister()
    generation.register(active)
    const error = new Error('closed')
    generation.close(error)
    generation.close(new Error('again'))
    expect(finished).not.toHaveBeenCalled()
    expect(active).toHaveBeenCalledExactlyOnceWith(error)
    const late = jest.fn()
    generation.register(late)()
    expect(late).toHaveBeenCalledExactlyOnceWith(error)
  })

  test('closing a generation clears the pending retry timer', async () => {
    const generation = new Generation()
    const closed = new Error('producer closed')
    const fn = jest.fn(async () => {
      throw Object.assign(new Error('retry'), { retriable: true })
    })
    const result = watch(
      createRetry({ retries: 10, initialRetryTime: 100, factor: 0, fence: generation })(fn)
    )
    try {
      await flushAll()
      expect(jest.getTimerCount()).toBe(1)
      generation.close(closed)
      await flushAll()
      expect(result).toMatchObject({ state: 'rejected', value: closed })
      expect(jest.getTimerCount()).toBe(0)
      await jest.advanceTimersByTimeAsync(100)
      expect(fn).toHaveBeenCalledTimes(1)
    } finally {
      generation.close(closed)
      await jest.runAllTimersAsync()
    }
  })

  test('closing a generation settles a held local retry caller before its answer', async () => {
    const generation = new Generation()
    const gate = deferred()
    const closed = new Error('producer closed')
    const result = watch(createRetry({ retries: 0, fence: generation })(async () => gate.promise))
    try {
      await flushAll()
      generation.close(closed)
      await flushAll()
      expect(result).toMatchObject({ state: 'rejected', value: closed })
    } finally {
      gate.resolve()
      await flushAll()
    }
  })

  test('completed close is shared until an explicit connect starts another lifecycle', async () => {
    const { producer, cluster } = bufferedProducer({ idempotent: false })
    await producer.connect()
    await Promise.all([producer.disconnect(), producer.disconnect()])
    await producer.disconnect({ timeout: 0 })
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    await producer.connect()
    const sent = producer.send({ topic: 'topic', messages: [{ value: 'new lifecycle' }] })
    await flushAll()
    await sent
    await producer.disconnect()
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })
})

describe('forced producer close on the wire', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test.each([true, false])(
    'a written send ends unknown before its held answer, idempotent %s',
    async (idempotent) => {
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'Produce' })
      const cluster = producerScript(
        createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], schedule })
      )
      const producer = new Kafka({
        ...cluster.kafkaOptions(),
        requestTimeout: 1000,
        enforceRequestTimeout: false,
      }).producer({ idempotent, linger: 0, deliveryTimeout: 1000 })
      let sent, closed
      try {
        await producer.connect()
        sent = watch(
          producer.send({ topic: TOPIC, messages: [{ value: 'close', timestamp: 1000 }] })
        )
        for (let i = 0; i < 200 && !gate.call; i += 1) {
          await jest.advanceTimersByTimeAsync(1)
          await new Promise((resolve) => setImmediate(resolve))
        }
        expect(gate.call).not.toBeNull()
        closed = watch(producer.disconnect({ timeout: 10 }))
        await jest.advanceTimersByTimeAsync(10)
        await flushAll()
        expect(closed.state).toBe('fulfilled')
        expect(sent).toMatchObject({
          state: 'rejected',
          value: { name: 'KafkaJSProducerClosedError', outcome: 'unknown' },
        })
        const frames = cluster.requests().length
        gate.release()
        await jest.advanceTimersByTimeAsync(2000)
        await flushAll()
        expect(cluster.requests()).toHaveLength(frames)
        expect(sent.value.outcome).toBe('unknown')
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        if (gate.settle) gate.release()
        await jest.advanceTimersByTimeAsync(2000)
        await flushAll()
        await producer.disconnect()
        cluster.assertClean()
      }
    }
  )
})

describe('producer close contract', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  /** Lets queued work and `ms` of fake time run. */
  const pump = async (ms = 0) => {
    await jest.advanceTimersByTimeAsync(ms)
    await flushAll()
  }

  /** A producer whose first Produce answer waits for `held.resolve()`, with its send pending. */
  const writing = async (options = {}) => {
    const fixture = bufferedProducer({ idempotent: false, ...options })
    const held = deferred()
    fixture.cluster.broker.produce.mockImplementationOnce(async (request) => {
      const refusal = request.guard?.(100)
      if (refusal) throw refusal
      await held.promise
      return succeeded(request)
    })
    await fixture.producer.connect()
    const send = watch(fixture.producer.send({ topic: 'topic', messages: [{ value: 'a' }] }))
    await pump()
    return { ...fixture, send, held }
  }

  test('two sequential disconnect() calls after an unknown commit send one EndTxn in total', async () => {
    const { producer, cluster } = bufferedProducer({
      transactionalId: 'close-contract',
      idempotent: true,
      deliveryTimeout: 120000,
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
    })
    const { broker } = cluster
    Object.assign(broker, {
      finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
    })
    broker.produce.mockImplementation(async (request) => succeeded(request))
    broker.endTxn.mockRejectedValue(new KafkaJSConnectionError('Connection error'))
    await producer.connect()
    const transaction = await producer.transaction()
    const sent = transaction.send({ topic: 'topic', messages: [{ value: 'a' }] })
    await pump()
    await sent
    const commit = watch(transaction.commit())
    await pump(10)
    expect(commit.state).toBe('rejected')
    broker.endTxn.mockClear()
    broker.endTxn.mockResolvedValue({ producerId: '1000', producerEpoch: 1 })

    await producer.disconnect()
    await producer.disconnect()

    expect(broker.endTxn).toHaveBeenCalledTimes(1)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
  })

  test.each([-1, 0.5, NaN, Infinity, 2147483648, '10', true, null, () => {}])(
    'a timeout of %p is refused with a rejection before the close changes anything',
    async (timeout) => {
      const { producer, cluster } = bufferedProducer({ idempotent: false })
      await producer.connect()
      let returned
      expect(() => {
        returned = producer.disconnect({ timeout })
      }).not.toThrow()
      const refused = watch(returned)
      await pump()

      expect(refused.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(cluster.disconnect).not.toHaveBeenCalled()
      const sent = producer.send({ topic: 'topic', messages: [{ value: 'still connected' }] })
      await pump()
      await sent
      await producer.disconnect()
      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    }
  )

  test.each(['SIGTERM', 5, true, null, { type: 'event' }, {}])(
    'an argument of %p is the no-argument close',
    async (argument) => {
      const { producer, cluster } = bufferedProducer({ idempotent: false })
      await producer.connect()

      await producer.disconnect(argument)

      expect(cluster.disconnect).toHaveBeenCalledExactlyOnceWith()
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  test.each([0, 2147483647])(
    'a timeout of %p is accepted and its timer is cleared',
    async (timeout) => {
      const { producer, cluster } = bufferedProducer({ idempotent: false })
      await producer.connect()

      await producer.disconnect({ timeout })

      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  test('concurrent closes share one episode, and the first timeout governs', async () => {
    const { producer, cluster, send, held } = await writing({ linger: 0, deliveryTimeout: 60000 })
    try {
      const first = producer.disconnect({ timeout: 10 })
      const second = producer.disconnect({ timeout: 0 })
      const third = watch(producer.disconnect({ timeout: 1000000 }))
      await pump()
      expect(second).toBe(first)
      expect(send.state).toBe('pending')
      expect(third.state).toBe('pending')

      await pump(10)
      await first
      expect(send.value).toMatchObject({ name: 'KafkaJSProducerClosedError', outcome: 'unknown' })
      expect(third.state).toBe('fulfilled')
      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    } finally {
      held.resolve()
      await producer.disconnect({ timeout: 0 })
    }
  })

  test('a send while a close waits for a held answer, and one after it ended, are refused untouched', async () => {
    const { producer, cluster, send, held } = await writing({ linger: 0, deliveryTimeout: 60000 })
    const produced = cluster.broker.produce.mock.calls.length
    const closed = watch(producer.disconnect())
    await pump()
    const timers = jest.getTimerCount()

    await expect(producer.send({ topic: 'topic', messages: [{ value: 'b' }] })).rejects.toThrow(
      /producer is disconnecting/
    )
    expect(jest.getTimerCount()).toBe(timers)
    held.resolve()
    await pump()
    expect(closed.state).toBe('fulfilled')
    expect(send.state).toBe('fulfilled')
    await expect(producer.send({ topic: 'topic', messages: [{ value: 'c' }] })).rejects.toThrow(
      /producer is disconnected/
    )
    expect(cluster.broker.produce).toHaveBeenCalledTimes(produced)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a held InitProducerId is abandoned at the close and the producer connects cleanly later', async () => {
    const { producer, cluster } = bufferedProducer({ idempotent: true })
    const { broker } = cluster
    const held = deferred()
    broker.initProducerId.mockImplementationOnce(async () => held.promise)
    broker.initProducerId.mockResolvedValue({ producerId: '2000', producerEpoch: 0 })
    broker.produce.mockImplementation(async (request) => succeeded(request))
    const connecting = watch(producer.connect())
    await pump()
    expect(connecting.state).toBe('pending')

    await producer.disconnect({ timeout: 10 })
    await pump()
    expect(connecting.value).toMatchObject({ name: 'KafkaJSProducerClosedError' })
    held.resolve({ producerId: '1000', producerEpoch: 9 })
    await pump()

    await producer.connect()
    const sent = producer.send({ topic: 'topic', messages: [{ value: 'a' }] })
    await pump()
    await sent
    expect(broker.produce.mock.calls.at(-1)[0]).toMatchObject({
      producerId: '2000',
      producerEpoch: 0,
    })
    await producer.disconnect()
  })
})

describe('Generation and the fenced retry', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test('wait() settles with the operation, or with the close, and unregisters either way', async () => {
    const generation = new Generation()
    await expect(generation.wait(Promise.resolve(7))).resolves.toBe(7)
    await expect(generation.wait(Promise.reject(new Error('failed')))).rejects.toThrow('failed')
    const gate = deferred()
    const waiting = watch(generation.wait(gate.promise))
    const closed = new Error('closed')
    generation.close(closed)
    await flushAll()
    expect(waiting).toMatchObject({ state: 'rejected', value: closed })
    gate.resolve(1)
    await flushAll()
    expect(waiting.value).toBe(closed)
    expect(() => generation.check()).toThrow(closed)
    expect(generation.guard()).toBe(closed)
    expect(generation.error).toBe(closed)
  })

  test('a closed fence refuses before calling, and a late result is ignored', async () => {
    const generation = new Generation()
    const closed = new Error('closed')
    generation.close(closed)
    const fn = jest.fn(async () => 'late')
    await expect(createRetry({ retries: 3, fence: generation })(fn)).rejects.toBe(closed)
    expect(fn).not.toHaveBeenCalled()

    const open = new Generation()
    const gate = deferred()
    const late = jest.fn(async () => gate.promise)
    const result = watch(createRetry({ retries: 3, fence: open })(late))
    await flushAll()
    open.close(closed)
    gate.resolve('late')
    await flushAll()
    expect(result).toMatchObject({ state: 'rejected', value: closed })
  })

  test('a retry that ends on its own unregisters from the fence', async () => {
    const generation = new Generation()
    const register = jest.spyOn(generation, 'register')
    await expect(createRetry({ retries: 0, fence: generation })(async () => 'done')).resolves.toBe(
      'done'
    )
    const cancel = register.mock.calls.at(-1)[0]
    generation.close(new Error('later'))
    expect(cancel).toBeInstanceOf(Function)
    await flushAll()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a non-retriable failure ends a fenced retry after one attempt, wrapped with its cause', async () => {
    const generation = new Generation()
    const failure = Object.assign(new Error('rejected for good'), { retriable: false })
    const fn = jest.fn(async () => {
      throw failure
    })

    const error = await createRetry({ retries: 3, fence: generation })(fn).catch((thrown) => thrown)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('rejected for good')
    expect(error.cause).toBe(failure)
    expect(fn).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('a closed lifecycle opens nothing on the wire', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  const NODES = [{ id: 0, host: 'node0', port: 9092 }]
  const closed = new KafkaJSConnectionError('the producer was closed')

  /** A producer cluster on a scripted wire with its metadata loaded, and the wire's schedule. */
  const loaded = async () => {
    const schedule = new Schedule()
    const wire = producerScript(createScriptedCluster({ nodes: NODES, schedule }), { topic: TOPIC })
    const { clientId, brokers, socketFactory } = wire.kafkaOptions()
    const cluster = new Cluster({
      logger: newLogger(),
      clientId,
      brokers,
      socketFactory,
      connectionTimeout: 1000,
      requestTimeout: 5000,
      metadataMaxAge: 1000,
      metadataMaxIdle: 300000,
      allowAutoTopicCreation: false,
      retry: { retries: 3, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
    })
    await cluster.connect()
    await cluster.addMultipleTargetTopics([TOPIC])
    return { schedule, wire, cluster, seed: cluster.brokerPool.seedBroker }
  }

  const apis = (wire) => wire.requests().map(({ api }) => api)

  test.each([
    ['an unconditional', (cluster, fence) => cluster.refreshMetadata({ fence })],
    ['a conditional', (cluster, fence) => cluster.refreshMetadataIfNecessary({ fence })],
  ])(
    '%s metadata refresh held at the seed reconnect writes no further frame',
    async (_, refresh) => {
      const { schedule, wire, cluster, seed } = await loaded()
      await seed.disconnect()
      await jest.advanceTimersByTimeAsync(2000)
      const generation = new Generation()
      const gate = schedule.hold({ api: 'ApiVersions' })
      const refreshing = watch(refresh(cluster, generation))
      await gate.arrived()
      const frames = wire.requests().length
      const sockets = wire.sockets().length

      generation.close(closed)
      gate.release()
      await jest.advanceTimersByTimeAsync(1000)
      await flushAll()

      expect(refreshing).toMatchObject({ state: 'rejected', value: closed })
      expect(wire.requests()).toHaveLength(frames)
      expect(wire.sockets()).toHaveLength(sockets)
      expect(apis(wire).filter((api) => api === 'Metadata')).toHaveLength(1)
      wire.assertClean()
      await cluster.disconnect({ drainTimeout: 0 })
    }
  )

  test('a Metadata request on a closed lifecycle opens no socket and writes no frame', async () => {
    const { wire, cluster, seed } = await loaded()
    await seed.disconnect()
    const frames = wire.requests().length
    const sockets = wire.sockets().length

    await expect(seed.metadata([TOPIC], { guard: () => closed })).rejects.toBe(closed)
    await jest.advanceTimersByTimeAsync(1000)

    expect(wire.requests()).toHaveLength(frames)
    expect(wire.sockets()).toHaveLength(sockets)
    await cluster.disconnect({ drainTimeout: 0 })
  })

  test('a Metadata request on a connected broker whose lifecycle closes before the write writes nothing', async () => {
    const { wire, cluster, seed } = await loaded()
    const frames = wire.requests().length
    let calls = 0
    const guard = () => (++calls > 1 ? closed : null)

    await expect(seed.metadata([TOPIC], { guard })).rejects.toBe(closed)

    expect(wire.requests()).toHaveLength(frames)
    await cluster.disconnect({ drainTimeout: 0 })
  })

  test('a coordinator lookup on a closed lifecycle asks no broker', async () => {
    const { wire, cluster } = await loaded()
    const generation = new Generation()
    generation.close(closed)
    const frames = wire.requests().length

    await expect(
      cluster.findGroupCoordinator({ groupId: 'group', fence: generation })
    ).rejects.toBe(closed)
    await expect(cluster.findConnectedBroker({ fence: generation })).rejects.toBe(closed)
    await expect(cluster.addMultipleTargetTopics(['other'], { fence: generation })).rejects.toBe(
      closed
    )

    expect(wire.requests()).toHaveLength(frames)
    expect(cluster.targetTopics.has('other')).toBe(false)
    await cluster.disconnect({ drainTimeout: 0 })
  })

  test('a forced close leaves a retrying plain send no ApiVersions, Metadata or socket afterwards', async () => {
    const schedule = new Schedule()
    const wire = producerScript(createScriptedCluster({ nodes: NODES, schedule }), { topic: TOPIC })
    wire.answer('Produce', ({ topics }) => ({
      responses: topics.map(({ partitions: entries, ...key }) => ({
        ...key,
        partitions: entries.map(({ partition }) => ({
          partition,
          errorCode: 6,
          baseOffset: '-1',
          logAppendTime: '-1',
          logStartOffset: '-1',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    }))
    const producer = new Kafka({
      ...wire.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({
      idempotent: false,
      linger: 0,
      deliveryTimeout: 60000,
      retry: { retries: 10, initialRetryTime: 100, maxRetryTime: 100, factor: 0 },
    })
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: 'retry me' }] }))
    for (let i = 0; i < 200 && apis(wire).filter((api) => api === 'Produce').length < 2; i += 1) {
      await jest.advanceTimersByTimeAsync(10)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(apis(wire).filter((api) => api === 'Produce').length).toBeGreaterThanOrEqual(2)

    await producer.disconnect({ timeout: 0 })
    expect(sent).toMatchObject({ state: 'rejected', value: { outcome: 'unknown' } })
    const frames = wire.requests().length
    const sockets = wire.sockets().length
    await jest.advanceTimersByTimeAsync(10000)
    await new Promise((resolve) => setImmediate(resolve))

    expect(wire.requests()).toHaveLength(frames)
    expect(wire.sockets()).toHaveLength(sockets)
    expect(jest.getTimerCount()).toBe(0)
    wire.assertClean()
  })
})
