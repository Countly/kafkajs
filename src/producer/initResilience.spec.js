const createProducer = require('./index')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../instrumentation/emitter')

// Everything src/producer/sendMessages.js touches on the cluster, in call order:
//   :59  refreshMetadataIfNecessary   :62  findTopicPartitionMetadata   :67  targetTopics
//   :81  findLeaderForPartitions      :90  findBroker                   :124 topicId
//   :187 addMultipleTargetTopics      :195 removeBroker                 :221 isConnected
//   :231 connect                      :236,:258 refreshMetadata
// A fake missing any of these fails with "… is not a function" *after* the fix as well as
// before, so the test could never go green.
const createFakeCluster = () => ({
  connect: jest.fn(async () => {}),
  disconnect: jest.fn(async () => {}),
  refreshMetadata: jest.fn(async () => {}),
  refreshMetadataIfNecessary: jest.fn(async () => {}),
  findControllerBroker: jest.fn(),
  addTargetTopic: jest.fn(async () => {}),
  addMultipleTargetTopics: jest.fn(async () => {}),
  targetTopics: new Set(['t']),
  findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 0, leader: 0 }]),
  findLeaderForPartitions: jest.fn(() => ({ 0: [0] })),
  findBroker: jest.fn(),
  // No topic here has a known id, so every send stays on the pre-topic-id (name-keyed) path.
  topicId: jest.fn(() => undefined),
  removeBroker: jest.fn(),
  isConnected: () => true,
  // retries: 0 so the eosManager's own retrier (built from cluster.retry) does not swallow the
  // single transient InitProducerId failure by retrying it internally — that would make
  // connect() resolve instead of reject and the test would never reach the poisoned state.
  retry: { retries: 0, initialRetryTime: 1 },
})

describe('Producer > idempotent init resilience', () => {
  // The real defect (Countly 2g): connect() fails InitProducerId once, the caller catches it and
  // keeps producing, and every later send() goes out with no producer id until the process
  // restarts. Driving connect() twice does NOT reproduce it — src/producer/index.js:223-225
  // already re-attempts init on a second connect().
  it('retries InitProducerId on the next send() after a failed connect()', async () => {
    const cluster = createFakeCluster()
    let initAttempts = 0
    const produce = jest.fn(async () => ({ topics: [] }))

    const broker = {
      nodeId: 0,
      initProducerId: async () => {
        initAttempts += 1
        if (initAttempts === 1) {
          throw Object.assign(new Error('transient'), {
            type: 'GROUP_LOAD_IN_PROGRESS',
            code: 14,
            retriable: true,
          })
        }
        return { producerId: '1', producerEpoch: 0 }
      },
      produce,
    }
    cluster.findControllerBroker.mockImplementation(async () => broker)
    cluster.findBroker.mockImplementation(async () => broker)

    const instrumentationEmitter = new InstrumentationEventEmitter()
    const initializedEvents = []

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      // retries: 1 (not 0) so construction doesn't trip the "idempotent producer must allow
      // retries" guard in src/producer/index.js; this only governs send()'s own retrier, not
      // eosManager's InitProducerId retries (those come from cluster.retry above).
      retry: { retries: 1 },
      instrumentationEmitter,
    })
    producer.on(producer.events.INITIALIZED, () => initializedEvents.push('initialized'))

    await expect(producer.connect()).rejects.toThrow()
    expect(initAttempts).toBe(1)
    // The failed connect() must not have announced readiness.
    expect(initializedEvents).toEqual([])

    // The producer must not stay poisoned: the next send re-initializes and succeeds.
    await expect(
      producer.send({ topic: 't', messages: [{ key: 'k', value: 'v' }] })
    ).resolves.toBeDefined()

    expect(initAttempts).toBe(2)
    // The lazy re-init on send() is the readiness signal here — INITIALIZED must fire exactly
    // once, from the recovery path, since connect() never got there.
    expect(initializedEvents).toEqual(['initialized'])
    // The recovered send must genuinely carry the producer id InitProducerId just handed back,
    // not merely prove initProducerId was called again.
    expect(produce).toHaveBeenCalledWith(expect.objectContaining({ producerId: '1' }))
    expect(cluster.addMultipleTargetTopics).toHaveBeenCalledWith(['t'])
  })

  it('emits INITIALIZED after InitProducerId succeeds, without moving CONNECT', async () => {
    const cluster = createFakeCluster()
    cluster.findControllerBroker.mockResolvedValue({
      initProducerId: async () => ({ producerId: '1', producerEpoch: 0 }),
    })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const seen = []

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      instrumentationEmitter,
    })
    producer.on(producer.events.CONNECT, () => seen.push('connect'))
    producer.on(producer.events.INITIALIZED, () => seen.push('initialized'))

    await producer.connect()
    await new Promise((resolve) => setImmediate(resolve))

    // CONNECT still fires first, where it always has; INITIALIZED is the new readiness signal.
    expect(seen).toEqual(['connect', 'initialized'])
  })

  it('does not emit INITIALIZED when InitProducerId fails', async () => {
    const cluster = createFakeCluster()
    cluster.findControllerBroker.mockResolvedValue({
      initProducerId: async () => {
        throw Object.assign(new Error('nope'), { type: 'GROUP_LOAD_IN_PROGRESS', code: 14 })
      },
    })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const seen = []

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      // retries: 1 (not 0) so construction doesn't trip the "idempotent producer must allow
      // retries" guard; see the first test above for why.
      retry: { retries: 1 },
      instrumentationEmitter,
    })
    producer.on(producer.events.INITIALIZED, () => seen.push('initialized'))

    await expect(producer.connect()).rejects.toThrow()
    await new Promise((resolve) => setImmediate(resolve))
    expect(seen).toEqual([])
  })

  it('does not emit INITIALIZED for a non-idempotent producer', async () => {
    const cluster = createFakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const seen = []

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      instrumentationEmitter,
    })
    producer.on(producer.events.CONNECT, () => seen.push('connect'))
    producer.on(producer.events.INITIALIZED, () => seen.push('initialized'))

    await producer.connect()
    await new Promise((resolve) => setImmediate(resolve))

    // Nothing to initialize for a non-idempotent producer, so INITIALIZED never fires; CONNECT
    // alone means "ready to send()" for this producer.
    expect(seen).toEqual(['connect'])
    expect(cluster.findControllerBroker).not.toHaveBeenCalled()
  })

  // F1: connect() and send()/sendBatch() must share the same deduped InitProducerId call so an
  // un-awaited connect() racing a concurrent send() cannot issue two InitProducerId requests
  // (which would allocate two producer ids and reset sequences out from under an in-flight send).
  it('shares a single InitProducerId request between an un-awaited connect() and a concurrent send()', async () => {
    const cluster = createFakeCluster()
    let initAttempts = 0
    let resolveInit
    const produce = jest.fn(async () => ({ topics: [] }))

    const broker = {
      nodeId: 0,
      initProducerId: () =>
        new Promise((resolve) => {
          initAttempts += 1
          resolveInit = () => resolve({ producerId: '1', producerEpoch: 0 })
        }),
      produce,
    }
    cluster.findControllerBroker.mockImplementation(async () => broker)
    cluster.findBroker.mockImplementation(async () => broker)

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      retry: { retries: 1 },
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    const connectPromise = producer.connect()
    // Let connect() run up to (and including) the InitProducerId call before send() starts.
    await new Promise((resolve) => setImmediate(resolve))
    expect(initAttempts).toBe(1)

    const sendPromise = producer.send({ topic: 't', messages: [{ key: 'k', value: 'v' }] })

    // send() must not have started a second InitProducerId request while one is already in flight.
    expect(initAttempts).toBe(1)
    resolveInit()

    await expect(connectPromise).resolves.toBeUndefined()
    await expect(sendPromise).resolves.toBeDefined()
    expect(initAttempts).toBe(1)
  })

  // F4: the same dedup must hold between two concurrent send()/sendBatch() calls recovering from
  // a poisoned producer, not just between connect() and send().
  it('shares a single lazy InitProducerId request between two concurrent send() calls', async () => {
    const cluster = createFakeCluster()
    let initAttempts = 0
    let resolveSecondInit
    const produce = jest.fn(async () => ({ topics: [] }))

    const broker = {
      nodeId: 0,
      initProducerId: () => {
        initAttempts += 1
        if (initAttempts === 1) {
          return Promise.reject(
            Object.assign(new Error('transient'), {
              type: 'GROUP_LOAD_IN_PROGRESS',
              code: 14,
              retriable: true,
            })
          )
        }
        return new Promise((resolve) => {
          resolveSecondInit = () => resolve({ producerId: '1', producerEpoch: 0 })
        })
      },
      produce,
    }
    cluster.findControllerBroker.mockImplementation(async () => broker)
    cluster.findBroker.mockImplementation(async () => broker)

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      retry: { retries: 1 },
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    await expect(producer.connect()).rejects.toThrow()
    expect(initAttempts).toBe(1)

    const firstSend = producer.send({ topic: 't', messages: [{ key: 'k', value: 'v' }] })
    await new Promise((resolve) => setImmediate(resolve))
    expect(initAttempts).toBe(2)

    const secondSend = producer.send({ topic: 't', messages: [{ key: 'k2', value: 'v2' }] })
    // The second send() must share the first send()'s in-flight InitProducerId request instead
    // of starting its own third one.
    expect(initAttempts).toBe(2)

    resolveSecondInit()
    await expect(firstSend).resolves.toBeDefined()
    await expect(secondSend).resolves.toBeDefined()
    expect(initAttempts).toBe(2)
  })
})
