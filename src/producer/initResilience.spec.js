const createProducer = require('./index')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../instrumentation/emitter')

// Everything src/producer/sendMessages.js touches on the cluster, in call order:
//   :26  refreshMetadataIfNecessary   :29  findTopicPartitionMetadata   :34  targetTopics
//   :48  findLeaderForPartitions      :57  findBroker                   :129 addMultipleTargetTopics
//   :137 removeBroker                 :140 isConnected                  :145 connect
//   :146,:160 refreshMetadata
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

    const producer = createProducer({
      cluster,
      logger: newLogger(),
      idempotent: true,
      // retries: 1 (not 0) so construction doesn't trip the "idempotent producer must allow
      // retries" guard in src/producer/index.js; this only governs send()'s own retrier, not
      // eosManager's InitProducerId retries (those come from cluster.retry above).
      retry: { retries: 1 },
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    await expect(producer.connect()).rejects.toThrow()
    expect(initAttempts).toBe(1)

    // The producer must not stay poisoned: the next send re-initializes and succeeds.
    await expect(
      producer.send({ topic: 't', messages: [{ key: 'k', value: 'v' }] })
    ).resolves.toBeDefined()

    expect(initAttempts).toBe(2)
    expect(produce).toHaveBeenCalled()
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
})
