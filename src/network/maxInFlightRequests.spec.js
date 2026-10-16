const RequestQueue = require('./requestQueue')
const { newLogger } = require('testHelpers')
const { KafkaJSNonRetriableError } = require('../errors')

const createQueue = (maxInFlightRequests) =>
  new RequestQueue({
    maxInFlightRequests,
    requestTimeout: 30000,
    enforceRequestTimeout: false,
    clientId: 'KafkaJS',
    broker: 'localhost:9092',
    logger: newLogger(),
  })

// SocketRequest#completed calls entry.resolve() (socketRequest.js:126) and #rejected calls
// entry.reject() (:151), so the entry must carry both — the existing
// src/network/requestQueue/index.spec.js:14-18 helper does exactly this.
const pushRequest = (queue, correlationId) => {
  const sendRequest = jest.fn()
  queue.push({
    entry: {
      correlationId,
      apiKey: 0,
      apiName: 'Produce',
      apiVersion: 7,
      resolve: jest.fn(),
      reject: jest.fn(),
    },
    expectResponse: true,
    sendRequest,
  })
  return sendRequest
}

describe('Network > maxInFlightRequests', () => {
  afterEach(() => jest.clearAllTimers())

  it('queues the sixth request when the limit is 5', () => {
    const queue = createQueue(5)
    const sends = [1, 2, 3, 4, 5, 6].map((id) => pushRequest(queue, id))

    expect(sends.slice(0, 5).every((s) => s.mock.calls.length === 1)).toBe(true)
    expect(sends[5]).not.toHaveBeenCalled()
    expect(queue.pending).toHaveLength(1)

    queue.fulfillRequest({ correlationId: 1, payload: null, size: 0 })
    expect(sends[5]).toHaveBeenCalled()
    queue.destroy()
  })

  it('keeps every request in flight when the limit is null', () => {
    const queue = createQueue(null)
    const sends = [1, 2, 3, 4, 5, 6].map((id) => pushRequest(queue, id))

    expect(sends.every((s) => s.mock.calls.length === 1)).toBe(true)
    expect(queue.pending).toHaveLength(0)
    queue.destroy()
  })
})

describe('Network > Connection > maxInFlightRequests default', () => {
  const Connection = require('./connection')

  const createConnection = (overrides) =>
    new Connection({
      host: 'localhost',
      port: 9092,
      logger: newLogger(),
      socketFactory: () => ({}),
      requestTimeout: 30000,
      connectionTimeout: 1000,
      ...overrides,
    })

  it('defaults to 5 (Java parity)', () => {
    expect(createConnection().requestQueue.maxInFlightRequests).toEqual(5)
  })

  it('treats an explicit null as unbounded, not as "use the default"', () => {
    expect(
      createConnection({ maxInFlightRequests: null }).requestQueue.maxInFlightRequests
    ).toBeNull()
  })

  it('honours an explicit number', () => {
    expect(createConnection({ maxInFlightRequests: 1 }).requestQueue.maxInFlightRequests).toEqual(1)
  })
})

describe('Kafka > client option validation', () => {
  const Kafka = require('../index')
  const create = (maxInFlightRequests) =>
    new Kafka({ clientId: 'v', brokers: ['localhost:9092'], logLevel: 0 }).producer({
      maxInFlightRequests,
    })

  it('rejects 0, negatives and fractions up front instead of deadlocking', () => {
    expect(() => create(0)).toThrow(/maxInFlightRequests must be null/)
    expect(() => create(-1)).toThrow(/maxInFlightRequests must be null/)
    expect(() => create(1.5)).toThrow(/maxInFlightRequests must be null/)
  })

  it('accepts null (unbounded) and positive integers', () => {
    expect(() => create(null)).not.toThrow()
    expect(() => create(1)).not.toThrow()
  })

  const createKafka = (options) =>
    new Kafka({ clientId: 'v', brokers: ['localhost:9092'], logLevel: 0, ...options })

  it('rejects a metadataRecoveryRebootstrapTriggerMs of 0, negative or fractional', () => {
    expect(() => createKafka({ metadataRecoveryRebootstrapTriggerMs: 0 })).toThrow(
      /metadataRecoveryRebootstrapTriggerMs must be an integer >= 1/
    )
    expect(() => createKafka({ metadataRecoveryRebootstrapTriggerMs: -1 })).toThrow(
      /metadataRecoveryRebootstrapTriggerMs must be an integer >= 1/
    )
    expect(() => createKafka({ metadataRecoveryRebootstrapTriggerMs: 1.5 })).toThrow(
      /metadataRecoveryRebootstrapTriggerMs must be an integer >= 1/
    )
  })

  it('rejects an unrecognised metadataRecoveryStrategy', () => {
    expect(() => createKafka({ metadataRecoveryStrategy: 'nope' })).toThrow(
      /metadataRecoveryStrategy must be one of/
    )
  })

  it.each(['metadataRecoveryStrategy', 'metadataRecoveryRebootstrapTriggerMs'])(
    '%s: refuses a Symbol with KafkaJSNonRetriableError, and quotes a string value',
    (option) => {
      expect(() => createKafka({ [option]: Symbol('x') })).toThrow(KafkaJSNonRetriableError)
      expect(() => createKafka({ [option]: Symbol('x') })).toThrow(/got Symbol\(x\)/)
      expect(() => createKafka({ [option]: 'x' })).toThrow(/got 'x'/)
    }
  )
})
