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

describe('Network > the request queue and a request timeout', () => {
  afterEach(() => jest.clearAllTimers())

  const createTimingQueue = (options = {}) =>
    new RequestQueue({
      maxInFlightRequests: 1,
      requestTimeout: 30000,
      enforceRequestTimeout: false,
      clientId: 'KafkaJS',
      broker: 'localhost:9092',
      logger: newLogger(),
      ...options,
    })

  it('writes no pending request when the request in flight times out', () => {
    const queue = createTimingQueue()
    const first = pushRequest(queue, 1)
    const second = pushRequest(queue, 2)

    queue.inflight.get(1).timeoutRequest()

    expect(first).toHaveBeenCalledTimes(1)
    expect(second).not.toHaveBeenCalled()
    expect(queue.pending).toHaveLength(1)
    expect(queue.inflight.size).toBe(0)
    queue.destroy()
  })

  it('tells its owner after the request was rejected with its own timeout error, and before the event', () => {
    const order = []
    const instrumentationEmitter = { emit: jest.fn((name) => order.push(name)) }
    const request = jest.fn()
    const onRequestTimeout = jest.fn((socketRequest) => {
      order.push(['owner', socketRequest.correlationId, request.mock.calls.length])
    })
    const queue = createTimingQueue({ onRequestTimeout, instrumentationEmitter })
    const reject = jest.fn(() => order.push('rejected'))
    queue.push({
      entry: {
        correlationId: 7,
        apiKey: 0,
        apiName: 'Produce',
        apiVersion: 7,
        resolve: jest.fn(),
        reject,
      },
      expectResponse: true,
      sendRequest: request,
    })

    queue.inflight.get(7).timeoutRequest()

    expect(reject).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'KafkaJSRequestTimeoutError' })
    )
    expect(order).toEqual([
      'rejected',
      ['owner', 7, 1],
      'network.request_queue_size',
      'network.request_timeout',
    ])
    queue.destroy()
  })

  it('rejects every in-flight and pending request without telling observers, then tells them once', () => {
    const instrumentationEmitter = { emit: jest.fn() }
    const queue = createTimingQueue({ maxInFlightRequests: 1, instrumentationEmitter })
    pushRequest(queue, 1)
    pushRequest(queue, 2)
    instrumentationEmitter.emit.mockClear()
    const error = new Error('closed')

    queue.settleAll(error)

    expect(queue.inflight.size).toBe(0)
    expect(queue.pending).toHaveLength(0)
    expect(instrumentationEmitter.emit).not.toHaveBeenCalled()

    queue.emitQueueState()

    expect(instrumentationEmitter.emit).toHaveBeenCalledWith('network.request_queue_size', {
      broker: 'localhost:9092',
      clientId: 'KafkaJS',
      queueSize: 0,
    })
    queue.destroy()
  })

  it('refuses a request whose guard returns an error, writing nothing and taking no slot', () => {
    const queue = createTimingQueue()
    const refusal = new Error('lost')
    const reject = jest.fn()
    const sendRequest = jest.fn()
    queue.push({
      entry: {
        correlationId: 1,
        apiKey: 0,
        apiName: 'Produce',
        apiVersion: 7,
        resolve: jest.fn(),
        reject,
      },
      expectResponse: true,
      guard: () => refusal,
      sendRequest,
    })

    expect(reject).toHaveBeenCalledWith(refusal)
    expect(sendRequest).not.toHaveBeenCalled()
    expect(queue.inflight.size).toBe(0)

    const next = pushRequest(queue, 2)
    expect(next).toHaveBeenCalledTimes(1)
    queue.destroy()
  })

  it('refuses a pending request whose guard throws with what it threw, without breaking the answer that freed its place', () => {
    const queue = createTimingQueue()
    pushRequest(queue, 1)
    const thrown = new Error('guard failed')
    const reject = jest.fn()
    queue.push({
      entry: {
        correlationId: 2,
        apiKey: 0,
        apiName: 'Produce',
        apiVersion: 7,
        resolve: jest.fn(),
        reject,
      },
      expectResponse: true,
      guard: () => {
        throw thrown
      },
      sendRequest: jest.fn(),
    })
    const third = pushRequest(queue, 3)

    expect(() => queue.fulfillRequest({ correlationId: 1, payload: null, size: 0 })).not.toThrow()

    expect(reject).toHaveBeenCalledWith(thrown)
    expect(third).toHaveBeenCalledTimes(1)
    queue.destroy()
  })

  it('passes over a pending request its guard refuses, and writes the one behind it', () => {
    const queue = createTimingQueue()
    pushRequest(queue, 1)
    const refusal = new Error('lost')
    const reject = jest.fn()
    queue.push({
      entry: {
        correlationId: 2,
        apiKey: 0,
        apiName: 'Produce',
        apiVersion: 7,
        resolve: jest.fn(),
        reject,
      },
      expectResponse: true,
      guard: () => refusal,
      sendRequest: jest.fn(),
    })
    const third = pushRequest(queue, 3)
    expect(queue.pending).toHaveLength(2)

    queue.fulfillRequest({ correlationId: 1, payload: null, size: 0 })

    expect(reject).toHaveBeenCalledWith(refusal)
    expect(third).toHaveBeenCalledTimes(1)
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
