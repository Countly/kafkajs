const createAdmin = require('..')
const Broker = require('../../broker')
const { createCluster } = require('testHelpers')
const { lookup } = require('../../protocol/requests')
const wire = require('../../../testHelpers/wire/unregisterControllerV0')
const {
  KafkaJSTimeout,
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../errors')

const SEND_REQUEST = Object.getOwnPropertySymbols(Broker.prototype).find((symbol) =>
  String(symbol).includes('sendRequest')
)
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const CAPABLE = { 94: { minVersion: 0, maxVersion: 0 } }
const OK = wire.buildResponse()

const held = () => {
  let resolve
  const promise = new Promise((yes) => {
    resolve = yes
  })
  return { promise, resolve }
}
const observe = (promise) => {
  const result = { state: 'pending' }
  promise.then(
    (value) => Object.assign(result, { state: 'fulfilled', value }),
    (error) => Object.assign(result, { state: 'rejected', error })
  )
  return result
}

/**
 * A real Broker over negotiated `versions` whose transport records each encoded request and
 * answers from `answers`: a response body, an Error to throw, or a promise of either.
 * @param {number} nodeId broker id
 * @param {object} versions broker ranges by api key
 * @param {(Buffer|Error|Promise)[]} [answers] consumed in send order
 * @returns {Broker} the broker, with its sends recorded in `sent`
 */
const brokerOver = (nodeId, versions, answers = []) => {
  const broker = Object.create(Broker.prototype)
  Object.assign(broker, { nodeId, versions, sent: [], lookupRequest: lookup(versions) })
  broker[SEND_REQUEST] = async ({ request, response }) => {
    broker.sent.push({
      apiKey: request.apiKey,
      version: request.apiVersion,
      body: (await request.encode()).buffer.toString('hex'),
    })
    const answer = await answers.shift()
    if (answer instanceof Error) throw answer
    return response.parse(await response.decode(answer))
  }
  return broker
}

/**
 * A public admin over a real Cluster whose pool holds the given brokers.
 * @param {Broker[]} brokers known brokers
 * @param {object} [options] admin options
 * @returns {{admin: object, cluster: object}} the admin and its cluster
 */
const fixture = (brokers, options = {}) => {
  const cluster = createCluster()
  cluster.brokerPool.brokers = Object.fromEntries(brokers.map((b) => [b.nodeId, b]))
  cluster.brokerPool.connectBroker = jest.fn(async () => {})
  cluster.refreshMetadataIfNecessary = jest.fn(async () => {})
  cluster.connect = jest.fn(async () => {})
  cluster.disconnect = jest.fn(async () => {})
  return { cluster, admin: createAdmin({ logger, cluster, retry, ...options }) }
}

describe('Admin > unregisterController', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('unregisters a controller on a capable broker', async () => {
    const broker = brokerOver(1, CAPABLE, [OK])
    const { admin } = fixture([broker])
    await expect(admin.unregisterController({ controllerId: 7 })).resolves.toBeUndefined()
    expect(broker.sent).toEqual([{ apiKey: 94, version: 0, body: '0000000700' }])
  })

  test('preserves code 136 as a non-retriable protocol error', async () => {
    const broker = brokerOver(1, CAPABLE, [wire.buildResponse({ errorCode: 136 }), OK])
    const { admin } = fixture([broker])
    const error = await admin.unregisterController({ controllerId: 7 }).catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({
      type: 'CONTROLLER_ID_NOT_REGISTERED',
      code: 136,
      retriable: false,
    })
    expect(broker.sent).toHaveLength(1)
  })

  test('preserves cluster authorization refusal', async () => {
    const broker = brokerOver(1, CAPABLE, [wire.buildResponse({ errorCode: 31 }), OK])
    const { admin } = fixture([broker])
    const error = await admin.unregisterController({ controllerId: 7 }).catch((e) => e)
    expect(error).toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
      code: 31,
      retriable: false,
    })
    expect(broker.sent).toHaveLength(1)
  })

  test('retries a retriable code and then succeeds', async () => {
    const broker = brokerOver(1, CAPABLE, [wire.buildResponse({ errorCode: 7 }), OK])
    const { admin } = fixture([broker])
    const call = observe(admin.unregisterController({ controllerId: 7 }))
    await jest.advanceTimersByTimeAsync(0)
    expect(broker.sent).toHaveLength(1)
    expect(call.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)
    expect(call).toEqual({ state: 'fulfilled', value: undefined })
    expect(broker.sent).toHaveLength(2)
  })

  test('preserves null empty and custom broker error messages', async () => {
    const outcomes = []
    for (const errorMessage of [null, '', 'unknown controller 7']) {
      const broker = brokerOver(1, CAPABLE, [wire.buildResponse({ errorCode: 136, errorMessage })])
      const { admin } = fixture([broker])
      const error = await admin.unregisterController({ controllerId: 7 }).catch((e) => e)
      outcomes.push({
        brokerErrorMessage: error.brokerErrorMessage,
        code: error.code,
        type: error.type,
        retriable: error.retriable,
        custom: error.message.includes('unknown controller 7'),
      })
    }
    const base = { code: 136, type: 'CONTROLLER_ID_NOT_REGISTERED', retriable: false }
    expect(outcomes).toEqual([
      { ...base, brokerErrorMessage: null, custom: false },
      { ...base, brokerErrorMessage: '', custom: false },
      { ...base, brokerErrorMessage: 'unknown controller 7', custom: true },
    ])
  })

  test('skips an incapable first broker and sends to a capable second broker', async () => {
    const first = brokerOver(1, {})
    const second = brokerOver(2, CAPABLE, [OK])
    const { admin, cluster } = fixture([first, second])
    await expect(admin.unregisterController({ controllerId: 7 })).resolves.toBeUndefined()
    expect(first.sent).toEqual([])
    expect(second.sent).toEqual([{ apiKey: 94, version: 0, body: '0000000700' }])
    expect(cluster.brokerPool.connectBroker).toHaveBeenCalledTimes(2)
  })

  test('throws unsupported API only after every broker lacks key 94', async () => {
    const brokers = [brokerOver(1, {}), brokerOver(2, { 94: { minVersion: 1, maxVersion: 1 } })]
    const { admin, cluster } = fixture(brokers)
    const error = await admin.unregisterController({ controllerId: 7 }).catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ apiKey: 94, apiName: 'UnregisterController' })
    expect(cluster.brokerPool.connectBroker).toHaveBeenCalledTimes(2)
    expect(brokers.flatMap((b) => b.sent)).toEqual([])
  })

  test('rejects invalid controller ids before discovery or sending', async () => {
    const broker = brokerOver(1, CAPABLE, [OK, OK])
    const { admin, cluster } = fixture([broker])
    const invalid = [undefined, null, '7', NaN, Infinity, -1, 1.5, 2147483648, true, {}]
    const outcomes = []
    for (const controllerId of invalid)
      outcomes.push(await admin.unregisterController({ controllerId }).catch((e) => e.name))
    outcomes.push(await admin.unregisterController().catch((e) => e.name))
    outcomes.push(await admin.unregisterController(null).catch((e) => e.name))
    expect(outcomes).toEqual(Array(invalid.length + 2).fill('KafkaJSInvalidConfigurationError'))
    expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
    expect(cluster.brokerPool.connectBroker).not.toHaveBeenCalled()
    expect(broker.sent).toEqual([])

    await admin.unregisterController({ controllerId: 0 })
    await admin.unregisterController({ controllerId: 2147483647 })
    expect(broker.sent.map((sent) => sent.body)).toEqual(['0000000000', '7fffffff00'])
  })

  test('disconnect and API deadline retire an outstanding unregister call', async () => {
    const lateDeadline = held()
    const timedOut = brokerOver(1, CAPABLE, [lateDeadline.promise])
    const bounded = fixture([timedOut], { apiTimeout: 10 })
    const call = observe(bounded.admin.unregisterController({ controllerId: 7 }))
    await jest.advanceTimersByTimeAsync(9)
    expect(call.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error).toBeInstanceOf(KafkaJSTimeout)
    lateDeadline.resolve(wire.buildResponse({ errorCode: 7 }))
    await jest.advanceTimersByTimeAsync(100)
    expect(timedOut.sent).toHaveLength(1)

    const oldAnswer = held()
    const newAnswer = held()
    const broker = brokerOver(1, CAPABLE, [oldAnswer.promise, newAnswer.promise])
    const { admin } = fixture([broker])
    const first = observe(admin.unregisterController({ controllerId: 7 }))
    await jest.advanceTimersByTimeAsync(0)
    const close = observe(admin.disconnect({ timeout: 10 }))
    await jest.advanceTimersByTimeAsync(10)
    expect(close.state).toBe('fulfilled')
    expect(first.error).toBeInstanceOf(KafkaJSNonRetriableError)

    await admin.connect()
    const second = observe(admin.unregisterController({ controllerId: 9 }))
    await jest.advanceTimersByTimeAsync(0)
    oldAnswer.resolve(OK)
    await jest.advanceTimersByTimeAsync(100)
    expect(second.state).toBe('pending')
    expect(first.state).toBe('rejected')
    newAnswer.resolve(OK)
    await jest.advanceTimersByTimeAsync(0)
    expect(second).toEqual({ state: 'fulfilled', value: undefined })
    expect(broker.sent.map((sent) => sent.body)).toEqual(['0000000700', '0000000900'])
  })
})
