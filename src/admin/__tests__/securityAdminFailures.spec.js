const createAdmin = require('..')
const Broker = require('../../broker')
const Connection = require('../../network/connection')
const { lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const { attachResponseSource, responseSourceOf } = require('../../broker/failureSource')
const { currentDeadline } = require('../deadline')
const {
  KafkaJSProtocolError,
  KafkaJSInvariantViolation,
  KafkaJSTimeout,
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const methods = [
  ['alterClientQuotas', apiKeys.AlterClientQuotas],
  ['describeUserScramCredentials', apiKeys.DescribeUserScramCredentials],
  ['alterUserScramCredentials', apiKeys.AlterUserScramCredentials],
]
const sources = [
  ['ApiVersions code 31', 31, apiKeys.ApiVersions],
  ['ApiVersions code 15', 15, apiKeys.ApiVersions],
  ['SaslHandshake code 33', 33, apiKeys.SaslHandshake],
]
const entity = (entityName) => [{ entityType: 'user', entityName }]
const input = (method) => {
  if (method === 'alterClientQuotas')
    return {
      entries: ['a', 'b'].map((user) => ({
        entity: entity(user),
        ops: [{ key: 'producer_byte_rate', value: 1 }],
      })),
    }
  if (method === 'describeUserScramCredentials') return { users: ['a', 'b'] }
  return { deletions: ['a', 'b'].map((user) => ({ user, mechanism: 'SCRAM-SHA-256' })) }
}
const identities = (method, fields) => {
  if (method === 'alterClientQuotas')
    return fields.entries.map(({ entity }) => entity[0].entityName)
  if (method === 'describeUserScramCredentials') return fields.users.map(({ name }) => name)
  return fields.deletions.map(({ name }) => name)
}
const response = (method, users, first = false) => {
  const rows = users
    .slice()
    .reverse()
    .map((user) => ({
      ...(method === 'alterClientQuotas' ? { entity: entity(user) } : { user }),
      errorCode: first && user === 'b' ? 7 : 0,
      errorMessage: null,
      ...(method === 'describeUserScramCredentials' ? { credentialInfos: [] } : {}),
    }))
  return method === 'alterClientQuotas' ? { entries: rows } : { results: rows }
}

/** Runs real Broker renegotiation and response parsers after a pool generation change.
 * The fake connection notes the answered API exactly where Connection.send does.
 * @param {number} target security request key
 * @param {number} code setup protocol error
 * @param {number} source setup request key
 * @returns {object} broker, sent keys, original errors and pool send spy
 */
const setupBroker = (target, code, source) => {
  const table = Object.fromEntries([
    [apiKeys.ApiVersions, { minVersion: 0, maxVersion: 4 }],
    [target, { minVersion: 0, maxVersion: 0 }],
    [apiKeys.SaslHandshake, { minVersion: 0, maxVersion: 1 }],
    [apiKeys.SaslAuthenticate, { minVersion: 0, maxVersion: 2 }],
  ])
  const sent = []
  const thrown = []
  const attempt = {
    status: 'active',
    assertCurrent() {},
    refusal: () => null,
    guard: (work) => work,
    fail() {},
  }
  const connection = {
    setupAttempt: attempt,
    sasl:
      source === apiKeys.SaslHandshake
        ? { mechanism: 'plain', username: 'u', password: 'p' }
        : null,
    versions: table,
    logger,
    preparing: 0,
    connectAttempt: 1,
    socket: {},
    authEpisode: null,
    isConnected: () => true,
    isAuthenticated: () => false,
    connect: async () => {},
    finishSetup() {},
    markReady() {},
    authenticate(options) {
      return Connection.prototype.authenticate.call(this, options)
    },
    send: async ({ request, response }) => {
      sent.push(request.apiKey)
      try {
        return await response.parse(
          request.apiKey === apiKeys.ApiVersions
            ? {
                errorCode: source === apiKeys.ApiVersions ? code : 0,
                apiVersions: Object.entries(table).map(([apiKey, range]) => ({
                  apiKey: Number(apiKey),
                  ...range,
                })),
              }
            : { errorCode: code, enabledMechanisms: ['SCRAM-SHA-512'] }
        )
      } catch (error) {
        attachResponseSource(error, request.apiKey)
        thrown.push(error)
        throw error
      }
    },
  }
  const pool = {
    host: 'node1',
    port: 9092,
    generation: 2,
    sasl: Boolean(connection.sasl),
    setupPolicy: { timeoutCeiling: 1000, waitCeiling: 100 },
    pool: [connection],
    isConnected: () => true,
    isAuthenticated: () => false,
    authenticationBlackoutEntry: () => null,
    beginSetup: () => attempt,
    setVersions() {},
    throwIfRetired() {},
    getConnectionByRequest: async () => connection,
    send: jest.fn(),
  }
  const broker = new Broker({ connectionPool: pool, logger, nodeId: 1, versions: table })
  broker.lookupRequest = lookup(table)
  broker.versionsGeneration = 1
  return { broker, sent, thrown, send: pool.send }
}

/** Builds a public wrapped call with one completed peer and one pending peer.
 * @param {string} method public operation
 * @param {object} [policy] retry policy
 * @returns {object} admin, route spy, healthy broker, selected identities and deadline tokens
 */
const fixture = (method, policy = retry) => {
  const requests = []
  const scopes = []
  const healthy = {
    [method]: jest.fn(async (fields) => {
      const users = identities(method, fields)
      requests.push(users)
      scopes.push(currentDeadline())
      return response(method, users, requests.length === 1)
    }),
  }
  const route = jest.fn(async () => {
    scopes.push(currentDeadline())
    return healthy
  })
  const cluster = {
    findConnectedBroker: route,
    findControllerBroker: route,
    refreshMetadata: async () => {
      scopes.push(currentDeadline())
    },
  }
  return {
    admin: createAdmin({ logger, cluster, retry: policy }),
    route,
    healthy,
    requests,
    scopes,
  }
}

const expectScope = (scopes) => {
  expect(scopes[0]).toBeDefined()
  for (const scope of scopes) expect(scope).toBe(scopes[0])
}
const expectResults = (method, rows, codes) => {
  expect(
    rows.map((row) => (method === 'alterClientQuotas' ? row.entity[0].entityName : row.user))
  ).toEqual(['a', 'b'])
  expect(rows.map(({ errorCode }) => errorCode)).toEqual(codes)
  expect(rows[0].error).toBeNull()
}

describe.each(methods)('%s retained outcomes', (method, key) => {
  test.each(sources)(
    'setup %s retains completed peers on exhaustion',
    async (_label, code, source) => {
      const f = fixture(method)
      const setup = setupBroker(key, code, source)
      const pending = jest.spyOn(setup.broker, method)
      f.route.mockResolvedValueOnce(f.healthy).mockResolvedValueOnce(setup.broker)
      const outcome = await f.admin[method](input(method)).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      expect(f.requests).toEqual([['a', 'b']])
      expect(pending.mock.calls.map(([fields]) => identities(method, fields))).toEqual([['b']])
      expect(setup.sent).toEqual(source === apiKeys.ApiVersions ? [18] : [18, 17])
      expect(setup.send).not.toHaveBeenCalled()
      expect(setup.thrown).toHaveLength(1)
      expect(setup.thrown[0]).toBeInstanceOf(KafkaJSProtocolError)
      expect(responseSourceOf(setup.thrown[0])).toBe(source)
      expect(outcome.error).toBeUndefined()
      expectResults(method, outcome.value, [0, -1])
      expect(outcome.value[1].error).toBe(setup.thrown[0])
      expectScope(f.scopes)
    }
  )

  test.each(sources)(
    'wrapped route %s retains the exact local wrapper',
    async (_label, code, source) => {
      const original = createErrorFromCode(code)
      attachResponseSource(original, source)
      const local = new KafkaJSNumberOfRetriesExceeded(original, { retryCount: 1, retryTime: 1 })
      const f = fixture(method)
      f.route.mockResolvedValueOnce(f.healthy).mockRejectedValueOnce(local)
      const outcome = await f.admin[method](input(method)).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      expect(outcome.error).toBeUndefined()
      const rows = outcome.value
      expectResults(method, rows, [0, -1])
      expect(rows[1].error).toBe(local)
      expect(rows[1].error.cause).toBe(original)
      expect(f.requests).toEqual([['a', 'b']])
      expect(f.route).toHaveBeenCalledTimes(2)
      expectScope(f.scopes)
    }
  )

  test('retriable setup recovers with only pending identities and one deadline', async () => {
    const f = fixture(method, { ...retry, retries: 2 })
    const setup = setupBroker(key, 15, apiKeys.ApiVersions)
    const pending = jest.spyOn(setup.broker, method)
    f.route.mockResolvedValueOnce(f.healthy).mockResolvedValueOnce(setup.broker)
    const rows = await f.admin[method](input(method))
    expectResults(method, rows, [0, 0])
    expect(f.requests).toEqual([['a', 'b'], ['b']])
    expect(pending.mock.calls.map(([fields]) => identities(method, fields))).toEqual([['b']])
    expect(setup.send).not.toHaveBeenCalled()
    expect(setup.sent).toEqual([18])
    expect(setup.thrown[0].code).toBe(15)
    expectScope(f.scopes)
  })

  test.each([sources[0], sources[2]])(
    'nonretriable setup %s ends locally and a later call can recover',
    async (_label, code, source) => {
      const f = fixture(method, { ...retry, retries: 2 })
      const setup = setupBroker(key, code, source)
      f.route.mockResolvedValueOnce(f.healthy).mockResolvedValueOnce(setup.broker)
      const outcome = await f.admin[method](input(method)).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      expect(outcome.error).toBeUndefined()
      expectResults(method, outcome.value, [0, -1])
      expect(outcome.value[1].error).toBe(setup.thrown[0])
      expect(setup.thrown[0].retriable).toBe(false)
      expect(f.route).toHaveBeenCalledTimes(2)
      expect(setup.send).not.toHaveBeenCalled()
      const selected = input(method)
      if (method === 'alterClientQuotas') selected.entries.shift()
      else if (method === 'describeUserScramCredentials') selected.users.shift()
      else selected.deletions.shift()
      const recovered = await f.admin[method](selected)
      expect(recovered.map(({ errorCode }) => errorCode)).toEqual([0])
      expect(f.requests).toEqual([['a', 'b'], ['b']])
    }
  )

  test('a wrapped retriable route failure recovers without resending completed work', async () => {
    const f = fixture(method, { ...retry, retries: 2 })
    const original = createErrorFromCode(15)
    attachResponseSource(original, apiKeys.ApiVersions)
    const local = new KafkaJSError(original, { cause: original, retriable: true })
    f.route.mockResolvedValueOnce(f.healthy).mockRejectedValueOnce(local)
    const rows = await f.admin[method](input(method))
    expectResults(method, rows, [0, 0])
    expect(f.requests).toEqual([['a', 'b'], ['b']])
    expectScope(f.scopes)
  })

  test('an invariant refusal after peer completion rejects the call', async () => {
    const f = fixture(method)
    const original = new KafkaJSInvariantViolation('invalid result identity')
    f.route.mockResolvedValueOnce(f.healthy).mockRejectedValueOnce(original)
    await expect(f.admin[method](input(method))).rejects.toBe(original)
    expect(f.requests).toEqual([['a', 'b']])
  })

  test('a stalled retry consumes the original deadline after peer completion', async () => {
    jest.useFakeTimers()
    try {
      const f = fixture(method)
      let scope
      f.route.mockResolvedValueOnce(f.healthy).mockImplementationOnce(() => {
        scope = currentDeadline()
        return new Promise(() => {})
      })
      const outcome = f.admin[method]({ ...input(method), apiTimeout: 30 }).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      await jest.advanceTimersByTimeAsync(30)
      const result = await outcome
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)
      expect(result.error).toBe(scope.refusal())
      expect(scope).toBe(f.scopes[0])
      expect(f.requests).toEqual([['a', 'b']])
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.useRealTimers()
    }
  })

  test.each([
    [
      'capability',
      () => new KafkaJSServerDoesNotSupportApiKey('unsupported', { apiKey: key, apiName: method }),
    ],
    ['validation', () => new KafkaJSInvalidConfigurationError('invalid options')],
  ])('%s refusal after peer completion still rejects', async (_label, makeError) => {
    const f = fixture(method)
    const original = makeError()
    f.route.mockResolvedValueOnce(f.healthy).mockRejectedValueOnce(original)
    await expect(f.admin[method](input(method))).rejects.toBe(original)
    expect(f.requests).toEqual([['a', 'b']])
    expect(f.route).toHaveBeenCalledTimes(2)
  })
})

test('a genuine SCRAM describe top-level response still rejects after peer completion', async () => {
  const method = 'describeUserScramCredentials'
  const f = fixture(method)
  const parser = require('../../protocol/requests/describeUserScramCredentials/v0/response')
  let original
  const broker = {
    [method]: jest.fn(async () => {
      try {
        return await parser.parse({ errorCode: 31, errorMessage: null, results: [] })
      } catch (error) {
        attachResponseSource(error, apiKeys.DescribeUserScramCredentials)
        original = error
        throw error
      }
    }),
  }
  f.route.mockResolvedValueOnce(f.healthy).mockResolvedValueOnce(broker)
  const outcome = await f.admin[method](input(method)).then(
    (value) => ({ value }),
    (error) => ({ error })
  )
  expect(outcome.error).toBeInstanceOf(KafkaJSProtocolError)
  expect(outcome.error).toBe(original)
  expect(original.code).toBe(31)
  expect(responseSourceOf(original)).toBe(apiKeys.DescribeUserScramCredentials)
  expect(identities(method, broker[method].mock.calls[0][0])).toEqual(['b'])
  expect(f.requests).toEqual([['a', 'b']])
})
