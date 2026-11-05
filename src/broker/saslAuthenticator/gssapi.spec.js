const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')

const b64 = (value) => Buffer.from(value).toString('base64')
const marker = 'private-ticket-key-marker'
let native
let client
let errors
let loads

beforeEach(() => {
  jest.resetModules()
  errors = require('../../errors')
  client = {
    contextComplete: false,
    username: 'client@KAFKAJS.TEST',
    step: jest
      .fn()
      .mockResolvedValueOnce(b64('initial'))
      .mockImplementationOnce(async () => {
        client.contextComplete = true
        return ''
      }),
    unwrap: jest.fn().mockResolvedValue(Buffer.from([7, 0, 16, 0]).toString('base64')),
    wrap: jest.fn(async (challenge, options = {}) => {
      if (typeof options.protect !== 'boolean') {
        throw new TypeError('options.protect must be a boolean.')
      }
      return b64('wrapped')
    }),
  }
  native = {
    GSS_MECH_OID_KRB5: 'kerberos-oid',
    initializeClient: jest.fn().mockResolvedValue(client),
  }
  loads = 0
  jest.doMock('kerberos/package.json', () => ({ version: '7.0.0' }), { virtual: true })
  jest.doMock(
    'kerberos',
    () => {
      loads++
      return native
    },
    { virtual: true }
  )
})

afterEach(() => {
  jest.dontMock('kerberos')
  jest.dontMock('kerberos/package.json')
  jest.dontMock('./plain')
})

/** Builds a SASL_AUTHENTICATION_FAILED refusal whose message carries the marker.
 * It is built after the module reset, so its classes are the adapter's own.
 * @returns {Error} protocol error
 */
const protocolRefusal = () =>
  Object.assign(require('../../protocol/error').createErrorFromCode(58), { message: marker })

/** Creates a decoded scripted peer without sockets or credentials.
 * @param {object} [options] SASL options
 * @param {string} [host] service host
 * @returns {object} provider, exchange spy and captured mechanism bytes
 */
const scenario = (options = {}, host = 'localhost') => {
  const provider = require('./gssapi')
  const replies = [Buffer.from('mutual'), Buffer.from('security-offer'), Buffer.alloc(0)]
  const sent = []
  const saslAuthenticate = jest.fn(async ({ request, response }) => {
    sent.push(new Decoder(await request.encode()).readBytes())
    return response.parse(await response.decode(new Encoder().writeBytes(replies.shift()).buffer))
  })
  const auth = provider(options)({ host, port: 9192, logger: {}, saslAuthenticate })
  return { auth, sent, saslAuthenticate }
}

it('requires the optional package only on first authentication', async () => {
  const { auth } = scenario()
  expect(loads).toBe(0)
  expect(native.initializeClient).not.toHaveBeenCalled()
  await auth.authenticate()
  expect(loads).toBe(1)
  expect(native.initializeClient).toHaveBeenCalledTimes(1)
})

it('sends context tokens and a no-layer reply through the framed mechanism boundary', async () => {
  const { auth, sent } = scenario()
  await auth.authenticate()
  expect(native.initializeClient).toHaveBeenCalledWith('kafka@localhost', {
    principal: undefined,
    mechOID: 'kerberos-oid',
  })
  expect(client.step.mock.calls).toEqual([[''], [b64('mutual')]])
  expect(client.unwrap).toHaveBeenCalledWith(b64('security-offer'))
  expect(client.wrap).toHaveBeenCalledWith(b64(Buffer.from([1, 0, 0, 0])), {
    user: 'client@KAFKAJS.TEST',
    protect: false,
  })
  expect(sent).toEqual([Buffer.from('initial'), Buffer.alloc(0), Buffer.from('wrapped')])
})

it('sends no data when the native step has no output token, as the real binding answers', async () => {
  const { auth, sent } = scenario()
  client.step = jest
    .fn()
    .mockResolvedValueOnce(b64('initial'))
    .mockImplementationOnce(async () => {
      client.contextComplete = true
      return null
    })
  await auth.authenticate()
  expect(sent).toEqual([Buffer.from('initial'), Buffer.alloc(0), Buffer.from('wrapped')])
})

it('uses the configured service and principal without changing the connected hostname', async () => {
  const { auth } = scenario(
    { serviceName: 'custom', principal: 'named@KAFKAJS.TEST' },
    'broker.example'
  )
  await auth.authenticate()
  expect(native.initializeClient).toHaveBeenCalledWith('custom@broker.example', {
    principal: 'named@KAFKAJS.TEST',
    mechOID: 'kerberos-oid',
  })
  expect(client.wrap.mock.calls[0][1]).toEqual({ user: 'named@KAFKAJS.TEST', protect: false })
})

it('does not reuse context when one provider instance authenticates twice', async () => {
  native.initializeClient.mockImplementation(async () => ({
    ...client,
    contextComplete: true,
    step: jest.fn().mockResolvedValue(b64('initial')),
  }))
  const provider = require('./gssapi')
  const factory = provider({})
  const saslAuthenticate = jest.fn(async ({ response }) =>
    response.parse(await response.decode(new Encoder().writeBytes(Buffer.from('offer')).buffer))
  )
  const auth = factory({ host: 'localhost', port: 9192, saslAuthenticate })
  await auth.authenticate()
  await auth.authenticate()
  expect(native.initializeClient).toHaveBeenCalledTimes(2)
  expect(saslAuthenticate).toHaveBeenCalledTimes(4)
})

it('has no extra public export for loading or injecting the optional package', () => {
  const provider = require('./gssapi')
  expect(typeof provider).toBe('function')
  expect(Object.keys(provider)).toEqual([])
})

it('fails with an actionable typed error when the optional package is absent', async () => {
  const absent = Object.assign(new Error("Cannot find module 'kerberos'"), {
    code: 'MODULE_NOT_FOUND',
  })
  jest.doMock(
    'kerberos',
    () => {
      throw absent
    },
    { virtual: true }
  )
  const { auth } = scenario()
  await expect(auth.authenticate()).rejects.toBeInstanceOf(errors.KafkaJSNotImplemented)
  await expect(auth.authenticate()).rejects.toThrow('npm install kerberos@7.0.0')
})

it.each(['6.2.1', '7.0.1', undefined])(
  'refuses an unsupported optional peer version %p before loading native code',
  async (version) => {
    jest.doMock('kerberos/package.json', () => ({ version }), { virtual: true })
    const { auth } = scenario()
    await expect(auth.authenticate()).rejects.toBeInstanceOf(errors.KafkaJSNotImplemented)
    await expect(auth.authenticate()).rejects.toThrow('at version 7.0.0')
    expect(loads).toBe(0)
    expect(native.initializeClient).not.toHaveBeenCalled()
  }
)

it('sanitizes a broken native binary separately from a missing optional package', async () => {
  jest.doMock(
    'kerberos',
    () => {
      throw new Error(marker)
    },
    { virtual: true }
  )
  const { auth } = scenario()
  await expect(auth.authenticate()).rejects.toMatchObject({
    name: 'KafkaJSSASLAuthenticationError',
    message: 'SASL GSSAPI authentication failed: broker=localhost:9192 stage=load',
  })
})

it.each(['initialize', 'step', 'unwrap', 'wrap', 'exchange'])(
  'sanitizes failures at %s without a cause',
  async (stage) => {
    const { auth, saslAuthenticate } = scenario()
    const failure = Object.assign(new Error(marker), { cause: new Error(marker), ticket: marker })
    if (stage === 'initialize') native.initializeClient.mockRejectedValue(failure)
    else if (stage === 'exchange') saslAuthenticate.mockRejectedValue(failure)
    else client[stage].mockReset().mockRejectedValue(failure)
    const error = await auth.authenticate().catch((e) => e)
    expect(error).toBeInstanceOf(errors.KafkaJSSASLAuthenticationError)
    expect(error.message).toContain(`stage=${stage}`)
    expect(error.message).not.toContain(marker)
    expect(error.cause).toBeUndefined()
    expect(error.ticket).toBeUndefined()
  }
)

it.each([
  [1, 'exchange:1'],
  [2, 'exchange:2'],
  [3, 'exchange:final'],
])('fails the SaslAuthenticate call number %i as stage=%s', async (call, stage) => {
  const { auth, saslAuthenticate } = scenario()
  const original = saslAuthenticate.getMockImplementation()
  for (let earlier = 1; earlier < call; earlier++) saslAuthenticate.mockImplementationOnce(original)
  saslAuthenticate.mockRejectedValueOnce(new Error(marker))
  await expect(auth.authenticate()).rejects.toMatchObject({
    message: `SASL GSSAPI authentication failed: broker=localhost:9192 stage=${stage}`,
  })
  expect(saslAuthenticate).toHaveBeenCalledTimes(call)
})

it.each([
  ['TypeError', () => new TypeError(marker), ' cause=TypeError'],
  [
    'KafkaJSConnectionError',
    () => new errors.KafkaJSConnectionError(marker),
    ' cause=KafkaJSConnectionError',
  ],
  [
    'KafkaJSProtocolError with its type and code',
    () => protocolRefusal(),
    ' cause=KafkaJSProtocolError type=SASL_AUTHENTICATION_FAILED code=58',
  ],
])('names an exchange failure by class and not by text: %s', async (_, failure, cause) => {
  const { auth, saslAuthenticate } = scenario()
  saslAuthenticate.mockRejectedValueOnce(failure())
  const error = await auth.authenticate().catch((e) => e)
  expect(error.message).toBe(
    `SASL GSSAPI authentication failed: broker=localhost:9192 stage=exchange:1${cause}`
  )
  expect(error.message).not.toContain(marker)
  expect(error.cause).toBeUndefined()
})

it('gives a typed sanitized failure for an IP literal and accepts a canonical resolver result', async () => {
  native.initializeClient.mockRejectedValueOnce(new Error(marker))
  const { auth } = scenario({}, '127.0.0.1')
  await expect(auth.authenticate()).rejects.toMatchObject({
    message: 'SASL GSSAPI authentication failed: broker=127.0.0.1:9192 stage=initialize',
  })
  const brokers = async () => ['broker.example:9192']
  const [address] = await brokers()
  await scenario({}, address.split(':')[0]).auth.authenticate()
  expect(native.initializeClient.mock.calls.map((call) => call[0])).toEqual([
    'kafka@127.0.0.1',
    'kafka@broker.example',
  ])
})

it.each(['', null, 4, 'bad/name', 'bad@realm', 'bad\0name'])(
  'rejects invalid serviceName %p',
  async (serviceName) => {
    const { auth } = scenario({ serviceName })
    await expect(auth.authenticate()).rejects.toBeInstanceOf(
      errors.KafkaJSInvalidConfigurationError
    )
    expect(native.initializeClient).not.toHaveBeenCalled()
  }
)

it.each(['', null, 4, 'bad\0name'])('rejects invalid principal %p', async (principal) => {
  const { auth } = scenario({ principal })
  await expect(auth.authenticate()).rejects.toBeInstanceOf(errors.KafkaJSInvalidConfigurationError)
})

it.each([
  Buffer.alloc(0),
  Buffer.from([1, 0, 0]),
  Buffer.from([2, 0, 0, 0]),
  Buffer.from([1, 0, 0, 0, 0]),
])('refuses an invalid or unsupported layer offer %p', async (offer) => {
  const { auth } = scenario()
  client.unwrap.mockResolvedValue(offer.toString('base64'))
  await expect(auth.authenticate()).rejects.toBeInstanceOf(errors.KafkaJSSASLAuthenticationError)
  expect(client.wrap).not.toHaveBeenCalled()
})

it('retains custom mechanism precedence and never loads the native package', async () => {
  jest.doMock(
    'kerberos',
    () => {
      throw new Error('must stay lazy')
    },
    { virtual: true }
  )
  const SASLAuthenticator = require('./index')
  const authenticate = jest.fn().mockResolvedValue()
  const authenticationProvider = jest.fn(() => ({ authenticate }))
  const connection = {
    sasl: { mechanism: 'GSSAPI', authenticationProvider },
    host: 'localhost',
    port: 9192,
    send: jest.fn().mockResolvedValue({ enabledMechanisms: ['GSSAPI'] }),
  }
  const auth = new SASLAuthenticator(
    connection,
    { namespace: () => ({}) },
    {
      17: { minVersion: 1, maxVersion: 1 },
      36: { minVersion: 2, maxVersion: 2 },
    }
  )
  await auth.authenticate()
  expect(authenticationProvider).toHaveBeenCalledTimes(1)
  expect(authenticate).toHaveBeenCalledTimes(1)
})

it('registers the builtin mechanism and retains the original invocation guard', async () => {
  const SASLAuthenticator = require('./index')
  const stale = new Error('original invocation expired')
  let refused = false
  const sent = []
  const replies = ['mutual', 'offer', '']
  const guard = () => (refused ? stale : null)
  const connection = {
    sasl: { mechanism: 'gssapi' },
    host: 'localhost',
    port: 9192,
    send: jest.fn(async (request) => {
      expect(request.guard).toBe(guard)
      if (request.request.apiKey === 17) return { enabledMechanisms: ['GSSAPI'] }
      sent.push(request)
      return {
        authBytes: new Encoder().writeBytes(Buffer.from(replies.shift())).buffer,
        sessionLifetimeMs: '15000',
      }
    }),
  }
  const auth = new SASLAuthenticator(
    connection,
    { namespace: () => ({}) },
    {
      17: { minVersion: 1, maxVersion: 1 },
      36: { minVersion: 2, maxVersion: 2 },
    },
    guard
  )
  const original = client.unwrap
  client.unwrap = jest.fn(async (value) => {
    refused = true
    return original(value)
  })
  const refusal = await auth.authenticate().catch((error) => error)
  expect(refusal).toBeInstanceOf(errors.KafkaJSSASLAuthenticationError)
  expect(sent).toHaveLength(2)
  expect(auth.sessionLifetime).toBe('15000')
})

it('sanitizes a broker rejection before authentication-failure publication', async () => {
  const SASLAuthenticator = require('./index')
  const reportAuthenticationFailure = jest.fn()
  const connection = {
    sasl: { mechanism: 'gssapi' },
    host: 'localhost',
    port: 9192,
    reportAuthenticationFailure,
    send: jest.fn(async ({ request }) => {
      if (request.apiKey === 17) return { enabledMechanisms: ['GSSAPI'] }
      throw Object.assign(new Error(marker), { code: 58 })
    }),
  }
  const auth = new SASLAuthenticator(
    connection,
    { namespace: () => ({}) },
    {
      17: { minVersion: 1, maxVersion: 1 },
      36: { minVersion: 2, maxVersion: 2 },
    }
  )
  await expect(auth.authenticate()).rejects.toBeInstanceOf(errors.KafkaJSSASLAuthenticationError)
  expect(reportAuthenticationFailure).toHaveBeenCalledWith(
    'GSSAPI',
    'SASL GSSAPI authentication failed: broker=localhost:9192 stage=exchange'
  )
})

it('checks the independent KDC oracles before fixture teardown', () => {
  const verify = require('../../../testHelpers/kerberos/verify')
  const corpus = Array.from({ length: 20 }, (_, index) => `record-${index}`)
  const oracle = {
    type: 'gssapi-oracle',
    service: 'kafka@localhost',
    principal: 'client@KAFKAJS.TEST',
    layer: 1,
    maxBuffer: 0,
    firstContexts: 1,
    reconnected: 2,
    reauthenticated: 3,
    issuedAtMs: 1000,
    expiresAtMs: 61000,
    observedAtMs: 61001,
    klistStatus: 1,
    corpus,
    delivered: corpus,
    rawLog: corpus.map((value, index) => ({ offset: String(index), value })),
    committedOffset: '20',
    socketsBeforeReauth: 1,
    socketsAfterReauth: 1,
    wireVersions: { SaslHandshake: 1, SaslAuthenticate: 2 },
  }
  expect(() => verify([oracle])).not.toThrow()
  expect(() => verify([])).toThrow('exactly one')
  for (const change of [
    { observedAtMs: 61000 - 1 },
    { socketsAfterReauth: 2 },
    { committedOffset: '19' },
    { delivered: corpus.slice(1) },
    { rawLog: [] },
    { reauthenticated: 2 },
  ])
    expect(() => verify([{ ...oracle, ...change }])).toThrow('oracle mismatch')
})

it('awaits the final broker status after wrapping the selected layer', async () => {
  const { auth, saslAuthenticate } = scenario()
  const original = saslAuthenticate.getMockImplementation()
  let rejectFinal
  saslAuthenticate
    .mockImplementationOnce(original)
    .mockImplementationOnce(original)
    .mockImplementationOnce(
      () =>
        new Promise((resolve, reject) => {
          rejectFinal = reject
        })
    )
  let settled = false
  const result = auth.authenticate().then(
    () => {
      settled = true
    },
    (error) => {
      settled = true
      return error
    }
  )
  await new Promise((resolve) => setImmediate(resolve))
  expect(saslAuthenticate).toHaveBeenCalledTimes(3)
  expect(settled).toBe(false)
  rejectFinal(new Error(marker))
  expect(await result).toBeInstanceOf(errors.KafkaJSSASLAuthenticationError)
})

it('keeps PLAIN bound to its existing builtin provider', async () => {
  const authenticate = jest.fn().mockResolvedValue()
  jest.doMock('./plain', () => () => () => ({ authenticate }))
  const SASLAuthenticator = require('./index')
  const connection = {
    sasl: { mechanism: 'plain', username: 'fixture', password: 'fixture' },
    host: 'localhost',
    port: 9192,
    send: jest.fn().mockResolvedValue({ enabledMechanisms: ['PLAIN'] }),
  }
  await new SASLAuthenticator(
    connection,
    { namespace: () => ({}) },
    {
      17: { minVersion: 1, maxVersion: 1 },
      36: { minVersion: 2, maxVersion: 2 },
    }
  ).authenticate()
  expect(authenticate).toHaveBeenCalledTimes(1)
  expect(loads).toBe(0)
})
