const { inspect } = require('node:util')
const createAdmin = require('..')
const { Kafka, logLevel } = require('../../..')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const literals = require('../../../testHelpers/wire/expected').securityAdmin
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const hmac = Buffer.from('hmac-marker-only-test')
const principal = { principalType: 'User', principalName: 'test-owner' }
const cases = [
  ['CreateDelegationToken', 1],
  ['CreateDelegationToken', 2],
  ['CreateDelegationToken', 3],
  ['RenewDelegationToken', 1],
  ['RenewDelegationToken', 2],
  ['ExpireDelegationToken', 1],
  ['ExpireDelegationToken', 2],
  ['DescribeDelegationToken', 1],
  ['DescribeDelegationToken', 2],
  ['DescribeDelegationToken', 3],
]
const call = (admin, api) =>
  admin[api[0].toLowerCase() + api.slice(1)](
    api === 'CreateDelegationToken'
      ? { renewers: [principal] }
      : api === 'DescribeDelegationToken'
        ? {}
        : { hmac }
  )

describe.each(['0', '1'])('token diagnostics buffers %s', (buffers) => {
  let previous
  beforeEach(() => {
    previous = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
  })
  afterEach(() => {
    if (previous === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = previous
  })

  test.each(cases)('%s v%s returns credentials only to its caller', async (api, version) => {
    const entries = []
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'securityAdmin',
      advertise: { [api]: [version, version] },
    })
    const admin = new Kafka(
      scripted.kafkaOptions({
        retry: { retries: 0 },
        logLevel: logLevel.DEBUG,
        logCreator: () => (entry) => entries.push(entry),
      })
    ).admin()
    try {
      await admin.connect()
      const result = await call(admin, api)
      if (api === 'CreateDelegationToken')
        expect(result).toMatchObject({
          hmac,
          renewers: [principal],
          owner: principal,
          issueTimestampMs: '9007199254740993',
        })
      else if (api === 'DescribeDelegationToken')
        expect(result[0]).toMatchObject({
          hmac,
          renewers: [principal],
          owner: principal,
          issueTimestampMs: '9007199254740993',
        })
      else expect(result).toEqual({ expiryTimestampMs: '9007199254740993' })
      const debug = entries.filter(
        ({ log }) => log.message.startsWith('Response ' + api + '(') && Object.hasOwn(log, 'data')
      )
      expect(debug).toHaveLength(1)
      expect(debug[0].log.data).toBe('[filtered]')
      const text = inspect(entries, { depth: null, maxArrayLength: null })
      for (const marker of [hmac.toString(), hmac.toString('hex'), hmac.toString('base64')])
        expect(text).not.toContain(marker)
      expect(scripted.requests().filter((r) => r.api === api)).toHaveLength(1)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  test.each(cases)(
    '%s v%s failure payload is filtered before logger arguments',
    async (api, version) => {
      const entries = []
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: { [api]: [version, version] },
      }).answer(api, () => ({ errorCode: 61 }))
      const admin = new Kafka(
        scripted.kafkaOptions({
          retry: { retries: 0 },
          logLevel: logLevel.DEBUG,
          logCreator: () => (entry) => entries.push(entry),
        })
      ).admin()
      try {
        await admin.connect()
        await expect(call(admin, api)).rejects.toMatchObject({
          code: 61,
          type: 'DELEGATION_TOKEN_AUTH_DISABLED',
        })
        const debug = entries.filter(
          ({ log }) =>
            log.message.startsWith('Response ' + api + '(') && Object.hasOwn(log, 'payload')
        )
        expect(debug).toHaveLength(1)
        expect(debug[0].log.payload).toBe('[filtered]')
        expect(debug[0].log.error).toBe('Sensitive response failed')
        expect(inspect(entries, { depth: null, maxArrayLength: null })).not.toContain(
          hmac.toString()
        )
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  test.each(cases.filter(([, version]) => version >= 2))(
    '%s v%s malformed payload carries no marker to a logger',
    async (api, version) => {
      const entries = []
      const pairName = api[0].toLowerCase() + api.slice(1) + 'V' + version
      const bytes = Buffer.concat([Buffer.from(literals[pairName].responseHex, 'hex'), hmac])
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: { [api]: [version, version] },
      }).answer(api, () => bytes)
      const admin = new Kafka(
        scripted.kafkaOptions({
          retry: { retries: 0 },
          logLevel: logLevel.DEBUG,
          logCreator: () => (entry) => entries.push(entry),
        })
      ).admin()
      try {
        await admin.connect()
        await expect(call(admin, api)).rejects.toThrow('Unexpected response bytes')
        expect(inspect(entries, { depth: null, maxArrayLength: null })).not.toContain(
          hmac.toString()
        )
        expect(entries.some(({ log }) => log.payload === '[filtered]')).toBe(true)
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )
})

describe('token input and capabilities', () => {
  test.each([1, 2])(
    'an owner is refused below v3, with no token request at v%s',
    async (version) => {
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: { CreateDelegationToken: [version, version] },
      })
      const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
      try {
        await admin.connect()
        await expect(admin.createDelegationToken({ owner: principal })).rejects.toBeInstanceOf(
          KafkaJSServerDoesNotSupportApiKey
        )
        expect(scripted.requests().filter((r) => r.api === 'CreateDelegationToken')).toEqual([])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )
  test('an owner at v3 and lossless duration sentinels survive the wire', async () => {
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'securityAdmin',
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await admin.createDelegationToken({ owner: principal, maxLifetimeMs: '9223372036854775807' })
      await admin.renewDelegationToken({ hmac })
      await admin.expireDelegationToken({ hmac, expiryTimePeriodMs: 0 })
      const sent = scripted.requests()
      expect(sent.find((r) => r.api === 'CreateDelegationToken').fields).toMatchObject({
        ownerPrincipalType: 'User',
        ownerPrincipalName: 'test-owner',
        maxLifetimeMs: '9223372036854775807',
      })
      expect(sent.find((r) => r.api === 'RenewDelegationToken').fields.renewPeriodMs).toBe('-1')
      expect(sent.find((r) => r.api === 'ExpireDelegationToken').fields.expiryTimePeriodMs).toBe(
        '0'
      )
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
  test.each([
    -2,
    0,
    1.1,
    Number.MAX_SAFE_INTEGER + 1,
    '01',
    '-0',
    '1e3',
    ' 1',
    '9223372036854775808',
    null,
  ])('refuses invalid create/renew durations before work: %p', async (value) => {
    const findConnectedBroker = jest.fn()
    const admin = createAdmin({ cluster: { findConnectedBroker }, logger })
    await expect(admin.createDelegationToken({ maxLifetimeMs: value })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    await expect(admin.renewDelegationToken({ hmac, renewPeriodMs: value })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(findConnectedBroker).not.toHaveBeenCalled()
  })
  test('empty owners select none, omitted owners select all, and null is invalid', async () => {
    const broker = { describeDelegationToken: jest.fn(async () => ({ tokens: [] })) }
    const findConnectedBroker = jest.fn(async () => broker)
    const admin = createAdmin({ cluster: { findConnectedBroker }, logger })
    await expect(admin.describeDelegationToken({ owners: [] })).resolves.toEqual([])
    expect(findConnectedBroker).not.toHaveBeenCalled()
    await admin.describeDelegationToken()
    expect(broker.describeDelegationToken).toHaveBeenCalledWith({ owners: null })
    await expect(admin.describeDelegationToken({ owners: null })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
  })
  test.each([
    { renewers: [principal, principal] },
    { renewers: [{ principalType: 'User', principalName: '' }] },
    { owner: null },
    { renewers: new Array(1) },
    null,
  ])('refuses invalid principals without discovery: %p', async (options) => {
    const findConnectedBroker = jest.fn()
    await expect(
      createAdmin({ cluster: { findConnectedBroker }, logger }).createDelegationToken(options)
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(findConnectedBroker).not.toHaveBeenCalled()
  })
  test.each([undefined, null, 'hmac', Buffer.alloc(0)])(
    'refuses a non-Buffer or empty HMAC before work',
    async (value) => {
      const findConnectedBroker = jest.fn()
      const admin = createAdmin({ cluster: { findConnectedBroker }, logger })
      await expect(admin.renewDelegationToken({ hmac: value })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      await expect(admin.expireDelegationToken({ hmac: value })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(findConnectedBroker).not.toHaveBeenCalled()
    }
  )
})

describe('token unsupported capabilities', () => {
  test.each([
    'CreateDelegationToken',
    'RenewDelegationToken',
    'ExpireDelegationToken',
    'DescribeDelegationToken',
  ])('%s rejects a range with no common version', async (api) => {
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'securityAdmin',
      advertise: { [api]: [0, 0] },
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await expect(call(admin, api)).rejects.toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(scripted.requests().filter((r) => r.api === api)).toEqual([])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})
