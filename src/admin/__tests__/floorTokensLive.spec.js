const FloorCapabilities = require('../../../testHelpers/floorCapabilities')
const { timingSafeEqual } = require('node:crypto')
const { inspect } = require('node:util')
const { Kafka, logLevel, KafkaJSProtocolError } = require('../../..')
const waitFor = require('../../utils/waitFor')
const Broker = require('../../broker')
const {
  saslBrokers,
  plainTextBrokers,
  saslConnectionOpts,
  createConnectionPool,
  newLogger,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')

const describeTokens = process.env.FLOOR_TOKENS_E2E === '1' ? describe : describe.skip
const VARIANTS = ['create', 'describe', 'renew', 'expire', 'plaintext', 'redaction']

describeTokens('Delegation tokens on the negotiated floor', () => {
  let admin
  let plain
  let token
  let expired = false
  let ranges
  const observed = []
  const logs = []
  const requests = []

  beforeAll(async () => {
    const transport = saslConnectionOpts()
    const config = {
      brokers: saslBrokers(),
      ssl: transport.ssl,
      sasl: transport.sasl,
      logLevel: logLevel.DEBUG,
      logCreator: () => (entry) => logs.push(entry),
      retry: { retries: 0 },
    }
    admin = new Kafka(config).admin({ apiTimeout: 15000 })
    plain = new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      retry: { retries: 0 },
    }).admin({ apiTimeout: 15000 })
    admin.on(admin.events.REQUEST, ({ payload }) => requests.push(payload))
    await admin.connect()
    const broker = new Broker({
      connectionPool: createConnectionPool(transport),
      logger: newLogger(),
    })
    try {
      await broker.connect()
      ranges = FloorCapabilities.fromBroker(
        broker,
        [
          'CreateDelegationToken',
          'DescribeDelegationToken',
          'RenewDelegationToken',
          'ExpireDelegationToken',
        ],
        process.env.KAFKA_VERSION
      )
    } finally {
      await broker.disconnect()
    }
  }, 15000)

  afterAll(async () => {
    const results = []
    if (token && !expired) {
      try {
        await admin.expireDelegationToken({ hmac: token.hmac, expiryTimePeriodMs: 0 })
      } catch (error) {
        results.push(error)
      }
    }
    for (const client of [plain, admin]) {
      try {
        if (client) await client.disconnect({ timeout: 5000 })
      } catch (error) {
        results.push(error)
      }
    }
    if (results.length) throw results[0]
  }, 30000)

  test('creates, describes, renews and expires a delegation token', async () => {
    token = await admin.createDelegationToken({ maxLifetimeMs: 120000 })
    expect(typeof token.tokenId).toBe('string')
    expect(token.tokenId.length > 0).toBe(true)
    expect(Buffer.isBuffer(token.hmac) && token.hmac.length > 0).toBe(true)
    expect(BigInt(token.issueTimestampMs) <= BigInt(token.expiryTimestampMs)).toBe(true)
    expect(BigInt(token.expiryTimestampMs) <= BigInt(token.maxTimestampMs)).toBe(true)
    observed.push('create')
    // A token change reaches the answering broker's cache after the controller commits it.
    const describedWhen = (predicate) =>
      waitFor(
        async () => {
          const all = await admin.describeDelegationToken()
          return predicate(all) && all
        },
        { delay: 250, maxWait: 15000, timeoutMessage: 'The token change never became visible' }
      )
    const described = await describedWhen((all) =>
      all.some((item) => item.tokenId === token.tokenId)
    )
    const matches = described.filter((item) => item.tokenId === token.tokenId)
    expect(matches.length).toBe(1)
    const equal =
      matches[0].hmac.length === token.hmac.length && timingSafeEqual(matches[0].hmac, token.hmac)
    expect(equal).toBe(true)
    observed.push('describe')
    const expiryOf = async (expected) =>
      BigInt(
        (
          await describedWhen((all) =>
            all.some(
              (item) =>
                item.tokenId === token.tokenId && BigInt(item.expiryTimestampMs) === expected
            )
          )
        ).find((item) => item.tokenId === token.tokenId).expiryTimestampMs
      )
    const shortened = await admin.expireDelegationToken({
      hmac: token.hmac,
      expiryTimePeriodMs: 30000,
    })
    expect(BigInt(shortened.expiryTimestampMs) < BigInt(token.expiryTimestampMs)).toBe(true)
    expect(await expiryOf(BigInt(shortened.expiryTimestampMs))).toBe(
      BigInt(shortened.expiryTimestampMs)
    )
    const renewed = await admin.renewDelegationToken({ hmac: token.hmac, renewPeriodMs: 90000 })
    expect(BigInt(renewed.expiryTimestampMs) > BigInt(shortened.expiryTimestampMs)).toBe(true)
    expect(BigInt(renewed.expiryTimestampMs) <= BigInt(token.maxTimestampMs)).toBe(true)
    expect(await expiryOf(BigInt(renewed.expiryTimestampMs))).toBe(
      BigInt(renewed.expiryTimestampMs)
    )
    observed.push('renew')
    await admin.expireDelegationToken({ hmac: token.hmac, expiryTimePeriodMs: 0 })
    expired = true
    const remaining = await describedWhen((all) =>
      all.every(
        (item) =>
          item.tokenId !== token.tokenId || BigInt(item.expiryTimestampMs) <= BigInt(Date.now())
      )
    )
    expect(
      remaining.every(
        (item) =>
          item.tokenId !== token.tokenId || BigInt(item.expiryTimestampMs) <= BigInt(Date.now())
      )
    ).toBe(true)
    let refusal
    try {
      await admin.renewDelegationToken({ hmac: token.hmac })
    } catch (error) {
      refusal = error
    }
    expect(refusal instanceof KafkaJSProtocolError).toBe(true)
    expect(['DELEGATION_TOKEN_NOT_FOUND', 'DELEGATION_TOKEN_EXPIRED'].includes(refusal?.type)).toBe(
      true
    )
    observed.push('expire')
    const text = inspect(logs, { depth: null, maxArrayLength: null })
    const markers = [
      token.hmac.toString(),
      token.hmac.toString('hex'),
      token.hmac.toString('base64'),
      token.hmac.toString('hex').match(/../g).join(' '),
    ]
    expect(markers.some((marker) => marker.length > 0 && text.includes(marker))).toBe(false)
    observed.push('redaction')
    writeEvidence('floor-tokens', {
      case: 'F16',
      lifecycle: ['create', 'describe', 'renew', 'expire'],
      hmacEqual: equal,
      leakedSecret: false,
      ranges,
      wireVersions: wireVersionsOf(requests),
    })
  }, 60000)

  test('rejects token administration on a plaintext channel without exposing credentials', async () => {
    await plain.connect()
    await expect(plain.createDelegationToken()).rejects.toMatchObject({ code: 64 })
    observed.push('plaintext')
    FloorCapabilities.variants(VARIANTS, observed)
    writeEvidence('floor-tokens', {
      case: 'F16',
      plaintextCode: 64,
      ranges,
      wireVersions: wireVersionsOf(requests),
    })
  }, 15000)
})

describe('Floor token expectations', () => {
  test('requires every named variant exactly once', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, VARIANTS)).not.toThrow()
  })

  test('refuses an omitted variant', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, VARIANTS.slice(1))).toThrow(
      'Variant population differs'
    )
  })

  test('refuses duplicate or extra variants', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, [...VARIANTS, VARIANTS[0]])).toThrow(
      'Duplicate observed variant'
    )
    expect(() => FloorCapabilities.variants(VARIANTS, [...VARIANTS, 'extra'])).toThrow(
      'Variant population differs'
    )
  })
})
