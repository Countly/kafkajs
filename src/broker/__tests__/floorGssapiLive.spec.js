/* eslint n/no-missing-require: ["error", { "allowModules": ["kerberos"] }] */
const FloorCapabilities = require('../../../testHelpers/floorCapabilities')
const { Kafka, KafkaJSSASLAuthenticationError, logLevel } = require('../../../index')
const { randomUUID } = require('node:crypto')
const KdcFixture = require('../../../testHelpers/kerberos')
const defaultSocketFactory = require('../../network/socketFactory')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')
const { writeEvidence, wireVersionsOf } = require('../../../testHelpers/e2eEvidence')

const live = process.env.FLOOR_GSSAPI_E2E === '1' ? describe : describe.skip
const VARIANTS = ['initial', 'expired', 'reconnect', 'reauth']
/** The most the cleanups of one case may take together. */
const CLEANUP_BUDGET_MS = 60000

live('GSSAPI with a ticket-granting service', () => {
  it('authenticates, expires, reconnects and reauthenticates with the ticket service', async () => {
    const installed = require('kerberos/package.json')
    expect(installed.version).toBe('7.0.0')
    const peer = require('kerberos')
    const fixture = new KdcFixture()
    const cleanup = new CleanupStack()
    cleanup.add('native peer restoration', fixture.observe(peer))
    const topic = `kerberos-${randomUUID()}`
    const groupId = `kerberos-${randomUUID()}`
    let createdSockets = 0
    const socketFactory = defaultSocketFactory()
    const config = {
      socketFactory: (args) => {
        createdSockets++
        return socketFactory(args)
      },
      brokers: ['localhost:9192'],
      logLevel: logLevel.NOTHING,
      sasl: { mechanism: 'gssapi', principal: 'client@KAFKAJS.TEST' },
      retry: { retries: 0 },
      connectionTimeout: 10000,
    }
    const kafka = new Kafka(config)
    const admin = kafka.admin()
    const producer = kafka.producer()
    const consumer = kafka.consumer({
      groupId,
      groupProtocol: 'classic',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 500,
    })
    cleanup.add('admin disconnect', () => admin.disconnect())
    cleanup.add('producer disconnect', () => producer.disconnect())
    cleanup.add('consumer disconnect', () => consumer.disconnect())
    const requests = []
    for (const client of [admin, producer, consumer]) {
      client.on(client.events.REQUEST, (event) => requests.push(event.payload))
    }
    const variants = []
    await cleanup.run(
      async () => {
        const fresh = fixture.ticket('fresh')
        fixture.select(fresh)
        await admin.connect()
        const firstContexts = fixture.events.filter((event) => event.method === 'initialize').length
        expect(firstContexts).toBeGreaterThan(0)
        const first = fixture.events.find((event) => event.method === 'initialize')
        expect(first.service).toBe('kafka@localhost')
        expect(first.principal).toBe('client@KAFKAJS.TEST')
        const offer = fixture.events.find((event) => event.method === 'unwrap')
        expect(Buffer.from(offer.result, 'base64')).toHaveLength(4)
        expect(Buffer.from(offer.result, 'base64')[0] & 1).toBe(1)
        const wrap = fixture.events.find((event) => event.method === 'wrap')
        expect(Buffer.from(wrap.args[0], 'base64')).toEqual(Buffer.from([1, 0, 0, 0]))
        expect(wrap.args[1]).toEqual({ user: 'client@KAFKAJS.TEST', protect: false })
        variants.push('initial')
        cleanup.add('topic deletion', async () => {
          fixture.select(fixture.ticket('fresh'))
          await admin.connect()
          try {
            await admin.deleteTopics({ topics: [topic] })
          } catch (error) {
            if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
          }
        })
        await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
        await producer.connect()
        const corpus = Array.from({ length: 20 }, (_, index) => `record-${index}`)
        await producer.send({ topic, messages: corpus.map((value) => ({ value })) })
        const seen = []
        let resolveRead
        const read = new Promise((resolve) => {
          resolveRead = resolve
        })
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            seen.push(message.value.toString())
            if (seen.length === corpus.length) resolveRead()
          },
        })
        const timer = setTimeout(() => resolveRead(), 30000)
        try {
          await read
        } finally {
          clearTimeout(timer)
        }
        expect(seen).toEqual(corpus)
        // Before stop: a stopped consumer has left its group and refuses manual commits.
        await consumer.commitOffsets([{ topic, partition: 0, offset: '20' }])
        await consumer.stop()
        const committed = await admin.fetchOffsets({ groupId, topics: [topic] })
        expect(committed[0].partitions[0].offset).toBe('20')
        const raw = fixture.control('gssapi-log', [topic, groupId])
        expect(raw.log.map((record) => record.value)).toEqual(corpus)
        expect(raw.log.map((record) => record.offset)).toEqual(
          corpus.map((_, index) => String(index))
        )
        expect(raw.committedOffset).toBe('20')
        await consumer.disconnect()
        await producer.disconnect()
        await admin.disconnect()
        fixture.select(fixture.ticket('fresh'))
        await admin.connect()
        await admin.listTopics()
        const beforeReauthSockets = createdSockets
        const reconnected = fixture.events.filter((event) => event.method === 'initialize').length
        expect(reconnected).toBeGreaterThan(firstContexts)
        variants.push('reconnect')
        await new Promise((resolve) => setTimeout(resolve, 15500))
        await admin.listTopics()
        const reauthenticated = fixture.events.filter(
          (event) => event.method === 'initialize'
        ).length
        expect(reauthenticated).toBeGreaterThan(reconnected)
        expect(createdSockets).toBe(beforeReauthSockets)
        variants.push('reauth')
        const expired = fixture.ticket('expired')
        const observed = fixture.control('gssapi-expired', ['expired'])
        expect(observed.cache).toBe(expired.cache)
        expect(observed.observedAtMs).toBeGreaterThanOrEqual(expired.expiresAtMs)
        expect(observed.klistStatus).not.toBe(0)
        fixture.select(expired)
        const expiredAdmin = new Kafka(config).admin()
        cleanup.add('expired admin disconnect', () => expiredAdmin.disconnect())
        await expect(expiredAdmin.connect()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
        variants.push('expired')
        FloorCapabilities.variants(VARIANTS, variants)
        fixture.capture()
        writeEvidence('floor-gssapi', {
          case: 'F19',
          type: 'gssapi-oracle',
          wireVersions: wireVersionsOf(requests),
          config: {
            connectionsMaxReauthMs: 15000,
            serviceName: 'kafka',
            principal: first.principal,
          },
          service: first.service,
          principal: first.principal,
          layer: 1,
          maxBuffer: 0,
          firstContexts,
          reconnected,
          reauthenticated,
          issuedAtMs: expired.issuedAtMs,
          expiresAtMs: expired.expiresAtMs,
          observedAtMs: observed.observedAtMs,
          klistStatus: observed.klistStatus,
          corpus,
          delivered: seen,
          rawLog: raw.log,
          committedOffset: raw.committedOffset,
          socketsBeforeReauth: beforeReauthSockets,
          socketsAfterReauth: beforeReauthSockets,
        })
      },
      { budgetMs: CLEANUP_BUDGET_MS }
    )
  }, 300000)

  it('reads the SASL capability facts from a GSSAPI-authenticated connection', async () => {
    const fixture = new KdcFixture()
    fixture.select(fixture.ticket('fresh'))
    const ranges = await FloorCapabilities.authenticated(
      {
        host: 'localhost',
        port: 9192,
        connectionTimeout: 10000,
        sasl: { mechanism: 'gssapi', principal: 'client@KAFKAJS.TEST' },
      },
      process.env.KAFKA_VERSION
    )

    writeEvidence('floor-gssapi', { case: 'F19-capability', authenticated: 'gssapi', ranges })
    expect(Object.keys(ranges)).toEqual(['SaslHandshake', 'SaslAuthenticate'])
  }, 120000)
})

describe('Floor GSSAPI expectations', () => {
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
