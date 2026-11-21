/* eslint n/no-missing-require: ["error", { "allowModules": ["kerberos"] }] */
const { Kafka, KafkaJSSASLAuthenticationError, logLevel } = require('../../../index')
const { randomUUID } = require('node:crypto')
const KdcFixture = require('../../../testHelpers/kerberos')
const defaultSocketFactory = require('../../network/socketFactory')
const { writeEvidence, wireVersionsOf } = require('../../../testHelpers/e2eEvidence')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { SaslAuthenticate } = require('../../protocol/requests/apiKeys')

const live = process.env.GSSAPI_E2E === '1' ? describe : describe.skip

live('GSSAPI with a ticket-granting service', () => {
  it('authenticates, negotiates no layer and rejects an observed expired cache', async () => {
    const installed = require('kerberos/package.json')
    expect(installed.version).toBe('7.0.0')
    const peer = require('kerberos')
    const fixture = new KdcFixture()
    const restore = fixture.observe(peer)
    const fresh = fixture.ticket('fresh')
    fixture.select(fresh)
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
    const requests = []
    for (const client of [admin, producer, consumer]) {
      client.on(client.events.REQUEST, (event) => requests.push(event.payload))
    }
    let expiredAdmin
    let primary
    let cleanupResults
    try {
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
      await new Promise((resolve) => setTimeout(resolve, 15500))
      await admin.listTopics()
      const reauthenticated = fixture.events.filter((event) => event.method === 'initialize').length
      expect(reauthenticated).toBeGreaterThan(reconnected)
      expect(createdSockets).toBe(beforeReauthSockets)
      const expired = fixture.ticket('expired')
      const observed = fixture.control('gssapi-expired', ['expired'])
      expect(observed.cache).toBe(expired.cache)
      expect(observed.observedAtMs).toBeGreaterThanOrEqual(expired.expiresAtMs)
      expect(observed.klistStatus).not.toBe(0)
      fixture.select(expired)
      expiredAdmin = new Kafka(config).admin()
      await expect(expiredAdmin.connect()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
      fixture.capture()
      writeEvidence('gssapi', {
        type: 'gssapi-oracle',
        wireVersions: wireVersionsOf(requests),
        config: { connectionsMaxReauthMs: 15000, serviceName: 'kafka', principal: first.principal },
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
    } catch (error) {
      primary = error
    } finally {
      cleanupResults = await Promise.allSettled([
        consumer.disconnect(),
        producer.disconnect(),
        admin.disconnect(),
        expiredAdmin?.disconnect(),
      ])
      restore()
    }
    if (primary) throw primary
    for (const result of cleanupResults) if (result.status === 'rejected') throw result.reason
  }, 300000)

  it('recovers with a fresh context after a reset during the first and the final-layer exchange, and an expired cache stays terminal', async () => {
    const peer = require('kerberos')
    const fixture = new KdcFixture()
    const restore = fixture.observe(peer)
    fixture.select(fixture.ticket('fresh'))
    const count = (method) => fixture.events.filter((event) => event.method === method).length
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const resets = []
    // The first socket loses its first SaslAuthenticate answer; the second loses the answer to the
    // exchange after `wrap`, the final security-layer reply.
    const socketFactory = (args) => {
      const socket = chaos(args)
      const ordinal = chaos.sockets.length
      const wrapsBefore = count('wrap')
      const write = socket.write.bind(socket)
      let armed = false
      socket.write = (data, encoding) => {
        const frame = Buffer.from(data, encoding)
        if (!armed && frame.length >= 6 && frame.readInt16BE(4) === SaslAuthenticate) {
          const final = count('wrap') > wrapsBefore
          if (ordinal === 1 || (ordinal === 2 && final)) {
            armed = true
            resets.push(final ? 'final' : 'first')
            socket.dropResponses(1, { apiKey: SaslAuthenticate, thenClose: true })
          }
        }
        return write(data, encoding)
      }
      return socket
    }
    const config = {
      brokers: ['localhost:9192'],
      logLevel: logLevel.NOTHING,
      sasl: { mechanism: 'gssapi', principal: 'client@KAFKAJS.TEST' },
      retry: { retries: 5, initialRetryTime: 100 },
      connectionTimeout: 10000,
    }
    const admin = new Kafka({ ...config, socketFactory }).admin()
    let expiredAdmin
    try {
      await admin.connect()
      const topics = await admin.listTopics()

      expect(Array.isArray(topics)).toBe(true)
      expect(resets).toEqual(['first', 'final'])
      expect(chaos.sockets.length).toBeGreaterThanOrEqual(3)
      expect(count('initialize')).toBeGreaterThanOrEqual(3)

      const expired = fixture.ticket('expired')
      fixture.control('gssapi-expired', ['expired'])
      fixture.select(expired)
      let expiredSockets = 0
      expiredAdmin = new Kafka({
        ...config,
        socketFactory: (args) => {
          expiredSockets++
          return defaultSocketFactory()(args)
        },
      }).admin()
      const error = await expiredAdmin.connect().catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(expiredSockets).toBe(1)
    } finally {
      await Promise.allSettled([admin.disconnect(), expiredAdmin?.disconnect()])
      restore()
    }
  }, 120000)
})
