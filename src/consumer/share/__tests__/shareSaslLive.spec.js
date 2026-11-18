const { Kafka, logLevel, KafkaJSSASLAuthenticationError } = require('../../../../index')
const defaultSocketFactory = require('../../../network/socketFactory')
const { createChaosSocketFactory } = require('../../../../testHelpers/chaosSocket')
const { SaslAuthenticate } = require('../../../protocol/requests/apiKeys')
const {
  createTopic,
  plainTextBrokers,
  saslBrokers,
  saslConnectionOpts,
  secureRandom,
} = require('testHelpers')
const { setShareGroupConfigs } = require('../../../../testHelpers/shareFixtures')

// Runs in the share-base lane (docker-compose.yml, whose brokers also listen with SASL PLAIN).
const live = process.env.SHARE_SASL_E2E === '1' ? describe : describe.skip

/** An inert marker placed in a revoked password; never a real secret. */
const SECRET = 'secret-marker-5c2e'

/**
 * Polls `predicate` every 100 ms.
 * @param {() => boolean} predicate
 * @param {number} ms how long to wait
 * @param {string} what named in the timeout error
 * @returns {Promise<void>}
 * @throws {Error} when `predicate` is still false after `ms`
 */
const until = async (predicate, ms, what) => {
  const deadline = Date.now() + ms
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

live('Share consumer over SASL PLAIN', () => {
  let topic, groupId, kafka, chaos, sasl, consumer, producer
  const delivered = []
  const crashes = []

  beforeEach(async () => {
    topic = `share-sasl-${secureRandom()}`
    groupId = `share-sasl-${secureRandom()}`
    delivered.length = 0
    crashes.length = 0
    await createTopic({ topic })
    const plain = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
    const admin = plain.admin()
    producer = plain.producer()
    await admin.connect()
    try {
      await setShareGroupConfigs(admin, groupId, { 'share.auto.offset.reset': 'earliest' })
    } finally {
      await admin.disconnect()
    }
    await producer.connect()

    chaos = createChaosSocketFactory(defaultSocketFactory())
    const opts = saslConnectionOpts()
    sasl = { ...opts.sasl }
    kafka = new Kafka({
      brokers: saslBrokers(),
      ssl: opts.ssl,
      sasl,
      socketFactory: chaos,
      logLevel: logLevel.NOTHING,
    })
  })

  afterEach(async () => {
    await Promise.allSettled([consumer?.disconnect({ timeout: 5000 }), producer?.disconnect()])
    consumer = producer = undefined
  })

  /**
   * Starts the share member and waits for its first record.
   * @param {object} [retry] the member's retry options
   * @returns {Promise<void>}
   */
  const startAndDeliver = async (retry) => {
    consumer = kafka.shareConsumer({ groupId, ...(retry && { retry }) })
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    await consumer.connect()
    await consumer.subscribe({ topics: [topic] })
    await consumer.run({
      eachMessage: async ({ message }) => {
        delivered.push(message.value.toString())
      },
    })
    await producer.send({ topic, messages: [{ value: 'before' }] })
    await until(() => delivered.includes('before'), 60000, 'the first record')
  }

  /**
   * Destroys every open client socket as a broker reset would, so the member reconnects.
   * @returns {object[]} the sockets that were open
   */
  const resetOpenSockets = () => {
    const open = chaos.sockets.filter((socket) => !socket.destroyed)
    for (const socket of open) {
      socket.destroy(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))
    }
    return open
  }

  test('a member whose login is reset while it reconnects resumes delivery', async () => {
    await startAndDeliver()
    const open = new Set(chaos.sockets)
    let armed = null
    const disarm = chaos.arm({}, (socket) => {
      if (armed || open.has(socket)) return
      armed = socket
      socket.dropResponses(1, { apiKey: SaslAuthenticate, thenClose: true })
    })
    try {
      resetOpenSockets()
      await producer.send({ topic, messages: [{ value: 'after' }] })

      await until(() => delivered.includes('after'), 60000, 'the record sent after the reset')
    } finally {
      disarm()
    }

    expect(armed).not.toBeNull()
    expect(armed.responseRule.count).toBe(0)
    expect(crashes.filter(({ error }) => error instanceof KafkaJSSASLAuthenticationError)).toEqual(
      []
    )
  }, 150000)

  test('a member whose password is revoked stops without restarting even when restartOnFailure asks to', async () => {
    let asked = 0
    await startAndDeliver({
      retries: 2,
      initialRetryTime: 100,
      restartOnFailure: async () => {
        asked++
        return true
      },
    })

    sasl.password = `wrong-${SECRET}`
    resetOpenSockets()
    await until(() => crashes.length > 0, 60000, 'the member to crash')
    const opened = chaos.sockets.length
    await new Promise((resolve) => setTimeout(resolve, 3000))

    const [{ error, restart }] = crashes
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(`${error.message}\n${error.stack}`).not.toContain(SECRET)
    expect(restart).toBe(false)
    expect(asked).toBe(1)
    expect(crashes).toHaveLength(1)
    expect(chaos.sockets).toHaveLength(opened)
  }, 150000)
})
