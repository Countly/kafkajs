const { createHmac, pbkdf2Sync } = require('crypto')
const { Kafka } = require('../../../index')
const { newLogger } = require('testHelpers')
const { KafkaJSSASLAuthenticationError } = require('../../errors')
const ConnectionPool = require('../../network/connectionPool')
const {
  createScriptedCluster,
  groupScript,
  producerScript,
} = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const Broker = require('../index')
const { DIGESTS, SCRAM } = require('../saslAuthenticator/scram')

const BLACKOUT_MS = 1000
const RETRY_MS = 100
const SASL_AUTHENTICATION_FAILED = 58

/**
 * Advances the real consumer's continuations with fake time, without exhausting retry timers.
 * @param {Promise<unknown>} work public setup or shutdown call
 * @returns {Promise<unknown>} its settled outcome
 * @throws {Error} if the call exceeds its fake-time budget
 */
const until = async (work) => {
  const state = { settled: false }
  const tracked = work.finally(() => (state.settled = true))
  tracked.catch(() => {})
  for (let elapsed = 0; !state.settled && elapsed < BLACKOUT_MS; elapsed += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
  if (!state.settled) throw new Error('Consumer did not settle within the fake-time budget')
  return tracked
}

describe('Kafka 4.4 client fix audit', () => {
  let cluster
  let consumer
  let brokerAnswerTimers

  beforeEach(() => {
    jest.useFakeTimers()
    brokerAnswerTimers = new Set()
  })

  afterEach(async () => {
    if (consumer) await until(consumer.disconnect({ timeout: 0 }))
    for (const timer of brokerAnswerTimers) clearTimeout(timer)
    await jest.advanceTimersByTimeAsync(1)
    cluster.assertClean()
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('KAFKA-20253 failed reauthentication cannot spin consumer retries', async () => {
    const attempts = []
    const crashes = []
    const restarts = []
    let failing = false
    cluster = groupScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node', port: 9092 }],
        profile: 'sasl',
      }),
      { batches: 0, brokerAnswerTimers }
    )
    cluster.answer('SaslAuthenticate', () => {
      if (!failing)
        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.alloc(0),
          sessionLifetimeMs: '10000',
        }
      attempts.push(Date.now())
      return {
        errorCode: SASL_AUTHENTICATION_FAILED,
        errorMessage: 'rejected',
        authBytes: null,
        sessionLifetimeMs: '0',
      }
    })
    consumer = new Kafka(
      cluster.kafkaOptions({
        sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
        authenticationTimeout: BLACKOUT_MS,
        reauthenticationThreshold: 9500,
        retry: { initialRetryTime: RETRY_MS, maxRetryTime: RETRY_MS, factor: 0, multiplier: 1 },
      })
    ).consumer({
      groupId: 'reauthentication-audit',
      maxWaitTimeInMs: 20,
      retry: {
        initialRetryTime: RETRY_MS,
        maxRetryTime: RETRY_MS,
        factor: 0,
        multiplier: 1,
        retries: 2,
        restartOnFailure: async (error) => {
          restarts.push(error)
          return true
        },
      },
    })
    consumer.on(consumer.events.CRASH, ({ payload }) =>
      crashes.push({ at: Date.now(), ...payload })
    )
    await until(consumer.connect())
    await until(consumer.subscribe({ topic: TOPIC }))
    await until(consumer.run({ autoCommit: false, eachMessage: async () => {} }))
    await jest.advanceTimersByTimeAsync(100)
    expect(cluster.requests().some(({ api }) => api === 'JoinGroup')).toBe(true)
    expect(cluster.requests().some(({ api }) => api === 'Fetch')).toBe(true)

    failing = true
    for (let elapsed = 0; elapsed < 4 * BLACKOUT_MS; elapsed += RETRY_MS) {
      await jest.advanceTimersByTimeAsync(RETRY_MS)
      const written = attempts.length
      for (let turn = 0; turn < 20; turn++) await Promise.resolve()
      expect(attempts).toHaveLength(written)
      expect(jest.getTimerCount()).toBeLessThanOrEqual(12)
    }

    expect(attempts.length).toBeGreaterThanOrEqual(3)
    expect(attempts.length).toBeLessThanOrEqual(1 + (4 * BLACKOUT_MS) / RETRY_MS)
    for (let index = 1; index < attempts.length; index++) {
      expect(attempts[index] - attempts[index - 1]).toBeGreaterThanOrEqual(RETRY_MS)
    }
    expect(crashes.length).toBeGreaterThanOrEqual(2)
    expect(crashes.length).toBeLessThanOrEqual((4 * BLACKOUT_MS) / RETRY_MS)
    expect(restarts).toHaveLength(crashes.length)
    expect(crashes.every(({ restart }) => restart)).toBe(true)
    for (let index = 1; index < crashes.length; index++) {
      expect(crashes[index].at - crashes[index - 1].at).toBeGreaterThanOrEqual(RETRY_MS)
    }
    const originalCause = (error) => (error.cause ? originalCause(error.cause) : error)
    expect(
      crashes.every(({ error }) => originalCause(error) instanceof KafkaJSSASLAuthenticationError)
    ).toBe(true)

    await until(consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(100)
    expect(brokerAnswerTimers.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    expect(cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    const written = cluster.requests().length
    const dials = cluster.sockets().length
    await jest.advanceTimersByTimeAsync(5 * BLACKOUT_MS)
    expect(cluster.requests()).toHaveLength(written)
    expect(cluster.sockets()).toHaveLength(dials)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('session start', () => {
  const PASSWORD = 'fixture-password'
  const SESSION_LIFETIME = '1000'
  const LOGIN_DELAY = 2000
  let broker
  let cluster

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  })

  afterEach(async () => {
    try {
      cluster?.cancelReplyHolds()
      await broker?.disconnect({ drainTimeout: 0 })
      await jest.advanceTimersByTimeAsync(1)
      cluster?.assertClean()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.restoreAllMocks()
      jest.useRealTimers()
      broker = null
      cluster = null
    }
  })

  /**
   * Scripts a broker with a one-second session and a separate login timeout.
   * @param {string} mechanism SASL handshake mechanism
   * @param {(fields: { authBytes: Buffer }) => object} [answer] SASL reply fields
   * @returns {void}
   */
  const arrange = (mechanism, answer = () => ({ authBytes: Buffer.alloc(0) })) => {
    const node = { id: 0, host: 'node', port: 9092 }
    cluster = createScriptedCluster({ nodes: [node], profile: 'sasl' })
      .answer('SaslHandshake', () => ({ enabledMechanisms: [mechanism] }))
      .answer('SaslAuthenticate', (fields) => ({
        errorCode: 0,
        errorMessage: null,
        sessionLifetimeMs: SESSION_LIFETIME,
        ...answer(fields),
      }))
    const logger = newLogger()
    const connectionPool = new ConnectionPool({
      ...node,
      logger,
      socketFactory: cluster.socketFactory,
      connectionTimeout: 10000,
      requestTimeout: 5000,
      authenticationTimeout: 5000,
      connectionsMaxIdle: 0,
      reauthenticationThreshold: 0,
      sasl: { mechanism, username: 'user', password: PASSWORD },
    })
    broker = new Broker({ connectionPool, logger, nodeId: node.id })
  }

  /**
   * Answers both SCRAM messages using the client's nonce and a valid server signature.
   * @param {{ type: string, length: number, minIterations: number }} digest SCRAM digest
   * @returns {(fields: { authBytes: Buffer }) => { authBytes: Buffer }} responder
   */
  const scramAnswer = ({ type, length, minIterations }) => {
    const salt = Buffer.from('fixture-salt')
    const saltedPassword = pbkdf2Sync(PASSWORD, salt, minIterations, length, type)
    const serverKey = createHmac(type, saltedPassword).update('Server Key').digest()
    let clientFirstBare
    let serverFirst
    return ({ authBytes }) => {
      const message = authBytes.toString()
      if (message.startsWith('n,,')) {
        clientFirstBare = message.slice('n,,'.length)
        const clientNonce = clientFirstBare.slice('n=user,r='.length)
        serverFirst = `r=${clientNonce}fixture,s=${salt.toString('base64')},i=${minIterations}`
        return { authBytes: Buffer.from(serverFirst), sessionLifetimeMs: '0' }
      }
      const withoutProof = message.slice(0, message.indexOf(',p='))
      const transcript = [clientFirstBare, serverFirst, withoutProof].join(',')
      const signature = createHmac(type, serverKey).update(transcript).digest('base64')
      return { authBytes: Buffer.from(`v=${signature}`) }
    }
  }

  test('a session is counted from when its final SASL request was written', async () => {
    arrange('PLAIN')
    const reply = cluster.holdReply({ api: 'SaslAuthenticate' })
    const connected = broker.connect()
    expect(await Promise.race([reply.executed(), connected])).toMatchObject({
      api: 'SaslAuthenticate',
    })
    await jest.advanceTimersByTimeAsync(LOGIN_DELAY)
    reply.deliver()
    await connected

    await broker.metadata()

    expect(cluster.requests().map(({ api }) => api)).toEqual([
      'ApiVersions',
      'SaslHandshake',
      'SaslAuthenticate',
      'SaslHandshake',
      'SaslAuthenticate',
      'Metadata',
    ])
  })

  test.each([
    ['SCRAM-SHA-256', DIGESTS.SHA256],
    ['SCRAM-SHA-512', DIGESTS.SHA512],
  ])(
    'a SCRAM session whose signature check crosses the deadline re-authenticates first (%s)',
    async (mechanism, digest) => {
      arrange(mechanism, scramAnswer(digest))
      const serverKey = SCRAM.prototype.serverKey
      jest.spyOn(SCRAM.prototype, 'serverKey').mockImplementationOnce(async function (response) {
        await jest.advanceTimersByTimeAsync(LOGIN_DELAY)
        return serverKey.call(this, response)
      })
      await broker.connect()

      await broker.metadata()

      expect(cluster.requests().map(({ api }) => api)).toEqual([
        'ApiVersions',
        'SaslHandshake',
        'SaslAuthenticate',
        'SaslAuthenticate',
        'SaslHandshake',
        'SaslAuthenticate',
        'SaslAuthenticate',
        'Metadata',
      ])
    }
  )

  test('a session answered at once keeps its lifetime', async () => {
    arrange('PLAIN')
    await broker.connect()
    await jest.advanceTimersByTimeAsync(900)

    await broker.metadata()

    expect(cluster.requests().map(({ api }) => api)).toEqual([
      'ApiVersions',
      'SaslHandshake',
      'SaslAuthenticate',
      'Metadata',
    ])
  })
})

describe('session at write', () => {
  let cluster
  let producer

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(async () => {
    try {
      cluster?.cancelReplyHolds()
      for (let i = 0; i < 50; i += 1) {
        await jest.advanceTimersByTimeAsync(20)
        await new Promise((resolve) => setImmediate(resolve))
      }
    } finally {
      try {
        if (producer) await producer.disconnect()
        cluster?.assertClean()
      } finally {
        producer = cluster = undefined
        jest.useRealTimers()
      }
    }
  })

  /**
   * An idempotent producer, one request in flight per connection, over a broker whose SASL
   * sessions last one second and whose topic has two partitions.
   * @param {object} producerOptions extra producer options
   * @returns {void}
   */
  const start = (producerOptions) => {
    cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile: 'sasl' }),
      { partitions: 2 }
    )
      .answer('SaslHandshake', () => ({ enabledMechanisms: ['PLAIN'] }))
      .answer('SaslAuthenticate', () => ({
        errorCode: 0,
        errorMessage: null,
        authBytes: Buffer.alloc(0),
        sessionLifetimeMs: '1000',
      }))
    producer = new Kafka(
      cluster.kafkaOptions({
        sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
        reauthenticationThreshold: 0,
        requestTimeout: 1500,
        enforceRequestTimeout: false,
      })
    ).producer({ idempotent: true, maxInFlightRequests: 1, linger: 0, ...producerOptions })
  }

  /**
   * Moves fake time a millisecond at a time until `predicate` holds, failing after 200 ms.
   * @param {() => boolean} predicate
   * @returns {Promise<void>}
   */
  const until = async (predicate) => {
    for (let i = 0; i < 200 && !predicate(); i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(predicate()).toBe(true)
  }

  /**
   * @param {Promise} promise
   * @returns {{ settled: boolean }} set once `promise` settles; its rejection is handled
   */
  const watch = (promise) => {
    const state = { settled: false }
    promise.then(
      () => (state.settled = true),
      () => (state.settled = true)
    )
    return state
  }

  /** @returns {string[]} each request written, a Produce named with the partitions it carries */
  const written = () =>
    cluster
      .requests()
      .map(({ api, fields }) =>
        api === 'Produce'
          ? `Produce:${fields.topics.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))}`
          : api
      )

  /**
   * @param {number} partition
   * @returns {object[]} the record batch of `partition` in each Produce request that carried it
   */
  const batchesOf = (partition) =>
    cluster
      .requests()
      .filter(({ api }) => api === 'Produce')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
      .filter((entry) => entry.partition === partition)
      .map(({ batches }) => batches[0])

  /**
   * Sends to partition 0 and holds its answer, then queues a send to partition 1 behind it on the
   * same connection and lets the session end before the answer is delivered.
   * @returns {Promise<{ firstResult: *, second: Promise<*>, secondState: { settled: boolean } }>}
   *   the first send's result or error, and the second send with its settlement
   */
  const queuedPastTheSession = async () => {
    await producer.connect()
    const held = cluster.holdReply({ api: 'Produce' })
    const firstSend = producer
      .send({ topic: TOPIC, messages: [{ partition: 0, value: 'a' }] })
      .catch((error) => error)
    const first = watch(firstSend)
    await until(() => written().includes('Produce:0'))
    const second = producer
      .send({ topic: TOPIC, messages: [{ partition: 1, value: 'b' }] })
      .catch((error) => error)
    const secondState = watch(second)
    await jest.advanceTimersByTimeAsync(1200)
    expect(written()).not.toContain('Produce:1')

    held.deliver()
    await until(() => first.settled)
    return { firstResult: await firstSend, second, secondState }
  }

  test('a session-refused produce is never recorded as written', async () => {
    start({
      deliveryTimeout: 1500,
      retry: { initialRetryTime: 5000, maxRetryTime: 5000, factor: 0 },
    })
    const { firstResult, second, secondState } = await queuedPastTheSession()

    await jest.advanceTimersByTimeAsync(400)
    await until(() => secondState.settled)

    expect(firstResult).toEqual([expect.objectContaining({ partition: 0, errorCode: 0 })])
    expect(await second).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
    expect(written()).not.toContain('Produce:1')
  })

  test('a session-refused produce is resent once, after re-authentication', async () => {
    start({ retry: { initialRetryTime: 10, maxRetryTime: 10, factor: 0 } })
    const { firstResult, second, secondState } = await queuedPastTheSession()

    await until(() => secondState.settled)

    expect(firstResult).toEqual([expect.objectContaining({ partition: 0, errorCode: 0 })])
    expect(await second).toEqual([expect.objectContaining({ partition: 1, errorCode: 0 })])
    const log = written()
    expect(log.filter((api) => api === 'Produce:1')).toHaveLength(1)
    expect(log.slice(log.indexOf('Produce:0'), log.indexOf('Produce:1'))).toEqual(
      expect.arrayContaining(['SaslHandshake', 'SaslAuthenticate'])
    )
    const [stamp] = batchesOf(0)
    expect(batchesOf(1)).toEqual([
      expect.objectContaining({
        producerId: stamp.producerId,
        producerEpoch: stamp.producerEpoch,
        baseSequence: 0,
      }),
    ])
  })
})
