const { execFileSync } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const { secureRandom } = require('testHelpers')

const COMPOSE_FILE = 'docker-compose.rebootstrap.yml'
const ORIGINAL = ['localhost:9092', 'localhost:9095', 'localhost:9098']
const FRESH = ['localhost:9192']

/**
 * Runs a `docker compose` subcommand against this spec's own compose file (the shared 3-node
 * cluster plus the independent `kafkaFresh` service), synchronously so the test never races the
 * container it just told to stop or start.
 *
 * @param {...string} args `docker compose -f docker-compose.rebootstrap.yml <args>`
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const compose = (...args) =>
  execFileSync('docker', ['compose', '-f', COMPOSE_FILE, ...args], { stdio: 'pipe' })

const enabled = process.env.REBOOTSTRAP_E2E === '1'
const describeLive = enabled ? describe : describe.skip

describeLive('Cluster > rebootstrap (live, cluster replaced on new ports)', () => {
  let addresses
  let kafka
  let producer
  let consumer
  let topic

  beforeAll(() => {
    addresses = [...ORIGINAL]
  })

  afterAll(async () => {
    producer && (await producer.disconnect().catch(() => {}))
    consumer && (await consumer.disconnect().catch(() => {}))
    compose('start', 'kafka1', 'kafka2', 'kafka3')
    // The wrapper's compose-down covers this too, but a direct run must not leave it behind.
    compose('stop', 'kafkaFresh')
  })

  test('a client whose entire cluster is replaced rebootstraps and resumes producing and consuming', async () => {
    topic = `test-topic-${secureRandom()}`
    kafka = new Kafka({
      clientId: 'rebootstrap-e2e',
      brokers: () => addresses,
      logLevel: logLevel.NOTHING,
      metadataRecoveryStrategy: 'rebootstrap',
      metadataRecoveryRebootstrapTriggerMs: 3000,
      retry: { retries: 30, initialRetryTime: 300, maxRetryTime: 1000 },
    })

    producer = kafka.producer()
    const rebootstraps = []
    producer.on(producer.events.REBOOTSTRAP, (event) => rebootstraps.push(event.payload))

    await producer.connect()
    await producer.send({ topic, messages: [{ value: 'before' }] })

    // Every broker the client knows about disappears, and the address list now points at a
    // cluster that shares nothing with the old one: different id, different port, empty log.
    compose('stop', 'kafka1', 'kafka2', 'kafka3')
    addresses = [...FRESH]

    await expect(
      (async () => {
        const deadline = Date.now() + 60000
        while (Date.now() < deadline) {
          try {
            await producer.send({ topic, messages: [{ value: 'after' }] })
            return true
          } catch (e) {
            await new Promise((resolve) => setTimeout(resolve, 500))
          }
        }
        throw new Error('producer never recovered onto the fresh cluster')
      })()
    ).resolves.toBe(true)

    // Which trigger fires is decided by the plan's own ordering, and it is (c), not (a): the
    // producer's next refresh enters findConnectedBroker(), every cached broker fails to
    // connect, `brokers` is non-empty and the pre-stop success armed the trigger - so
    // rebootstrap('all-brokers-unavailable') runs on the first attempt after the stop,
    // createSeedBroker() re-resolves `brokers: () => addresses` onto the fresh cluster, and the
    // client recovers long before the continuous-failure clock reaches 3000 ms. Asserting
    // 'timeout' here would be asserting against the design.
    expect(rebootstraps.length).toBeGreaterThanOrEqual(1)
    for (const { reason } of rebootstraps) {
      expect(['timeout', 'required', 'all-brokers-unavailable']).toContain(reason)
    }
    expect(rebootstraps.map(({ reason }) => reason)).toContain('all-brokers-unavailable')

    consumer = kafka.consumer({ groupId: `test-group-${secureRandom()}` })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    const received = []
    await consumer.run({
      eachMessage: async ({ message }) => received.push(message.value.toString()),
    })

    const deadline = Date.now() + 30000
    while (received.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 200))
    }

    // The fresh cluster has no history, so only the post-rebootstrap record exists.
    expect(received).toEqual(['after'])
  }, 180000)
})
