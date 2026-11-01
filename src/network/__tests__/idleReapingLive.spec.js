const { Kafka, logLevel } = require('../../../index')
const { plainTextBrokers, secureRandom, createTopic, newLogger } = require('testHelpers')
const { instrument } = require('../../../scripts/soak/soak')
const { readLog } = require('../../../testHelpers/rawLog')
const { writeEvidence, wireVersionsOf } = require('../../../testHelpers/e2eEvidence')
const Broker = require('../../broker')
const ConnectionPool = require('../connectionPool')
const defaultSocketFactory = require('../socketFactory')

const live = process.env.IDLE_REAP_E2E === '1' ? describe : describe.skip
const BUDGET_MS = 120000
/** @param {number} ms bounded delay @returns {Promise<void>} */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
/**
 * @param {Function} predicate observation, evaluated without creating an orphan promise
 * @param {number} ms total wait budget
 * @returns {Promise<void>}
 * @throws {Error} if the observed state never occurs
 */
const waitFor = async (predicate, ms) => {
  const until = Date.now() + ms
  while (!(await predicate())) {
    if (Date.now() >= until) throw new Error('Idle observation deadline expired')
    await sleep(50)
  }
}

live('Idle reaping live', () => {
  test(
    'idle producer reconnects and all three replay sets remain exact',
    async () => {
      const brokers = plainTextBrokers()
      const topic = `e2e-p9d-idle-reap-${secureRandom()}`
      const groupId = `${topic}-group`
      const box = instrument({
        brokers,
        clientId: 'idle-producer',
        connectionsMaxIdle: 5000,
        logLevel: logLevel.NOTHING,
      })
      const kafka = new Kafka({
        brokers,
        clientId: 'idle-member',
        connectionsMaxIdle: 5000,
        logLevel: logLevel.NOTHING,
      })
      const producer = box.kafka.producer({ idempotent: true })
      const consumer = kafka.consumer({
        groupId,
        groupProtocol: 'classic',
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxWaitTimeInMs: 500,
      })
      const admin = kafka.admin()
      const delivered = []
      const commits = []
      const versions = []
      const requests = []
      const crashes = []
      let rebalances = 0
      const remove = [
        admin.on(admin.events.REQUEST, ({ payload }) => requests.push(payload)),
        consumer.on(consumer.events.REQUEST, ({ payload }) => requests.push(payload)),
        producer.on(producer.events.REQUEST, ({ payload }) => {
          requests.push(payload)
          if (payload.apiName === 'ApiVersions') versions.push(payload)
        }),
        consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload)),
        consumer.on(consumer.events.REBALANCING, () => {
          rebalances++
        }),
      ]
      const cycles = []
      try {
        await createTopic({ topic, partitions: 1 })
        await admin.connect()
        await producer.connect()
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            delivered.push(message.value.toString())
            const offset = String(BigInt(message.offset) + 1n)
            await consumer.commitOffsets([{ topic, partition, offset }])
            commits.push(offset)
          },
        })
        for (let i = 0; i < 4; i++) {
          const startedAt = Date.now()
          await producer.send({ topic, messages: [{ partition: 0, value: String(i) }] })
          await waitFor(() => commits.length === i + 1, 10000)
          const before = await box.probe.snapshot()
          expect(before.sockets.open).toBeGreaterThan(0)
          await waitFor(async () => (await box.probe.snapshot()).sockets.open === 0, 7000)
          const idle = await box.probe.snapshot()
          expect(idle.queueDepth).toBe(0)
          cycles.push({ before, idle })
          if (i < 3) await sleep(Math.max(0, 20000 - (Date.now() - startedAt)))
        }
        expect(versions.length).toBeGreaterThanOrEqual(4)
        expect(crashes).toEqual([])
        expect(rebalances).toBe(0)
        expect(delivered).toEqual(['0', '1', '2', '3'])
        const log = await readLog({ brokers, topic })
        const records = [...log.values()].flatMap(({ records }) => records)
        expect(records.map(({ value }) => value.toString())).toEqual(['0', '1', '2', '3'])
        const brokerOffsets = await admin.fetchOffsets({
          groupId,
          topics: [topic],
          resolveOffsets: false,
        })
        expect(brokerOffsets[0].partitions[0].offset).toBe(commits.at(-1))
        expect(commits.at(-1)).toBe(String(BigInt(records.at(-1).offset) + 1n))
        writeEvidence('idle-reap', {
          kind: 'oracle',
          config: {
            connectionsMaxIdle: 5000,
            sendPeriodMs: 20000,
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            maxWaitTimeInMs: 500,
            autoCommit: false,
          },
          wireVersions: wireVersionsOf(requests),
          cycles,
          versions: versions.length,
          log: records.map(({ value }) => value.toString()),
          committedOffsets: brokerOffsets,
          handlerDelivery: delivered,
          crashes: crashes.length,
          rebalances,
          ok: true,
        })
      } finally {
        for (const off of remove) off()
        const closed = await Promise.allSettled([
          consumer.disconnect(),
          producer.disconnect(),
          admin.disconnect(),
        ])
        expect(closed.every(({ status }) => status === 'fulfilled')).toBe(true)
        expect((await box.probe.snapshot()).sockets.open).toBe(0)
      }
    },
    BUDGET_MS
  )

  test('a live long poll is not reaped while its request is pending', async () => {
    const brokers = plainTextBrokers()
    const topic = `e2e-p9d-idle-held-${secureRandom()}`
    await createTopic({ topic, partitions: 1 })
    const [host, port] = brokers[0].split(':')
    const logger = newLogger()
    let pool = new ConnectionPool({
      host,
      port: Number(port),
      logger,
      socketFactory: defaultSocketFactory(),
      connectionTimeout: 3000,
      requestTimeout: 30000,
      connectionsMaxIdle: 5000,
    })
    let broker = new Broker({ connectionPool: pool, logger })
    let pending
    try {
      await broker.connect()
      const metadata = await broker.metadata([topic])
      const leaderId = metadata.topicMetadata[0].partitionMetadata[0].leader
      const leader = metadata.brokers.find(({ nodeId }) => nodeId === leaderId)
      if (leader.host !== host || leader.port !== Number(port)) {
        await broker.disconnect()
        pool = new ConnectionPool({
          host: leader.host,
          port: leader.port,
          logger,
          socketFactory: defaultSocketFactory(),
          connectionTimeout: 3000,
          requestTimeout: 30000,
          connectionsMaxIdle: 5000,
        })
        broker = new Broker({ connectionPool: pool, logger })
        await broker.connect()
      }
      pending = broker.fetch({
        minBytes: 1,
        maxWaitTime: 10000,
        topics: [{ topic, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] }],
      })
      pending.catch(() => {})
      await waitFor(
        () => pool.pool.some((connection) => connection.requestQueue.inflight.size > 0),
        2000
      )
      const fetch = pool.pool.find((connection) => connection.requestQueue.inflight.size > 0)
      expect(fetch.idleTimer).not.toBe(null)
      expect(fetch.idleReadyAttempt).toBe(fetch.connectAttempt)
      const socket = fetch.socket
      const entry = [...fetch.requestQueue.inflight.values()][0].entry
      const wireVersions = { [entry.apiName]: entry.apiVersion }
      await sleep(6000)
      expect(fetch.socket).toBe(socket)
      expect(socket.destroyed).toBe(false)
      expect(fetch.requestQueue.inflight.size).toBe(1)
      await pending
      writeEvidence('idle-reap', {
        kind: 'held-fetch',
        protected: true,
        idleEligible: true,
        ok: true,
        wireVersions,
        config: { connectionsMaxIdle: 5000, maxWaitTime: 10000, requestTimeout: 30000 },
      })
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
      if (pending) await pending.catch(() => {})
    }
  }, 30000)
})
