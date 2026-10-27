const Broker = require('../../broker')
const Cluster = require('../../cluster')
const Runner = require('../../consumer/runner')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { readLog, committedRecords } = require('../../../testHelpers/rawLog')
const {
  createConnectionPool,
  createTopic,
  newLogger,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  evidenceLines,
} = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const {
  sleep,
  waitForCondition,
  withTimeout,
} = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: every case drops a transaction coordinator's request or answer on purpose. Run alone:
// `npm run test:e2e:send-offsets-visibility`.
const describeVisibility =
  process.env.SEND_OFFSETS_VISIBILITY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'send-offsets-visibility'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
// The broker's default partition count, so an incarnation it auto-creates has the same shape.
const PARTITIONS = 2
const RECORDS = 10
const PREVIOUS = '1'
const SENT = '5'
const NEWER = '7'
const REQUEST_TIMEOUT_MS = 4000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500
const CASE_BUDGET_MS = 60000
const HOOK_BUDGET_MS = 90000
const DISCONNECT_BUDGET_MS = 15000

/** Every write of the shared committed-offset cache, with the Cluster that made it. */
const cacheWrites = []
/** Every consumer runner started, so a case can read its own cluster's cache. */
const runners = []

/**
 * The committed offset the coordinator holds once it is stable: a `requireStable` read through a
 * raw Broker, repeated while a transaction keeps it unstable.
 * @param {string} groupId
 * @param {string} topic
 * @param {{ wait?: boolean }} [options] `wait: false` answers `null` at the first unstable read
 * @returns {Promise<string|null>} the offset, `-1` when none
 */
const brokerOffset = async (groupId, topic, { wait = true } = {}) => {
  const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  let coordinatorBroker
  try {
    await seed.connect()
    const { coordinator } = await seed.findGroupCoordinator({
      groupId,
      coordinatorType: COORDINATOR_TYPES.GROUP,
    })
    coordinatorBroker = new Broker({
      connectionPool: createConnectionPool({ host: coordinator.host, port: coordinator.port }),
      logger: newLogger(),
    })
    await coordinatorBroker.connect()
    for (let attempt = 0; attempt < WAIT_BUDGET.attempts; attempt++) {
      try {
        const { responses } = await coordinatorBroker.offsetFetch({
          groupId,
          topics: [{ topic, partitions: [{ partition: 0 }] }],
          requireStable: true,
        })
        return String(responses[0].partitions[0].offset)
      } catch (error) {
        if (error.type !== 'UNSTABLE_OFFSET_COMMIT') throw error
        if (!wait) return null
        await sleep(WAIT_BUDGET.delay)
      }
    }
    throw new Error(`The committed offset of ${groupId} stayed unstable`)
  } finally {
    await coordinatorBroker?.disconnect()
    await seed.disconnect()
  }
}

/**
 * The node a transactional id's coordinator lives on.
 * @param {string} transactionalId
 * @returns {Promise<{ host: string, port: number }>}
 */
const transactionCoordinatorOf = async (transactionalId) => {
  const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  try {
    await seed.connect()
    const { coordinator } = await seed.findGroupCoordinator({
      groupId: transactionalId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })
    return { host: coordinator.host, port: coordinator.port }
  } finally {
    await seed.disconnect()
  }
}

/**
 * Transactional offsets reach a `Kafka` instance's shared committed-offset
 * cache only after the coordinator confirmed the commit, once, and never over a newer cache
 * operation or a newer topic incarnation. Each case has two observers: a consumer of the same
 * instance, whose cache is read, and a separate raw `requireStable` OffsetFetch. The unconfirmed
 * outcomes are made without a proxy, by the chaos wrapper dropping the EndTxn request or its
 * answer on the transaction coordinator's connection only (application level, no kernel fault).
 *
 * Replay sets: durable log, none; committed offsets, as each case states; handler delivery may
 * repeat after an abort or a loss.
 */
describeVisibility('Transactional offsets become visible after a confirmed commit (live)', () => {
  beforeAll(async () => {
    const start = Runner.prototype.start
    jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runners.push(this)
      return start.apply(this, args)
    })
    const write = Cluster.prototype.markOffsetAsCommitted
    jest.spyOn(Cluster.prototype, 'markOffsetAsCommitted').mockImplementation(function (value) {
      cacheWrites.push({ cluster: this, ...value, offset: String(value.offset) })
      return write.call(this, value)
    })
    const warm = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING }).producer({
      transactionalId: `e2e-p9a-send-offsets-visibility-warm-${secureRandom()}`,
      maxInFlightRequests: 1,
    })
    await warm.connect()
    const transaction = await warm.transaction()
    await transaction.abort()
    await warm.disconnect()
  }, HOOK_BUDGET_MS)

  afterAll(() => {
    jest.restoreAllMocks()
  })

  describe.each(['classic', 'consumer'])('%s protocol', (groupProtocol) => {
    let kafka
    let chaos
    let consumer
    let consumerCluster
    let producer
    let admin
    let topic
    let groupId
    let transactionalId

    /** @returns {string|undefined} the same-instance consumer's cached offset of partition 0 */
    const cached = () => consumerCluster.committedOffsets({ groupId })[topic]?.[0]?.toString()

    /** @returns {object[]} the cache writes of this case's offsets that did not come from the consumer */
    const published = () =>
      cacheWrites.filter(
        (write) =>
          write.cluster !== consumerCluster && write.groupId === groupId && write.topic === topic
      )

    beforeEach(async () => {
      const id = secureRandom()
      topic = `e2e-p9a-send-offsets-visibility-${id}`
      groupId = `e2e-p9a-send-offsets-visibility-group-${id}`
      transactionalId = `e2e-p9a-send-offsets-visibility-txn-${id}`
      chaos = createChaosSocketFactory(defaultSocketFactory())
      await createTopic({ topic, partitions: PARTITIONS, replicas: 3 })
      kafka = new Kafka({
        clientId: `send-offsets-visibility-${id}`,
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
        socketFactory: chaos,
        requestTimeout: REQUEST_TIMEOUT_MS,
      })
      admin = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING }).admin()
      await admin.connect()
      const input = kafka.producer()
      await input.connect()
      await input.send({
        topic,
        messages: Array.from({ length: RECORDS }, (_, n) => ({ partition: 0, value: `${n}` })),
      })
      await input.disconnect()

      const before = runners.length
      consumer = kafka.consumer({
        groupId,
        groupProtocol,
        ...(groupProtocol === 'classic' ? { sessionTimeout: 30000, heartbeatInterval: 500 } : {}),
        metadataMaxAge: 1000,
      })
      const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve))
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await joined
      consumerCluster = runners[before].consumerGroup.cluster
      await consumer.commitOffsets([{ topic, partition: 0, offset: PREVIOUS }])

      producer = kafka.producer({
        transactionalId,
        maxInFlightRequests: 1,
        // Two EndTxn attempts per commit(): a case that drops both leaves the outcome unknown.
        retry: { retries: 1, initialRetryTime: 100, maxRetryTime: 200 },
      })
      await producer.connect()
    }, HOOK_BUDGET_MS)

    afterEach(async () => {
      chaos.sockets.forEach((socket) =>
        socket.destroy(Object.assign(new Error('Connection killed'), { code: 'ECONNRESET' }))
      )
      for (const client of [producer, consumer]) {
        await withTimeout(
          client?.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        )
      }
      await admin?.deleteTopics({ topics: [topic] }).catch(() => {})
      await withTimeout(
        admin?.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }, HOOK_BUDGET_MS)

    /** @returns {Promise<object>} a transaction that has sent the offsets `SENT` */
    const sendOffsets = async () => {
      const transaction = await producer.transaction()
      await transaction.sendOffsets({
        consumerGroupMetadata: consumer.groupMetadata(),
        topics: [{ topic, partitions: [{ partition: 0, offset: SENT }] }],
      })
      expect(published()).toEqual([])
      return transaction
    }

    /**
     * Arms every socket to the transaction coordinator, now and after each reconnect, until the
     * returned function is called.
     * @param {(socket: object) => void} apply
     * @returns {Promise<() => void>}
     */
    const onCoordinator = async (apply) => {
      const { port } = await transactionCoordinatorOf(transactionalId)
      return chaos.arm({ port }, apply)
    }

    /** @param {object} fields written with the case's identity @returns {void} */
    const record = (fields) =>
      writeEvidence(EVIDENCE, { groupProtocol, groupId, transactionalId, ...fields })

    test(
      'a confirmed abort publishes nothing and the broker keeps the previous commit',
      async () => {
        const transaction = await sendOffsets()
        await transaction.abort()
        const broker = await brokerOffset(groupId, topic)
        record({ case: 'abort', cached: cached(), broker, publications: published().length })

        expect(published()).toEqual([])
        expect(cached()).toBe(PREVIOUS)
        expect(broker).toBe(PREVIOUS)
      },
      CASE_BUDGET_MS
    )

    test(
      'an EndTxn that never left keeps the outcome unknown, and the confirmed retry publishes once',
      async () => {
        const transaction = await sendOffsets()
        const disarm = await onCoordinator((socket) =>
          socket.dropRequests({ apiKey: apiKeys.EndTxn })
        )
        const unknown = await transaction.commit().catch((error) => error)
        disarm()
        const pending = await brokerOffset(groupId, topic, { wait: false })
        const cachedWhileUnknown = cached()
        const publishedWhileUnknown = published().length
        await transaction.commit()
        const broker = await brokerOffset(groupId, topic)
        record({
          case: 'not-applied',
          unknown: unknown?.name,
          pending,
          cachedWhileUnknown,
          cached: cached(),
          broker,
          publications: published().length,
        })

        expect(unknown).toMatchObject({ retriable: true })
        expect(publishedWhileUnknown).toBe(0)
        expect(cachedWhileUnknown).toBe(PREVIOUS)
        expect(pending).toBeNull()
        expect(published().map(({ offset }) => offset)).toEqual([SENT])
        expect(cached()).toBe(SENT)
        expect(broker).toBe(SENT)
      },
      CASE_BUDGET_MS
    )

    test(
      'an applied EndTxn whose answer was lost publishes only after the confirmed retry, once',
      async () => {
        const transaction = await sendOffsets()
        const disarm = await onCoordinator((socket) =>
          socket.dropResponses(1, { apiKey: apiKeys.EndTxn })
        )
        const unknown = await transaction.commit().catch((error) => error)
        disarm()
        const appliedOffset = await brokerOffset(groupId, topic)
        const cachedWhileUnknown = cached()
        const publishedWhileUnknown = published().length
        await transaction.commit()
        record({
          case: 'applied-answer-lost',
          unknown: unknown?.name,
          appliedOffset,
          cachedWhileUnknown,
          cached: cached(),
          publications: published().length,
        })

        expect(unknown).toMatchObject({ retriable: true })
        expect(publishedWhileUnknown).toBe(0)
        expect(cachedWhileUnknown).toBe(PREVIOUS)
        expect(appliedOffset).toBe(SENT)
        expect(published().map(({ offset }) => offset)).toEqual([SENT])
        expect(cached()).toBe(SENT)
      },
      CASE_BUDGET_MS
    )

    test(
      'a confirmed commit publishes once',
      async () => {
        const transaction = await sendOffsets()
        await transaction.commit()
        const broker = await brokerOffset(groupId, topic)
        record({ case: 'commit', cached: cached(), broker, publications: published().length })

        expect(published().map(({ offset }) => offset)).toEqual([SENT])
        expect(cached()).toBe(SENT)
        expect(broker).toBe(SENT)
      },
      CASE_BUDGET_MS
    )

    test(
      'a commit confirmed by disconnect() publishes once, before the release',
      async () => {
        const transaction = await sendOffsets()
        const disarm = await onCoordinator((socket) =>
          socket.dropResponses(1, { apiKey: apiKeys.EndTxn })
        )
        const unknown = await transaction.commit().catch((error) => error)
        disarm()
        const publishedWhileUnknown = published().length
        await producer.disconnect()
        const broker = await brokerOffset(groupId, topic)
        record({
          case: 'disconnect-commit',
          unknown: unknown?.name,
          cached: cached(),
          broker,
          publications: published().length,
        })

        expect(unknown).toMatchObject({ retriable: true })
        expect(publishedWhileUnknown).toBe(0)
        expect(published().map(({ offset }) => offset)).toEqual([SENT])
        expect(cached()).toBe(SENT)
        expect(broker).toBe(SENT)
      },
      CASE_BUDGET_MS
    )

    test(
      'an older completion does not replace a newer commit of the same instance',
      async () => {
        const transaction = await sendOffsets()
        const disarm = await onCoordinator((socket) =>
          socket.dropResponses(1, { apiKey: apiKeys.EndTxn })
        )
        const unknown = await transaction.commit().catch((error) => error)
        disarm()
        await consumer.commitOffsets([{ topic, partition: 0, offset: NEWER }])
        await transaction.commit()
        const broker = await brokerOffset(groupId, topic)
        record({
          case: 'newer-commit',
          unknown: unknown?.name,
          cached: cached(),
          broker,
          publications: published().length,
        })

        expect(unknown).toMatchObject({ retriable: true })
        expect(published()).toEqual([])
        expect(cached()).toBe(NEWER)
        expect(broker).toBe(NEWER)
      },
      CASE_BUDGET_MS
    )

    test(
      'an older completion does not publish into a newer incarnation of the topic',
      async () => {
        const recreated = []
        consumer.on(consumer.events.TOPIC_RECREATED, ({ payload }) => recreated.push(payload))
        const transaction = await sendOffsets()
        const disarm = await onCoordinator((socket) =>
          socket.dropResponses(1, { apiKey: apiKeys.EndTxn })
        )
        const unknown = await transaction.commit().catch((error) => error)
        disarm()
        await admin.deleteTopics({ topics: [topic], timeout: 10000 })
        await waitForCondition(
          async () => recreated.length > 0 || !(await admin.listTopics()).includes(topic),
          WAIT_BUDGET
        )
        await admin
          .createTopics({ topics: [{ topic, numPartitions: PARTITIONS }], waitForLeaders: true })
          .catch(() => {})
        const observed = await waitForCondition(() => recreated.length > 0, WAIT_BUDGET)
        await transaction.commit()
        const broker = await brokerOffset(groupId, topic)
        record({
          case: 'newer-incarnation',
          unknown: unknown?.name,
          observed,
          cached: cached(),
          broker,
          publications: published().length,
        })

        expect(unknown).toMatchObject({ retriable: true })
        expect(observed).toBe(true)
        expect(published()).toEqual([])
        expect(cached()).not.toBe(SENT)
      },
      CASE_BUDGET_MS
    )
  })

  test(
    'aborted data keyed with four zero bytes stays hidden from read_committed readers',
    async () => {
      const id = secureRandom()
      const topic = `e2e-p9a-send-offsets-visibility-abort-key-${id}`
      await createTopic({ topic, partitions: 1, replicas: 3 })
      const kafka = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
      const transactional = kafka.producer({
        transactionalId: `e2e-p9a-send-offsets-visibility-abort-key-${id}`,
        maxInFlightRequests: 1,
      })
      const plain = kafka.producer()
      const consumer = kafka.consumer({ groupId: `e2e-p9a-abort-key-${id}` })
      const delivered = []
      try {
        await transactional.connect()
        await plain.connect()
        const aborted = await transactional.transaction()
        await aborted.send({
          topic,
          messages: [
            { value: 'aborted-before' },
            { key: Buffer.alloc(4), value: 'aborted-keyed' },
            { value: 'aborted-after' },
          ],
        })
        await aborted.abort()
        const committed = await transactional.transaction()
        await committed.send({
          topic,
          messages: [{ value: 'committed-1' }, { value: 'committed-2' }],
        })
        await committed.commit()
        await plain.send({ topic, messages: [{ value: 'plain' }] })

        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({
          eachMessage: async ({ message }) => delivered.push(String(message.value)),
        })
        await waitForCondition(() => delivered.length >= 3, WAIT_BUDGET)
        await sleep(QUIET_MS)

        const [uncommitted] = (await readLog({ brokers: plainTextBrokers(), topic })).values()
        const [readCommitted] = (
          await readLog({
            brokers: plainTextBrokers(),
            topic,
            isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
          })
        ).values()
        const data = uncommitted.records.filter(({ batchContext }) => !batchContext.isControlBatch)
        const raw = committedRecords(readCommitted).map(({ value }) => String(value))
        writeEvidence(EVIDENCE, {
          case: 'aborted-zero-key',
          consumer: delivered,
          raw,
          log: data.map(({ value }) => String(value)),
        })

        expect(data.map(({ value }) => String(value))).toEqual([
          'aborted-before',
          'aborted-keyed',
          'aborted-after',
          'committed-1',
          'committed-2',
          'plain',
        ])
        expect(delivered).toEqual(['committed-1', 'committed-2', 'plain'])
        expect(raw).toEqual(['committed-1', 'committed-2', 'plain'])
      } finally {
        for (const client of [consumer, plain, transactional]) {
          await withTimeout(
            client.disconnect().catch(() => {}),
            DISCONNECT_BUDGET_MS
          )
        }
      }
    },
    CASE_BUDGET_MS
  )

  test('every evidence line of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines).toHaveLength(15)
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})
