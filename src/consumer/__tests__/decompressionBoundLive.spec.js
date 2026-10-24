const zlib = require('zlib')
const { Kafka, CompressionTypes, logLevel } = require('../../../index')
const Broker = require('../../broker')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { sleep, waitForCondition } = require('./fixtures/liveScenario')
const {
  WireTap,
  createLiveSockets,
  repliesOf,
} = require('../../producer/__tests__/fixtures/liveWire')
const {
  CleanupStack,
  ProducerProbe,
  describeError,
  traceCalls,
  within,
} = require('../../producer/__tests__/fixtures/liveProbe')
const { caseTimeout, liveCase } = require('../../producer/__tests__/fixtures/bounded')

// Gated: it produces a compressed batch that decompresses past the consumer's bound. Run alone:
// `npm run test:e2e:decompression-bound`.
const describeBound = process.env.DECOMPRESSION_BOUND_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'decompression-bound'
const CASE_BUDGET_MS = 120000
const DISCONNECT_BUDGET_MS = 15000
const JOIN_BUDGET_MS = 30000
const BOMB_BYTES = 65536
const COMPRESSED_LIMIT_BYTES = 32768
const BOUND_BYTES = 32768
const RAISED_BOUND_BYTES = 131072
const QUIET_MS = 1500
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const MAX_REQUEST_SIZE = 1048576

const REPLAY = Object.freeze({
  log: 'the compressed batch and every marker stay in the log, none deleted or skipped on the broker',
  offsets: 'only the explicit healthy-partition commit; nothing derived crosses the bad batch',
  handlers: 'each marker once per run; at-least-once for the bomb across the restart arm',
})

/**
 * @param {object} admin a connected admin client
 * @param {string} groupId
 * @param {string} topic
 * @returns {Promise<Record<number, string>>} the committed offset of each partition, '-1' when none
 */
const committedOffsets = async (admin, groupId, topic) => {
  const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [topic] })
  return Object.fromEntries(partitions.map(({ partition, offset }) => [partition, String(offset)]))
}

/**
 * One consumer of the topic, with everything it was given and told.
 */
class Member {
  /**
   * @param {object} options
   * @param {Kafka} options.kafka
   * @param {string} options.topic
   * @param {string} options.groupId
   * @param {number} options.maxDecompressedBatchBytes
   */
  constructor({ kafka, topic, groupId, maxDecompressedBatchBytes }) {
    this.topic = topic
    this.consumer = kafka.consumer({
      groupId,
      maxDecompressedBatchBytes,
      checkCrcs: true,
      maxWaitTimeInMs: 500,
    })
    /** @type {Array<{ partition: number, offset: string, size: number, text: string, at: number }>} */
    this.deliveries = []
    this.halts = []
    this.crashes = []
    this.rebalances = 0
    this.requests = []
  }

  /**
   * Joins the group and starts delivering, with manual commits.
   * @returns {Promise<void>}
   */
  async start() {
    const { consumer } = this
    const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve))
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) =>
      this.halts.push({ ...payload, at: performance.now() })
    )
    consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload.error))
    consumer.on(consumer.events.REBALANCING, () => (this.rebalances += 1))
    consumer.on(consumer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    await consumer.connect()
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        this.deliveries.push({
          partition,
          offset: message.offset,
          size: message.value.length,
          text: message.value.toString('latin1', 0, 24),
          at: performance.now(),
        })
      },
    })
    await within(JOIN_BUDGET_MS, joined, 'The group join')
  }

  /**
   * @param {number} partition
   * @returns {string[]} what the handler was given from the partition, as text
   */
  texts(partition) {
    return this.deliveries.filter((d) => d.partition === partition).map(({ text }) => text)
  }
}

/**
 * Live proof that a batch which decompresses past `maxDecompressedBatchBytes` halts only its own
 * partition, as the byte bound (not the record-count ceiling or the broker's size limit) decides:
 * a gzip batch of one 65536-byte repeated value, whose compressed bytes are far under 32768,
 * against a 32768 bound. One halt names the partition, the codec, the limit, the offset and the
 * offset after the batch; the healthy partition keeps delivering; later Fetch requests leave the
 * halted partition out; no offset is committed over the bad batch; a seek to the named next
 * offset resumes it with no second halt. A fresh member with the bound raised to 131072 is given
 * the genuine record, once.
 *
 * Replay sets: the physical log keeps the compressed batch and the markers throughout; committed
 * offsets are only the explicit healthy-partition commit; handler delivery is exactly once per
 * marker within each run and at-least-once for the bomb across the restart arm.
 * @see https://kafka.apache.org/protocol#protocol_compression
 */
describeBound('Consumer > a compressed batch past the decompression bound (live)', () => {
  test(
    'halts only the partition holding the bomb, resumes it by seeking past the batch, and delivers it under a raised bound',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const id = secureRandom()
      const topic = `e2e-p9b-decompression-bound-${id}`
      const clientId = `e2e-p9b-decompression-bound-${id}`
      const brokers = plainTextBrokers()
      const sockets = createLiveSockets()
      const cleanup = new CleanupStack()
      const requests = []
      const fetches = traceCalls(Broker.prototype, 'fetch', ({ topics }) => {
        const positions = topics
          .filter((entry) => entry.topic === topic)
          .flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
        return positions.length > 0 ? { partitions: positions } : null
      })
      cleanup.add('fetch trace', () => fetches.restore())

      await cleanup.run(async () => {
        await createTopic({ topic, partitions: 2, replicas: 2 })
        const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.NOTHING })
        const producer = new Kafka({
          clientId,
          brokers,
          logLevel: logLevel.NOTHING,
          socketFactory: sockets.socketFactory,
        }).producer({ idempotent: true, linger: 0 })
        producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
        const probe = new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          maxRequestSize: MAX_REQUEST_SIZE,
        })
        const admin = kafka.admin()
        probe.install()
        cleanup.add('admission probe', () => probe.restore())
        cleanup.add('resources after disconnect', () => probe.settled())
        await admin.connect()
        cleanup.add('admin', () => admin.disconnect())
        await producer.connect()
        cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))

        const bomb = Buffer.alloc(BOMB_BYTES, 'a')
        await probe.capture(
          () =>
            producer.send({
              topic,
              messages: [{ partition: 0, value: bomb }],
              compression: CompressionTypes.GZIP,
              acks: -1,
            }),
          { signal: scope }
        )

        const tap = new WireTap()
        await readLog({ brokers, topic, open: tap.open })
        const stored = tap.batches().find((batch) => batch.baseOffset === '0' && batch.codec === 1)
        const sizes = {
          codec: stored?.codecName,
          compressed: stored?.body.length,
          decompressed: stored ? zlib.gunzipSync(stored.body).length : undefined,
        }

        const groupId = `e2e-p9b-decompression-bound-group-${id}`
        const bounded = new Member({
          kafka,
          topic,
          groupId,
          maxDecompressedBatchBytes: BOUND_BYTES,
        })
        cleanup.add('bounded consumer', () => bounded.consumer.disconnect())
        await bounded.start()
        const halted = await waitForCondition(() => bounded.halts.length >= 1, WAIT_BUDGET)
        if (!halted) throw new Error('The bomb did not halt partition 0 within the wait budget')
        const [halt] = bounded.halts
        await producer.send({
          topic,
          messages: [0, 1].map((index) => ({ partition: 1, value: `healthy-${index}` })),
          acks: -1,
        })
        const markersDelivered = await waitForCondition(
          () => bounded.texts(1).length >= 2,
          WAIT_BUDGET
        )
        await sleep(QUIET_MS)
        const afterHalt = fetches.entries.filter(({ at }) => at >= halt.at)
        const committedHeld = await committedOffsets(admin, groupId, topic)
        await bounded.consumer.commitOffsets([{ topic, partition: 1, offset: '2' }])
        const committedHealthy = await committedOffsets(admin, groupId, topic)
        const deliveriesWhileHalted = bounded.deliveries.filter(({ partition }) => partition === 0)
        const markers = bounded.deliveries.filter(({ partition }) => partition === 1)
        const rebalancesFirstArm = bounded.rebalances

        const recoveredAt = performance.now()
        await bounded.consumer.seek({ topic, partition: 0, offset: halt.nextOffset })
        await producer.send({
          topic,
          messages: [{ partition: 0, value: 'after-skip' }],
          acks: -1,
        })
        const resumed = await waitForCondition(() => bounded.texts(0).length >= 1, WAIT_BUDGET)
        await sleep(QUIET_MS)
        await bounded.consumer.disconnect()

        const raised = new Member({
          kafka,
          topic,
          groupId: `e2e-p9b-decompression-bound-raised-${id}`,
          maxDecompressedBatchBytes: RAISED_BOUND_BYTES,
        })
        cleanup.add('raised consumer', () => raised.consumer.disconnect())
        await raised.start()
        const delivered = await waitForCondition(() => raised.deliveries.length >= 4, WAIT_BUDGET)
        await sleep(QUIET_MS)
        await raised.consumer.disconnect()

        const log = await readLog({ brokers, topic })
        const sizesInLog = [...log.entries()]
          .sort(([a], [b]) => a - b)
          .map(([partition, { records }]) => ({
            partition,
            sizes: records.filter((r) => !r.batchContext.isControlBatch).map((r) => r.value.length),
          }))
        const closing = await probe.closing({
          topic: `e2e-p9b-decompression-bound-probe-${id}`,
          budgetMs: 120000,
        })

        writeEvidence(EVIDENCE, {
          scenario: 'decompression-bound',
          case: 'bomb',
          config: {
            maxDecompressedBatchBytes: BOUND_BYTES,
            raisedMaxDecompressedBatchBytes: RAISED_BOUND_BYTES,
            checkCrcs: true,
            autoCommit: false,
            bombBytes: BOMB_BYTES,
          },
          replay: REPLAY,
          wireVersions: wireVersionsOf([...requests, ...bounded.requests, ...raised.requests]),
          wire: { frames: sockets.frames.trace(), replies: repliesOf(requests) },
          sizes,
          halts: bounded.halts.map(({ at, error, ...fields }) => ({
            ...fields,
            error: describeError(error),
          })),
          healthy: bounded.texts(1),
          markersAfterHaltMs: markers.map(({ at }) => at - halt.at),
          recoveredAfterHaltMs: recoveredAt - halt.at,
          deliveriesWhileHalted,
          fetchesAfterHalt: afterHalt.map(({ partitions, error }) => ({
            partitions,
            error: describeError(error),
          })),
          committed: { held: committedHeld, healthy: committedHealthy },
          resumed: bounded.texts(0),
          rebalances: rebalancesFirstArm,
          raised: { deliveries: raised.deliveries, halts: raised.halts.length },
          log: sizesInLog,
          closing,
        })

        expect(sizes).toEqual({
          codec: 'gzip',
          compressed: expect.any(Number),
          decompressed: expect.any(Number),
        })
        expect(sizes.compressed).toBeLessThan(COMPRESSED_LIMIT_BYTES)
        expect(sizes.decompressed).toBeGreaterThan(BOUND_BYTES)
        expect(sizes.decompressed).toBeGreaterThanOrEqual(BOMB_BYTES)
        expect(halted).toBe(true)
        expect(bounded.halts).toHaveLength(1)
        expect(halt).toMatchObject({
          topic,
          partition: 0,
          reason: 'decompressed-size',
          codec: 'gzip',
          limit: BOUND_BYTES,
          offset: '0',
          nextOffset: '1',
        })
        expect(bounded.crashes).toEqual([])
        expect(rebalancesFirstArm).toBe(0)
        expect(deliveriesWhileHalted).toEqual([])
        expect(markersDelivered).toBe(true)
        expect(bounded.texts(1)).toEqual(['healthy-0', 'healthy-1'])
        expect(markers.map(({ at }) => at)).toEqual(
          markers.map(({ at }) => at).sort((a, b) => a - b)
        )
        expect(markers.every(({ at }) => at > halt.at && at < recoveredAt)).toBe(true)
        expect(afterHalt.some(({ partitions }) => partitions.includes(1))).toBe(true)
        for (const { partitions } of afterHalt) expect(partitions).not.toContain(0)
        expect(committedHeld).toEqual({ 0: '-1', 1: '-1' })
        expect(committedHealthy).toEqual({ 0: '-1', 1: '2' })
        expect(resumed).toBe(true)
        expect(bounded.texts(0)).toEqual(['after-skip'])
        expect(bounded.halts).toHaveLength(1)
        expect(delivered).toBe(true)
        expect(raised.halts).toEqual([])
        expect(raised.crashes).toEqual([])
        const bombDeliveries = raised.deliveries.filter(({ size }) => size === BOMB_BYTES)
        expect(bombDeliveries).toHaveLength(1)
        expect(bombDeliveries[0]).toMatchObject({ partition: 0, offset: '0' })
        expect(sizesInLog).toEqual([
          { partition: 0, sizes: [BOMB_BYTES, 'after-skip'.length] },
          { partition: 1, sizes: ['healthy-0'.length, 'healthy-1'.length] },
        ])
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
