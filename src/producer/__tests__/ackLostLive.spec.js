const { Kafka, logLevel } = require('../../../index')
const Broker = require('../../broker')
const apiKeys = require('../../protocol/requests/apiKeys')
const { readData } = require('./fixtures/liveProbe')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const { ResponseDrop, createLiveSockets, repliesOf, scanBatches } = require('./fixtures/liveWire')
const {
  COORDINATOR_TYPES,
  CleanupStack,
  ProduceLedger,
  ProducerProbe,
  coordinatorOf,
  describeError,
  leaderOf,
  settle,
  traceCalls,
  within,
} = require('./fixtures/liveProbe')
const { caseTimeout, liveCase } = require('./fixtures/bounded')

// Gated: it drops one Produce, Fetch or OffsetCommit answer on purpose. Run alone:
// `npm run test:e2e:ack-lost`.
const describeAckLost = process.env.ACK_LOST_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'ack-lost'
const VARIANT_BUDGET_MS = 90000
const REQUEST_TIMEOUT_MS = 1000
const DELIVERY_TIMEOUT_MS = 10000
const MAX_WAIT_MS = 500
const DRAIN_MS = 1500
const QUIET_MS = 1500
const DISCONNECT_BUDGET_MS = 15000
const JOIN_BUDGET_MS = 30000
const FIXED_TIMESTAMP = 1700000000000
const DUPLICATE_SEQUENCE_NUMBER = 46
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const APPLIED_BUDGET = { attempts: 100, delay: 20 } // inside the first request's timeout
const OBSERVE_BUDGET = { attempts: 60, delay: 50 }
/** Enough retries for one dropped answer, with a backoff that does not draw a random jitter. */
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000, factor: 0 }

/** The settings every variant declares on its faulted client. */
const FAULT_CLIENT = Object.freeze({
  requestTimeout: REQUEST_TIMEOUT_MS,
  enforceRequestTimeout: true,
})

/** @returns {Kafka} a client to the local cluster, with the given options */
const kafkaOf = (options = {}) =>
  new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING, ...options })

/**
 * The topic, the admin and setup producer, and every resource a consumer variant holds, closed
 * together. The setup producer and the admin sit on clients no fault touches.
 */
class ConsumerRig {
  /** @param {string} name the variant, for the topic and group names */
  constructor(name) {
    this.id = secureRandom()
    this.topic = `e2e-p9b-ack-lost-${name}-${this.id}`
    this.groupId = `e2e-p9b-ack-lost-${name}-group-${this.id}`
    this.sockets = createLiveSockets()
    this.cleanup = new CleanupStack()
    this.delivered = []
    this.rebalances = 0
    this.requests = []
  }

  /**
   * Creates the topic and writes `values` to it through the setup producer.
   * @param {string[]} values
   * @returns {Promise<void>}
   */
  async start(values) {
    await createTopic({ topic: this.topic, partitions: 1, replicas: 3 })
    this.admin = kafkaOf().admin()
    await this.admin.connect()
    this.cleanup.add('admin', () => this.admin.disconnect())
    this.setup = kafkaOf().producer({ idempotent: true })
    await this.setup.connect()
    this.cleanup.add('setup producer', () => this.setup.disconnect())
    await this.append(values)
  }

  /**
   * @param {string[]} values
   * @returns {Promise<void>} once the setup producer's send was acknowledged
   */
  async append(values) {
    await this.setup.send({
      topic: this.topic,
      messages: values.map((value) => ({ value })),
      acks: -1,
    })
  }

  /**
   * Starts a consumer of the group with manual commits and waits for its first join.
   * @param {Kafka} kafka the client to start it on
   * @param {string[]} [into] where its handler collects the values it is given
   * @returns {Promise<object>} the running consumer
   */
  async consume(kafka, into = this.delivered) {
    const consumer = kafka.consumer({
      groupId: this.groupId,
      maxWaitTimeInMs: MAX_WAIT_MS,
      retry: { retries: 5, initialRetryTime: 100, factor: 0 },
    })
    const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve))
    consumer.on(consumer.events.REBALANCING, () => (this.rebalances += 1))
    consumer.on(consumer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.cleanup.add('consumer', () => consumer.disconnect())
    await consumer.connect()
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        into.push(String(message.value))
      },
    })
    await within(JOIN_BUDGET_MS, joined, 'The first group join')
    return consumer
  }

  /** @returns {Promise<string>} the offset the broker holds for the group, '-1' when none */
  async committed() {
    const [{ partitions }] = await this.admin.fetchOffsets({
      groupId: this.groupId,
      topics: [this.topic],
    })
    return String(partitions[0].offset)
  }
}

/**
 * Live proof that a lost answer, whichever request it belongs to, costs neither a record nor a
 * position. A Produce answer lost after the broker appended is retried with the same producer id,
 * epoch and sequence, and the caller resolves: once in the log for an idempotent producer, twice
 * for a plain one. A Fetch answer lost after the broker served it is retried at the same position
 * and the handler sees each record once, in order. An OffsetCommit answer lost after the broker
 * applied it is retried at the same offset, never below the last confirmed one, and a restarted
 * member resumes from it. Each variant drops exactly one answer of exactly one api, counted across
 * reconnects, after the broker side was observed through an independent reader.
 *
 * Replay sets, by variant: Produce, the log holds the record once (idempotent) or twice (plain),
 * offsets and handlers do not apply; Fetch, a fixed three-record log, offsets only what was
 * handled, the handler sees each record exactly once; OffsetCommit, a fixed three-record log, the
 * committed offset is the processed 3 and never lower, the handler sees each record once and the
 * restarted member is given only the record appended after the commit.
 * @see https://kafka.apache.org/protocol#The_Messages_Produce
 */
describeAckLost('Producer and consumer > a lost answer is retried without loss (live)', () => {
  test.each([
    ['idempotent', 1],
    ['plain', 2],
  ])(
    'a Produce answer lost after the append is retried and the caller resolves: %s producer, %i copy in the log',
    liveCase(VARIANT_BUDGET_MS, async (scope, kind, copies) => {
      const id = secureRandom()
      const topic = `e2e-p9b-ack-lost-produce-${kind}-${id}`
      const clientId = `e2e-p9b-ack-lost-produce-${kind}-${id}`
      const brokers = plainTextBrokers()
      const sockets = createLiveSockets({ retain: [apiKeys.Produce] })
      const ledger = new ProduceLedger()
      const requests = []
      const cleanup = new CleanupStack()
      const maxRequestSize = 1048576

      await cleanup.run(async () => {
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const producer = kafkaOf({
          clientId,
          socketFactory: sockets.socketFactory,
          ...FAULT_CLIENT,
        }).producer({
          idempotent: kind === 'idempotent',
          linger: 0,
          deliveryTimeout: DELIVERY_TIMEOUT_MS,
          maxRequestSize,
          retry: RETRY,
        })
        producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
        const probe = new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          maxRequestSize,
        })
        const admin = kafkaOf().admin()
        ledger.install()
        cleanup.add('produce ledger', () => ledger.restore())
        probe.install()
        cleanup.add('admission probe', () => probe.restore())
        cleanup.add('resources after disconnect', () => probe.settled())
        await admin.connect()
        cleanup.add('admin', () => admin.disconnect())
        await producer.connect()
        cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))

        await probe.capture(
          () => producer.send({ topic, messages: [{ value: 'warm' }], acks: -1 }),
          { signal: scope }
        )
        const leader = await leaderOf(admin, topic)
        const drop = new ResponseDrop(sockets.chaos, {
          port: leader.port,
          apiKey: apiKeys.Produce,
        }).arm()
        cleanup.add('response drop', () => drop.release())

        const rawValues = async () =>
          (await readData(brokers, topic)).map(({ value }) => String(value))
        const mark = sockets.frames.mark()
        const sent = settle(
          producer.send({
            topic,
            messages: [{ value: 'subject', timestamp: FIXED_TIMESTAMP }],
            acks: -1,
          })
        )
        let seen = []
        const applied = await waitForCondition(async () => {
          seen = await rawValues()
          return seen.includes('subject')
        }, APPLIED_BUDGET)
        const writesWhenApplied = sockets.frames.since(apiKeys.Produce, mark).length
        const copiesWhenApplied = seen.filter((value) => value === 'subject').length

        const outcome = await sent
        const frames = sockets.frames.since(apiKeys.Produce, mark)
        const batches = frames.flatMap(({ buffer }) => scanBatches(buffer))
        sockets.frames.release()
        const final = (await rawValues()).filter((value) => value === 'subject').length
        const calls = ledger.of(topic).slice(-frames.length)
        const closing = await probe.closing({
          topic: `e2e-p9b-ack-lost-produce-probe-${kind}-${id}`,
          budgetMs: DELIVERY_TIMEOUT_MS,
        })
        await producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })

        writeEvidence(EVIDENCE, {
          scenario: 'ack-lost',
          case: 'produce',
          kind,
          config: {
            ...FAULT_CLIENT,
            linger: 0,
            deliveryTimeout: DELIVERY_TIMEOUT_MS,
            retry: RETRY,
          },
          replay: {
            log: `${copies} copy of the record: the append before the loss, and the resend for a plain producer`,
            offsets: 'n/a',
            handlers: 'n/a',
          },
          wireVersions: wireVersionsOf(requests),
          wire: { frames: sockets.frames.trace(), replies: repliesOf(requests) },
          drops: drop.dropped,
          applied: { seen: applied, writes: writesWhenApplied, copies: copiesWhenApplied },
          resolved: describeError(outcome.error),
          batches: batches.map((batch) => ({
            producerId: batch.producerId,
            producerEpoch: batch.producerEpoch,
            baseSequence: batch.baseSequence,
            count: batch.count,
            bytes: batch.bytes.length,
          })),
          calls: calls.map((call) => ({
            partitions: call.partitions,
            errorCodes: call.results?.map(({ errorCode }) => errorCode) ?? null,
            error: describeError(call.error),
          })),
          log: { copies: final },
          closing,
        })

        expect(applied).toBe(true)
        expect(writesWhenApplied).toBe(1)
        expect(copiesWhenApplied).toBe(1)
        expect(drop.dropped).toBe(1)
        expect(outcome.error).toBeNull()
        expect(batches.length).toBeGreaterThanOrEqual(2)
        for (const batch of batches) {
          expect(batch.bytes.equals(batches[0].bytes)).toBe(true)
          expect(batch).toMatchObject({
            producerId: batches[0].producerId,
            producerEpoch: batches[0].producerEpoch,
            baseSequence: batches[0].baseSequence,
          })
        }
        expect(calls[0].error).not.toBeNull()
        expect([0, DUPLICATE_SEQUENCE_NUMBER]).toContain(calls.at(-1).results[0].errorCode)
        expect(final).toBe(copies)
      })
    }),
    caseTimeout(VARIANT_BUDGET_MS)
  )

  test(
    'a Fetch answer lost after the broker served it is retried at the same position and the handler sees each record once',
    liveCase(VARIANT_BUDGET_MS, async () => {
      const rig = new ConsumerRig('fetch')
      const fetches = traceCalls(Broker.prototype, 'fetch', ({ topics }) => {
        const positions = topics
          .filter(({ topic }) => topic === rig.topic)
          .flatMap(({ partitions }) =>
            partitions.map(({ partition, fetchOffset }) => ({
              partition,
              offset: String(fetchOffset),
            }))
          )
        return positions.length > 0 ? { positions } : null
      })
      rig.cleanup.add('fetch trace', () => fetches.restore())

      await rig.cleanup.run(async () => {
        await rig.start([])
        const consumer = await rig.consume(
          kafkaOf({
            clientId: `e2e-p9b-ack-lost-fetch-${rig.id}`,
            socketFactory: rig.sockets.socketFactory,
            ...FAULT_CLIENT,
          })
        )
        const topicRef = { topic: rig.topic }
        consumer.pause([topicRef])
        await sleep(DRAIN_MS)
        const leader = await leaderOf(rig.admin, rig.topic)
        const rebalancesAtFault = rig.rebalances
        const idleSince = fetches.entries.at(-1)?.at ?? 0
        const drop = new ResponseDrop(rig.sockets.chaos, {
          port: leader.port,
          apiKey: apiKeys.Fetch,
        }).arm()
        rig.cleanup.add('response drop', () => drop.release())

        await rig.append(['a', 'b', 'c'])
        const before = fetches.entries.length
        const resumedAt = performance.now()
        consumer.resume([topicRef])
        const handled = await waitForCondition(() => rig.delivered.length >= 3, WAIT_BUDGET)
        await sleep(QUIET_MS)
        const [dropped, retried] = fetches.entries.slice(before)
        const committedBefore = await rig.committed()
        await consumer.commitOffsets([{ topic: rig.topic, partition: 0, offset: '3' }])
        const committedAfter = await rig.committed()
        const rebalancesAtEnd = rig.rebalances

        writeEvidence(EVIDENCE, {
          scenario: 'ack-lost',
          case: 'fetch',
          config: { ...FAULT_CLIENT, maxWaitTimeInMs: MAX_WAIT_MS, retry: RETRY },
          replay: {
            log: 'a fixed three-record log, never rewritten',
            offsets: 'only the handled position 3, committed after the fault',
            handlers: 'a, b, c exactly once, in order',
          },
          wireVersions: wireVersionsOf(rig.requests),
          wire: { frames: rig.sockets.frames.trace(), replies: repliesOf(rig.requests) },
          drops: drop.dropped,
          idleMs: resumedAt - idleSince,
          trace: fetches.entries.slice(before),
          delivered: rig.delivered,
          rebalances: { atFault: rebalancesAtFault, atEnd: rebalancesAtEnd },
          committed: { before: committedBefore, after: committedAfter },
        })

        expect(handled).toBe(true)
        expect(drop.dropped).toBe(1)
        expect(dropped.error).toMatchObject({ name: 'KafkaJSRequestTimeoutError' })
        expect(dropped.positions).toEqual([{ partition: 0, offset: '0' }])
        expect(retried.positions).toEqual(dropped.positions)
        expect(retried.error).toBeNull()
        expect(rig.delivered).toEqual(['a', 'b', 'c'])
        expect(rebalancesAtEnd).toBe(rebalancesAtFault)
        expect(committedBefore).toBe('-1')
        expect(committedAfter).toBe('3')
      })
    }),
    caseTimeout(VARIANT_BUDGET_MS)
  )

  test(
    'an OffsetCommit answer lost after the broker applied it is retried at the same offset and a restarted member resumes from it',
    liveCase(VARIANT_BUDGET_MS, async () => {
      const rig = new ConsumerRig('offset-commit')
      const commits = traceCalls(Broker.prototype, 'offsetCommit', ({ groupId, topics }) =>
        groupId === rig.groupId
          ? {
              offsets: topics.flatMap(({ partitions }) =>
                partitions.map(({ partition, offset }) => ({ partition, offset: String(offset) }))
              ),
            }
          : null
      )
      rig.cleanup.add('offset commit trace', () => commits.restore())

      await rig.cleanup.run(async () => {
        await rig.start(['a', 'b', 'c'])
        const consumer = await rig.consume(
          kafkaOf({
            clientId: `e2e-p9b-ack-lost-offset-commit-${rig.id}`,
            socketFactory: rig.sockets.socketFactory,
            ...FAULT_CLIENT,
          })
        )
        const handled = await waitForCondition(() => rig.delivered.length >= 3, WAIT_BUDGET)
        await sleep(QUIET_MS)
        const deliveredBeforeFault = [...rig.delivered]
        const rebalancesAtFault = rig.rebalances
        const coordinator = await coordinatorOf(rig.groupId, COORDINATOR_TYPES.GROUP)
        const drop = new ResponseDrop(rig.sockets.chaos, {
          port: coordinator.port,
          apiKey: apiKeys.OffsetCommit,
        }).arm()
        rig.cleanup.add('response drop', () => drop.release())

        let settled = false
        const commit = settle(
          consumer.commitOffsets([{ topic: rig.topic, partition: 0, offset: '3' }])
        ).then((outcome) => {
          settled = true
          return outcome
        })
        const applied = await waitForCondition(
          async () => (await rig.committed()) === '3',
          OBSERVE_BUDGET
        )
        const settledWhenApplied = settled
        const outcome = await commit
        const committedAfter = await rig.committed()
        const rebalancesAtEnd = rig.rebalances
        await consumer.disconnect()

        await rig.append(['d'])
        const resumed = []
        await rig.consume(kafkaOf({ clientId: `e2e-p9b-ack-lost-restart-${rig.id}` }), resumed)
        const resumedHandled = await waitForCondition(() => resumed.length >= 1, WAIT_BUDGET)
        await sleep(QUIET_MS)

        writeEvidence(EVIDENCE, {
          scenario: 'ack-lost',
          case: 'offset-commit',
          config: { ...FAULT_CLIENT, maxWaitTimeInMs: MAX_WAIT_MS, retry: RETRY },
          replay: {
            log: 'a fixed three-record log, then one record appended after the commit',
            offsets: 'the processed 3, never lower than the last confirmed commit',
            handlers: 'a, b, c once without a restart; only d after the restart',
          },
          wireVersions: wireVersionsOf(rig.requests),
          wire: { frames: rig.sockets.frames.trace(), replies: repliesOf(rig.requests) },
          drops: drop.dropped,
          commits: commits.entries,
          applied: { seen: applied, settledWhenApplied },
          committed: committedAfter,
          noRestart: {
            delivered: deliveredBeforeFault,
            rebalances: [rebalancesAtFault, rebalancesAtEnd],
          },
          restart: { delivered: resumed },
        })

        expect(handled).toBe(true)
        expect(deliveredBeforeFault).toEqual(['a', 'b', 'c'])
        expect(drop.dropped).toBe(1)
        expect(applied).toBe(true)
        expect(settledWhenApplied).toBe(false)
        expect(outcome.error).toBeNull()
        expect(commits.entries.length).toBeGreaterThanOrEqual(2)
        for (const { offsets } of commits.entries) {
          expect(offsets).toEqual([{ partition: 0, offset: '3' }])
        }
        expect(committedAfter).toBe('3')
        expect(rebalancesAtEnd).toBe(rebalancesAtFault)
        expect(resumedHandled).toBe(true)
        expect(resumed).toEqual(['d'])
      })
    }),
    caseTimeout(VARIANT_BUDGET_MS)
  )
})
