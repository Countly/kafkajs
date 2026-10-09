const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const { KafkaJSTransactionAbortableError } = require('../../errors')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const {
  sleep,
  waitForCondition,
  withTimeout,
} = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: every arm holds a transaction open on purpose. Run alone: `npm run test:e2e:send-offsets`.
const describeSendOffsets = process.env.SEND_OFFSETS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'send-offsets'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 120000
const RECORDS = 10
const MAX_POLL_INTERVAL_MS = 8000
// Held past the poll deadline after the last rebalance: a waiting member that did not count as
// polling would be evicted, and the lookup reaches its bound (the poll deadline) once.
const HOLD_AFTER_REBALANCE_MS = MAX_POLL_INTERVAL_MS + 2000
// A joining member must get its partition well before the lookup's own 8 s bound: a rebalance that
// waited for the open transaction would land at that bound, and a healthy one lands within a second
// or two.
const REBALANCE_YIELD_BUDGET_MS = 5000
// A waiting lookup re-sends at most once every 100 ms, and every broker's fetcher runs its own.
const UNSTABLE_RETRY_INTERVAL_MS = 100
const FETCHERS = plainTextBrokers().length
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500 // how long an output reader waits for a record that should not exist
const DISCONNECT_BUDGET_MS = 15000
const HOOK_BUDGET_MS = 60000
const STALE_MEMBER_CODES = [
  'ILLEGAL_GENERATION',
  'UNKNOWN_MEMBER_ID',
  'FENCED_INSTANCE_ID',
  'STALE_MEMBER_EPOCH',
]

/** @param {() => boolean|Promise<boolean>} predicate @returns {Promise<boolean>} whether it held within the wait budget */
const waitFor = (predicate) => waitForCondition(predicate, WAIT_BUDGET)

/**
 * The membership a transactional offset commit carried.
 * @param {{ generationId: number, memberId: string, groupInstanceId: string|null }} [request]
 */
const triple = ({ generationId, memberId, groupInstanceId } = {}) => ({
  generationId,
  memberId,
  groupInstanceId,
})

/**
 * Live proof of KIP-447 through the producer: offsets committed with the consumer's group
 * membership, a joining consumer held back by `RequireStable` while a transaction holding its
 * group's offsets is open, and a stale membership making `sendOffsets` abortable. Every OffsetFetch
 * and TxnOffsetCommit any client in the process sends is observed on `Broker`, because the
 * `REQUEST` event carries no request body.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
 */
describeSendOffsets('Producer > sendOffsets with group metadata (live)', () => {
  /** @type {object[]} the arguments of every TxnOffsetCommit sent */
  const txnOffsetCommits = []
  /** @type {{ groupId: string, broker: object, at: number, outcome: string }[]} every OffsetFetch answered */
  const offsetFetches = []

  beforeAll(() => {
    const { txnOffsetCommit, offsetFetch } = Broker.prototype
    jest.spyOn(Broker.prototype, 'txnOffsetCommit').mockImplementation(function (request) {
      txnOffsetCommits.push(request)
      return txnOffsetCommit.call(this, request)
    })
    jest.spyOn(Broker.prototype, 'offsetFetch').mockImplementation(async function (request) {
      const answered = (outcome) =>
        offsetFetches.push({ groupId: request.groupId, broker: this, at: Date.now(), outcome })
      try {
        const response = await offsetFetch.call(this, request)
        answered('ok')
        return response
      } catch (e) {
        answered(e.type)
        throw e
      }
    })
  })

  afterAll(() => {
    jest.restoreAllMocks()
  })

  describe.each(['classic', 'consumer'])('%s protocol', (groupProtocol) => {
    const membership =
      groupProtocol === 'consumer'
        ? { groupProtocol }
        : { groupProtocol, sessionTimeout: 30000, heartbeatInterval: 500 }

    let kafka
    let clients
    let input
    let output
    let groupId
    let admin

    beforeEach(async () => {
      kafka = new Kafka({
        clientId: `e2e-p7-send-offsets-${secureRandom()}`,
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })
      clients = []
      admin = null
      input = `e2e-p7-send-offsets-in-${secureRandom()}`
      output = `e2e-p7-send-offsets-out-${secureRandom()}`
      groupId = `e2e-p7-send-offsets-group-${secureRandom()}`
      await createTopic({ topic: input, partitions: 2 })
      await createTopic({ topic: output })
    }, HOOK_BUDGET_MS)

    afterEach(async () => {
      for (const client of clients.reverse()) {
        await withTimeout(
          client.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        )
      }
    }, HOOK_BUDGET_MS)

    /** A connected client of `kafka`, disconnected after the test. */
    const connected = async (client) => {
      clients.push(client)
      await client.connect()
      return client
    }

    /**
     * Writes `count` records to each of `partitions` of the input topic, values
     * `value-<partition>-<n>` from `first`, and returns them.
     */
    const produceInput = async (first, { count = RECORDS, partitions = [0] } = {}) => {
      const producer = await connected(kafka.producer())
      const messages = partitions.flatMap((partition) =>
        Array.from({ length: count }, (_, index) => ({
          partition,
          value: `value-${partition}-${first + index}`,
        }))
      )
      await producer.send({ topic: input, messages })
      return messages
    }

    /**
     * Starts a consumer of the input topic in the scenario's group that commits nothing itself,
     * and records the events that would show it was evicted or restarted.
     * @param {object} [options] extra consumer options
     */
    const startConsumer = async (options = {}) => {
      const consumer = await connected(
        kafka.consumer({ groupId, maxWaitTimeInMs: 100, ...membership, ...options })
      )
      const state = { consumer, consumed: [], joins: [], pollExpiries: 0, crashes: 0 }
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
        state.joins.push({ at: Date.now(), partitions: payload.memberAssignment[input] || [] })
      )
      consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, () => (state.pollExpiries += 1))
      consumer.on(consumer.events.CRASH, () => (state.crashes += 1))
      await consumer.subscribe({ topics: [input], fromBeginning: true })
      state.run = () =>
        consumer.run({
          autoCommit: false,
          eachMessage: async ({ partition, message }) => {
            state.consumed.push({
              partition,
              offset: Number(message.offset),
              value: message.value.toString(),
            })
          },
        })
      await state.run()
      return state
    }

    /**
     * Every value a reader sees on the output topic, once it has gone quiet: a `read_committed`
     * reader, or with `readUncommitted` one that also sees records an abort discarded.
     */
    const readOutput = async (expected, { readUncommitted = false } = {}) => {
      const reader = await connected(
        kafka.consumer({
          groupId: `e2e-p7-send-offsets-reader-${secureRandom()}`,
          readUncommitted,
          ...membership,
        })
      )
      const values = []
      await reader.subscribe({ topics: [output], fromBeginning: true })
      await reader.run({
        eachMessage: async ({ message }) => values.push(message.value.toString()),
      })
      await waitFor(() => values.length >= expected)
      await sleep(QUIET_MS)
      return values
    }

    /** The group's committed offset on one of the input topic's partitions. */
    const committedOffset = async (partition = 0) => {
      admin ??= await connected(kafka.admin())
      const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [input] })
      return partitions.find((entry) => entry.partition === partition).offset
    }

    /**
     * A committed offset is applied by the transaction's marker, which lands after `commit()`
     * returns, so a positive read polls until it reaches `expected`; the last value read is returned.
     */
    const committedOffsetReaching = async (expected, partition = 0) => {
      let offset
      await waitFor(async () => {
        offset = await committedOffset(partition)
        return offset === String(expected)
      })
      return offset
    }

    /** The input topic's `partitions` committed up to `offset`. */
    const offsetsAt = (offset, partitions = [0]) => [
      {
        topic: input,
        partitions: partitions.map((partition) => ({ partition, offset: String(offset) })),
      },
    ]

    /** A transactional producer, with the versions of the requests it sends. */
    const transactionalProducer = async () => {
      const producer = await connected(
        kafka.producer({ transactionalId: `e2e-p7-send-offsets-${secureRandom()}` })
      )
      const versions = []
      producer.on(producer.events.REQUEST, ({ payload }) =>
        versions.push(`${payload.apiName} v${payload.apiVersion}`)
      )
      return { producer, versions }
    }

    /** The membership of the last transactional offset commit this scenario's group received. */
    const lastTxnOffsetCommit = () =>
      txnOffsetCommits.filter((request) => request.groupId === groupId).at(-1)

    test(
      'offsets and output commit or abort together; the commit carries the consumer’s membership, and the legacy group id still commits',
      async () => {
        await produceInput(0)
        const reader = await startConsumer()
        await waitFor(() => reader.consumed.length >= RECORDS)
        const { producer, versions } = await transactionalProducer()
        const transform = (prefix) =>
          reader.consumed.map(({ value }) => ({ value: `${prefix}-${value}` }))

        const aborted = await producer.transaction()
        await aborted.send({ topic: output, messages: transform('aborted') })
        await aborted.sendOffsets({
          consumerGroupMetadata: reader.consumer.groupMetadata(),
          topics: offsetsAt(RECORDS, [1]),
        })
        await aborted.abort()

        const metadata = reader.consumer.groupMetadata()
        const committed = await producer.transaction()
        await committed.send({ topic: output, messages: transform('committed') })
        await committed.sendOffsets({ consumerGroupMetadata: metadata, topics: offsetsAt(RECORDS) })
        await committed.commit()
        const afterCommit = await committedOffsetReaching(RECORDS)
        const fenced = triple(lastTxnOffsetCommit())

        await produceInput(RECORDS)
        await waitFor(() => reader.consumed.length >= 2 * RECORDS)
        const legacy = await producer.transaction()
        await legacy.send({ topic: output, messages: [{ value: 'legacy' }] })
        await legacy.sendOffsets({ consumerGroupId: groupId, topics: offsetsAt(2 * RECORDS) })
        await legacy.commit()
        const afterLegacy = await committedOffsetReaching(2 * RECORDS)
        const unfenced = triple(lastTxnOffsetCommit())
        const abortedPartition = await committedOffset(1)
        const values = await readOutput(RECORDS + 1)
        const uncommitted = await readOutput(2 * RECORDS + 1, { readUncommitted: true })

        writeEvidence(EVIDENCE, {
          scenario: 'commit-and-abort',
          groupProtocol,
          metadata,
          fenced,
          unfenced,
          committedOffsets: { afterCommit, afterLegacy, abortedPartition },
          abortedRecordsWritten: uncommitted.filter((value) => value.startsWith('aborted-')).length,
          requests: [...new Set(versions)],
        })
        expect(abortedPartition).toEqual('-1')
        expect(afterCommit).toEqual(String(RECORDS))
        expect(afterLegacy).toEqual(String(2 * RECORDS))
        expect(fenced).toEqual({
          generationId: metadata.generationId,
          memberId: metadata.memberId,
          groupInstanceId: metadata.groupInstanceId,
        })
        expect(metadata.generationId).toBeGreaterThan(0)
        expect(unfenced).toEqual({ generationId: -1, memberId: '', groupInstanceId: null })
        expect(versions).toContain('TxnOffsetCommit v5')
        expect(versions.some((version) => version.startsWith('AddOffsetsToTxn'))).toBe(false)
        expect(uncommitted.filter((value) => value.startsWith('aborted-'))).toHaveLength(RECORDS)
        expect(values.filter((value) => value.startsWith('aborted-'))).toEqual([])
        expect(values.filter((value) => value.startsWith('committed-'))).toHaveLength(RECORDS)
        expect(values).toContain('legacy')
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'a joining consumer waits for the open transaction holding its group’s offsets, a rebalance meanwhile does not, and nobody is evicted',
      async () => {
        const partitions = [0, 1]
        const baseline = RECORDS / 2
        await produceInput(0, { partitions })
        const { producer } = await transactionalProducer()
        const settled = await producer.transaction()
        await settled.sendOffsets({
          consumerGroupId: groupId,
          topics: offsetsAt(baseline, partitions),
        })
        await settled.commit()
        const open = await producer.transaction()
        await open.sendOffsets({ consumerGroupId: groupId, topics: offsetsAt(RECORDS, partitions) })

        const unstable = () =>
          offsetFetches.filter(
            (fetch) => fetch.groupId === groupId && fetch.outcome === 'UNSTABLE_OFFSET_COMMIT'
          )
        const waiting = await startConsumer({ maxPollInterval: MAX_POLL_INTERVAL_MS })
        const waited = await waitFor(() => unstable().length > 0)
        const firstUnstableAt = unstable()[0]?.at
        const joiningStartedAt = Date.now()
        const joining = await startConsumer({ maxPollInterval: MAX_POLL_INTERVAL_MS })
        const rebalanced = await waitFor(() =>
          joining.joins.some((join) => join.partitions.length > 0)
        )
        await sleep(HOLD_AFTER_REBALANCE_MS)

        const abortStartedAt = Date.now()
        await open.abort()
        const members = [waiting, joining]
        const consumed = () => members.flatMap((member) => member.consumed)
        await waitFor(() => consumed().length >= partitions.length * (RECORDS - baseline))

        const answered = offsetFetches.filter(
          (fetch) => fetch.groupId === groupId && fetch.outcome === 'ok'
        )
        const joined = joining.joins.find((join) => join.partitions.length > 0)
        const waitingAfterRebalance = waiting.joins.filter((join) => join.at >= joiningStartedAt)
        const retriesByBroker = [...new Set(unstable().map(({ broker }) => broker))].map(
          (broker) => unstable().filter((fetch) => fetch.broker === broker).length
        )
        const retryCeiling =
          FETCHERS * Math.ceil((abortStartedAt - firstUnstableAt) / UNSTABLE_RETRY_INTERVAL_MS + 1)
        const offsetsByPartition = partitions.map((partition) =>
          consumed()
            .filter((record) => record.partition === partition)
            .map(({ offset }) => offset)
            .sort((a, b) => a - b)
        )

        writeEvidence(EVIDENCE, {
          scenario: 'stable-offsets',
          groupProtocol,
          unstableOffsetCommitRetries: unstable().length,
          retriesByBroker,
          retryCeiling,
          firstUnstableAt,
          joinedAt: joined && joined.at,
          joiningPartitions: joined && joined.partitions,
          waitingPartitions: waitingAfterRebalance.map((join) => join.partitions),
          abortStartedAt,
          firstAnsweredAt: answered.map(({ at }) => at).sort((a, b) => a - b)[0],
          pollExpiries: members.map(({ pollExpiries }) => pollExpiries),
          crashes: members.map(({ crashes }) => crashes),
          offsetsByPartition,
        })
        expect(waited).toBe(true)
        expect(unstable().length).toBeGreaterThan(0)
        expect(retriesByBroker.every((count) => count <= retryCeiling)).toBe(true)
        expect(rebalanced).toBe(true)
        expect(joined.at).toBeLessThan(abortStartedAt)
        // The yield is mechanism-tested under the classic protocol only: the consumer protocol
        // revokes cooperatively and sets no rebalance verdict, so there this is an outcome guard.
        expect(joined.at - joiningStartedAt).toBeLessThan(REBALANCE_YIELD_BUDGET_MS)
        expect(waitingAfterRebalance.length).toBeGreaterThan(0)
        expect(waitingAfterRebalance.at(-1).partitions).toHaveLength(1)
        expect(waitingAfterRebalance.at(-1).at).toBeLessThan(abortStartedAt)
        expect(answered.length).toBeGreaterThan(0)
        expect(answered.every(({ at }) => at >= abortStartedAt)).toBe(true)
        expect(members.map(({ pollExpiries }) => pollExpiries)).toEqual([0, 0])
        expect(members.map(({ crashes }) => crashes)).toEqual([0, 0])
        for (const offsets of offsetsByPartition) {
          expect(offsets).toEqual(
            Array.from({ length: RECORDS - baseline }, (_, index) => baseline + index)
          )
        }
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'a membership gone stale before sendOffsets makes it abortable; abort, rejoin and reprocess leave no duplicate and no gap',
      async () => {
        const messages = await produceInput(0)
        const member = await startConsumer()
        await waitFor(() => member.consumed.length >= RECORDS)
        const stale = member.consumer.groupMetadata()
        await member.consumer.stop()
        const { producer } = await transactionalProducer()

        const first = await producer.transaction()
        await first.send({
          topic: output,
          messages: member.consumed.map(({ value }) => ({ value })),
        })
        const error = await first
          .sendOffsets({ consumerGroupMetadata: stale, topics: offsetsAt(RECORDS) })
          .catch((e) => e)
        await first.abort()

        member.consumed.length = 0
        await member.run()
        await waitFor(() => member.consumed.length >= RECORDS)
        const second = await producer.transaction()
        await second.send({
          topic: output,
          messages: member.consumed.map(({ value }) => ({ value })),
        })
        await second.sendOffsets({
          consumerGroupMetadata: member.consumer.groupMetadata(),
          topics: offsetsAt(RECORDS),
        })
        await second.commit()
        const values = await readOutput(RECORDS)
        const committed = await committedOffsetReaching(RECORDS)

        writeEvidence(EVIDENCE, {
          scenario: 'stale-generation',
          groupProtocol,
          error: { name: error && error.name, cause: error && error.cause && error.cause.type },
          committed,
          values: values.length,
        })
        expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(STALE_MEMBER_CODES).toContain(error.cause.type)
        expect(committed).toEqual(String(RECORDS))
        expect(values.sort()).toEqual(messages.map(({ value }) => value).sort())
      },
      SCENARIO_BUDGET_MS
    )
  })
})
