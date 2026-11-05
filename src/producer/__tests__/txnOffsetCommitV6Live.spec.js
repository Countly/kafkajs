const { Kafka, logLevel } = require('../../../index')
const Broker = require('../../broker')
const Cluster = require('../../cluster')
const apiKeys = require('../../protocol/requests/apiKeys')
const { NO_TOPIC_ID } = require('../../protocol/requests/topicIds')
const {
  secureRandom,
  plainTextBrokers,
  createTopic,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it holds a transaction open on purpose and creates a topic and groups on a live cluster.
// Run it alone through `npm run test:e2e:txn-offset-commit-v6`; `KAFKA_VERSION` names the cluster
// under test.
const describeTxnOffsetCommitV6 =
  process.env.TXN_OFFSET_COMMIT_V6_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'txn-offset-commit-v6'
// Named so each wait says how long it is prepared to be wrong for. The scenario budget is the whole
// file, from the first hook to the last cleanup; a case and a hook have their own, shorter ones.
const SCENARIO_BUDGET_MS = 180000
const CASE_BUDGET_MS = 90000
const HOOK_BUDGET_MS = 30000
const DISCONNECT_BUDGET_MS = 10000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500 // how long a reader waits for a record that should not exist
const RECORDS = 5
const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/

/**
 * The TxnOffsetCommit version a broker of the lane's `KAFKA_VERSION` offers: v6, which keys topics
 * by id, from Kafka 4.4, and v5 before it. The test also reads the version the broker advertised,
 * so a lane that names the wrong cluster fails on the comparison instead of passing on a guess.
 * @returns {5|6}
 * @throws {Error} when `KAFKA_VERSION` is not `major.minor.patch`, so no lane is guessed
 */
const offeredVersion = () => {
  const [major, minor] = (process.env.KAFKA_VERSION || '').split('.').map(Number)
  if (!Number.isInteger(major) || !Number.isInteger(minor)) {
    throw new Error(
      `KAFKA_VERSION must name the cluster under test, got ${process.env.KAFKA_VERSION}`
    )
  }

  return major > 4 || (major === 4 && minor >= 4) ? 6 : 5
}

/** @returns {{ promise: Promise<void>, resolve: () => void }} */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/**
 * Live proof of transactional offset commits at the version the broker offers (KIP-1319). A
 * transaction writes records to a topic and commits the group's next offset on it together with
 * them: on Kafka 4.4 the TxnOffsetCommit names the topic by its id (v6), on 4.3 by its name (v5),
 * read from the version the broker advertised and from the wire. The group's raw committed offset
 * stays at its baseline while the offsets are staged and while EndTxn is held, and becomes the
 * committed value only after the commit; an aborted transaction's larger offset never lands and its
 * records are in the log, for a `read_uncommitted` reader, but not for the consumer. A topic whose
 * id the cache does not hold keeps the whole request at v5. Versions are read from `REQUEST`
 * events and from the private map the producer handed `Broker`; counts and versions are asserted,
 * never durations.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/TxnOffsetCommitRequest.json
 */
describeTxnOffsetCommitV6('Producer > TxnOffsetCommit v6 (live)', () => {
  const runId = secureRandom()
  const topic = `e2e-txn-offset-commit-v6-${runId}-topic`
  const groupId = `e2e-txn-offset-commit-v6-${runId}-group`
  const kafka = new Kafka({
    clientId: 'txn-offset-commit-v6-live',
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  const producer = kafka.producer({ transactionalId: `e2e-txn-offset-commit-v6-${runId}` })
  /** @type {{ apiName: string, apiVersion: number }[]} */
  const requests = []
  /** @type {{ advertisedMax: number|null, transactionVersion: number, groupId: string, topicIds: [string, string][]|null }[]} */
  const offsetCommits = []
  const clients = []
  /** what cleanup could not undo */
  const cleanupFailures = []
  const gate = { endTxn: null, endTxnReached: false, hideTopicIds: false }
  let startedAt

  const waitFor = (predicate) => waitForCondition(predicate, WAIT_BUDGET)

  /** A connected client, disconnected after the scenario. */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** The group's committed (applied) next offset on partition 0, as a raw OffsetFetch says. */
  const committedOffset = async (group = groupId) => {
    const [{ partitions }] = await admin.fetchOffsets({ groupId: group, topics: [topic] })
    return partitions.find(({ partition }) => partition === 0).offset
  }

  /**
   * A committed offset is applied by the transaction's marker, which lands after `commit()`
   * returns, so a positive read polls until it reaches `expected`; the last value read is returned.
   */
  const committedOffsetReaching = async (expected, group = groupId) => {
    let offset
    await waitFor(async () => {
      offset = await committedOffset(group)
      return offset === String(expected)
    })
    return offset
  }

  /**
   * Every value a fresh reader sees on the topic, once it has gone quiet: a `read_committed` reader,
   * or with `readUncommitted` one that also sees the records an abort discarded.
   */
  const readValues = async (expected, { readUncommitted = false } = {}) => {
    const reader = await connected(
      kafka.consumer({ groupId: `${groupId}-reader-${secureRandom()}`, readUncommitted })
    )
    const values = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => values.push(message.value.toString()) })
    expect(await waitFor(() => values.length >= expected)).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, QUIET_MS))
    return values
  }

  /** Offsets for partition 0 of the topic: the group's next offset to read. */
  const offsetsAt = (offset) => [{ topic, partitions: [{ partition: 0, offset: String(offset) }] }]

  /** The topic's records of one transaction, each value a unique id. */
  const records = (prefix, count = RECORDS) =>
    Array.from({ length: count }, (_, index) => ({ partition: 0, value: `${prefix}-${index}` }))

  /** Runs one cleanup step; what it could not undo is kept. */
  const cleanup = async (work) => {
    try {
      await work()
    } catch (error) {
      cleanupFailures.push(error)
    }
  }

  beforeAll(async () => {
    const { txnOffsetCommit, endTxn } = Broker.prototype
    jest.spyOn(Broker.prototype, 'txnOffsetCommit').mockImplementation(function (request) {
      offsetCommits.push({
        advertisedMax: this.versions?.[apiKeys.TxnOffsetCommit]?.maxVersion ?? null,
        transactionVersion: request.transactionVersion,
        groupId: request.groupId,
        topicIds: request.topicIds ? [...request.topicIds] : null,
      })
      return txnOffsetCommit.call(this, request)
    })
    jest.spyOn(Broker.prototype, 'endTxn').mockImplementation(async function (request) {
      if (gate.endTxn) {
        gate.endTxnReached = true
        await gate.endTxn.promise
      }
      return endTxn.call(this, request)
    })
    const { topicId } = Cluster.prototype
    jest.spyOn(Cluster.prototype, 'topicId').mockImplementation(function (name) {
      return gate.hideTopicIds ? undefined : topicId.call(this, name)
    })

    producer.on(producer.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    startedAt = Date.now()
    await Promise.all([admin.connect(), producer.connect()])
    await createTopic({ topic, partitions: 1 })
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    jest.restoreAllMocks()
    gate.endTxn?.resolve()
    for (const client of clients.reverse()) {
      await cleanup(() => withTimeout(client.disconnect(), DISCONNECT_BUDGET_MS))
    }
    await cleanup(() => admin.deleteTopics({ topics: [topic] }))
    await cleanup(() => withTimeout(producer.disconnect(), DISCONNECT_BUDGET_MS))
    await cleanup(() => withTimeout(admin.disconnect(), DISCONNECT_BUDGET_MS))

    expect(cleanupFailures).toEqual([])
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  }, HOOK_BUDGET_MS)

  const txnOffsetCommitVersions = () =>
    requests
      .filter(({ apiName }) => apiName === 'TxnOffsetCommit')
      .map(({ apiVersion }) => apiVersion)

  test(
    'commits offsets at the version the broker offers, only after the transaction commits, and an abort commits nothing',
    async () => {
      const expectedVersion = offeredVersion()
      const handled = []
      let joined = false
      const consumer = await connected(
        kafka.consumer({ groupId, maxWaitTimeInMs: 100, sessionTimeout: 30000 })
      )
      consumer.on(consumer.events.GROUP_JOIN, () => {
        joined = true
      })
      await consumer.subscribe({ topics: [topic], fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => handled.push(message.value.toString()),
      })
      expect(await waitFor(() => joined)).toBe(true)
      const baseline = await committedOffset()
      expect(baseline).toEqual('-1')

      const committedIds = records('commit').map(({ value }) => value)
      const abortedIds = records('abort', 3).map(({ value }) => value)

      // The commit: staged, then held at EndTxn, then confirmed.
      const committing = await producer.transaction()
      await committing.send({ topic, messages: records('commit') })
      await committing.sendOffsets({
        consumerGroupMetadata: consumer.groupMetadata(),
        topics: offsetsAt(RECORDS),
      })
      const stagedCall = offsetCommits.at(-1)
      const staged = await committedOffset()
      gate.endTxn = deferred()
      gate.endTxnReached = false
      let settled = false
      const commit = committing.commit().finally(() => {
        settled = true
      })
      expect(await waitFor(() => gate.endTxnReached)).toBe(true)
      expect(settled).toBe(false)
      const held = await committedOffset()
      gate.endTxn.resolve()
      gate.endTxn = null
      await commit
      const afterCommit = await committedOffsetReaching(RECORDS)

      // The abort: a different, larger offset that must never land.
      const aborting = await producer.transaction()
      await aborting.send({ topic, messages: records('abort', 3) })
      await aborting.sendOffsets({
        consumerGroupMetadata: consumer.groupMetadata(),
        topics: offsetsAt(RECORDS + 4),
      })
      const abortedCall = offsetCommits.at(-1)
      await aborting.abort()
      // both markers are in the log once its end is: five records, a marker, three records, a marker
      const logEnd = RECORDS + 1 + 3 + 1
      expect(
        await waitFor(async () => {
          const [{ high }] = await admin.fetchTopicOffsets(topic)
          return high === String(logEnd)
        })
      ).toBe(true)
      await new Promise((resolve) => setTimeout(resolve, QUIET_MS))
      const afterAbort = await committedOffset()

      const physical = await readValues(committedIds.length + abortedIds.length, {
        readUncommitted: true,
      })
      const visible = await readValues(committedIds.length)
      expect(await waitFor(() => handled.length >= committedIds.length)).toBe(true)
      await new Promise((resolve) => setTimeout(resolve, QUIET_MS))
      await withTimeout(consumer.disconnect(), DISCONNECT_BUDGET_MS)

      const wire = txnOffsetCommitVersions()
      writeEvidence(EVIDENCE, {
        step: 'commit-and-abort',
        wireVersions: wireVersionsOf(requests),
        expectedVersion,
        advertisedMax: stagedCall.advertisedMax,
        requestedNextOffsets: { committed: RECORDS, aborted: RECORDS + 4 },
        rawNextOffsets: { baseline, staged, held, afterCommit, afterAbort },
        logEnd,
        recordIds: {
          physical: [...physical].sort(),
          committedVisible: [...visible].sort(),
          handler: [...handled].sort(),
        },
        topicIds: stagedCall.topicIds,
      })
      // what the broker offers: the lane's name for it, and the table the broker itself sent
      expect(stagedCall.advertisedMax).toEqual(expectedVersion)
      expect(stagedCall.transactionVersion).toEqual(2)
      expect(new Set(wire)).toEqual(new Set([expectedVersion]))
      expect(wire.length).toBeGreaterThanOrEqual(2)
      // the producer knew the topic's id, whichever version carried it
      expect(stagedCall.topicIds).toEqual([[topic, expect.stringMatching(UUID)]])
      expect(stagedCall.topicIds[0][1]).not.toEqual(NO_TOPIC_ID)
      expect(abortedCall.topicIds).toEqual(stagedCall.topicIds)
      // commit-only publication
      expect(staged).toEqual(baseline)
      expect(held).toEqual(baseline)
      expect(afterCommit).toEqual(String(RECORDS))
      expect(afterAbort).toEqual(String(RECORDS))
      // exactly the records of the committed transaction are visible, once; the physical log has both
      expect([...physical].sort()).toEqual([...committedIds, ...abortedIds].sort())
      expect([...visible].sort()).toEqual([...committedIds].sort())
      expect([...handled].sort()).toEqual([...committedIds].sort())
    },
    CASE_BUDGET_MS
  )

  test(
    'a topic id the cache does not hold keeps the whole offset commit at v5, where the broker offers v6',
    async () => {
      const expectedVersion = offeredVersion()
      const before = offsetCommits.length
      const versionsBefore = txnOffsetCommitVersions().length
      const baseline = await committedOffset()
      expect(baseline).not.toEqual('7')

      gate.hideTopicIds = true
      try {
        const transaction = await producer.transaction()
        await transaction.send({ topic, messages: records('without-ids', 2) })
        await transaction.sendOffsets({ consumerGroupId: groupId, topics: offsetsAt(7) })
        await transaction.commit()
      } finally {
        gate.hideTopicIds = false
      }
      const afterCommit = await committedOffsetReaching(7)

      const calls = offsetCommits.slice(before)
      const wire = txnOffsetCommitVersions().slice(versionsBefore)
      writeEvidence(EVIDENCE, {
        step: 'missing-topic-id',
        wireVersions: wireVersionsOf(requests),
        expectedVersion,
        advertisedMax: calls[0]?.advertisedMax,
        wire,
        topicIds: calls.map(({ topicIds }) => topicIds),
        rawNextOffsets: { baseline, afterCommit },
      })
      expect(calls.length).toBeGreaterThanOrEqual(1)
      for (const call of calls) {
        expect(call.advertisedMax).toEqual(expectedVersion)
        expect(call.transactionVersion).toEqual(2)
        expect(call.topicIds).toBeNull()
      }
      expect(new Set(wire)).toEqual(new Set([5]))
      expect(afterCommit).toEqual('7')
    },
    CASE_BUDGET_MS
  )
})
