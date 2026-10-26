const { isDeepStrictEqual } = require('util')
const { setTimeout: sleep } = require('timers/promises')
const { Kafka, logLevel } = require('../../../index')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { BrokerBouncer } = require('../../../testHelpers/leaderElection')
const { committedRecords, readLog } = require('../../../testHelpers/rawLog')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const {
  armedPermanence,
  outcomeOf,
  settledTransactionVersion,
} = require('./fixtures/liveTransactions')
const { createLiveSockets } = require('./fixtures/liveWire')
const { heartbeatInBatch } = require('./fixtures/memberHeartbeat')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('./fixtures/bounded')
const {
  CleanupStack,
  ProducerProbe,
  describeError,
  retryCoordinator,
} = require('./fixtures/liveProbe')
const { CaseTimeline, JOIN_RESERVE_MS } = require('./fixtures/replicaOutage')

// Gated: it kills connections and rolls the three brokers. Run alone, through the guarded entry:
// `npm run test:e2e:exactly-once-chaos`.
const describeCopier = process.env.EXACTLY_ONCE_CHAOS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'exactly-once-chaos'
const PARTITIONS = 6
const RECORDS = 20000
const SLOTS = 2
// Records per copy transaction: many transactions, so that a fault lands inside some of them.
const CHUNK = 50
// What a fetch asks for per partition, so that one handler call covers a few chunks, not a log.
const FETCH_BYTES = 16384
const SEED_CHUNK = 1000
const HOOK_BUDGET_MS = 60000
const BUFFER_MEMORY = 1048576
const MAX_REQUEST_SIZE = 1048576
const DELIVERY_BUDGET_MS = 30000
const DISCONNECT_BUDGET_MS = 15000
const STEP_MS = 15000
const JOIN_MS = 45000
const INIT_MS = 40000
const READ_MS = 20000
const READ_ATTEMPTS = 8
const READ_RETRY_MS = 1000
const QUIET_MS = 2000
const TICK_MS = 500
const SNAPSHOT_EVERY_MS = 2000
const IDLE_MS = 250
const COMMIT_ATTEMPTS = 6
const COMMIT_RETRY_MS = 1000
// The classic group's liveness window, and what an outage must stay under to be a short one.
const CLASSIC_SESSION_MS = 30000
const CLASSIC_HEARTBEAT_MS = 3000
const SHORT_FRACTION = 0.8
const STABLE_MS = 3000
const MEMBERSHIP = Object.freeze([
  'GROUP_JOIN',
  'REBALANCING',
  'PARTITIONS_ASSIGNED',
  'PARTITIONS_REVOKED',
  'PARTITIONS_LOST',
  'CRASH',
])
const KILL_AFTER_MS = [500, 1500]
const CHAOS_MS = 40000
const BOUNCE_ROUND_MS = 150000
const FIRST_REPLACEMENT_MS = 3000
const REPLACE_EVERY_MS = 6000
const MAX_REPLACEMENTS = 4
// Think time per copy transaction while a fault is on, so that the copying outlasts it.
const PACE_MS = Object.freeze({ socket: 50, brokers: 2000 })
const BOUNCE_ROUNDS = Object.freeze([
  { name: 'clean', hard: false },
  { name: 'hard', hard: true },
])
const BETWEEN_ROUNDS_MS = 15000
// How far before an outage a copy transaction may have begun for the copying to count as under way.
const TRAFFIC_WINDOW_MS = 10000
const FENCING_CODES = Object.freeze([47, 90])
// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready, and a takeover until the coordinator has aborted the previous
// owner's transaction: 12 retries, 50 ms doubling to a 3 s cap, about 21 s in all.
const RETRY = { retries: 12, initialRetryTime: 50, multiplier: 2, maxRetryTime: 3000, factor: 0 }

const SOCKET_ARM = Object.freeze({
  kind: 'socket',
  budgetMs: 300000,
  phases: Object.freeze({
    setup: 60000,
    traffic: 100000,
    observe: 40000,
    stale: 35000,
    probe: 35000,
    cleanup: 20000,
  }),
})
const BROKERS_ARM = Object.freeze({
  kind: 'brokers',
  budgetMs: 600000,
  phases: Object.freeze({
    setup: 60000,
    traffic: 330000,
    observe: 40000,
    stale: 35000,
    probe: 35000,
    cleanup: 20000,
  }),
})

const REPLAY = Object.freeze({
  log: 'each input id once, in order per partition, in the committed output; aborted physical records are counted apart',
  offsets:
    'broker offsets move only with committed transactions and equal the position a restarted copier resumes from',
  handlers: 'at least once for input after the last committed transaction',
})

const noop = () => {}

/**
 * @param {number} partition
 * @returns {number[]} the input ids a partition holds, in order: those that are `partition` modulo
 *   the partition count
 */
const idsOf = (partition) =>
  Array.from(
    { length: Math.ceil((RECORDS - partition) / PARTITIONS) },
    (_, n) => partition + n * PARTITIONS
  )

/**
 * @param {string|number} offset a committed offset as the broker reports it; -1 for none
 * @returns {number} the offset a consumer resumes from: the start of the log when none is committed
 */
const resumeFrom = (offset) => Math.max(0, Number(offset))

/**
 * What the committed offsets read during the run say: each is the start of the log or the offset
 * after a chunk some copier began to commit, never ahead of the committed output and never going
 * back; so an offset only ever moved with a transaction.
 * @param {Array<{ at: number, offsets: number[] }>} snapshots broker offsets by partition, in time order
 * @param {Array<{ partition: number, ids: number[] }>} visible the committed output, by partition
 * @param {Array<{ partition: number, last: number, commitStartedAt: number|null }>} chunks every
 *   copy transaction begun
 * @returns {Array<{ at: number, offsets: number[], onlyCommittedTransactions: boolean }>}
 */
const snapshotVerdict = (snapshots, visible, chunks) => {
  const attempted = new Set(
    chunks
      .filter((chunk) => chunk.commitStartedAt !== null)
      .map((chunk) => `${chunk.partition}:${chunk.last + 1}`)
  )
  let previous = null
  return snapshots.map(({ at, offsets }) => {
    const covered = offsets.every((offset, partition) => offset <= visible[partition].ids.length)
    const moved = offsets.every(
      (offset, partition) => offset === 0 || attempted.has(`${partition}:${offset}`)
    )
    const monotonic =
      previous === null || offsets.every((offset, partition) => offset >= previous[partition])
    previous = offsets
    return { at, offsets, onlyCommittedTransactions: covered && moved && monotonic }
  })
}

/**
 * Oracle part, handler delivery: every input record reached a handler, and none reached one after
 * a confirmed commit had already covered it. A record delivered again before any commit covered it
 * is the at-least-once redelivery the oracle allows.
 * @param {object[]} chunks every copy transaction begun: `{ instance, partition, first, last,
 *   startedAt, committedAt }`, offsets of the input
 * @param {number[]} ends the end offset of each input partition
 * @returns {{ missing: number, duplicatesAfterLastCommit: number, violations: object[] }}
 *   `duplicatesAfterLastCommit` counts the records delivered again that no confirmed commit covered
 *   yet, the allowed redeliveries
 */
const deliveryVerdict = (chunks, ends) => {
  const covered = ends.map((end) => new Uint8Array(end))
  const committed = chunks.filter((chunk) => chunk.committedAt !== null)
  const violations = []
  let duplicatesAfterLastCommit = 0
  for (const chunk of [...chunks].sort((a, b) => a.startedAt - b.startedAt)) {
    const committedUpTo = Math.max(
      0,
      ...committed
        .filter((done) => done.partition === chunk.partition && done.committedAt <= chunk.startedAt)
        .map((done) => done.last + 1)
    )
    if (chunk.first < committedUpTo) {
      violations.push({
        instance: chunk.instance,
        partition: chunk.partition,
        first: chunk.first,
        committedUpTo,
      })
    }
    for (let offset = chunk.first; offset <= chunk.last; offset++) {
      if (covered[chunk.partition][offset] && offset >= committedUpTo)
        duplicatesAfterLastCommit += 1
      covered[chunk.partition][offset] = 1
    }
  }
  return {
    missing: covered.reduce((sum, seen) => sum + seen.filter((flag) => flag === 0).length, 0),
    duplicatesAfterLastCommit,
    violations,
  }
}

/**
 * Oracle part, ownership: no two committed transactions cover the same input of a partition, and
 * no transaction whose commit began after its copier was replaced committed.
 * @param {object[]} chunks see `deliveryVerdict`; `commitStartedAt` when a commit was begun
 * @param {Array<{ id: string, fencedAt: number|null }>} instances every copier incarnation, with
 *   when its replacement finished fencing it
 * @returns {{ committedTransactions: number, violations: object[] }}
 */
const ownershipVerdict = (chunks, instances) => {
  const fencedAt = new Map(instances.map(({ id, fencedAt: at }) => [id, at]))
  const committed = chunks.filter((chunk) => chunk.outcome === 'committed')
  const violations = []
  for (let partition = 0; partition < PARTITIONS; partition++) {
    const ordered = committed
      .filter((chunk) => chunk.partition === partition)
      .sort((a, b) => a.first - b.first)
    ordered.slice(1).forEach((chunk, index) => {
      if (chunk.first <= ordered[index].last) {
        violations.push({
          kind: 'overlapping-commit',
          partition,
          instances: [ordered[index].instance, chunk.instance],
          first: chunk.first,
        })
      }
    })
  }
  for (const chunk of committed) {
    const fenced = fencedAt.get(chunk.instance)
    if (fenced != null && chunk.commitStartedAt > fenced) {
      violations.push({
        kind: 'stale-commit',
        instance: chunk.instance,
        partition: chunk.partition,
        first: chunk.first,
      })
    }
  }
  return { committedTransactions: committed.length, violations }
}

/**
 * Oracle part, rebalances during an outage, as the classic group is judged: none through the
 * stabilisation of a short outage, at most one change in a long one. Only copiers that lived
 * through the whole window count, and a window in which this scenario had to replace a copier is a
 * violation of its own, since that replacement changed the membership.
 * @param {Array<{ instance: string, kind: string, at: number }>} members membership events
 * @param {Array<{ id: string, bornAt: number, retiredAt: number|null }>} instances
 * @param {Array<{ service: string, round: string, tDown: number, tRestored: number,
 *   outageMs: number }>} outages
 * @param {number[]} replacedAt when each copier was replaced, ms since the epoch
 * @returns {Array<{ service: string, round: string, kind: 'short'|'long', events: number,
 *   changes: number, replaced: boolean, violation: boolean }>}
 */
const rebalanceVerdict = (members, instances, outages, replacedAt = []) =>
  outages.map(({ service, round, tDown, tRestored, outageMs }) => {
    const end = tRestored + STABLE_MS
    const stable = new Set(
      instances
        .filter(
          ({ bornAt, retiredAt }) => bornAt < tDown && (retiredAt === null || retiredAt > end)
        )
        .map(({ id }) => id)
    )
    const events = members
      .filter(({ instance, at }) => stable.has(instance) && at >= tDown && at <= end)
      .sort((a, b) => a.at - b.at)
    let changes = 0
    let last = -Infinity
    for (const { at } of events) {
      if (at - last >= STABLE_MS) changes += 1
      last = at
    }
    const kind = outageMs < SHORT_FRACTION * CLASSIC_SESSION_MS ? 'short' : 'long'
    const replaced = replacedAt.some((at) => at >= tDown && at <= end)
    return {
      service,
      round,
      kind,
      events: events.length,
      changes,
      replaced,
      violation: replaced || (kind === 'short' ? changes > 0 : changes > 1),
    }
  })

/**
 * The first place where two committed outputs differ, for a failure that can be read.
 * @param {Array<{ partition: number, ids: number[] }>} actual
 * @param {Array<{ partition: number, ids: number[] }>} expected
 * @returns {{ partition: number, index: number, found: number|null, wanted: number|null }|null}
 */
const firstDifference = (actual, expected) => {
  for (const { partition, ids } of expected) {
    const found = actual.find((entry) => entry.partition === partition)?.ids ?? []
    const length = Math.max(found.length, ids.length)
    for (let index = 0; index < length; index++) {
      if (found[index] !== ids[index]) {
        return { partition, index, found: found[index] ?? null, wanted: ids[index] ?? null }
      }
    }
  }
  return null
}

/**
 * The acceptance of one arm: the committed output equals the input exactly, per partition and in
 * order; the broker's committed offsets only ever followed committed transactions and a restarted
 * copier resumed from them; a copier fenced by its replacement committed nothing; delivery,
 * ownership and rebalances broke no rule; and every surviving copier's producer passed the closing
 * admission probe.
 * @param {'socket'|'brokers'} kind
 * @param {object} observation what `CopierScenario#run` returned
 * @returns {void}
 * @throws {Error} through a failed expectation, naming the first invariant that does not hold
 */
const accept = (kind, observation) => {
  expect(firstDifference(observation.visibleByPartition, observation.inputByPartition)).toBeNull()
  expect(observation.visibleByPartition).toEqual(observation.inputByPartition)
  expect(observation.visibleByPartition.flatMap((p) => p.ids)).toHaveLength(RECORDS)
  expect(observation.rawVisibleEqualsPublic).toBe(true)
  expect(observation.brokerOffsets).toEqual(observation.visibleByPartition.map((p) => p.ids.length))
  expect(observation.commitSnapshots.length).toBeGreaterThan(0)
  expect(observation.commitSnapshots.every((s) => s.onlyCommittedTransactions)).toBe(true)
  expect(observation.restartPositions.length).toBeGreaterThan(0)
  expect(observation.restartPositions.every((s) => s.firstPosition === s.brokerOffset)).toBe(true)
  expect(observation.restartPositions.some((s) => s.incarnation > 0 && s.brokerOffset > 0)).toBe(
    true
  )
  expect(observation.staleCommitResults).toHaveLength(2)
  expect(observation.staleCommitResults.every((s) => s.rejected && s.before === s.after)).toBe(true)
  expect(
    observation.staleCommitResults.every((s) => isDeepStrictEqual(s.visible, ['current']))
  ).toBe(true)
  expect(observation.ownership.violations).toEqual([])
  expect(observation.delivery.missing).toBe(0)
  expect(observation.delivery.violations).toEqual([])
  expect(observation.rebalance.assertedViolations).toEqual([])
  expect(observation.admissionProbes).toHaveLength(SLOTS)
  for (const { closing } of observation.admissionProbes) {
    expect(closing).toEqual(
      expect.objectContaining({
        records: expect.any(Number),
        observed: expect.objectContaining({ inEntry: true, waitingAfterEntry: 0 }),
      })
    )
  }
  if (kind === 'socket') {
    expect(observation.killedConnections).toBeGreaterThan(0)
    expect(observation.replacements).toBeGreaterThanOrEqual(2)
  } else {
    expect(observation.outages).toHaveLength(BOUNCE_ROUNDS.length * 3)
    expect(
      observation.outages.every((outage) => outage.copiedAtDown < RECORDS && outage.trafficNear)
    ).toBe(true)
  }
}

/**
 * One incarnation of a copier slot: a consumer in the shared group and a transactional producer on
 * the slot's stable transactional id. It copies each fetched batch, in transactions of at most
 * `CHUNK` records, to the same partition of the output topic and commits the input offsets in the
 * same transaction, fenced by the consumer's group membership. It stops at the first failure it
 * cannot undo by aborting, which a replacement then recovers from the broker's own state; and a
 * transaction whose commit outcome is unknown is committed again, never replayed.
 */
class Copier {
  /**
   * @param {CopierScenario} scenario the owner, which holds the journals and the chaos
   * @param {number} slot
   * @param {number} incarnation how many copiers this slot had before
   */
  constructor(scenario, slot, incarnation) {
    this.scenario = scenario
    this.slot = slot
    this.id = `slot${slot}-${incarnation}`
    this.transactionalId = scenario.transactionalIdOf(slot)
    this.producerSockets = createLiveSockets()
    this.consumerSockets = createLiveSockets()
    this.producer = null
    this.consumer = null
    this.retired = false
    this.draining = false
    /** Aborts when this copier is retired or draining, to end its waits; never a harness scope. */
    this.halted = new AbortController()
    this.failure = null
    /** @type {{ instance: string, error: object, retries: number, lastRetry: object|null }|null} */
    this.recovery = null
    this.seen = new Set()
    this.working = Promise.resolve()
    /** The journal's record of this incarnation. */
    this.incarnation = incarnation
    this.instance = { id: this.id, slot, bornAt: Date.now(), retiredAt: null, fencedAt: null }
    scenario.instances.push(this.instance)
    scenario.enlist(this.producerSockets.chaos)
    scenario.cleanup.add(this.id, () => this.close())
  }

  /**
   * Connects the producer and takes over the slot's transactional id: the first transaction is
   * begun, which fences the previous owner and aborts what it left open, and ended at once.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   * @throws {Error} a failed step, or the reason `signal` aborted with
   */
  async openProducer(signal) {
    const { scenario } = this
    this.producer = new Kafka({
      clientId: `${scenario.name}-${this.id}-producer`,
      brokers: scenario.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.producerSockets.socketFactory,
    }).producer({
      transactionalId: this.transactionalId,
      retry: RETRY,
      bufferMemory: BUFFER_MEMORY,
      maxRequestSize: MAX_REQUEST_SIZE,
    })
    await bounded(signal, STEP_MS, () => this.producer.connect(), { label: 'The producer connect' })
    await bounded(
      signal,
      INIT_MS,
      async () => {
        const takeover = await this.producer.transaction()
        await takeover.abort()
      },
      { label: 'The takeover of the transactional id' }
    )
  }

  /**
   * Joins the group and starts copying. The consumer's connections join the chaos only once it is a
   * member: a new member whose parked JoinGroup is lost starts over without the id the coordinator
   * assigned (as kafkajs 2.2.4 does), which leaves a member that never answers, and the group
   * waits for each such member's session timeout before it can rebalance again. A member that
   * holds an id keeps it, so every rejoin under the chaos is still exercised.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   * @throws {Error} a step that did not settle in time, or the reason `signal` aborted with
   */
  async startConsumer(signal) {
    const { scenario } = this
    const consumer = new Kafka({
      clientId: `${scenario.name}-${this.id}-consumer`,
      brokers: scenario.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.consumerSockets.socketFactory,
    }).consumer({
      groupId: scenario.groupId,
      sessionTimeout: CLASSIC_SESSION_MS,
      heartbeatInterval: CLASSIC_HEARTBEAT_MS,
      maxWaitTimeInMs: 250,
      maxBytesPerPartition: FETCH_BYTES,
    })
    for (const kind of MEMBERSHIP.filter((name) => name !== 'CRASH')) {
      consumer.on(consumer.events[kind], () =>
        scenario.members.push({ instance: this.id, kind, at: Date.now() })
      )
    }
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      scenario.members.push({ instance: this.id, kind: 'CRASH', at: Date.now() })
      if (!payload.restart) this.failure ??= payload.error
    })
    this.consumer = consumer
    await bounded(signal, STEP_MS, () => consumer.connect(), { label: 'The consumer connect' })
    await bounded(
      signal,
      STEP_MS,
      () => consumer.subscribe({ topics: [scenario.input], fromBeginning: true }),
      { label: 'The subscription' }
    )
    await bounded(
      signal,
      JOIN_MS,
      () =>
        consumer.run({
          autoCommit: false,
          eachBatchAutoResolve: false,
          eachBatch: (payload) => {
            const done = this.process(payload)
            this.working = done.catch(() => {})
            return done
          },
        }),
      { label: 'The group join' }
    )
    scenario.enlist(this.consumerSockets.chaos)
  }

  /**
   * Handles one fetched batch: its records in chunks of `CHUNK`, each copied and committed in a
   * transaction before its offset is resolved. A chunk that did not commit is not resolved, so the
   * next fetch repeats it; a retired, draining or failed copier does nothing. Once a chunk is
   * resolved the handler heartbeats (`heartbeatInBatch`), so that a rebalance ends the batch and
   * the consumer rejoins at once, not after every batch it has fetched; the next fetch repeats
   * whatever was not resolved.
   * @param {object} payload the consumer's `eachBatch` payload
   * @returns {Promise<void>}
   * @throws {Error} the rebalance or lost-membership error the heartbeat raised
   */
  async process({ batch, resolveOffset, heartbeat, isRunning, isStale }) {
    const { partition, messages } = batch
    for (let at = 0; at < messages.length; at += CHUNK) {
      if (this.retired || this.draining || this.failure) return this.rest()
      if (!isRunning() || isStale()) return
      const chunk = messages.slice(at, at + CHUNK)
      if (!(await this.noteStart(partition, Number(chunk[0].offset)))) return this.rest()
      if (!(await this.copy(partition, chunk))) return this.rest()
      resolveOffset(chunk.at(-1).offset)
      await heartbeatInBatch(heartbeat)
      if (this.scenario.paceMs > 0) await this.rest(this.scenario.paceMs)
    }
  }

  /**
   * Pauses, and stops pausing once this copier is retired or draining. Woken by the copier's own
   * signal, not a harness scope, so nothing of the harness is aborted and the run is not tainted.
   * @param {number} [ms]
   * @returns {Promise<void>}
   */
  rest(ms = IDLE_MS) {
    return sleep(ms, undefined, { signal: this.halted.signal }).catch(noop)
  }

  /**
   * The first time this copier is given a partition, records where its handler starts and what
   * the broker holds committed for the group, read independently.
   * @param {number} partition
   * @param {number} firstPosition the offset of the first record handed to the handler
   * @returns {Promise<boolean>} false when the broker's offset could not be read, which stops the
   *   copier
   */
  async noteStart(partition, firstPosition) {
    if (this.seen.has(partition)) return true
    try {
      const offsets = await this.scenario.committedOffsetsPatiently()
      this.scenario.restartPositions.push({
        instance: this.id,
        slot: this.slot,
        incarnation: this.incarnation,
        partition,
        firstPosition,
        brokerOffset: offsets[partition],
      })
      this.seen.add(partition)
      return true
    } catch (error) {
      this.failure ??= error
      return false
    }
  }

  /**
   * Copies one chunk in one transaction and commits it. The transaction is aborted when that is
   * safe (an abortable error); any other failure, and a commit whose outcome stays unknown, stops
   * this copier, because nothing says whether the chunk was committed.
   * @param {number} partition
   * @param {Array<{ offset: string, value: Buffer }>} chunk
   * @returns {Promise<boolean>} whether the chunk committed
   */
  async copy(partition, chunk) {
    const { scenario } = this
    const entry = {
      instance: this.id,
      partition,
      first: Number(chunk[0].offset),
      last: Number(chunk.at(-1).offset),
      records: chunk.length,
      startedAt: Date.now(),
      commitStartedAt: null,
      committedAt: null,
      outcome: 'failed',
    }
    scenario.chunks.push(entry)
    let transaction = null
    try {
      transaction = await this.producer.transaction()
      await transaction.send({
        topic: scenario.output,
        acks: -1,
        messages: chunk.map(({ value }) => ({ partition, value })),
      })
      await transaction.sendOffsets({
        consumerGroupMetadata: this.consumer.groupMetadata(),
        topics: [
          {
            topic: scenario.input,
            partitions: [{ partition, offset: String(entry.last + 1) }],
          },
        ],
      })
      entry.commitStartedAt = Date.now()
      await this.commit(transaction)
      entry.committedAt = Date.now()
      entry.outcome = 'committed'
      return true
    } catch (error) {
      entry.error = describeError(error)
      if (transaction !== null && error.category === 'abortable') {
        try {
          await transaction.abort()
          entry.outcome = 'aborted'
          return false
        } catch (abortError) {
          entry.error = describeError(abortError)
        }
      }
      this.failure ??= error
      return false
    }
  }

  /**
   * Commits a transaction, sending the commit again while its outcome is unknown (a retriable
   * error), which completes it once.
   * @param {object} transaction
   * @returns {Promise<void>}
   * @throws {Error} the last failure, when it is not retriable or the attempts ran out
   */
  async commit(transaction) {
    for (let attempt = 1; ; attempt++) {
      try {
        return await transaction.commit()
      } catch (error) {
        if (!error.retriable || attempt >= COMMIT_ATTEMPTS) throw error
        await delay(NEVER, COMMIT_RETRY_MS)
      }
    }
  }

  /**
   * Gives up this copier for good: no further batch is handled. Idempotent.
   * @returns {void}
   */
  retire() {
    if (this.retired) return
    this.retired = true
    this.halted.abort()
    this.instance.retiredAt = Date.now()
  }

  /**
   * Lets the transaction in flight finish, takes no further chunk, and stops the consumer; the
   * producer stays connected and usable.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   * @throws {Error} a step that did not settle in time, or the reason `signal` aborted with
   */
  async stop(signal) {
    this.draining = true
    this.halted.abort()
    await bounded(signal, STEP_MS, () => this.working, { label: `The work of ${this.id}` })
    await bounded(signal, STEP_MS, () => this.consumer?.stop(), { label: `The stop of ${this.id}` })
  }

  /**
   * Disconnects the consumer, then the producer.
   * @returns {Promise<void>}
   * @throws {Error} the first failure, after both were tried
   */
  async close() {
    this.retire()
    const failures = []
    for (const close of [
      () => this.consumer?.disconnect(),
      () => this.producer?.disconnect({ timeout: DISCONNECT_BUDGET_MS }),
    ]) {
      try {
        await close()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length > 0) throw failures[0]
  }
}

/**
 * A transactional copier group under a fault: 20000 input records over six partitions, copied to an
 * output topic by `SLOTS` copiers, while either every client connection (a consumer's from the
 * moment it is a member) is killed and copiers are replaced (`socket`), or the brokers are rolled
 * once clean and once hard (`brokers`). The three readings are kept apart: the committed output
 * and the physical log, the broker's committed offsets with the position a restarted copier
 * resumed from, and what the handlers were given. Every resource it opens goes to the case's
 * `CleanupStack`.
 */
class CopierScenario {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses
   * @param {'socket'|'brokers'} options.kind
   * @param {number} options.transactionVersion the cluster's finalized level, for the evidence
   * @param {CleanupStack} options.cleanup receives every resource the scenario opens
   */
  constructor({ brokers, kind, transactionVersion, cleanup }) {
    const id = secureRandom()
    Object.assign(this, { brokers, kind, transactionVersion, cleanup })
    this.name = `e2e-p9b-copier-${kind}-${id}`
    this.input = `${this.name}-in`
    this.output = `${this.name}-out`
    this.capture = `${this.name}-capture`
    this.staleOutput = `${this.name}-stale`
    this.groupId = `${this.name}-group`
    this.inputByPartition = Array.from({ length: PARTITIONS }, (_, partition) => ({
      partition,
      ids: idsOf(partition),
    }))
    this.ends = this.inputByPartition.map(({ ids }) => ids.length)
    /** @type {Copier[]} the current copier of each slot */
    this.copiers = []
    this.chunks = []
    this.members = []
    this.instances = []
    this.restartPositions = []
    this.snapshots = []
    this.staleCommitResults = []
    this.recoveries = []
    this.leaving = []
    this.closeFailures = []
    this.roles = []
    this.disarms = []
    this.killing = false
    this.killedConnections = 0
    this.replacements = 0
    /** @type {number[]} when each copier was replaced, ms since the epoch */
    this.replacedAt = []
    this.chaosTimer = null
    this.plannedFailures = 0
    this.faultsDone = false
    this.paceMs = PACE_MS[kind]
    this.outages = []
    this.startedAt = 0
    this.trafficAt = 0
    this.lastSnapshotAt = 0
    this.admin = null
    this.bounceAdmin = null
    this.cleanup.add('connection kills', () => this.endChaos())
    this.observer = createLiveSockets()
    this.observed = null
    this.probes = []
  }

  /**
   * @param {number} slot
   * @returns {string} the transactional id of the slot, which every incarnation of it takes over
   */
  transactionalIdOf(slot) {
    return `${this.name}-slot-${slot}`
  }

  /**
   * Registers a client role's chaos factory; while connections are being killed it is armed at once.
   * @param {Function & { arm: Function, sockets: object[] }} chaos
   * @returns {void}
   */
  enlist(chaos) {
    this.roles.push(chaos)
    if (this.killing) this.armRole(chaos)
  }

  /**
   * Kills every socket of a role after a lifetime of 0.5 to 1.5 s, the ones open and the ones
   * a reconnect opens, and counts the kills.
   * @param {{ arm: Function }} chaos
   * @returns {void}
   */
  armRole(chaos) {
    this.disarms.push(
      chaos.arm({}, (socket) => {
        socket.killAfter(KILL_AFTER_MS)
        socket.on('error', (error) => {
          if (error?.message === 'Connection killed') this.killedConnections += 1
        })
      })
    )
  }

  /**
   * Starts killing the connections of every role.
   * @returns {void}
   */
  startChaos() {
    this.killing = true
    this.roles.forEach((chaos) => this.armRole(chaos))
    this.chaosTimer = setTimeout(() => this.endChaos(), CHAOS_MS)
  }

  /**
   * Stops killing connections and cancels the kills still pending. Idempotent.
   * @returns {void}
   */
  endChaos() {
    this.killing = false
    clearTimeout(this.chaosTimer)
    for (const disarm of this.disarms.splice(0)) disarm()
    for (const chaos of this.roles) {
      for (const socket of chaos.sockets) {
        clearTimeout(socket.killTimer)
        socket.killTimer = null
      }
    }
  }

  /**
   * Creates the topics, writes the input records by explicit partition, and reads them back.
   * @param {AbortSignal} signal the setup phase's scope
   * @returns {Promise<void>}
   * @throws {Error} when the input log does not hold exactly what was written
   */
  async start(signal) {
    const { brokers } = this
    await bounded(
      signal,
      STEP_MS * 2,
      () =>
        Promise.all([
          createTopic({ topic: this.input, partitions: PARTITIONS, replicas: 3 }),
          createTopic({ topic: this.output, partitions: PARTITIONS, replicas: 3 }),
          createTopic({ topic: this.capture, partitions: 1, replicas: 3 }),
        ]),
      { label: 'The topic creation' }
    )
    const warm = new Kafka({
      clientId: `${this.name}-warm`,
      brokers,
      logLevel: logLevel.NOTHING,
    }).producer({ transactionalId: `${this.name}-warm`, retry: RETRY })
    this.cleanup.add('warm-up producer', () => warm.disconnect())
    await bounded(
      signal,
      INIT_MS,
      async () => {
        await warm.connect()
        const first = await warm.transaction()
        await first.abort()
      },
      { label: 'The warm-up of the transaction coordinator' }
    )
    const seeder = new Kafka({
      clientId: `${this.name}-seed`,
      brokers,
      logLevel: logLevel.NOTHING,
    }).producer()
    this.cleanup.add('seeder', () => seeder.disconnect())
    await bounded(signal, STEP_MS, () => seeder.connect(), { label: 'The seeder connect' })
    await bounded(
      signal,
      STEP_MS * 2,
      async (scope) => {
        for (const { partition, ids } of this.inputByPartition) {
          for (let at = 0; at < ids.length; at += SEED_CHUNK) {
            scope.throwIfAborted()
            await seeder.send({
              topic: this.input,
              acks: -1,
              messages: ids
                .slice(at, at + SEED_CHUNK)
                .map((id) => ({ partition, value: String(id) })),
            })
          }
        }
      },
      { label: 'The seeding' }
    )
    const log = await bounded(signal, STEP_MS, () => readLog({ brokers, topic: this.input }), {
      label: 'The input read',
    })
    const written = [...log.entries()].map(([partition, { records }]) => ({
      partition,
      ids: records
        .filter(({ batchContext }) => !batchContext.isControlBatch)
        .map(({ value }) => Number(value.toString())),
    }))
    const difference = firstDifference(written, this.inputByPartition)
    if (difference) {
      throw new Error(`The input log differs from what was written: ${JSON.stringify(difference)}`)
    }
    this.admin = new Kafka({
      clientId: `${this.name}-observer`,
      brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.observer.socketFactory,
    }).admin()
    this.enlist(this.observer.chaos)
    this.bounceAdmin = new Kafka({
      clientId: `${this.name}-bounce`,
      brokers,
      logLevel: logLevel.NOTHING,
    }).admin()
    this.cleanup.add('observer admin', () => this.admin.disconnect())
    this.cleanup.add('bounce admin', () => this.bounceAdmin.disconnect())
    await bounded(signal, STEP_MS, () => this.admin.connect(), { label: 'The observer connect' })
    await bounded(signal, STEP_MS, () => this.bounceAdmin.connect(), {
      label: 'The bounce admin connect',
    })
  }

  /**
   * @param {AbortSignal} signal
   * @param {string} [groupId] the copiers' group by default
   * @returns {Promise<number[]>} what the broker holds committed for the group on each input
   *   partition, as the offset a consumer resumes from; read from the coordinator, never from a copier
   * @throws {Error} the read's failure, or the reason `signal` aborted with
   */
  async committedOffsets(signal, groupId = this.groupId) {
    const [{ partitions }] = await retryCoordinator(signal, () =>
      this.admin.fetchOffsets({ groupId, topics: [this.input] })
    )
    return Array.from({ length: PARTITIONS }, (_, partition) =>
      resumeFrom(partitions.find((entry) => entry.partition === partition).offset)
    )
  }

  /**
   * `committedOffsets`, read again after a failure: the connection of a reader that is itself under
   * a fault may be reset, and the admin client does not retry that.
   * @returns {Promise<number[]>}
   * @throws {Error} the last failure, once the attempts ran out
   */
  async committedOffsetsPatiently() {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.committedOffsets(NEVER)
      } catch (error) {
        if (attempt >= READ_ATTEMPTS) throw error
        await delay(NEVER, READ_RETRY_MS)
      }
    }
  }

  /**
   * Keeps one reading of the broker's committed offsets, at most once every `SNAPSHOT_EVERY_MS`
   * unless forced. A reading that fails is skipped: the cluster may be rolling.
   * @param {AbortSignal} signal
   * @param {boolean} [force=false] fail instead of skipping
   * @returns {Promise<void>}
   */
  async snapshot(signal, force = false) {
    if (!force && Date.now() - this.lastSnapshotAt < SNAPSHOT_EVERY_MS) return
    this.lastSnapshotAt = Date.now()
    try {
      this.snapshots.push({ at: this.lastSnapshotAt, offsets: await this.committedOffsets(signal) })
    } catch (error) {
      signal.throwIfAborted()
      if (force) throw error
    }
  }

  /**
   * @returns {boolean} whether the last reading shows every input record committed
   */
  complete() {
    const latest = this.snapshots.at(-1)
    return (
      latest !== undefined &&
      latest.offsets.every((offset, partition) => offset === this.ends[partition])
    )
  }

  /**
   * @param {number} at a time, ms since the epoch
   * @returns {number} the input records committed by the last reading before `at`
   */
  copiedBefore(at) {
    const before = this.snapshots.filter((snapshot) => snapshot.at <= at).at(-1)
    return before ? before.offsets.reduce((sum, offset) => sum + offset, 0) : 0
  }

  /**
   * Starts every slot's copier: producer first, which takes over the slot's transactional id, then
   * the consumer.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async spawn(signal) {
    for (let slot = 0; slot < SLOTS; slot++) {
      const copier = new Copier(this, slot, 0)
      this.copiers[slot] = copier
      await copier.openProducer(signal)
    }
    for (const copier of this.copiers) await copier.startConsumer(signal)
  }

  /**
   * Replaces a slot's copier: the new one takes over the transactional id, which fences the old
   * one, then joins the group, and the old one is closed in the background. The old copier's
   * transaction in flight, if any, is the stale owner the fence must reject.
   * @param {number} slot
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async replace(slot, signal) {
    const old = this.copiers[slot]
    const next = new Copier(this, slot, old.incarnation + 1)
    try {
      await next.openProducer(signal)
      old.instance.fencedAt = Date.now()
      this.replacedAt.push(old.instance.fencedAt)
      this.copiers[slot] = next
      old.retire()
      this.leaving.push(old.close().catch((error) => this.closeFailures.push(describeError(error))))
      await next.startConsumer(signal)
    } catch (error) {
      if (this.copiers[slot] === next) {
        next.failure ??= error
      } else {
        next.retire()
        this.leaving.push(
          next.close().catch((failure) => this.closeFailures.push(describeError(failure)))
        )
      }
      throw error
    }
  }

  /**
   * Replaces every copier that stopped on a failure. A replacement that fails for a reason other
   * than a step running out of time, as one made while a broker is down may, is tried again at the
   * next round.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async heal(signal) {
    for (const copier of [...this.copiers]) {
      if (!copier.failure) continue
      if (!copier.recovery) {
        copier.recovery = {
          instance: copier.id,
          error: describeError(copier.failure),
          retries: 0,
          lastRetry: null,
        }
        this.recoveries.push(copier.recovery)
      }
      try {
        await this.replace(copier.slot, signal)
      } catch (error) {
        signal.throwIfAborted()
        if (liveRun.tainted) throw error
        copier.recovery.retries += 1
        copier.recovery.lastRetry = describeError(error)
      }
    }
  }

  /**
   * One round of the supervisor: recover failed copiers, replace one on schedule while connections
   * are being killed and stop the killing at its end, keep a reading of the broker's offsets.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<boolean>} whether the faults are over and every record is committed
   */
  async tick(signal) {
    await this.heal(signal)
    if (this.kind === 'socket' && !this.faultsDone) {
      const elapsed = performance.now() - this.trafficAt
      const due = FIRST_REPLACEMENT_MS + this.replacements * REPLACE_EVERY_MS
      if (this.killing && this.replacements < MAX_REPLACEMENTS && elapsed >= due) {
        try {
          await this.replace(this.replacements % SLOTS, signal)
          this.replacements += 1
        } catch (error) {
          signal.throwIfAborted()
          if (liveRun.tainted) throw error
          this.plannedFailures += 1
        }
      }
      if (this.killing && this.complete()) this.endChaos()
      this.faultsDone = !this.killing
    }
    if (this.faultsDone) this.paceMs = 0
    await this.snapshot(signal)
    return this.faultsDone && this.complete()
  }

  /**
   * Rolls the brokers once clean and once hard, one node at a time and never two down, noting for
   * each outage how much of the input was committed when it began and whether copying was under
   * way. Once the last node is back and its window has passed, a copier is restarted, so that a
   * restart position is judged on a run that has seen outages.
   * @param {AbortSignal} signal the traffic phase's scope
   * @returns {Promise<void>}
   * @throws {Error} a failed round, after it restored its node, or the restart's failure
   */
  async bounce(signal) {
    const bouncer = new BrokerBouncer()
    for (const { name, hard } of BOUNCE_ROUNDS) {
      const outages = await bounded(
        signal,
        BOUNCE_ROUND_MS,
        (scope) =>
          bouncer.rollingBounce({
            hard,
            admin: this.bounceAdmin,
            topics: [this.input, this.output],
            signal: scope,
          }),
        { label: `The ${name} rolling restart` }
      )
      for (const outage of outages) {
        this.outages.push({
          ...outage,
          round: name,
          copiedAtDown: this.copiedBefore(outage.tDown),
          trafficNear: this.chunks.some(
            ({ startedAt }) =>
              startedAt >= outage.tDown - TRAFFIC_WINDOW_MS && startedAt <= outage.tRestored
          ),
        })
      }
      if (name !== BOUNCE_ROUNDS.at(-1).name) await delay(signal, BETWEEN_ROUNDS_MS)
    }
    await delay(signal, 2 * STABLE_MS)
    await this.replace(0, signal)
    this.replacements += 1
    this.faultsDone = true
  }

  /**
   * Copies under the fault until every record is committed, then stops the copiers: their
   * transactions are joined and their consumers stopped, while their producers stay connected.
   * @param {AbortSignal} signal the traffic phase's scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the copying or of the faults, or the reason `signal`
   *   aborted with
   */
  async traffic(signal) {
    await this.spawn(signal)
    this.trafficAt = performance.now()
    if (this.kind === 'socket') this.startChaos()
    const bouncing = this.kind === 'brokers' ? this.bounce(signal) : Promise.resolve()
    this.cleanup.add('rolling restart', () => bouncing.then(noop, noop))
    try {
      await Promise.all([
        bouncing,
        pollUntil(signal, Infinity, (scope) => this.tick(scope), {
          delayMs: TICK_MS,
          label: 'The copying',
        }),
      ])
    } finally {
      this.endChaos()
    }
    for (const copier of this.copiers) await copier.stop(signal)
    await bounded(signal, STEP_MS, () => Promise.all(this.leaving), {
      label: 'The closes of replaced copiers',
    })
    await this.snapshot(signal, true)
  }

  /**
   * Reads what a `read_committed` consumer is given from the output topic, once it has the
   * expected number of records and has been quiet.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<Array<{ partition: number, ids: number[] }>>}
   */
  async readVisible(signal) {
    const reader = new Kafka({
      clientId: `${this.name}-reader`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    }).consumer({ groupId: `${this.name}-reader` })
    this.cleanup.add('output reader', () => reader.disconnect())
    const seen = Array.from({ length: PARTITIONS }, () => [])
    let count = 0
    await bounded(signal, STEP_MS, () => reader.connect(), { label: 'The reader connect' })
    await bounded(
      signal,
      STEP_MS,
      () => reader.subscribe({ topics: [this.output], fromBeginning: true }),
      { label: 'The reader subscription' }
    )
    await bounded(
      signal,
      JOIN_MS,
      () =>
        reader.run({
          eachMessage: async ({ partition, message }) => {
            seen[partition].push(Number(message.value.toString()))
            count += 1
          },
        }),
      { label: 'The reader join' }
    )
    const until = performance.now() + READ_MS
    const complete = () => count >= RECORDS
    while (!complete() && performance.now() < until) await delay(signal, TICK_MS)
    await delay(signal, QUIET_MS)
    await bounded(signal, STEP_MS, () => reader.stop(), { label: 'The reader stop' })
    return seen.map((ids, partition) => ({ partition, ids }))
  }

  /**
   * The oracle readings, once every copier stopped: the broker's committed offsets, the committed
   * output as a public reader and as the raw log decodes it, and the physical log with what an
   * abort left in it.
   * @param {AbortSignal} signal the observe phase's scope
   * @returns {Promise<void>} what it read is kept in `observed`
   * @throws {Error} a read that did not settle in time, or the reason `signal` aborted with
   */
  async observe(signal) {
    const { brokers, output } = this
    const brokerOffsets = await this.committedOffsets(signal)
    const visibleByPartition = await this.readVisible(signal)
    const physical = await bounded(signal, STEP_MS, () => readLog({ brokers, topic: output }), {
      label: 'The physical read',
    })
    const committed = await bounded(
      signal,
      STEP_MS,
      () => readLog({ brokers, topic: output, isolationLevel: ISOLATION_LEVEL.READ_COMMITTED }),
      { label: 'The committed read' }
    )
    const raw = [...committed.entries()].map(([partition, part]) => ({
      partition,
      ids: committedRecords(part).map(({ value }) => Number(value.toString())),
    }))
    this.observed = {
      brokerOffsets,
      visibleByPartition,
      rawVisibleEqualsPublic: firstDifference(raw, visibleByPartition) === null,
      physicalByPartition: [...physical.entries()].map(([partition, { records }]) => {
        const data = records.filter(({ batchContext }) => !batchContext.isControlBatch)
        return {
          partition,
          records: data.length,
          committed: raw[partition].ids.length,
          aborted: data.length - raw[partition].ids.length,
          abortedTransactions: committed.get(partition).abortedTransactions.length,
        }
      }),
    }
  }

  /**
   * Opens a producer for the stale-owner control.
   * @param {string} label
   * @param {string} transactionalId
   * @param {boolean} probed whether it gets a closing admission probe
   * @returns {{ producer: object, probe: ProducerProbe|null }}
   */
  openControlProducer(label, transactionalId, probed) {
    const clientId = `${this.name}-${label}`
    const sockets = createLiveSockets()
    const producer = new Kafka({
      clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: sockets.socketFactory,
    }).producer({
      transactionalId,
      retry: RETRY,
      bufferMemory: BUFFER_MEMORY,
      maxRequestSize: MAX_REQUEST_SIZE,
    })
    const probe = probed
      ? new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          transactionalId,
          maxRequestSize: MAX_REQUEST_SIZE,
          brokers: this.brokers,
        }).install()
      : null
    if (probe) this.cleanup.add(`${label} admission probe`, () => probe.restore())
    this.cleanup.add(`${label} producer`, () =>
      producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })
    )
    return { producer, probe }
  }

  /**
   * One controlled stale owner. A producer holds a transaction open, a second one takes over its
   * transactional id and commits offsets for a group of its own, and then the first one is let
   * go on: in the `sendOffsets` variant it tries to add offsets, in the `commit` variant it had
   * added them before it was fenced and tries to commit. The attempt must be rejected as a fenced
   * producer, must change no offset (read from the coordinator before and after), and must leave
   * nothing of the fenced transaction readable. The fenced producer, armed by the fencing, must
   * keep refusing work, disconnect and hold nothing.
   * @param {'sendOffsets'|'commit'} variant what the stale owner attempts
   * @param {AbortSignal} signal the stale phase's scope
   * @returns {Promise<void>}
   * @throws {Error} a step that did not settle in time, or the reason `signal` aborted with
   */
  async staleOwner(variant, signal) {
    const transactionalId = `${this.name}-stale-${variant}`
    const groupId = `${this.groupId}-stale-${variant}`
    const output = `${this.staleOutput}-${variant}`
    const offsetsFor = (offset) => [
      { topic: this.input, partitions: [{ partition: 0, offset: String(offset) }] },
    ]
    const committed = async (scope) => {
      const [{ partitions }] = await retryCoordinator(scope, () =>
        this.admin.fetchOffsets({ groupId, topics: [this.input] })
      )
      return partitions.find((entry) => entry.partition === 0).offset
    }
    await bounded(
      signal,
      STEP_MS,
      () => createTopic({ topic: output, partitions: 1, replicas: 3 }),
      { label: `The ${variant} output topic` }
    )
    const old = this.openControlProducer(`stale-${variant}-old`, transactionalId, true)
    await bounded(signal, STEP_MS, () => old.producer.connect(), {
      label: 'The stale owner connect',
    })
    const held = await bounded(signal, STEP_MS, () => old.producer.transaction(), {
      label: 'The stale owner transaction',
    })
    await bounded(
      signal,
      STEP_MS,
      (scope) =>
        old.probe.capture(
          () => held.send({ topic: output, messages: [{ value: 'stale' }], acks: -1 }),
          { signal: scope }
        ),
      { label: 'The stale owner send' }
    )
    if (variant === 'commit') {
      await bounded(
        signal,
        STEP_MS,
        () => held.sendOffsets({ consumerGroupId: groupId, topics: offsetsFor(9) }),
        { label: 'The stale owner offsets, before the fence' }
      )
    }

    const successor = this.openControlProducer(`stale-${variant}-new`, transactionalId, false)
    await bounded(signal, STEP_MS, () => successor.producer.connect(), {
      label: 'The successor connect',
    })
    await bounded(
      signal,
      INIT_MS,
      async () => {
        const next = await successor.producer.transaction()
        await next.send({ topic: output, messages: [{ value: 'current' }], acks: -1 })
        await next.sendOffsets({ consumerGroupId: groupId, topics: offsetsFor(5) })
        await next.commit()
      },
      { label: 'The successor transaction' }
    )
    await pollUntil(signal, STEP_MS, async (scope) => (await committed(scope)) === '5', {
      label: 'The successor commit becoming visible',
    })
    const before = await committed(signal)

    const attempt = await outcomeOf(
      bounded(
        signal,
        STEP_MS,
        () =>
          variant === 'commit'
            ? held.commit()
            : held.sendOffsets({ consumerGroupId: groupId, topics: offsetsFor(9) }),
        { label: `The stale ${variant}` }
      )
    )
    const after = await committed(signal)
    const visible = await bounded(
      signal,
      STEP_MS,
      async () => {
        const log = await readLog({
          brokers: this.brokers,
          topic: output,
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
        return [...log.values()].flatMap((part) =>
          committedRecords(part).map(({ value }) => value.toString())
        )
      },
      { label: 'The stale output read' }
    )
    const permanence = await bounded(signal, STEP_MS, () => armedPermanence(old.producer), {
      label: 'The armed permanence of the stale owner',
    })
    await bounded(signal, DISCONNECT_BUDGET_MS, () => old.producer.disconnect(), {
      label: 'The disconnect of the stale owner',
    })
    const fatalCleanup = await old.probe.settled({ signal })
    old.probe.restore()
    this.staleCommitResults.push({
      arm: this.kind,
      variant,
      rejected:
        attempt.error !== null &&
        attempt.error.category === 'applicationRecoverable' &&
        FENCING_CODES.includes(errorCodeOf(attempt.error)),
      before,
      after,
      visible,
      attempt: describeError(attempt.error),
      armed: describeError(permanence.armed),
      sameForFlush: permanence.flush === permanence.armed,
      sameForConnect: permanence.connect === permanence.armed,
      fatalCleanup,
    })
  }

  /**
   * Both stale-owner variants, one after the other.
   * @param {AbortSignal} signal the stale phase's scope
   * @returns {Promise<void>}
   */
  async staleOwners(signal) {
    await this.staleOwner('sendOffsets', signal)
    await this.staleOwner('commit', signal)
  }

  /**
   * The closing admission probe on every copier producer that is still usable, one after another,
   * since the probe's spies are process-wide: a committed one-record transaction captures the
   * producer, then the probe runs in a fresh transaction.
   * @param {AbortSignal} signal the probe phase's scope
   * @returns {Promise<void>}
   * @throws {Error} through a failed expectation of the probe, or the reason `signal` aborted with
   */
  async probeSlots(signal) {
    for (const copier of this.copiers.filter(({ failure }) => !failure)) {
      const clientId = `${this.name}-${copier.id}-producer`
      const probe = new ProducerProbe({
        producer: copier.producer,
        frames: copier.producerSockets.frames,
        clientId,
        transactionalId: copier.transactionalId,
        maxRequestSize: MAX_REQUEST_SIZE,
        brokers: this.brokers,
      }).install()
      this.cleanup.add(`${copier.id} admission probe`, () => probe.restore())
      try {
        await bounded(
          signal,
          STEP_MS,
          (scope) =>
            probe.capture(
              async () => {
                const transaction = await copier.producer.transaction()
                await transaction.send({
                  topic: this.capture,
                  messages: [{ value: copier.id }],
                  acks: -1,
                })
                await transaction.commit()
              },
              { signal: scope }
            ),
          { label: `The capture of ${copier.id}` }
        )
        const sender = await bounded(signal, STEP_MS, () => copier.producer.transaction(), {
          label: `The probe transaction of ${copier.id}`,
        })
        this.probes.push({
          arm: this.kind,
          slot: copier.slot,
          instance: copier.id,
          closing: await probe.closing({
            sender,
            topic: `${this.name}-probe-${copier.slot}`,
            finish: () => sender.commit(),
            isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
            budgetMs: DELIVERY_BUDGET_MS,
            signal,
          }),
        })
      } finally {
        probe.restore()
      }
    }
  }

  /**
   * The faults and the copying, the oracle readings, the stale-owner control and the closing
   * probes, each in its own phase of the case's timeline. What each part saw is stamped as it
   * completes.
   * @param {CaseTimeline} timeline
   * @param {AbortSignal} scope the case's
   * @returns {Promise<object>} the observation `accept` judges
   * @throws {Error} a phase's failure
   */
  async run(timeline, scope) {
    this.startedAt = Date.now()
    await timeline.run('traffic', (signal) => this.traffic(signal), scope)
    writeEvidence(EVIDENCE, {
      step: 'arm',
      arm: this.kind,
      killedConnections: this.killedConnections,
      replacements: this.replacements,
      plannedReplacementFailures: this.plannedFailures,
      recoveries: this.recoveries,
      outages: this.outages,
      closeFailures: this.closeFailures,
      durationMs: Date.now() - this.startedAt,
      timings: timeline.timings(),
    })
    await timeline.run('observe', (signal) => this.observe(signal), scope)
    await timeline.run('stale', (signal) => this.staleOwners(signal), scope)

    const { visibleByPartition, physicalByPartition, brokerOffsets } = this.observed
    const delivery = deliveryVerdict(this.chunks, this.ends)
    const ownership = {
      ...ownershipVerdict(this.chunks, this.instances),
      fencedOwners: this.staleCommitResults.map(
        ({ variant, armed, sameForFlush, sameForConnect, fatalCleanup }) => ({
          variant,
          armed,
          sameForFlush,
          sameForConnect,
          fatalCleanup,
        })
      ),
    }
    const rebalances = rebalanceVerdict(this.members, this.instances, this.outages, this.replacedAt)
    const rebalance = {
      policy:
        this.kind === 'brokers'
          ? 'classic group: none in a short outage, at most one change in a long one'
          : 'copiers are replaced by design: reported, not asserted',
      events: this.members.length,
      outages: rebalances,
      assertedViolations: rebalances.filter(({ violation }) => violation),
    }
    const commitSnapshots = snapshotVerdict(this.snapshots, visibleByPartition, this.chunks)
    writeEvidence(EVIDENCE, {
      step: 'oracle',
      arm: this.kind,
      log: {
        visibleByPartition,
        physical: physicalByPartition,
        aborted: physicalByPartition.reduce((sum, { aborted }) => sum + aborted, 0),
        rawVisibleEqualsPublic: this.observed.rawVisibleEqualsPublic,
      },
      committedOffsets: {
        brokerOffsets,
        snapshots: commitSnapshots,
        restartPositions: this.restartPositions,
        staleCommitResults: this.staleCommitResults,
      },
      handlerDelivery: delivery,
      ownership,
      rebalance,
      replay: REPLAY,
    })

    await timeline.run('probe', (signal) => this.probeSlots(signal), scope)
    for (const record of this.probes) writeEvidence(EVIDENCE, { step: 'probe', ...record })
    return {
      inputByPartition: this.inputByPartition,
      visibleByPartition,
      physicalByPartition,
      brokerOffsets,
      rawVisibleEqualsPublic: this.observed.rawVisibleEqualsPublic,
      commitSnapshots,
      restartPositions: this.restartPositions,
      staleCommitResults: this.staleCommitResults,
      ownership,
      rebalance,
      delivery,
      admissionProbes: this.probes,
      killedConnections: this.killedConnections,
      replacements: this.replacements,
      outages: this.outages,
    }
  }
}

/**
 * Live proof of exactly-once consume-transform-produce (KIP-98, KIP-447) under chaos. Two copiers
 * in one group copy 20000 input records over six partitions to an output topic with transactions
 * that carry the input offsets and are fenced by the group membership, each slot on a stable
 * transactional id. One arm kills every client connection (a consumer's from the moment it is a
 * member) after 0.5 to 1.5 s while copiers are replaced; the other rolls the three brokers once
 * clean and once hard. The committed output must hold every input id once and in order per
 * partition, aborted physical records counted apart;
 * the broker's committed offsets, read from the coordinator, must follow only committed
 * transactions and equal the position a restarted copier resumes from; a producer fenced by its
 * replacement must have its `sendOffsets` and `commit` rejected without moving an offset; and the
 * handlers must have been given every record, none of them after a commit that covered it.
 * Handler delivery is at least once for input after the last committed transaction, not an
 * exactly-once promise. The closing admission probe then runs on each surviving copier producer.
 *
 * Replay sets: log, committed offsets and handler delivery, each judged on its own reading.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
 */
describeCopier('Producer > exactly-once copying under chaos (live)', () => {
  /** @type {{ level: number, epoch: number }} the cluster's finalized transaction version */
  let cluster

  beforeAll(async () => {
    liveRun.assertClean()
    cluster = await bounded(
      NEVER,
      HOOK_BUDGET_MS - CASE_JOIN_MS,
      () => settledTransactionVersion(),
      {
        label: 'The setup',
      }
    )
  }, HOOK_BUDGET_MS)

  /**
   * One arm as a live case.
   * @param {{ kind: 'socket'|'brokers', budgetMs: number, phases: Record<string, number> }} arm
   * @returns {(...row: unknown[]) => Promise<void>}
   */
  const armCase = ({ kind, budgetMs, phases }) =>
    liveCase(budgetMs, async (scope) => {
      const timeline = new CaseTimeline({ phases, limitMs: budgetMs, reserveMs: JOIN_RESERVE_MS })
      const cleanup = new CleanupStack()

      await cleanup.run(
        async () => {
          const scenario = new CopierScenario({
            brokers: plainTextBrokers(),
            kind,
            transactionVersion: cluster.level,
            cleanup,
          })
          writeEvidence(EVIDENCE, {
            step: 'setup',
            arm: kind,
            topics: { input: scenario.input, output: scenario.output },
            partitions: PARTITIONS,
            recordCount: RECORDS,
            transactionVersion: cluster.level,
            config: {
              slots: SLOTS,
              chunk: CHUNK,
              sessionTimeout: CLASSIC_SESSION_MS,
              heartbeatInterval: CLASSIC_HEARTBEAT_MS,
              maxBytesPerPartition: FETCH_BYTES,
              retry: RETRY,
              bufferMemory: BUFFER_MEMORY,
              maxRequestSize: MAX_REQUEST_SIZE,
            },
          })
          await timeline.run('setup', (signal) => scenario.start(signal), scope)
          accept(kind, await scenario.run(timeline, scope))
        },
        { budgetMs: phases.cleanup }
      )
    })

  test(
    'committed output survives connection replacement and restores broker positions',
    armCase(SOCKET_ARM),
    caseTimeout(SOCKET_ARM.budgetMs)
  )

  test(
    'committed output survives clean and hard rolling restarts',
    armCase(BROKERS_ARM),
    caseTimeout(BROKERS_ARM.budgetMs)
  )
})
