const RebalanceAssertions = require('./fixtures/rebalanceAssertions')
const PRODUCTION_RETRY = require('../../retry/defaults')
const Broker = require('../../broker')
const { Kafka, logLevel, PartitionAssigners, ConfigResourceTypes } = require('../../../index')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const {
  createConnectionPool,
  newLogger,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  evidenceLines,
} = require('testHelpers')
const { BrokerBouncer, NODES, waitForFullIsr } = require('../../../testHelpers/leaderElection')
const { readLog, endOffsets } = require('../../../testHelpers/rawLog')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { createLedger, sleep, waitForCondition, withTimeout } = require('./fixtures/liveScenario')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')

// Gated: it pauses, stops and kills the compose brokers one at a time. Run alone, as the only
// cluster user: `npm run test:e2e:broker-bounce`.
const describeBounce = process.env.BROKER_BOUNCE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'broker-bounce'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
const PARTITIONS = 6
const SEND_INTERVAL_MS = 20
const CLASSIC_SESSION_MS = 30000
const CLASSIC_HEARTBEAT_MS = 3000
// The consumer-protocol group's liveness window and heartbeat are the broker's (docker-compose.yml).
const CONSUMER_SESSION_MS = 6000
const CONSUMER_HEARTBEAT_MS = 500
const SHORT_FRACTION = 0.8
const PAUSE_MS = 2000
const LONG_HOLD_MS = 45000
const STABLE_MS = 3000
const AUTO_COMMIT_INTERVAL_MS = 1000
// Each handler spans this long, so another member's delivery can fall inside its window.
const HANDLER_MS = 5
const STABLE_BUDGET_MS = 120000
const SETTLE_BUDGET_MS = 30000
const DELIVERY_BUDGET = { attempts: 240, delay: 250 } // 60 s
const JOIN_BUDGET_MS = 60000
const HOOK_BUDGET_MS = 180000
const DISCONNECT_BUDGET_MS = 30000
const BROKERS_BACK_MS = 120000
// The whole lane: ten outages, each with its restore, ISR and stabilisation waits.
const SCENARIO_BUDGET_MS = 15 * 60 * 1000
const MEMBERSHIP = [
  'GROUP_JOIN',
  'REBALANCING',
  'PARTITIONS_ASSIGNED',
  'PARTITIONS_REVOKED',
  'PARTITIONS_LOST',
]

/**
 * An outage is short for a group when it lasts less than 0.8 of the group's liveness window.
 * @param {number} outageMs measured through restored communication
 * @param {number} windowMs the group's session timeout
 * @returns {'short'|'long'}
 */
const armKind = (outageMs, windowMs) => (outageMs < SHORT_FRACTION * windowMs ? 'short' : 'long')

/**
 * Everything one group's members did, in the order observed: deliveries with their handler
 * windows, membership events, crashes and commits. `seq` orders entries; `at` (ms) places them
 * against outage windows.
 */
class GroupJournal {
  /** @param {string} name @param {number} windowMs the group's liveness window */
  constructor(name, windowMs) {
    this.name = name
    this.windowMs = windowMs
    this.entries = []
    this.seq = 0
    this.ledger = createLedger()
    this.maxDelivered = new Map()
  }

  /** @param {object} entry @returns {object} the entry, stamped */
  add(entry) {
    const stamped = { seq: this.seq++, at: Date.now(), ...entry }
    this.entries.push(stamped)
    return stamped
  }

  /**
   * Opens a handler window; `delivered` closes it.
   * @param {string} member
   * @param {{ partition: number, offset: string }} record
   * @returns {object} the window
   */
  started(member, { partition, offset }) {
    return this.add({ kind: 'START', member, partition, offset })
  }

  /** @param {object} window @param {string} topic @returns {void} */
  delivered(window, topic) {
    const { member, partition, offset } = window
    window.end = this.seq++
    this.ledger.delivered(member, { topic, partition, offset })
    const highest = this.maxDelivered.get(partition) ?? -1n
    if (BigInt(offset) > highest) this.maxDelivered.set(partition, BigInt(offset))
  }

  /**
   * @param {string} member
   * @param {object} payload a COMMIT_OFFSETS payload
   * @returns {void}
   */
  committed(member, payload) {
    const limits = Object.fromEntries(
      [...this.maxDelivered].map(([partition, offset]) => [partition, String(offset + 1n)])
    )
    this.add({ kind: 'COMMIT_OFFSETS', member, payload, limits })
  }
}

/**
 * Oracle part 1, the durable log: every acked sequence value is in the log and none is twice.
 * @param {number[]} acked
 * @param {number[]} logged every `seq` the raw read found, in log order
 * @returns {{ missing: number[], twice: number[] }}
 */
const logVerdict = (acked, logged) => {
  const counts = new Map()
  logged.forEach((seq) => counts.set(seq, (counts.get(seq) || 0) + 1))
  return {
    missing: acked.filter((seq) => !counts.has(seq)),
    twice: [...counts].filter(([, count]) => count > 1).map(([seq]) => seq),
  }
}

/**
 * When an outage's window closes: its stabilisation, `STABLE_MS` after the node is restored.
 * @param {{ tRestored: number }} outage
 * @returns {number} ms since the epoch
 */
const windowEnd = ({ tRestored }) => tRestored + STABLE_MS

/**
 * The outage whose window, stabilisation included, holds `at`.
 * @param {object[]} outages
 * @param {number} at
 * @returns {object|undefined}
 */
const outageAt = (outages, at) =>
  outages.find((outage) => at >= outage.tDown && at <= windowEnd(outage))

/**
 * Oracle part 2, delivery: every acked record delivered to the group, and a duplicate only with a
 * membership change or a crash of the group between its two deliveries, never in a short arm.
 * @param {GroupJournal} journal
 * @param {object[]} outages
 * @returns {{ missing: string[], unexplained: string[] }}
 */
const deliveryVerdict = (journal, outages) => {
  const { missing } = journal.ledger.summary()
  const windows = journal.entries.filter(({ kind }) => kind === 'START')
  const changes = journal.entries.filter(
    ({ kind }) => MEMBERSHIP.includes(kind) || kind === 'CRASH'
  )
  const byOffset = new Map()
  for (const window of windows) {
    const key = `${window.partition}|${window.offset}`
    byOffset.set(key, [...(byOffset.get(key) || []), window])
  }
  const unexplained = []
  for (const [key, deliveries] of byOffset) {
    for (let index = 1; index < deliveries.length; index++) {
      const [first, second] = [deliveries[index - 1], deliveries[index]]
      const explained = changes.some(({ seq }) => seq > first.seq && seq < second.seq)
      const outage = outageAt(outages, second.at)
      const short = outage && armKind(outage.outageMs, journal.windowMs) === 'short'
      if (!explained || short) unexplained.push(key)
    }
  }
  return { missing, unexplained }
}

/**
 * Oracle part 3, unexpected rebalances: none in a short arm through its stabilisation, and at most
 * one membership change in a long arm. Events less than the stabilisation window apart belong
 * to one change. A member's PARTITIONS_LOST or CRASH keeps its loss pending until it is assigned
 * again, so its delayed rejoin stays in that change. A new loss after assignment during the
 * current change starts another change even if another member's loss is still pending. A crash
 * counts even when no membership event follows it. The events count excludes standalone crashes.
 * @param {GroupJournal} journal
 * @param {object[]} outages each with `before`/`after` samples of the members' `groupMetadata()`
 * @returns {{ arm: string, kind: string, events: number, changes: number, violation: boolean }[]}
 */
const rebalanceVerdict = (journal, outages) =>
  outages.map((outage) => {
    const kind = armKind(outage.outageMs, journal.windowMs)
    const inWindow = journal.entries.filter(
      (entry) => entry.at >= outage.tDown && entry.at <= windowEnd(outage)
    )
    const events = inWindow.filter((entry) => MEMBERSHIP.includes(entry.kind))
    let changes = 0
    let last = -Infinity
    const unsettled = new Set()
    const assigned = new Set()
    for (const entry of inWindow) {
      const loss = entry.kind === 'PARTITIONS_LOST' || entry.kind === 'CRASH'
      if (MEMBERSHIP.includes(entry.kind) || loss) {
        if (
          last === -Infinity ||
          (loss && assigned.has(entry.member)) ||
          (unsettled.size === 0 && entry.at - last >= STABLE_MS)
        ) {
          changes++
          assigned.clear()
        }
        last = entry.at
      }
      if (loss) unsettled.add(entry.member)
      if (entry.kind === 'PARTITIONS_ASSIGNED') {
        unsettled.delete(entry.member)
        assigned.add(entry.member)
      }
    }
    const identity = ({ memberId, generationId }) => `${memberId}|${generationId}`
    const sampled =
      (outage.before?.[journal.name] || []).map(identity).join(',') !==
      (outage.after?.[journal.name] || []).map(identity).join(',')
    if (sampled && changes === 0) changes = 1
    const violation = kind === 'short' ? changes > 0 : changes > 1
    return { arm: outage.arm, kind, events: events.length, changes, violation }
  })

const shortArmStop = RebalanceAssertions.shortArmStop
const assertedRebalanceViolations = RebalanceAssertions.violations

/**
 * Oracle part 4, position: once the members have closed gracefully, each partition's committed
 * offset is the highest delivered offset plus one, and no commit ever named more than what was
 * delivered by then plus one.
 * @param {GroupJournal} journal
 * @param {string} topic
 * @param {Map<number, string>} committed the group's committed offsets, by partition
 * @returns {{ behind: number[], ahead: string[] }}
 */
const positionVerdict = (journal, topic, committed) => {
  const behind = [...journal.maxDelivered]
    .filter(([partition, offset]) => committed.get(partition) !== String(offset + 1n))
    .map(([partition]) => partition)
  const ahead = journal.entries
    .filter(({ kind }) => kind === 'COMMIT_OFFSETS')
    .flatMap(({ payload, limits }) =>
      payload.topics
        .filter((entry) => entry.topic === topic)
        .flatMap(({ partitions }) => partitions)
        .filter(({ partition, offset }) => BigInt(offset) > BigInt(limits[partition] ?? '0'))
        .map(({ partition, offset }) => `${partition}:${offset}`)
    )
  return { behind, ahead }
}

/**
 * Oracle part 5, ownership and stale work: no two members deliver one partition in overlapping
 * handler windows unless the first member's PARTITIONS_REVOKED for it came after its own window
 * opened and before the second's delivery, and no member commits a partition after its
 * PARTITIONS_LOST for it until it is assigned again.
 * @param {GroupJournal} journal
 * @param {string} topic
 * @returns {{ overlaps: string[], staleCommits: string[] }}
 */
const ownershipVerdict = (journal, topic) => {
  const windows = journal.entries.filter(({ kind }) => kind === 'START')
  const names = (entry) =>
    (entry.payload?.partitions || [])
      .filter((assigned) => assigned.topic === topic)
      .flatMap(({ partitions }) => partitions)
  const revoked = (member, partition, after, before) =>
    journal.entries.some(
      (entry) =>
        entry.kind === 'PARTITIONS_REVOKED' &&
        entry.member === member &&
        entry.seq > after &&
        entry.seq < before &&
        names(entry).includes(partition)
    )
  const overlaps = []
  for (const a of windows) {
    for (const b of windows) {
      if (a.member === b.member || a.partition !== b.partition || a.seq >= b.seq) continue
      const overlapping = b.seq < (a.end ?? Infinity)
      if (overlapping && !revoked(a.member, a.partition, a.seq, b.seq)) {
        overlaps.push(`${a.partition}:${a.member}/${b.member}`)
      }
    }
  }
  const lost = new Map()
  const staleCommits = []
  for (const entry of journal.entries) {
    const key = (partition) => `${entry.member}|${partition}`
    if (entry.kind === 'PARTITIONS_LOST') names(entry).forEach((p) => lost.set(key(p), true))
    if (entry.kind === 'PARTITIONS_ASSIGNED') names(entry).forEach((p) => lost.delete(key(p)))
    if (entry.kind === 'COMMIT_OFFSETS') {
      entry.payload.topics
        .filter((committed) => committed.topic === topic)
        .flatMap(({ partitions }) => partitions)
        .filter(({ partition }) => lost.has(key(partition)))
        .forEach(({ partition }) => staleCommits.push(`${entry.member}:${partition}`))
    }
  }
  return { overlaps, staleCommits }
}

/**
 * Waits for every producer send to settle, failed ones included, and gives up at the deadline.
 * @param {Promise<unknown>[]} sends
 * @param {number} budgetMs
 * @returns {Promise<boolean>} whether all of them settled in time
 */
const awaitSettled = async (sends, budgetMs) => {
  let settled = false
  await withTimeout(
    Promise.allSettled(sends).then(() => {
      settled = true
    }),
    budgetMs
  )
  return settled
}

/**
 * Awaits `work` under a case's scope: it starts only while the scope is live, and the awaiting
 * code never resumes on an answer that arrives after the case's deadline.
 * @template T
 * @param {AbortSignal} scope the case's or hook's scope
 * @param {() => Promise<T>} work
 * @returns {Promise<T>} what `work` returned in time
 * @throws {Error} `work`'s failure, or the reason the scope aborted with
 */
const scoped = (scope, work) => bounded(scope, Infinity, work)

describe('Broker bounce oracle', () => {
  const journalOf = (entries, windowMs = 30000) => {
    const journal = new GroupJournal('g', windowMs)
    for (const entry of entries) {
      if (entry.kind === 'START') {
        const window = journal.started(entry.member, entry)
        journal.entries[journal.entries.length - 1].at = entry.at ?? 0
        if (entry.done !== false) journal.delivered(window, 't')
      } else {
        journal.add(entry).at = entry.at ?? 0
      }
    }
    return journal
  }

  test('classifies an outage per group from its measurement, never from the arm', () => {
    expect(armKind(23999, CLASSIC_SESSION_MS)).toBe('short')
    expect(armKind(24000, CLASSIC_SESSION_MS)).toBe('long')
    expect(armKind(4799, CONSUMER_SESSION_MS)).toBe('short')
    expect(armKind(4800, CONSUMER_SESSION_MS)).toBe('long')
  })

  test('the log verdict finds an acked value missing and a value written twice', () => {
    expect(logVerdict([1, 2, 3], [1, 3, 3])).toEqual({ missing: [2], twice: [3] })
    expect(logVerdict([1, 2], [1, 2, 4])).toEqual({ missing: [], twice: [] })
  })

  test('a duplicate needs a membership change or a crash between its deliveries', () => {
    const plain = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '5' },
      { kind: 'START', member: 'a', partition: 0, offset: '5' },
    ])
    const rejoined = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '5' },
      { kind: 'REBALANCING', member: 'a' },
      { kind: 'START', member: 'a', partition: 0, offset: '5' },
    ])
    const crashed = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '5' },
      { kind: 'CRASH', member: 'a' },
      { kind: 'START', member: 'b', partition: 0, offset: '5' },
    ])

    expect(deliveryVerdict(plain, []).unexplained).toEqual(['0|5'])
    expect(deliveryVerdict(rejoined, []).unexplained).toEqual([])
    expect(deliveryVerdict(crashed, []).unexplained).toEqual([])
  })

  test('a duplicate inside a short arm is never explained', () => {
    const journal = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '5', at: 100 },
      { kind: 'REBALANCING', member: 'a', at: 150 },
      { kind: 'START', member: 'a', partition: 0, offset: '5', at: 200 },
    ])
    const short = [{ tDown: 50, tRestored: 250, outageMs: 200 }]

    expect(deliveryVerdict(journal, short).unexplained).toEqual(['0|5'])
  })

  test('the delivery verdict lists an acked record no member delivered', () => {
    const journal = journalOf([{ kind: 'START', member: 'a', partition: 0, offset: '1' }])
    journal.ledger.produced({ topic: 't', partition: 0, offset: '1' })
    journal.ledger.produced({ topic: 't', partition: 0, offset: '2' })

    expect(deliveryVerdict(journal, []).missing).toEqual(['t|0|2'])
  })

  test('an event at the close of an arm window belongs to that arm, one after it does not', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 2000, outageMs: 1000 }
    const at = (time) => journalOf([{ kind: 'REBALANCING', member: 'a', at: time }])

    expect(windowEnd(outage)).toBe(2000 + STABLE_MS)
    expect(rebalanceVerdict(at(windowEnd(outage)), [outage])[0]).toMatchObject({ events: 1 })
    expect(rebalanceVerdict(at(windowEnd(outage) + 1), [outage])[0]).toMatchObject({ events: 0 })
    expect(outageAt([outage], windowEnd(outage))).toBe(outage)
    expect(outageAt([outage], windowEnd(outage) + 1)).toBeUndefined()
  })

  test('a short arm allows no membership event; a long arm allows one change', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 2000, outageMs: 1000 }
    const one = journalOf([
      { kind: 'REBALANCING', member: 'a', at: 1500 },
      { kind: 'GROUP_JOIN', member: 'a', at: 1600 },
    ])
    const two = journalOf([
      { kind: 'REBALANCING', member: 'a', at: 1100 },
      { kind: 'REBALANCING', member: 'a', at: 1100 + STABLE_MS },
    ])

    expect(rebalanceVerdict(one, [outage])[0]).toMatchObject({ kind: 'short', violation: true })
    expect(rebalanceVerdict(journalOf([]), [outage])[0]).toMatchObject({ violation: false })
    expect(rebalanceVerdict(one, [{ ...outage, outageMs: 25000 }])[0]).toMatchObject({
      kind: 'long',
      changes: 1,
      violation: false,
    })
    expect(rebalanceVerdict(two, [{ ...outage, outageMs: 25000 }])[0]).toMatchObject({
      changes: 2,
      violation: true,
    })
  })

  test('a loss and its late rejoin are one change; a new loss after the rejoin settled is another', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 30000, outageMs: 29000 }
    const crashed = [
      { kind: 'PARTITIONS_REVOKED', member: 'a', at: 2000 },
      { kind: 'CRASH', member: 'a', at: 2001 },
      { kind: 'GROUP_JOIN', member: 'a', at: 2001 + STABLE_MS + 4000 },
      { kind: 'PARTITIONS_REVOKED', member: 'b', at: 2001 + 2 * STABLE_MS + 5000 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'a', at: 2001 + 2 * STABLE_MS + 5100 },
    ]
    const fenced = [
      { kind: 'PARTITIONS_ASSIGNED', member: 'b', at: 2000 },
      { kind: 'REBALANCING', member: 'a', at: 4000 },
      { kind: 'PARTITIONS_LOST', member: 'a', at: 4000 },
      { kind: 'GROUP_JOIN', member: 'a', at: 4000 + STABLE_MS + 400 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'a', at: 4000 + STABLE_MS + 900 },
    ]
    const again = [
      ...crashed,
      { kind: 'PARTITIONS_LOST', member: 'b', at: 2001 + 3 * STABLE_MS + 5100 },
    ]

    expect(rebalanceVerdict(journalOf(crashed), [outage])[0]).toMatchObject({
      events: 4,
      changes: 1,
      violation: false,
    })
    expect(rebalanceVerdict(journalOf(fenced), [outage])[0]).toMatchObject({
      changes: 1,
      violation: false,
    })
    expect(rebalanceVerdict(journalOf(again), [outage])[0]).toMatchObject({
      changes: 2,
      violation: true,
    })
  })

  test('a crash before the first event still opens a change in a short arm', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 2000, outageMs: 1000 }
    const journal = journalOf([
      { kind: 'CRASH', member: 'a', at: 1100 },
      { kind: 'GROUP_JOIN', member: 'a', at: 1500 },
    ])

    expect(rebalanceVerdict(journal, [outage])[0]).toMatchObject({ changes: 1, violation: true })
  })

  test('a crash alone opens a change in a short arm, whatever the sampled identity', () => {
    const members = [{ memberId: 'm', generationId: 1 }]
    const outage = {
      arm: 'x',
      tDown: 1000,
      tRestored: 2000,
      outageMs: 1000,
      before: { g: members },
      after: { g: members },
    }
    const journal = journalOf([{ kind: 'CRASH', member: 'a', at: 1100 }])

    expect(rebalanceVerdict(journal, [outage])[0]).toMatchObject({ changes: 1, violation: true })
  })

  test('a member assigned again that loses at once starts another change', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 30000, outageMs: 29000 }
    const journal = journalOf([
      { kind: 'PARTITIONS_LOST', member: 'a', at: 2000 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'a', at: 5000 },
      { kind: 'PARTITIONS_LOST', member: 'a', at: 5100 },
    ])

    expect(rebalanceVerdict(journal, [outage])[0]).toMatchObject({ changes: 2, violation: true })
  })

  test('one member pending loss cannot hide the later loss of a recovered member', () => {
    const outage = { arm: 'x', tDown: 1000, tRestored: 30000, outageMs: 29000 }
    const owned = { partitions: [{ topic: 't', partitions: [0] }] }
    const journal = journalOf([
      { kind: 'PARTITIONS_REVOKED', member: 'a', at: 2000, payload: owned },
      { kind: 'CRASH', member: 'a', at: 2001 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'b', at: 3000, payload: owned },
      { kind: 'GROUP_JOIN', member: 'b', at: 3001 },
      { kind: 'REBALANCING', member: 'b', at: 10000 },
      { kind: 'PARTITIONS_LOST', member: 'b', at: 10000, payload: owned },
      { kind: 'GROUP_JOIN', member: 'b', at: 15000 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'b', at: 16000, payload: owned },
      { kind: 'CRASH', member: 'a', at: 20000 },
      { kind: 'CRASH', member: 'a', at: 30000 },
      { kind: 'START', member: 'b', partition: 0, offset: '0', at: 31000 },
    ])
    journal.committed('b', {
      topics: [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }],
    })
    journal.ledger.produced({ topic: 't', partition: 0, offset: '0' })

    expect(logVerdict([1], [1])).toEqual({ missing: [], twice: [] })
    expect(deliveryVerdict(journal, [outage])).toEqual({ missing: [], unexplained: [] })
    expect(positionVerdict(journal, 't', new Map([[0, '1']]))).toEqual({ behind: [], ahead: [] })
    expect(ownershipVerdict(journal, 't')).toEqual({ overlaps: [], staleCommits: [] })
    expect(rebalanceVerdict(journal, [outage])[0]).toMatchObject({ violation: true })
  })

  test('a changed member identity counts as a change even without an event', () => {
    const outage = {
      arm: 'x',
      tDown: 0,
      tRestored: 10,
      outageMs: 10,
      before: { g: [{ memberId: 'm', generationId: 1 }] },
      after: { g: [{ memberId: 'm', generationId: 2 }] },
    }

    expect(rebalanceVerdict(journalOf([]), [outage])[0]).toMatchObject({
      changes: 1,
      violation: true,
    })
  })

  test('both group protocols stop a short-arm violation and fail any rebalance violation', () => {
    const short = { arm: 'short', tDown: 1000, tRestored: 2000, outageMs: 1000 }
    const long = { arm: 'long', tDown: 100000, tRestored: 130000, outageMs: 30000 }
    const outages = [short, long]
    const churn = [
      { kind: 'PARTITIONS_LOST', member: 'a', at: 101000 },
      { kind: 'PARTITIONS_ASSIGNED', member: 'a', at: 105000 },
      { kind: 'PARTITIONS_LOST', member: 'a', at: 105100 },
    ]
    const judge = (classic, consumer) => {
      const journals = { classic: journalOf(classic, 30000), consumer: journalOf(consumer, 6000) }
      const verdict = Object.fromEntries(
        Object.entries(journals).map(([group, journal]) => [
          group,
          { rebalances: rebalanceVerdict(journal, outages) },
        ])
      )
      const stops = outages.map((_, arm) =>
        shortArmStop(
          Object.fromEntries(Object.entries(verdict).map(([g, v]) => [g, v.rebalances[arm]]))
        )
      )
      return { verdict, stops, failures: assertedRebalanceViolations(verdict) }
    }
    const consumerOnly = judge([], [{ kind: 'REBALANCING', member: 'a', at: 1500 }, ...churn])
    const classicShort = judge([{ kind: 'REBALANCING', member: 'a', at: 1500 }], [])
    const classicLong = judge(churn, [])

    expect(consumerOnly.verdict.consumer.rebalances.map(({ violation }) => violation)).toEqual([
      true,
      true,
    ])
    expect(consumerOnly.stops).toEqual([
      ['consumer', consumerOnly.verdict.consumer.rebalances[0]],
      undefined,
    ])
    expect(consumerOnly.failures).toEqual(consumerOnly.verdict.consumer.rebalances)
    expect(classicShort.stops[0]).toEqual(['classic', classicShort.verdict.classic.rebalances[0]])
    expect(classicShort.failures).toHaveLength(1)
    expect(classicLong.stops).toEqual([undefined, undefined])
    expect(classicLong.failures).toEqual([classicLong.verdict.classic.rebalances[1]])
  })

  test('the position verdict finds a committed offset behind, and a commit ahead of delivery', () => {
    const journal = journalOf([{ kind: 'START', member: 'a', partition: 0, offset: '4' }])
    journal.committed('a', {
      topics: [{ topic: 't', partitions: [{ partition: 0, offset: '5' }] }],
    })
    journal.committed('a', {
      topics: [{ topic: 't', partitions: [{ partition: 0, offset: '7' }] }],
    })

    expect(positionVerdict(journal, 't', new Map([[0, '5']]))).toEqual({
      behind: [],
      ahead: ['0:7'],
    })
    expect(positionVerdict(journal, 't', new Map([[0, '4']])).behind).toEqual([0])
  })

  test('overlapping handler windows of two members are allowed only after a revocation', () => {
    const overlap = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '1', done: false },
      { kind: 'START', member: 'b', partition: 0, offset: '2' },
    ])
    const handover = journalOf([
      { kind: 'START', member: 'a', partition: 0, offset: '1', done: false },
      {
        kind: 'PARTITIONS_REVOKED',
        member: 'a',
        payload: { partitions: [{ topic: 't', partitions: [0] }] },
      },
      { kind: 'START', member: 'b', partition: 0, offset: '2' },
    ])

    expect(ownershipVerdict(overlap, 't').overlaps).toEqual(['0:a/b'])
    expect(ownershipVerdict(handover, 't').overlaps).toEqual([])
  })

  test('historical revoke cannot excuse overlap after reassignment', () => {
    const journal = new GroupJournal('g', 30000)
    const payload = { partitions: [{ topic: 't', partitions: [0] }] }
    journal.add({ kind: 'PARTITIONS_REVOKED', member: 'a', payload })
    journal.add({ kind: 'PARTITIONS_ASSIGNED', member: 'a', payload })
    const a = journal.started('a', { partition: 0, offset: '1' })
    const b = journal.started('b', { partition: 0, offset: '2' })
    journal.delivered(a, 't')
    journal.delivered(b, 't')

    expect(ownershipVerdict(journal, 't').overlaps).toEqual(['0:a/b'])
  })

  test('sends that all settle, failed ones included, count as settled', async () => {
    const sends = [Promise.resolve(), Promise.reject(new Error('send failed'))]

    await expect(awaitSettled(sends, 1000)).resolves.toBe(true)
  })

  test('a send that never settles fails the settle check at the deadline', async () => {
    const sends = [Promise.resolve(), new Promise(() => {})]

    await expect(awaitSettled(sends, 5)).resolves.toBe(false)
  })

  test('a commit after PARTITIONS_LOST is stale until the partition is assigned again', () => {
    const commit = (offset) => ({
      kind: 'COMMIT_OFFSETS',
      member: 'a',
      payload: { topics: [{ topic: 't', partitions: [{ partition: 0, offset }] }] },
    })
    const journal = journalOf([
      {
        kind: 'PARTITIONS_LOST',
        member: 'a',
        payload: { partitions: [{ topic: 't', partitions: [0] }] },
      },
      commit('3'),
      {
        kind: 'PARTITIONS_ASSIGNED',
        member: 'a',
        payload: { partitions: [{ topic: 't', partitions: [0] }] },
      },
      commit('4'),
    ])

    expect(ownershipVerdict(journal, 't').staleCommits).toEqual(['a:0'])
  })
})

/**
 * An idempotent producer and two groups keep running while the brokers
 * are paused, stopped, killed and held down one at a time, and the outcome is judged only from
 * observed state: a fresh raw read of the log, the delivery journal and the instrumentation
 * events, and the committed offsets.
 *
 * Replay sets: durable log, none (no sequence value twice); committed offsets, back only to the
 * last commit on a crash or rebalance; handler delivery, at least once for records after the last
 * commit, nothing else.
 *
 * Arms, per node role resolved from live metadata (the classic group's coordinator, the leader of
 * partition 0, a follower-only node): A, pause for 2 000 ms; B, stop then start; C, kill then
 * start; D, once on the classic coordinator, stop and hold 45 s. An outage is short for a group
 * when it lasts less than 0.8 of that group's liveness window (classic 30 000 ms, consumer
 * protocol the broker's 6 000 ms), classified per group from the measurement.
 *
 * A deviation from the oracle is a finding to report, never a reason to widen it.
 */
describeBounce('Broker bounce: durable log, delivery and rebalances across outages (live)', () => {
  const runId = secureRandom()
  const topic = `e2e-p9a-broker-bounce-${runId}`
  const classicGroup = `e2e-p9a-broker-bounce-classic-${runId}`
  const consumerGroup = `e2e-p9a-broker-bounce-consumer-${runId}`
  const bouncer = new BrokerBouncer()
  const journals = {
    classic: new GroupJournal('classic', CLASSIC_SESSION_MS),
    consumer: new GroupJournal('consumer', CONSUMER_SESSION_MS),
  }
  const acked = []
  const failedSends = []
  const sends = []
  const outages = []
  const members = []
  let kafka
  let admin
  let producer
  let timer
  let seq = 0
  let startedAt

  /**
   * One member of one group, journalling everything it does.
   * @param {'classic'|'consumer'} group
   * @param {string} name
   * @param {object} options consumer options beyond the group's own
   */
  const startMember = async (scope, group, name, options) => {
    const journal = journals[group]
    const consumer = kafka.consumer({
      groupId: group === 'classic' ? classicGroup : consumerGroup,
      ...(group === 'consumer' ? { groupProtocol: 'consumer' } : {}),
      ...options,
    })
    for (const kind of MEMBERSHIP) {
      consumer.on(consumer.events[kind], ({ payload }) => {
        const { generationId, memberId } = consumer.groupMetadata()
        journal.add({ kind, member: name, payload, generationId, memberId })
      })
    }
    consumer.on(consumer.events.CRASH, ({ payload }) =>
      journal.add({ kind: 'CRASH', member: name, error: payload.error?.name })
    )
    consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => journal.committed(name, payload))
    members.push({ group, name, consumer })
    await scoped(scope, () => consumer.connect())
    await scoped(scope, () => consumer.subscribe({ topic, fromBeginning: true }))
    await scoped(scope, () =>
      consumer.run({
        autoCommit: true,
        autoCommitInterval: AUTO_COMMIT_INTERVAL_MS,
        eachMessage: async ({ partition, message }) => {
          const window = journal.started(name, { partition, offset: message.offset })
          await sleep(HANDLER_MS)
          journal.delivered(window, topic)
        },
      })
    )
  }

  /** @returns {Record<string, { memberId: string, generationId: number }[]>} by group */
  const sampleMembership = () =>
    Object.fromEntries(
      Object.keys(journals).map((group) => [
        group,
        members
          .filter((member) => member.group === group)
          .map(({ consumer }) => {
            const { memberId, generationId } = consumer.groupMetadata()
            return { memberId, generationId }
          }),
      ])
    )

  /** @param {number} nodeId @returns {string} the compose service */
  const serviceOf = (nodeId) => Object.entries(NODES).find(([, node]) => node.nodeId === nodeId)[0]

  /**
   * The roles the arms target, from live metadata: the classic group's coordinator, the leader of
   * partition 0, and a node that is neither.
   * @returns {Promise<{ coordinator: string, leader: string, follower: string }>}
   */
  const resolveRoles = async () => {
    const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
    try {
      await seed.connect()
      const { coordinator } = await seed.findGroupCoordinator({ groupId: classicGroup })
      const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
      const leaderId = topics[0].partitions.find(({ partitionId }) => partitionId === 0).leader
      const others = Object.values(NODES)
        .map(({ nodeId }) => nodeId)
        .filter((nodeId) => nodeId !== coordinator.nodeId && nodeId !== leaderId)
        .sort((a, b) => a - b)
      return {
        coordinator: serviceOf(coordinator.nodeId),
        leader: serviceOf(leaderId),
        follower: serviceOf(others[0]),
      }
    } finally {
      await seed.disconnect()
    }
  }

  /**
   * Waits until no member reports GROUP_JOIN or REBALANCING for the stabilisation window.
   * @param {AbortSignal} scope the case's or hook's scope: no wait starts once it has aborted
   * @returns {Promise<number>} how long it took, ms
   */
  const waitForStable = async (scope) => {
    const started = Date.now()
    const quietFor = () => {
      const last = Object.values(journals)
        .flatMap(({ entries }) => entries)
        .filter(({ kind }) => kind === 'GROUP_JOIN' || kind === 'REBALANCING')
        .reduce((latest, { at }) => Math.max(latest, at), 0)
      return Date.now() - last
    }
    while (quietFor() < STABLE_MS) {
      if (Date.now() - started > STABLE_BUDGET_MS) {
        throw new Error(`Groups not stable within ${STABLE_BUDGET_MS} ms`)
      }
      await delay(scope, 250)
    }
    return Date.now() - started
  }

  /**
   * Runs one arm on one role's node and records the outage with both groups' classification.
   * @param {AbortSignal} scope the case's scope: no step starts once the case has ended
   * @param {string} arm
   * @param {'coordinator'|'leader'|'follower'} role
   * @param {{ kind: 'pause'|'stop'|'kill', holdMs?: number }} step
   * @returns {Promise<void>}
   */
  const runArm = async (scope, arm, role, step) => {
    const roles = await scoped(scope, resolveRoles)
    const service = roles[role]
    const before = sampleMembership()
    const issuedAt = Date.now()
    const returned = await bouncer.step(
      { service, ...step },
      { admin, topics: [topic], signal: scope }
    )
    const outage = RebalanceAssertions.issueWindow(returned, issuedAt)
    await scoped(scope, () => waitForFullIsr(admin, [topic]))
    const stabilisedMs = await waitForStable(scope)
    // The verdict below reads the whole window: an event late in it belongs to this arm.
    await delay(scope, Math.max(0, windowEnd(outage) - Date.now()))
    const record = {
      ...outage,
      arm: `${arm}:${role}`,
      role,
      roles,
      before,
      after: sampleMembership(),
    }
    outages.push(record)
    const verdicts = Object.fromEntries(
      Object.entries(journals).map(([group, journal]) => [
        group,
        rebalanceVerdict(journal, [record])[0],
      ])
    )
    writeEvidence(EVIDENCE, {
      step: 'outage',
      arm: record.arm,
      node: NODES[service].nodeId,
      service,
      role,
      kind: step.kind,
      outageMs: outage.outageMs,
      windows: { classic: CLASSIC_SESSION_MS, consumer: CONSUMER_SESSION_MS },
      armKinds: {
        classic: armKind(outage.outageMs, CLASSIC_SESSION_MS),
        consumer: armKind(outage.outageMs, CONSUMER_SESSION_MS),
      },
      stabilisedMs,
      verdicts,
    })
    // The oracle's part 3: a violation in a short arm fails and stops the run.
    const stopped = shortArmStop(verdicts)
    if (stopped) {
      const [group, verdict] = stopped
      throw new Error(`A ${group} rebalance in a short arm: ${JSON.stringify(verdict)}`)
    }
  }

  /**
   * Builds the scenario's state: the topic, the checked cluster shape, the producer, four members
   * and the send loop.
   * @param {AbortSignal} scope the hook's scope: nothing starts or installs once it has aborted
   * @returns {Promise<void>}
   * @throws {Error} the cluster does not match the scenario, a member never joins, or the scope aborts
   */
  const setUp = async (scope) => {
    writeEvidence(EVIDENCE, { step: 'start', topic, partitions: PARTITIONS })
    await scoped(scope, () => bouncer.assertAllUp())
    kafka = new Kafka({
      clientId: `broker-bounce-${runId}`,
      retry: { ...PRODUCTION_RETRY },
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    admin = kafka.admin()
    await scoped(scope, () => admin.connect())
    await scoped(scope, () =>
      admin.createTopics({
        waitForLeaders: true,
        timeout: 30000,
        topics: [
          {
            topic,
            numPartitions: PARTITIONS,
            replicationFactor: 3,
            configEntries: [{ name: 'min.insync.replicas', value: '2' }],
          },
        ],
      })
    )

    const { resources } = await scoped(scope, () =>
      admin.describeConfigs({
        includeSynonyms: false,
        resources: [
          { type: ConfigResourceTypes.TOPIC, name: topic, configNames: ['min.insync.replicas'] },
          {
            type: ConfigResourceTypes.BROKER,
            name: '1',
            configNames: [
              'group.consumer.session.timeout.ms',
              'group.consumer.heartbeat.interval.ms',
            ],
          },
        ],
      })
    )
    const value = (name) =>
      resources.flatMap(({ configEntries }) => configEntries).find((e) => e.configName === name)
        ?.configValue
    const readiness = {
      minInsyncReplicas: value('min.insync.replicas'),
      consumerSessionMs: Number(value('group.consumer.session.timeout.ms')),
      consumerHeartbeatMs: Number(value('group.consumer.heartbeat.interval.ms')),
    }
    const { topics } = await scoped(scope, () => admin.fetchTopicMetadata({ topics: [topic] }))
    readiness.replicas = topics[0].partitions.map(({ replicas }) => replicas.length)
    writeEvidence(EVIDENCE, { step: 'readiness', ...readiness })
    if (
      readiness.minInsyncReplicas !== '2' ||
      readiness.consumerSessionMs !== CONSUMER_SESSION_MS ||
      readiness.consumerHeartbeatMs !== CONSUMER_HEARTBEAT_MS ||
      readiness.replicas.length !== PARTITIONS ||
      readiness.replicas.some((count) => count !== 3)
    ) {
      throw new Error(`The cluster does not match the scenario: ${JSON.stringify(readiness)}`)
    }

    producer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      // At least 60 s of attempts, against a longest planned outage of 45 s.
      retry: { retries: 40, initialRetryTime: 200, maxRetryTime: 2000 },
    })
    await scoped(scope, () => producer.connect())

    const classic = { sessionTimeout: CLASSIC_SESSION_MS, heartbeatInterval: CLASSIC_HEARTBEAT_MS }
    const cooperative = { ...classic, partitionAssigners: [PartitionAssigners.cooperativeSticky] }
    await startMember(scope, 'classic', 'classic-static', {
      ...cooperative,
      groupInstanceId: `static-${runId}`,
    })
    await startMember(scope, 'classic', 'classic-dynamic', cooperative)
    await startMember(scope, 'consumer', 'consumer-1', {})
    await startMember(scope, 'consumer', 'consumer-2', {})
    await pollUntil(
      scope,
      JOIN_BUDGET_MS,
      () =>
        members.every(({ name, group }) =>
          journals[group].entries.some((e) => e.kind === 'GROUP_JOIN' && e.member === name)
        ),
      { label: 'Every member joining its group' }
    )
    await waitForStable(scope)

    const names = (await scoped(scope, () => admin.listTopics())).filter((name) =>
      ['__consumer_offsets', '__transaction_state'].includes(name)
    )
    const internal = await scoped(scope, () => admin.fetchTopicMetadata({ topics: names }))
    const internalConfigs = await scoped(scope, () =>
      admin.describeConfigs({
        includeSynonyms: false,
        resources: names.map((name) => ({
          type: ConfigResourceTypes.TOPIC,
          name,
          configNames: ['min.insync.replicas'],
        })),
      })
    )
    writeEvidence(EVIDENCE, {
      step: 'internal-topics',
      replicas: Object.fromEntries(
        internal.topics.map(({ name, partitions }) => [
          name,
          [...new Set(partitions.map(({ replicas }) => replicas.length))],
        ])
      ),
      minInsyncReplicas: Object.fromEntries(
        internalConfigs.resources.map(({ resourceName, configEntries }) => [
          resourceName,
          configEntries[0]?.configValue,
        ])
      ),
    })

    scope.throwIfAborted()
    timer = setInterval(() => {
      const value = seq++
      sends.push(
        producer.send({ topic, messages: [{ value: JSON.stringify({ seq: value }) }] }).then(
          ([{ partition, baseOffset }]) => {
            acked.push(value)
            Object.values(journals).forEach(({ ledger }) =>
              ledger.produced({ topic, partition, offset: baseOffset })
            )
          },
          (error) => failedSends.push({ seq: value, error: error.name })
        )
      )
    }, SEND_INTERVAL_MS)
  }

  beforeAll(async () => {
    liveRun.assertClean()
    startedAt = Date.now()
    await bounded(NEVER, HOOK_BUDGET_MS - CASE_JOIN_MS, setUp, { label: 'The setup' })
  }, HOOK_BUDGET_MS)

  // Runs whether or not the case was admitted: the setup created these clients either way. A
  // cleanup that fails or runs out of time taints the run and fails the hook.
  afterAll(async () => {
    clearInterval(timer)
    const teardown = new CleanupStack()
    const closing = (name, close) =>
      teardown.add(name, (signal) =>
        bounded(signal, DISCONNECT_BUDGET_MS, close, { label: `The ${name}` })
      )
    teardown.add('broker check', (signal) =>
      pollUntil(
        signal,
        BROKERS_BACK_MS,
        async () => {
          try {
            await bouncer.assertAllUp()
            return true
          } catch {
            return false
          }
        },
        { label: 'Every broker answering again' }
      )
    )
    closing('admin disconnect', () => admin?.disconnect())
    closing('topic deletion', () => admin?.deleteTopics({ topics: [topic] }))
    closing('producer disconnect', () => producer?.disconnect())
    for (const { name, consumer } of members) {
      closing(`${name} disconnect`, () => consumer.disconnect())
    }
    await teardown.run(async () => {}, { budgetMs: HOOK_BUDGET_MS - CASE_JOIN_MS })
  }, HOOK_BUDGET_MS)

  test(
    'every outage arm keeps the log, delivery, rebalance, position and ownership oracle',
    liveCase(SCENARIO_BUDGET_MS, async (scope) => {
      for (const role of ['coordinator', 'leader', 'follower']) {
        await runArm(scope, 'A', role, { kind: 'pause', holdMs: PAUSE_MS })
      }
      for (const role of ['coordinator', 'leader', 'follower']) {
        await runArm(scope, 'B', role, { kind: 'stop' })
      }
      for (const role of ['coordinator', 'leader', 'follower']) {
        await runArm(scope, 'C', role, { kind: 'kill' })
      }
      await runArm(scope, 'D', 'coordinator', { kind: 'stop', holdMs: LONG_HOLD_MS })

      clearInterval(timer)
      expect(await scoped(scope, () => awaitSettled(sends, SETTLE_BUDGET_MS))).toBe(true)
      const stable = await scoped(scope, () =>
        waitForCondition(
          async () => {
            const [high, stableEnd] = await Promise.all([
              endOffsets({ brokers: plainTextBrokers(), topic }),
              endOffsets({
                brokers: plainTextBrokers(),
                topic,
                isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
              }),
            ])
            return [...high].every(([partition, offset]) => stableEnd.get(partition) === offset)
          },
          { attempts: SETTLE_BUDGET_MS / 250, delay: 250 }
        )
      )
      expect(stable).toBe(true)

      const log = await scoped(scope, () => readLog({ brokers: plainTextBrokers(), topic }))
      const logged = [...log.values()].flatMap(({ records }) =>
        records.map(({ value }) => JSON.parse(value).seq)
      )
      const logResult = logVerdict(acked, logged)

      await scoped(scope, () =>
        waitForCondition(
          () =>
            Object.values(journals).every(({ ledger }) => ledger.summary().missing.length === 0),
          DELIVERY_BUDGET
        )
      )
      const committedOf = async (groupId) => {
        const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [topic] })
        return new Map(partitions.map(({ partition, offset }) => [partition, offset]))
      }
      const verdict = Object.fromEntries(
        Object.entries(journals).map(([group, journal]) => [
          group,
          {
            delivery: deliveryVerdict(journal, outages),
            rebalances: rebalanceVerdict(journal, outages),
            ownership: ownershipVerdict(journal, topic),
          },
        ])
      )
      // The interval cadence has no background timer: the tail after the last commit stays
      // uncommitted while idle and is committed by the graceful close (docs/Consuming.md).
      await Promise.all(
        members.map(({ name, consumer }) =>
          bounded(scope, DISCONNECT_BUDGET_MS, () => consumer.disconnect(), {
            label: `The ${name} graceful close`,
          })
        )
      )
      const positions = {}
      await scoped(scope, () =>
        waitForCondition(async () => {
          for (const [group, groupId] of [
            ['classic', classicGroup],
            ['consumer', consumerGroup],
          ]) {
            positions[group] = positionVerdict(journals[group], topic, await committedOf(groupId))
          }
          return Object.values(positions).every(({ behind }) => behind.length === 0)
        }, DELIVERY_BUDGET)
      )

      for (const group of Object.keys(journals)) verdict[group].position = positions[group]
      writeEvidence(EVIDENCE, {
        step: 'verdict',
        acked: acked.length,
        failedSends: failedSends.length,
        logged: logged.length,
        log: logResult,
        verdict,
        elapsedMs: Date.now() - startedAt,
      })

      expect(logResult).toEqual({ missing: [], twice: [] })
      expect(assertedRebalanceViolations(verdict)).toEqual([])
      for (const group of Object.keys(journals)) {
        expect(verdict[group].delivery).toEqual({ missing: [], unexplained: [] })
        expect(verdict[group].position).toEqual({ behind: [], ahead: [] })
        expect(verdict[group].ownership).toEqual({ overlaps: [], staleCommits: [] })
      }
      expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
    }),
    caseTimeout(SCENARIO_BUDGET_MS)
  )

  test('every evidence line of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines.map(({ step }) => step)).toEqual(
      expect.arrayContaining(['start', 'readiness', 'internal-topics', 'outage', 'verdict'])
    )
    expect(lines.filter(({ step }) => step === 'outage')).toHaveLength(10)
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})
