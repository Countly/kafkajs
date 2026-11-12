#!/usr/bin/env node
'use strict'

/**
 * Soak driver: runs a client for a stretch of time against a cluster, reads what it holds on to
 * every 30 s, and checks the readings against the bounds of `bounds.js`.
 *
 * Usage: node --expose-gc scripts/soak/soak.js --profile
 *          <idle|member-churn|regex-topic-churn|outage|first-run|producer-churn|consumer-churn|
 *          share-consumer|share-consumer-short|telemetry> [--phase-seconds <n>]
 *   (= npm run test:kafka -- "node --expose-gc scripts/soak/soak.js --profile first-run" for the
 *   15-minute run, `npm run soak:first-run`)
 * Writes one JSON line per sample and per quiet point to `.tmp/e2e/soak.jsonl` through
 * `writeEvidence` (`.tmp/e2e/producer-churn.jsonl` for the producer-churn profile), prints the
 * verdict, and sets the exit code: 0 when every bound held, 1 when one broke or the run failed. It
 * ends by disconnecting what it built and does not call `process.exit`, so a run that leaves a
 * handle behind does not end, which is also a finding.
 *
 * Profiles, over finite populations: one topic of 6 partitions and two member slots. A profile of
 * one phase runs 7 minutes, which leaves six readings after the warm-up.
 *   idle              one member polling an idle topic, a message a second
 *   member-churn      two members, one of them replaced by a new consumer every 15 s, rebalancing
 *                     cooperatively (`cooperativeSticky`)
 *   regex-topic-churn a regex subscriber while 50 distinct topic names are created and deleted
 *   outage            one member while a broker is stopped for a third of the phase and restarted
 *   first-run         5 minutes of idle, of member churn and of outage, the 15-minute screen
 *   producer-churn    5 minutes each of steady production, rolling broker restarts and buffer
 *                     pressure from one idempotent producer; see `ProducerChurn`
 *   consumer-churn    a 3-minute warm-up of one short pass of each of the four shapes below, then
 *                     5 minutes each of member churn, regex-topic
 *                     churn, pause and topic recreation, and a mostly idle 1000-partition
 *                     assignment, judged by an independent log, broker-offset and handler check;
 *                     see `ConsumerChurnRun`
 *   share-consumer    one fixed 960 s first-use pass, then three scored phases of 640 s (steady, reset, members), ten cycles of 64 s each: a share
 *                     group member and a transient second one join and leave every cycle, sixty
 *                     fixed records are delivered and acknowledged, and the reset phase forces one
 *                     session reset per cycle; judged by independent log, broker-position and
 *                     handler checks and a per-phase resource screen; see `ShareSoak`
 *   share-consumer-short  the same first-use pass, then three scored phases of 320 s with cycles
 *                     of 32 s; takes no --phase-seconds. Inclusive child bounds are 2040/3000 s;
 *                     cold guard caps are 2460/3420 s for short/full, respectively.
 *   telemetry         one phase of 360 s (60 s warm-up, 300 s measurement) on the configured telemetry
 *                     fixture: four one-client worker processes, one per client kind, each held to
 *                     its own memory and exact resource identities through subscription changes,
 *                     telemetry reconnects and public close/reopen cycles, with an independent
 *                     record oracle and natural-exit checks; takes no --phase-seconds; see
 *                     `TelemetryChurn`
 * Every phase ends by disconnecting its clients, also when it fails; the readings that must be zero
 * are taken there.
 * `--phase-seconds n` shortens every phase to n seconds: a smoke run of the driver, not of memory.
 *
 * Development-only. The first run is a short-run screen, never proof of bounded long-run memory.
 * The gauges read internals: the clusters a `Kafka` builds, their request queues, and the
 * operation ledgers and reset obligations of its `operations` store.
 */
/**
 * The memory of a telemetry worker before any application module is loaded, read only in a worker
 * process: current sizes and the kernel's lifetime peak of this executable image.
 */
const STARTUP = (() => {
  if (process.env.TELEMETRY_WORKER_KIND === undefined) return null
  const usage = process.memoryUsage()
  const hwm = require('../../testHelpers/processMemory').readProcStatus()?.vmhwm ?? null
  return {
    phase: 'startup',
    at: performance.now(),
    rss: usage.rss,
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
    retained: usage.heapUsed + usage.external,
    hwm,
    rawMaxRss: process.resourceUsage().maxRSS * 1024,
  }
})()
const { subscribeNamedTopic } = require('./subscribeNamedTopic')
const fs = require('node:fs')
const { AsyncLocalStorage } = require('node:async_hooks')
const path = require('node:path')
const { Kafka, KafkaJSBufferExhaustedError, logLevel, PartitionAssigners } = require('../../index')
const Broker = require('../../src/broker')
const ConnectionPool = require('../../src/network/connectionPool')
const defaultSocketFactory = require('../../src/network/socketFactory')
const ISOLATION_LEVEL = require('../../src/protocol/isolationLevel')
const { createLogger } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')
const { Clients } = require('../clients')
const { createChaosSocketFactory } = require('../../testHelpers/chaosSocket')
const { assertProbe } = require('../../testHelpers/producerProbe')
const { endOffsets, readLog } = require('../../testHelpers/rawLog')
const { createResourceProbe } = require('../../testHelpers/resourceProbe')
const { ShareTimerObserver } = require('./shareTimerObserver')
const { evidenceLines, evidencePath, writeEvidence } = require('../../testHelpers/e2eEvidence')
const { NEVER, bounded, delay } = require('../../src/producer/__tests__/fixtures/bounded')
const {
  ENDPOINT_BYTES,
  MIN_PHASE_SAMPLES,
  PHASE_WARMUP_MS,
  SAMPLE_MS,
  SLOPE_BYTES_PER_MINUTE,
  WARMUP_MS,
  evaluate,
  evaluatePhases,
  median,
  slope,
} = require('./bounds')
const {
  LATENCY_SAMPLE_EVERY,
  STATUS,
  ObservationJournal,
  ObservedLog,
  OfferLedger,
  decodeId,
  encodeValue,
  mergeDurability,
  sequenceOf,
} = require('./churnLedger')
const { ClosingProbe } = require('./closingProbe')
const { ConsumerChurnOracle } = require('./consumerChurn')
const LongWindow = require('./longWindow')

const PARTITIONS = 6
/** The seed brokers of the CLI, and of the configured telemetry fixture, which publishes others. */
const DEFAULT_BROKERS = Object.freeze(['localhost:9092', 'localhost:9095', 'localhost:9098'])
const TELEMETRY_BROKERS = Object.freeze(['localhost:19092', 'localhost:19095', 'localhost:19098'])
const MEMBER_CHURN_MS = 15000
const OUTAGE_SERVICE = 'kafka3'
const TOPIC_NAMES = 50
/**
 * How long a quiet point waits for the sockets the clients closed to go. An obsolete connection
 * drains in the background for up to the client's `requestTimeout` (30 s by default) after a
 * disconnect, so the wait outlasts that before a socket counts as left behind.
 */
const SETTLE_MS = 31000
const PHASE_MS = 300000
/** A profile of one phase: the warm-up, then six readings and a margin for their collection. */
const STANDALONE_MS = 420000

/**
 * The producer-churn workload. It offers `rate` public sends a second, never more than
 * `maxUnsettled` unsettled at once, through one idempotent producer whose buffer is small enough
 * for the pressure phase to fill and whose largest request admits the closing probe's record.
 */
const CHURN = Object.freeze({
  rate: 2000,
  maxUnsettled: 8192,
  steadyBytes: 200,
  pressureBytes: 1024,
  pressureDelayMs: 5000,
  partitions: PARTITIONS,
  minInsyncReplicas: 2,
  /** The least the offered rate may fall to in the steady phase, as a fraction of `rate`. */
  targetFraction: 0.95,
  /** Offered ids kept beyond the offered sends, for the sends of recovery and the capture. */
  idSlack: 4096,
  recoveryMs: 60000,
  recoveryRetryMs: 500,
  joinMs: 150000,
  flushMs: 150000,
  pollMs: 20,
  minLatencySamples: 20000,
  /** The fewest heap samples each phase keeps after its warm-up. */
  minHeapSamples: MIN_PHASE_SAMPLES,
  /** One offered id in this many is timed from send to first sighting. */
  latencySampleEvery: LATENCY_SAMPLE_EVERY,
  producer: Object.freeze({
    idempotent: true,
    bufferMemory: 4194304,
    maxBlockMs: 2000,
    maxRequestSize: 8388608,
    deliveryTimeout: 120000,
  }),
})
/** The nodes of the outage phase, in rolling order. */
const CHURN_SERVICES = Object.freeze(['kafka1', 'kafka2', 'kafka3'])
/** The kind of outage each `BrokerBouncer` step is. */
const OUTAGE_KINDS = Object.freeze({ stop: 'clean', kill: 'hard' })

/**
 * One immutable first-use pass, always short-shaped, before the separate scored workload.
 * Its final boundary reserve follows successful joined cleanup; no allocation is borrowed.
 */
const SHARE_SOAK_PRIMING = Object.freeze({
  passes: 1,
  cycles: 30,
  cyclesPerPhase: 10,
  resets: 10,
  slotMs: 32000,
  phaseMs: 320000,
  profileMs: 960000,
  setupMs: 15000,
  transitionMs: 5000,
  cleanupMs: 20000,
  idleWakeMarginMs: 10000,
  boundaryMs: 10000,
  totalMs: 1020000,
})

/**
 * The scored thirty-cycle workload and inclusive child allocations. First-use work has a separate
 * allocation and contributes nothing to scored cycles, identities, baselines or heap fits.
 */
const SHARE_SOAK = Object.freeze({
  shortMs: 960000,
  fullMs: 1920000,
  phases: Object.freeze(['steady', 'reset', 'members']),
  cycles: 30,
  cyclesPerPhase: 10,
  resets: 10,
  partitions: 3,
  recordsPerCycle: 60,
  maxRecords: 100,
  nodes: 3,
  lockMs: 30000,
  warmupMs: 10000,
  settleMs: 1000,
  slotMs: Object.freeze({ short: 32000, full: 64000 }),
  sampleMs: Object.freeze({ short: 4000, full: 20000 }),
  quietMs: Object.freeze({ cycle: 5000, final: 5000 }),
  budgets: Object.freeze({
    short: Object.freeze({
      setupMs: 15000,
      profileMs: 960000,
      transitionMs: 5000,
      cleanupMs: 20000,
      exitMs: 10000,
      primingMs: SHARE_SOAK_PRIMING.totalMs,
      totalMs: 2040000,
      watchdogMs: 2040000,
      lifecycleMs: 2460000,
    }),
    full: Object.freeze({
      setupMs: 15000,
      profileMs: 1920000,
      transitionMs: 5000,
      cleanupMs: 20000,
      exitMs: 10000,
      primingMs: SHARE_SOAK_PRIMING.totalMs,
      totalMs: 3000000,
      watchdogMs: 3000000,
      lifecycleMs: 3420000,
    }),
  }),
  endpointBytes: ENDPOINT_BYTES,
  slopeBytesPerMinute: SLOPE_BYTES_PER_MINUTE,
  quietSlopeBytesPerMinute: 96 * 1024,
})
/** The group configs the share soak sets, and removes again at cleanup. */
const GROUP_CONFIG_NAMES = ['share.auto.offset.reset', 'share.record.lock.duration.ms']

/** The phases of each profile: how long each runs and which workload it is. */
const PROFILES = {
  'share-consumer': SHARE_SOAK.phases.map((name) => ({
    name: `share-${name}`,
    workload: 'shareConsumer',
    ms: SHARE_SOAK.fullMs / SHARE_SOAK.phases.length,
  })),
  'share-consumer-short': SHARE_SOAK.phases.map((name) => ({
    name: `share-${name}`,
    workload: 'shareConsumer',
    ms: SHARE_SOAK.shortMs / SHARE_SOAK.phases.length,
  })),
  idle: [{ name: 'idle', workload: 'idle', ms: STANDALONE_MS }],
  'member-churn': [{ name: 'member-churn', workload: 'memberChurn', ms: STANDALONE_MS }],
  'regex-topic-churn': [
    { name: 'regex-topic-churn', workload: 'regexTopicChurn', ms: STANDALONE_MS },
  ],
  outage: [{ name: 'outage', workload: 'outage', ms: STANDALONE_MS }],
  'producer-churn': [
    { name: 'steady', workload: 'producerSteady', ms: PHASE_MS },
    { name: 'outage', workload: 'producerOutage', ms: PHASE_MS },
    { name: 'buffer-pressure', workload: 'producerPressure', ms: PHASE_MS },
  ],
  'consumer-churn': [
    { name: 'member-churn', workload: 'consumerMemberChurn', ms: PHASE_MS },
    { name: 'regex-topics', workload: 'consumerRegexTopics', ms: PHASE_MS },
    { name: 'pause-recreate', workload: 'consumerPauseRecreate', ms: PHASE_MS },
    { name: 'mostly-idle', workload: 'consumerMostlyIdle', ms: PHASE_MS },
  ],
  'first-run': [
    { name: 'idle', workload: 'idle', ms: PHASE_MS },
    { name: 'member-churn', workload: 'memberChurn', ms: PHASE_MS },
    { name: 'outage', workload: 'outage', ms: PHASE_MS },
  ],
  telemetry: [{ name: 'telemetry', workload: 'telemetry', ms: 360000 }],
}

/** Fixed telemetry resource limits; consumer and producer profiles keep their own bounds. */
const TELEMETRY = Object.freeze({
  warmupMs: 60000,
  measureMs: 300000,
  cleanupMs: 120000,
  caseMs: 540000,
  exitMs: 10000,
  sampleMs: 30000,
  /** Phase-anchored admission times of the post-collection readings: 60 s to 330 s every 30 s, then 359 s. */
  sampleAt: Object.freeze([...Array.from({ length: 10 }, (_, i) => 60000 + i * 30000), 359000]),
  minSamples: 10,
  minSpanMs: 270000,
  slopeBytesPerMinute: 64 * 1024,
  endpointBytes: 1024 * 1024,
  activeBytes: 8 * 1024 * 1024,
  closedBytes: 256 * 1024,
  rssDeltaBytes: 64 * 1024 * 1024,
  externalDeltaBytes: 64 * 1024 * 1024,
  setupPeakBytes: 96 * 1024 * 1024,
  peakBytes: 160 * 1024 * 1024,
  controlRssDriftBytes: 2 * 1024 * 1024,
  controlRetainedDriftBytes: 262144,
  noiseLoadPerCpu: 0.75,
  noiseFreeBytes: 268435456,
  noisePressure: 5,
})

/** The long-window shadow candidate's limits, derived from `TELEMETRY` without changing it. */
const TELEMETRY_LONG = LongWindow.limitsOf(TELEMETRY)

/** The limits and profiles this process runs the telemetry churn with: the candidate's only when selected. */
const ACTIVE_TELEMETRY = LongWindow.selected() ? TELEMETRY_LONG : TELEMETRY
const ACTIVE_PROFILES = LongWindow.selected()
  ? {
      ...PROFILES,
      telemetry: [
        { ...PROFILES.telemetry[0], ms: TELEMETRY_LONG.warmupMs + TELEMETRY_LONG.measureMs },
      ],
    }
  : PROFILES

/** The quiet-identity roles every telemetry reading must carry, whatever they hold. */
const TELEMETRY_ROLES = Object.freeze([
  'resources:fixture',
  'resources:target',
  'resources:unknown',
  'sockets',
])

/**
 * Judges telemetry process memory and exact quiet identities without changing inherited bounds.
 * Each client keeps its own allowance. clientCount scales the retained allowance; the live profile
 * passes 1. Resident-size gates are measured against the worker's own telemetry-disabled control
 * and its recorded peak source, never against an absolute ceiling set by the interpreter.
 */
class TelemetryResourceVerdict {
  /**
   * @param {object} run observed, serializable evidence, never expected input counts
   * @param {boolean} [run.diagnostic] true excludes the input from acceptance
   * @param {boolean} [run.acceptanceEligible] false excludes the input from acceptance
   * @param {number} run.clientCount clients measured in this process, 1 on the live profile
   * @param {object} run.baseline disconnected descriptor/UUID baseline after warm-up
   * @param {Array<{admittedAtMs:number, completedAtMs:number, snapshot:object}>} run.samples
   *   post-GC process readings with the monotonic times their admission and completion had
   * @param {object} run.teardown disconnected reading at the same retained-state sizes
   * @param {number} run.cleanupMs cleanup elapsed time within the original case deadline
   * @param {number} run.measurementMs how long the measurement window lasted
   * @param {object} run.reference the replayable resident-size reference: peak source and
   *   capability, startup, loaded and control readings, host contexts and the final reading
   * @param {{skipped:number[], readFailures:object[]}} [run.sampler] the sampler's own facts: fixed
   *   grid times skipped and readings that failed after they were admitted
   * @returns {{ok:boolean, outcome:'passed'|'failed'|'inconclusive', violations:string[],
   *   inconclusive:string[], measured:object}} the verdict: failures take precedence over an
   *   inconclusive measurement, and only a passed outcome is `ok`
   * @throws {TypeError} for a missing evidence container; callers fail the run
   */
  static evaluate({
    diagnostic,
    acceptanceEligible,
    clientCount,
    baseline,
    samples,
    teardown,
    cleanupMs,
    measurementMs,
    reference,
    sampler,
  }) {
    const violations = []
    const measured = {
      samples: samples.length,
      skippedSamples: sampler?.skipped?.length ?? 0,
      slopeBytesPerMinute: null,
      endpointBytes: null,
    }
    const fail = (condition, label) => {
      if (!condition) violations.push(label)
    }
    const bytes = (snapshot) => snapshot?.memory?.retainedBytes
    const finiteBytes = (value) => Number.isSafeInteger(value) && value >= 0
    const baselineBytes = bytes(baseline)
    const closedBytes = bytes(teardown)
    fail(
      diagnostic !== true && acceptanceEligible !== false,
      'diagnostic inputs are not acceptance-eligible'
    )
    fail(Number.isInteger(clientCount) && clientCount >= 1 && clientCount <= 4, 'client count')
    fail(finiteBytes(baselineBytes) && finiteBytes(closedBytes), 'settled baseline and teardown')
    fail(
      Number.isFinite(cleanupMs) && cleanupMs >= 0 && cleanupMs <= TELEMETRY.cleanupMs,
      'cleanup budget'
    )
    fail(samples.length >= TELEMETRY.minSamples, 'post-GC sample count')
    fail(!sampler?.readFailures?.length, 'sampler read failure')
    fail(
      Number.isFinite(measurementMs) && measurementMs >= TELEMETRY.measureMs,
      'measurement duration'
    )
    fail(
      typeof baseline?.registryDigest === 'string' &&
        baseline.registryDigest.length > 0 &&
        baseline.registryDigest === teardown?.registryDigest,
      'retained descriptor and UUID baseline'
    )

    const beforeRoles = Object.keys(baseline?.identities ?? {}).sort()
    const afterRoles = Object.keys(teardown?.identities ?? {}).sort()
    fail(
      TELEMETRY_ROLES.every((role) => beforeRoles.includes(role) && afterRoles.includes(role)) &&
        JSON.stringify(beforeRoles) === JSON.stringify(afterRoles),
      'identity roles'
    )
    for (const role of beforeRoles) {
      const before = baseline.identities[role]
      const after = teardown.identities?.[role]
      fail(
        Array.isArray(before) &&
          Array.isArray(after) &&
          before.every(Number.isSafeInteger) &&
          after.every(Number.isSafeInteger) &&
          JSON.stringify(before) === JSON.stringify(after),
        `quiet identity ${role}`
      )
    }

    const windowEnd = TELEMETRY.warmupMs + TELEMETRY.measureMs
    let validSeries = samples.length >= TELEMETRY.minSamples
    let previous = -Infinity
    const points = []
    for (const { admittedAtMs, completedAtMs, snapshot } of samples) {
      const value = bytes(snapshot)
      const valid =
        Number.isFinite(admittedAtMs) &&
        Number.isFinite(completedAtMs) &&
        admittedAtMs >= TELEMETRY.warmupMs &&
        completedAtMs >= admittedAtMs &&
        completedAtMs <= windowEnd &&
        admittedAtMs > previous &&
        finiteBytes(value) &&
        snapshot.registryDigest === baseline.registryDigest
      fail(valid, 'post-GC sample shape or retained state')
      if (!valid) validSeries = false
      fail(value - baselineBytes <= TELEMETRY.activeBytes * clientCount, 'active retained ceiling')
      previous = admittedAtMs
      points.push([completedAtMs / 60000, value])
    }
    if (validSeries) {
      fail(
        samples[0].admittedAtMs <= TELEMETRY.warmupMs + TELEMETRY.sampleMs &&
          samples.at(-1).admittedAtMs >= windowEnd - 2 * TELEMETRY.sampleMs &&
          samples.at(-1).admittedAtMs - samples[0].admittedAtMs >= TELEMETRY.minSpanMs,
        'measurement span'
      )
      measured.slopeBytesPerMinute = slope(points)
      measured.endpointBytes = points.at(-1)[1] - points[0][1]
      fail(
        measured.slopeBytesPerMinute <= TELEMETRY.slopeBytesPerMinute * clientCount,
        'retained slope'
      )
      fail(measured.endpointBytes <= TELEMETRY.endpointBytes * clientCount, 'retained endpoint')
    }
    fail(
      closedBytes - baselineBytes <= TELEMETRY.closedBytes * clientCount,
      'closed transient ceiling'
    )
    const inconclusive = TelemetryResourceVerdict.reference(reference, {
      baseline,
      samples,
      teardown,
      violations,
      measured,
    })
    const outcome = violations.length ? 'failed' : inconclusive.length ? 'inconclusive' : 'passed'
    return { ok: outcome === 'passed', outcome, violations, inconclusive, measured }
  }

  /**
   * The setup-peak allowance of one client kind: `TELEMETRY.setupPeakBytes`, plus for the share
   * consumer the exact bytes of its declared, prefilled journals under the limits this process runs
   * with. The telemetry increase and
   * absolute peak limits are the same for every kind and are never raised.
   * @param {string} [kind] the client kind of the control reference
   * @returns {number} allowed setup peak in bytes
   */
  static setupAllowance(kind) {
    const { declaredJournalBytes } = require('./telemetryChurn')
    const declared = kind === 'shareConsumer' ? declaredJournalBytes(kind, ACTIVE_TELEMETRY) : 0
    return TELEMETRY.setupPeakBytes + declared
  }

  /**
   * Validates one memory reading of the versioned catalogue: its phase, monotonic time, every
   * counter as a nonnegative safe integer, `retained = heapUsed + external` with `arrayBuffers`
   * inside `external` (never added again), and for a settled reading real collections admitted
   * before it completed. A missing peak (`hwm` null) is a capability fact, not a malformed reading.
   * @param {object} reading the reading @param {string} phase the catalogue name it must carry
   * @param {boolean} settled whether it must carry its collection facts
   * @returns {string|null} what is wrong with it, or null
   */
  static malformed(reading, phase, settled) {
    const counter = (value) => Number.isSafeInteger(value) && value >= 0
    if (reading?.phase !== phase) return 'resident-size reading phase'
    if (!Number.isFinite(reading.at)) return 'resident-size reading time'
    const values = ['rss', 'heapUsed', 'external', 'arrayBuffers', 'rawMaxRss']
    if (!values.every((key) => counter(reading[key]))) return 'resident-size counters'
    if (reading.hwm !== null && !counter(reading.hwm)) return 'resident-size counters'
    const retained = reading.retainedBytes ?? reading.retained
    if (
      retained !== reading.heapUsed + reading.external ||
      (reading.retainedBytes !== undefined &&
        reading.retained !== undefined &&
        reading.retainedBytes !== reading.retained) ||
      reading.arrayBuffers > reading.external
    )
      return 'resident-size arithmetic'
    if (settled) {
      const gc = reading.gc
      if (
        !Number.isFinite(gc?.admittedAt) ||
        gc.completedAt !== reading.at ||
        gc.admittedAt > gc.completedAt ||
        !Number.isSafeInteger(gc.observed) ||
        gc.observed < 2
      )
        return 'resident-size collection facts'
    }
    return null
  }

  /**
   * Judges the resident-size gates against the worker's own disabled control, over every reading
   * of the versioned catalogue and the native work journal, which must hold at least one resolved
   * production compression. Failures are pushed into `violations`; what only makes the measurement
   * uninformative is returned.
   * @param {object} reference the replayable reference facts
   * @param {object} run `baseline`, `samples`, `teardown`, `violations` to append to and `measured`
   * @returns {string[]} the reasons the measurement is inconclusive
   */
  static reference(reference, { baseline, samples, teardown, violations, measured }) {
    const inconclusive = []
    const fail = (condition, label) => {
      if (!condition) violations.push(label)
    }
    if (reference?.version !== 'rss-reference-2') {
      violations.push('resident-size reference facts')
      return inconclusive
    }
    fail(
      reference.source === 'proc-status-vmhwm' && reference.units === 'bytes',
      'resident-size peak source'
    )
    if (reference.capability?.gc !== true || reference.capability?.peak !== true)
      inconclusive.push('collection or peak capability')
    const control = reference.control
    const catalogue = [
      [reference.startup, 'startup', false],
      [reference.loaded, 'loaded', false],
      [control?.c0, 'c0', true],
      [control?.c1, 'c1', true],
      [control?.released, 'control-released', true],
      [baseline?.memory, 'settled', true],
      ...samples.map((s) => [s.snapshot?.memory, 'settled', true]),
      [teardown?.memory, 'settled', true],
      [reference.final, 'final', false],
    ]
    const problems = catalogue
      .map(([reading, phase, settled]) =>
        TelemetryResourceVerdict.malformed(reading, phase, settled && reference.capability?.gc)
      )
      .filter(Boolean)
    for (const problem of new Set(problems)) violations.push(problem)
    if (problems.length) return inconclusive
    const readings = catalogue.map(([reading]) => reading)
    fail(
      readings.every((r, i) => i === 0 || r.at >= readings[i - 1].at),
      'resident-size reading order'
    )
    const native = reference.native
    const rows = native?.journal?.rows
    const nativeReadings = Array.isArray(rows)
      ? rows.flatMap((row) => [row.submitted, row.settled])
      : []
    fail(
      Array.isArray(rows) &&
        native.journal.overflow === false &&
        native.journal.live === 0 &&
        rows.some((row) => row.outcome === 'resolved') &&
        rows.every((row) => ['resolved', 'rejected'].includes(row.outcome)) &&
        nativeReadings.every(
          (r) => Number.isSafeInteger(r?.rss) && r.rss >= 0 && Number.isSafeInteger(r.external)
        ) &&
        Number.isSafeInteger(native.sampler?.count) &&
        native.sampler.count >= 2 * rows.length &&
        Number.isSafeInteger(native.sampler.peak?.rss) &&
        Number.isSafeInteger(native.sampler.peak?.external),
      'native work journal'
    )
    if (readings.some((r) => r.hwm === null)) {
      inconclusive.push('resident-size peak capability')
    } else {
      let high = -Infinity
      let covered = -Infinity
      for (const reading of readings) {
        covered = Math.max(covered, reading.rss)
        fail(reading.hwm >= high && reading.hwm >= covered, 'resident-size peak series')
        high = Math.max(high, reading.hwm)
      }
    }
    fail(
      control.telemetryFrames === 0 &&
        control.openSockets === 0 &&
        control.leftoverHandles === 0 &&
        Array.isArray(control.manifest?.operations) &&
        Array.isArray(control.outcomes) &&
        control.outcomes.length === control.manifest.operations.length &&
        control.outcomes.every((o, n) => o.n === n && o.status === 'fulfilled'),
      'telemetry-disabled control'
    )
    fail(
      Array.isArray(reference.foreground) &&
        reference.foreground.length === control.manifest?.operations?.length &&
        reference.foreground.every((o, n) => o.n === n && o.status === 'fulfilled'),
      'enabled replay of the control catalogue'
    )
    const rssDrift = Math.abs(control.c1.rss - control.c0.rss)
    const retainedDrift = Math.abs(control.c1.retained - control.c0.retained)
    if (
      rssDrift > TELEMETRY.controlRssDriftBytes ||
      retainedDrift > TELEMETRY.controlRetainedDriftBytes
    )
      inconclusive.push('unstable control')
    const peakOf = (list) => Math.max(...list.flatMap((r) => [r.hwm ?? 0, r.rss]))
    const setupPeak = peakOf(readings.slice(0, 5))
    const share = control.kind === 'shareConsumer'
    const allowance = TelemetryResourceVerdict.setupAllowance(control.kind)
    const recorded = reference.journals?.setupAllowanceBytes
    fail(recorded === allowance || (recorded === undefined && !share), 'recorded setup allowance')
    fail(
      !share || reference.journals?.allocation?.byteLength === allowance - TELEMETRY.setupPeakBytes,
      'declared journal bytes'
    )
    if (setupPeak > allowance) inconclusive.push('setup peak above its allowance')
    const base = Math.min(control.c0.rss, control.c1.rss)
    const externalReference = Math.min(control.c0.external, control.c1.external)
    const sampled = native?.sampler?.peak ?? { rss: 0, external: 0 }
    const peak = Math.max(
      peakOf(readings),
      ...nativeReadings.map((r) => r?.rss ?? 0),
      sampled.rss ?? 0
    )
    const externalPeak = Math.max(
      ...readings.map((r) => r.external),
      ...nativeReadings.map((r) => r?.external ?? 0),
      sampled.external ?? 0
    )
    Object.assign(measured, {
      controlBaseRss: base,
      controlExternal: externalReference,
      setupPeak,
      setupAllowanceBytes: allowance,
      peak,
      rssDelta: peak - base,
      externalPeak,
      externalDelta: externalPeak - externalReference,
      nativeJobs: Array.isArray(rows) ? rows.length : null,
    })
    fail(peak - base <= TELEMETRY.rssDeltaBytes, 'telemetry resident-size increase')
    fail(peak <= TELEMETRY.peakBytes, 'absolute resident-size peak')
    fail(
      externalPeak - externalReference <= TELEMETRY.externalDeltaBytes,
      'external-memory increase'
    )
    for (const [name, host] of Object.entries(reference.host ?? {})) {
      const finite = (value) => Number.isFinite(value) && value >= 0
      const known = finite(host?.cpus) && finite(host?.load1) && finite(host?.freeBytes)
      fail(known, `host context ${name}`)
      if (
        known &&
        (host.load1 / host.cpus > TELEMETRY.noiseLoadPerCpu ||
          host.freeBytes < TELEMETRY.noiseFreeBytes ||
          (host.pressure?.available && host.pressure.someAvg10 > TELEMETRY.noisePressure))
      )
        inconclusive.push(`host noise ${name}`)
    }
    fail(
      ['before', 'boundary', 'after'].every((name) => reference.host?.[name]),
      'host contexts'
    )
    return inconclusive
  }
}

const USAGE = `usage: node --expose-gc scripts/soak/soak.js --profile <${Object.keys(PROFILES).join('|')}> [--phase-seconds <n>]`

/**
 * @param {string[]} argv
 * @returns {{ profile: string, phaseSeconds: number|null }} `phaseSeconds` shortens every phase to
 *   that long, for a smoke run that checks the driver and proves nothing about memory
 * @throws {Error} the usage line, for a missing or unknown profile or any other argument, and for
 *   `--phase-seconds` with a share or telemetry profile, whose phases and cycles are fixed
 */
const parseArgs = (argv) => {
  const [flag, profile, ...rest] = argv
  const shortened = rest[0] === '--phase-seconds'
  const phaseSeconds = shortened ? Number(rest[1]) : null
  const valid =
    flag === '--profile' &&
    Boolean(PROFILES[profile]) &&
    rest.length === (shortened ? 2 : 0) &&
    (!shortened || (Number.isInteger(phaseSeconds) && phaseSeconds >= 1)) &&
    !(shortened && (profile.startsWith('share-consumer') || profile === 'telemetry'))

  if (!valid) {
    throw new Error(USAGE)
  }

  return { profile, phaseSeconds }
}

/**
 * @param {number} ms
 * @param {AbortSignal} [signal] ends the wait early; the timer is cleared either way
 * @returns {Promise<void>}
 */
const sleep = (ms, signal) =>
  new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    if (signal?.aborted) done()
    else signal?.addEventListener('abort', done, { once: true })
  })

/**
 * @param {Array<import('events').EventEmitter>} emitters
 * @returns {{ eventNames: () => string[], listenerCount: (name: string) => number }} the listeners
 *   of all of them as one emitter's, so the probe counts them together
 */
const together = (emitters) => ({
  eventNames: () => [...new Set(emitters.flatMap((emitter) => emitter.eventNames()))],
  listenerCount: (name) =>
    emitters.reduce((total, emitter) => total + emitter.listenerCount(name), 0),
})

/**
 * @param {object} kafka a `Kafka`
 * @param {string} name `createCluster` or `operations`
 * @returns {{ symbol: symbol, value: * }} the key and the value of the private member of `kafka`
 *   with that name
 * @throws {Error} when this version of the client has no such member
 */
const privateOf = (kafka, name) => {
  const symbol = Object.getOwnPropertySymbols(kafka).find(
    ({ description }) => description === `private:Kafka:${name}`
  )
  if (!symbol) {
    throw new Error(
      `The soak driver reads Kafka's private ${name}, which this client does not have`
    )
  }

  return { symbol, value: kafka[symbol] }
}

/**
 * Builds a client whose sockets and clusters are counted, and the probe that reads them. The driver
 * holds no socket the client destroyed and no cluster the client let go (a cluster is held through
 * a `WeakRef`), so what it measures is not inflated by what it keeps.
 * @param {object} options `Kafka` options; a `socketFactory` among them is the one counted
 * @param {object} [readings]
 * @param {boolean} [readings.activeResources=true] count the event loop's resources by type; a run
 *   that shares its process with others, as a spec does, leaves it off
 * @param {boolean} [readings.heap] collect before reading heap; defaults to whether GC is exposed
 * @param {boolean} [readings.socketIdentities=false] attach creation IDs and endpoint inventory
 * @param {() => object[]} [readings.originalQueues] Live original queues, including detached owners;
 *   also enables per-emitter socket/queue names without changing ordinary listeners.
 * @param {Function} [readings.shareState] the actual share session, record and range counts; a
 *   reading is absent without it
 * @param {boolean} [readings.publicOnly=false] read no private member of the client: no request
 *   queue, ledger or obligation reading is taken, and those fields are omitted, never zero
 * @param {() => Record<string, Iterable<object|Function>>} [readings.identities] owned process
 *   resources by role, added to the open sockets and their listeners by event
 * @param {boolean} [readings.memory=false] settled heap plus external and RSS after two collections
 * @returns {{ kafka: object, probe: { snapshot: () => Promise<object> },
 *   socketFactory: Function, pruneSockets: () => void,
 *   listeners: () => Record<string, Record<string, number>> }}
 *   `socketFactory` is the counted factory the client was given: a connection made through it,
 *   such as a raw reader's, is measured like the client's own. `listeners` names the listeners of
 *   each tracked emitter by event, so a swapped listener shows where a total would not.
 */
const instrument = (
  { socketFactory = defaultSocketFactory(), ...options },
  {
    activeResources = true,
    socketIdentities = false,
    shareState,
    originalQueues,
    publicOnly = false,
    identities: ownedIdentities,
    memory = false,
    heap = typeof globalThis.gc === 'function',
  } = {}
) => {
  const open = new Set()
  const originalSockets = []
  const emitterIds = new WeakMap()
  let nextEmitterId = 0
  const identities = socketIdentities ? new WeakMap() : null
  let created = 0
  const clusters = new Set()
  const counted = (args) => {
    const socket = socketFactory(args)
    created += 1
    if (identities) identities.set(socket, { id: created, node: `${args.host}:${args.port}` })
    open.add(socket)
    if (originalQueues) originalSockets.push(new WeakRef(socket))
    return socket
  }
  const createKafka = (configuration = {}) => {
    const client = new Kafka({ ...options, ...configuration, socketFactory: counted })
    if (publicOnly) return client
    const { symbol, value: createCluster } = privateOf(client, 'createCluster')
    client[symbol] = (...args) => {
      const cluster = createCluster(...args)
      clusters.add(new WeakRef(cluster))
      return cluster
    }
    return client
  }
  const kafka = createKafka()
  const operations = publicOnly ? null : privateOf(kafka, 'operations').value

  const openSockets = () => {
    for (const socket of open) {
      if (socket.destroyed) open.delete(socket)
    }
    return [...open]
  }
  const liveClusters = () => {
    const live = []
    for (const ref of clusters) {
      const cluster = ref.deref()
      if (cluster === undefined) clusters.delete(ref)
      else live.push(cluster)
    }
    return live
  }
  const queues = () => [
    ...(originalQueues?.() ?? []),
    ...(publicOnly ? [] : liveClusters())
      .flatMap(({ brokerPool }) => [...Object.values(brokerPool.brokers), brokerPool.seedBroker])
      .filter(Boolean)
      .flatMap(({ connectionPool }) => connectionPool.pool.map(({ requestQueue }) => requestQueue)),
  ]
  const unique = (items) => [...new Set(items)]

  /** Open sockets and their listeners by event, plus the caller's owned resources, by role. */
  const publicIdentities = () => {
    const roles = { sockets: openSockets() }
    for (const socket of roles.sockets) {
      if (!emitterIds.has(socket)) emitterIds.set(socket, ++nextEmitterId)
      for (const event of socket.eventNames())
        (roles[`listener:socket${emitterIds.get(socket)}:${String(event)}`] ??= []).push(
          ...socket.rawListeners(event)
        )
    }
    return { ...roles, ...(ownedIdentities?.() ?? {}) }
  }
  /** Lets the collection callback's timer be destroyed before identities are read. */
  const settlingPause = async (ms) => {
    await sleep(ms)
    await new Promise((resolve) => setImmediate(resolve))
    await new Promise((resolve) => setImmediate(resolve))
  }
  const probe = createResourceProbe({
    emitters: {
      ...(publicOnly
        ? {}
        : {
            get requestQueues() {
              return together(unique(queues()))
            },
          }),
      get sockets() {
        return together(openSockets())
      },
    },
    sockets: () => ({ created, destroyed: created - openSockets().length }),
    ...(publicOnly
      ? {}
      : {
          queueDepth: () =>
            unique(queues()).reduce(
              (total, queue) => total + queue.pending.length + queue.inflight.size,
              0
            ),
          ledgerSize: () =>
            [...operations.values()].reduce((total, ledger) => total + ledger.size(), 0),
          obligationSize: () =>
            [...operations.values()]
              .flatMap((ledger) => [...ledger.attached])
              .reduce((total, obligations) => total + obligations.size(), 0),
        }),
    shareState,
    activeResources,
    heap,
    ...(publicOnly && memory ? { memory: true, pause: settlingPause } : {}),
    ...(publicOnly ? { identities: publicIdentities } : {}),
  })

  if (identities) {
    const snapshot = probe.snapshot.bind(probe)
    /** Reads counters and identities in one turn before heap sampling can await.
     * Closed sockets are not retained by the identity map.
     * @returns {Promise<object>} a fresh, serializable observation; no socket references
     */
    probe.snapshot = async () => {
      const live = openSockets().map((socket) => identities.get(socket))
      const nodes = liveClusters().flatMap(({ brokerPool }) => [
        ...(brokerPool.metadata?.brokers || []).map(({ host, port }) => `${host}:${port}`),
        ...[brokerPool.seedBroker]
          .filter(Boolean)
          .map(({ connectionPool }) => connectionPool.brokerAddress),
      ])
      const row = await snapshot()
      return {
        ...row,
        sockets: { ...row.sockets, live },
        socketNodes: unique(nodes),
      }
    }
  }

  const named = (emitter) =>
    Object.fromEntries(
      emitter.eventNames().map((name) => [String(name), emitter.listenerCount(name)])
    )
  const listeners = () => {
    if (publicOnly) return { sockets: named(together(openSockets())) }
    if (!originalQueues) {
      return {
        requestQueues: named(together(unique(queues()))),
        sockets: named(together(openSockets())),
      }
    }
    const live = []
    for (let i = 0; i < originalSockets.length;) {
      const socket = originalSockets[i].deref()
      if (socket) {
        live.push(socket)
        i++
      } else originalSockets.splice(i, 1)
    }
    return Object.fromEntries(
      [...unique(queues()), ...live].map((emitter) => {
        if (!emitterIds.has(emitter)) emitterIds.set(emitter, ++nextEmitterId)
        return [`owner${emitterIds.get(emitter)}`, named(emitter)]
      })
    )
  }

  /** Releases destroyed sockets without reading weak owners.
   * @returns {void}
   */
  const pruneSockets = () => {
    openSockets()
  }

  return { kafka, createKafka, probe, socketFactory: counted, listeners, pruneSockets }
}

/**
 * Verifies idle reaping on a producer whose sockets are counted separately from
 * the polling consumer and admin. Checks precede explicit disconnect, so shutdown
 * cannot impersonate reaping. No time or memory slope is inferred from a cycle.
 */
class IdleSocketCheck {
  /**
   * @param {object} options owned producer, probe and evidence ports
   * @param {object} options.producer connected public producer
   * @param {object} options.probe independent resource probe for that producer
   * @param {string} options.topic one stable topic
   * @param {Function} [options.nap] bounded foreground delay
   * @param {Function} [options.record] stamped evidence writer supplied by the soak
   * @returns {IdleSocketCheck} reusable cycle owner
   */
  constructor({
    producer,
    probe,
    topic,
    nap = sleep,
    record = (row) => writeEvidence('soak', row),
  }) {
    this.producer = producer
    this.probe = probe
    this.topic = topic
    this.nap = nap
    this.record = record
    this.sequence = 0
  }

  /**
   * Checks a positive population against the current bootstrap/metadata node inventory.
   * The isolated producer uses one primary socket per endpoint. Metadata can select
   * another endpoint at any send; endpoint populations need not match between sends.
   * @param {object} row observed counters, live identities and current node inventory
   * @param {number} [after=0] all live IDs must have been created after this counter
   * @returns {void}
   * @throws {Error} a missing, stale, unknown or duplicate socket, or inconsistent counters
   */
  population(row, after = 0) {
    const { sockets, socketNodes } = row
    const live = sockets.live
    if (
      !Number.isInteger(sockets.created) ||
      !Number.isInteger(sockets.destroyed) ||
      sockets.destroyed < 0 ||
      sockets.open < 1 ||
      sockets.created - sockets.destroyed !== sockets.open ||
      !Array.isArray(live) ||
      live.length !== sockets.open ||
      !Array.isArray(socketNodes) ||
      !socketNodes.length ||
      new Set(live.map(({ node }) => node)).size !== live.length ||
      new Set(live.map(({ id }) => id)).size !== live.length ||
      live.some(
        ({ id, node }) =>
          !Number.isInteger(id) ||
          id <= after ||
          id > sockets.created ||
          !socketNodes.includes(node)
      )
    )
      throw new Error('Idle producer did not reopen fresh bounded node sockets')
  }

  /**
   * Verifies that every tracked transport and request was reaped without an explicit close.
   * @param {object} row observed producer resources
   * @returns {void}
   * @throws {Error} any socket identity, open counter or request survives
   */
  reaped(row) {
    if (
      row.sockets.open !== 0 ||
      row.sockets.created !== row.sockets.destroyed ||
      !Array.isArray(row.sockets.live) ||
      row.sockets.live.length !== 0 ||
      row.queueDepth !== 0
    )
      throw new Error('Idle producer socket was not reaped')
  }

  /**
   * Reaps cold bootstrap work, then checks fresh per-node identities across another
   * idle interval. An expired metadata cache may add healthy sockets on any send.
   * @returns {Promise<void>}
   * @throws {Error} surviving resources or stale, unknown or duplicate reopened sockets
   */
  async cycle() {
    const send = () =>
      this.producer.send({
        topic: this.topic,
        messages: [{ partition: 0, value: String(this.sequence++) }],
      })
    await send()
    const warmup = await this.probe.snapshot()
    this.population(warmup)
    await this.nap(6000)
    const warmIdle = await this.probe.snapshot()
    this.reaped(warmIdle)
    await send()
    const before = await this.probe.snapshot()
    this.population(before, warmIdle.sockets.created)
    await this.nap(6000)
    const idle = await this.probe.snapshot()
    this.reaped(idle)
    await send()
    const reopened = await this.probe.snapshot()
    this.population(reopened, idle.sockets.created)
    this.record({ kind: 'idle-cycle', warmup, warmIdle, before, idle, reopened })
  }
}

/**
 * What a workload is handed.
 * @typedef {object} WorkloadContext
 * @property {object} kafka the `Kafka` the clients are built from
 * @property {string} topic the topic of the run
 * @property {IdleSocketCheck} [idleSocketCheck] separately counted idle producer checks
 * @property {string} groupId the consumer group of the run
 * @property {ProducerChurn} [producerChurn] the owner of the producer-churn phases
 * @property {{ runPhase: (name: string, ms: number) => Promise<void> }} [consumerChurn] the owner of the
 *   consumer-churn phases
 */

/**
 * The workloads. Each runs for `ms` milliseconds and resolves once every client it built is
 * disconnected. When it fails, it rejects with that failure after the same disconnects; a failed
 * disconnect never hides the failure of the workload, and does not stop the others.
 */
const WORKLOADS = {
  /**
   * Runs the telemetry-only workload under its existing Soak owner.
   * @param {WorkloadContext & { telemetryChurn: { work: (ms: number) => Promise<void> } }} context
   *   worker ownership
   * @param {number} ms fixed warm-up and measurement duration
   * @returns {Promise<void>} settles after public cleanup; failures remain failures
   * @throws {Error} for any phase or cleanup failure
   */
  telemetry: (context, ms) => context.telemetryChurn.work(ms),
  /**
   * Runs the next phase of the share soak through its owner, which keeps every client public and
   * disconnects them before it returns. The phase is the owner's own cursor, not an ambient member.
   * @param {WorkloadContext & { share: { runPhase: Function } }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves once the phase's ten cycles ran and its clients are closed.
   * @throws {Error} For a cycle, oracle or cleanup failure.
   */
  shareConsumer: (context, ms) => context.share.runPhase(ms),
  /**
   * Runs one measured phase of the consumer-churn profile through its owner, which keeps the clients
   * public and disconnects them before it returns.
   * @param {WorkloadContext & { consumerChurn: { runPhase: Function } }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves once the phase's clients are drained and disconnected.
   * @throws {Error} For workload or cleanup failure.
   */
  consumerMemberChurn: (context, ms) => context.consumerChurn.runPhase('member-churn', ms),
  /** Runs the regex-topic phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerRegexTopics: (context, ms) => context.consumerChurn.runPhase('regex-topics', ms),
  /** Runs the pause and recreation phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerPauseRecreate: (context, ms) => context.consumerChurn.runPhase('pause-recreate', ms),
  /** Runs the mostly idle phase of the consumer-churn profile; see `consumerMemberChurn`. */
  consumerMostlyIdle: (context, ms) => context.consumerChurn.runPhase('mostly-idle', ms),
  /**
   * Runs the steady producer phase through its owned lifecycle.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after drain, admission probe and disconnect.
   * @throws {Error} For workload or cleanup failure.
   */
  producerSteady(context, ms) {
    return context.producerChurn.runPhase('steady', ms)
  },
  /**
   * Runs the producer outage phase and restores every broker before returning.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after recovery and disconnect.
   * @throws {Error} For workload, restore or cleanup failure.
   */
  producerOutage(context, ms) {
    return context.producerChurn.runPhase('outage', ms)
  },
  /**
   * Runs buffer pressure through delayed writes, then clears the delay and drains.
   * @param {WorkloadContext & { producerChurn: ProducerChurn }} context
   * @param {number} ms Phase duration in milliseconds.
   * @returns {Promise<void>} Resolves after recovery and disconnect.
   * @throws {Error} For workload, positive-control or cleanup failure.
   */
  producerPressure(context, ms) {
    return context.producerChurn.runPhase('buffer-pressure', ms)
  },
  /**
   * One member polling the topic while a message arrives every second.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run or send, after the clients are
   *   disconnected; when only a disconnect fails, that failure
   */
  async idle({ kafka, topic, groupId, idleSocketCheck }, ms) {
    await Clients.use(kafka, async (clients) => {
      const producer = idleSocketCheck ? null : clients.producer()
      const consumer = clients.consumer({ groupId })
      if (producer) await producer.connect()
      await consumer.connect()
      await subscribeNamedTopic(consumer, { topic, fromBeginning: false })
      await consumer.run({ eachMessage: async () => {} })

      const deadline = Date.now() + ms
      for (let key = 0; Date.now() < deadline; key++) {
        if (idleSocketCheck) {
          await idleSocketCheck.cycle()
          continue
        }
        await producer.send({ topic, messages: [{ key: String(key % PARTITIONS), value: 'x' }] })
        await sleep(1000)
      }
    })
  },

  /**
   * Two members of one group, one slot's consumer replaced by a new one every 15 s, cooperatively.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run, send or replacement, after every
   *   client built so far, a replacement half built among them, is disconnected; when only a
   *   disconnect fails, that failure
   */
  async memberChurn({ kafka, topic, groupId }, ms) {
    await Clients.use(kafka, async (clients) => {
      const producer = clients.producer()
      await producer.connect()
      const start = async () => {
        const consumer = clients.consumer({
          groupId,
          partitionAssigners: [PartitionAssigners.cooperativeSticky],
        })
        await consumer.connect()
        await subscribeNamedTopic(consumer, { topic, fromBeginning: false })
        await consumer.run({ eachMessage: async () => {} })
        return consumer
      }
      const slots = [await start(), await start()]

      const deadline = Date.now() + ms
      for (let round = 0; Date.now() < deadline; round++) {
        await producer.send({ topic, messages: [{ key: String(round % PARTITIONS), value: 'x' }] })
        await sleep(Math.min(MEMBER_CHURN_MS, Math.max(deadline - Date.now(), 0)))
        const slot = round % slots.length
        await clients.release(slots[slot])
        slots[slot] = await start()
      }
    })
  },

  /**
   * A regex subscriber while 50 distinct names are created, written to and deleted in turn.
   * @param {WorkloadContext} context `topic` is not used: the topics are `soak-regex-<n>`
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of a connect, subscribe, run, topic creation, send or deletion,
   *   after the admin, the producer and the consumer are disconnected; when only a disconnect
   *   fails, that failure
   */
  async regexTopicChurn({ kafka, groupId }, ms) {
    await Clients.use(kafka, async (clients) => {
      const admin = clients.admin()
      const producer = clients.producer()
      const consumer = clients.consumer({ groupId })
      await admin.connect()
      await producer.connect()
      await consumer.connect()
      await consumer.subscribe({ topics: [/^soak-regex-/], fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      const deadline = Date.now() + ms
      for (let round = 0; Date.now() < deadline; round++) {
        const topic = `soak-regex-${round % TOPIC_NAMES}`
        await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })
        await producer.send({ topic, messages: [{ value: 'x' }] })
        await sleep(1000)
        await admin.deleteTopics({ topics: [topic] })
      }
    })
  },

  /**
   * The idle workload while a broker is stopped for the middle third of the phase. Both branches
   * settle before this does, and the broker is started again even when stopping it throws.
   * @param {WorkloadContext} context
   * @param {number} ms how long to run, in milliseconds
   * @returns {Promise<void>}
   * @throws {Error} the failure of the idle workload, else of stopping the broker, else of starting
   *   it again, each after the clients are disconnected and the restart attempted
   */
  async outage(context, ms) {
    const { stopBroker, startBroker } = require('../../testHelpers/leaderElection')
    const third = Math.floor(ms / 3)
    const outage = async () => {
      await sleep(third)
      try {
        stopBroker(OUTAGE_SERVICE)
        await sleep(third)
      } catch (error) {
        try {
          startBroker(OUTAGE_SERVICE)
        } catch {}
        throw error
      }
      startBroker(OUTAGE_SERVICE)
    }

    const [idle, broker] = await Promise.allSettled([WORKLOADS.idle(context, ms), outage()])
    for (const result of [idle, broker]) {
      if (result.status === 'rejected') {
        throw result.reason
      }
    }
  },
}

/**
 * Creates the topic of a run, over three replicas, and waits for its leaders.
 * @param {object} kafka a `Kafka`
 * @param {string} topic
 * @returns {Promise<void>}
 * @throws {Error} the failure of the creation, after the admin is disconnected
 */
const createTopic = (kafka, topic) =>
  Clients.use(kafka, async (clients) => {
    const admin = clients.admin()
    await admin.connect()
    await admin.createTopics({
      topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 3 }],
      waitForLeaders: true,
    })
  })

/** The bytes one Fetch may return for a partition: 1 MiB. */
const PARTITION_MAX_BYTES = 1048576

/** @type {{ now: () => number }} the monotonic clock of a run; a spec supplies its own */
const REAL_CLOCK = Object.freeze({ now: () => performance.now() })
/** The default Soak clock; it looks `Date` up at each reading, as the sampler always did. */
const WALL_CLOCK = Object.freeze({ now: () => Date.now() })

/**
 * Connects one broker through `socketFactory`, with no client around it: no retry, no metadata
 * cache, no group.
 * @param {{ host: string, port: number, socketFactory: Function }} options
 * @returns {Promise<Broker>} connected
 */
const openRawBroker = async ({ host, port, socketFactory }) => {
  const logger = createLogger({ level: logLevel.NOTHING, logCreator: LoggerConsole })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host,
      port,
      logger,
      socketFactory,
      connectionTimeout: 3000,
      requestTimeout: 30000,
    }),
    logger,
  })
  await broker.connect()
  return broker
}

/**
 * The live reader of the producer-churn soak: one raw reader role that follows the topic from
 * offset 0 with `Broker#fetch`, a connection to each partition leader and one to ask for metadata,
 * all opened through the counted socket factory so what it leaves open is measured. It joins no
 * group and commits nothing. Each partition has its own cursor, which moves only past a record
 * actually read; after a leadership or connection error the connections concerned are dropped and
 * the routing is fetched again. Cursors survive `close`, so one reader serves every phase. It hands
 * each data record on in order and keeps no record itself.
 */
class RawProducerReader {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses, `host:port`, asked in turn for metadata
   * @param {string} options.topic
   * @param {(line: object) => void} options.record where the reader's routing lines go
   * @param {Function} [options.socketFactory] the counted factory of `instrument`
   * @param {(partition: number, record: object) => void} [options.onRecord] given every data
   *   record, in partition order, before the cursor passes it
   * @param {number} [options.partitions=PARTITIONS]
   * @param {(options: object) => Promise<object>} [options.openBroker] connects one broker; a spec
   *   supplies a fake
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {number} [options.maxWaitTime=100] how long a Fetch waits at the broker for data, ms
   * @param {number} [options.retryMs=200] the pause after an error, ms
   * @param {number} [options.drainMs=120000] how long `drain` may take, ms
   */
  constructor({
    brokers,
    topic,
    record,
    socketFactory = defaultSocketFactory(),
    onRecord = () => {},
    partitions = PARTITIONS,
    openBroker = openRawBroker,
    clock = REAL_CLOCK,
    sleep: pause = sleep,
    maxWaitTime = 100,
    retryMs = 200,
    drainMs = 120000,
  }) {
    Object.assign(this, {
      brokers,
      topic,
      record,
      onRecord,
      partitions,
      openBroker,
      clock,
      pause,
      maxWaitTime,
      retryMs,
      drainMs,
    })
    this.cursor = new Float64Array(partitions)
    this.leaders = new Int32Array(partitions).fill(-1)
    this.addresses = new Map()
    this.connections = new Map()
    this.seed = null
    this.routed = false
    this.sockets = new Set()
    this.creating = false
    this.refetches = 0
    this.records = 0
    this.lastRoute = ''
    this.factory = (args) => {
      this.creating = true
      try {
        const socket = socketFactory(args)
        for (const own of this.sockets) {
          if (own.destroyed) this.sockets.delete(own)
        }
        this.sockets.add(socket)
        return socket
      } finally {
        this.creating = false
      }
    }
  }

  /**
   * @param {object} socket a socket of the counted factory
   * @returns {boolean} whether the reader made it, or is making it now
   */
  owns(socket) {
    return this.creating || this.sockets.has(socket)
  }

  /**
   * @returns {Map<number, number>} the next offset to read of each partition
   */
  cursors() {
    return new Map(Array.from(this.cursor, (offset, partition) => [partition, offset]))
  }

  /**
   * Opens the connections the routing needs.
   * @returns {Promise<void>}
   * @throws {Error} when no seed broker answers or the topic has no metadata
   */
  start() {
    return this.#route()
  }

  /**
   * One round: fetches from every partition leader at once, waiting at most `maxWaitTime` for data,
   * and hands each record read on. A retriable error, such as a leader that moved or a node that
   * is down, drops the connections concerned, schedules a new routing and pauses `retryMs`.
   * @returns {Promise<number>} how many records were read
   * @throws {Error} for an error that is not retriable
   */
  async poll() {
    let failed = false
    try {
      if (!this.routed) await this.#route()
    } catch (error) {
      if (error?.retriable !== true) throw error
      failed = true
    }
    let read = 0
    if (!failed) {
      const byNode = new Map()
      this.leaders.forEach((node, partition) => {
        if (node < 0) failed = true
        else byNode.set(node, [...(byNode.get(node) ?? []), partition])
      })
      const rounds = await Promise.all([...byNode].map(([node, ids]) => this.#fetch(node, ids)))
      for (const round of rounds) {
        read += round.read
        failed ||= round.failed
      }
    }
    if (failed) {
      this.routed = false
      await this.pause(this.retryMs)
    }
    return read
  }

  /**
   * Polls until every cursor has reached its frozen end.
   * @param {Map<number, string>} ends the end offset of each partition
   * @returns {Promise<void>}
   * @throws {Error} naming the first partition still short when `drainMs` runs out
   */
  async drain(ends) {
    const deadline = this.clock.now() + this.drainMs
    for (;;) {
      const short = [...ends].find(([partition, end]) => this.cursor[partition] < Number(end))
      if (!short) return
      if (this.clock.now() >= deadline) {
        throw new Error(
          `The reader is at ${this.cursor[short[0]]} of partition ${short[0]}, below its frozen end ${short[1]}`
        )
      }
      await this.poll()
    }
  }

  /**
   * Disconnects every connection the reader holds; the cursors stay.
   * @returns {Promise<void>}
   * @throws {Error} the first failed disconnect, after all were tried
   */
  async close() {
    const held = [this.seed, ...this.connections.values()].filter(Boolean)
    this.seed = null
    this.connections.clear()
    this.routed = false
    const results = await Promise.allSettled(held.map((broker) => broker.disconnect()))
    this.sockets.clear()
    const failed = results.find(({ status }) => status === 'rejected')
    if (failed) throw failed.reason
  }

  /**
   * @private
   * @param {number} node
   * @param {number[]} partitions the partitions it leads
   * @returns {Promise<{ read: number, failed: boolean }>}
   * @throws {Error} for an error that is not retriable
   */
  async #fetch(node, partitions) {
    try {
      const broker = await this.#connection(node)
      const { responses } = await broker.fetch({
        isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
        maxWaitTime: this.maxWaitTime,
        topics: [
          {
            topic: this.topic,
            partitions: partitions.map((partition) => ({
              partition,
              fetchOffset: String(this.cursor[partition]),
              maxBytes: PARTITION_MAX_BYTES,
            })),
          },
        ],
      })
      let read = 0
      for (const { partitions: answered } of responses) {
        for (const { partition, messages } of answered) read += this.#take(partition, messages)
      }
      return { read, failed: false }
    } catch (error) {
      if (error?.retriable !== true) throw error
      this.refetches += 1
      await this.connections
        .get(node)
        ?.disconnect()
        .catch(() => {})
      this.connections.delete(node)
      return { read: 0, failed: true }
    }
  }

  /**
   * Hands the data records of one partition on and moves its cursor past each.
   * @private
   * @param {number} partition
   * @param {object[]} messages
   * @returns {number} how many data records it passed
   */
  #take(partition, messages) {
    let taken = 0
    for (const message of messages) {
      const offset = Number(message.offset)
      if (offset < this.cursor[partition] || message.batchContext?.isControlBatch) continue
      this.onRecord(partition, message)
      this.cursor[partition] = offset + 1
      taken += 1
    }
    this.records += taken
    return taken
  }

  /**
   * @private
   * @param {number} node
   * @returns {Promise<object>} the connection to the node, opened when it has none
   */
  async #connection(node) {
    let broker = this.connections.get(node)
    if (!broker) {
      broker = await this.#open(this.addresses.get(node))
      this.connections.set(node, broker)
    }
    return broker
  }

  /**
   * @private
   * @param {{ host: string, port: number }} address
   * @returns {Promise<object>}
   */
  #open({ host, port }) {
    return this.openBroker({ host, port, socketFactory: this.factory })
  }

  /**
   * Asks the first seed broker that answers for the topic's leaders, and keeps that connection.
   * @private
   * @returns {Promise<void>}
   * @throws {Error} the last retriable failure when no seed answers; any other failure at once
   */
  async #route() {
    let failure
    for (const address of this.brokers) {
      const [seedHost, seedPort] = address.split(':')
      let seed
      try {
        seed = await this.#open({ host: seedHost, port: Number(seedPort) })
        const { brokers, topicMetadata } = await seed.metadata([this.topic])
        const metadata = topicMetadata.find(({ topic }) => topic === this.topic)
        if (!metadata || metadata.topicErrorCode) {
          throw Object.assign(new Error(`No metadata for ${this.topic}`), { retriable: true })
        }
        this.addresses = new Map(brokers.map(({ nodeId, host, port }) => [nodeId, { host, port }]))
        this.leaders.fill(-1)
        for (const { partitionId, leader } of metadata.partitionMetadata) {
          if (partitionId < this.partitions) this.leaders[partitionId] = leader
        }
        await this.seed?.disconnect().catch(() => {})
        this.seed = seed
        this.routed = true
        const leaders = Array.from(this.leaders).join(',')
        if (leaders !== this.lastRoute) {
          this.lastRoute = leaders
          this.record({ kind: 'reader', event: 'route', leaders })
        }
        return
      } catch (error) {
        await seed?.disconnect().catch(() => {})
        if (error?.retriable !== true) throw error
        failure = error
      }
    }
    throw failure ?? new Error('The reader has no seed broker')
  }
}

/**
 * Delays what the workload's own producer writes, to fill its buffer, and takes the delay off
 * again. The reader's sockets are left alone, so what it measures is not itself delayed.
 */
class WriteDelay {
  /**
   * @param {object} options
   * @param {ReturnType<typeof createChaosSocketFactory>} options.chaos the factory the client's
   *   sockets come from
   * @param {{ owns: (socket: object) => boolean }} options.reader whose sockets are spared
   */
  constructor({ chaos, reader }) {
    this.chaos = chaos
    this.reader = reader
    this.disarm = null
  }

  /**
   * Delays every write of every open and later client socket.
   * @param {number} ms
   * @returns {void}
   */
  delayWrites(ms) {
    this.disarm?.()
    this.disarm = this.chaos.arm({}, (socket) => {
      if (!this.reader.owns(socket)) socket.delay({ write: ms })
    })
  }

  /**
   * Takes the delay off every socket, so later writes pass at once, and forgets destroyed sockets.
   * @returns {void}
   */
  clear() {
    this.disarm?.()
    this.disarm = null
    for (const socket of this.chaos.sockets) {
      if (!socket.destroyed && !this.reader.owns(socket)) socket.delay({ write: 0 })
    }
    this.chaos.sockets = this.chaos.sockets.filter(({ destroyed }) => !destroyed)
  }
}

/**
 * The producer-churn soak: one idempotent producer offers `rate` public sends a second to one
 * six-partition topic for three phases, while one raw reader reads the log, and what is left is
 * judged by four independent readings.
 *
 * - **steady**: the load alone, which has to hold its offered rate.
 * - **outage**: a clean, then a hard, rolling restart of the brokers, one node at a time, each
 *   restored before the next, and after each restoration a normal send that must be acknowledged.
 * - **buffer-pressure**: every write is delayed 5 s so the producer's buffer fills; a typed
 *   `KafkaJSBufferExhaustedError` must refuse sends, and once the delay is off a normal send must
 *   be acknowledged.
 *
 * Every phase ends the same way: the offering stops, every call is joined, the producer is
 * flushed, the closing admission probe runs, the reader catches up, and the producer, the admin and
 * the reader disconnect. The readings: the resource bounds of `Soak` (the sockets, listeners by
 * name, queues and timers, and the heap, which `evaluatePhases` judges inside each phase and across
 * the quiet points, since the buffer-pressure phase holds a full buffer by design); the controls of
 * each phase; the closing probe of each phase; and
 * the durability of the log, read once through the reader's own journal and once again from the
 * quiet log. Each acknowledged id must be in the log exactly once, an id of unknown outcome at most
 * once, an id refused before sending never, and no record may repeat a `(partition, pid, epoch,
 * sequence)`. `ok` is the conjunction of all four.
 *
 * Observer storage is allocated once in `prepare`, before the baseline reading, and never grows.
 * Nothing resends an unknown outcome: a record the application would send again would have a new id.
 */
class ProducerChurn {
  /**
   * @param {object} options
   * @param {object} options.kafka a `Kafka` whose sockets are counted (`instrument`)
   * @param {{ snapshot: () => Promise<object> }} options.probe the resource probe of `instrument`
   * @param {string[]} options.brokers seed addresses, `host:port`
   * @param {string} options.topic the workload topic, made by `prepare`
   * @param {(line: object) => void} options.record where every evidence line goes
   * @param {Array<{ name: string, workload: string, ms: number }>} options.phases
   * @param {{ rollingBounce: Function }} options.bouncer a `BrokerBouncer`
   * @param {object} options.reader a `RawProducerReader`; `onRecord` is set here
   * @param {object} options.admissionProbe a `ClosingProbe`: `prepare`, `install`, `restore`,
   *   `capture`, `run` and `retained`
   * @param {{ delayWrites: (ms: number) => void, clear: () => void }} options.faults
   * @param {() => Record<string, Record<string, number>>} [options.listeners] the listeners of each
   *   tracked emitter by event name
   * @param {{ append: Function, replay: Function, close: Function }} [options.journal] what the
   *   reader saw; a file under `.tmp/e2e` by default
   * @param {{ now: () => number }} [options.clock]
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [options.sleep]
   * @param {{ endOffsets: Function, readLog: Function }} [options.rawLog]
   * @param {Record<string, Function>} [options.workloads=WORKLOADS]
   * @param {object} [options.soak] `sampleMs`, `warmupMs` (how long into each phase its heap
   *   samples start to count; `PHASE_WARMUP_MS` by default) and `settleMs` for `Soak`
   * @param {Partial<typeof CHURN> & { requiredKinds?: string[] }} [options.settings] overrides of
   *   `CHURN`, and which kinds of outage the outage phase must have made
   */
  constructor({
    kafka,
    probe,
    brokers,
    topic,
    record,
    phases,
    bouncer,
    reader,
    admissionProbe,
    faults,
    listeners = () => ({}),
    journal,
    clock = REAL_CLOCK,
    sleep: pause = sleep,
    rawLog = { endOffsets, readLog },
    workloads = WORKLOADS,
    soak = {},
    settings = {},
  }) {
    Object.assign(this, {
      kafka,
      brokers,
      topic,
      record,
      phases,
      bouncer,
      reader,
      admissionProbe,
      faults,
      listeners,
      journal,
      clock,
      pause,
      rawLog,
      workloads,
      soak: { warmupMs: PHASE_WARMUP_MS, ...soak },
    })
    this.settings = { requiredKinds: ['clean', 'hard'], ...CHURN, ...settings }
    this.measure = {
      snapshot: async () => ({
        ...(await probe.snapshot()),
        named: this.listeners(),
        unhandled: this.unhandled,
      }),
    }
    this.tag = (Math.random() * 0xffffffff) >>> 0
    this.reader.onRecord = (partition, message) => this.#observe(partition, message)
    this.ledger = null
    this.observed = null
    this.producer = null
    this.admin = null
    this.bytes = this.settings.steadyBytes
    this.unsettled = 0
    this.unhandled = 0
    this.capacityReached = false
    this.firstUnexpected = null
    this.prepared = false
    this.closed = false
    this.connected = false
    this.captured = false
    this.startedAt = 0
    this.phase = null
    this.phaseResults = {}
    this.quietPoints = []
    this.wire = {}
    this.outcome = null
    this.stopWire = () => {}
    this.onUnhandled = () => {
      this.unhandled += 1
    }
  }

  /**
   * Allocates every observer, installs the probe, and makes the topics. Everything the run keeps
   * is made here, before the baseline reading.
   * @returns {Promise<void>}
   * @throws {Error} when called twice, or the topics cannot be made
   */
  async prepare() {
    if (this.prepared) throw new Error('The producer churn is already prepared')
    this.prepared = true
    const { rate, idSlack, partitions } = this.settings
    const totalMs = this.phases.reduce((total, { ms }) => total + ms, 0)
    const capacity = Math.ceil((rate * totalMs) / 1000) + idSlack
    this.ledger = new OfferLedger({
      capacity,
      partitions,
      sampleEvery: this.settings.latencySampleEvery,
    })
    this.observed = new ObservedLog({ capacity })
    this.record({
      kind: 'config',
      config: {
        ...this.settings,
        phases: this.phases,
        ledgerCapacity: capacity,
        replicationFactor: 3,
        brokers: this.brokers,
        topic: this.topic,
      },
    })
    this.journal ??= openJournal()
    this.startedAt = this.clock.now()
    process.on('unhandledRejection', this.onUnhandled)
    this.admissionProbe.install()
    await Clients.use(this.kafka, async (clients) => {
      const admin = clients.admin()
      await admin.connect()
      await admin.createTopics({
        topics: [
          {
            topic: this.topic,
            numPartitions: partitions,
            replicationFactor: 3,
            configEntries: [
              { name: 'min.insync.replicas', value: String(this.settings.minInsyncReplicas) },
            ],
          },
        ],
        waitForLeaders: true,
      })
    })
    await this.admissionProbe.prepare(this.kafka)
  }

  /**
   * Runs the whole profile: `prepare`, then the phases of `Soak` with this object as their owner,
   * the durability reading before the teardown reading, and the verdict.
   * @returns {Promise<{ ok: boolean, violations: string[] }>} the verdict, also recorded as the
   *   run's one `verdict` line; the resource verdict of `Soak` is recorded as `resource-verdict`
   * @throws {Error} the failure of a phase, after its clients are disconnected and a failed
   *   verdict is recorded; or a profile that names a workload that is not registered
   */
  async run() {
    const unregistered = this.phases.find(
      ({ workload }) => typeof this.workloads[workload] !== 'function'
    )
    if (unregistered) {
      throw new Error(
        `The phase ${unregistered.name} names the workload ${unregistered.workload}, which is not registered`
      )
    }
    try {
      await this.prepare()
      const soak = new Soak({
        kafka: this.kafka,
        probe: this.measure,
        phases: this.phases,
        judge: (run) => evaluatePhases({ ...run, minSamples: this.settings.minHeapSamples }),
        context: {
          kafka: this.kafka,
          topic: this.topic,
          groupId: `${this.topic}-group`,
          producerChurn: this,
        },
        record: (line) => this.#recordSoak(line),
        workloads: this.workloads,
        beforeTeardown: async () => {
          this.outcome = await this.finish()
        },
        ...this.soak,
      })
      const resources = await soak.run()
      await this.close()
      await this.pause(1)
      const violations = [
        ...this.outcome.violations,
        ...this.#quietPointViolations(),
        ...(this.unhandled === 0 ? [] : [`unhandled rejections: ${this.unhandled} after close`]),
        ...resources.violations.map((line) => `resources: ${line}`),
      ]
      const verdict = {
        ok: violations.length === 0,
        violations,
        resources,
        controls: this.outcome.controls,
        durability: this.outcome.durability,
        latency: this.outcome.latency,
        counts: this.outcome.counts,
        wireVersions: this.wire,
      }
      this.record({ kind: 'verdict', ...verdict })
      return verdict
    } catch (error) {
      this.record({ kind: 'verdict', ok: false, violations: [`run failed: ${error.message}`] })
      throw error
    } finally {
      await this.close()
    }
  }

  /**
   * One phase: connects the producer and the reader, offers load for `ms` while the phase's own
   * fault runs, reaches the quiet point, and disconnects. Whatever fails, the fault is taken off,
   * the node is restored, the background work is joined and every client is disconnected, and the
   * first failure is the one thrown.
   * @param {'steady'|'outage'|'buffer-pressure'} name
   * @param {number} ms how long to offer load
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the work, the quiet point or the cleanup
   */
  async runPhase(name, ms) {
    if (!this.prepared || this.closed) {
      throw new Error(`The phase ${name} has no prepared, open producer churn to run in`)
    }
    this.#begin(name, ms)
    const work = new AbortController()
    const reading = new AbortController()
    let failure
    const guard = (task) =>
      task().catch((error) => {
        failure ??= error
        work.abort(error)
        reading.abort(error)
      })
    const tasks = []
    const spawn = (task) => tasks.push(guard(task))
    let following = Promise.resolve()
    try {
      await this.#open(name)
      following = guard(() => this.#follow(reading.signal))
      await this.#work(name, { until: this.clock.now() + ms, signal: work.signal, spawn })
      work.abort()
      await Promise.all(tasks)
      if (failure) throw failure
      await this.quiet(name)
      reading.abort()
      await following
      if (failure) throw failure
      const ends = await this.rawLog.endOffsets({ brokers: this.brokers, topic: this.topic })
      await this.reader.drain(ends)
    } catch (error) {
      failure ??= error
    }
    work.abort()
    reading.abort()
    await Promise.allSettled([...tasks, following])
    const cleanup = await this.#disconnect()
    this.#end()
    if (failure) {
      failure.cleanupErrors = cleanup
      throw failure
    }
    if (cleanup.length > 0) throw cleanup[0]
  }

  /**
   * Offers sends at the configured rate until `until`, on a deadline schedule: the n-th send is
   * due at `n / rate` after the start, so a late tick is made up and its lateness is recorded as
   * slip. A tick that finds `maxUnsettled` sends unsettled offers none and counts backpressure.
   * @param {number} until a time of the clock
   * @param {AbortSignal} [signal] ends the loop early
   * @returns {Promise<void>}
   */
  async offerUntil(until, signal) {
    const { rate, maxUnsettled } = this.settings
    const start = this.clock.now()
    const stats = this.phase
    let slot = 0
    while (this.clock.now() < until && !signal?.aborted) {
      const now = this.clock.now()
      const due = Math.floor(((now - start) * rate) / 1000) + 1
      if (slot < due) {
        const slip = Math.max(0, now - (start + (slot * 1000) / rate))
        stats.slip.maxMs = Math.max(stats.slip.maxMs, slip)
        stats.slip.totalMs += slip
        stats.slip.ticks += 1
      }
      for (; slot < due; slot++) {
        if (this.unsettled >= maxUnsettled) stats.backpressure += 1
        else if (this.#offer(this.bytes) === null) return
      }
      await this.pause(Math.max(1, start + (slot * 1000) / rate - this.clock.now()), signal)
    }
  }

  /**
   * The quiet point of a phase, with the producer still connected: every call joined, the producer
   * flushed, and the closing admission probe run. A probe that fails is recorded for the verdict,
   * it does not stop the phase.
   * @param {string} name the phase
   * @returns {Promise<void>}
   * @throws {Error} when calls are still unsettled after `joinMs` or the flush fails
   */
  async quiet(name) {
    const began = this.clock.now()
    const { joinMs, flushMs } = this.settings
    if (!(await this.#waitFor(() => this.unsettled === 0, joinMs))) {
      throw new Error(`${name}: ${this.unsettled} calls are still unsettled after ${joinMs} ms`)
    }
    await this.producer.flush({ timeout: flushMs })
    try {
      this.phase.probe = await this.admissionProbe.run({ producer: this.producer, label: name })
    } catch (error) {
      this.phase.probeError = error.message
    }
    this.phase.quietMs = this.clock.now() - began
    this.record({
      kind: 'quiet',
      phase: name,
      quietMs: this.phase.quietMs,
      probe: this.phase.probe,
      probeError: this.phase.probeError,
    })
  }

  /**
   * The durability reading and the controls, once every phase is over: freezes each partition's end
   * offset, drains the live reader to it, merges the reader's journal by id, then reads the quiet
   * log independently and merges that too.
   * @returns {Promise<{ ok: boolean, violations: string[], durability: object, controls: object,
   *   latency: object, counts: object }>} every violation, not the first
   */
  async finish() {
    const violations = []
    let ends = null
    try {
      ends = await this.rawLog.endOffsets({ brokers: this.brokers, topic: this.topic })
      await this.reader.start()
      try {
        await this.reader.drain(ends)
      } finally {
        await this.reader.close()
      }
    } catch (error) {
      violations.push(`durability: the reader did not reach the frozen ends: ${error.message}`)
    }
    const cursors = this.reader.cursors()
    for (const [partition, end] of ends ?? []) {
      const cursor = cursors.get(partition) ?? 0
      if (cursor < Number(end)) {
        violations.push(
          `durability: the final cursor ${cursor} of partition ${partition} is below the frozen end ${end}`
        )
      }
    }

    this.observed.reset()
    this.journal.replay((...entry) => this.observed.add(...entry))
    const live = mergeDurability(this.ledger, this.observed, 'live reader')
    violations.push(...live.violations)
    const { lastAckId, lastAckOffset } = this.ledger
    lastAckId.forEach((id, partition) => {
      if (
        id >= 0 &&
        !(this.observed.seen[id] > 0 && cursors.get(partition) > lastAckOffset[partition])
      ) {
        violations.push(
          `durability: the last acknowledged record of partition ${partition} (id ${id}, offset ${lastAckOffset[partition]}) was not reached by the reader`
        )
      }
    })
    const quiet = await this.#readQuiet(ends, violations)

    const latency = this.ledger.latency()
    const controls = this.#controls(violations, latency)
    const counts = { offered: this.ledger.offered, ...this.ledger.counts }
    this.record({ kind: 'control', controls, latency })
    this.record({ kind: 'oracle', live, quiet, counts })
    return {
      ok: violations.length === 0,
      violations,
      durability: { live, quiet },
      controls,
      latency,
      counts,
    }
  }

  /**
   * Disconnects whatever is still open, restores the probe, takes the listener off the process and
   * closes the journal; safe to call again.
   * @returns {Promise<void>}
   * @throws {Error} the first failure, after every step was tried
   */
  async close() {
    if (this.closed) return
    this.closed = true
    const failures = await this.#disconnect()
    for (const step of [
      () => this.stopWire(),
      () => process.off('unhandledRejection', this.onUnhandled),
      () => this.admissionProbe.restore(),
      () => this.journal?.close(),
    ]) {
      try {
        step()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length > 0) throw failures[0]
  }

  /**
   * @private
   * @param {string} name
   * @param {number} ms
   * @returns {void}
   */
  #begin(name, ms) {
    this.phase = {
      name,
      ms,
      startedAt: this.clock.now(),
      before: { offered: this.ledger.offered, ...this.ledger.counts },
      refusals: {},
      backpressure: 0,
      slip: { maxMs: 0, totalMs: 0, ticks: 0 },
      outages: [],
      recoveries: [],
      pressureRecovery: null,
      probe: null,
      probeError: null,
      afterDisconnect: null,
      quietMs: null,
    }
    this.bytes = this.settings.steadyBytes
  }

  /**
   * Closes the books of the current phase and records them.
   * @private
   * @returns {void}
   */
  #end() {
    const { phase, ledger } = this
    const delta = (key) =>
      (key === 'offered' ? ledger.offered : ledger.counts[key]) - phase.before[key]
    const refused = Object.values(phase.refusals).reduce((total, count) => total + count, 0)
    phase.offered = delta('offered')
    phase.ack = delta('ack')
    phase.unknown = delta('unknown')
    phase.unsent = delta('unsent')
    phase.unexpected = delta('unexpected')
    phase.admitted = phase.offered - refused
    phase.offeredPerSecond = phase.offered / (phase.ms / 1000)
    phase.slip.meanMs = phase.slip.ticks === 0 ? 0 : phase.slip.totalMs / phase.slip.ticks
    phase.endedAt = this.clock.now()
    this.phaseResults[phase.name] = phase
    const { name, before, probe, ...rest } = phase
    this.record({ kind: 'phase', phase: name, ...rest })
  }

  /**
   * @private
   * @param {string} name
   * @returns {Promise<void>}
   */
  async #open(name) {
    if (this.producer === null) {
      this.producer = this.kafka.producer({ ...this.settings.producer })
      this.stopWire = this.producer.on(this.producer.events.REQUEST, ({ payload }) => {
        this.wire[payload.apiName] = Math.max(this.wire[payload.apiName] ?? 0, payload.apiVersion)
      })
    }
    await this.producer.connect()
    this.connected = true
    await this.reader.start()
    if (name === 'outage') {
      this.admin = this.kafka.admin()
      await this.admin.connect()
    }
    if (!this.captured) {
      const status = await this.admissionProbe.capture(() => this.#offer(this.settings.steadyBytes))
      if (status !== STATUS.ACK) throw new Error('The first send of the run was not acknowledged')
      this.captured = true
    }
  }

  /**
   * Runs what a phase adds to the load.
   * @private
   * @param {string} name
   * @param {{ until: number, signal: AbortSignal, spawn: Function }} context
   * @returns {Promise<void>}
   */
  async #work(name, { until, signal, spawn }) {
    if (name === 'outage') {
      spawn(() => this.#roll(signal))
    } else if (name === 'buffer-pressure') {
      this.bytes = this.settings.pressureBytes
      this.faults.delayWrites(this.settings.pressureDelayMs)
    }
    try {
      await this.offerUntil(until, signal)
    } finally {
      if (name === 'buffer-pressure') {
        this.faults.clear()
        this.bytes = this.settings.steadyBytes
      }
    }
    if (name === 'buffer-pressure') this.phase.pressureRecovery = await this.#recover('pressure')
  }

  /**
   * The outage phase's fault: a clean and then a hard rolling restart, node by node, each node
   * restored and answered by a normal send before the next goes down. It stops when `signal` aborts.
   * @private
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   * @throws {Error} a failed step, after its node was restored
   */
  async #roll(signal) {
    for (const hard of [false, true]) {
      for (const service of CHURN_SERVICES) {
        if (signal.aborted) return
        let outages
        try {
          outages = await this.bouncer.rollingBounce({
            hard,
            admin: this.admin,
            topics: [this.topic],
            services: [service],
            signal,
          })
        } catch (error) {
          if (signal.aborted && error === signal.reason) return
          throw error
        }
        for (const { kind, outageMs } of outages) {
          this.phase.outages.push({ service, kind: OUTAGE_KINDS[kind] ?? kind, outageMs })
          const recovery = await this.#recover(`${service} ${kind}`)
          if (recovery) this.phase.recoveries.push(recovery)
        }
      }
    }
  }

  /**
   * Keeps the reader polling until `signal` aborts.
   * @private
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   * @throws {Error} a reader error that is not retriable
   */
  async #follow(signal) {
    while (!signal.aborted) {
      if ((await this.reader.poll()) === 0) await this.pause(this.settings.pollMs, signal)
    }
  }

  /**
   * Sends normal sends until one is acknowledged: the proof that the producer works again after a
   * restoration or after the pressure is off.
   * @private
   * @param {string} label
   * @returns {Promise<{ label: string, attempts: number }|null>} null when none was acknowledged
   *   within `recoveryMs`
   */
  async #recover(label) {
    const deadline = this.clock.now() + this.settings.recoveryMs
    for (let attempts = 1; ; attempts++) {
      const sent = this.#offer(this.settings.steadyBytes)
      if (sent !== null && (await sent) === STATUS.ACK) return { label, attempts }
      if (sent === null || this.clock.now() >= deadline) return null
      await this.pause(this.settings.recoveryRetryMs)
    }
  }

  /**
   * Makes one public send and settles its id with the outcome.
   * @private
   * @param {number} bytes the size of the value
   * @returns {Promise<number>|null} the status it ended with; null when the ledger is full
   */
  #offer(bytes) {
    const id = this.ledger.offer(this.clock.now())
    if (id < 0) {
      this.capacityReached = true
      return null
    }
    this.unsettled += 1
    let call
    try {
      call = this.producer.send({
        topic: this.topic,
        messages: [{ value: encodeValue(bytes, id, this.tag) }],
        acks: -1,
      })
    } catch (error) {
      call = Promise.reject(error)
    }
    return call.then(
      (metadata) => this.#acknowledged(id, metadata),
      (error) => this.#refused(id, error)
    )
  }

  /**
   * @private
   * @param {number} id
   * @param {Array<{ partition: number, baseOffset: string }>} [metadata]
   * @returns {number}
   */
  #acknowledged(id, metadata) {
    const [landed] = metadata ?? []
    this.ledger.settle(
      id,
      STATUS.ACK,
      landed && { partition: landed.partition, offset: Number(landed.baseOffset) }
    )
    this.unsettled -= 1
    return STATUS.ACK
  }

  /**
   * Classifies a rejected send: refused by the full buffer, or annotated `unsent` or `unknown` by
   * the producer; anything else is unexpected and fails the run.
   * @private
   * @param {number} id
   * @param {Error} error
   * @returns {number}
   */
  #refused(id, error) {
    let status = STATUS.UNEXPECTED
    if (error instanceof KafkaJSBufferExhaustedError) {
      status = STATUS.UNSENT
      this.phase.refusals[error.reason] = (this.phase.refusals[error.reason] ?? 0) + 1
    } else if (error?.outcome === 'unsent') {
      status = STATUS.UNSENT
    } else if (error?.outcome === 'unknown') {
      status = STATUS.UNKNOWN
    } else {
      this.firstUnexpected ??= `${error?.name}: ${error?.message}`
    }
    this.ledger.settle(id, status)
    this.unsettled -= 1
    return status
  }

  /**
   * Takes a record the live reader read.
   * @private
   * @param {number} partition
   * @param {object} message a decoded record
   * @returns {void}
   */
  #observe(partition, message) {
    const id = decodeId(message.value, this.tag)
    const now = this.clock.now()
    this.journal.append(
      id,
      partition,
      Number(message.batchContext.producerId),
      message.batchContext.producerEpoch,
      sequenceOf(message),
      Number(message.offset),
      now - this.startedAt
    )
    this.ledger.observed(id, now)
  }

  /**
   * @private
   * @param {() => boolean} done
   * @param {number} budgetMs
   * @returns {Promise<boolean>} whether `done` held within the budget
   */
  async #waitFor(done, budgetMs) {
    const deadline = this.clock.now() + budgetMs
    while (!done()) {
      if (this.clock.now() >= deadline) return false
      await this.pause(this.settings.pollMs)
    }
    return true
  }

  /**
   * Takes the delay off and disconnects the producer, the admin and the reader, trying every one,
   * then reads what the producer still retains.
   * @private
   * @returns {Promise<Error[]>} what failed
   */
  async #disconnect() {
    const failures = []
    const attempt = async (step) => {
      try {
        await step()
      } catch (error) {
        failures.push(error)
      }
    }
    await attempt(() => this.faults.clear())
    if (this.connected) {
      this.connected = false
      await attempt(() => this.producer.disconnect())
    }
    if (this.admin) {
      const { admin } = this
      this.admin = null
      await attempt(() => admin.disconnect())
    }
    await attempt(() => this.reader.close())
    if (this.phase && this.captured && this.phase.afterDisconnect === null) {
      try {
        this.phase.afterDisconnect = this.admissionProbe.retained()
      } catch (error) {
        failures.push(error)
      }
    }
    return failures
  }

  /**
   * @private
   * @param {string[]} violations
   * @param {Map<number, string>|null} ends
   * @returns {Promise<object|null>} the merge of the quiet log, or null when it could not be read
   */
  async #readQuiet(ends, violations) {
    try {
      const log = await this.rawLog.readLog({ brokers: this.brokers, topic: this.topic })
      this.observed.reset()
      for (const [partition, { endOffset, records }] of log) {
        if (ends && ends.has(partition) && String(endOffset) !== String(ends.get(partition))) {
          violations.push(
            `durability: partition ${partition} ends at ${endOffset} in the quiet read, ${ends.get(partition)} when frozen`
          )
        }
        for (const message of records) {
          if (message.batchContext.isControlBatch) continue
          this.observed.add(
            decodeId(message.value, this.tag),
            partition,
            Number(message.batchContext.producerId),
            message.batchContext.producerEpoch,
            sequenceOf(message)
          )
        }
      }
      const merged = mergeDurability(this.ledger, this.observed, 'quiet read')
      violations.push(...merged.violations)
      return merged
    } catch (error) {
      violations.push(`durability: the quiet read failed: ${error.message}`)
      return null
    }
  }

  /**
   * Judges every phase's controls, its closing probe and the latency samples.
   * @private
   * @param {string[]} violations receives each failed control
   * @param {ReturnType<OfferLedger['latency']>} latency
   * @returns {object} what each phase measured, for the evidence
   */
  #controls(violations, latency) {
    const { rate, targetFraction, requiredKinds, minLatencySamples } = this.settings
    const phases = this.phaseResults
    for (const { name } of this.phases) {
      const phase = phases[name]
      if (!phase) {
        violations.push(`${name}: the phase did not finish`)
        continue
      }
      if (phase.unexpected > 0) {
        violations.push(
          `sends: ${phase.unexpected} rejections in ${name} carry no outcome, the first ${this.firstUnexpected}`
        )
      }
      if (phase.probeError || !phase.probe) {
        violations.push(`admission probe (${name}): ${phase.probeError ?? 'it did not run'}`)
      } else {
        try {
          assertProbe(phase.probe)
        } catch (error) {
          violations.push(`admission probe (${name}): ${error.message.split('\n')[0].trim()}`)
        }
      }
      const retained = phase.afterDisconnect
      for (const field of ['pins', 'registrations', 'settlePromises']) {
        if (retained && retained[field] !== 0) {
          violations.push(
            `after disconnect (${name}): the producer still holds ${retained[field]} ${field}`
          )
        }
      }
    }
    const steady = phases.steady
    if (steady) {
      if (steady.offeredPerSecond < rate * targetFraction || steady.backpressure > 0) {
        violations.push(
          `steady: offered ${Math.round(steady.offeredPerSecond)} a second, target ${rate}, ` +
            `${steady.backpressure} ticks at the unsettled bound, slip up to ${Math.round(steady.slip.maxMs)} ms`
        )
      }
    }
    const outage = phases.outage
    if (outage) {
      const kinds = new Set(outage.outages.map(({ kind }) => kind))
      if (outage.outages.length === 0) {
        violations.push('outage: no broker outage was observed')
      } else if (outage.recoveries.length !== outage.outages.length) {
        violations.push(
          `outage: ${outage.outages.length} restorations, ${outage.recoveries.length} followed by an acknowledged send`
        )
      }
      for (const kind of requiredKinds) {
        if (!kinds.has(kind)) violations.push(`outage: no ${kind} outage was observed`)
      }
    }
    const pressure = phases['buffer-pressure']
    if (pressure) {
      const refused = (pressure.refusals.timeout ?? 0) + (pressure.refusals['queue-full'] ?? 0)
      if (refused === 0) {
        violations.push(
          'buffer-pressure: the full buffer refused no send with KafkaJSBufferExhaustedError'
        )
      }
      if (!pressure.pressureRecovery) {
        violations.push(
          'buffer-pressure: no normal send was acknowledged after the delay was cleared'
        )
      }
    }
    if (this.capacityReached)
      violations.push('sends: the offered-id table filled before the run ended')
    if (this.ledger.counts.pending > 0 || this.unsettled > 0) {
      violations.push(
        `sends: ${this.ledger.counts.pending} offered ids and ${this.unsettled} calls are unsettled`
      )
    }
    if (latency.samples < minLatencySamples || latency.invalid > 0) {
      violations.push(
        `latency: ${latency.samples} samples, ${minLatencySamples} are needed, and ${latency.invalid} are negative`
      )
    }
    return Object.fromEntries(
      Object.entries(phases).map(([name, { probe, ...rest }]) => [
        name,
        { ...rest, before: undefined },
      ])
    )
  }

  /**
   * Keeps the quiet points of `Soak`'s lines, then passes each line on; the resource verdict is
   * renamed, since the run's one verdict is this object's.
   * @private
   * @param {object} line
   * @returns {void}
   */
  #recordSoak(line) {
    if (line.snapshot?.named && ['baseline', 'phase-end', 'teardown'].includes(line.kind)) {
      const when =
        { baseline: 'baseline', teardown: 'after disconnect' }[line.kind] ?? `after ${line.phase}`
      this.quietPoints.push({
        when,
        named: line.snapshot.named,
        unhandled: line.snapshot.unhandled,
      })
    }
    this.record(line.kind === 'verdict' ? { ...line, kind: 'resource-verdict' } : line)
  }

  /**
   * Holds each quiet point to the baseline listener by listener, and to no unhandled rejection.
   * @private
   * @returns {string[]}
   */
  #quietPointViolations() {
    const [baseline, ...points] = this.quietPoints
    const expected = this.phases.length + 2
    if (!baseline || points.length !== expected - 1) {
      return [
        `listeners: ${this.quietPoints.length} of ${expected} quiet points carried a listener reading`,
      ]
    }
    const violations = []
    for (const { when, named, unhandled } of points) {
      for (const group of new Set([...Object.keys(baseline.named), ...Object.keys(named)])) {
        const before = baseline.named[group] ?? {}
        const now = named[group] ?? {}
        for (const event of new Set([...Object.keys(before), ...Object.keys(now)])) {
          if ((before[event] ?? 0) !== (now[event] ?? 0)) {
            violations.push(
              `listeners ${when}: ${now[event] ?? 0} on ${group} '${event}', ${before[event] ?? 0} at the start`
            )
          }
        }
      }
      if (unhandled !== 0) violations.push(`unhandled rejections ${when}: ${unhandled}`)
    }
    return violations
  }
}

/**
 * @returns {ObservationJournal} a journal in the evidence directory, next to the evidence file
 */
const openJournal = () => {
  const file = evidencePath('producer-churn').replace(/\.jsonl$/, '.journal')
  fs.mkdirSync(path.dirname(file), { recursive: true })
  return new ObservationJournal({ file })
}

/**
 * One soak run: the phases of a profile, the readings, and the verdict.
 */
class Soak {
  /**
   * @param {object} options
   * @param {object} options.kafka the client the workloads use
   * @param {ReturnType<typeof createResourceProbe>} options.probe
   * @param {Array<{ name: string, workload: keyof typeof WORKLOADS, ms: number }>} options.phases
   * @param {WorkloadContext} options.context handed to every workload
   * @param {number} [options.sampleMs=SAMPLE_MS]
   * @param {number[]} [options.sampleAt] fixed admission times, in ms after each phase began; the
   *   sampler waits for the next one instead of napping `sampleMs`, and a time already past when it
   *   looks is skipped, never taken late in a burst
   * @param {number} [options.warmupMs=WARMUP_MS] how long the heap series is left to settle; for a
   *   `judge` that reads a phase on its own, how long into each phase
   * @param {(run: object) => { ok: boolean, violations: string[] }} [options.judge=evaluate] holds
   *   the readings to the bounds: it is given `baseline`, `phaseEnds`, `samples`, `teardown` and
   *   `warmupMs`, and each sample names its phase and how long into it it was taken
   * @param {number} [options.settleMs=SETTLE_MS] how long a quiet point waits for closed sockets to go
   * @param {(record: object) => void} [options.record=writeEvidence('soak', ...)] where every line goes
   * @param {object|null} [options.sampleJournal] preallocated scalar storage with `push`/`values`;
   *   materialised only after the teardown reading, used by telemetry workers
   * @param {(snapshot: object) => object} [options.sampleSnapshot] Selects the snapshot fields kept
   *   for the judge. Evidence always receives the complete original snapshot.
   * @param {Record<string, Function>} [options.workloads=WORKLOADS]
   * @param {{now: () => number}} [options.clock] defaults to the wall clock, read on every call so a
   *   faked `Date` is honoured; telemetry uses monotonic elapsed time
   * @param {(work: Function, stage?: string) => Promise<*>} [options.transition] Bounds and charges
   *   matched quiet observations and sampler joins; ordinary profiles call through unchanged.
   * @param {() => Promise<void>} [options.beforeTeardown] runs once every phase has ended, before
   *   the sockets are awaited and the teardown reading is taken
   */
  constructor({
    kafka,
    probe,
    phases,
    context,
    sampleMs = SAMPLE_MS,
    sampleAt = null,
    sampleJournal = null,
    warmupMs = WARMUP_MS,
    judge = evaluate,
    settleMs = SETTLE_MS,
    record = (line) => writeEvidence('soak', line),
    sampleSnapshot = (snapshot) => snapshot,
    workloads = WORKLOADS,
    clock = WALL_CLOCK,
    beforeTeardown = async () => {},
    transition = (work) => work(),
  }) {
    Object.assign(this, {
      kafka,
      probe,
      phases,
      context,
      sampleMs,
      sampleAt,
      sampleJournal,
      warmupMs,
      judge,
      settleMs,
      record,
      sampleSnapshot,
      workloads,
      clock,
      beforeTeardown,
      transition,
    })
    this.samples = sampleJournal ?? []
    this.phaseEnds = []
    this.sampling = false
    this.sampler = Promise.resolve()
    this.wake = () => {}
    /** When the latest phase began, on this Soak's clock; null before the first one. */
    this.phaseStartedAt = null
    /** Fixed sample times skipped because they were already past, and readings that failed late. */
    this.skipped = []
    this.readFailures = []
  }

  /**
   * Runs every phase, reads the quiet points and judges the run. The baseline is read once the
   * sockets the setup closed are gone, so it counts only what the clients will leave behind.
   * @returns {Promise<ReturnType<typeof evaluate>>} the verdict of `judge`; the lines were
   *   recorded as they were taken
   * @throws {Error} the failure of a workload, after its sampler is stopped
   */
  async run() {
    const startedAt = this.clock.now()
    const baseline = await this.transition(async () => {
      await this.settle()
      const snapshot = await this.probe.snapshot()
      this.record({ kind: 'baseline', snapshot })
      return snapshot
    })

    for (const { name, workload, ms } of this.phases) {
      await this.runPhase({ name, workload, ms }, startedAt)
    }

    await this.beforeTeardown()
    const teardown = await this.transition(async () => {
      await this.settle()
      const snapshot = await this.probe.snapshot()
      this.record({ kind: 'teardown', snapshot })
      return snapshot
    }, 'cleanup')

    return this.transition(() => {
      const verdict = this.judge({
        baseline,
        phaseEnds: this.phaseEnds,
        samples: this.sampleJournal ? this.sampleJournal.values() : this.samples,
        teardown,
        warmupMs: this.warmupMs,
      })
      this.record({ kind: 'verdict', ...verdict })
      return verdict
    }, 'cleanup')
  }

  /**
   * Runs one workload with the sampler beside it, then takes the quiet reading.
   * @private
   * @param {{ name: string, workload: string, ms: number }} phase
   * @param {number} startedAt when the run began, for the sample times
   * @returns {Promise<void>}
   * @throws {Error} the failure of the workload, or a failed sample or evidence record, after the
   *   phase's workload has ended and its clients are disconnected
   */
  async runPhase({ name, workload, ms }, startedAt) {
    const phaseStartedAt = this.clock.now()
    this.phaseStartedAt = phaseStartedAt
    this.sampling = true
    const sampler = (async () => {
      let planned = 0
      while (this.sampling) {
        let delayMs = this.sampleMs
        if (this.sampleAt) {
          const elapsed = this.clock.now() - phaseStartedAt
          while (planned < this.sampleAt.length && this.sampleAt[planned] < elapsed) {
            this.skipped.push(this.sampleAt[planned])
            planned += 1
          }
          delayMs = planned < this.sampleAt.length ? this.sampleAt[planned] - elapsed : 2 ** 31 - 1
        }
        await this.nap(delayMs)
        while (this.sampling && this.sampleAt && planned < this.sampleAt.length) {
          const remaining = this.sampleAt[planned] - (this.clock.now() - phaseStartedAt)
          if (remaining <= 0) break
          await this.nap(remaining)
        }
        if (this.sampling) {
          const admitted = this.clock.now()
          let snapshot
          try {
            snapshot = await this.probe.snapshot()
          } catch (error) {
            this.readFailures.push({
              admittedPhaseMs: admitted - phaseStartedAt,
              message: String(error?.message ?? error),
            })
            throw error
          }
          const now = this.clock.now()
          const sample = {
            phase: name,
            atMs: now - startedAt,
            phaseMs: now - phaseStartedAt,
            admittedAtMs: admitted - startedAt,
            admittedPhaseMs: admitted - phaseStartedAt,
            snapshot,
          }
          if (this.sampleAt) planned += 1
          this.samples.push({ ...sample, snapshot: this.sampleSnapshot(snapshot) })
          this.record({ kind: 'sample', ...sample })
        }
      }
    })()
    sampler.catch(() => {})
    this.sampler = sampler

    try {
      await this.workloads[workload](this.context, ms)
    } finally {
      this.sampling = false
      this.wake()
      await this.transition(() => sampler)
    }

    await this.transition(async () => {
      await this.settle()
      const snapshot = await this.probe.snapshot()
      this.phaseEnds.push({ phase: name, snapshot })
      this.record({ kind: 'phase-end', phase: name, snapshot })
    })
  }

  /**
   * Stops the sampler of the running phase and joins it, so no later reading is admitted. A
   * workload may call it at its measurement boundary, before its own cleanup; the phase then
   * stops and joins the same sampler again when the workload ends.
   * @returns {Promise<void>} settles once the sampler has stopped; a failed reading rejects
   */
  endSampling() {
    this.sampling = false
    this.wake()
    return this.sampler
  }

  /**
   * Waits one sample interval, or until `wake()` ends the wait, and leaves no timer behind.
   * @private
   * @param {number} [ms=this.sampleMs] how long to wait
   * @returns {Promise<void>}
   */
  nap(ms = this.sampleMs) {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms)
      this.wake = () => {
        clearTimeout(timer)
        resolve()
      }
    })
  }

  /**
   * Waits for the sockets the clients closed to be gone.
   * @private
   * @returns {Promise<void>}
   */
  async settle() {
    const deadline = this.clock.now() + this.settleMs
    while (this.clock.now() < deadline && (await this.probe.snapshot()).sockets.open > 0) {
      await sleep(100)
    }
  }
}

/**
 * @param {string} profile a key of `PROFILES`
 * @param {number|null} phaseSeconds
 * @returns {Array<{ name: string, workload: string, ms: number }>} the profile's phases, every one
 *   shortened to `phaseSeconds` for a smoke run
 */
const phasesOf = (profile, phaseSeconds) =>
  PROFILES[profile].map((phase) =>
    phaseSeconds === null ? phase : { ...phase, ms: phaseSeconds * 1000 }
  )

/**
 * @param {ReturnType<typeof createChaosSocketFactory>} chaos
 * @param {object} [options] Socket inventory ownership.
 * @param {boolean} [options.releaseOnClose=false] Release each closed socket immediately, so a
 *   share observer's collection does not depend on another connection being created.
 * @returns {Function} a socket factory that makes chaos sockets and first forgets the destroyed
 *   ones, so the list of sockets a long outage reconnects through does not grow
 */
const forgettingDestroyed =
  (chaos, { releaseOnClose = false } = {}) =>
  (args) => {
    chaos.sockets = chaos.sockets.filter(({ destroyed }) => !destroyed)
    const socket = chaos(args)
    if (releaseOnClose) {
      socket.once('close', () => {
        chaos.sockets = chaos.sockets.filter((known) => known !== socket)
      })
    }
    return socket
  }

/**
 * Builds the producer-churn owner for a real cluster: the counted client over a chaos socket
 * factory, the reader on the same counted factory, the brokers' bouncer and the closing probe. Every
 * evidence line goes to `.tmp/e2e/producer-churn.jsonl`.
 * @param {object} options
 * @param {string[]} options.brokers
 * @param {number|null} options.phaseSeconds a smoke run's phase length; its sampling is then
 *   shortened and the latency and outage-kind requirements of a full run are not asked
 * @param {Array<{ name: string, workload: string, ms: number }>} options.phases
 * @returns {ProducerChurn}
 */
const buildProducerChurn = ({ brokers, phaseSeconds, phases }) => {
  const { BrokerBouncer } = require('../../testHelpers/leaderElection')
  const smoke = phaseSeconds !== null
  const topic = `soak-churn-${Date.now()}`
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  const { kafka, probe, socketFactory, listeners } = instrument({
    clientId: 'soak',
    brokers,
    logLevel: logLevel.NOTHING,
    socketFactory: forgettingDestroyed(chaos),
  })
  const record = (line) => writeEvidence('producer-churn', line)
  const reader = new RawProducerReader({ brokers, topic, record, socketFactory })

  return new ProducerChurn({
    kafka,
    probe,
    brokers,
    topic,
    record,
    phases,
    listeners,
    reader,
    bouncer: new BrokerBouncer(),
    faults: new WriteDelay({ chaos, reader }),
    admissionProbe: new ClosingProbe({
      brokers,
      clientId: 'soak',
      topic: `${topic}-probe`,
      capacity: CHURN.producer.bufferMemory,
      maxRequestSize: CHURN.producer.maxRequestSize,
      budgetMs: CHURN.producer.deliveryTimeout,
    }),
    soak: smoke ? { sampleMs: Math.max(1000, (phaseSeconds * 1000) / 6), warmupMs: 0 } : {},
    settings: smoke ? { minLatencySamples: 1, minHeapSamples: 2, requiredKinds: [] } : {},
  })
}

/**
 * Builds the owner of the consumer-churn profile: a public client whose sockets the probe counts,
 * the journal on disk and the four phase workloads. Nothing here imports the client's sources or
 * reads its private members.
 * @param {object} options
 * @param {string[]} options.brokers
 * @param {number|null} options.phaseSeconds a smoke run's phase length; such a run is never acceptance
 * @returns {import('./consumerChurnRun').ConsumerChurnRun}
 */
const buildConsumerChurn = ({ brokers, phaseSeconds }) => {
  const {
    ChurnJournal,
    GapSampler,
    PublicConsumerProbe,
    ChurnClientLog,
    consumerChurnPlan,
    publicSocketFactory,
  } = require('./consumerChurn')
  const { ChurnWorkloads, ConsumerChurnRun } = require('./consumerChurnRun')
  const v8 = require('node:v8')
  const heap = async () => {
    globalThis.gc()
    await sleep(100)
    globalThis.gc()
    return v8.getHeapStatistics().used_heap_size
  }
  const probe = new PublicConsumerProbe({ heap })
  const file = evidencePath('consumer-churn').replace(/\.jsonl$/, '.journal')
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const clientLog = new ChurnClientLog({
    file: evidencePath('consumer-churn').replace(/\.jsonl$/, '.clientlog'),
  })
  const kafka = new Kafka({
    clientId: 'soak-consumer-churn',
    brokers,
    logLevel: logLevel.WARN,
    logCreator: clientLog.creator(),
    socketFactory: probe.wrap(publicSocketFactory()),
  })
  const workloads = new ChurnWorkloads({
    kafka,
    probe,
    tag: `cc${Date.now().toString(36)}`,
    clientLog,
  })
  return new ConsumerChurnRun({
    workloads,
    scope: workloads.groups,
    probe,
    journal: new ChurnJournal({ file }),
    clientLog,
    record: (line) => writeEvidence('consumer-churn', line),
    plan: consumerChurnPlan(phaseSeconds),
    clock: REAL_CLOCK,
    sleep,
    gaps: new GapSampler(phaseSeconds === null ? {} : { minimum: 1 }),
    heap,
  })
}

/**
 * Reduces the listener tables of `instrument` to one flat map, `<owner>:<event>`, so a swapped
 * listener shows where a total would not.
 * @param {Record<string, Record<string, number>>} listeners listener counts by owner and event
 * @returns {Record<string, number>}
 */
const flattenListeners = (listeners) =>
  Object.fromEntries(
    Object.entries(listeners).flatMap(([owner, events]) =>
      Object.entries(events).map(([event, count]) => [`${owner}:${event}`, count])
    )
  )

/**
 * Fit post-warm-up heap samples separately inside each scored phase. Independently compare the
 * three disconnected phase-end heaps using their original monotonic observation times, with an
 * additional 98304-byte/minute slope limit. Keep the existing quiet-heap endpoint, resource,
 * source-validity and minimum-sample checks. Do not fit active samples from different phases as
 * one working set.
 */
class ShareSoakScreen {
  /**
   * Judges one share run inside each phase and across the phase-end quiet points. Samples use the
   * phase clock of the driver, `phaseMs`.
   * @param {object} run baseline, phase samples, quiet points and the final teardown
   * @param {object} run.baseline quiet snapshot taken before the profile's clients
   * @param {Array<{ phase: string, snapshot: object }>} run.phaseEnds one quiet point per phase
   * @param {Array<{ phase: string, phaseMs: number, snapshot: object }>} run.samples periodic readings
   * @param {object} run.teardown snapshot after the last disconnect
   * @param {number} run.warmupMs how far into a phase its heap samples start to count
   * @returns {{ ok: boolean, violations: string[], measured: object }} the resource and per-phase
   *   heap verdicts; never a delivery oracle
   * @throws {Error} when a heap slope has fewer than two different times
   */
  static evaluate(run) {
    const violations = []
    const phases = []
    ShareSoakScreen.quiet(run.baseline, run.baseline, 'baseline', violations)
    for (const end of run.phaseEnds) {
      ShareSoakScreen.quiet(end.snapshot, run.baseline, end.phase, violations)
      const phase = ShareSoakScreen.phase(end.phase, run, violations)
      if (phase) phases.push(phase)
    }
    ShareSoakScreen.quiet(run.teardown, run.baseline, 'teardown', violations)
    const quiet = [...run.phaseEnds.map((end) => end.snapshot?.heapUsed), run.teardown?.heapUsed]
    if (
      run.phaseEnds.length !== SHARE_SOAK.phases.length ||
      !quiet.every(ShareSoakScreen.count) ||
      quiet.some((heap) => heap - quiet[0] > SHARE_SOAK.endpointBytes)
    ) {
      violations.push('phase-end quiet heap screen')
    }

    const quietSlope = ShareSoakScreen.quietSlope(run, violations)
    return {
      ok: violations.length === 0,
      violations,
      measured: { phases, quietHeap: quiet, quietSlope },
    }
  }

  /**
   * Fits only the three ordered disconnected phase ends on the original owner's elapsed clock.
   * Baseline and teardown must surround them, but neither belongs to the fitted working set.
   * @param {object} run original baseline, phase ends and teardown
   * @param {string[]} violations receives a failure for invalid sources, chronology or slope
   * @returns {{ points: object[], bytesPerMinute: number|null }} original points even on failure
   */
  static quietSlopeObservations(run, violations) {
    const points = run.phaseEnds.map(({ phase, snapshot }) => ({
      phase,
      heapAtMs: snapshot?.heapAtMs ?? null,
      heapUsed: snapshot?.heapUsed ?? null,
    }))
    const observations = [run.baseline, ...run.phaseEnds.map((end) => end.snapshot), run.teardown]
    const valid =
      points.length === SHARE_SOAK.phases.length &&
      points.every(({ phase }, index) => phase === `share-${SHARE_SOAK.phases[index]}`) &&
      observations.every(
        (point, index) =>
          ShareSoakScreen.count(point?.heapUsed) &&
          Number.isFinite(point?.heapAtMs) &&
          point.heapAtMs >= 0 &&
          (index === 0 || point.heapAtMs > observations[index - 1].heapAtMs)
      )
    let bytesPerMinute = null
    if (valid) {
      try {
        bytesPerMinute = slope(points.map(({ heapAtMs, heapUsed }) => [heapAtMs / 60000, heapUsed]))
      } catch {}
    }
    if (bytesPerMinute === null || !Number.isFinite(bytesPerMinute)) {
      violations.push('phase-end quiet heap slope screen')
    }
    return { points, bytesPerMinute: Number.isFinite(bytesPerMinute) ? bytesPerMinute : null }
  }

  /**
   * Applies the scored quiet growth limit to the original elapsed-time fit.
   * @param {object} run Original quiet observations.
   * @param {string[]} violations Original failure list.
   * @returns {object} Original points and measured slope.
   */
  static quietSlope(run, violations) {
    const measured = ShareSoakScreen.quietSlopeObservations(run, violations)
    if (measured.bytesPerMinute > SHARE_SOAK.quietSlopeBytesPerMinute)
      violations.push('phase-end quiet heap slope screen')
    return measured
  }

  /**
   * Validates every first-use source and resource while archiving growth measurements separately.
   * Only growth predicates are measurement-only; invalid readings and missing samples still fail.
   * @param {object} run Original first-use observations.
   * @returns {object} Structural verdict with the complete measured scored growth assessment.
   * @throws {Error} Invalid original regression times.
   */
  static priming(run) {
    const violations = []
    ShareSoakScreen.quiet(run.baseline, run.baseline, 'baseline', violations)
    for (const end of run.phaseEnds) {
      ShareSoakScreen.quiet(end.snapshot, run.baseline, end.phase, violations)
      ShareSoakScreen.phaseObservations(end.phase, run, violations)
    }
    ShareSoakScreen.quiet(run.teardown, run.baseline, 'teardown', violations)
    ShareSoakScreen.quietSlopeObservations(run, violations)
    for (const point of [run.baseline, ...run.phaseEnds.map((end) => end.snapshot), run.teardown]) {
      for (const when of ['beforeCollection', 'afterCollection']) {
        const owners = point?.timerOwners?.[when]
        if (
          !owners ||
          !Array.isArray(owners.rows) ||
          owners.dropped !== 0 ||
          !ShareSoakScreen.same(
            owners.unattributed,
            run.baseline?.timerOwners?.[when]?.unattributed
          )
        )
          violations.push('priming: missing or changed timer inventory source')
      }
    }
    if (
      run.samples.some(
        (sample) =>
          !ShareSoakScreen.count(sample.snapshot?.heapUsed) ||
          !Number.isFinite(sample.phaseMs) ||
          !Number.isFinite(sample.atMs)
      )
    )
      violations.push('priming: invalid periodic source')
    return {
      ok: violations.length === 0,
      violations,
      measurementOnly: ShareSoakScreen.evaluate(run),
    }
  }

  /**
   * @param {*} value
   * @returns {boolean} whether it is a measured, nonnegative whole number
   */
  static count(value) {
    return Number.isSafeInteger(value) && value >= 0
  }

  /**
   * @param {Record<string, number>|undefined} actual
   * @param {Record<string, number>|undefined} expected
   * @returns {boolean} whether both exist and agree on every name, an absent name meaning zero
   */
  static same(actual, expected) {
    if (!actual || !expected) return false
    return [...new Set([...Object.keys(actual), ...Object.keys(expected)])].every(
      (name) =>
        ShareSoakScreen.count(actual[name] ?? 0) &&
        ShareSoakScreen.count(expected[name] ?? 0) &&
        (actual[name] ?? 0) === (expected[name] ?? 0)
    )
  }

  /**
   * Checks one quiet point: nothing held, nothing open, and every resource type and named listener
   * back at the baseline's count.
   * @param {object|undefined} point snapshot at the quiet point
   * @param {object|undefined} baseline snapshot before the clients
   * @param {string} label how a violation names the point
   * @param {string[]} violations receives one line when the point differs
   * @returns {void}
   */
  static quiet(point, baseline, label, violations) {
    const held = ['queueDepth', 'shareSessions', 'shareRecords', 'shareRanges']
    const expectedResources = ShareTimerObserver.comparableResources(
      baseline?.activeResources,
      baseline?.timerOwners?.afterCollection
    )
    const resources = {
      activeResources: ShareTimerObserver.comparableResources(
        point?.activeResources,
        point?.timerOwners?.afterCollection
      ),
      activeResourcesBeforeCollection: ShareTimerObserver.comparableResources(
        point?.activeResourcesBeforeCollection,
        point?.timerOwners?.beforeCollection
      ),
    }
    if (
      !point ||
      held.some((name) => !ShareSoakScreen.count(point[name]) || point[name] !== 0) ||
      !ShareSoakScreen.count(point.sockets?.open) ||
      point.sockets.open !== 0 ||
      !ShareSoakScreen.same(resources.activeResources, expectedResources) ||
      !ShareSoakScreen.same(resources.activeResourcesBeforeCollection, expectedResources) ||
      Object.values(point.timerOwners ?? {}).some(({ dropped }) => dropped !== 0) ||
      !ShareSoakScreen.same(point.namedListeners, baseline?.namedListeners)
    ) {
      violations.push(`${label}: resources differ or a source is absent`)
      const report = (source, before, after) =>
        violations.push(
          `${label}: ${source} baseline=${JSON.stringify(before) ?? 'absent'} after=${JSON.stringify(after) ?? 'absent'}`
        )
      for (const name of held)
        if (!ShareSoakScreen.count(point?.[name]) || point[name] !== 0)
          report(name, baseline?.[name], point?.[name])
      if (!ShareSoakScreen.count(point?.sockets?.open) || point.sockets.open !== 0)
        report('sockets.open', baseline?.sockets?.open, point?.sockets?.open)
      for (const source of [
        'activeResources',
        'activeResourcesBeforeCollection',
        'namedListeners',
      ]) {
        const before = source === 'namedListeners' ? baseline?.namedListeners : expectedResources
        const after = source === 'namedListeners' ? point?.namedListeners : resources[source]
        if (!before || !after) report(source, before, after)
        else
          for (const name of new Set([...Object.keys(before), ...Object.keys(after)]))
            if (
              !ShareSoakScreen.count(before[name] ?? 0) ||
              !ShareSoakScreen.count(after[name] ?? 0) ||
              (before[name] ?? 0) !== (after[name] ?? 0)
            )
              report(`${source}.${name}`, before[name] ?? 0, after[name] ?? 0)
      }
      for (const [when, owners] of Object.entries(point?.timerOwners ?? {}))
        if (owners.dropped !== 0) report(`timerOwners.${when}.dropped`, 0, owners.dropped)
    }
  }

  /**
   * Screens the heap and the held-state ceilings of one phase.
   * @param {string} name phase name
   * @param {object} run the run being judged
   * @param {string[]} violations receives every bound that broke
   * @returns {{ phase: string, heapSamples: number, endpointBytes: number,
   *   slopeBytesPerMinute: number }|null} the phase's reading, null when it had too few samples
   */
  static phaseObservations(name, run, violations) {
    const readings = run.samples.filter(
      (sample) => sample.phase === name && sample.phaseMs >= run.warmupMs
    )
    const heap = readings.map((sample) => sample.snapshot.heapUsed)
    if (heap.length < MIN_PHASE_SAMPLES || !heap.every(ShareSoakScreen.count)) {
      violations.push(`${name}: six post-warm-up heap samples are required`)
      return null
    }
    for (const { snapshot } of readings) {
      if (
        !ShareSoakScreen.count(snapshot.shareRecords) ||
        snapshot.shareRecords > SHARE_SOAK.maxRecords * SHARE_SOAK.nodes
      ) {
        violations.push(`${name}: held-record ceiling`)
      }
      if (
        !ShareSoakScreen.count(snapshot.shareRanges) ||
        !ShareSoakScreen.count(snapshot.shareSessions)
      ) {
        violations.push(`${name}: missing compact-state measurement`)
      }
    }
    const endpointBytes = median(heap.slice(-3)) - median(heap.slice(0, 3))
    const slopeBytesPerMinute = slope(
      readings.map((sample) => [sample.phaseMs / 60000, sample.snapshot.heapUsed])
    )
    return { phase: name, heapSamples: heap.length, endpointBytes, slopeBytesPerMinute }
  }

  /**
   * Applies the scored growth predicates to the original structural phase measurement.
   * @param {string} name Original phase.
   * @param {object} run Original source observations.
   * @param {string[]} violations Original failure list.
   * @returns {object|null} Measurement, including failures.
   * @throws {Error} Invalid original regression times.
   */
  static phase(name, run, violations) {
    const measured = ShareSoakScreen.phaseObservations(name, run, violations)
    if (!measured) return null
    const { endpointBytes, slopeBytesPerMinute } = measured
    if (
      endpointBytes > SHARE_SOAK.endpointBytes ||
      slopeBytesPerMinute > SHARE_SOAK.slopeBytesPerMinute
    ) {
      violations.push(`${name}: retained-heap screen`)
    }

    return measured
  }
}

/**
 * Reads what the original share owners of this process retain. It sees them through call-through
 * wrappers that only remember each receiver, so no argument, decision or result changes, and it
 * holds every owner weakly, so reading never keeps a disposed run, ledger or emitter alive. It adds
 * nothing to the fetch, dispatch or settlement paths beyond one set lookup per wrapped call, and
 * the wrapped methods run once per run or per listener, never per record.
 */
class ShareStateObserver {
  /**
   * @param {object} [owners] the classes to watch; a spec supplies fakes
   * @param {Function} [owners.ShareRun] the run, which owns the per-node session map
   * @param {Function} [owners.ShareAckLedger] the acquisition ledger, which must offer `counts`
   * @param {Function} [owners.InstrumentationEventEmitter] the client event bus
   */
  constructor(owners = ShareStateObserver.originals()) {
    this.owners = owners
    this.runs = []
    this.seenRuns = new WeakSet()
    this.loops = []
    this.seenLoops = new WeakSet()
    this.ledgers = []
    this.seenLedgers = new WeakSet()
    this.queues = []
    this.seenQueues = new WeakSet()
    this.emitters = []
    this.seenEmitters = new WeakSet()
    this.ids = new WeakMap()
    this.nextId = 1
    this.tracked = new Set()
    this.restores = []
  }

  /** @returns {object} the real classes, loaded only when a share soak is built */
  static originals() {
    return {
      ShareRun: require('../../src/consumer/share/shareRun').ShareRun,
      ShareFetchLoop: require('../../src/consumer/share/shareFetchLoop').ShareFetchLoop,
      RequestQueue: require('../../src/network/requestQueue'),
      ShareAckLedger: require('../../src/consumer/share/shareAckLedger').ShareAckLedger,
      InstrumentationEventEmitter: require('../../src/instrumentation/emitter'),
    }
  }

  /**
   * Starts remembering original runs and client emitters.
   * @returns {void}
   * @throws {Error} when the ledger offers no read-only `counts` accessor: record and range counts
   *   are never derived from the total size, so the source must exist before anything is wrapped
   */
  install() {
    const { ShareRun, ShareAckLedger, InstrumentationEventEmitter } = this.owners
    if (
      typeof Object.getOwnPropertyDescriptor(ShareAckLedger.prototype, 'counts')?.get !== 'function'
    ) {
      throw new Error('Missing share resource source: ShareAckLedger#counts')
    }
    if (this.owners.ShareFetchLoop) {
      this.watch(
        this.owners.ShareFetchLoop.prototype,
        ['adopt', 'dispose'],
        this.seenLoops,
        this.loops
      )
    }
    this.watch(
      ShareAckLedger.prototype,
      ['prepare', 'invalidate', 'settle'],
      this.seenLedgers,
      this.ledgers
    )
    if (this.owners.RequestQueue) {
      this.watch(
        this.owners.RequestQueue.prototype,
        ['push', 'destroy', 'scheduleRequestTimeoutCheck'],
        this.seenQueues,
        this.queues
      )
    }
    this.watch(ShareRun.prototype, ['start', 'syncNodes'], this.seenRuns, this.runs)
    this.watch(
      InstrumentationEventEmitter.prototype,
      ['addListener'],
      this.seenEmitters,
      this.emitters
    )
  }

  /**
   * Wraps methods so each receiver is remembered once, weakly, and the call goes through unchanged.
   * @param {object} prototype the class prototype
   * @param {string[]} names the methods to wrap
   * @param {WeakSet<object>} seen receivers already remembered
   * @param {WeakRef<object>[]} refs where a new receiver is remembered
   * @returns {void}
   */
  watch(prototype, names, seen, refs) {
    for (const name of names) {
      const original = prototype[name]
      if (typeof original !== 'function') continue
      const observer = this
      const wrapper = function (...args) {
        if (!seen.has(this)) {
          seen.add(this)
          refs.push(new WeakRef(this))
        }
        observer.discover()
        const result = original.apply(this, args)
        observer.discover()
        return result
      }
      prototype[name] = wrapper
      this.restores.push(() => {
        if (prototype[name] === wrapper) prototype[name] = original
        return prototype[name] === original
      })
    }
  }

  /**
   * Puts every wrapped method back.
   * @returns {void}
   * @throws {Error} when a method could not be restored, because something else wrapped it since
   */
  restore() {
    const failed = this.restores
      .splice(0)
      .reverse()
      .map((restore) => restore())
      .includes(false)
    this.runs.length = 0
    this.emitters.length = 0
    this.loops.length = 0
    this.ledgers.length = 0
    this.queues.length = 0
    this.tracked.clear()
    if (failed) throw new Error('A share observer wrapper could not be restored')
  }

  /**
   * @param {WeakRef<object>[]} refs remembered owners
   * @returns {object[]} the ones still alive, in the order they were first seen; collected ones
   *   are forgotten
   */
  live(refs) {
    const owners = []
    for (let i = 0; i < refs.length;) {
      const owner = refs[i].deref()
      if (owner === undefined) {
        refs.splice(i, 1)
      } else {
        owners.push(owner)
        i += 1
      }
    }
    return owners
  }

  /**
   * Counts the state the original runs retain right now: their node-session map entries and, from
   * each node's acquisitions, the delivered-record and compact-range entries of their ledgers.
   * @returns {{ shareSessions: number, shareRecords: number, shareRanges: number }}
   * @throws {Error} when a run, node or ledger does not show the source it should
   */
  counts() {
    this.discover()
    const counts = { shareSessions: 0, shareRecords: 0, shareRanges: 0 }
    for (const run of this.live(this.runs)) {
      if (!(run.nodes instanceof Map))
        throw new Error('Missing share resource source: ShareRun#nodes')
      counts.shareSessions += run.nodes.size
    }
    for (const ledger of this.live(this.ledgers)) {
      const { records, ranges } = ledger.counts ?? {}
      if (
        !Number.isSafeInteger(records) ||
        records < 0 ||
        !Number.isSafeInteger(ranges) ||
        ranges < 0
      )
        throw new Error('Missing share resource source: ShareAckLedger#counts')
      counts.shareRecords += records
      counts.shareRanges += ranges
    }

    return counts
  }

  /**
   * Remembers original loops and ledgers before their ownership maps can detach them.
   * @returns {void}
   */
  discover() {
    const remember = (owner, seen, refs) => {
      if (owner && !seen.has(owner)) {
        seen.add(owner)
        refs.push(new WeakRef(owner))
      }
    }
    for (const run of this.live(this.runs)) {
      if (!(run.nodes instanceof Map)) continue
      for (const node of run.nodes.values()) remember(node.loop, this.seenLoops, this.loops)
    }
    for (const loop of this.live(this.loops)) {
      if (!(loop.cohorts instanceof Map))
        throw new Error('Missing share resource source: ShareFetchLoop#cohorts')
      for (const rows of loop.cohorts.values())
        for (const { ledger } of rows) remember(ledger, this.seenLedgers, this.ledgers)
    }
  }

  /**
   * @returns {Record<string, number>} the listeners of every live client emitter by stable emitter
   *   id and event, so a swapped listener changes the map where a total would not
   */
  named() {
    const named = {}
    for (const holder of this.live(this.emitters)) {
      if (!this.ids.has(holder)) this.ids.set(holder, this.nextId++)
      for (const event of holder.emitter.eventNames()) {
        const count = holder.emitter.listenerCount(event)
        if (count > 0) named[`client${this.ids.get(holder)}:${String(event)}`] = count
      }
    }

    return named
  }

  /**
   * Tracks a public share client until it reports its disconnect.
   * @param {{ on: Function, events: Record<string, string> }} client a public share consumer
   * @returns {object} the same client
   */
  track(client) {
    this.tracked.add(client)
    const off = client.on(client.events.DISCONNECT, () => {
      this.tracked.delete(client)
      off()
    })

    return client
  }
}

/**
 * Builds a client whose resources, including the share owners' retained state, are measured. It is
 * `instrument` plus the actual session, delivered-record and compact-range counts, read from the
 * original owners.
 * @param {object} options `Kafka` options, as for `instrument`
 * @param {object} [readings] as for `instrument`
 * @param {Function} [readings.gc=globalThis.gc] collection for share-only settled observations
 * @param {Function} [readings.pause] the collection gap; defaults to the real sleep
 * @returns {ReturnType<typeof instrument> & { shareResources: { track: (client: object) => object,
 *   snapshot: () => Promise<object>, restore: () => void } }} `snapshot` adds the tracked client
 *   count and the flattened named listeners to the probe's reading; `restore` removes the observer
 * @throws {Error} when a source of the share counts is missing
 */
const instrumentShare = (options, { gc = globalThis.gc, pause, ...readings } = {}) => {
  const observer = new ShareStateObserver()
  const timers = new ShareTimerObserver()
  try {
    timers.install()
    observer.install()
    const instrumented = instrument(options, {
      ...readings,
      heap: false,
      originalQueues: () => observer.live(observer.queues),
      shareState: () => observer.counts(),
    })
    const resources = createResourceProbe({ activeResources: readings.activeResources ?? true })
    const heap = createResourceProbe({ heap: typeof gc === 'function', gc, pause })
    const shareResources = {
      track: (client) => observer.track(client),
      snapshot: async () => {
        // Prune without dereferencing weak owners; all owner readings belong after collection.
        instrumented.pruneSockets()
        const before = resources.snapshot()
        const beforeOwners = timers.snapshot()
        const { activeResources: activeResourcesBeforeCollection } = await before
        const settled = await heap.snapshot()
        // These synchronous reads share one observation point, including identities and endpoints.
        const reading = instrumented.probe.snapshot()
        const namedListeners = {
          ...flattenListeners(instrumented.listeners()),
          ...observer.named(),
        }
        const shareClients = observer.tracked.size
        const afterOwners = timers.snapshot()
        const snapshot = await reading
        return {
          ...settled,
          ...snapshot,
          shareClients,
          namedListeners,
          activeResourcesBeforeCollection,
          timerOwners: {
            beforeCollection: ShareTimerObserver.evidence(
              beforeOwners,
              activeResourcesBeforeCollection
            ),
            afterCollection: ShareTimerObserver.evidence(afterOwners, snapshot.activeResources),
          },
        }
      },
      restore: () => {
        timers.restore()
        observer.restore()
      },
    }

    return { ...instrumented, shareResources }
  } catch (error) {
    timers.restore()
    observer.restore()
    throw error
  }
}

/** The title every share soak case carries, so the guard verifies one common identity. */
const SHARE_SOAK_TITLE = 'preserves share identities and resources through the short soak'

/**
 * The independent oracles of one cycle, as pure functions of what the cycle observed: the producer
 * acknowledgements, the raw log, the handler journal, the public acknowledgements, the broker's
 * terminal positions and the membership and reset facts. None of them reads another's output, so
 * a corrupted observation fails the check that owns it.
 */
class ShareCycleOracle {
  /**
   * @param {number} cycle zero-based cycle, 0 to 29
   * @returns {{ cycle: number, phase: string, reset: boolean, records: object[],
   *   perPartition: number, first: bigint, end: string }} the fixed corpus of the cycle: sixty
   *   records with explicit partitions `i % 3` and the immutable sequence `cycle * 60 + i`, and
   *   the cumulative per-partition log end the cycle must reach
   */
  static plan(cycle) {
    const { recordsPerCycle, partitions, cyclesPerPhase, phases } = SHARE_SOAK
    const kind = phases[Math.floor(cycle / cyclesPerPhase)]
    const perPartition = recordsPerCycle / partitions
    return {
      cycle,
      phase: `share-${kind}`,
      reset: kind === 'reset',
      records: Array.from({ length: recordsPerCycle }, (_, i) => {
        const sequence = cycle * recordsPerCycle + i
        return {
          sequence,
          partition: i % partitions,
          key: `soak-key-${sequence}`,
          value: `soak-value-${sequence}`,
        }
      }),
      perPartition,
      first: BigInt(perPartition) * BigInt(cycle),
      end: String(perPartition * (cycle + 1)),
    }
  }

  /**
   * Checks one cycle's observations against its plan.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle's fixed corpus
   * @param {object} observation what the cycle saw
   * @param {Array<{ partition: number, identity: string }>} observation.produced acknowledged records
   * @param {string[]} observation.log identities of this cycle's offsets in the raw log
   * @param {Record<number, { end: string, count: number }>} observation.logs each partition's raw
   *   log end and record count
   * @param {Array<{ identity: string, deliveryCount: number }>} observation.handler handler entries
   * @param {string[]} observation.held identities started and held when the reset was forced
   * @param {Array<{ partition: number, firstOffset: string, lastOffset: string }>} observation.accepted
   *   successful public Accept results
   * @param {object[]} observation.broker the three terminal partition rows
   * @param {object} observation.members join, leave, additional member add and remove facts
   * @param {object|null} observation.reset the observed reset and reopen, or null
   * @param {{ active: boolean, delivered: string[] }} observation.verifier the same-group verifier
   * @param {boolean} observation.clientsDisconnected whether every cycle client left
   * @returns {string[]} one line for every check that failed; empty when the cycle holds
   */
  static judge(plan, observation) {
    const violations = []
    const fail = (message) => violations.push(`cycle ${plan.cycle}: ${message}`)
    const corpus = ShareCycleOracle.corpus(plan, observation.produced ?? [], fail)
    ShareCycleOracle.log(plan, corpus, observation, fail)
    ShareCycleOracle.handler(plan, corpus, observation, fail)
    ShareCycleOracle.accepts(corpus, observation, fail)
    ShareCycleOracle.broker(plan, observation.broker ?? [], fail)
    ShareCycleOracle.membership(plan, observation, fail)
    if (observation.positions?.length !== 2)
      fail('both terminal position observations are required')
    for (const position of observation.positions ?? []) {
      ShareCycleOracle.position(plan, position, fail)
      if (
        position.topicId !== observation.verifier?.topicId ||
        position.target?.groupId !== observation.verifier?.groupId ||
        position.target?.topic !== JSON.parse(observation.produced?.[0]?.identity ?? '[]')[0]
      )
        fail('the terminal proof belongs to another group or topic')
    }
    ShareCycleOracle.verifier(observation.verifier, SHARE_SOAK.quietMs.cycle, fail)
    ShareCycleOracle.sequence(observation, fail)
    if (observation.timing?.cycle !== plan.cycle || observation.timing?.kind !== 'cycle')
      fail('the proof belongs to another cycle')

    return violations
  }

  /**
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {Array<{ identity: string }>} produced acknowledged records in send order
   * @param {(message: string) => void} fail
   * @returns {Map<string, { partition: number, offset: bigint }>} the acknowledged identities
   */
  static corpus(plan, produced, fail) {
    const corpus = new Map()
    const next = new Map()
    if (produced.length !== plan.records.length) {
      fail(`${produced.length} acknowledged records, ${plan.records.length} expected`)
    }
    plan.records.forEach((record, i) => {
      let tuple
      try {
        tuple = JSON.parse(produced[i]?.identity)
      } catch {
        fail(`acknowledged record ${i} has no identity`)
        return
      }
      const [, partition, offset, key, value] = tuple
      const index = next.get(record.partition) ?? 0
      next.set(record.partition, index + 1)
      const hex = (text) => Buffer.from(text).toString('hex')
      if (
        partition !== record.partition ||
        key !== hex(record.key) ||
        value !== hex(record.value) ||
        offset !== String(plan.first + BigInt(index))
      ) {
        fail(`acknowledged record ${i} differs from the corpus`)
      }
      if (corpus.has(produced[i].identity)) fail(`acknowledged record ${i} repeats an identity`)
      corpus.set(produced[i].identity, { partition: record.partition, offset: BigInt(offset) })
    })

    return corpus
  }

  /**
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {Map<string, object>} corpus the acknowledged identities
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static log(plan, corpus, { log = [], logs = {}, prefix = [], acknowledgedPrefix = [] }, fail) {
    if (
      acknowledgedPrefix.length !== (plan.cycle + 1) * SHARE_SOAK.recordsPerCycle ||
      new Set(acknowledgedPrefix).size !== acknowledgedPrefix.length ||
      JSON.stringify([...prefix].sort()) !== JSON.stringify([...acknowledgedPrefix].sort())
    )
      fail('the cumulative raw log identities differ from the acknowledged prefix')
    const once = new Map()
    for (const identity of log) once.set(identity, (once.get(identity) ?? 0) + 1)
    if (log.length !== corpus.size || [...corpus.keys()].some((id) => once.get(id) !== 1)) {
      fail('the raw log does not hold the acknowledged corpus exactly once')
    }
    for (let partition = 0; partition < SHARE_SOAK.partitions; partition++) {
      const { end, count } = logs[partition] ?? {}
      if (end !== plan.end || count !== Number(plan.end)) {
        fail(`partition ${partition} raw log is not the cumulative prefix ending at ${plan.end}`)
      }
    }
  }

  /**
   * Every acknowledged identity reaches a handler, and a repeat is allowed only for an identity of
   * the held set frozen before the reset, with a strictly higher delivery count each time.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {Map<string, object>} corpus the acknowledged identities
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static handler(plan, corpus, { handler = [], held = [], provenance = {}, verifier = {} }, fail) {
    const heldSet = new Set(held)
    if (!plan.reset && heldSet.size > 0) fail('a held set was frozen outside a reset cycle')
    if (plan.reset && heldSet.size === 0) fail('no held set was frozen before the reset')
    if ([...heldSet].some((id) => !corpus.has(id))) fail('a held identity is outside the corpus')
    const entries = new Map()
    const dispatched = new Set()
    for (const { identity, deliveryCount, acquisition, dispatch } of handler) {
      const row = provenance.acquisitions?.find((row) => row.id === acquisition)
      const exchange = provenance.exchanges?.find((exchange) => exchange.id === row?.exchange)
      const source = ShareCycleOracle.partitionResponse(exchange, row)
      const offset = corpus.get(identity)?.offset
      const ranges =
        row?.acquiredRecords?.filter(
          (range) =>
            offset !== undefined &&
            BigInt(range.firstOffset) <= offset &&
            offset <= BigInt(range.lastOffset)
        ) ?? []
      const original = `${exchange?.frames?.[0]?.socket}:${exchange?.frames?.[0]?.id}:${row?.topicId}:${row?.partition}:${offset}`
      if (
        !row ||
        !source ||
        exchange.apiKey !== 78 ||
        !Number.isInteger(dispatch) ||
        row.partition !== corpus.get(identity)?.partition ||
        row.topic !== JSON.parse(identity)[0] ||
        row.topicId !== verifier.topicId ||
        exchange.groupId !== verifier.groupId ||
        ranges.length !== 1 ||
        ranges[0].deliveryCount !== deliveryCount ||
        source.acquiredRecords?.every(
          (range, index) => JSON.stringify(range) === JSON.stringify(row.acquiredRecords[index])
        ) !== true ||
        source.acquiredRecords.length !== row.acquiredRecords.length
      )
        fail('a handler lacks original successful acquisition provenance')
      if (dispatched.has(original)) fail('one original acquisition was dispatched more than once')
      dispatched.add(original)
      entries.set(identity, [...(entries.get(identity) ?? []), deliveryCount])
    }
    if ([...corpus.keys()].some((id) => !entries.has(id)))
      fail('an identity never reached a handler')
    for (const [identity, counts] of entries) {
      const valid = (count) => Number.isInteger(count) && count >= 1 && count <= 32767
      if (!corpus.has(identity)) fail('a handler received an identity outside the corpus')
      else if (!counts.every(valid)) fail('a delivery count is not a valid acquisition count')
      else if (counts.length > 1 && !heldSet.has(identity)) {
        fail('a duplicate delivery falls outside the frozen held set')
      } else if (heldSet.has(identity) && !counts.slice(1).every((c, i) => c > counts[i])) {
        fail('a replay did not raise its delivery count')
      }
    }
    for (const identity of heldSet) {
      if ((entries.get(identity)?.length ?? 0) < 2) fail('a held identity was not recovered')
    }
  }

  /**
   * @param {Map<string, { partition: number, offset: bigint }>} corpus the acknowledged identities
   * @param {object} observation Successful public ranges and their original attempt journals.
   * @param {object[]} observation.accepted Public Accept results.
   * @param {object} observation.provenance Original acquisitions, tickets and matching wire answers.
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static accepts(corpus, { accepted = [], provenance = {} }, fail) {
    const receipts = []
    const used = new Set()
    for (const range of accepted) {
      const matches = (provenance.outcomes ?? []).filter(
        (outcome) =>
          outcome.type === 'accept' &&
          !outcome.error &&
          outcome.topic === range.topic &&
          outcome.partition === range.partition &&
          outcome.firstOffset === range.firstOffset &&
          outcome.lastOffset === range.lastOffset
      )
      const covered = matches.filter((outcome) => {
        const acquisition = provenance.acquisitions?.find((row) => row.id === outcome.acquisition)
        const source = provenance.exchanges?.find(
          (exchange) => exchange.id === acquisition?.exchange
        )
        if (!source || !ShareCycleOracle.partitionResponse(source, acquisition)) return false
        return (provenance.exchanges ?? []).some((exchange) => {
          const ticket = exchange.tickets.filter(
            (ticket) => ticket.acquisition === acquisition.id && !ticket.isRenewAck
          )
          if (ticket.length !== 1) return false
          const result = ShareCycleOracle.partitionResponse(exchange, acquisition)
          if (
            !source ||
            !result ||
            (exchange.apiKey === 78 && result.acknowledgeErrorCode !== 0) ||
            exchange.open !== source.open ||
            exchange.pool !== source.pool ||
            exchange.generation !== source.generation ||
            exchange.memberId !== source.memberId ||
            exchange.frames[0].socket !== source.frames[0].socket ||
            exchange.request.isRenewAck ||
            exchange.frames[0].request.isRenewAck
          )
            return false
          const parts =
            exchange.frames[0].request.topics
              ?.filter((topic) => topic.topicId === acquisition.topicId)
              .flatMap((topic) =>
                topic.partitions.filter((part) => part.partitionIndex === range.partition)
              ) ?? []
          return (
            ticket.length === 1 &&
            parts.length === 1 &&
            ticket[0].batches.some(
              (batch) =>
                batch.firstOffset === range.firstOffset &&
                batch.lastOffset === range.lastOffset &&
                batch.acknowledgeTypes.every((type) => type === 1) &&
                parts[0].acknowledgementBatches.some(
                  (written) => JSON.stringify(written) === JSON.stringify(batch)
                )
            )
          )
        })
      })
      if (covered.length !== 1 || used.has(covered[0])) {
        fail('a successful Accept lacks a unique original request and partition receipt')
      } else {
        used.add(covered[0])
        receipts.push({ ...range, acquisition: covered[0].acquisition })
      }
    }
    const uncovered = [...corpus].filter(
      ([, { partition, offset }]) =>
        !receipts.some(
          (range) =>
            range.partition === partition &&
            BigInt(range.firstOffset) <= offset &&
            offset <= BigInt(range.lastOffset) &&
            provenance.acquisitions.some(
              (row) =>
                row.id === range.acquisition &&
                row.partition === partition &&
                row.acquiredRecords.some(
                  (acquired) =>
                    BigInt(acquired.firstOffset) <= offset && offset <= BigInt(acquired.lastOffset)
                )
            )
        )
    )
    if (uncovered.length > 0) fail(`${uncovered.length} identities lack a successful Accept`)
  }

  /**
   * @param {object} exchange Original exchange with socket-scoped wire identity.
   * @returns {object|null} Wire-validated successful response, including empty session deltas.
   */
  static response(exchange) {
    const frame = exchange?.frames?.[0]
    if (
      exchange?.error ||
      exchange?.frames?.length !== 1 ||
      ![78, 79].includes(exchange.apiKey) ||
      frame.apiKey !== exchange.apiKey ||
      frame.apiVersion !== 2 ||
      !frame.bytes ||
      !frame.responseBytes ||
      !Number.isInteger(exchange.responseIdentity) ||
      !Number.isInteger(frame.id) ||
      frame.request.groupId !== exchange.groupId ||
      frame.request.memberId !== exchange.memberId ||
      frame.endpoint !== exchange.endpoint ||
      exchange.response?.errorCode !== 0
    )
      return null
    let wire
    try {
      const { readRequestHeader } = require('../../testHelpers/fakeSocket')
      const bytes = Buffer.from(frame.bytes, 'base64')
      const header = readRequestHeader(bytes)
      const reader = require(
        exchange.apiKey === 78
          ? '../../testHelpers/wire/shareFetchV2'
          : '../../testHelpers/wire/shareAcknowledgeV2'
      )
      const written = reader.readRequest(bytes.subarray(header.byteLength))
      if (
        bytes.readInt32BE(0) !== bytes.length - 4 ||
        header.apiKey !== exchange.apiKey ||
        header.apiVersion !== 2 ||
        header.correlationId !== frame.correlationId ||
        JSON.stringify(written) !== JSON.stringify(frame.request)
      )
        return null
      wire = ShareCycleOracle.wireResponse(frame)
      const fields = (responses) =>
        responses.map(({ topicId, partitions }) => ({
          topicId,
          partitions: partitions.map(
            ({ partitionIndex, errorCode, acknowledgeErrorCode, acquiredRecords }) => ({
              partitionIndex,
              errorCode,
              acknowledgeErrorCode,
              acquiredRecords,
            })
          ),
        }))
      if (
        wire?.errorCode !== 0 ||
        JSON.stringify(fields(wire.responses)) !==
          JSON.stringify(fields(exchange.response.responses))
      )
        return null
    } catch {
      return null
    }
    return wire
  }

  /**
   * @param {object} exchange Original exchange with socket-scoped wire identity.
   * @param {object} acquisition Original partition acquisition.
   * @returns {object|null} Sole successful matching partition response, or no receipt.
   */
  static partitionResponse(exchange, acquisition) {
    if (!acquisition) return null
    const response = ShareCycleOracle.response(exchange)
    const parts =
      response?.responses
        .filter((topic) => topic.topicId === acquisition.topicId)
        .flatMap((topic) =>
          topic.partitions.filter((part) => part.partitionIndex === acquisition.partition)
        ) ?? []
    return parts.length === 1 && parts[0].errorCode === 0 ? parts[0] : null
  }

  /**
   * Reads partition errors/acquisition ranges directly from the matching socket response bytes.
   * @param {object} frame Original request and answer frames.
   * @returns {object|null} Raw top-level and partition results, including omitted partitions.
   * @throws {Error} On malformed frame bounds or fields; the receipt predicate rejects it.
   */
  static wireResponse(frame) {
    const Decoder = require('../../src/protocol/decoder')
    const bytes = Buffer.from(frame.responseBytes, 'base64')
    const decoder = new Decoder(bytes)
    if (decoder.readInt32() !== bytes.length - 4 || decoder.readInt32() !== frame.correlationId)
      return null
    decoder.readTaggedFields()
    decoder.readInt32()
    const errorCode = decoder.readInt16()
    decoder.readUVarIntString()
    decoder.readInt32()
    const topics = decoder.readUVarIntArray((decoder) => {
      const topicId = decoder.readUuid()
      const partitions = decoder.readUVarIntArray((decoder) => {
        const row = { partitionIndex: decoder.readInt32(), errorCode: decoder.readInt16() }
        decoder.readUVarIntString()
        if (frame.apiKey === 78) {
          row.acknowledgeErrorCode = decoder.readInt16()
          decoder.readUVarIntString()
        }
        decoder.readInt32()
        decoder.readInt32()
        decoder.readTaggedFields()
        if (frame.apiKey === 78) {
          decoder.readUVarIntBytes()
          row.acquiredRecords = decoder.readUVarIntArray((decoder) => {
            const range = {
              firstOffset: decoder.readInt64().toString(),
              lastOffset: decoder.readInt64().toString(),
              deliveryCount: decoder.readInt16(),
            }
            decoder.readTaggedFields()
            return range
          })
        }
        decoder.readTaggedFields()
        return row
      })
      decoder.readTaggedFields()
      return { topicId, partitions }
    })
    return { errorCode, responses: topics }
  }

  /**
   * Decodes a socket-matched heartbeat answer without using the Broker completion as a receipt.
   * @param {object} frame Original heartbeat request and response bytes.
   * @returns {object|null} Raw heartbeat fields, or no matched response.
   * @throws {Error} On malformed/truncated response fields.
   */
  static heartbeatResponse(frame) {
    const Decoder = require('../../src/protocol/decoder')
    const bytes = Buffer.from(frame.responseBytes, 'base64')
    const decoder = new Decoder(bytes)
    if (decoder.readInt32() !== bytes.length - 4 || decoder.readInt32() !== frame.correlationId)
      return null
    decoder.readTaggedFields()
    decoder.readInt32()
    const errorCode = decoder.readInt16()
    decoder.readUVarIntString()
    const memberId = decoder.readUVarIntString()
    const memberEpoch = decoder.readInt32()
    const heartbeatIntervalMs = decoder.readInt32()
    const present = decoder.readInt8()
    if (present !== -1 && present !== 1) return null
    if (present === 1) {
      decoder.readUVarIntArray((entry) => {
        entry.readUuid()
        entry.readUVarIntArray((entry) => entry.readInt32())
        entry.readTaggedFields()
      })
      decoder.readTaggedFields()
    }
    decoder.readTaggedFields()
    return decoder.canReadBytes(1)
      ? null
      : { errorCode, memberId, memberEpoch, heartbeatIntervalMs }
  }

  /**
   * Proves a member's departure from its invoked epoch -1 request and matched successful answer.
   * @param {object} provenance Original heartbeat call-through and wire archive.
   * @param {string} groupId Original group.
   * @param {string} memberId Original member, never inferred from disconnect completion.
   * @returns {object|null} The successful leave frame, or no departure proof.
   */
  static departure(provenance, groupId, memberId) {
    if (!groupId || !memberId) return null
    for (const beat of provenance.heartbeats ?? []) {
      if (
        beat.groupId !== groupId ||
        beat.memberId !== memberId ||
        beat.request?.groupId !== groupId ||
        beat.request?.memberId !== memberId ||
        beat.request?.memberEpoch !== -1
      )
        continue
      for (const frame of beat.frames ?? []) {
        if (
          frame.apiKey !== 76 ||
          frame.apiVersion !== 1 ||
          !Number.isInteger(frame.socket) ||
          frame.endpoint !== beat.endpoint ||
          !Number.isInteger(frame.correlationId) ||
          ![beat.at, frame.at, frame.answeredAt].every(Number.isFinite) ||
          frame.at < beat.at ||
          frame.answeredAt < frame.at ||
          frame.lostAt !== undefined ||
          !frame.responseBytes
        )
          continue
        try {
          const { readRequestHeader } = require('../../testHelpers/fakeSocket')
          const wire = require('../../testHelpers/wire/shareGroupHeartbeatV1')
          const bytes = Buffer.from(frame.bytes, 'base64')
          const header = readRequestHeader(bytes)
          const request = wire.readRequest(bytes.subarray(header.byteLength))
          const response = ShareCycleOracle.heartbeatResponse(frame)
          if (
            bytes.readInt32BE(0) === bytes.length - 4 &&
            header.apiKey === 76 &&
            header.apiVersion === 1 &&
            header.correlationId === frame.correlationId &&
            JSON.stringify(request) === JSON.stringify(frame.request) &&
            request.groupId === groupId &&
            request.memberId === memberId &&
            request.memberEpoch === -1 &&
            response?.errorCode === 0 &&
            response.memberEpoch === -1 &&
            (response.memberId === null || response.memberId === memberId)
          )
            return frame
        } catch {}
      }
    }
    return null
  }

  /**
   * Recomputes terminality from raw named/all listings and independent ends.
   * @param {object} plan Fixed cumulative end.
   * @param {object} position Archived primitive observation.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static position(plan, position, fail) {
    const { terminalSharePosition } = require('../../testHelpers/shareLiveDiagnostics')
    if (
      !position?.target ||
      position.target.partitions.length !== SHARE_SOAK.partitions ||
      position.target.partitions.some(({ floor, end }) => floor !== '0' || end !== plan.end) ||
      !terminalSharePosition(position.target, position).settled
    )
      fail('the raw terminal listings or independent ends are incomplete or disagree')
  }

  /**
   * Reconstructs verifier activity from assignment reads and actual request chronology.
   * @param {object} verifier Original member observations.
   * @param {number} windowMs Required unchanged window.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static verifier(verifier = {}, windowMs, fail) {
    const start = verifier.events?.find(({ kind }) => kind === 'window-start')
    const end = verifier.events?.findLast(({ kind }) => kind === 'window-end')
    const owns = (assignment) =>
      assignment?.length === 1 &&
      assignment[0].topic === verifier.topic &&
      [...assignment[0].partitions].sort().join() === '0,1,2'
    const times = [verifier.openedAt, start?.at, end?.at, verifier.closedAt]
    const ordered =
      times.every(Number.isFinite) &&
      times.every((at, i) => !i || at >= times[i - 1]) &&
      Number.isFinite(start?.wallAt) &&
      Number.isFinite(end?.wallAt) &&
      end.wallAt >= start.wallAt &&
      verifier.events.every(
        ({ at }, i, events) =>
          Number.isFinite(at) &&
          at >= verifier.openedAt &&
          at <= verifier.closedAt &&
          (!i || at >= events[i - 1].at)
      )
    const joined = verifier.events?.filter(({ kind }) => kind === 'GROUP_JOIN') ?? []
    const covered = new Set()
    const sessions = new Map()
    const activity = []
    for (const exchange of verifier.provenance?.exchanges ?? []) {
      const frame = exchange.frames?.[0]
      if (!frame || exchange.apiKey !== 78 || exchange.memberId !== verifier.memberId) continue
      const key = `${frame.socket}:${exchange.open}:${exchange.pool}:${exchange.generation}`
      const epoch = frame.request.shareSessionEpoch
      const previous = sessions.get(key)
      const response = ShareCycleOracle.response(exchange)
      const detail = {
        exchange: exchange.id,
        socket: frame.socket,
        open: exchange.open,
        pool: exchange.pool,
        generation: exchange.generation,
        epoch,
        at: frame.at,
        answeredAt: frame.answeredAt,
        partitions: [],
      }
      activity.push(detail)
      const valid =
        ordered &&
        response &&
        frame.request.isRenewAck === false &&
        frame.request.maxRecords > 0 &&
        frame.request.maxBytes > 0 &&
        exchange.groupId === verifier.groupId &&
        Number.isFinite(frame.at) &&
        Number.isFinite(frame.answeredAt) &&
        frame.at >= verifier.openedAt &&
        frame.answeredAt >= frame.at &&
        frame.answeredAt <= verifier.closedAt &&
        Number.isInteger(frame.socket) &&
        Number.isInteger(exchange.open) &&
        Number.isInteger(exchange.pool) &&
        Number.isInteger(exchange.generation) &&
        (epoch === 0 ||
          (previous &&
            epoch === (previous.epoch === 2147483647 ? 1 : previous.epoch + 1) &&
            frame.at >= previous.answeredAt))
      if (!valid) {
        sessions.delete(key)
        detail.reason = 'invalid wire ownership, response, session epoch or timestamps'
        continue
      }
      const partitions = epoch === 0 ? new Set() : previous.partitions
      const parts = (partition) =>
        response.responses
          .filter(({ topicId }) => topicId === verifier.topicId)
          .flatMap(({ partitions }) =>
            partitions.filter(({ partitionIndex }) => partitionIndex === partition)
          )
      for (const topic of frame.request.topics ?? []) {
        if (topic.topicId !== verifier.topicId) continue
        for (const { partitionIndex } of topic.partitions) {
          const rows = parts(partitionIndex)
          if (rows.length === 1 && rows[0].errorCode === 0) partitions.add(partitionIndex)
        }
      }
      for (const topic of frame.request.forgottenTopicsData ?? [])
        if (topic.topicId === verifier.topicId)
          for (const partition of topic.partitions) partitions.delete(partition)
      sessions.set(key, { partitions, epoch, answeredAt: frame.answeredAt })
      detail.partitions = [...partitions]
      const request = verifier.events.find(
        ({ kind, payload, at }) =>
          kind === 'REQUEST' &&
          payload.apiName === 'ShareFetch' &&
          payload.correlationId === frame.correlationId &&
          payload.broker === frame.endpoint &&
          payload.sentAt >= start.wallAt &&
          payload.sentAt <= end.wallAt &&
          at >= frame.answeredAt &&
          at <= end.at
      )
      if (frame.at < start.at || frame.answeredAt > end.at || !request) {
        detail.reason = 'request write, response or public completion outside observation window'
        continue
      }
      detail.requestAt = request.at
      // An empty incremental response still fetches the previously established session model.
      for (const partition of partitions) {
        const rows = parts(partition)
        if (
          rows.length === 0 ||
          (rows.length === 1 && rows[0].errorCode === 0 && rows[0].acquiredRecords?.length === 0)
        )
          covered.add(partition)
      }
    }
    const checks = {
      'ordered timestamps': ordered,
      'measured duration': ordered && end.at - start.at >= windowMs,
      'full endpoint assignment': owns(start?.payload) && owns(end?.payload),
      'all-partition in-window wire activity': [...covered].sort().join() === '0,1,2',
      'same-group member join':
        joined.length === 1 &&
        joined[0].payload.memberId === verifier.memberId &&
        joined[0].payload.groupId === verifier.groupId &&
        joined[0].at <= start?.at,
      'joined leave': verifier.events?.some(
        ({ kind, at }) => kind === 'DISCONNECT' && at >= end?.at && at <= verifier.closedAt
      ),
      'continuous assignment':
        start &&
        end &&
        !verifier.events
          .slice(verifier.events.indexOf(start), verifier.events.indexOf(end))
          .some(({ kind, payload }) => kind === 'ASSIGNMENT_CHANGED' && !owns(payload.assignment)),
      'declared duration': verifier.windowMs === windowMs,
      'member and topic identity': Boolean(verifier.memberId && verifier.topicId),
      'active empty verifier': verifier.active === true && verifier.delivered?.length === 0,
    }
    const failed = Object.keys(checks).filter((name) => !checks[name])
    if (failed.length)
      fail(
        'the same-group verifier activity or measured observation window is incomplete: ' +
          `${failed.join(', ')}; ${JSON.stringify({
            openedAt: verifier.openedAt,
            window: [start?.at, end?.at],
            wallWindow: [start?.wallAt, end?.wallAt],
            closedAt: verifier.closedAt,
            coveredPartitions: [...covered].sort(),
            missingPartitions: [0, 1, 2].filter((partition) => !covered.has(partition)),
            activity,
          })}`
      )
  }

  /**
   * Binds original reads, departures and the joined verifier to one enclosing observation.
   * @param {object} observation Original cycle or final proof.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static sequence({ timing = {}, positions = [], verifier = {}, members = {} }, fail) {
    const [before, after] = positions
    const departures =
      timing.kind === 'final'
        ? [members.bRemovedAt, members.aLeftAt, timing.startedAt]
        : [timing.startedAt, members.bRemovedAt, members.aLeftAt]
    const times = [
      ...departures,
      before?.startedAt,
      before?.completedAt,
      members.emptyAt,
      verifier.openedAt,
      verifier.closedAt,
      after?.startedAt,
      after?.completedAt,
      timing.completedAt,
      timing.due,
    ]
    if (
      !times.every(Number.isFinite) ||
      times.some((at, i) => i && at < times[i - 1]) ||
      !Number.isInteger(timing.cycle) ||
      positions.length !== 2 ||
      [before, after, verifier].some(
        (part) => part?.cycle !== timing.cycle || part?.kind !== timing.kind
      )
    )
      fail(
        'the terminal reads, departures and verifier are not bound to their original cycle window'
      )
  }

  /**
   * The terminal position oracle: three distinct partition rows, each answered without error,
   * with zero lag, a canonical start offset inside the log and the independently read end.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {object[]} rows the terminal partition rows
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static broker(plan, rows, fail) {
    const partitions = rows.map(({ partition }) => partition).sort()
    if (partitions.join() !== '0,1,2') fail('three distinct terminal partition rows are required')
    for (const row of rows) {
      const decimal = (value) => typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value)
      if (
        row.errorCode !== 0 ||
        row.lag !== '0' ||
        row.logEnd !== plan.end ||
        !decimal(row.startOffset) ||
        BigInt(row.startOffset) > BigInt(plan.end)
      ) {
        fail(`partition ${row.partition} is not at its terminal position ${plan.end}`)
      }
    }
  }

  /**
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static membership(plan, observation, fail) {
    const { members = {}, reset = null, verifier = {}, provenance = {} } = observation
    for (const label of ['A', 'B']) {
      const member = observation.memberObservations?.find((member) => member.member === label)
      const leave = ShareCycleOracle.departure(provenance, verifier.groupId, member?.memberId)
      const removedAt = label === 'A' ? members.aLeftAt : members.bRemovedAt
      if (
        !leave ||
        !Number.isFinite(removedAt) ||
        leave.answeredAt > removedAt ||
        leave.at < observation.timing?.startedAt
      )
        fail(
          `member ${label} (${member?.memberId ?? 'unknown'}) lacks original successful leave evidence`
        )
    }
    if (!(members.aJoined && members.aLeft && members.bAdded && members.bRemoved)) {
      fail('the member joins, leaves and the additional member add and remove are incomplete')
    }
    if (plan.reset) {
      const reopened =
        reset?.apiKey === 78 &&
        Number.isInteger(reset.oldEpoch) &&
        reset.oldEpoch > 0 &&
        reset.nextEpoch === 0 &&
        Boolean(reset.reason) &&
        Boolean(reset.oldSocket) &&
        Boolean(reset.nextSocket) &&
        reset.oldSocket !== reset.nextSocket
      if (!reopened) fail('no original session reset and new epoch-zero open was observed')
    } else if (reset !== null) {
      fail('a reset was observed outside a reset cycle')
    }
    if (
      verifier.active !== true ||
      !Array.isArray(verifier.delivered) ||
      verifier.delivered.length > 0
    ) {
      fail('the same-group verifier was inactive or received a record')
    }
    if (observation.clientsDisconnected !== true) fail('a cycle client was still connected')
  }
}

/** The timestamp of every share soak record: old and fixed, so no clock reading changes a byte. */
const SHARE_SOAK_TIMESTAMP = '1700000000000'
/** Monotonic millisecond timestamps for the declared share schedule and its integer boundaries. */
const SHARE_CLOCK = Object.freeze({ now: () => Math.floor(performance.now()) })

/**
 * Lets plain `node` resolve the `testHelpers` module name that Jest maps, which the share fixtures
 * use. A resolver that already knows the name is left alone.
 * @returns {void}
 */
const aliasTestHelpers = () => {
  try {
    require.resolve('testHelpers')
    return
  } catch {}
  const Module = require('node:module')
  const resolve = Module._resolveFilename
  Module._resolveFilename = function (request, ...rest) {
    const target =
      request === 'testHelpers' ? path.resolve(__dirname, '../../testHelpers') : request
    return resolve.call(this, target, ...rest)
  }
}

/**
 * @returns {object} the share fixtures and live-run helpers the share soak drives, loaded only when
 *   a share soak is built so the other profiles never load them
 */
const loadSharePorts = () => {
  aliasTestHelpers()
  return {
    fixtures: require('../../testHelpers/shareFixtures'),
    diagnostics: require('../../testHelpers/shareLiveDiagnostics'),
    probe: require('../../src/producer/__tests__/fixtures/liveProbe'),
    bounded: require('../../src/producer/__tests__/fixtures/bounded'),
  }
}

/**
 * Observes the ShareFetch requests written on the sockets of one run without changing a byte, a
 * return value or a callback. Recent reset facts are bounded; an active cycle separately archives
 * original request/answer frames. Sockets are held weakly across destruction.
 */
class ShareFetchTap {
  /** @param {number} [limit=64] Retained facts. @param {object} [clock] Monotonic clock. */
  constructor(limit = 64, clock = SHARE_CLOCK) {
    this.clock = clock
    this.limit = limit
    this.facts = []
    this.sockets = new Map()
    this.sequence = 0
    this.created = 0
    this.frameSequence = 0
    this.faults = 0
    this.context = new AsyncLocalStorage()
    this.operations = new AsyncLocalStorage()
    this.operationSequence = 0
    this.archive = null
    this.observers = new Set()
    this.socketObservers = new WeakMap()
  }

  /**
   * @param {Function} inner a socket factory
   * @returns {Function} a factory whose sockets this tap observes
   */
  factory(inner) {
    const tap = this
    return (args) => {
      const id = ++this.created
      const endpoint = `${args.host}:${args.port}`
      this.notify('socket-connect-start', { socket: id, endpoint })
      const socket = inner({
        ...args,
        onConnect(...rest) {
          tap.notify('socket-connect-end', { socket: id, endpoint })
          return args.onConnect.apply(this, rest)
        },
      })
      return this.wrap(socket, endpoint, id)
    }
  }

  /**
   * @param {object} socket the socket the product will write to
   * @param {string} endpoint `host:port` it dialled
   * @param {number} [id] Identity reserved by the observed factory.
   * @returns {object} the same socket, observed
   */
  wrap(socket, endpoint, id = ++this.created) {
    for (const [known, ref] of this.sockets)
      if (ref.deref()?.destroyed !== false) this.sockets.delete(known)
    this.sockets.set(id, new WeakRef(socket))
    let carry = Buffer.alloc(0)
    let incoming = Buffer.alloc(0)
    const outstanding = new Map()
    const receive = (chunk) => {
      incoming = Buffer.concat([incoming, chunk])
      while (incoming.length >= 4 && incoming.length >= 4 + incoming.readInt32BE(0)) {
        const end = 4 + incoming.readInt32BE(0)
        const frame = outstanding.get(incoming.readInt32BE(4))
        if (frame) {
          frame.responseBytes = incoming.subarray(0, end).toString('base64')
          frame.answeredAt = this.clock.now()
          outstanding.delete(frame.correlationId)
          this.archive?.({
            kind: 'response',
            frame: frame.id,
            socket: id,
            generation: id,
            endpoint,
            correlationId: frame.correlationId,
            at: frame.answeredAt,
            bytes: frame.responseBytes,
          })
          if (frame.apiKey === 76 && frame.request?.memberEpoch === -1) {
            let response
            let error
            try {
              response = ShareCycleOracle.heartbeatResponse(frame)
            } catch (failure) {
              error = { name: failure.name, message: failure.message }
            }
            this.leave(
              'leave-answer',
              frame,
              frame,
              {
                errorCode: response?.errorCode ?? null,
                responseMemberId: response?.memberId ?? null,
                responseMemberEpoch: response?.memberEpoch ?? null,
                error,
              },
              frame.answeredAt
            )
          }
        }
        incoming = incoming.subarray(end)
      }
    }
    const ended = () => this.notify('socket-end', { socket: id, endpoint })
    const errored = (error) =>
      this.notify('socket-error', {
        socket: id,
        endpoint,
        error: { name: error.name, message: error.message, code: error.code },
      })
    socket.on?.('data', receive)
    this.socketObservers.set(socket, () => {
      socket.on?.('end', ended)
      socket.on?.('error', errored)
    })
    socket.once?.('close', (hadError) => {
      this.notify('socket-close', { socket: id, endpoint, hadError, destroyed: socket.destroyed })
      socket.off('end', ended)
      socket.off('error', errored)
      this.socketObservers.delete(socket)
      socket.off('data', receive)
      for (const frame of outstanding.values()) {
        frame.lostAt = this.clock.now()
        this.archive?.({
          kind: 'lost',
          frame: frame.id,
          socket: id,
          generation: id,
          endpoint,
          correlationId: frame.correlationId,
          at: frame.lostAt,
        })
        if (frame.apiKey === 76 && frame.request?.memberEpoch === -1)
          this.leave('leave-lost', frame, frame, {}, frame.lostAt)
      }
      outstanding.clear()
    })
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      try {
        const bytes = Buffer.isBuffer(data)
          ? data
          : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
        carry = Buffer.concat([carry, bytes])
        while (carry.length >= 4 && carry.length >= 4 + carry.readInt32BE(0)) {
          const end = 4 + carry.readInt32BE(0)
          const fact = this.observe(id, endpoint, carry.subarray(0, end))
          if (fact) outstanding.set(fact.correlationId, fact)
          carry = carry.subarray(end)
        }
      } catch {
        carry = Buffer.alloc(0)
        this.faults += 1
      }
      return write(data, ...rest)
    }

    return socket
  }

  /**
   * @param {number} socket the observed socket's id
   * @param {string} endpoint the socket's `host:port`
   * @param {Buffer} frame one whole request frame
   * @returns {object|undefined} the original wire fact when a journal is active
   */
  observe(socket, endpoint, frame) {
    const { readRequestHeader } = require('../../testHelpers/fakeSocket')
    const header = readRequestHeader(frame)
    const context = this.context.getStore()
    let written
    if (context && [76, 78, 79].includes(header.apiKey)) {
      const modules = {
        76: 'shareGroupHeartbeatV1',
        78: 'shareFetchV2',
        79: 'shareAcknowledgeV2',
      }
      const wire = require(`../../testHelpers/wire/${modules[header.apiKey]}`)
      written = {
        id: ++this.frameSequence,
        socket,
        endpoint,
        ...header,
        at: this.clock.now(),
        bytes: frame.toString('base64'),
        request: wire.readRequest(frame.subarray(header.byteLength)),
      }
      context.frames.push(written)
      if (header.apiKey === 76 && written.request.memberEpoch === -1) {
        written.invokedAt = context.at
        this.leave('leave-write', context, written, {}, written.at)
      }
    }
    const operation = this.operations.getStore()
    written ??= { id: ++this.frameSequence, socket, endpoint, ...header, at: this.clock.now() }
    const codecs =
      this.archive && header.apiKey === 0
        ? require('./shareProtocol').produceCodecs(frame)
        : undefined
    this.archive?.({
      kind: 'request',
      frame: written.id,
      socket,
      generation: socket,
      endpoint,
      ...header,
      at: written.at,
      operation: operation?.name ?? 'share-loop',
      operationId: operation?.id ?? null,
      role: operation?.role ?? 'share',
      bytes: frame.toString('base64'),
      codecs,
    })
    if (header.apiKey !== 78) return written
    let epoch = null
    try {
      const { readRequest } = require('../../testHelpers/wire/shareFetchV2')
      epoch = readRequest(frame.subarray(header.byteLength)).shareSessionEpoch
    } catch {}
    const fact = {
      sequence: ++this.sequence,
      socket,
      endpoint,
      epoch,
      at: written?.at ?? this.clock.now(),
    }
    this.facts.push(fact)
    this.notify(
      'api78-write',
      {
        ...fact,
        frame: written?.id,
        exchange: context?.id,
        memberId: context?.memberId,
        groupId: context?.groupId,
        attempted: true,
      },
      fact.at
    )
    if (this.facts.length > this.limit) this.facts.shift()
    return written
  }

  /**
   * Emits original leave facts; absent transport fields stay explicitly unknown before a write.
   * @param {string} step Invocation, wire, failure or no-write observation.
   * @param {object} fact Original heartbeat invocation.
   * @param {object|null} frame Original socket frame, when one exists.
   * @param {object} [fields] Decoded response or failure details.
   * @param {number} [at] Original event timestamp.
   * @returns {void}
   */
  leave(step, fact, frame, fields = {}, at = this.clock.now()) {
    this.notify(
      step,
      {
        groupId: fact.request.groupId,
        memberId: fact.request.memberId,
        memberEpoch: fact.request.memberEpoch,
        endpoint: frame?.endpoint ?? fact.endpoint,
        socket: frame?.socket ?? null,
        correlationId: frame?.correlationId ?? null,
        frame: frame?.id ?? null,
        invokedAt: frame?.invokedAt ?? fact.at,
        writtenAt: frame?.at ?? null,
        answeredAt: frame?.answeredAt ?? null,
        lostAt: frame?.lostAt ?? null,
        ...fields,
      },
      at
    )
  }

  /**
   * Observes a public call without altering its return value, rejection or arguments.
   * Completed call facts corroborate the encoded request/response inventory, including fallbacks.
   * @template T
   * @param {string} role Client role.
   * @param {string} name Public operation and concrete option shape.
   * @param {Function} work Original invocation.
   * @param {object} [target] Original public topic/group selection for response correlation.
   * @returns {T} Original return value.
   * @throws {Error} Original synchronous error.
   */
  operation(role, name, work, target) {
    if (!this.archive) return work()
    const context = { role, name, id: ++this.operationSequence, ...(target && { target }) }
    return this.operations.run(context, () => {
      const record = (status, result) =>
        this.archive?.({
          kind: 'operation',
          ...context,
          at: this.clock.now(),
          status,
          ...([
            'send',
            'describeConfigs',
            'fetchTopicMetadata',
            'listTopics',
            'describeCluster',
            'describeShareGroups',
            'listShareGroupOffsets:named',
            'listShareGroupOffsets:all',
            'fetchTopicOffsets',
            'listGroups',
          ].includes(name) &&
            status === 'passed' && { result }),
        })
      record('started')
      let result
      try {
        result = work()
      } catch (error) {
        record('failed')
        throw error
      }
      if (result?.then)
        result
          .then(
            (value) => record('passed', value),
            () => record('failed')
          )
          .catch(() => {
            this.faults++
          })
      else record('passed', result)
      return result
    })
  }

  /**
   * Copies the frozen public facade and observes calls through its original methods.
   * @param {string} role Separate client role.
   * @param {object} client Original public facade.
   * @returns {object} Facade with unchanged call arguments, receivers and return values.
   */
  client(role, client) {
    return Object.fromEntries(
      Object.entries(client).map(([name, value]) => [
        name,
        typeof value !== 'function'
          ? value
          : (...args) => {
              let operation = name
              if (name === 'listShareGroupOffsets') operation += args[0]?.topics ? ':named' : ':all'
              if (name === 'incrementalAlterConfigs') {
                const operations = args[0]?.resources?.flatMap((r) =>
                  r.configEntries.map((e) => e.operation)
                )
                operation += `:${[...new Set(operations)].sort().join(',')}`
              }
              return this.operation(role, operation, () => value.apply(client, args), {
                topic: args[0]?.topic,
                groupId: args[0]?.groupId,
              })
            },
      ])
    )
  }

  /**
   * @param {Function} observer Receives original transport times without retaining sockets.
   * @returns {Function} Removes this cycle's observer.
   */
  listen(observer) {
    this.observers.add(observer)
    return () => this.observers.delete(observer)
  }

  /**
   * @param {string} step Original socket or frame observation.
   * @param {object} fields Primitive event details.
   * @param {number} [at] Original monotonic timestamp.
   * @returns {void}
   */
  notify(step, fields, at = this.clock.now()) {
    for (const observer of this.observers) observer(step, fields, at)
  }

  /** @returns {number} a position after which `since` reads only later requests */
  mark() {
    return this.sequence
  }

  /**
   * @param {number} mark a position from `mark`
   * @param {string} endpoint `host:port`
   * @returns {object[]} the ShareFetch requests written to the endpoint since the mark
   */
  since(mark, endpoint) {
    return this.facts.filter((fact) => fact.sequence > mark && fact.endpoint === endpoint)
  }

  /**
   * Destroys one original socket.
   * @param {number} socket the id from a fact
   * @returns {void}
   * @throws {Error} when that socket is gone already
   */
  destroy(socket) {
    const target = this.sockets.get(socket)?.deref()
    if (!target || target.destroyed) throw new Error('The original data socket is already gone')
    this.socketObservers.get(target)?.()
    target.destroy()
  }
}

/**
 * Archives original exchange, acquisition, dispatch and settlement authority for one cycle.
 * Object labels are local to the journal; detached acquisitions remain evidence until archived.
 */
class ShareCycleJournal {
  /** @param {string} group Owned group. @param {ShareFetchTap} tap Original wire observer. */
  constructor(group, tap) {
    this.group = group
    this.tap = tap
    this.labels = new Map()
    this.exchanges = []
    this.heartbeats = []
    this.groupDescriptions = []
    this.acquisitions = []
    this.outcomes = []
    this.retirements = []
    this.byExchange = new WeakMap()
    this.byCohort = new WeakMap()
    this.handlers = new AsyncLocalStorage()
    this.restores = []
    this.dispatch = 0
  }

  /** @param {*} value Original authority. @returns {number|null} Stable journal identity. */
  label(value) {
    if (value === undefined || value === null) return null
    if (!this.labels.has(value)) this.labels.set(value, this.labels.size + 1)
    return this.labels.get(value)
  }

  /**
   * @param {object} prototype Original prototype.
   * @param {string} method Observed method.
   * @param {Function} observe Call-through observer.
   * @returns {void}
   */
  wrap(prototype, method, observe) {
    const original = prototype[method]
    const wrapper = function (...args) {
      return observe(this, original, args)
    }
    prototype[method] = wrapper
    this.restores.push(() => {
      if (prototype[method] !== wrapper)
        throw new Error('A share journal wrapper could not be restored')
      prototype[method] = original
    })
  }

  /**
   * @param {object} loop Original node owner.
   * @param {object} exchange Original request or captured closing authority.
   * @returns {object} One archived request with original tickets.
   */
  begin(loop, exchange) {
    const fact = {
      id: this.label(exchange),
      apiKey: exchange.apiKey,
      groupId: exchange.fields.groupId ?? loop.groupId,
      memberId: exchange.fields.memberId ?? loop.memberId,
      open: this.label(exchange.open),
      pool: this.label(exchange.binding?.pool),
      generation: exchange.binding?.generation,
      endpoint: loop.broker.brokerAddress,
      request: exchange.fields,
      frames: [],
      tickets: exchange.prepared.map(({ row, ticket }) => ({
        acquisition: this.label(row),
        ticket: this.label(ticket),
        batches: ticket.acknowledgementBatches,
        isRenewAck: ticket.isRenewAck,
      })),
    }
    this.byExchange.set(exchange, fact)
    this.exchanges.push(fact)
    return fact
  }

  /** @returns {void} Installs observers before any cycle member connects. */
  install() {
    const { ShareFetchLoop } = require('../../src/consumer/share/shareFetchLoop')
    const { ShareRun } = require('../../src/consumer/share/shareRun')
    const RequestQueue = require('../../src/network/requestQueue')
    this.wrap(RequestQueue.prototype, 'push', (queue, original, [request]) => {
      const context = this.tap.context.getStore()
      return original.call(
        queue,
        context
          ? {
              ...request,
              sendRequest: () => this.tap.context.run(context, () => request.sendRequest()),
            }
          : request
      )
    })
    this.wrap(Broker.prototype, 'shareGroupHeartbeat', (broker, original, args) => {
      const [request] = args
      if (request.groupId !== this.group) return original.apply(broker, args)
      const fact = {
        groupId: this.group,
        memberId: request.memberId,
        endpoint: broker.brokerAddress,
        at: this.tap.clock.now(),
        request: { ...request },
        frames: [],
      }
      this.heartbeats.push(fact)
      const leaving = request.memberEpoch === -1
      if (leaving) this.tap.leave('leave-invoked', fact, null, {}, fact.at)
      const noWrite = () => {
        if (leaving && fact.frames.length === 0) this.tap.leave('leave-no-write', fact, null)
      }
      const failed = (error) => {
        fact.failedAt = this.tap.clock.now()
        fact.error = { name: error.name, message: error.message, code: error.code }
        noWrite()
        if (leaving) {
          for (const frame of fact.frames.length ? fact.frames : [null])
            this.tap.leave('leave-failure', fact, frame, { error: fact.error }, fact.failedAt)
        }
      }
      let pending
      try {
        pending = this.tap.context.run(fact, () => original.apply(broker, args))
      } catch (error) {
        failed(error)
        throw error
      }
      pending.then((response) => {
        fact.answeredAt = this.tap.clock.now()
        fact.response = response
        noWrite()
      }, failed)
      return pending
    })
    this.wrap(ShareFetchLoop.prototype, 'send', (loop, original, [exchange]) => {
      if (loop.groupId !== this.group) return original.call(loop, exchange)
      const fact = this.begin(loop, exchange)
      return this.tap.context.run(fact, () => original.call(loop, exchange))
    })
    this.wrap(ShareFetchLoop.prototype, 'writeFinal', (loop, original, args) => {
      if (loop.groupId !== this.group) return original.apply(loop, args)
      const captured = args[2]
      const fact = this.begin(loop, { ...captured, apiKey: 79, open: captured.descriptor.token })
      return this.tap.context.run(fact, () => original.apply(loop, args))
    })
    this.wrap(Broker.prototype, 'shareAcknowledge', (broker, original, args) => {
      const fact = this.tap.context.getStore()
      const pending = original.apply(broker, args)
      if (fact?.groupId === this.group)
        pending.then(
          (response) => {
            fact.response = response
            fact.responseIdentity = this.label(response)
          },
          (error) => {
            fact.error = { name: error.name, message: error.message }
          }
        )
      return pending
    })
    this.wrap(ShareFetchLoop.prototype, 'complete', (loop, original, [exchange, response]) => {
      const fact = this.byExchange.get(exchange)
      if (fact) {
        fact.responseIdentity = this.label(response)
        fact.response = response
      }
      return original.call(loop, exchange, response)
    })
    this.wrap(ShareFetchLoop.prototype, 'failExchange', (loop, original, [exchange, error]) => {
      const fact = this.byExchange.get(exchange)
      if (fact) fact.error = { name: error.name, message: error.message }
      return original.call(loop, exchange, error)
    })
    this.wrap(ShareFetchLoop.prototype, 'adopt', (loop, original, args) => {
      const row = original.apply(loop, args)
      if (loop.groupId !== this.group) return row
      const [exchange, topicId, partition] = args
      const fact = {
        id: this.label(row),
        token: this.label(row.token),
        ledger: this.label(row.ledger),
        exchange: this.byExchange.get(exchange)?.id,
        topicId,
        topic: row.topic,
        partition: row.partition,
        acquiredRecords: partition.acquiredRecords,
      }
      this.byCohort.set(row, fact)
      this.acquisitions.push(fact)
      return row
    })
    this.wrap(ShareRun.prototype, 'invoke', (run, original, args) => {
      const acquisition = this.byCohort.get(args[1])
      return acquisition
        ? this.handlers.run({ acquisition: acquisition.id, dispatch: ++this.dispatch }, () =>
            original.apply(run, args)
          )
        : original.apply(run, args)
    })
    this.wrap(ShareFetchLoop.prototype, 'emit', (loop, original, [results, row]) => {
      if (loop.groupId === this.group)
        for (const result of results) {
          this.outcomes.push({
            ...result,
            topic: this.byCohort.get(row)?.topic,
            partition: this.byCohort.get(row)?.partition,
            acquisition: this.label(row),
            error: result.error ? { name: result.error.name, message: result.error.message } : null,
          })
        }
      return original.call(loop, results, row)
    })
    for (const method of ['onSessionReset', 'retireKey', 'dispose']) {
      this.wrap(ShareFetchLoop.prototype, method, (loop, original, args) => {
        if (loop.groupId === this.group)
          this.retirements.push({
            method,
            at: performance.now(),
            open: this.label(method === 'onSessionReset' ? args[0] : loop.session.token),
            acquisitions: [...loop.cohorts.values()].flat().map((row) => this.label(row)),
            reason: typeof args[1] === 'string' ? args[1] : null,
          })
        return original.apply(loop, args)
      })
    }
  }

  /** @returns {object} Serializable primitive journals, including failed and retired attempts. */
  snapshot() {
    return JSON.parse(
      JSON.stringify({
        exchanges: this.exchanges,
        heartbeats: this.heartbeats,
        groupDescriptions: this.groupDescriptions,
        acquisitions: this.acquisitions,
        outcomes: this.outcomes,
        retirements: this.retirements,
      })
    )
  }

  /** @returns {void} Restores observers only after all member work has joined. */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.handlers.disable()
    this.labels.clear()
  }
}

/**
 * One public share client of a cycle, with the journals that observe it from the outside: the
 * handler entries, the public acknowledgement results, the session resets and the membership it
 * reports. It stores identities only, never a delivered message.
 */
class ShareMember {
  /**
   * @param {object} options
   * @param {object} options.kafka the counted client
   * @param {{ track: (client: object) => object }} options.shares resource observer
   * @param {string} options.label `A` or `B`
   * @param {string} options.group share group id
   * @param {string} options.topic
   * @param {{ shareRecordIdentity: Function }} options.fixtures the share fixtures
   * @param {ShareCycleJournal} [options.journal] Original acquisition and handler authority.
   * @param {Function} [options.onLateEntry] Invalidates the run on any post-join callback.
   * @param {Function} [options.onProgress] Records original membership, delivery and close times.
   * @param {(entries: object[], partition: number) => Promise<void>} [options.onBatch] awaited after
   *   a batch's entries are journalled
   */
  constructor({
    kafka,
    shares,
    label,
    group,
    topic,
    fixtures,
    journal,
    onLateEntry = () => {},
    onProgress = () => {},
    onBatch = async () => {},
  }) {
    this.label = label
    this.topic = topic
    this.fixtures = fixtures
    this.onBatch = onBatch
    this.journal = journal
    this.onLateEntry = onLateEntry
    this.onProgress = onProgress
    this.entries = []
    this.acks = []
    this.resets = []
    this.crashes = []
    this.memberId = null
    this.assignment = []
    this.client = shares.track(
      kafka.shareConsumer({
        groupId: group,
        acquireMode: 'record_limit',
        maxRecords: SHARE_SOAK.maxRecords,
        maxWaitTimeInMs: 250,
        acknowledgementMode: 'implicit',
      })
    )
    const { events } = this.client
    this.removers = [
      this.client.on(events.GROUP_JOIN, ({ payload }) => {
        this.memberId = payload.memberId
        this.onProgress('member-joined', { memberId: this.memberId })
      }),
      this.client.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        this.assignment = payload.assignment
        this.onProgress('assignment-changed', { assignment: this.assignment })
      }),
      this.client.on(events.ACKNOWLEDGE, ({ payload }) => {
        this.acks.push(payload)
        const { partition, firstOffset, lastOffset, type, error } = payload
        this.onProgress('acknowledgement', {
          partition,
          firstOffset,
          lastOffset,
          type,
          error: error ? { name: error.name, message: error.message } : null,
        })
      }),
      this.client.on(events.SESSION_RESET, ({ payload }) => {
        this.resets.push(payload)
        this.onProgress('SESSION_RESET', payload)
      }),
      this.client.on(events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
    ]
    if (events.HEARTBEAT)
      this.removers.push(
        this.client.on(events.HEARTBEAT, ({ payload }) => this.onProgress('heartbeat', payload))
      )
  }

  /**
   * @param {AbortSignal} scope the cycle scope
   * @param {Function} bounded the live-run bound
   * @returns {Promise<void>} once the client is running
   */
  async start(scope, bounded) {
    this.onProgress('connect-start')
    await bounded(scope, Infinity, () => this.client.connect())
    this.onProgress('connect-end')
    await bounded(scope, Infinity, () => this.client.subscribe({ topics: [this.topic] }))
    this.onProgress('join-start')
    await bounded(scope, Infinity, () =>
      this.client.run({
        eachBatch: async ({ batch }) => {
          const entries = batch.messages.map((message) => ({
            identity: this.fixtures.shareRecordIdentity({
              ...message,
              topic: batch.topic,
              partition: batch.partition,
            }),
            deliveryCount: message.deliveryCount,
            ...this.journal?.handlers.getStore(),
          }))
          const first = this.entries.length === 0 && entries.length > 0
          this.entries.push(...entries)
          if (first) this.onProgress('first-delivery', { partition: batch.partition })
          if (this.finalized) this.onLateEntry(entries)
          await this.onBatch(entries, batch.partition)
        },
      })
    )
    this.onProgress('run-ready')
  }

  /**
   * Leaves the group and removes the journals' listeners, even if a progress sink fails.
   * @returns {Promise<void>}
   */
  async stop() {
    this.stopTask ??= (async () => {
      let status = 'failed'
      try {
        try {
          this.onProgress('disconnect-start')
        } finally {
          await this.client.disconnect({ timeout: 2000 })
          status = 'completed'
          this.disconnected = true
        }
      } finally {
        try {
          this.onProgress('disconnect-end', { status })
        } finally {
          this.removers.splice(0).forEach((remove) => remove())
        }
      }
    })()
    return this.stopTask
  }

  /** @returns {object} Current public observations without declaring the handler joined. */
  snapshot() {
    return {
      member: this.label,
      memberId: this.memberId,
      assignment: this.assignment,
      handler: this.entries,
      acknowledgements: this.acks.map(({ error, ...ack }) => ({
        ...ack,
        error: error ? { name: error.name, message: error.message, code: error.code } : null,
      })),
      resets: this.resets,
      crashes: this.crashes.map(({ name, message }) => ({ name, message })),
      disconnected: this.disconnected === true,
      finalized: this.finalized === true,
    }
  }

  /** @returns {object[]} The complete joined journal; any later callback invalidates the run. */
  finalize() {
    this.finalized = true
    return this.entries.map((entry) => ({ ...entry }))
  }

  /** @returns {Array<{ partition: number, firstOffset: string, lastOffset: string }>} successful Accepts */
  accepted() {
    return this.acks
      .filter(({ type, error }) => type === 'accept' && !error)
      .map(({ topic, partition, firstOffset, lastOffset }) => ({
        topic,
        partition,
        firstOffset,
        lastOffset,
      }))
  }

  /** @returns {Set<string>} the distinct identities this member's handler received */
  distinct() {
    return new Set(this.entries.map(({ identity }) => identity))
  }
}

/**
 * The live side of the share soak: every call that touches the cluster. It owns the fixed topic and
 * group, builds the cycle's clients through the public factory and returns what each independent
 * oracle saw, never a verdict. It registers each cleanup before the effect it undoes.
 */
class ShareSoakDriver {
  /**
   * @param {object} options
   * @param {object} options.kafka the counted client
   * @param {string[]} options.brokers seed addresses, `host:port`
   * @param {{ snapshot: () => Promise<object>, track: (client: object) => object }} options.shares
   * @param {ShareFetchTap} options.tap observer of the ShareFetch writes
   * @param {object} options.ports share fixtures, diagnostics and live-run helpers
   * @param {Function} options.socketFactory Counted original socket factory for raw broker ownership.
   * @param {Function} [options.onProgress] Archives cycle steps as they occur, including failures.
   */
  constructor({
    kafka,
    brokers,
    shares,
    tap,
    ports,
    socketFactory,
    onProgress = () => {},
    prefix = 'soak-share',
  }) {
    Object.assign(this, { kafka, brokers, shares, tap, ports, socketFactory, onProgress, prefix })
    this.admin = null
    this.producer = null
    this.leaders = new Map()
    this.nodes = new Map()
    this.completedAt = null
    this.acknowledged = []
  }

  /**
   * Creates the topic and group, reads the group configuration back, captures routing and the
   * unrelated groups, closes the setup clients and takes the client-free baseline.
   * Requests the API-60 form of cluster discovery for the mandatory protocol inventory.
   * @param {{ add: Function }} stack cleanup stack; every cleanup is registered before its effect
   * @param {AbortSignal} scope the scenario scope
   * @returns {Promise<{ topic: string, group: string }>}
   * @throws {Error} the first setup failure
   */
  async prepare(stack, scope) {
    const { bounded } = this.ports.bounded
    const { fixtures } = this.ports
    const nonce = require('node:crypto').randomUUID()
    this.topic = `${this.prefix}-${nonce}`
    this.group = `${this.prefix}-group-${nonce}`
    stack.add('prove the share soak cleanup', (signal) => this.proveCleanup(signal))
    stack.add('delete the share soak topic', (signal) => this.deleteTopic(signal))
    stack.add('delete the share soak group', (signal) => this.deleteGroup(signal))
    stack.add('reset the share soak group configs', async (signal) => {
      await fixtures.resetShareGroupConfigs(
        await this.cleanupAdmin(signal),
        this.group,
        GROUP_CONFIG_NAMES,
        { signal }
      )
    })
    stack.add('disconnect the share soak phase clients', () => this.closePhase())

    const admin = this.kafka.admin()
    this.admin = admin
    await bounded(scope, Infinity, () => admin.connect())
    await bounded(scope, Infinity, () =>
      admin.createTopics({
        topics: [
          {
            topic: this.topic,
            numPartitions: SHARE_SOAK.partitions,
            replicationFactor: 3,
            configEntries: [
              { name: 'min.insync.replicas', value: '2' },
              { name: 'retention.ms', value: '-1' },
              { name: 'cleanup.policy', value: 'delete' },
            ],
          },
        ],
        waitForLeaders: true,
      })
    )
    await bounded(scope, Infinity, () =>
      fixtures.setShareGroupConfigs(admin, this.group, {
        'share.auto.offset.reset': 'earliest',
        'share.record.lock.duration.ms': SHARE_SOAK.lockMs,
      })
    )
    const metadata = await bounded(scope, Infinity, () =>
      admin.fetchTopicMetadata({ topics: [this.topic] })
    )
    const [topic] = metadata.topics.filter(({ name }) => name === this.topic)
    const { brokers } = await bounded(scope, Infinity, () =>
      admin.describeCluster({ includeAuthorizedOperations: true })
    )
    this.nodes = new Map(brokers.map(({ host, port, nodeId }) => [`${host}:${port}`, nodeId]))
    for (const { partitionId, leader } of topic.partitions) {
      const node = brokers.find(({ nodeId }) => nodeId === leader)
      if (!node) throw new Error(`Partition ${partitionId} has no known leader`)
      this.leaders.set(partitionId, `${node.host}:${node.port}`)
    }
    this.topicId = topic.topicId
    this.configuration = await this.readConfiguration(admin, brokers, scope)
    const { groups } = await bounded(scope, Infinity, () => admin.listGroups())
    this.unrelated = groups.map(({ groupId }) => groupId).filter((id) => id !== this.group)
    await bounded(scope, Infinity, () => admin.disconnect())
    this.admin = null
    this.baseline = await fixtures.captureShareCleanupBaseline(this.shares, { signal: scope })

    return { topic: this.topic, group: this.group }
  }

  /**
   * Reads effective group timings and each owned broker's defaults and bounds without changing
   * them. Missing group values fall back only to that broker's observed value, never a constant.
   * Raw resources retain configuration sources and synonyms for independent reconstruction.
   * @param {object} admin Connected owned admin.
   * @param {object[]} brokers Original cluster node identities and endpoints.
   * @param {AbortSignal} scope Original setup scope.
   * @returns {Promise<object>} Original reads, effective values and their fallback sources.
   * @throws {Error} On failed, missing or unusable configuration readback.
   */
  async readConfiguration(admin, brokers, scope) {
    const { GROUP, BROKER } = require('../../src/protocol/configResourceTypes')
    const timings = ['heartbeat.interval.ms', 'session.timeout.ms', 'assignment.interval.ms']
    const groupNames = [...GROUP_CONFIG_NAMES, ...timings.map((key) => `share.${key}`)]
    const brokerNames = [
      ...timings.flatMap((key) => [
        `group.share.${key}`,
        `group.share.min.${key}`,
        `group.share.max.${key}`,
      ]),
      'group.share.assignors',
    ]
    const queries = [
      { type: GROUP, name: this.group, configNames: groupNames },
      ...brokers.map(({ nodeId }) => ({
        type: BROKER,
        name: String(nodeId),
        configNames: brokerNames,
      })),
    ]
    const startedAt = SHARE_CLOCK.now()
    const results = await Promise.allSettled(
      queries.map(async (query) => {
        const response = await this.ports.bounded.bounded(scope, Infinity, () =>
          admin.describeConfigs({ resources: [query], includeSynonyms: true })
        )
        const resource = response.resources.find(
          ({ resourceType, resourceName }) =>
            resourceType === query.type && resourceName === query.name
        )
        if (!resource || resource.errorCode !== 0)
          throw new Error('The share soak configuration readback failed')
        return { query, resource, completedAt: SHARE_CLOCK.now() }
      })
    )
    const failed = results.find(({ status }) => status === 'rejected')
    if (failed) throw failed.reason
    const reads = results.map(({ value }) => value)
    const entry = (read, name) =>
      read.resource.configEntries.find(({ configName }) => configName === name)
    const usable = (value) => value?.configValue != null && Number.isInteger(value.configSource)
    const effective = brokers.map((broker, index) => {
      const read = reads[index + 1]
      if (brokerNames.some((name) => !usable(entry(read, name))))
        throw new Error('The share soak broker configuration is incomplete')
      return {
        ...broker,
        timings: Object.fromEntries(
          timings.map((key) => {
            const group = entry(reads[0], `share.${key}`)
            const fallback = !usable(group)
            return [
              key,
              {
                ...(fallback ? entry(read, `group.share.${key}`) : group),
                resourceType: fallback ? BROKER : GROUP,
                resourceName: fallback ? String(broker.nodeId) : this.group,
                fallback,
              },
            ]
          })
        ),
      }
    })
    return {
      groupId: this.group,
      topic: this.topic,
      topicId: this.topicId,
      startedAt,
      completedAt: SHARE_CLOCK.now(),
      reads,
      effective,
    }
  }

  /**
   * @param {AbortSignal} signal cleanup scope
   * @returns {Promise<object>} a connected admin, opened when none is
   */
  async cleanupAdmin(signal) {
    if (!this.admin) {
      this.admin = this.kafka.admin()
      await this.ports.bounded.bounded(signal, Infinity, () => this.admin.connect())
    }

    return this.admin
  }

  /**
   * Connects the admin and the producer a phase's cycles share.
   * @param {AbortSignal} scope the scenario scope
   * @returns {Promise<void>}
   */
  async openPhase(scope) {
    const { bounded } = this.ports.bounded
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer({ idempotent: true })
    await bounded(scope, Infinity, () => this.admin.connect())
    await bounded(scope, Infinity, () => this.producer.connect())
  }

  /**
   * Disconnects what `openPhase` connected; safe when nothing is open.
   * @returns {Promise<void>}
   */
  async closePhase() {
    const { admin, producer } = this
    this.admin = null
    this.producer = null
    const failures = []
    for (const client of [producer, admin]) {
      if (client) await client.disconnect().catch((error) => failures.push(error))
    }
    if (failures.length > 0) throw failures[0]
  }

  /**
   * Runs one cycle: A joins, a transient B joins and leaves, the corpus is produced and delivered
   * (with held identities redelivered and all three leaders fetched after a reset before A closes),
   * and every independent oracle is read.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {AbortSignal} scope the cycle's slot; every await below ends with it
   * @param {{due?: number, clock?: {now: Function}}} [timing] Original absolute slot cutoff and clock.
   * @returns {Promise<object>} the observations `ShareCycleOracle.judge` checks
   * @throws {Error} a setup, reset or observation failure, after the cycle's clients are closed
   */
  async cycle(plan, scope, { due = Infinity, clock = SHARE_CLOCK } = {}) {
    const { bounded, pollUntil } = this.ports.bounded
    const { fixtures, probe } = this.ports
    const hold = { entries: null, partition: null, release: null }
    const stack = new probe.CleanupStack()
    const journal = new ShareCycleJournal(this.group, this.tap)
    journal.install()
    stack.add('restore the cycle journal', () => {
      try {
        observation.provenance = journal.snapshot()
        observation.memberObservations = members.map((member) => member.snapshot())
        observation.snapshot = {
          at: clock.now(),
          workloadJoined:
            members.length === 2 &&
            members.every((member) => member.disconnected && member.finalized),
        }
        if (active.failureArchived) this.onCycleCleanup?.(capture())
      } finally {
        unlisten()
        journal.restore()
        active.restored = true
      }
    })
    const observation = {
      members: { aJoined: false, aLeft: false, bAdded: false, bRemoved: false },
      reset: null,
      held: [],
      positions: [],
      steps: [],
      timing: { cycle: plan.cycle, kind: 'cycle', startedAt: clock.now(), due },
    }
    const note = (step, fields = {}, at = clock.now()) => {
      const row = {
        cycle: plan.cycle,
        step,
        at,
        elapsedMs: at - observation.timing.startedAt,
        due,
        remainingMs: due - at,
        ...fields,
      }
      observation.steps.push(row)
      this.onProgress(row)
    }
    const members = []
    const capture = () =>
      JSON.parse(
        JSON.stringify({
          ...observation,
          provenance: journal.snapshot(),
          handler: members.flatMap((member) => member.entries),
          accepted: members.flatMap((member) => member.accepted()),
          memberObservations: members.map((member) => member.snapshot()),
          snapshot: {
            at: clock.now(),
            workloadJoined:
              members.length === 2 &&
              members.every((member) => member.disconnected && member.finalized),
          },
        })
      )
    const active = { capture, observation, failureArchived: false }
    this.activeCycle = active
    const postResetNodes = new Set()
    const unlisten = this.tap.listen((step, fields, at) => {
      if (step.startsWith('leave-') && fields.groupId === this.group) {
        const member = members.find((member) => member.memberId === fields.memberId)
        note(step, { member: member?.label ?? null, ...fields }, at)
        return
      }
      const target = observation.resetTarget
      if (!target) return
      const nodeId = this.nodes.get(fields.endpoint)
      if (step === 'api78-write') {
        if (
          fields.memberId !== target.memberId ||
          fields.groupId !== this.group ||
          postResetNodes.has(fields.endpoint)
        )
          return
        postResetNodes.add(fields.endpoint)
        note('post-reset-api78', { ...fields, nodeId }, at)
      } else if (fields.socket === target.socket) {
        note(step, { ...fields, nodeId }, at)
      } else if (fields.endpoint === target.endpoint && step.startsWith('socket-connect-')) {
        note(step.replace('socket-connect-', 'reconnect-'), { ...fields, nodeId }, at)
      }
    })
    let allAcknowledged = false
    const noteAcknowledgements = () => {
      if (allAcknowledged || !observation.produced) return
      const accepted = members.flatMap((member) => member.accepted())
      if (
        observation.produced.every(({ partition, identity }) => {
          const offset = BigInt(JSON.parse(identity)[2])
          return accepted.some(
            (range) =>
              range.topic === this.topic &&
              range.partition === partition &&
              BigInt(range.firstOffset) <= offset &&
              BigInt(range.lastOffset) >= offset
          )
        })
      ) {
        allAcknowledged = true
        note('all-acks', { identities: observation.produced.length })
      }
    }
    stack.add('finalize workload journals', () => {
      observation.handler = members.flatMap((member) => member.finalize())
      observation.accepted = members.flatMap((member) => member.accepted())
    })
    const member = (label, onBatch) =>
      new ShareMember({
        kafka: this.kafka,
        shares: this.shares,
        label,
        group: this.group,
        topic: this.topic,
        fixtures,
        journal,
        onLateEntry: (entries) => this.onLateEntry?.(entries),
        onProgress: (step, fields) => {
          note(step, { member: label, ...fields })
          if (step === 'acknowledgement') noteAcknowledgements()
          if (
            step === 'disconnect-end' &&
            active.restored &&
            active.failureArchived &&
            members.every((member) => member.disconnected && member.finalized)
          )
            this.onCycleCleanup?.(capture())
        },
        onBatch,
      })
    const track = (client) => {
      members.push(client)
      stack.add(`disconnect ${client.label}`, () => client.stop())
      return client
    }

    this.lastCycle = observation
    return stack.run(
      async () => {
        note('cycle-start')
        const a = track(
          member('A', async (entries, partition) => {
            if (observation.resetTarget)
              for (const entry of entries)
                if (observation.held.includes(entry.identity))
                  note('held-redelivery', { member: 'A', partition, ...entry })
            if (!plan.reset || hold.entries) return
            hold.entries = entries.map(({ identity }) => identity)
            hold.partition = partition
            hold.acquisition = entries[0]?.acquisition
            hold.dispatch = entries[0]?.dispatch
            await new Promise((resolve) => {
              hold.release = resolve
            })
          })
        )
        stack.add('release the held handler', () => hold.release?.())
        await a.start(scope, bounded)
        note('a-membership-start')
        await this.awaitMembers(scope, [a])
        observation.members.aJoined = true
        note('a-membership-end')
        const b = track(member('B', async () => {}))
        note('member-add-start')
        await b.start(scope, bounded)
        await this.awaitMembers(scope, [a, b])
        observation.members.bAdded = true
        note('member-add-end')
        note('member-remove-start')
        await b.stop()
        if (!ShareCycleOracle.departure(journal, this.group, b.memberId))
          throw new Error(`Member B (${b.memberId}) has no original successful leave exchange`)
        await this.awaitMembers(scope, [a])
        observation.members.bRemoved = b.entries.length === 0
        observation.members.bRemovedAt = clock.now()
        note('member-remove-end')

        const mark = this.tap.mark()
        if (plan.reset) {
          note('sessions-start')
          await this.awaitEstablished(scope, mark)
          note('sessions-end')
        }
        note('produce-start')
        observation.produced = await this.produce(plan, scope)
        note('produce-end')
        noteAcknowledgements()
        this.acknowledged.push(...observation.produced.map(({ identity }) => identity))
        observation.acknowledgedPrefix = [...this.acknowledged]
        if (plan.reset) {
          await pollUntil(scope, Infinity, () => hold.entries !== null, {
            delayMs: 25,
            label: 'The first batch reaching its handler',
          })
          note('reset-start')
          observation.reset = await this.forceReset(scope, a, hold, mark, observation)
          note('reset-end')
        }
        await pollUntil(
          scope,
          Infinity,
          () => {
            if (a.crashes.length > 0) throw a.crashes[0]
            const counts = new Map()
            for (const { identity } of a.entries)
              counts.set(identity, (counts.get(identity) ?? 0) + 1)
            return (
              counts.size >= plan.records.length &&
              observation.held.every((identity) => counts.get(identity) >= 2) &&
              (!plan.reset || postResetNodes.size === 3)
            )
          },
          { delayMs: 25, label: 'Every identity delivered and all reset nodes progressed' }
        )
        note('all-delivered')
        const reads = await Promise.allSettled([
          (async () => {
            note('raw-log-start')
            const log = await this.rawLog(scope)
            Object.assign(observation, this.readLogs(plan, log))
            note('raw-log-end')
          })(),
          (async () => {
            await a.stop()
            const leave = ShareCycleOracle.departure(journal, this.group, a.memberId)
            observation.members.aLeft = Boolean(leave) && a.crashes.length === 0
            if (leave) observation.members.aLeftAt = leave.answeredAt
            noteAcknowledgements()
          })(),
        ])
        const failures = reads
          .filter(({ status }) => status === 'rejected')
          .map(({ reason }) => reason)
        if (failures.length === 1) throw failures[0]
        if (failures.length > 1) throw new AggregateError(failures, 'Cycle read and close failed')
        observation.handler = [...a.finalize(), ...b.finalize()]
        observation.accepted = a.accepted()
        observation.provenance = journal.snapshot()
        note('position-before-start')
        observation.positions.push(await this.position(plan, scope, { clock }))
        note('position-before-end')
        note('group-empty-start')
        try {
          await bounded(scope, Infinity, () =>
            fixtures.waitForShareGroupsEmpty(this.admin, [this.group], {
              signal: scope,
              record: (step, { group }) => {
                const at = clock.now()
                const described = JSON.parse(JSON.stringify(group))
                journal.groupDescriptions.push({ at, group: described })
                note(step, { group: described }, at)
              },
            })
          )
        } catch (error) {
          const latest = journal.groupDescriptions.at(-1)?.group
          const retained = latest?.members?.map(({ memberId }) => memberId) ?? []
          throw new Error(
            `${error.message}; last described members still listed in ${this.group}: ` +
              (retained.length ? retained.join(', ') : '(none observed)'),
            { cause: error }
          )
        }
        if (!observation.members.aLeft)
          throw new Error(`Member A (${a.memberId}) has no original successful leave exchange`)
        observation.members.emptyAt = clock.now()
        note('group-empty-end', { emptyAt: observation.members.emptyAt })
        if (due - clock.now() < SHARE_SOAK.quietMs.cycle)
          throw new Error('The cycle cannot contain its complete verifier window')
        note('verifier-start')
        observation.verifier = await this.quiet(scope, SHARE_SOAK.quietMs.cycle, {
          due,
          clock,
          journal,
          cycle: plan.cycle,
          onProgress: (step) => note(`verifier-${step.replaceAll(' ', '-')}`),
        })
        note('verifier-end')
        note('position-after-start')
        const final = await this.position(plan, scope, { clock })
        observation.positions.push(final)
        note('position-after-end')
        observation.broker = final.rows
        observation.clientsDisconnected = true
        observation.timing.completedAt = clock.now()
        note('cycle-end')
        this.departures = { ...observation.members }
        return observation
      },
      { budgetMs: 2000 }
    )
  }

  /**
   * Freezes active provenance and partial public observations at the failure archive boundary.
   * Cleanup may still be running; its later snapshot is archived separately.
   * @returns {object|undefined} Detached evidence without finalizing a member.
   */
  snapshotCycle() {
    if (!this.activeCycle || this.activeCycle.observation !== this.lastCycle) return this.lastCycle
    this.activeCycle.failureArchived = true
    return this.activeCycle.capture()
  }

  /**
   * Waits until the admin API and the clients report the same members, each with the assignment it
   * reports publicly, and the three partitions are covered.
   * @param {AbortSignal} scope the cycle scope
   * @param {ShareMember[]} members the members that must be present, and only them
   * @returns {Promise<void>}
   */
  async awaitMembers(scope, members) {
    const { bounded, pollUntil } = this.ports.bounded
    const normal = (assignment) =>
      JSON.stringify(
        assignment
          .map(({ topic, partitions }) => [topic, [...partitions].sort((a, b) => a - b)])
          .sort()
      )
    await pollUntil(
      scope,
      Infinity,
      async (poll) => {
        if (members.some((member) => member.memberId === null || member.assignment.length === 0))
          return false
        const { groups } = await bounded(poll, Infinity, () =>
          this.admin.describeShareGroups([this.group], { apiTimeout: 3000 })
        )
        const described = groups[0]?.members ?? []
        const covered = new Set(
          members.flatMap(({ assignment }) => assignment.flatMap((a) => a.partitions))
        )
        return (
          described.length === members.length &&
          covered.size === SHARE_SOAK.partitions &&
          members.every((member) => {
            const found = described.find(({ memberId }) => memberId === member.memberId)
            return found && normal(found.assignment) === normal(member.assignment)
          })
        )
      },
      { delayMs: 25, label: 'The member and assignment barrier' }
    )
  }

  /**
   * Waits until A's ShareFetch to every leader has an established session, so the reset has an
   * original data slot to select.
   * @param {AbortSignal} scope the cycle scope
   * @param {number} mark the tap position after B left
   * @returns {Promise<void>}
   */
  async awaitEstablished(scope, mark) {
    await this.ports.bounded.pollUntil(
      scope,
      Infinity,
      () =>
        [...new Set(this.leaders.values())].every((endpoint) =>
          this.tap.since(mark, endpoint).some(({ epoch }) => epoch > 0)
        ),
      { delayMs: 25, label: 'Established share sessions on every leader' }
    )
  }

  /**
   * Produces the cycle's corpus with concurrent partition sends, joins every send, and derives
   * each identity from the acknowledged base offset and the known ordered payloads.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {AbortSignal} scope the cycle scope
   * @returns {Promise<Array<{ partition: number, identity: string }>>} one entry per record, in plan order
   * @throws {Error} when an acknowledgement is not exactly one clean batch at the cycle's offset
   */
  async produce(plan, scope) {
    const { bounded } = this.ports.bounded
    const produced = new Array(plan.records.length)
    const sends = await Promise.allSettled(
      Array.from({ length: SHARE_SOAK.partitions }, async (_, partition) => {
        const rows = plan.records
          .map((record, index) => ({ record, index }))
          .filter(({ record }) => record.partition === partition)
        const messages = rows.map(({ record }) => ({
          partition,
          key: Buffer.from(record.key),
          value: Buffer.from(record.value),
          timestamp: SHARE_SOAK_TIMESTAMP,
        }))
        const sent = await bounded(scope, Infinity, () =>
          this.producer.send({ topic: this.topic, acks: -1, messages })
        )
        if (
          sent.length !== 1 ||
          sent[0].errorCode !== 0 ||
          sent[0].partition !== partition ||
          BigInt(sent[0].baseOffset) !== plan.first
        ) {
          throw new Error(
            `Partition ${partition} was not acknowledged as one batch at offset ${plan.first}`
          )
        }
        rows.forEach(({ index }, position) => {
          produced[index] = {
            partition,
            identity: this.ports.fixtures.shareRecordIdentity({
              topic: this.topic,
              partition,
              offset: String(plan.first + BigInt(position)),
              key: messages[position].key,
              value: messages[position].value,
            }),
          }
        })
      })
    )
    const failures = sends.filter(({ status }) => status === 'rejected').map(({ reason }) => reason)
    if (failures.length === 1) throw failures[0]
    if (failures.length > 1) throw new AggregateError(failures, 'Cycle partition sends failed')

    return produced
  }

  /**
   * Destroys A's original established data socket while its first batch is held, releases the
   * handler, and observes the session reset and the new epoch-zero open on a new socket.
   * @param {AbortSignal} scope the cycle scope
   * @param {ShareMember} member A
   * @param {{ entries: string[], partition: number, acquisition: number, dispatch: number,
   *   release: Function }} hold the held batch
   * @param {number} mark the tap position after B left
   * @param {object} observation receives the frozen held set before the fault
   * @returns {Promise<object>} the observed reset and reopen
   * @throws {Error} when no established slot exists or no reset follows
   */
  async forceReset(scope, member, hold, mark, observation) {
    const endpoint = this.leaders.get(hold.partition)
    const old = this.tap
      .since(mark, endpoint)
      .filter(({ epoch }) => epoch > 0)
      .at(-1)
    if (!old) throw new Error(`No established ShareFetch slot on ${endpoint}`)
    observation.held = [...hold.entries]
    observation.resetTarget = {
      socket: old.socket,
      endpoint,
      nodeId: this.nodes.get(endpoint),
      memberId: member.memberId,
      epoch: old.epoch,
      partition: hold.partition,
      acquisition: hold.acquisition,
      dispatch: hold.dispatch,
      held: observation.held,
    }
    const target = observation.resetTarget
    this.tap.notify('reset-target', target)
    this.tap.destroy(old.socket)
    hold.release()
    let next
    await this.ports.bounded.pollUntil(
      scope,
      Infinity,
      () => {
        next = this.tap
          .since(old.sequence, endpoint)
          .find(({ epoch, socket }) => epoch === 0 && socket !== old.socket)
        return member.resets.length > 0 && next !== undefined
      },
      { delayMs: 25, label: 'The session reset and its epoch-zero reopen' }
    )

    return {
      apiKey: 78,
      nodeId: member.resets[0].nodeId,
      reason: member.resets[0].reason,
      oldSocket: old.socket,
      oldEpoch: old.epoch,
      nextSocket: next.socket,
      nextEpoch: next.epoch,
    }
  }

  /**
   * Polls the broker's terminal position of every partition against the cycle's cumulative end.
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {AbortSignal} scope the cycle scope
   * @param {object} [timing] Monotonic clock and cycle/final observation kind.
   * @returns {Promise<object>} Both raw listings, ownership, independent ends and normalized rows.
   */
  async position(plan, scope, { clock = SHARE_CLOCK, kind = 'cycle' } = {}) {
    const startedAt = clock.now()
    const target = {
      groupId: this.group,
      topic: this.topic,
      partitions: Array.from({ length: SHARE_SOAK.partitions }, (_, partition) => ({
        partition,
        floor: '0',
        end: plan.end,
      })),
    }
    const observations = []
    const result = await this.ports.diagnostics.awaitTerminalSharePosition(this.admin, target, {
      signal: scope,
      timeoutMs: 3000,
      record: (row) => observations.push(JSON.parse(JSON.stringify(row))),
    })
    return {
      target,
      ...result,
      ...observations.at(-1)?.last,
      observations,
      cycle: plan.cycle,
      kind,
      startedAt,
      completedAt: clock.now(),
    }
  }

  /**
   * Reads through counted original raw brokers and reports disconnect failures the reader suppresses.
   * @param {AbortSignal} scope Original cycle or cleanup deadline.
   * @returns {Promise<Map<number, object>>} Independent cumulative raw log.
   * @throws {Error} On a read, connect or disconnect failure.
   */
  async rawLog(scope) {
    return this.tap?.operation
      ? this.tap.operation('raw', 'readLog', () => this.readRawLog(scope))
      : this.readRawLog(scope)
  }

  /**
   * Joins independent original raw-broker connections, including failure paths.
   * @param {AbortSignal} scope Original enclosing cutoff.
   * @returns {Promise<Map<number, object>>} Original cumulative raw log.
   * @throws {Error} Read or joined disconnect failure.
   */
  async readRawLog(scope) {
    const failures = []
    const owned = []
    const open = async ({ host, port }) => {
      const logger = createLogger({ level: logLevel.NOTHING, logCreator: LoggerConsole })
      const broker = new Broker({
        connectionPool: new ConnectionPool({
          host,
          port,
          logger,
          socketFactory: this.socketFactory,
          connectionTimeout: 3000,
          requestTimeout: 30000,
        }),
        logger,
      })
      const disconnect = broker.disconnect.bind(broker)
      let attempted = false
      broker.disconnect = async () => {
        if (attempted) return
        attempted = true
        try {
          await this.ports.bounded.bounded(scope, Infinity, () => disconnect())
        } catch (error) {
          failures.push(error)
          throw error
        }
      }
      owned.push(broker)
      await this.ports.bounded.bounded(scope, Infinity, () => broker.connect())
      return broker
    }
    let result
    let primary
    try {
      result = await this.ports.bounded.bounded(scope, Infinity, () =>
        (this.ports.readLog ?? readLog)({ brokers: this.brokers, topic: this.topic, open })
      )
    } catch (error) {
      primary = error
    } finally {
      for (const broker of owned) await broker.disconnect().catch(() => {})
    }
    if (primary) {
      primary.cleanupErrors = failures
      throw primary
    }
    if (failures.length) throw new AggregateError(failures, 'Raw reader cleanup failed')
    return result
  }

  /**
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {Map<number, { endOffset: string, records: object[] }>} log the raw log by partition
   * @returns {{ log: string[], prefix: string[], logs: Record<number, { end: string, count: number }> }}
   *   Current-cycle identities, every cumulative raw identity, and independently read ends/counts.
   */
  readLogs(plan, log) {
    const identities = []
    const prefix = []
    const logs = {}
    for (const [partition, { endOffset, records }] of log) {
      logs[partition] = { end: endOffset, count: records.length }
      for (const record of records) {
        const identity = this.ports.fixtures.shareRecordIdentity({
          topic: this.topic,
          partition,
          offset: String(record.offset),
          key: record.key,
          value: record.value,
        })
        prefix.push(identity)
        if (BigInt(record.offset) >= plan.first) identities.push(identity)
      }
    }

    return { log: identities, logs, prefix }
  }

  /**
   * Reads the group with one fresh assigned member for `windowMs`; the fixture refuses a member
   * that was never assigned or never fetched, so silence from an inactive member never counts.
   * @param {AbortSignal} scope the enclosing scope
   * @param {number} windowMs how long the assigned member fetches
   * @param {object} [timing] Original slot, member journal, monotonic clock and progress callback.
   * @returns {Promise<object>} Joined verifier identity, assignments, fetch chronology and deliveries.
   */
  async quiet(
    scope,
    windowMs,
    {
      due = Infinity,
      clock = SHARE_CLOCK,
      journal,
      cycle = SHARE_SOAK.cycles - 1,
      kind = 'cycle',
      onProgress = () => {},
    } = {}
  ) {
    const ownJournal = !journal
    journal ??= new ShareCycleJournal(this.group, this.tap)
    if (ownJournal) journal.install()
    const events = []
    let client
    const removers = []
    const record = (kind, payload) =>
      events.push({ kind, at: clock.now(), wallAt: Date.now(), payload })
    const kafka = {
      shareConsumer: (options) => {
        client = this.shares.track(this.kafka.shareConsumer(options))
        for (const kind of ['GROUP_JOIN', 'ASSIGNMENT_CHANGED', 'REQUEST', 'FETCH', 'DISCONNECT']) {
          if (client.events[kind])
            removers.push(client.on(client.events[kind], ({ payload }) => record(kind, payload)))
        }
        const assignment = client.assignment
        // The public consumer is frozen: observe through a copy, never by mutating it.
        return {
          ...client,
          assignment: () => {
            const current = assignment()
            record('assignment', current)
            return current
          },
        }
      },
    }
    const openedAt = clock.now()
    try {
      const delivered = await this.ports.fixtures.verifierMember(this.group, this.topic, {
        kafka,
        signal: scope,
        windowMs,
        record: ({ step, assignment }) => {
          onProgress(step)
          if (step === 'window start') {
            if (due - clock.now() < windowMs)
              throw new Error('The cycle cannot contain its complete assigned verifier window')
            record('window-start', assignment)
          } else if (step === 'window end') record('window-end', assignment)
        },
      })
      return {
        active: true,
        cycle,
        kind,
        provenance: journal.snapshot(),
        groupId: this.group,
        topic: this.topic,
        topicId: this.topicId,
        memberId: events.find(({ kind }) => kind === 'GROUP_JOIN')?.payload.memberId,
        windowMs,
        openedAt,
        closedAt: clock.now(),
        events,
        delivered: delivered.map(({ identity }) => identity),
      }
    } finally {
      if (ownJournal) journal.restore()
      removers.forEach((remove) => remove())
    }
  }

  /**
   * The final proof after the last cycle, before any cleanup: the active same-group check for the
   * final window, then the terminal position at the last cumulative end.
   * @param {AbortSignal} scope the scenario scope
   * @param {object} [timing] Original cleanup cutoff and monotonic clock.
   * @returns {Promise<{ broker: object[], verifier: object }>}
   */
  async final(scope, { clock = SHARE_CLOCK, due = Infinity } = {}) {
    const startedAt = clock.now()
    const plan = ShareCycleOracle.plan(SHARE_SOAK.cycles - 1)
    await this.cleanupAdmin(scope)
    const members = { ...this.departures }
    const before = await this.position(plan, scope, { clock, kind: 'final' })
    await this.ports.fixtures.waitForShareGroupsEmpty(this.admin, [this.group], { signal: scope })
    members.emptyAt = clock.now()
    const verifier = await this.quiet(scope, SHARE_SOAK.quietMs.final, {
      clock,
      due,
      cycle: plan.cycle,
      kind: 'final',
    })
    const position = await this.position(plan, scope, { clock, kind: 'final' })
    const raw = this.readLogs(plan, await this.rawLog(scope))
    return {
      verifier,
      members,
      timing: { cycle: plan.cycle, kind: 'final', startedAt, completedAt: clock.now(), due },
      broker: position.rows,
      positions: [before, position],
      ...raw,
      acknowledgedPrefix: [...this.acknowledged],
    }
  }

  /**
   * Deletes the fixed topic and waits until no broker lists it.
   * @param {AbortSignal} signal cleanup scope
   * @returns {Promise<void>}
   */
  async deleteTopic(signal) {
    const { bounded, pollUntil } = this.ports.bounded
    const admin = await this.cleanupAdmin(signal)
    await bounded(signal, 10000, () =>
      admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
    await pollUntil(signal, 10000, async () => !(await admin.listTopics()).includes(this.topic), {
      label: 'The topic disappearing',
    })
  }

  /**
   * Deletes the fixed group once its members are gone.
   * @param {AbortSignal} signal cleanup scope
   * @returns {Promise<void>}
   */
  async deleteGroup(signal) {
    const { pollUntil } = this.ports.bounded
    const admin = await this.cleanupAdmin(signal)
    await pollUntil(
      signal,
      10000,
      async () => {
        try {
          await admin.deleteGroups([this.group])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      },
      { label: 'The group deletion' }
    )
  }

  /**
   * Proves the owned group and topic are absent on every broker and the measured owners are back at
   * the client-free baseline, then disconnects the admin. It is the last cleanup to run.
   * @param {AbortSignal} signal cleanup scope
   * @returns {Promise<void>}
   */
  async proveCleanup(signal) {
    const admin = await this.cleanupAdmin(signal)
    try {
      this.after = await this.ports.fixtures.assertShareCleanup(
        admin,
        [this.group],
        [this.topic],
        this.baseline,
        this.shares,
        { signal, unrelated: this.unrelated, disconnectAdmin: true }
      )
      this.completedAt = Number(process.hrtime.bigint() / 1000000n)
      this.cleanupClock = {
        origin: 'hrtime',
        completedAt: this.completedAt,
        performanceAt: performance.now(),
      }
    } finally {
      this.admin = null
      await admin.disconnect().catch(() => {})
    }
  }
}

/**
 * @param {object} snapshot a resource reading
 * @returns {object} its evidence form: open sockets, timers, listeners, queue depth, ledger size
 */
const resourceSummary = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((sum, count) => sum + count, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * Validates independently observed natural child exit on one established monotonic clock origin.
 * @param {object} observation Parent facts: origin, child entry, cleanup completion and exit.
 * @param {object} budget Frozen selector allocation.
 * @returns {boolean} Whether status, order, watchdog and both inclusive elapsed bounds hold.
 */
const shareSoakNaturalExit = (observation, budget) => {
  const {
    origin,
    cleanupOrigin,
    enteredAt,
    completedAt,
    exitedAt,
    code,
    signal,
    watchdog,
    forced,
  } = observation
  return (
    typeof origin === 'string' &&
    origin.length > 0 &&
    cleanupOrigin === origin &&
    [enteredAt, completedAt, exitedAt].every(Number.isFinite) &&
    enteredAt <= completedAt &&
    completedAt <= exitedAt &&
    code === 0 &&
    signal === null &&
    watchdog === false &&
    forced === false &&
    exitedAt - completedAt <= budget.exitMs &&
    exitedAt - enteredAt <= budget.totalMs
  )
}

/**
 * The owner of the share soak: three phases of ten cycles, each cycle inside its own slot, judged
 * by independent per-cycle oracles and, through the `Soak` driver, by the per-phase resource
 * screen. It owns the cleanup stack, the phase and cycle cursor and the evidence; every cluster
 * effect belongs to its driver, so each rule here runs against a scripted driver.
 *
 * A failure of any kind, an expired bound included, taints the live run, so a later case refuses
 * before it issues anything.
 */
class ShareSoak {
  /**
   * @param {object} options
   * @param {ShareSoakDriver} options.driver every cluster effect and observation
   * @param {object} options.kafka the counted client, handed to `Soak`
   * @param {{ snapshot: () => Promise<object>, restore: () => void }} options.shares the resource
   *   observer; `snapshot` is the probe every reading goes through
   * @param {{ add: Function, run: Function }} options.stack the cleanup stack the driver registers on
   * @param {boolean} options.short whether this is the 960 s profile rather than the 1920 s one
   * @param {{ write: (row: object) => void, read: () => object[] }} options.evidence where every row
   *   goes, and the rows the run wrote, in order
   * @param {{ taint: Function, assertClean: Function }} options.gate the live run's taint gate
   * @param {(scope: AbortSignal, ms: number) => Promise<void>} [options.pause] waits `ms` under a scope
   * @param {{ now: () => number }} [options.clock] the monotonic clock
   * @param {number} [options.slotMs] one cycle's slot; a spec shortens it
   * @param {object} [options.soak] `Soak` options a spec overrides
   * @param {Function} options.createPriming Builds the separate fixed first-use scope.
   */
  constructor({
    driver,
    kafka,
    shares,
    stack,
    short,
    evidence,
    gate,
    pause = delay,
    clock = SHARE_CLOCK,
    slotMs,
    soak = {},
    createPriming,
  }) {
    const mode = short ? 'short' : 'full'
    Object.assign(this, {
      driver,
      kafka,
      shares,
      stack,
      short,
      evidence,
      gate,
      pause,
      clock,
      soakOptions: soak,
      createPriming,
    })
    this.budget = SHARE_SOAK.budgets[mode]
    const { totalMs, primingMs, setupMs, profileMs, transitionMs, cleanupMs, exitMs } = this.budget
    // Only idle-wake overhead may use the unallocated margin; cycle work keeps its fixed cutoff.
    this.profileMarginMs =
      totalMs - (primingMs + setupMs + profileMs + transitionMs + cleanupMs + exitMs)
    this.slotMs = slotMs ?? SHARE_SOAK.slotMs[mode]
    this.phaseMs = (short ? SHARE_SOAK.shortMs : SHARE_SOAK.fullMs) / SHARE_SOAK.phases.length
    this.stem = short ? 'share-soak' : 'share-soak-full'
    this.nonce = require('node:crypto').randomUUID()
    this.scope = NEVER
    this.phasesDone = 0
    this.elapsedProfileMs = 0
    this.counts = { cycles: 0, resets: 0, memberAdds: 0, memberRemoves: 0 }
    this.closed = false
    this.spent = { priming: 0, setup: 0, transition: 0, profile: 0, cleanup: 0 }
    this.timing = { stages: [], phases: [], slots: [] }
    this.workDue = Infinity
    this.driver.onLateEntry = (entries) =>
      this.fail(
        new Error(
          `A joined workload member dispatched later identities: ${JSON.stringify(entries)}`
        )
      )
    this.driver.onProgress = ({ at, ...row }) =>
      this.row('soak-cycle-step', { ...row, monotonicAt: at })
    this.driver.onCycleCleanup = (observation) =>
      this.row('soak-cycle-cleanup', { cycle: observation.timing.cycle, observation })
  }

  /**
   * Runs setup, the three phases, the final proof and the cleanup inside the whole child's bound.
   * @returns {Promise<{ ok: boolean, violations: string[], resources: object, counts: object }>}
   *   the combined verdict; `ok` only when every cycle, oracle and resource check held
   * @throws {Error} a refusal of a tainted run, a failed or expired step, or a failed cleanup
   */
  async run() {
    this.gate.assertClean()
    if (this.startedAt !== undefined) {
      const error = new Error('The original share soak owner cannot be restarted')
      this.fail(error)
      throw error
    }
    this.startedAt = this.clock.now()
    this.workDue = this.startedAt + this.budget.totalMs - this.budget.exitMs
    try {
      const verdict = await bounded(
        NEVER,
        this.workDue - this.startedAt + 1,
        async (scope) => {
          this.scope = scope
          await this.stage('priming', (primeScope) => this.prime(primeScope))
          await this.prepare()
          const resources = await this.soak().run()
          return this.stage('cleanup', () => {
            this.restore()
            return this.complete(resources)
          })
        },
        { label: 'The share soak' }
      )
      if (!verdict.ok) this.fail(new Error(verdict.violations.join('; ')), verdict.violations)
      return verdict
    } catch (error) {
      this.fail(error)
      throw error
    }
  }

  /**
   * Executes one fixed first-use scope in this realm and releases its strong roots before the
   * boundary observation. Both boundary censuses must match the settled reference population;
   * its pre-collection transients remain archived and validated. Global observation history
   * remains installed until scored cleanup.
   * @param {AbortSignal} scope Original nested priming bound.
   * @returns {Promise<void>} Only a scalar completion link survives in this owner.
   * @throws {Error} Any priming or boundary failure; scored setup has not started.
   */
  async prime(scope) {
    if (this.priming || !this.createPriming)
      throw new Error('Exactly one priming scope is required')
    const due = Math.min(this.startedAt + SHARE_SOAK_PRIMING.totalMs, this.workDue)
    let prime = this.createPriming()
    this.row('priming-start', { ...SHARE_SOAK_PRIMING, startedAt: this.startedAt, due })
    const original = await bounded(
      scope,
      SHARE_SOAK_PRIMING.boundaryMs + 1,
      () => this.shares.snapshot(),
      { label: 'The priming boundary baseline' }
    )
    this.row('priming-boundary-reference', { snapshot: original })
    try {
      this.priming = await prime.execute(scope, this.startedAt, due - SHARE_SOAK_PRIMING.boundaryMs)
    } finally {
      prime = null
    }
    await bounded(
      scope,
      Math.min(SHARE_SOAK_PRIMING.boundaryMs, due - this.clock.now()) + 1,
      async () => {
        await new Promise((resolve) => setImmediate(resolve))
        const snapshot = await this.snapshot()
        const violations = []
        if (
          !Number.isSafeInteger(snapshot.heapUsed) ||
          snapshot.heapUsed < 0 ||
          !Number.isFinite(snapshot.heapAtMs) ||
          snapshot.heapAtMs < this.priming.completedAt - this.startedAt ||
          snapshot.heapAtMs > this.clock.now() - this.startedAt
        )
          violations.push('priming boundary: invalid heap source or elapsed timestamp')
        ShareSoakScreen.quiet(snapshot, original, 'priming boundary', violations)
        for (const [when, resource] of [
          ['beforeCollection', 'activeResourcesBeforeCollection'],
          ['afterCollection', 'activeResources'],
        ]) {
          const source = original.timerOwners?.[when]
          const counts = original[resource]
          if (
            !Array.isArray(source?.rows) ||
            source.dropped !== 0 ||
            !counts ||
            !Object.values(counts).every(ShareSoakScreen.count) ||
            !source.unattributed ||
            !Object.values(source.unattributed).every(ShareSoakScreen.count) ||
            !ShareSoakScreen.same(
              snapshot.timerOwners?.[when]?.unattributed,
              original.timerOwners?.afterCollection?.unattributed
            )
          )
            violations.push('priming boundary: timer attribution differs')
        }
        if (violations.length) throw new Error(violations.join('; '))
        this.priming = Object.freeze({ ...this.priming, boundaryAt: this.clock.now() })
        this.row('priming-boundary', { priming: this.priming, snapshot })
      },
      { label: 'The priming boundary' }
    )
    if (this.clock.now() > due) throw new Error('The priming deadline exceeded')
  }

  /**
   * Records the failure and taints the run.
   * @private
   * @param {Error} error what failed
   * @param {string[]} [violations] the broken checks, when the failure is a verdict
   * @returns {void}
   */
  fail(error, violations = []) {
    this.gate.taint(error)
    try {
      this.row('case-failed', {
        error: { name: error.name, message: String(error.message).slice(0, 600) },
        violations,
        counts: this.counts,
      })
    } catch {}
  }

  /**
   * Creates the fixed topic and group and the client-free baseline, inside the setup bound.
   * @returns {Promise<void>}
   * @throws {Error} the setup failure; its cleanups are registered already and run on `close`
   */
  async prepare() {
    const evidence = require('./shareSoakEvidence').ciEvidenceEnabled()
    if (evidence && this.driver instanceof ShareSoakDriver)
      require('./shareSoakEvidence').observeShareSoakDriver(this.driver, (event, fields) =>
        this.row(event, fields)
      )
    await this.stage('setup', async (scope) => {
      if (evidence && this.driver.tap)
        this.driver.tap.archive = (fact) => this.row('soak-wire', fact)
      const digest = (file) =>
        require('node:crypto').createHash('sha256').update(fs.readFileSync(file)).digest('hex')
      this.row('soak-schedule', {
        schedule: { slotMs: this.slotMs, ...this.budget },
        config: {
          compose: (process.env.COMPOSE_FILE || 'docker-compose.yml')
            .split(path.delimiter)
            .map((file) => ({ file, sha256: digest(file) })),
          source: { file: 'scripts/soak/soak.js', sha256: digest(__filename) },
          sources: [
            'scripts/soak/soak.js',
            'scripts/soak/shareProtocol.js',
            'scripts/soak/shareTimerObserver.js',
            'scripts/evidence/sharePriming.js',
            'scripts/evidence/sharePrimingLink.js',
            'scripts/evidence/shareQuietHeap.js',
            'testHelpers/shareFixtures.js',
            'testHelpers/shareLiveDiagnostics.js',
            'testHelpers/resourceProbe.js',
            'testHelpers/rawLog.js',
          ].map((file) => ({ file, sha256: digest(file) })),
          brokers: this.driver.brokers,
        },
      })
      await this.driver.prepare(this.stack, scope)
      if (this.driver.configuration) {
        if (
          this.priming &&
          ['topic', 'groupId', 'topicId'].some(
            (key) => !this.priming[key] || this.priming[key] === this.driver.configuration[key]
          )
        )
          throw new Error('Priming and scored resource identities must be disjoint')
        this.row('soak-configuration', { config: this.driver.configuration })
      }
    })
  }

  /**
   * @private
   * @returns {Soak} the landed driver, with the per-phase screen as its judge and this owner's
   *   workload, final proof and evidence
   */
  soak() {
    const mode = this.short ? 'short' : 'full'
    return new Soak({
      kafka: this.kafka,
      probe: { snapshot: () => this.snapshot() },
      phases: PROFILES[this.short ? 'share-consumer-short' : 'share-consumer'],
      context: { share: this },
      sampleMs: SHARE_SOAK.sampleMs[mode],
      warmupMs: SHARE_SOAK.warmupMs,
      settleMs: SHARE_SOAK.settleMs,
      judge: ShareSoakScreen.evaluate,
      // Archive full diagnostics, but do not keep retired owners and listener names in the heap series.
      sampleSnapshot: ({ heapUsed, shareRecords, shareRanges, shareSessions }) => ({
        heapUsed,
        shareRecords,
        shareRanges,
        shareSessions,
      }),
      record: (line) => this.recordResources(line),
      beforeTeardown: () => this.finish(),
      transition: (work, stage = 'transition') => this.stage(stage, work),
      ...this.soakOptions,
    })
  }

  /**
   * Timestamps the completed resource observation against the original monotonic run origin.
   * @returns {Promise<object>} complete share snapshot, including elapsed `heapAtMs`
   * @throws {Error} when the original resource probe fails
   */
  async snapshot() {
    const snapshot = await this.shares.snapshot()
    snapshot.heapAtMs = this.clock.now() - this.startedAt
    return snapshot
  }

  /**
   * Runs the next phase of the fixed cursor: its ten cycles, in order, with the phase's admin and
   * producer connected around them.
   * @param {number} ms the phase length the `Soak` driver passes; it must be the declared one
   * @returns {Promise<void>}
   * @throws {Error} for a wrong length, a fourth phase or a failed cycle, after the phase's
   *   clients are disconnected
   */
  async runPhase(ms) {
    const index = this.phasesDone
    if (index >= SHARE_SOAK.phases.length) throw new Error('Every share phase already ran')
    if (ms !== this.phaseMs) {
      throw new Error(`Phase length ${ms} ms is not the declared ${this.phaseMs} ms`)
    }
    try {
      await this.stage('transition', (scope) => this.driver.openPhase(scope))
    } catch (error) {
      await this.stage('transition', () => this.driver.closePhase()).catch(() => {})
      throw error
    }
    const startedAt = this.clock.now()
    this.phaseStartedAt = startedAt
    this.phaseDue = startedAt + ms
    try {
      await this.stage('profile', async (scope) => {
        for (let i = 0; i < SHARE_SOAK.cyclesPerPhase; i++) {
          await this.runCycle(index * SHARE_SOAK.cyclesPerPhase + i, scope)
        }
      })
    } finally {
      const completedAt = this.clock.now()
      this.timing.phases.push({ phase: index, startedAt, due: this.phaseDue, completedAt })
      this.elapsedProfileMs += completedAt - startedAt
      await this.stage('transition', () => this.driver.closePhase())
    }
    this.phasesDone++
  }

  /**
   * Runs one cycle inside its slot, judges it, records it, and sleeps the rest of the slot. A cycle
   * that cannot finish inside its slot fails the scenario; the cycle count never shrinks to fit.
   * @param {number} cycle zero-based cycle
   * @param {AbortSignal} [idleScope] enclosing profile scope for boundary padding
   * @returns {Promise<void>}
   * @throws {Error} the slot's expiry, or every broken check of the cycle
   */
  async runCycle(cycle, idleScope = this.scope) {
    const plan = ShareCycleOracle.plan(cycle)
    const startedAt = this.clock.now()
    const slotIndex = cycle % SHARE_SOAK.cyclesPerPhase
    const due = Math.min(
      this.phaseDue ?? Infinity,
      (this.phaseStartedAt ?? startedAt) + (slotIndex + 1) * this.slotMs,
      this.workDue
    )
    const remaining = due - startedAt
    if (remaining <= 0) throw new Error(`Cycle ${cycle} deadline exhausted`)
    try {
      // The timer contains failure after the inclusive boundary; uncapped timestamps decide acceptance.
      await this.within(
        remaining + 1,
        async (scope) => {
          const observation = await this.driver.cycle(plan, scope, { due, clock: this.clock })
          if (this.clock.now() > due)
            throw new Error(`Cycle ${cycle} completed after its absolute cutoff`)
          const violations = this.verifyCycle(plan, observation)
          if (
            observation.timing?.due !== due ||
            observation.timing?.startedAt < startedAt ||
            observation.timing?.completedAt > this.clock.now()
          )
            violations.push(`cycle ${cycle}: the proof is outside its original slot`)
          this.recordCycle(plan, observation, violations)
          this.driver.lastCycle = null
          this.driver.activeCycle = null
          if (violations.length > 0) throw new Error(violations.join('; '))
          if (this.clock.now() > due)
            throw new Error(`Cycle ${cycle} completed after its absolute cutoff`)
          this.counts.cycles += 1
          this.counts.memberAdds += 1
          this.counts.memberRemoves += 1
          if (observation.reset) this.counts.resets += 1
        },
        `Cycle ${cycle}`
      )
      // Boundary padding uses the profile's cumulative margin, outside the cycle-work timer.
      while (this.clock.now() < due) await this.pause(idleScope, due - this.clock.now())
    } catch (error) {
      if (this.driver.lastCycle) {
        const observation = this.driver.snapshotCycle?.() ?? this.driver.lastCycle
        this.recordCycle(plan, observation, [error.message])
      }
      throw error
    } finally {
      this.timing.slots.push({ cycle, startedAt, due, completedAt: this.clock.now() })
    }
  }

  /**
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {object} observation what the cycle saw
   * @returns {string[]} every check of the cycle that failed
   */
  verifyCycle(plan, observation) {
    return ShareCycleOracle.judge(plan, observation)
  }

  /**
   * Archives the cycle's complete identities and proofs, and the reset beside it, to the evidence.
   * The evidence sink owns the archive; the next cycle drops the in-memory attempt journal.
   * @private
   * @param {ReturnType<typeof ShareCycleOracle.plan>} plan the cycle
   * @param {object} observation what the cycle saw
   * @param {string[]} violations the cycle's broken checks
   * @returns {void}
   */
  recordCycle(plan, observation, violations) {
    const { members = {}, verifier = {}, produced = [], handler = [] } = observation
    const reset = observation.reset ?? null
    this.row('soak-cycle', {
      cycle: plan.cycle,
      phase: plan.phase,
      status: violations.length === 0 ? 'passed' : 'failed',
      violations,
      joined: members.aJoined === true,
      left: members.aLeft === true,
      memberAdded: members.bAdded === true,
      memberRemoved: members.bRemoved === true,
      held: observation.held ?? [],
      resetTarget: observation.resetTarget,
      snapshot: observation.snapshot,
      memberObservations: observation.memberObservations,
      resetObserved: reset !== null,
      reset,
      cleanup: {
        clientsDisconnected: observation.clientsDisconnected === true,
        verifierEmpty: verifier.active === true && verifier.delivered?.length === 0,
      },
      oracles: {
        log: { expected: produced.map(({ identity }) => identity), actual: observation.log ?? [] },
        handler: {
          expected: produced.map(({ identity }) => identity),
          actual: handler.map(({ identity }) => identity),
        },
        broker: observation.broker ?? [],
        accepted: observation.accepted ?? [],
      },
      positions: observation.positions ?? [],
      steps: observation.steps ?? [],
      timing: observation.timing,
      members,
      provenance: observation.provenance,
      verifier,
      handler,
      prefix: observation.prefix,
      acknowledgedPrefix: observation.acknowledgedPrefix,
      logs: observation.logs,
    })
    if (reset) this.row('soak-reset', { cycle: plan.cycle, ...reset })
  }

  /**
   * Records one `Soak` line as a resource row; the generic phase verdict keeps its own kind.
   * @private
   * @param {object} line a `Soak` line
   * @returns {void}
   */
  recordResources(line) {
    if (line.kind === 'baseline') this.baseline = line.snapshot
    if (line.kind === 'teardown') this.teardown = line.snapshot
    this.row('soak-resources', {
      ...line,
      kind: line.kind === 'verdict' ? 'resource-verdict' : line.kind,
    })
  }

  /**
   * The final proof, then the strict cleanup, once every phase has ended and before the teardown
   * reading: the cleanups run here, so the reading compares what a finished run leaves.
   * @returns {Promise<void>}
   * @throws {Error} a failed proof or cleanup
   */
  async finish() {
    await this.stage('cleanup', async (scope) => {
      this.final = await this.driver.final(scope, { clock: this.clock, due: this.cleanupDue })
      await this.cleanupStack()
    })
  }

  /**
   * Runs the remaining cleanups against the original shared final deadline, even on failure.
   * @returns {Promise<void>}
   */
  async cleanupStack() {
    this.cleanupDue ??= Math.min(this.clock.now() + this.budget.cleanupMs, this.workDue)
    const remaining = this.cleanupDue - this.clock.now()
    if (remaining <= 0) throw new Error('The shared cleanup deadline exhausted')
    await this.stack.run(async () => {}, { budgetMs: remaining })
    if (this.clock.now() > this.cleanupDue) throw new Error('The shared cleanup deadline exceeded')
  }

  /**
   * Charges actual monotonic time to a cumulative allocation and its original enclosing deadline.
   * @template T
   * @param {'setup'|'transition'|'profile'|'cleanup'} name Shared allocation.
   * @param {(scope: AbortSignal) => T|Promise<T>} work Lazy operation.
   * @returns {Promise<T>} Original result, only within all applicable cutoffs.
   * @throws {Error} On exhaustion, including a late timer whose abort has not fired yet.
   */
  async stage(name, work) {
    const startedAt = this.clock.now()
    const margin = name === 'profile' ? this.profileMarginMs : 0
    const allowance = this.budget[`${name}Ms`] + margin - this.spent[name]
    const previousOvershoot = this.elapsedProfileMs - this.phasesDone * this.phaseMs
    if (name === 'cleanup') this.cleanupDue ??= Math.min(startedAt + allowance, this.workDue)
    const due = Math.min(
      startedAt + allowance,
      this.workDue,
      name === 'cleanup' ? this.cleanupDue : Infinity,
      name === 'profile' ? this.phaseDue + margin - previousOvershoot : Infinity
    )
    if (startedAt >= due) throw new Error(`The shared ${name} deadline exhausted`)
    try {
      const result = await this.within(due - startedAt + 1, work, `The shared ${name}`)
      if (this.clock.now() > due) throw new Error(`The shared ${name} deadline exceeded`)
      return result
    } finally {
      const completedAt = this.clock.now()
      this.spent[name] += completedAt - startedAt
      this.timing.stages.push({ name, startedAt, due, completedAt })
    }
  }

  /**
   * Combines the cycle rows, the final proof and the resource verdict into one verdict, and
   * records the single completion row only when every check held.
   * @private
   * @param {{ ok: boolean, violations: string[] }} resources the screen's verdict
   * @returns {{ ok: boolean, violations: string[], resources: object, counts: object }}
   */
  complete(resources) {
    this.gate?.assertClean()
    if (this.driver.tap?.faults) throw new Error('The protocol observer lost source observations')
    const rows = this.evidence
      .read()
      .filter(({ event, scenario }) => event === 'soak-cycle' && scenario === this.nonce)
      .sort((a, b) => a.cycle - b.cycle)
    const violations = [...resources.violations]
    if (!(this instanceof SharePriming) && this.priming?.status !== 'passed')
      violations.push('A successful joined priming boundary is required')
    if (!Number.isFinite(this.elapsedProfileMs) || this.elapsedProfileMs < this.budget.profileMs)
      violations.push('the actual uncapped profile duration does not reach the declared schedule')
    if (this.elapsedProfileMs > this.budget.profileMs + this.profileMarginMs)
      violations.push(
        `the actual uncapped profile overshoot exceeds the ${this.profileMarginMs} ms unallocated margin`
      )
    const expected = SHARE_SOAK.cycles
    const { slots, phases, stages } = this.timing
    if (
      slots.length !== expected ||
      phases.length !== SHARE_SOAK.phases.length ||
      slots.some((slot, index) => {
        const phase = phases[Math.floor(index / SHARE_SOAK.cyclesPerPhase)]
        return (
          slot.cycle !== index ||
          !phase ||
          ![slot.startedAt, slot.due, slot.completedAt].every(Number.isFinite) ||
          slot.due !== phase.startedAt + ((index % SHARE_SOAK.cyclesPerPhase) + 1) * this.slotMs ||
          slot.completedAt < slot.due ||
          slot.startedAt < phase.startedAt ||
          slot.startedAt >= slot.due ||
          slot.completedAt > phase.completedAt ||
          (index > 0 && slot.startedAt < slots[index - 1].completedAt)
        )
      }) ||
      phases.some(
        (phase, index) =>
          phase.phase !== index ||
          ![phase.startedAt, phase.due, phase.completedAt].every(Number.isFinite) ||
          phase.due !== phase.startedAt + this.phaseMs ||
          phase.completedAt < phase.due ||
          (index > 0 && phase.startedAt < phases[index - 1].completedAt)
      ) ||
      phases.reduce((total, phase) => total + (phase.completedAt - phase.startedAt), 0) !==
        this.elapsedProfileMs ||
      stages.some(
        ({ startedAt, completedAt, due }) =>
          ![startedAt, completedAt, due].every(Number.isFinite) ||
          completedAt < startedAt ||
          completedAt > due
      )
    )
      violations.push(
        'the original slot, phase or stage timestamps do not prove the fixed schedule'
      )
    for (const name of ['priming', 'setup', 'transition', 'profile', 'cleanup']) {
      const spent = stages
        .filter((stage) => stage.name === name)
        .reduce((total, stage) => total + stage.completedAt - stage.startedAt, 0)
      if (spent > this.budget[`${name}Ms`] + (name === 'profile' ? this.profileMarginMs : 0))
        violations.push(`the cumulative ${name} allocation exceeded`)
    }
    if (
      rows.length !== expected ||
      rows.some((row, i) => row.cycle !== i || row.status !== 'passed')
    ) {
      violations.push(`${rows.length} passed cycle rows, ${expected} are required`)
    }
    const prefix = []
    for (const row of rows) {
      const produced = row.oracles.log.expected.map((identity) => ({
        identity,
        partition: JSON.parse(identity)[1],
      }))
      prefix.push(...produced.map(({ identity }) => identity))
      const observation = {
        produced,
        log: row.oracles.log.actual,
        logs: row.logs,
        handler: row.handler,
        accepted: row.oracles.accepted,
        broker: row.oracles.broker,
        positions: row.positions,
        timing: row.timing,
        held: row.held,
        reset: row.reset,
        verifier: row.verifier,
        provenance: row.provenance,
        memberObservations: row.memberObservations,
        prefix: row.prefix,
        acknowledgedPrefix: row.acknowledgedPrefix,
        members: {
          ...row.members,
          aJoined: row.joined,
          aLeft: row.left,
          bAdded: row.memberAdded,
          bRemoved: row.memberRemoved,
        },
        clientsDisconnected: row.cleanup.clientsDisconnected,
      }
      const slot = slots.find(({ cycle }) => cycle === row.cycle)
      if (
        !slot ||
        row.timing?.cycle !== row.cycle ||
        row.timing?.kind !== 'cycle' ||
        row.timing.startedAt < slot.startedAt ||
        row.timing.due !== slot.due ||
        row.timing.completedAt > slot.due
      )
        violations.push(`cycle ${row.cycle}: the proof is outside its original slot`)
      violations.push(...ShareCycleOracle.judge(ShareCycleOracle.plan(row.cycle), observation))
      if (JSON.stringify([...prefix].sort()) !== JSON.stringify([...(row.prefix ?? [])].sort()))
        violations.push(
          `cycle ${row.cycle}: archived cumulative prefix differs from original producer receipts`
        )
    }
    const verifiers = [...rows.map(({ verifier }) => verifier), this.final?.verifier]
    if (new Set(verifiers.map((verifier) => verifier?.memberId)).size !== expected + 1)
      violations.push('every cycle and final verifier must be a distinct original member')
    const { cycles, resets, memberAdds, memberRemoves } = this.counts
    if (
      cycles !== expected ||
      resets !== SHARE_SOAK.resets ||
      memberAdds !== expected ||
      memberRemoves !== expected
    ) {
      violations.push(
        `counted ${cycles} cycles, ${resets} resets, ${memberAdds} adds and ${memberRemoves} removes`
      )
    }
    const corpus = rows.flatMap((row) => row.oracles.log.expected)
    const physical = rows.flatMap((row) => row.oracles.log.actual)
    const handled = rows.flatMap((row) => row.oracles.handler.actual)
    if (
      new Set(corpus).size !== expected * SHARE_SOAK.recordsPerCycle ||
      physical.length !== corpus.length
    ) {
      violations.push('the acknowledged corpus is not 1800 distinct physical identities')
    }
    const { final = {} } = this
    const last = ShareCycleOracle.plan(expected - 1)
    ShareCycleOracle.sequence(final, (message) => violations.push(`final: ${message}`))
    if (
      final.timing?.kind !== 'final' ||
      final.timing?.cycle !== last.cycle ||
      final.timing?.startedAt < phases.at(-1)?.completedAt ||
      final.timing?.due !== this.cleanupDue ||
      final.timing?.completedAt > this.clock.now()
    )
      violations.push('final: a distinct post-profile observation inside cleanup is required')
    ShareCycleOracle.broker(last, final.broker ?? [], (message) =>
      violations.push(`final: ${message}`)
    )
    if (final.verifier?.active !== true || final.verifier.delivered?.length !== 0) {
      violations.push('the final same-group verifier was inactive or received a record')
    }
    for (const position of final.positions ?? []) {
      ShareCycleOracle.position(last, position, (message) => violations.push(`final: ${message}`))
      if (
        position.target?.groupId !== final.verifier?.groupId ||
        position.topicId !== final.verifier?.topicId ||
        position.target?.topic !== final.verifier?.topic
      )
        violations.push('final: terminal and verifier ownership disagree')
    }
    if (final.positions?.length !== 2)
      violations.push('final: both terminal observations are required')
    ShareCycleOracle.verifier(final.verifier, SHARE_SOAK.quietMs.final, (message) =>
      violations.push(`final: ${message}`)
    )
    ShareCycleOracle.log(
      last,
      new Map(rows.at(-1)?.oracles.log.expected.map((id) => [id, {}])),
      final,
      (message) => violations.push(`final: ${message}`)
    )
    if (JSON.stringify([...(final.prefix ?? [])].sort()) !== JSON.stringify([...corpus].sort()))
      violations.push('final: the raw prefix differs from the original producer receipts')
    if (this.driver.tap?.archive) {
      const { verifyShareProtocol } = require('../evidence/sharePriming')
      const manifest = verifyShareProtocol(
        this.evidence.read().filter((r) => r.scenario === this.nonce)
      )
      violations.push(...manifest.problems)
      this.protocolSha256 = manifest.sha256
      this.row('soak-protocol-manifest', { inventory: manifest.inventory, sha256: manifest.sha256 })
    }
    const completedAt = this.driver.completedAt
    if (!Number.isFinite(completedAt)) violations.push('the final cleanup proof never completed')
    const verdict = {
      ok: violations.length === 0,
      violations,
      resources,
      counts: { ...this.counts },
    }
    if (verdict.ok) {
      this.row('case-complete', {
        priming: this.priming,
        protocolSha256: this.protocolSha256,
        status: 'passed',
        tainted: false,
        schedule: { slotMs: this.slotMs, ...this.budget },
        profileMs: this.short ? SHARE_SOAK.shortMs : SHARE_SOAK.fullMs,
        elapsedProfileMs: this.elapsedProfileMs,
        memberAdds,
        memberRemoves,
        oracles: {
          log: { expected: [...corpus].sort(), actual: [...physical].sort() },
          handler: { expected: [...corpus].sort(), actual: [...handled].sort() },
          broker: final.broker,
        },
        resources: {
          before: resourceSummary(this.baseline),
          after: resourceSummary(this.teardown),
        },
        final: this.final,
        timing: {
          ...this.timing,
          origin: 'performance-ms',
          startedAt: this.startedAt,
          workDue: this.workDue,
          observedAt: this.clock.now(),
          spent: { ...this.spent },
        },
        cleanup: {
          groupsAbsent: true,
          topicsAbsent: true,
          completedAt,
          clock: this.driver.cleanupClock,
        },
      })
    }

    return verdict
  }

  /**
   * Runs a lazy step under the scenario's own bound, so nothing starts once the scenario's time is
   * spent and an expiry taints the live run.
   * @private
   * @template T
   * @param {number} ms the step's own allowance
   * @param {(scope: AbortSignal) => Promise<T>} thunk the step; it is not called under an aborted scope
   * @param {string} label names the step in its time-out message
   * @returns {Promise<T>}
   */
  within(ms, thunk, label) {
    return bounded(this.scope, ms, thunk, { label })
  }

  /**
   * @private
   * @param {string} event the row kind
   * @param {object} [fields] the measured facts
   * @returns {void}
   */
  row(event, fields = {}) {
    this.evidence.write({
      event,
      case: SHARE_SOAK_TITLE,
      profile: 'tv2',
      scenario: this.nonce,
      stem: this.stem,
      scope: this instanceof SharePriming ? 'priming' : 'scored',
      schema: 'share-soak-scopes-v1',
      ci: {
        runId: process.env.GITHUB_RUN_ID ?? null,
        attempt: process.env.GITHUB_RUN_ATTEMPT ?? null,
        profile: process.env.KAFKAJS_SOAK_PROFILE ?? null,
      },
      selector: this.short ? 'SHARE_SOAK_E2E' : 'SHARE_SOAK_FULL',
      soakProfile: this.short ? 'share-consumer-short' : 'share-consumer',
      ...fields,
    })
  }

  /** @returns {void} Restores original owner observers once, before a passing completion. */
  restore() {
    if (this.restored) return
    this.shares.restore()
    this.restored = true
  }

  /**
   * Joins the owned cleanup and validates its original observation before global observer restore.
   * @param {Error[]} failures Receives a joined stack failure without hiding resource failures.
   * @returns {Promise<void>} Both normal and exceptional scopes use the same original sources.
   * @throws {Error} Invalid resources, lost inventory or changed attribution.
   */
  async cleanupObserved(failures) {
    try {
      await this.cleanupStack()
    } catch (error) {
      failures.push(error)
    }
    if (this.driver.baseline && this.driver.after) {
      const { after, baseline } = this.driver
      const violations = []
      ShareSoakScreen.quiet(after, baseline, 'cleanup', violations)
      for (const when of ['beforeCollection', 'afterCollection']) {
        const before = baseline.timerOwners?.[when]?.unattributed
        const observed = after?.timerOwners?.[when]?.unattributed
        if (!ShareSoakScreen.same(observed, before))
          violations.push(`cleanup: timerOwners.${when}.unattributed differs from baseline`)
      }
      if (violations.length > 0) throw new Error(violations.join('; '))
    }
  }

  /**
   * Runs remaining cleanup under the original child and cleanup-stage deadlines, then restores
   * observers. Matching the setup's enclosing bounds preserves raw cleanup populations on failure;
   * no timer is discounted and no deadline is renewed. The complete cleanup observation is checked
   * against the original baseline before observers are restored. Safe to call twice.
   * @returns {Promise<void>}
   * @throws {Error} a cleanup or restore failure, or their aggregate; every failure is archived
   */
  async close() {
    if (this.closed) return
    this.closed = true
    const failures = []
    try {
      if (!this.restored) {
        await bounded(
          NEVER,
          this.workDue - this.clock.now() + 1,
          async (scope) => {
            this.scope = scope
            await this.stage('cleanup', async () => {
              await this.cleanupObserved(failures)
            })
          },
          { label: 'The share soak cleanup' }
        )
      }
    } catch (error) {
      failures.push(error)
    }
    try {
      this.restore()
    } catch (error) {
      this.gate.taint(error)
      failures.push(error)
    }
    if (failures.length > 0) {
      for (const failure of failures)
        for (const error of failure.errors ?? [failure]) this.fail(error)
      throw failures.length === 1
        ? failures[0]
        : new AggregateError(failures, 'Share cleanup failed')
    }
  }
}

/** Internal fixed first-use runner; it never calls the scored owner's run method. */
class SharePriming extends ShareSoak {
  /**
   * @param {object} options Original driver, shared observations and separate client/stack.
   */
  constructor(options) {
    super({ ...options, short: true })
    this.stem = 'share-soak-prime'
    this.budget = Object.freeze({
      ...SHARE_SOAK_PRIMING,
      primingMs: 0,
      exitMs: 0,
      totalMs: SHARE_SOAK_PRIMING.totalMs - SHARE_SOAK_PRIMING.boundaryMs,
    })
    this.profileMarginMs = SHARE_SOAK_PRIMING.idleWakeMarginMs
  }

  /**
   * Uses the original schedule/oracles, with a structural first-use judge and separate archive.
   * @returns {Soak} Original sampler and workload primitives.
   */
  soak() {
    const soak = super.soak()
    soak.judge = ShareSoakScreen.priming
    return soak
  }

  /**
   * Archives a distinct completion type; a first-use pass is never a scored receipt.
   * @param {string} event Original event kind.
   * @param {object} [fields] Original evidence fields.
   * @returns {void}
   */
  row(event, fields = {}) {
    if (event === 'case-complete')
      this.receiptSha256 = require('../evidence/sharePrimingLink').primingReceiptHash({
        scenario: this.nonce,
        ...fields,
      })
    super.row(event === 'case-complete' ? 'priming-complete' : event, fields)
  }

  /** @returns {void} Leaves the shared global observers installed for the scored owner. */
  restore() {
    this.restored = true
  }

  /**
   * Joins exceptional cleanup inside the still-active original priming envelope. Adding another
   * envelope here would change the raw wrapper population captured by original preparation.
   * @returns {Promise<void>} Safe to call twice; global observations remain installed.
   * @throws {Error} All joined cleanup failures, archived before rethrow.
   */
  async close() {
    if (this.closed) return
    this.closed = true
    const failures = []
    try {
      await this.stage('cleanup', () => this.cleanupObserved(failures))
    } catch (error) {
      failures.push(error)
    }
    this.restore()
    if (failures.length) {
      for (const failure of failures)
        for (const error of failure.errors ?? [failure]) this.fail(error)
      throw failures.length === 1
        ? failures[0]
        : new AggregateError(failures, 'Priming cleanup failed')
    }
  }

  /**
   * Runs exactly one fixed short pass under the parent's original clock and nested cutoff.
   * Cleanup is joined on failure; primary and secondary failures remain separately archived.
   * @param {AbortSignal} scope Original priming scope.
   * @param {number} startedAt Original scored-owner entry, before first-use work.
   * @param {number} due Absolute first-use cutoff excluding the boundary reserve.
   * @returns {Promise<object>} Bounded scalar completion linkage; full evidence stays on disk.
   * @throws {Error} Any workload, source, resource, deadline or joined cleanup failure.
   */
  async execute(scope, startedAt, due) {
    this.scope = scope
    this.startedAt = startedAt
    this.workDue = due
    try {
      await this.prepare()
      const resources = await this.soak().run()
      const verdict = await this.stage('cleanup', () => this.complete(resources))
      if (!verdict.ok) throw new Error(verdict.violations.join('; '))
      this.restore()
      return Object.freeze({
        scenario: this.nonce,
        receiptSha256: this.receiptSha256,
        topic: this.driver.configuration?.topic,
        groupId: this.driver.configuration?.groupId,
        topicId: this.driver.configuration?.topicId,
        passes: 1,
        cycles: 30,
        resets: 10,
        verifiers: 31,
        completedAt: this.clock.now(),
        status: 'passed',
      })
    } catch (error) {
      this.fail(error)
      try {
        await this.close()
      } catch (cleanup) {
        error.cleanupErrors = [...(error.cleanupErrors ?? []), cleanup]
      }
      throw error
    } finally {
      if (this.driver.tap) this.driver.tap.archive = null
    }
  }
}

/**
 * Builds the owner of a share-consumer profile for a real cluster: the counted client over the
 * chaos and tap socket factories, the live driver and the cleanup stack. Rows go to
 * `.tmp/e2e/share-soak.jsonl` (`share-soak-full.jsonl` for the full profile), which is also where
 * the final summary reads each cycle's archived identities back from.
 * @param {object} options
 * @param {string[]} options.brokers seed addresses, `host:port`
 * @param {boolean} [options.short=false] the 960 s profile rather than the 1920 s one
 * @param {(row: object) => void} [options.record] a second sink beside the evidence file
 * @param {{ taint: Function, assertClean: Function }} [options.run] the live run's gate; the
 *   run of this process by default
 * @returns {ShareSoak}
 * @throws {Error} when the share resource sources are missing
 */
const buildShareSoak = ({ brokers, short = false, record, run }) => {
  const ports = loadSharePorts()
  const stem = short ? 'share-soak' : 'share-soak-full'
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  const tap = new ShareFetchTap()
  const { kafka, createKafka, shareResources, socketFactory } = instrumentShare({
    clientId: 'soak-share',
    brokers,
    logLevel: logLevel.NOTHING,
    socketFactory: tap.factory(forgettingDestroyed(chaos, { releaseOnClose: true })),
  })
  const observedKafka = (client) =>
    require('./shareSoakEvidence').ciEvidenceEnabled()
      ? {
          admin: (...args) => tap.client('admin', client.admin(...args)),
          producer: (...args) => tap.client('producer', client.producer(...args)),
          shareConsumer: (...args) => tap.client('share', client.shareConsumer(...args)),
        }
      : client
  const scoredKafka = observedKafka(kafka)
  return new ShareSoak({
    driver: new ShareSoakDriver({
      kafka: scoredKafka,
      brokers,
      shares: shareResources,
      tap,
      ports,
      socketFactory,
    }),
    createPriming: () => {
      const primeKafka = observedKafka(createKafka({ clientId: 'soak-share-prime' }))
      return new SharePriming({
        driver: new ShareSoakDriver({
          kafka: primeKafka,
          brokers,
          shares: shareResources,
          tap,
          ports,
          socketFactory,
          prefix: 'soak-share-prime',
        }),
        kafka: primeKafka,
        shares: shareResources,
        stack: new ports.probe.CleanupStack(),
        short: true,
        gate: run ?? ports.bounded.liveRun,
        evidence: {
          write: (row) => {
            writeEvidence(stem, row)
            record?.(row)
          },
          read: () => evidenceLines(stem),
        },
      })
    },
    kafka,
    shares: shareResources,
    stack: new ports.probe.CleanupStack(),
    short,
    evidence: {
      write: (row) => {
        writeEvidence(stem, row)
        record?.(row)
      },
      read: () => evidenceLines(stem),
    },
    gate: run ?? ports.bounded.liveRun,
  })
}

/**
 * Builds the owner of the telemetry profile. In the parent it holds the broker resource, the four
 * supervised workers and the independent oracle; in a worker process (named by the development-only
 * worker environment, with the parent's IPC channel) it is the one-client worker. Either way it
 * reads no private member of the client and never starts a cluster.
 * @param {object} options
 * @param {string[]} options.brokers seed addresses
 * @param {Array<{ name: string, workload: string, ms: number }>} options.phases exactly the fixed phase
 * @param {(row: object) => void} [options.record] where the parent's rows go; `.tmp/e2e/telemetry-churn.jsonl`
 *   by default, a live spec supplies its own sink
 * @returns {{ run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }}
 * @throws {Error} without `--expose-gc`, for another phase list, an unconfigured broker line or
 *   evidence directory, for a worker without its kind or IPC channel, and for a seeded or long-window
 *   run (the `TELEMETRY_SEED` control or the `TELEMETRY_LONG_WINDOW` candidate) given no sink of its
 *   own, so it never writes scenario evidence
 */
const buildTelemetryChurn = ({ brokers: requested, phases, record }) => {
  if (typeof globalThis.gc !== 'function')
    throw new Error(
      'The telemetry profile reads memory after a collection: run node with --expose-gc'
    )
  if (JSON.stringify(phases) !== JSON.stringify(PROFILES.telemetry))
    throw new Error('The telemetry profile runs its one fixed phase')
  const churn = require('./telemetryChurn')
  const { LIVE_SEED } = require('./liveSeed')
  const kind = process.env[churn.WORKER_ENV]
  if (kind !== undefined) {
    if (!churn.KINDS.includes(kind) || typeof process.send !== 'function')
      throw new Error('A telemetry worker needs its kind and its parent channel')
    const worker = new churn.TelemetryWorkerProcess({
      kind,
      soak: {
        Soak,
        WORKLOADS,
        PROFILES: ACTIVE_PROFILES,
        instrument,
        TELEMETRY: ACTIVE_TELEMETRY,
        TelemetryResourceVerdict,
        logLevel,
        Kafka,
        startup: STARTUP,
        socketFactory: defaultSocketFactory(),
      },
    })
    return { run: () => worker.run(), close: async () => {} }
  }
  if (process.env[LIVE_SEED.env] && !record)
    throw new Error('A seeded telemetry run records only to a control sink')
  if (LongWindow.selected() && !record)
    throw new Error('A long-window telemetry run records only to a diagnostic sink')
  const line = process.env.KAFKA_VERSION
  const directory = process.env.TELEMETRY_EVIDENCE_DIR ?? ''
  if (
    !['4.3.1', '4.2.2'].includes(line) ||
    process.env.COMPOSE_FILE !== 'docker-compose.telemetry.yml' ||
    !process.env.COMPOSE_PROJECT_NAME ||
    !path.isAbsolute(directory) ||
    !process.env.TELEMETRY_JAVA_CACHE
  )
    throw new Error(
      'The telemetry profile needs the configured broker line, the telemetry compose file, its project, an absolute evidence directory and the independent decoder cache'
    )
  const brokers =
    JSON.stringify(requested) === JSON.stringify(DEFAULT_BROKERS) ? TELEMETRY_BROKERS : requested
  const { withTelemetryResource } = require('../../testHelpers/telemetryFixtures')
  const stem = `telemetry-churn-${Date.now().toString(36)}`
  const kafka = new Kafka({ clientId: `parent-${stem}`, brokers, logLevel: logLevel.NOTHING })
  const clock = { now: () => performance.now() }
  return new churn.TelemetryChurn({
    fixture: new churn.ParentFixture({
      brokers,
      stem,
      kafka,
      readLog,
      withResource: withTelemetryResource,
      clock,
    }),
    processes: new churn.TelemetryProcesses({
      script: __filename,
      messageBytes: ACTIVE_TELEMETRY.messageBytes,
      totalBytes: ACTIVE_TELEMETRY.totalBytes,
    }),
    decoder: new churn.DecoderCoordinator({
      directory,
      line,
      clock,
      sinkBytes: ACTIVE_TELEMETRY.sinkBytes,
    }),
    record: record ?? ((row) => writeEvidence('telemetry-churn', row)),
    clock,
    limits: ACTIVE_TELEMETRY,
    verdict: (input) => TelemetryResourceVerdict.evaluate(input),
    brokers,
    stem,
    line,
    directory,
    source: churn.sourceStamp(__filename),
  })
}

/**
 * Runs a profile against the cluster `brokers` names.
 * @param {string} profile a key of `PROFILES`
 * @param {object} [options]
 * @param {string[]} [options.brokers=['localhost:9092', 'localhost:9095', 'localhost:9098']]
 * @param {number|null} [options.phaseSeconds=null] the length of every phase, for a smoke run:
 *   samples are then taken six to a phase and the whole series counts, so the verdict is of the
 *   driver and not of memory
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.churn=buildProducerChurn]
 *   builds the owner of the `producer-churn` profile; a spec supplies its own
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.consumerChurn=buildConsumerChurn]
 *   builds the owner of the `consumer-churn` profile
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.shareSoak=buildShareSoak]
 *   builds the owner of both share-consumer profiles
 * @param {(options: object) => { run: () => Promise<{ ok: boolean }>, close: () => Promise<void> }} [options.telemetry=buildTelemetryChurn]
 *   builds the owner of the `telemetry` profile
 * @returns {Promise<{ ok: boolean }>} the verdict: `evaluate`'s for a consumer profile, the
 *   combined verdict of `ProducerChurn` for `producer-churn`, that of `ShareSoak` for a share
 *   profile
 * @throws {Error} unless the process runs with `--expose-gc`, and for a share profile given a phase
 *   length: its phases and cycles are fixed. A share workload failure is rethrown unchanged, with
 *   any joined close failure appended to its existing `cleanupErrors`; a close-only failure is thrown
 */
const main = async (
  profile,
  {
    brokers = [...DEFAULT_BROKERS],
    phaseSeconds = null,
    churn = buildProducerChurn,
    consumerChurn = buildConsumerChurn,
    shareSoak = buildShareSoak,
    telemetry = buildTelemetryChurn,
  } = {}
) => {
  if (typeof globalThis.gc !== 'function') {
    throw new Error('The soak reads the heap after a collection: run node with --expose-gc')
  }

  if (profile.startsWith('share-consumer')) {
    if (phaseSeconds !== null) throw new Error('A share profile takes no phase length')
    const owner = shareSoak({ brokers, short: profile === 'share-consumer-short' })
    let verdict
    let primary
    try {
      verdict = await owner.run()
    } catch (error) {
      primary = error
    }
    try {
      await owner.close()
    } catch (error) {
      if (!primary) throw error
      primary.cleanupErrors = [...(primary.cleanupErrors ?? []), error]
    }
    if (primary) throw primary
    return verdict
  }

  const phases = phasesOf(profile, phaseSeconds)
  if (profile === 'telemetry') {
    const owner = telemetry({ brokers, phases })
    let outcome
    try {
      outcome = { ok: true, value: await owner.run() }
    } catch (error) {
      outcome = { ok: false, error }
    }
    try {
      await owner.close()
    } catch (error) {
      if (!outcome.ok) throw new AggregateError([outcome.error, error], 'Telemetry cleanup failed')
      throw error
    }
    if (!outcome.ok) throw outcome.error
    return outcome.value
  }

  if (profile === 'producer-churn') {
    const owner = churn({ brokers, phaseSeconds, phases })
    try {
      return await owner.run()
    } finally {
      await owner.close()
    }
  }

  if (profile === 'consumer-churn') {
    const owner = consumerChurn({ brokers, phaseSeconds, phases })
    try {
      return await owner.run()
    } finally {
      await owner.close()
    }
  }

  const { kafka, probe } = instrument({ clientId: 'soak', brokers, logLevel: logLevel.NOTHING })
  const stamp = Date.now()
  const topic = `soak-${stamp}`
  await createTopic(kafka, topic)

  const smoke = phaseSeconds !== null
  const soak = new Soak({
    kafka,
    probe,
    phases,
    context: { kafka, topic, groupId: `${topic}-group` },
    ...(smoke && { sampleMs: Math.max(1000, (phaseSeconds * 1000) / 6), warmupMs: 0 }),
  })
  if (profile !== 'idle') return soak.run()
  const counted = instrument(
    {
      clientId: 'soak-idle-producer',
      brokers,
      connectionsMaxIdle: 5000,
      logLevel: logLevel.NOTHING,
    },
    { socketIdentities: true }
  )
  const producer = counted.kafka.producer({ idempotent: true })
  const wireVersions = {}
  const off = producer.on(producer.events.REQUEST, ({ payload }) => {
    wireVersions[payload.apiName] = Math.max(
      wireVersions[payload.apiName] ?? payload.apiVersion,
      payload.apiVersion
    )
  })
  soak.context.idleSocketCheck = new IdleSocketCheck({
    producer,
    probe: counted.probe,
    topic,
    record: (row) =>
      writeEvidence('soak', { ...row, wireVersions, config: { connectionsMaxIdle: 5000 } }),
  })
  let verdict
  let closed
  try {
    await producer.connect()
    verdict = await soak.run()
  } finally {
    off()
    await producer.disconnect()
    closed = await counted.probe.snapshot()
  }
  if (closed.sockets.open !== 0 || closed.queueDepth !== 0)
    throw new Error('Idle producer shutdown retained resources')
  return verdict
}

module.exports = {
  CHURN,
  TELEMETRY,
  TELEMETRY_LONG,
  TelemetryResourceVerdict,
  ConsumerChurnOracle,
  buildConsumerChurn,
  buildProducerChurn,
  buildTelemetryChurn,
  forgettingDestroyed,
  PROFILES,
  SHARE_SOAK,
  SHARE_SOAK_PRIMING,
  SharePriming,
  SHARE_SOAK_TITLE,
  WORKLOADS,
  IdleSocketCheck,
  ProducerChurn,
  RawProducerReader,
  shareSoakNaturalExit,
  ShareCycleOracle,
  ShareCycleJournal,
  ShareFetchTap,
  ShareMember,
  ShareSoak,
  ShareSoakDriver,
  ShareSoakScreen,
  ShareStateObserver,
  Soak,
  WriteDelay,
  buildShareSoak,
  createTopic,
  parseArgs,
  privateOf,
  instrument,
  instrumentShare,
  main,
}

if (require.main === module) {
  const { profile, phaseSeconds } = parseArgs(process.argv.slice(2))
  // The parent validates diagnostic workers; their resource verdicts are intentionally ineligible.
  const diagnosticWorker =
    profile === 'telemetry' &&
    LongWindow.selected(process.env) &&
    require('./telemetryChurn').KINDS.includes(process.env.TELEMETRY_WORKER_KIND)
  main(profile, { phaseSeconds }).then(
    (verdict) => {
      console.log(JSON.stringify(verdict))
      process.exitCode = diagnosticWorker || verdict.ok ? 0 : 1
    },
    (error) => {
      console.error(error)
      process.exitCode = 1
    }
  )
}
