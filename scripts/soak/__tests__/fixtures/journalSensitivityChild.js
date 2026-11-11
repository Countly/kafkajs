'use strict'

const { performance } = require('node:perf_hooks')
const churn = require('../../telemetryChurn')
const { ReferenceWork, ReferenceShareLedger } = require('./journalReference')
const { RetainedCalibration } = require('./journalCalibration')
const { journalState } = require('../../../../testHelpers/fixedJournal')
const { MEMORY_FLAGS, SETTLE, ProcessMemory } = require('../../../../testHelpers/processMemory')
const TelemetryMetrics = require('../../../../src/telemetry/metrics')
const { TELEMETRY } = require('../../soak')

/** The live worker's settled reading: collection count, their spacing and the event-loop turns. */
const LIVE_SETTLEMENT = Object.freeze({ collections: 2, pauseMs: 100, turns: 2 })

/** @param {number} at monotonic deadline @returns {Promise<void>} no earlier than the deadline */
const until = async (at) => {
  while (performance.now() < at)
    await new Promise((resolve) => setTimeout(resolve, Math.max(1, at - performance.now())))
}

/**
 * Scalar companion to the ownership replay. Only one positive-root WeakRef is allocated, before M;
 * per-record weak observation belongs in the separate ownership child, never in measured memory.
 * The `resource` protocol is the resource workers' (`MEMORY_FLAGS`, `ProcessMemory.settled()`); the
 * `live` protocol is the telemetry worker's: its exact `WORKER_FLAGS` and its settled reading of two
 * collections 100 ms apart and two event-loop turns, with the same warm-up, window, grid and evaluator.
 * @param {string} mode legacy or fixed journal implementation
 * @param {string} kind producer, consumer, admin or shareConsumer; the admin offers the same
 *   product allocation per corpus record while its paced describes do nothing else
 * @param {boolean} seeded retains an actual product snapshot and 4096 bytes for every record
 * @param {'resource'|'live'} [protocol] executable flags and settled reading of the arm
 * @returns {Promise<object>} all readings, real elapsed times, engine facts and component verdict;
 *   a live arm adds the shadow raw, code and data series of its samples
 * @throws {Error} on invalid flags, overflow, timing failure or lost positive root
 */
const run = async (mode, kind, seeded, protocol = 'resource') => {
  if (
    !['legacy', 'fixed'].includes(mode) ||
    !['producer', 'consumer', 'admin', 'shareConsumer'].includes(kind) ||
    !['resource', 'live'].includes(protocol)
  )
    throw new Error('Unknown sensitivity arm')
  const flags = protocol === 'live' ? churn.WORKER_FLAGS : MEMORY_FLAGS
  if (JSON.stringify(process.execArgv) !== JSON.stringify(flags))
    throw new Error(`Sensitivity arm requires the ${protocol} workers executable flags`)
  const Work = mode === 'fixed' ? churn.OrdinaryWork : ReferenceWork
  const Ledger = mode === 'fixed' ? churn.ShareLedger : ReferenceShareLedger
  const clock = { now: () => performance.now() }
  const slots = new churn.TelemetrySchedule(TELEMETRY).corpusSlots()
  const ledger = kind === 'shareConsumer' ? new Ledger({ group: 'group', clock }) : null
  const metrics = new TelemetryMetrics({
    kind,
    clock: { wallMs: () => Date.now(), monotonicNs: () => process.hrtime.bigint() },
  })
  metrics.register({ name: 'com.example.journal.diagnostic', type: 'gauge', value: 1 })
  const subscription = { revision: 1n, requestedMetrics: ['com.example.'], deltaTemporality: false }
  metrics.select(subscription)
  const positive = metrics.snapshot(subscription)
  metrics.abandon(positive)
  const weakRefs = Object.freeze([new WeakRef(positive)])
  const retained = Array(churn.CORPUS.records).fill(null)
  let records = 0
  let batches = 0
  let handler

  /** @returns {void} exercises the same product allocation and release in every arm */
  const product = () => {
    const snapshot = metrics.snapshot(subscription)
    metrics.abandon(snapshot)
    if (seeded) retained[records] = { snapshot, bytes: Buffer.alloc(4096, 0x6d) }
    records++
  }

  /** @returns {void} records identical scalar share facts, with actual event timestamps */
  const batchDone = () => {
    batches++
    if (!ledger) return
    const at = clock.now()
    ledger.exchanges.push({
      n: batches,
      at,
      responseAt: at,
      request: { groupId: 'group', topics: [] },
      response: null,
    })
    ledger.record({
      payload: {
        topic: 'topic',
        partition: 0,
        firstOffset: String((batches - 1) * churn.CORPUS.batch),
        lastOffset: String(batches * churn.CORPUS.batch - 1),
        type: 'accept',
        error: null,
      },
    })
  }
  const client = {
    events: { ACKNOWLEDGE: 'ack', FETCH: 'fetch' },
    on: () => {},
    subscribe: async () => {},
    commitOffsets: async () => {},
    run: async ({ eachMessage }) => (handler = eachMessage),
    send: async ({ messages }) => {
      for (let n = 0; n < messages.length; n++) product()
      batchDone()
    },
    describeCluster: async () => {},
  }
  const work = new Work({
    kind,
    client,
    stem: 'journal',
    topic: 'topic',
    ctl: 'ctl',
    clock,
    ledger,
    slots,
    sleep: (ms) => until(clock.now() + ms),
  })
  const memory = new ProcessMemory()
  const engine = protocol === 'live' ? new churn.EngineCodeJournal() : null
  const reader =
    protocol === 'live'
      ? {
          settled: () =>
            churn.settledReading('settled', {
              gc: globalThis.gc,
              pause: churn.wait,
              now: clock.now,
              collections: () => memory.collections,
              engine,
            }),
        }
      : memory
  const storage = () =>
    mode === 'fixed' ? journalState([work, ...(ledger ? [ledger] : [])]) : null
  const calibration = new RetainedCalibration({
    memory: reader,
    now: clock.now,
    origin: 0,
    inventory: () => ({
      weakRefs: weakRefs.length,
      weakMaps: 0,
      maps: 0,
      buffers: buffers.length,
      byteLength: buffers.reduce((sum, buffer) => sum + buffer.byteLength, 0),
      recordWeakRefs: 0,
      retainedSlots: retained.length,
    }),
    journals: () => ({
      workload: storage(),
      observer: calibration.rows.journalState(),
      positiveAlive: weakRefs[0].deref() !== undefined,
    }),
  })
  const buffers = Object.freeze([calibration.rows.storage])
  try {
    await work.start()
    const baseline = await calibration.take('M')
    const origin = clock.now()
    calibration.origin = origin
    const skipped = []
    const sampling = (async () => {
      for (const [index, at] of TELEMETRY.sampleAt.entries()) {
        if (at < clock.now() - origin) {
          skipped.push(at)
          continue
        }
        await until(origin + at)
        await calibration.take(
          index === 0
            ? 'first'
            : index === TELEMETRY.sampleAt.length - 1
              ? 'last'
              : `sample-${index}`,
          at
        )
      }
    })()
    const corpus = (async () => {
      await until(origin + slots[0])
      if (kind === 'producer') return work.pace()
      for (const [batch, at] of slots.entries()) {
        await until(origin + at)
        for (let record = 0; record < churn.CORPUS.batch; record++) {
          const index = batch * churn.CORPUS.batch + record
          product()
          if (kind !== 'admin')
            await handler({
              partition: index % 3,
              message: { offset: String(index), key: churn.recordId('journal', kind, index) },
            })
        }
        batchDone()
      }
    })()
    await Promise.all([sampling, corpus, until(origin + TELEMETRY.warmupMs + TELEMETRY.measureMs)])
    const boundaryAt = clock.now()
    await work.stop()
    metrics.resetTransient()
    const closed = await calibration.take('closed')
    if (
      records !== churn.CORPUS.records ||
      positive.points.length !== 1 ||
      (seeded && retained.some((row) => row?.snapshot.points.length !== 1))
    )
      throw new Error('Sensitivity product roots or corpus were lost')
    const rows = calibration.rows.values()
    const samples = rows.filter((row) => row.scheduledAtMs !== null)
    const measurementMs = boundaryAt - origin - TELEMETRY.warmupMs
    let verdict
    try {
      verdict = RetainedCalibration.evaluate({ samples, measurementMs })
    } catch (error) {
      verdict = {
        scope: 'slope-and-endpoint-only',
        ok: false,
        violations: ['calibration evidence'],
        error: error.message,
      }
    }
    return {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      scope: 'real-time broker-free product and journal calibration; not live acceptance',
      mode,
      kind,
      seeded,
      records,
      runtime: {
        executable: process.execPath,
        flags: process.execArgv,
        node: process.version,
        v8: process.versions.v8,
      },
      protocol: {
        name: protocol,
        settlement: protocol === 'live' ? LIVE_SETTLEMENT : SETTLE,
        sampleAt: TELEMETRY.sampleAt,
        perRecordWeakObservation: false,
      },
      sampler: { skipped },
      phaseOrigin: origin,
      boundaryAt,
      measurementMs,
      elapsedMs: clock.now() - origin,
      baseline,
      samples,
      closed,
      verdict,
      storage: { before: baseline.snapshot.journals.workload, closed: storage() },
      seededRoots: seeded ? retained.length : 0,
      ...(engine ? { shadow: engine.report(samples) } : {}),
    }
  } finally {
    memory.stop()
  }
}

if (require.main === module)
  run(process.argv[2], process.argv[3], process.argv[4] === 'true', process.argv[5])
    .then((result) => process.stdout.write(JSON.stringify(result)))
    .catch((error) => {
      process.stderr.write(error.stack)
      process.exitCode = 1
    })

module.exports = { run }
