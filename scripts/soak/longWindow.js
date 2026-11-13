'use strict'

/**
 * Frozen telemetry slope/endpoint method: 420 s warm-up, 900 s window and OLS over fifteen quiet
 * mid-cycle readings. The driver stamps adopted CI reports eligible; raw measurements stay diagnostic
 * so the five-minute resource verdict cannot accept this separate protocol.
 */
const v8 = require('node:v8')
const { FixedJournal } = require('../../testHelpers/fixedJournal')
const { slope } = require('./bounds')
const { LIVE_SEED, WARMUP_SEED } = require('./liveSeed')

const KIB = 1024
const MIB = 1024 * KIB
const MINUTE_MS = 60000

/**
 * The frozen method. `thresholds` are the S1/S2 limits and the S3 sigma count; the sizes
 * are what the longer run needs to keep the existing journals, segments and IPC bounds from overflowing.
 */
const LONG_WINDOW = Object.freeze({
  version: 2,
  v8Flags: Object.freeze(['--no-flush-bytecode']),
  env: 'TELEMETRY_LONG_WINDOW',
  arms: Object.freeze(['unseeded', 'seeded', 'warmup-seed']),
  warmupMs: 420000,
  measureMs: 900000,
  sampleMs: 30000,
  firstSampleMs: 60000,
  reopenMs: 60000,
  corpusRecords: 44000,
  nativeRows: 4096,
  engineRows: 128,
  heapSpaceRows: 128,
  heapSpaceRowBytes: 1536,
  messageBytes: 32 * MIB,
  totalBytes: 128 * MIB,
  scenario: 'telemetry-long-window',
  midCycleFraction: 0.25,
  thresholds: Object.freeze({
    seededSlopeBytesPerMinute: 96 * KIB,
    unseededSlopeBytesPerMinute: 64 * KIB,
    maskingSlopeBytesPerMinute: -32 * KIB,
    endpointBytes: MIB,
    firstUseBytes: MIB,
    sigmas: 3,
  }),
})

/**
 * A sink can receive all four clients: 1,320 periodic Pushes per client, plus the unchanged
 * 180 s setup/cleanup reserve, two edge Pushes for each of 23 lifecycles, and an extra Push for
 * each retirement (43) and subscription transition (131). These deliberately overcount overlapping
 * events. Each JSON record is at most 4,096 bytes plus its newline; reserve another 25%, then
 * round up to MiB. The queue, record size, per-lifecycle observer and packed-file caps stay fixed.
 * The long diagnostic writes worker archives directly, without the 512-file/16 MiB scenario packer.
 */
const phaseMs = LONG_WINDOW.warmupMs + LONG_WINDOW.measureMs
const sinkLifecycles = Math.ceil(phaseMs / LONG_WINDOW.reopenMs) + 1
const sinkRows =
  4 *
  (Math.ceil((phaseMs + 180000) / 1000) +
    2 * sinkLifecycles +
    Math.ceil(phaseMs / 30000) -
    1 +
    Math.ceil(phaseMs / 10000) -
    1)
const SINK_BUDGET = Object.freeze({
  lifecycles: sinkLifecycles,
  rows: sinkRows,
  requiredBytes: sinkRows * 4097,
  margin: 1.25,
})
const SINK_BYTES = Math.ceil((SINK_BUDGET.requiredBytes * SINK_BUDGET.margin) / MIB) * MIB

/**
 * @returns {ReadonlyArray<number>} the phase-anchored reading times of the candidate: every 30 s from
 *   60 s up to one interval before the end, then one 1 s before the end, as the existing lane does
 */
const gridOf = () => {
  const { warmupMs, measureMs, sampleMs, firstSampleMs } = LONG_WINDOW
  const end = warmupMs + measureMs
  const regular = Array.from(
    { length: (end - sampleMs - firstSampleMs) / sampleMs + 1 },
    (_, i) => firstSampleMs + i * sampleMs
  )
  return Object.freeze([...regular, end - 1000])
}

const SAMPLE_AT = gridOf()

/**
 * @param {object} [env] the environment to read, `process.env` by default
 * @returns {boolean} whether the candidate is selected; both the parent and its workers read it
 */
const selected = (env = process.env) => env[LONG_WINDOW.env] === '1'

/**
 * Derives the limits of the long run from the limits of the existing lane without changing them.
 * @param {object} base the frozen `TELEMETRY` limits
 * @returns {Readonly<object>} the same limits with the candidate's window, reading grid, case budget,
 *   journal rows, IPC bounds, segment scenario and the flags that make every input diagnostic and
 *   start traffic with the warm-up
 */
const limitsOf = (base) =>
  Object.freeze({
    ...base,
    warmupMs: LONG_WINDOW.warmupMs,
    measureMs: LONG_WINDOW.measureMs,
    caseMs:
      base.caseMs + LONG_WINDOW.warmupMs + LONG_WINDOW.measureMs - base.warmupMs - base.measureMs,
    sampleAt: SAMPLE_AT,
    diagnostic: true,
    corpusStartMs: 0,
    nativeRows: LONG_WINDOW.nativeRows,
    engineRows: LONG_WINDOW.engineRows,
    heapSpaceRows: LONG_WINDOW.heapSpaceRows,
    messageBytes: LONG_WINDOW.messageBytes,
    totalBytes: LONG_WINDOW.totalBytes,
    scenario: LONG_WINDOW.scenario,
    sinkBytes: SINK_BYTES,
    sinkBudget: SINK_BUDGET,
  })

/**
 * @param {'unseeded'|'seeded'|'warmup-seed'} arm a long-window arm
 * @returns {string|undefined} the `TELEMETRY_SEED` mode that arm selects, none for the unseeded arm
 */
const seedModeOf = (arm) => ({ seeded: LIVE_SEED.mode, 'warmup-seed': WARMUP_SEED.mode })[arm]

/**
 * The facts every record of one long-window job carries: the shadow flags, the arm and replicate, the
 * commit, the run, the Node and broker, and the frozen candidate itself.
 * @param {object} options
 * @param {string} options.arm one of `LONG_WINDOW.arms`
 * @param {string|number} options.replicate the replicate number, from 1
 * @param {string} options.tree the commit the job checked out
 * @param {object} [options.env] the environment, `process.env` by default
 * @returns {object} the identity, diagnostic and acceptance-ineligible by construction
 * @throws {Error} for an unknown arm, an invalid replicate or a missing commit
 */
const identityOf = ({ arm, replicate, tree, env = process.env }) => {
  if (!LONG_WINDOW.arms.includes(arm)) throw new Error(`Unknown long-window arm ${arm}`)
  const number = Number(replicate)
  if (!Number.isInteger(number) || number < 1)
    throw new Error(`Invalid long-window replicate ${replicate}`)
  if (typeof tree !== 'string' || tree.length === 0)
    throw new Error('A long-window record needs its commit')
  const { version, v8Flags, warmupMs, measureMs, sampleMs, reopenMs, corpusRecords, thresholds } =
    LONG_WINDOW
  return {
    diagnostic: true,
    scope: 'shadow',
    acceptanceEligible: false,
    arm,
    replicate: number,
    tree,
    sha: env.GITHUB_SHA ?? null,
    runId: env.GITHUB_RUN_ID,
    attempt: env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    kafka: env.KAFKA_VERSION,
    flags: ['--expose-gc', ...v8Flags],
    candidate: {
      version,
      v8Flags,
      warmupMs,
      measureMs,
      sampleMs,
      sampleAt: SAMPLE_AT,
      reopenMs,
      corpusRecords,
      recordsPerMinute: corpusRecords / ((warmupMs + measureMs) / MINUTE_MS),
      thresholds,
      seeds: { seeded: seedModeOf('seeded'), 'warmup-seed': seedModeOf('warmup-seed') },
    },
  }
}

/**
 * The engine's heap-space sizes and the collection count beside every measured reading of one worker,
 * in fixed storage reserved before the baseline, so a down-step in the series can be attributed to a
 * space. Like the engine code journal it is shadow evidence and never enters a verdict.
 */
class HeapSpaceJournal {
  /**
   * @param {object} [options]
   * @param {number} [options.capacity] the most readings one worker takes
   * @param {number} [options.bytes] arena bytes, `capacity` rows of `heapSpaceRowBytes` by default
   * @param {function():object[]} [options.statistics] the engine's heap space statistics
   */
  constructor({
    capacity = LONG_WINDOW.heapSpaceRows,
    bytes = capacity * LONG_WINDOW.heapSpaceRowBytes,
    statistics = () => v8.getHeapSpaceStatistics(),
  } = {}) {
    this.rows = new FixedJournal({ name: 'heap spaces', capacity, bytes })
    this.statistics = statistics
  }

  /**
   * Appends the heap spaces for a reading just taken; nothing about the reading changes.
   * @param {{at:number}} reading the reading's monotonic time
   * @param {number} collections the collections the engine reported so far
   * @returns {void}
   * @throws {Error} when the journal is full
   */
  record({ at }, collections) {
    this.rows.push({
      at,
      collections,
      spaces: this.statistics().map((space) => [
        space.space_name,
        space.space_size,
        space.space_used_size,
        space.physical_space_size,
      ]),
    })
  }

  /** @returns {object} fixed storage and append cursor, never the rows */
  journalState() {
    return this.rows.journalState()
  }

  /** @returns {Array<{at:number, collections:number, spaces:Array<[string,number,number,number]>}>} the rows, after the run */
  values() {
    return this.rows.values()
  }
}

/**
 * Median of every pairwise slope: the robust estimator the candidate reports beside OLS.
 * @param {Array<[number, number]>} points `[x, y]`
 * @returns {number} the median slope; the mean of the two middle slopes for an even count
 * @throws {Error} unless two points have different x
 */
const theilSen = (points) => {
  const slopes = []
  for (let i = 0; i < points.length; i++)
    for (let j = i + 1; j < points.length; j++)
      if (points[j][0] !== points[i][0])
        slopes.push((points[j][1] - points[i][1]) / (points[j][0] - points[i][0]))
  if (slopes.length === 0) throw new Error('a slope needs at least two different x')
  slopes.sort((a, b) => a - b)
  const middle = slopes.length >> 1
  return slopes.length % 2 ? slopes[middle] : (slopes[middle - 1] + slopes[middle]) / 2
}

/** Picks the readings taken in the quiet middle of a reopen cycle from the worker's own stage times. */
class MidCycle {
  /**
   * A cycle runs from the end of one reopen to the start of the next; the last one ends with the
   * window. A reading is mid-cycle when its whole collection interval lies inside the middle half of
   * one cycle, so no reopen, close or connect overlaps it.
   * @param {Array<{admittedAt:number, completedAt:number}>} intervals each reading's collection
   *   interval on the worker clock
   * @param {Array<{stage:string, startedAt:number, doneAt:number}>} stages the worker's recorded stages
   * @param {number} endAt the worker clock time at the end of the window
   * @param {number} [fraction] the part of a cycle cut from each side
   * @returns {number[]} indexes of the mid-cycle readings, in order
   */
  static indexes(intervals, stages, endAt, fraction = LONG_WINDOW.midCycleFraction) {
    const reopens = stages
      .filter((stage) => stage.stage === 'lifecycle')
      .sort((a, b) => a.startedAt - b.startedAt)
    const cycles = reopens
      .map((reopen, i) => {
        const from = reopen.doneAt
        const to = reopens[i + 1]?.startedAt ?? endAt
        const margin = (to - from) * fraction
        return { low: from + margin, high: to - margin }
      })
      .filter((cycle) => cycle.high >= cycle.low)
    return intervals.flatMap((interval, index) =>
      cycles.some((cycle) => interval.admittedAt >= cycle.low && interval.completedAt <= cycle.high)
        ? [index]
        : []
    )
  }
}

/**
 * @param {Array<[number, number]>} points `[minutes, bytes]` @returns {object} the OLS and Theil-Sen
 *   slopes per minute and the first-to-last change of the series
 */
const seriesOf = (points) => ({
  slopeBytesPerMinute: slope(points),
  theilSenBytesPerMinute: theilSen(points),
  endpointBytes: points.at(-1)[1] - points[0][1],
})

/** The shadow report of one worker: every field the candidate asks for, none of it a verdict. */
class ShadowReport {
  /**
   * @param {object} worker what the worker archived
   * @param {string} worker.kind the client kind
   * @param {'unseeded'|'seeded'|'warmup-seed'} worker.arm the arm of the job
   * @param {object[]} worker.samples the verdict input samples: `{admittedAtMs, completedAtMs,
   *   snapshot.memory:{at, retainedBytes, gc:{admittedAt, completedAt, observed}}}`
   * @param {{readings:Array<{at:number, code:number, bytecode:number}>}} worker.engine the engine
   *   code archive of the worker, joined to a sample by the reading's clock time
   * @param {Array<{at:number, collections:number, spaces:Array<Array>}>} [worker.heapSpaces] the heap
   *   space rows of the worker, joined the same way; readings without one report `heap: null`
   * @param {Array<{stage:string, startedAt:number, doneAt:number}>} worker.stages the worker's stages
   * @param {number} worker.endAt the worker clock time at the end of the window
   * @param {object|null} [worker.seed] the worker's seed summary, passed through
   * @param {object} [params] the candidate, `LONG_WINDOW` by default
   * @returns {object} `complete: true` with the readings, the mid-cycle and boundary series, first-use
   *   growth, the masking flag and the would-be verdicts; otherwise `complete: false` with the
   *   reason. It never throws and every record is `diagnostic` with `acceptance: false`.
   */
  static of(
    { kind, arm, samples, engine, heapSpaces = [], stages, endAt, seed = null },
    params = LONG_WINDOW
  ) {
    const head = { scope: 'shadow', acceptance: false, diagnostic: true, kind, arm }
    try {
      return {
        ...head,
        complete: true,
        ...ShadowReport.measure(samples, engine, heapSpaces, stages, endAt, arm, params),
        seed,
      }
    } catch (error) {
      return { ...head, complete: false, error: error.message }
    }
  }

  /**
   * @param {object[]} samples @param {object} engine @param {object[]} heapSpaces
   * @param {object[]} stages @param {number} endAt @param {string} arm @param {object} params
   * @returns {object} the measured part of the report
   * @throws {Error} for an incomplete frozen grid, a reading without its engine row or collection
   *   interval, or a prescribed mid-cycle reading outside the quiet interval
   */
  static measure(samples, engine, heapSpaces, stages, endAt, arm, params) {
    if (!samples?.length) throw new Error('no readings')
    const codeAt = new Map((engine?.readings ?? []).map((row) => [row.at, row]))
    const heapAt = new Map((heapSpaces ?? []).map((row) => [row.at, row]))
    const readings = samples.map((sample, index) => {
      const memory = sample.snapshot?.memory
      const code = codeAt.get(memory?.at)
      if (!code) throw new Error(`no engine reading for sample ${index}`)
      const gc = memory.gc
      if (![gc?.admittedAt, gc?.completedAt].every(Number.isFinite))
        throw new Error(`no collection interval for sample ${index}`)
      const raw = memory.retainedBytes
      const codeBytes = code.code + code.bytecode
      if (![raw, codeBytes, sample.completedAtMs].every(Number.isFinite))
        throw new Error(`a non-finite reading at sample ${index}`)
      const row = heapAt.get(memory.at)
      return {
        index,
        admittedAtMs: sample.admittedAtMs,
        completedAtMs: sample.completedAtMs,
        minutes: sample.completedAtMs / MINUTE_MS,
        rawBytes: raw,
        codeBytes,
        dataBytes: raw - codeBytes,
        gc: { admittedAt: gc.admittedAt, completedAt: gc.completedAt, observed: gc.observed },
        heap: row ? ShadowReport.heapOf(row) : null,
      }
    })
    for (const [index, at] of SAMPLE_AT.entries()) {
      const reading = readings[index]
      const next = SAMPLE_AT[index + 1] ?? params.warmupMs + params.measureMs
      if (
        !reading ||
        !(reading.admittedAtMs >= at && reading.admittedAtMs < next) ||
        reading.completedAtMs < reading.admittedAtMs ||
        reading.completedAtMs > next
      )
        throw new Error(`missing or invalid frozen grid reading at ${at} ms`)
    }
    if (readings.length !== SAMPLE_AT.length) throw new Error('extra readings on the frozen grid')
    const middle = new Set(
      MidCycle.indexes(
        readings.map((r) => r.gc),
        stages,
        endAt,
        params.midCycleFraction
      )
    )
    for (const reading of readings)
      reading.class =
        reading.admittedAtMs < params.warmupMs
          ? 'warmup'
          : middle.has(reading.index)
            ? 'mid'
            : 'boundary'
    const mid = readings.filter((r) => r.class === 'mid')
    const boundary = readings.filter((r) => r.class === 'boundary')
    const prescribed = SAMPLE_AT.filter(
      (at) => at > params.warmupMs && at % params.reopenMs === params.sampleMs
    )
    for (const at of prescribed)
      if (!middle.has(SAMPLE_AT.indexOf(at)))
        throw new Error(`prescribed mid-cycle reading at ${at} ms is not quiet`)
    if (mid.length !== prescribed.length)
      throw new Error('unexpected mid-cycle readings on the frozen grid')
    const fitSeries = (rows) => ({
      raw: seriesOf(rows.map((r) => [r.minutes, r.rawBytes])),
      data: seriesOf(rows.map((r) => [r.minutes, r.dataBytes])),
    })
    const first = readings[SAMPLE_AT.indexOf(params.firstSampleMs)]
    const entry = readings[SAMPLE_AT.indexOf(params.warmupMs)]
    const growth = {
      rawBytes: entry.rawBytes - first.rawBytes,
      dataBytes: entry.dataBytes - first.dataBytes,
    }
    const { thresholds } = params
    const series = fitSeries(mid)
    const slopeBytes = series.data.slopeBytesPerMinute
    const masked = slopeBytes < thresholds.maskingSlopeBytesPerMinute
    const unseeded = {
      thresholdBytesPerMinute: thresholds.unseededSlopeBytesPerMinute,
      endpointLimitBytes: thresholds.endpointBytes,
      slopeBytesPerMinute: slopeBytes,
      endpointBytes: series.data.endpointBytes,
      slopeOk: slopeBytes <= thresholds.unseededSlopeBytesPerMinute,
      endpointOk: series.data.endpointBytes <= thresholds.endpointBytes,
      notMasked: !masked,
    }
    return {
      counts: {
        readings: readings.length,
        window: mid.length + boundary.length,
        mid: mid.length,
        boundary: boundary.length,
      },
      midCycle: { fraction: params.midCycleFraction, admittedAtMs: mid.map((r) => r.admittedAtMs) },
      mid: { readings: mid.length, ...series },
      boundary: {
        readings: boundary.length,
        ...(boundary.length >= 2 ? fitSeries(boundary) : { raw: null, data: null }),
      },
      firstUse: {
        fromAtMs: first.admittedAtMs,
        toAtMs: entry.admittedAtMs,
        ...growth,
        boundBytes: thresholds.firstUseBytes,
        tripped: growth.dataBytes > thresholds.firstUseBytes,
      },
      masked,
      wouldBe: {
        s1:
          arm === 'seeded'
            ? {
                thresholdBytesPerMinute: thresholds.seededSlopeBytesPerMinute,
                slopeBytesPerMinute: slopeBytes,
                pass: slopeBytes >= thresholds.seededSlopeBytesPerMinute,
              }
            : null,
        s2:
          arm === 'unseeded'
            ? { ...unseeded, pass: unseeded.slopeOk && unseeded.endpointOk && unseeded.notMasked }
            : null,
      },
      readings: readings.map(({ minutes, ...reading }) => reading),
    }
  }

  /**
   * @param {{collections:number, spaces:Array<[string,number,number,number]>}} row a heap space row
   * @returns {{committedBytes:number, usedBytes:number, physicalBytes:number, collections:number,
   *   spaces:Array}} the reading's heap totals beside the rows of every space
   */
  static heapOf({ collections, spaces }) {
    const sum = (column) => spaces.reduce((total, space) => total + space[column], 0)
    return {
      committedBytes: sum(1),
      usedBytes: sum(2),
      physicalBytes: sum(3),
      collections,
      spaces,
    }
  }
}

module.exports = {
  SINK_BUDGET,
  SINK_BYTES,
  LONG_WINDOW,
  HeapSpaceJournal,
  MidCycle,
  ShadowReport,
  identityOf,
  limitsOf,
  seedModeOf,
  selected,
  theilSen,
}
