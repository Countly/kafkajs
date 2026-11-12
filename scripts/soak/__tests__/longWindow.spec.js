const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { Kafka } = require('../../../index')
const { TELEMETRY, TelemetryResourceVerdict, PROFILES } = require('../soak')
const churn = require('../telemetryChurn')
const { LIVE_SEED, LiveSeed } = require('../liveSeed')
const { judgeMemoryArm, SCHEMA } = require('../../e2e/telemetry/resourceArtifact')
const { ReferenceControl, TelemetryWorkerProcess, declaredJournalBytes } = churn
const { TelemetrySegments, FILE_BOUND } = require('../../../testHelpers/telemetrySegments')
const { TelemetryExportReader, OBSERVER_LIMITS } = require('../../../testHelpers/telemetryFixtures')
const {
  LONG_WINDOW,
  HeapSpaceJournal,
  MidCycle,
  ShadowReport,
  identityOf,
  limitsOf,
  selected,
  theilSen,
} = require('../longWindow')

const KIB = 1024
const MIB = 1024 * KIB
const PHASE_AT = 5000
const LONG = limitsOf(TELEMETRY)
const GRID = LONG.sampleAt
const minutesOf = (at) => (at + 360) / 60000

/**
 * Runs `use` against fresh copies of the modules while the long-window switch is set or unset. The
 * work happens inside the isolated registry, because the verdict loads the driver lazily.
 * @param {boolean} on whether the switch is set while the modules load
 * @param {function(object):*} use receives `{churn, soak, segments}` and returns plain facts
 * @returns {*} what `use` returned
 */
const withModules = (on, use) => {
  const previous = process.env[LONG_WINDOW.env]
  if (on) process.env[LONG_WINDOW.env] = '1'
  else delete process.env[LONG_WINDOW.env]
  try {
    let result
    jest.isolateModules(() => {
      result = use({
        churn: require('../telemetryChurn'),
        soak: require('../soak'),
        segments: require('../../../testHelpers/telemetrySegments'),
      })
    })
    return result
  } finally {
    if (previous === undefined) delete process.env[LONG_WINDOW.env]
    else process.env[LONG_WINDOW.env] = previous
  }
}

/** @returns {object} the sizes the driver derives while the switch is `on` */
const sizesWith = (on) =>
  withModules(on, ({ churn: c, soak, segments }) => {
    const schedule = new c.TelemetrySchedule(soak.TELEMETRY_LONG)
    const plain = new c.TelemetrySchedule(soak.TELEMETRY)
    const plan = segments.scheduleOf(c.SCENARIO)
    return {
      records: c.CORPUS.records,
      scenario: c.SCENARIO,
      plan: {
        perKind: plan.perKind,
        cap: plan.cap,
        kinds: plan.kinds,
        name: plan.name('producer', 3),
      },
      churnPlan: segments.scheduleOf('telemetry-churn').name('producer', 3),
      declared: {
        share: c.declaredJournalBytes('shareConsumer', soak.TELEMETRY),
        shareImplicit: c.declaredJournalBytes('shareConsumer'),
        shareLong: c.declaredJournalBytes('shareConsumer', soak.TELEMETRY_LONG),
        producerLong: c.declaredJournalBytes('producer', soak.TELEMETRY_LONG),
      },
      allowance: {
        share: soak.TelemetryResourceVerdict.setupAllowance('shareConsumer'),
        producer: soak.TelemetryResourceVerdict.setupAllowance('producer'),
        setupPeak: soak.TELEMETRY.setupPeakBytes,
      },
      long: {
        warmupMs: soak.TELEMETRY_LONG.warmupMs,
        reopens: schedule.ticks.filter((tick) => tick.actions.includes('reopen')).length,
        required: schedule.required(),
        slots: schedule.corpusSlots(),
      },
      plain: { slots: plain.corpusSlots(), required: plain.required() },
    }
  })

/** @param {number} lifecycleMs how long every reopen takes @returns {object[]} 21 reopen stages and their retires */
const stagesOf = (lifecycleMs = 4000) =>
  Array.from({ length: 21 }, (_, i) => {
    const at = 60000 * (i + 1)
    return [
      {
        tick: i,
        stage: 'lifecycle',
        startedAt: PHASE_AT + at + 5,
        doneAt: PHASE_AT + at + 5 + lifecycleMs,
      },
      { tick: i, stage: 'retire', startedAt: PHASE_AT + at + 30000, doneAt: PHASE_AT + at + 31000 },
    ]
  }).flat()

/** @returns {object} one worker sample taken at `at` ms into the phase, 350 ms of collection */
const sampleOf = (at, data, code) => ({
  admittedAtMs: at,
  completedAtMs: at + 360,
  snapshot: {
    memory: {
      at: PHASE_AT + at + 350,
      retainedBytes: data + code,
      gc: { admittedAt: PHASE_AT + at + 10, completedAt: PHASE_AT + at + 350, observed: 2 },
    },
  },
})

const isBoundary = (at) => at % 60000 === 0 || at === GRID.at(-1)

/**
 * @param {object} options
 * @param {function(number):number} options.dataAt the data series (raw less engine code) by time
 * @param {function(number):number} [options.codeAt] engine code plus bytecode by time
 * @returns {object} the report of canned samples on the candidate grid
 */
const reportOf = ({
  arm = 'unseeded',
  dataAt,
  codeAt = () => 4 * MIB,
  stages = stagesOf(),
  heap = false,
  engineGap = null,
  grid = GRID,
}) => {
  const samples = grid.map((at) => sampleOf(at, dataAt(at), codeAt(at)))
  const readings = samples
    .map((sample, i) => ({
      readingIndex: i,
      phase: 'settled',
      at: sample.snapshot.memory.at,
      code: codeAt(grid[i]) - MIB,
      bytecode: MIB,
    }))
    .filter((_, i) => i !== engineGap)
  const heapSpaces = heap
    ? samples.map((sample, i) => ({
        at: sample.snapshot.memory.at,
        collections: 100 + i,
        spaces: [
          ['old_space', 8000 + i, 6000 + i, 7000],
          ['new_space', 2000, 500, 1500],
        ],
      }))
    : undefined
  return ShadowReport.of({
    kind: 'producer',
    arm,
    samples,
    engine: { readings },
    heapSpaces,
    stages,
    endAt: PHASE_AT + LONG.warmupMs + LONG.measureMs,
  })
}

/** @param {number} perMinute bytes per minute @returns {function(number):number} an exact line */
const line = (perMinute) => (at) => 20 * MIB + perMinute * minutesOf(at)

describe('long-window shadow candidate', () => {
  test('freezes the candidate at a 420 s warm-up, a 900 s window and the existing 30 s readings', () => {
    expect(Object.isFrozen(LONG_WINDOW)).toBe(true)
    expect(LONG_WINDOW).toMatchObject({
      warmupMs: 420000,
      measureMs: 900000,
      sampleMs: 30000,
      firstSampleMs: 60000,
      reopenMs: 60000,
      corpusRecords: 44000,
    })
    expect(LONG_WINDOW.thresholds).toEqual({
      seededSlopeBytesPerMinute: 96 * KIB,
      unseededSlopeBytesPerMinute: 64 * KIB,
      maskingSlopeBytesPerMinute: -32 * KIB,
      endpointBytes: MIB,
      firstUseBytes: MIB,
      sigmas: 3,
    })
    expect(LONG_WINDOW.arms).toEqual(['unseeded', 'seeded', 'warmup-seed'])
  })

  test('derives limits whose grid holds 43 readings every 30 s and one 1 s before the end', () => {
    expect(LONG.warmupMs + LONG.measureMs).toBe(1320000)
    expect(GRID).toHaveLength(43)
    expect(GRID[0]).toBe(60000)
    expect(GRID[41]).toBe(1290000)
    expect(GRID.at(-1)).toBe(1319000)
    for (let i = 1; i < 42; i++) expect(GRID[i] - GRID[i - 1]).toBe(30000)
    expect(LONG.caseMs).toBe(TELEMETRY.caseMs + 1320000 - 360000)
    expect(LONG).toMatchObject({
      diagnostic: true,
      corpusStartMs: 0,
      nativeRows: 4096,
      engineRows: 128,
      heapSpaceRows: 128,
      messageBytes: 32 * MIB,
      totalBytes: 128 * MIB,
      scenario: 'telemetry-long-window',
    })
    expect(Object.isFrozen(LONG)).toBe(true)
  })

  test('leaves the limits of the existing lane untouched', () => {
    expect(TELEMETRY.warmupMs).toBe(60000)
    expect(TELEMETRY.measureMs).toBe(300000)
    expect(TELEMETRY.sampleAt).toEqual([
      60000, 90000, 120000, 150000, 180000, 210000, 240000, 270000, 300000, 330000, 359000,
    ])
    for (const key of ['diagnostic', 'corpusStartMs', 'nativeRows', 'engineRows', 'heapSpaceRows'])
      expect(TELEMETRY).not.toHaveProperty(key)
    expect(PROFILES.telemetry).toEqual([{ name: 'telemetry', workload: 'telemetry', ms: 360000 }])
  })

  test('keeps the traffic rate of the existing lane through warm-up and window', () => {
    const base = TELEMETRY.measureMs / 60000
    const rate = churn.CORPUS.records / base
    expect(rate).toBe(2000)
    expect(LONG_WINDOW.corpusRecords / ((LONG.warmupMs + LONG.measureMs) / 60000)).toBe(rate)
  })

  test('selects the candidate only by its switch', () => {
    expect(selected({})).toBe(false)
    expect(selected({ [LONG_WINDOW.env]: '0' })).toBe(false)
    expect(selected({ [LONG_WINDOW.env]: '1' })).toBe(true)
  })
})

describe('long-window sizing', () => {
  test('keeps the default corpus, segment and journal sizes byte-identical when the switch is unset', () => {
    const sizes = sizesWith(false)
    expect(sizes.records).toBe(10000)
    expect(sizes.scenario).toBe('telemetry-churn')
    expect(sizes.plan).toMatchObject({ perKind: 7, cap: 16 })
    expect(sizes.declared.share).toBe(23192560)
    expect(sizes.declared.shareImplicit).toBe(23192560)
    expect(sizes.allowance.share).toBe(sizes.allowance.setupPeak + 23192560)
    expect(sizes.allowance.producer).toBe(sizes.allowance.setupPeak)
    expect(sizes.plain.slots).toHaveLength(100)
    expect(sizes.plain.slots[0]).toBe(60000)
    expect(sizes.plain.required).toEqual({ subscription: 30, retire: 10, reopen: 5 })
  })

  test('sizes the long run at 44,000 records and one segment per reopen plus two', () => {
    const sizes = sizesWith(true)
    expect(sizes.records).toBe(44000)
    expect(sizes.scenario).toBe('telemetry-long-window')
    expect(sizes.long.warmupMs).toBe(420000)
    expect(sizes.long.reopens).toBe(21)
    expect(sizes.long.required.reopen).toBe(15)
    expect(sizes.long.slots).toHaveLength(440)
    expect(sizes.long.slots[0]).toBe(0)
    expect(sizes.long.slots.at(-1)).toBe(439 * 3000)
    expect(sizes.plan.perKind).toBe(sizes.long.reopens + 2)
    expect(sizes.plan.cap).toBeGreaterThanOrEqual(sizes.plan.perKind)
    expect(sizes.plan.kinds).toEqual(['producer', 'consumer', 'admin', 'shareConsumer'])
    expect(sizes.plan.name).not.toBe(sizes.churnPlan)
  })

  test('reserves the longer share journal inside the setup allowance and the job', () => {
    const { declared, allowance } = sizesWith(true)
    expect(declared.shareLong).toBeGreaterThanOrEqual(55 * MIB)
    expect(declared.shareLong).toBeLessThanOrEqual(100 * MIB)
    expect(declared.producerLong).toBeLessThan(declared.shareLong)
    expect(allowance.share).toBe(allowance.setupPeak + declared.shareLong)
    expect(allowance.producer).toBe(allowance.setupPeak)
    expect(declared.shareLong + 4 * 20 * MIB).toBeLessThan(1024 * MIB)
  })

  test('journals every native job and reading of the longer run with room', () => {
    expect(LONG.nativeRows).toBeGreaterThanOrEqual((2 * (LONG.warmupMs + LONG.measureMs)) / 1000)
    expect(LONG.engineRows).toBeGreaterThanOrEqual(GRID.length + 4 + 20)
    expect(LONG.heapSpaceRows).toBeGreaterThanOrEqual(GRID.length + 2)
  })

  test('takes journal rows from the limits and leaves the default rows unchanged', () => {
    const base = declaredJournalBytes('producer', TELEMETRY)
    expect(
      declaredJournalBytes('producer', {
        ...TELEMETRY,
        nativeRows: 1024,
        engineRows: 64,
        heapSpaceRows: 0,
      })
    ).toBe(base)
    expect(declaredJournalBytes('producer', { ...TELEMETRY, nativeRows: 1025 }) - base).toBe(
      2048 + 256
    )
    expect(declaredJournalBytes('producer', { ...TELEMETRY, engineRows: 65 }) - base).toBe(64)
    expect(declaredJournalBytes('producer', { ...TELEMETRY, heapSpaceRows: 2 }) - base).toBe(
      2 * LONG_WINDOW.heapSpaceRowBytes
    )
  })
})

describe('long-window fixture capacity', () => {
  let directory
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'long-sink-'))
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  test('budgets all four clients on one sink for the frozen schedule with 25 percent margin', () => {
    const schedule = new churn.TelemetrySchedule(LONG)
    const count = (action) => schedule.ticks.filter((tick) => tick.actions.includes(action)).length
    const lifecycles = count('reopen') + 2
    const rows =
      4 * (Math.ceil(LONG.caseMs / 1000) + 2 * lifecycles + count('retire') + count('subscription'))
    const requiredBytes = rows * (4096 + 1)
    const bytesWithMargin = Math.ceil(requiredBytes * 1.25)
    expect(lifecycles).toBe(23)
    expect(rows).toBe(6880)
    expect(LONG.sinkBytes).toBe(Math.ceil(bytesWithMargin / MIB) * MIB)
    expect(LONG.sinkBytes).toBeGreaterThanOrEqual(bytesWithMargin)
    expect(LONG.sinkBudget).toEqual({ lifecycles, rows, requiredBytes, margin: 1.25 })
    expect(TELEMETRY).not.toHaveProperty('sinkBytes')
    expect(TELEMETRY).not.toHaveProperty('sinkBudget')
    expect(OBSERVER_LIMITS).toEqual({
      sockets: 64,
      pending: 128,
      records: 4096,
      retained: 2 * MIB,
      frame: MIB,
      prefix: 8192,
    })
    expect(FILE_BOUND).toBe(2 * MIB)
  })

  test('retains every row beyond the default sink cap and keeps overflow fatal on the long cap', () => {
    const rows = Array.from({ length: 8000 }, (_, i) => ({
      v: 1,
      node: '1',
      seq: i + 1,
      clientInstanceId: '00112233-4455-6677-8899-aabbccddeeff',
      parse: 'ok',
      dataBytes: 22,
      dataSha256: 'a'.repeat(64),
      terminating: false,
      pushIntervalMs: 1000,
      detail: 'x'.repeat(100),
    }))
    const bytes = rows.map((row) => JSON.stringify(row) + '\n').join('')
    expect(Buffer.byteLength(bytes)).toBeGreaterThan(2 * MIB)
    const file = path.join(directory, 'exports-1.jsonl')
    fs.writeFileSync(file, bytes)
    expect(() => new TelemetryExportReader(directory).readExports()).toThrow('file cap')
    const reader = new TelemetryExportReader(directory, { fileBytes: LONG.sinkBytes })
    expect(reader.readExports()).toEqual(rows)
    expect(reader.checkpoint()).toEqual({ 1: rows.length, 2: 0, 3: 0 })
    fs.writeFileSync(path.join(directory, 'OVERFLOW-3.json'), '{"reason":"file-cap"}')
    expect(() => reader.readExports({ after: { 1: rows.length } })).toThrow(
      'Telemetry fixture overflow'
    )
    fs.unlinkSync(path.join(directory, 'OVERFLOW-3.json'))
    fs.truncateSync(file, LONG.sinkBytes + 1)
    expect(() => reader.readExports()).toThrow('file cap')
  })

  test('passes the long sink cap to both worker observers and the parent decoder', () => {
    const make = jest.fn(() => ({}))
    const decoder = new churn.DecoderCoordinator({
      directory,
      line: '4.3.1',
      clock: { now: () => 0 },
      make,
      sinkBytes: LONG.sinkBytes,
    })
    expect(decoder.observer).toEqual({})
    expect(make).toHaveBeenCalledWith(expect.objectContaining({ sinkBytes: LONG.sinkBytes }))
    const source = fs.readFileSync(path.join(__dirname, '../telemetryChurn.js'), 'utf8')
    expect(source).toMatch(/sinkBytes: soak\.TELEMETRY\.sinkBytes/)
    const soak = fs.readFileSync(path.join(__dirname, '../soak.js'), 'utf8')
    expect(soak).toMatch(/sinkBytes: ACTIVE_TELEMETRY\.sinkBytes/)
  })

  test('changes only the reporter file cap and preserves its fatal overflow paths', () => {
    const { LongWindowReporter } = jest.requireActual('../longWindowReporter')
    const source = fs.readFileSync(
      path.join(__dirname, '../../e2e/telemetry/CountingTelemetryReporter.java'),
      'utf8'
    )
    const prepared = LongWindowReporter.source(source)
    expect(prepared).toBe(
      source.replace('MAX_FILE_BYTES = 2L * 1024 * 1024;', `MAX_FILE_BYTES = ${LONG.sinkBytes}L;`)
    )
    expect(prepared).not.toBe(source)
    expect(prepared).toContain('overflow("file-cap")')
    expect(prepared).toContain('overflow("queue-full")')
    expect(() =>
      LongWindowReporter.source(source.replace('MAX_FILE_BYTES = 2L', 'MAX_FILE_BYTES = 3L'))
    ).toThrow('reporter file cap')
  })

  test('uses the derived reporter only in the long job and verifies the mounted jar identity', () => {
    const yaml = require('js-yaml')
    const jobs = yaml.load(
      fs.readFileSync(path.join(__dirname, '../../../.github/workflows/memory-gates.yml'), 'utf8')
    ).jobs
    const long = jobs['telemetry-long-window']
    expect(long.steps.filter((step) => step.run?.includes('longWindowReporter.js'))).toHaveLength(1)
    expect(
      long.steps.findIndex((step) => step.run?.includes('longWindowReporter.js'))
    ).toBeLessThan(long.steps.findIndex((step) => step.run?.includes('ci.js preflight')))
    const start = long.steps.find((step) => step.run?.includes('up -d --pull never'))
    expect(start.run).toContain('-f .tmp/telemetry-long-reporter/compose.json')
    for (const action of ['preflight', 'capture', 'stop']) {
      const step = long.steps.find((entry) => entry.run?.includes(`ci.js ${action}`))
      expect(step.env.TELEMETRY_LONG_WINDOW).toBe('1')
    }
    for (const [name, job] of Object.entries(jobs)) {
      if (name !== 'telemetry-long-window')
        expect(JSON.stringify(job)).not.toContain('longWindowReporter')
    }
    const { CiOwnership } = require('../../e2e/telemetry/ci')
    const docker = jest.fn(() => '')
    const owner = new CiOwnership({
      env: {
        CI: 'true',
        COMPOSE_PROJECT_NAME: 'telemetry-default',
        COMPOSE_FILE: 'docker-compose.telemetry.yml',
      },
      docker,
    })
    owner.run(['compose', '-f', owner.file, 'config'])
    expect(docker.mock.calls[0][0]).toEqual(['compose', '-f', owner.file, 'config'])
    const ci = fs.readFileSync(path.join(__dirname, '../../e2e/telemetry/ci.js'), 'utf8')
    expect(ci).toMatch(/reporterSha256, this\.longReporter\?\.sha256 \?\? manifest/)
  })

  test('offers all 440 batches before the long boundary without changing the corpus rate', () => {
    const sizes = sizesWith(true)
    expect(sizes.long.slots).toEqual(Array.from({ length: 440 }, (_, i) => i * 3000))
    expect(sizes.long.slots.at(-1) + churn.CORPUS.batchMs).toBe(LONG.warmupMs + LONG.measureMs)
    expect(sizes.records).toBe(440 * churn.CORPUS.batch)
  })
})

describe('HeapSpaceJournal', () => {
  const spaces = () => [
    { space_name: 'old_space', space_size: 8192, space_used_size: 6000, physical_space_size: 7000 },
    { space_name: 'new_space', space_size: 2048, space_used_size: 500, physical_space_size: 1500 },
  ]

  test('records each reading in fixed storage with the collection count and every space', () => {
    const journal = new HeapSpaceJournal({ capacity: 3, bytes: 3 * 1024, statistics: spaces })
    const before = journal.journalState()
    journal.record({ at: 10.5 }, 4)
    journal.record({ at: 40.25 }, 6)
    expect(journal.values()).toEqual([
      {
        at: 10.5,
        collections: 4,
        spaces: [
          ['old_space', 8192, 6000, 7000],
          ['new_space', 2048, 500, 1500],
        ],
      },
      {
        at: 40.25,
        collections: 6,
        spaces: [
          ['old_space', 8192, 6000, 7000],
          ['new_space', 2048, 500, 1500],
        ],
      },
    ])
    const after = journal.journalState()
    expect(after.byteLength).toBe(before.byteLength)
    expect(after.length).toBe(2)
    expect(after.cursor).toBeGreaterThan(before.cursor)
  })

  test('refuses a row past its capacity instead of growing', () => {
    const journal = new HeapSpaceJournal({ capacity: 1, bytes: 1024, statistics: spaces })
    journal.record({ at: 1 }, 1)
    expect(() => journal.record({ at: 2 }, 2)).toThrow('heap spaces journal overflow')
    expect(journal.journalState().overflow).toBe(true)
    expect(journal.values()).toHaveLength(1)
  })

  test('fits the real engine statistics of this Node in its reserved row', () => {
    const journal = new HeapSpaceJournal({ capacity: 2 })
    journal.record({ at: 1 }, 1)
    journal.record({ at: 2 }, 2)
    expect(journal.values()[0].spaces.length).toBeGreaterThanOrEqual(5)
    expect(journal.journalState().overflow).toBe(false)
  })
})

describe('mid-cycle selection', () => {
  const intervals = GRID.map((at) => ({
    admittedAt: PHASE_AT + at + 10,
    completedAt: PHASE_AT + at + 350,
  }))
  const endAt = PHASE_AT + 1320000

  test('selects exactly the readings at 450 s to 1,290 s from canned stage times', () => {
    const picked = MidCycle.indexes(intervals, stagesOf(), endAt).map((i) => GRID[i])
    const warm = picked.filter((at) => at < LONG.warmupMs)
    const window = picked.filter((at) => at >= LONG.warmupMs)
    expect(window).toEqual(Array.from({ length: 15 }, (_, i) => 450000 + i * 60000))
    expect(window.at(0)).toBe(450000)
    expect(window.at(-1)).toBe(1290000)
    expect(warm).toEqual(Array.from({ length: 6 }, (_, i) => 90000 + i * 60000))
  })

  test('drops a reading whose collection runs into a reopen', () => {
    const shifted = intervals.map((row, i) =>
      GRID[i] === 690000 ? { ...row, completedAt: PHASE_AT + 720005 + 100 } : row
    )
    const picked = MidCycle.indexes(shifted, stagesOf(), endAt).map((i) => GRID[i])
    expect(picked).not.toContain(690000)
    expect(picked).toContain(630000)
    expect(picked).toContain(750000)
  })

  test('follows the recorded stage times, not the reading index', () => {
    const slow = MidCycle.indexes(intervals, stagesOf(25000), endAt)
    expect(slow).toEqual([])
    const fast = MidCycle.indexes(intervals, stagesOf(1000), endAt)
    expect(fast.length).toBeGreaterThan(15)
  })

  test('closes the last cycle at the end of the window', () => {
    const picked = MidCycle.indexes(intervals, stagesOf(), endAt).map((i) => GRID[i])
    expect(picked).toContain(1290000)
    expect(picked).not.toContain(1319000)
  })
})

describe('shadow report', () => {
  test('fits OLS and Theil-Sen on the 15 mid-cycle readings and reports boundary readings apart', () => {
    const mid = line(128 * KIB)
    const boundary = line(192 * KIB)
    const report = reportOf({
      arm: 'seeded',
      dataAt: (at) => (isBoundary(at) ? boundary(at) : mid(at)),
    })
    expect(report).toMatchObject({
      scope: 'shadow',
      acceptance: false,
      diagnostic: true,
      complete: true,
      kind: 'producer',
      arm: 'seeded',
      counts: { readings: 43, window: 31, mid: 15, boundary: 16 },
    })
    expect(report.midCycle.admittedAtMs).toEqual(
      Array.from({ length: 15 }, (_, i) => 450000 + i * 60000)
    )
    expect(report.mid.data.slopeBytesPerMinute).toBeCloseTo(128 * KIB, 3)
    expect(report.mid.data.theilSenBytesPerMinute).toBeCloseTo(128 * KIB, 3)
    expect(report.mid.data.endpointBytes).toBeCloseTo(14 * 128 * KIB, 2)
    expect(report.mid.raw.slopeBytesPerMinute).toBeCloseTo(128 * KIB, 3)
    expect(report.boundary.readings).toBe(16)
    expect(report.boundary.data.slopeBytesPerMinute).toBeCloseTo(192 * KIB, 3)
    expect(report.readings).toHaveLength(43)
    expect(report.readings.filter((row) => row.class === 'mid')).toHaveLength(15)
    expect(report.readings.filter((row) => row.class === 'warmup')).toHaveLength(12)
  })

  test('subtracts engine code from the raw series for the data slope', () => {
    const report = reportOf({
      dataAt: () => 20 * MIB,
      codeAt: (at) => 4 * MIB + 100 * KIB * minutesOf(at),
    })
    expect(report.mid.raw.slopeBytesPerMinute).toBeCloseTo(100 * KIB, 3)
    expect(report.mid.data.slopeBytesPerMinute).toBeCloseTo(0, 3)
    expect(report.readings[0]).toMatchObject({
      rawBytes: expect.any(Number),
      codeBytes: expect.any(Number),
    })
    expect(report.readings[0].dataBytes).toBeCloseTo(20 * MIB, 3)
  })

  test('reports the median of pairwise slopes so one outlier cannot move Theil-Sen', () => {
    const base = line(100 * KIB)
    const report = reportOf({
      dataAt: (at) => (at === 810000 ? base(at) + 5 * MIB : base(at)),
    })
    expect(report.mid.data.theilSenBytesPerMinute).toBeCloseTo(100 * KIB, 3)
    expect(report.mid.data.slopeBytesPerMinute).not.toBeCloseTo(100 * KIB, 0)
    expect(
      theilSen([
        [0, 0],
        [1, 10],
        [2, 20],
        [3, 30],
        [4, 1000],
      ])
    ).toBe(10)
    expect(
      theilSen([
        [0, 0],
        [1, 2],
      ])
    ).toBe(2)
    expect(() =>
      theilSen([
        [1, 1],
        [1, 2],
      ])
    ).toThrow('a slope needs at least two different x')
  })

  test('measures first-use growth from the 60 s reading to the first window reading', () => {
    const step = (size) => (at) => 20 * MIB + (at >= 420000 ? size : 0)
    const tripped = reportOf({ dataAt: step(1.5 * MIB) })
    expect(tripped.firstUse).toMatchObject({
      fromAtMs: 60000,
      toAtMs: 420000,
      boundBytes: MIB,
      tripped: true,
    })
    expect(tripped.firstUse.dataBytes).toBeCloseTo(1.5 * MIB, 3)
    expect(tripped.firstUse.rawBytes).toBeCloseTo(1.5 * MIB, 3)
    const quiet = reportOf({ dataAt: step(0.5 * MIB) })
    expect(quiet.firstUse.tripped).toBe(false)
    const exact = reportOf({ dataAt: step(MIB) })
    expect(exact.firstUse.tripped).toBe(false)
  })

  test('flags masking below -32 KiB/min and fails the would-be unseeded verdict with it', () => {
    const masked = reportOf({ dataAt: line(-40 * KIB) })
    expect(masked.masked).toBe(true)
    expect(masked.wouldBe.s2).toMatchObject({ notMasked: false, pass: false })
    const clear = reportOf({ dataAt: line(-20 * KIB) })
    expect(clear.masked).toBe(false)
    expect(clear.wouldBe.s2).toMatchObject({
      slopeOk: true,
      endpointOk: true,
      notMasked: true,
      pass: true,
    })
  })

  test('gives the would-be seeded verdict from the data slope at 96 KiB/min', () => {
    const detected = reportOf({ arm: 'seeded', dataAt: line(97 * KIB) })
    expect(detected.wouldBe.s1).toMatchObject({ thresholdBytesPerMinute: 96 * KIB, pass: true })
    expect(detected.wouldBe.s2).toBeNull()
    const missed = reportOf({ arm: 'seeded', dataAt: line(95 * KIB) })
    expect(missed.wouldBe.s1.pass).toBe(false)
  })

  test('gives the would-be unseeded verdict from the 64 KiB/min slope, the 1 MiB endpoint and masking', () => {
    const quiet = reportOf({ dataAt: line(60 * KIB) })
    expect(quiet.wouldBe.s1).toBeNull()
    expect(quiet.wouldBe.s2).toMatchObject({
      slopeBytesPerMinute: expect.any(Number),
      slopeOk: true,
      endpointOk: true,
      notMasked: true,
      pass: true,
    })
    const steep = reportOf({ dataAt: line(70 * KIB) })
    expect(steep.wouldBe.s2).toMatchObject({ slopeOk: false, endpointOk: true, pass: false })
    const jump = reportOf({
      dataAt: (at) => (at === 1290000 ? 20 * MIB + 1.2 * MIB : 20 * MIB),
    })
    expect(jump.mid.data.slopeBytesPerMinute).toBeLessThan(64 * KIB)
    expect(jump.mid.data.endpointBytes).toBeGreaterThan(MIB)
    expect(jump.wouldBe.s2).toMatchObject({ slopeOk: true, endpointOk: false, pass: false })
  })

  test('attributes readings to heap spaces and collections when the journal rows exist', () => {
    const report = reportOf({ dataAt: line(0), heap: true })
    expect(report.readings[3]).toMatchObject({
      gc: {
        admittedAt: PHASE_AT + GRID[3] + 10,
        completedAt: PHASE_AT + GRID[3] + 350,
        observed: 2,
      },
      heap: {
        committedBytes: 8003 + 2000,
        usedBytes: 6003 + 500,
        physicalBytes: 8500,
        collections: 103,
        spaces: [
          ['old_space', 8003, 6003, 7000],
          ['new_space', 2000, 500, 1500],
        ],
      },
    })
    expect(reportOf({ dataAt: line(0) }).readings[3].heap).toBeNull()
  })

  test('is incomplete, never throwing, for a missing engine row, too few mid readings or no samples', () => {
    const gap = reportOf({ dataAt: line(0), engineGap: 10 })
    expect(gap).toMatchObject({ complete: false, scope: 'shadow', acceptance: false })
    expect(gap.error).toMatch(/engine/)
    const slow = reportOf({ dataAt: line(0), stages: stagesOf(25000) })
    expect(slow).toMatchObject({ complete: false })
    expect(slow.error).toMatch(/mid-cycle/)
    const empty = ShadowReport.of({
      kind: 'admin',
      arm: 'unseeded',
      samples: [],
      engine: { readings: [] },
      stages: [],
      endAt: 0,
    })
    expect(empty).toMatchObject({ complete: false, kind: 'admin', arm: 'unseeded' })
  })

  test('rejects a missing or duplicate reading anywhere on the frozen grid', () => {
    for (const missing of [60000, 420000, 450000, 480000, 1290000, 1319000]) {
      const report = reportOf({ dataAt: line(0), grid: GRID.filter((at) => at !== missing) })
      expect(report).toMatchObject({ complete: false })
      expect(report.error).toMatch(/grid|checkpoint/)
      expect(report).not.toHaveProperty('wouldBe')
    }
    const duplicate = reportOf({ dataAt: line(0), grid: [GRID[0], ...GRID] })
    expect(duplicate.complete).toBe(false)
    expect(duplicate.error).toMatch(/grid/)
    expect(duplicate).not.toHaveProperty('wouldBe')
  })

  test('rejects the full grid when one prescribed mid-cycle reading overlaps a lifecycle', () => {
    const stages = stagesOf().map((stage) =>
      stage.stage === 'lifecycle' && stage.tick === 10
        ? { ...stage, doneAt: PHASE_AT + 695000 }
        : stage
    )
    for (const arm of ['unseeded', 'seeded']) {
      const report = reportOf({ arm, dataAt: line(128 * KIB), stages })
      expect(report.complete).toBe(false)
      expect(report.error).toMatch(/mid-cycle.*690000/)
      expect(report).not.toHaveProperty('wouldBe')
    }
  })

  test('accepts admission jitter within each frozen grid slot', () => {
    const report = reportOf({ dataAt: line(0), grid: GRID.map((at) => at + 5) })
    expect(report.complete).toBe(true)
    expect(report.counts.mid).toBe(15)
    expect(report.firstUse).toMatchObject({ fromAtMs: 60005, toAtMs: 420005 })
  })

  test('passes the worker seed summary through beside the series', () => {
    const seed = { mode: 'slope2x', intervals: 43, buffers: 43, bytes: 43 * 65536 }
    const samples = GRID.map((at) => sampleOf(at, line(0)(at), 4 * MIB))
    const report = ShadowReport.of({
      kind: 'consumer',
      arm: 'seeded',
      samples,
      engine: {
        readings: samples.map((sample) => ({
          at: sample.snapshot.memory.at,
          code: 3 * MIB,
          bytecode: MIB,
        })),
      },
      stages: stagesOf(),
      endAt: PHASE_AT + 1320000,
      seed,
    })
    expect(report.seed).toEqual(seed)
  })
})

describe('long-window records', () => {
  const identity = () =>
    identityOf({
      arm: 'seeded',
      replicate: '2',
      tree: 'a'.repeat(40),
      env: {
        GITHUB_RUN_ID: '7',
        GITHUB_RUN_ATTEMPT: '1',
        GITHUB_SHA: 'b'.repeat(40),
        KAFKA_VERSION: '4.3.1',
      },
    })

  test('marks every record diagnostic and acceptance-ineligible and carries the frozen candidate with the commit', () => {
    expect(identity()).toMatchObject({
      diagnostic: true,
      scope: 'shadow',
      acceptanceEligible: false,
      arm: 'seeded',
      replicate: 2,
      tree: 'a'.repeat(40),
      sha: 'b'.repeat(40),
      runId: '7',
      attempt: '1',
      kafka: '4.3.1',
      node: process.version,
      flags: ['--expose-gc'],
    })
    expect(identity().candidate).toEqual({
      warmupMs: 420000,
      measureMs: 900000,
      sampleMs: 30000,
      sampleAt: GRID,
      reopenMs: 60000,
      corpusRecords: 44000,
      recordsPerMinute: 2000,
      thresholds: LONG_WINDOW.thresholds,
      seeds: { seeded: 'slope2x', 'warmup-seed': 'warmup1p5mib' },
    })
  })

  test('is refused by the resource verdict and the memory arm judge', () => {
    const record = identity()
    const verdict = TelemetryResourceVerdict.evaluate({
      diagnostic: record.diagnostic,
      acceptanceEligible: record.acceptanceEligible,
      clientCount: 1,
      baseline: {},
      samples: [],
      teardown: {},
      reference: null,
    })
    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain('diagnostic inputs are not acceptance-eligible')
    const arm = judgeMemoryArm({ ...record, schema: SCHEMA, arm: 'public', kind: 'producer' })
    expect(arm.verdict).toBe('fail')
    expect(arm.problems).toContain('diagnostic records are ineligible for acceptance')
  })

  test('refuses an unknown arm and a missing commit', () => {
    expect(() => identityOf({ arm: 'other', replicate: '1', tree: 'a', env: {} })).toThrow(
      'long-window arm'
    )
    expect(() => identityOf({ arm: 'unseeded', replicate: '1', tree: '', env: {} })).toThrow(
      'commit'
    )
    expect(() => identityOf({ arm: 'unseeded', replicate: 'x', tree: 'a', env: {} })).toThrow(
      'replicate'
    )
  })
})

describe('warm-up-only seed', () => {
  test('retains 1.5 MiB over the 12 warm-up samples and nothing after', () => {
    const root = {}
    const seed = LiveSeed.of('warmup1p5mib', { root, limits: TELEMETRY })
    for (let i = 0; i < GRID.length; i++) seed.retain()
    expect(root[LIVE_SEED.root]).toHaveLength(12)
    expect(root[LIVE_SEED.root].every((buffer) => buffer.length === 131072)).toBe(true)
    expect(seed.summary()).toEqual({
      mode: 'warmup1p5mib',
      rateBytesPerMinute: 0,
      bytesPerInterval: 131072,
      intervals: 43,
      buffers: 12,
      bytes: 12 * 131072,
    })
    expect(seed.summary().bytes).toBeGreaterThan(MIB)
    seed.release()
    expect(root[LIVE_SEED.root]).toBeUndefined()
  })

  test('is not retained through the product root until a sample is recorded', () => {
    const root = {}
    const seed = LiveSeed.of('warmup1p5mib', { root, limits: TELEMETRY })
    expect(root[LIVE_SEED.root]).toBeUndefined()
    expect(seed.summary()).toMatchObject({ intervals: 0, buffers: 0, bytes: 0 })
    expect(Kafka[LIVE_SEED.root]).toBeUndefined()
  })

  test('still refuses an unknown mode', () => {
    expect(() => LiveSeed.of('slope3x', { root: {}, limits: TELEMETRY })).toThrow(
      'Unknown telemetry seed mode slope3x'
    )
  })
})

describe('long-window worker', () => {
  const runWorker = async (limits) => {
    const messages = []
    const snapshot = { journals: { byteLength: 4096, cursor: 0 } }
    const run = {
      baseline: snapshot,
      samples: [{ admittedPhaseMs: 60000, phaseMs: 60300, snapshot }],
      teardown: snapshot,
    }
    const verdict = { ok: true, outcome: 'passed', violations: [] }
    jest.spyOn(ReferenceControl.prototype, 'run').mockResolvedValue({ manifest: {} })
    jest.spyOn(TelemetrySegments, 'ordinary').mockReturnValue({})
    try {
      const owner = new TelemetryWorkerProcess({
        kind: 'producer',
        soak: {
          TELEMETRY: limits,
          PROFILES,
          logLevel: { NOTHING: 0 },
          instrument: () => ({ kafka: { producer: () => ({}) } }),
          SharePeakWorker: class {
            constructor() {
              this.timing = { measurementMs: 300000, cleanupMs: 1000 }
              this.claims = { foreground: [] }
            }

            async prepare() {
              return 'uuid'
            }

            facts() {
              return {}
            }
          },
          TelemetryResourceVerdict: { evaluate: () => verdict },
          Soak: class {
            constructor({ judge }) {
              this.judge = judge
              this.skipped = []
              this.readFailures = []
            }

            async run() {
              return this.judge(run)
            }
          },
        },
        proc: {
          on() {},
          send(message, callback) {
            messages.push(JSON.parse(JSON.stringify(message)))
            callback?.()
          },
        },
        env: {},
      })
      owner.stamps = () => ({})
      owner.reference = () => ({})
      owner.inbox.push({ type: 'config', stem: 'long', topic: 'topic', group: 'group' })
      owner.inbox.push({ type: 'start' })
      await owner.run()
      return messages.find((message) => message.type === 'result')
    } finally {
      jest.restoreAllMocks()
    }
  }

  test('flags its judged input diagnostic and exports heap spaces only on the long limits', async () => {
    const long = await runWorker(LONG)
    expect(long.input).toMatchObject({ diagnostic: true, acceptanceEligible: false })
    expect(long.shadow.heapSpaces).toEqual([])
    const base = await runWorker(TELEMETRY)
    expect(base.input).not.toHaveProperty('diagnostic')
    expect(base.input).not.toHaveProperty('acceptanceEligible')
    expect(base.shadow).not.toHaveProperty('heapSpaces')
  })
})
