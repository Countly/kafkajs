const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { Kafka } = require('../../../index')
const { PROFILES, TELEMETRY, TelemetryResourceVerdict, buildTelemetryChurn } = require('../soak')
const churn = require('../telemetryChurn')
const { verifyChurn } = require('../../e2e/telemetry/evidence')
const {
  LIVE_SEED,
  LiveSeed,
  ControlRecords,
  factsOf,
  judgeSeededRun,
  requireControl,
} = require('../liveSeed')

const RATE = TELEMETRY.slopeBytesPerMinute * 2
const INTERVAL = (RATE * TELEMETRY.sampleMs) / 60000
const FLOOR = RATE * (1 - LIVE_SEED.toleranceFraction)

describe('live seeded control', () => {
  test('retains exactly twice the slope limit per sample interval through the product Kafka root', () => {
    const seed = LiveSeed.of(LIVE_SEED.mode, { root: Kafka, limits: TELEMETRY })
    try {
      expect(seed.rateBytesPerMinute).toBe(RATE)
      expect(seed.bytesPerInterval).toBe(INTERVAL)
      expect(INTERVAL).toBe(65536)
      for (let i = 0; i < TELEMETRY.sampleAt.length; i++) seed.retain()
      const held = Kafka[LIVE_SEED.root]
      expect(held).toHaveLength(TELEMETRY.sampleAt.length)
      for (const buffer of held) {
        expect(buffer).toHaveLength(INTERVAL)
        expect(buffer.equals(Buffer.alloc(INTERVAL, LIVE_SEED.fill))).toBe(true)
      }
      expect(seed.summary()).toEqual({
        mode: 'slope2x',
        rateBytesPerMinute: RATE,
        bytesPerInterval: INTERVAL,
        intervals: 11,
        buffers: 11,
        bytes: 11 * INTERVAL,
      })
    } finally {
      seed.release()
    }
    expect(Kafka[LIVE_SEED.root]).toBeUndefined()
  })

  test('derives its bytes from the limits and refuses an unknown mode', () => {
    for (const mode of [undefined, ''])
      expect(LiveSeed.of(mode, { root: {}, limits: TELEMETRY })).toBeNull()
    expect(() => LiveSeed.of('slope3x', { root: {}, limits: TELEMETRY })).toThrow(
      'Unknown telemetry seed mode slope3x'
    )
    const root = {}
    const seed = LiveSeed.of(LIVE_SEED.mode, {
      root,
      limits: { slopeBytesPerMinute: 1000, sampleMs: 15000 },
    })
    expect(seed.bytesPerInterval).toBe(500)
    seed.retain()
    expect(root[LIVE_SEED.root][0]).toHaveLength(500)
    expect(seed.summary().bytes).toBe(500)
  })

  describe('in a worker', () => {
    const worker = (env, root = {}) => {
      const proc = { send: jest.fn() }
      const runner = new churn.TelemetryWorkerProcess({
        kind: 'admin',
        soak: { Kafka: root, TELEMETRY },
        proc,
        env: { [churn.WORKER_TOKEN]: 'tok', ...env },
      })
      return { runner, proc, root }
    }
    const rows = ['baseline', 'sample', 'sample', 'sample', 'phase-end', 'teardown', 'verdict']

    test('retains one buffer for each recorded sample of a seeded worker and none for any other row', () => {
      const { runner, proc, root } = worker({ [LIVE_SEED.env]: LIVE_SEED.mode })
      const seed = runner.seeded()
      for (const kind of rows) runner.progress({ kind }, seed)
      expect(root[LIVE_SEED.root]).toHaveLength(3)
      expect(root[LIVE_SEED.root].every((buffer) => buffer.length === INTERVAL)).toBe(true)
      expect(seed.summary()).toMatchObject({ intervals: 3, bytes: 3 * INTERVAL })
      expect(proc.send).toHaveBeenCalledTimes(rows.length)
      expect(proc.send).toHaveBeenLastCalledWith({
        type: 'progress',
        row: { kind: 'verdict' },
        kind: 'admin',
        token: 'tok',
      })
    })

    test('keeps an unseeded worker untouched', () => {
      const { runner, proc, root } = worker({})
      const seed = runner.seeded()
      expect(seed).toBeNull()
      for (const kind of rows) runner.progress({ kind }, seed)
      expect(Reflect.ownKeys(root)).toEqual([])
      expect(proc.send).toHaveBeenCalledTimes(rows.length)
      expect(() => worker({ [LIVE_SEED.env]: 'other' }).runner.seeded()).toThrow(
        'Unknown telemetry seed mode other'
      )
    })
  })

  test('reports seeded growth without a five-minute slope failure and keeps the diagnostic data slope', () => {
    const seed = LiveSeed.of(LIVE_SEED.mode, { root: {}, limits: TELEMETRY })
    const base = 10000000
    const samples = TELEMETRY.sampleAt.map((at) => {
      const retainedBytes = base + seed.summary().bytes
      seed.retain()
      return {
        admittedAtMs: at,
        completedAtMs: at + 300,
        snapshot: { registryDigest: 'diagnostic', memory: { retainedBytes } },
      }
    })
    const verdict = TelemetryResourceVerdict.evaluate({
      samples,
      measurementMs: TELEMETRY.measureMs,
      clientCount: 1,
      baseline: samples[0].snapshot,
      teardown: samples.at(-1).snapshot,
      cleanupMs: 1,
    })
    expect(verdict.violations).not.toContain('retained slope')
    expect(verdict.violations).not.toContain('retained endpoint')
    const shadow = churn.EngineCodeJournal.shadow(
      samples.map((sample) => ({
        minutes: sample.completedAtMs / 60000,
        raw: sample.snapshot.memory.retainedBytes,
        code: 0,
      }))
    )
    expect(shadow.data.slopeBytesPerMinute).toBeCloseTo(verdict.measured.slopeBytesPerMinute, 3)
    expect(shadow.data.slopeBytesPerMinute).toBeGreaterThanOrEqual(FLOOR)
    expect(shadow.data.slopeBytesPerMinute).toBeLessThan(RATE * 1.01)
  })

  test('forwards the seed beside, never into, the judged resource verdict of each worker', () => {
    const record = jest.fn()
    const verdict = jest.fn(() => ({
      ok: true,
      outcome: 'passed',
      violations: [],
      inconclusive: [],
      measured: {},
    }))
    const run = new churn.TelemetryChurn({
      fixture: {},
      processes: { workers: new Map() },
      decoder: {},
      record,
      clock: { now: () => 0 },
      limits: {},
      verdict,
      brokers: [],
      stem: 'stem',
      line: '4.3.1',
      directory: '/evidence',
    })
    const seed = { mode: LIVE_SEED.mode, bytesPerInterval: INTERVAL, intervals: 11 }
    const results = churn.KINDS.map((kind) => ({
      kind,
      clientId: `stem-${kind}`,
      uuid: `uuid-${kind}`,
      input: {},
      facts: { requirements: { ok: true, violations: [] } },
      ...(kind === 'consumer' ? { seed } : {}),
    }))
    const exits = Object.fromEntries(churn.KINDS.map((kind) => [kind, { kind }]))
    const judged = run.judge(
      results,
      { errors: [], exits, oracle: null },
      { t0: 0, epochAt: 1, boundaryAt: 2, cleanupEndAt: 3 },
      new churn.TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    )
    const verdicts = record.mock.calls
      .map(([row]) => row)
      .filter((row) => row.kind === 'resource-verdict')
    expect(verdicts.find((row) => row.clientKind === 'consumer').seed).toEqual(seed)
    expect(judged.workers.find((entry) => entry.kind === 'consumer').seed).toEqual(seed)
    for (const row of verdicts) {
      expect(row.result).not.toHaveProperty('seed')
      if (row.clientKind !== 'consumer') expect(row).not.toHaveProperty('seed')
    }
    for (const [input] of verdict.mock.calls) expect(input).not.toHaveProperty('seed')
  })
})

describe('live seeded control records', () => {
  const directories = []
  const temporary = () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'live-seed-'))
    directories.push(directory)
    return directory
  }

  afterEach(() => {
    for (const directory of directories.splice(0))
      fs.rmSync(directory, { recursive: true, force: true })
  })

  test('writes its records only under controls and never over an earlier one', () => {
    const directory = temporary()
    const records = new ControlRecords(directory)
    records.record({ kind: 'sample' })
    records.record({ kind: 'verdict' })
    records.finish({ ok: true, failure: null, workers: [] })
    expect(fs.readdirSync(directory)).toEqual(['controls'])
    expect(fs.readdirSync(path.join(directory, 'controls')).sort()).toEqual([
      'telemetry-seeded-rows.jsonl',
      'telemetry-seeded-verdict.json',
    ])
    const lines = fs
      .readFileSync(path.join(directory, 'controls', 'telemetry-seeded-rows.jsonl'), 'utf8')
      .trim()
      .split('\n')
    expect(lines.map((line) => JSON.parse(line).kind)).toEqual(['sample', 'verdict'])
    const verdict = JSON.parse(
      fs.readFileSync(path.join(directory, 'controls', 'telemetry-seeded-verdict.json'), 'utf8')
    )
    expect(verdict).toMatchObject({
      control: 'telemetry-seeded',
      mode: LIVE_SEED.mode,
      acceptance: false,
      node: process.version,
      ok: true,
    })
    expect(() => new ControlRecords(directory)).toThrow('EEXIST')
    expect(() => records.finish({ ok: false, failure: 'again', workers: [] })).toThrow('EEXIST')
  })

  test('is refused by the churn archive verifier when any row carries a seed', () => {
    const read = () => {
      throw new Error('the archive was read')
    }
    const row = { kind: 'resource-verdict', clientKind: 'producer' }
    const seeded = { results: { rows: [{ ...row, seed: { mode: LIVE_SEED.mode } }] } }
    expect(() => verifyChurn(seeded, '/', { root: 'root' }, read)).toThrow(
      'A seeded run is never acceptance evidence'
    )
    expect(() => verifyChurn({ results: { rows: [row] } }, '/', { root: 'root' }, read)).toThrow(
      'the archive was read'
    )
  })

  test('refuses a seeded parent that would write the default scenario evidence', () => {
    const saved = { seed: process.env[LIVE_SEED.env], line: process.env.KAFKA_VERSION }
    const exposed = globalThis.gc
    process.env[LIVE_SEED.env] = LIVE_SEED.mode
    delete process.env.KAFKA_VERSION
    globalThis.gc = () => {}
    const build = (options = {}) =>
      buildTelemetryChurn({ brokers: [], phases: PROFILES.telemetry, ...options })
    try {
      expect(() => build()).toThrow('A seeded telemetry run records only to a control sink')
      expect(() => build({ record: () => {} })).toThrow('needs the configured broker line')
      delete process.env[LIVE_SEED.env]
      expect(() => build()).toThrow('needs the configured broker line')
    } finally {
      globalThis.gc = exposed
      for (const [name, value] of [
        [LIVE_SEED.env, saved.seed],
        ['KAFKA_VERSION', saved.line],
      ])
        if (value === undefined) delete process.env[name]
        else process.env[name] = value
    }
  })
})

describe('live seeded control judge', () => {
  const exit = (over = {}) => ({
    event: 'close',
    code: 0,
    signal: null,
    killed: false,
    aborted: false,
    watchdogExpired: false,
    stderrBytes: 0,
    unhandled: 0,
    receipts: 1,
    afterDisconnectMs: 120,
    ...over,
  })
  const facts = (kind, over = {}) => ({
    kind,
    violations: [],
    measured: { samples: 11, slopeBytesPerMinute: 131270, endpointBytes: 655360 },
    samples: 11,
    shadow: {
      scope: 'shadow',
      acceptance: false,
      complete: true,
      readings: 11,
      data: { slopeBytesPerMinute: 131000, endpointBytes: 655360 },
    },
    seed: {
      mode: LIVE_SEED.mode,
      rateBytesPerMinute: RATE,
      bytesPerInterval: INTERVAL,
      intervals: 11,
      buffers: 11,
      bytes: 11 * INTERVAL,
    },
    exit: exit(),
    ...over,
  })
  const workers = (over = {}) => churn.KINDS.map((kind) => facts(kind, over))
  /** The run with one change applied to its first worker. */
  const changed = (over) => [facts('producer', over), ...workers().slice(1)]
  const judge = (run) => judgeSeededRun(run, TELEMETRY)

  test('keeps the five-minute seed diagnostic and permits verdicts without slope or endpoint failures', () => {
    expect(() => judge(workers())).not.toThrow()
    expect(LIVE_SEED.allowed).toEqual([])
    expect(() => judge(changed({ violations: ['closed transient ceiling'] }))).toThrow(
      'violations the seed does not explain'
    )
    expect(() => judge(changed({ violations: ['retained slope'] }))).toThrow(
      'violations the seed does not explain'
    )
    expect(() => judge(changed({ violations: ['retained endpoint'] }))).toThrow(
      'violations the seed does not explain'
    )
    expect(() => judge([factsOf({ kind: 'producer' }), ...workers().slice(1)])).toThrow(
      'violations list'
    )
    expect(() => judge(churn.KINDS.slice(1).map((kind) => facts(kind)))).toThrow(
      'one worker of every kind'
    )
  })

  test('fails a violation the seed does not explain', () => {
    for (const label of ['identity roles', 'sampler read failure', 'active retained ceiling'])
      expect(() => judge(changed({ violations: [label] }))).toThrow(
        'violations the seed does not explain'
      )
  })

  test('fails a data slope below the seeded rate minus the stated tolerance', () => {
    const data = (slopeBytesPerMinute) => ({
      shadow: { ...facts('producer').shadow, data: { slopeBytesPerMinute, endpointBytes: 0 } },
    })
    expect(FLOOR).toBe(RATE - LIVE_SEED.toleranceFraction * RATE)
    expect(() => judge(changed(data(FLOOR)))).not.toThrow()
    expect(() => judge(changed(data(FLOOR - 1)))).toThrow('shadow data slope')
    expect(() => judge(changed(data(TELEMETRY.slopeBytesPerMinute)))).toThrow('shadow data slope')
    for (const shadow of [
      { ...facts('producer').shadow, complete: false },
      { ...facts('producer').shadow, acceptance: true },
      { ...facts('producer').shadow, scope: 'acceptance' },
      null,
    ])
      expect(() => judge(changed({ shadow }))).toThrow('complete shadow report')
  })

  test('fails a seed that retained other than the configured bytes per interval', () => {
    const seed = facts('producer').seed
    for (const [over, message] of [
      [{ bytes: 10 * INTERVAL }, 'retained bytes'],
      [{ bytesPerInterval: INTERVAL / 2, bytes: (11 * INTERVAL) / 2 }, 'bytes per interval'],
      [{ intervals: 10, bytes: 10 * INTERVAL }, 'one buffer for every sample'],
      [{ buffers: 10 }, 'one buffer for every sample'],
      [{ mode: 'slope3x' }, 'seed mode'],
    ])
      expect(() => judge(changed({ seed: { ...seed, ...over } }))).toThrow(message)
    expect(() => judge(changed({ seed: null }))).toThrow('seed mode')
  })

  test('fails a worker that did not exit naturally on its own verdict', () => {
    for (const over of [
      { killed: true },
      { aborted: true },
      { watchdogExpired: true },
      { stderrBytes: 5 },
      { unhandled: 1 },
      { receipts: 0 },
      { code: 1 },
      { signal: 'SIGKILL' },
      { event: 'timeout' },
      { afterDisconnectMs: TELEMETRY.exitMs + 1 },
      { afterDisconnectMs: null },
    ])
      expect(() => judge(changed({ exit: exit(over) }))).toThrow('natural exit')
  })

  test('requires the recorded control to have passed and judges it again from its workers', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'live-seed-'))
    try {
      const file = path.join(directory, 'controls', 'telemetry-seeded-verdict.json')
      expect(() => requireControl(directory, TELEMETRY)).toThrow('ENOENT')
      new ControlRecords(directory).finish({ ok: true, failure: null, workers: workers() })
      expect(() => requireControl(directory, TELEMETRY)).not.toThrow()
      const record = JSON.parse(fs.readFileSync(file, 'utf8'))
      const rewrite = (over) => fs.writeFileSync(file, JSON.stringify({ ...record, ...over }))
      rewrite({ ok: false, failure: 'the seed was not detected' })
      expect(() => requireControl(directory, TELEMETRY)).toThrow('the seed was not detected')
      rewrite({ node: 'v0.0.0' })
      expect(() => requireControl(directory, TELEMETRY)).toThrow('node')
      rewrite({ acceptance: true })
      expect(() => requireControl(directory, TELEMETRY)).toThrow('acceptance')
      rewrite({ workers: changed({ violations: ['sampler read failure'] }) })
      expect(() => requireControl(directory, TELEMETRY)).toThrow(
        'violations the seed does not explain'
      )
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})
