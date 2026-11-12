const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const vm = require('node:vm')
const { LongWindowDiagnostic } = require('../longWindowDiagnostic')
const { LONG_WINDOW, ShadowReport, identityOf } = require('../longWindow')

const KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']

const reportOf = (kind, { tripped = false, complete = true } = {}) =>
  complete
    ? { kind, complete: true, firstUse: { tripped }, mid: { data: { slopeBytesPerMinute: 0 } } }
    : { kind, complete: false, error: 'no worker result' }

/** @returns {object} a complete worker with independent functional and journal evidence */
const workerOf = (kind) => {
  const samples = [...Array.from({ length: 42 }, (_, i) => 60000 + i * 30000), 1319000].map(
    (at) => ({
      admittedAtMs: at,
      completedAtMs: at + 100,
      snapshot: {
        memory: {
          at: at + 100,
          retainedBytes: 100000,
          gc: { admittedAt: at, completedAt: at + 100, observed: 2 },
        },
        journals: { byteLength: 4096, cursor: at / 1000, overflow: false },
      },
    })
  )
  return {
    kind,
    execArgv: ['--expose-gc', '--no-flush-bytecode'],
    input: {
      diagnostic: true,
      acceptanceEligible: false,
      baseline: { journals: { byteLength: 4096, cursor: 0, overflow: false } },
      samples,
      teardown: { journals: { byteLength: 4096, cursor: 1400, overflow: false } },
      sampler: { skipped: [], readFailures: [] },
    },
    result: { ok: false, violations: ['diagnostic inputs are not acceptance-eligible'] },
    exit: {
      event: 'close',
      code: 0,
      signal: null,
      stderrBytes: 0,
      unhandled: 0,
      receipts: 1,
      afterDisconnectMs: 10,
    },
    facts: {
      timing: { phaseAt: 0 },
      stages: Array.from({ length: 21 }, (_, i) => ({
        stage: 'lifecycle',
        startedAt: (i + 1) * 60000,
        doneAt: (i + 1) * 60000 + 4000,
      })),
      requirements: { ok: true, violations: [] },
    },
    shadow: {
      complete: true,
      engine: {
        readings: samples.map(({ snapshot }) => ({
          at: snapshot.memory.at,
          code: 1000,
          bytecode: 1000,
        })),
      },
    },
  }
}

describe('long-window driver', () => {
  let directory
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'long-window-driver-'))
  })
  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  const driver = (arm) =>
    new LongWindowDiagnostic({
      directory,
      arm,
      identity: identityOf({ arm, replicate: '1', tree: 'a'.repeat(40), env: {} }),
      modules: { ShadowReport, LONG_WINDOW },
    })

  test('refuses to run outside diagnostic CI, for an unknown arm or with a stale switch', async () => {
    const env = { CI: 'true', LONG_WINDOW_ARM: 'unseeded', LONG_WINDOW_REPLICATE: '1' }
    const run = (override, argv = ['--diagnostic-only', directory]) =>
      LongWindowDiagnostic.main(argv, { ...env, ...override })
    await expect(run({ CI: undefined })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ LONG_WINDOW_ARM: 'other' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ TELEMETRY_SEED: 'slope2x' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ [LONG_WINDOW.env]: '1' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ SHARE_PEAK_DIAGNOSTIC: '1' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({}, [directory])).rejects.toThrow('Use --diagnostic-only')
    fs.writeFileSync(path.join(directory, 'stale'), '')
    await expect(run({})).rejects.toThrow('must be fresh')
  })

  test('writes every file with the shadow flags, the frozen candidate and the commit first', () => {
    driver('seeded').write('long-window.json', { workers: [] })
    const written = JSON.parse(fs.readFileSync(path.join(directory, 'long-window.json'), 'utf8'))
    expect(Object.keys(written).slice(0, 3)).toEqual(['diagnostic', 'scope', 'acceptanceEligible'])
    expect(written).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      arm: 'seeded',
      tree: 'a'.repeat(40),
      workers: [],
    })
    expect(written.candidate.warmupMs).toBe(420000)
  })

  test('reports a worker without a result as an incomplete shadow report', () => {
    expect(driver('unseeded').reportOf({ kind: 'admin' })).toEqual({
      scope: 'shadow',
      acceptance: false,
      diagnostic: true,
      kind: 'admin',
      arm: 'unseeded',
      execArgv: null,
      complete: false,
      error: 'no worker result',
    })
  })

  test('rejects functional failures independently of the ineligible resource verdict', () => {
    const run = driver('unseeded')
    const oracle = { ok: true, violations: [] }
    expect(run.reportOf(workerOf('producer'), oracle).complete).toBe(true)
    for (const change of [
      (worker) => {
        worker.facts.requirements = { ok: false, violations: ['missing reopen'] }
      },
      (worker) => {
        worker.exit.code = 1
      },
      (worker) => {
        worker.exit.killed = true
      },
      ...[
        { event: 'timeout' },
        { signal: 'SIGKILL' },
        { stderrBytes: 1 },
        { unhandled: 1 },
        { receipts: 0 },
        { receipts: 2 },
        { aborted: true },
        { watchdogExpired: true },
        { afterDisconnectMs: null },
        { afterDisconnectMs: -1 },
      ].map((exit) => (worker) => Object.assign(worker.exit, exit)),
      (worker) => {
        worker.shadow.complete = false
      },
      (worker) => {
        worker.input.sampler.readFailures.push({ message: 'failed reading' })
      },
    ]) {
      const worker = workerOf('producer')
      change(worker)
      const report = run.reportOf(worker, oracle)
      expect(report.complete).toBe(false)
      expect(report.error).toMatch(/functional/)
      expect(report).not.toHaveProperty('wouldBe')
    }
    for (const invalid of [undefined, { ok: false, violations: ['durability mismatch'] }]) {
      const report = run.reportOf(workerOf('producer'), invalid)
      expect(report.complete).toBe(false)
      expect(report.error).toMatch(/oracle/)
      expect(report).not.toHaveProperty('wouldBe')
    }
  })

  test('rejects journal overflow, storage changes and cursor regression before reporting slopes', () => {
    for (const change of [
      (worker) => {
        worker.input.samples[3].snapshot.journals.overflow = true
      },
      (worker) => {
        worker.input.teardown.journals.byteLength += 1
      },
      (worker) => {
        worker.input.teardown.journals.cursor = 0
      },
    ]) {
      const worker = workerOf('producer')
      change(worker)
      const report = driver('unseeded').reportOf(worker, { ok: true, violations: [] })
      expect(report.complete).toBe(false)
      expect(report.error).toMatch(/journal/)
      expect(report).not.toHaveProperty('wouldBe')
    }
  })

  test('archives incomplete reports and fails the job on an oracle mismatch', async () => {
    const run = driver('unseeded')
    const result = {
      ok: false,
      violations: ['diagnostic inputs are not acceptance-eligible'],
      workers: KINDS.map(workerOf),
      oracle: { ok: false, violations: ['durability mismatch'] },
      archive: {
        processes: {},
        handshakes: KINDS.map((kind) => ({
          kind,
          execArgv: ['--expose-gc', '--no-flush-bytecode'],
        })),
      },
    }
    run.modules.PROFILES = { telemetry: [] }
    run.modules.buildTelemetryChurn = () => ({
      run: async () => result,
      close: async () => {},
    })
    await expect(run.execute()).rejects.toThrow(/oracle/)
    const archived = JSON.parse(fs.readFileSync(path.join(directory, 'long-window.json'), 'utf8'))
    expect(archived.workers).toHaveLength(4)
    for (const report of archived.workers) {
      expect(report.complete).toBe(false)
      expect(report).not.toHaveProperty('wouldBe')
    }
    result.oracle = { ok: true, violations: [] }
    await expect(run.execute()).resolves.toBeUndefined()
  })

  test('records each worker runtime flag and fails the job when a required flag or handshake is missing', async () => {
    const run = driver('unseeded')
    const result = {
      workers: KINDS.map(workerOf),
      oracle: { ok: true, violations: [] },
      archive: {
        processes: {},
        handshakes: KINDS.map((kind) => ({
          kind,
          execArgv: ['--expose-gc', '--no-flush-bytecode'],
        })),
      },
    }
    run.modules.PROFILES = { telemetry: [] }
    run.modules.buildTelemetryChurn = () => ({
      run: async () => result,
      close: async () => {},
    })
    await expect(run.execute()).resolves.toBeUndefined()
    const read = (name) => JSON.parse(fs.readFileSync(path.join(directory, name), 'utf8'))
    for (const name of ['long-window.json', 'workers.json']) {
      const archived = read(name)
      expect(archived.candidate).toMatchObject({ version: 2, v8Flags: ['--no-flush-bytecode'] })
      for (const worker of archived.workers)
        expect(worker.execArgv).toEqual(['--expose-gc', '--no-flush-bytecode'])
    }
    for (const kind of KINDS) {
      const handshake = result.archive.handshakes.find((row) => row.kind === kind)
      for (const execArgv of [undefined, [], ['--expose-gc']]) {
        handshake.execArgv = execArgv
        await expect(run.execute()).rejects.toThrow(/no-flush-bytecode/)
        const worker = read('long-window.json').workers.find((row) => row.kind === kind)
        expect(worker.complete).toBe(false)
        expect(worker).not.toHaveProperty('wouldBe')
      }
      handshake.execArgv = ['--expose-gc', '--no-flush-bytecode']
    }
    result.archive.handshakes.pop()
    await expect(run.execute()).rejects.toThrow(/no-flush-bytecode/)
  })

  test('archives a worker beside its report without the id ledgers', () => {
    const archived = driver('unseeded').archiveOf({
      kind: 'producer',
      uuid: 'u',
      exit: { code: 1 },
      result: { ok: false },
      input: { samples: [] },
      shadow: { heapSpaces: [] },
      seed: undefined,
      facts: {
        stages: [{ tick: 1 }],
        timing: { phaseAt: 1 },
        actions: {},
        requirements: { ok: true },
        ordinary: { offered: [1, 2, 3] },
        share: { exchanges: [1] },
      },
    })
    expect(archived.facts).toEqual({
      stages: [{ tick: 1 }],
      timing: { phaseAt: 1 },
      actions: {},
      requirements: { ok: true },
    })
    expect(archived).toMatchObject({ kind: 'producer', seed: null, input: { samples: [] } })
  })

  test('requires all four reports and, in the warm-up-seed arm, a tripped first-use bound', () => {
    const reports = (options) => KINDS.map((kind) => reportOf(kind, options))
    expect(() => driver('unseeded').verify(reports({}))).not.toThrow()
    expect(() => driver('unseeded').verify(reports({}).slice(1))).toThrow('incomplete long-window')
    expect(() =>
      driver('seeded').verify([...reports({}).slice(1), reportOf('producer', { complete: false })])
    ).toThrow('producer: no worker result')
    expect(() => driver('warmup-seed').verify(reports({ tripped: true }))).not.toThrow()
    expect(() => driver('warmup-seed').verify(reports({ tripped: false }))).toThrow(
      'did not trip the first-use bound for producer, consumer, admin, shareConsumer'
    )
  })
})

/**
 * Executes the real CLI completion handlers with a broker-free fulfilled or rejected driver.
 * @param {object} options worker environment, resource verdict and optional execution failure
 * @returns {Promise<object>} process exit status and unchanged output of the CLI
 */
const cliOf = async ({ env = {}, verdict = { ok: false }, error, profile = 'telemetry' } = {}) => {
  const source = fs.readFileSync(path.join(__dirname, '../soak.js'), 'utf8')
  const mainModule = {}
  const load = Object.assign(
    (name) => {
      if (name === './telemetryChurn') return { KINDS }
      throw new Error(`Unexpected CLI dependency ${name}`)
    },
    { main: mainModule }
  )
  const proc = { env, argv: ['node', 'soak.js'], send: () => {}, exitCode: undefined }
  const output = { log: jest.fn(), error: jest.fn() }
  await vm.runInNewContext(source.slice(source.lastIndexOf('if (require.main === module)')), {
    require: load,
    module: mainModule,
    process: proc,
    console: output,
    LongWindow: require('../longWindow'),
    parseArgs: () => ({ profile, phaseSeconds: null }),
    main: async () => {
      if (error) throw error
      return verdict
    },
  })
  return { proc, output }
}

describe('long-window worker CLI', () => {
  test('exits zero after successful diagnostic execution for every kind while preserving the failed resource verdict', async () => {
    const verdict = {
      ok: false,
      outcome: 'failed',
      violations: [
        'diagnostic inputs are not acceptance-eligible',
        'post-GC sample shape or retained state',
      ],
    }
    for (const kind of KINDS) {
      const { proc, output } = await cliOf({
        env: { TELEMETRY_LONG_WINDOW: '1', TELEMETRY_WORKER_KIND: kind },
        verdict,
      })
      expect(proc.exitCode).toBe(0)
      expect(output.log).toHaveBeenCalledWith(JSON.stringify(verdict))
      expect(output.error).not.toHaveBeenCalled()
      expect(verdict.ok).toBe(false)
    }
  })

  test('keeps failed verdicts fatal outside the long worker and keeps execution failures fatal inside it', async () => {
    for (const env of [
      {},
      { TELEMETRY_WORKER_KIND: 'producer' },
      { TELEMETRY_LONG_WINDOW: '0', TELEMETRY_WORKER_KIND: 'producer' },
      { TELEMETRY_LONG_WINDOW: '1' },
      { TELEMETRY_LONG_WINDOW: '1', TELEMETRY_WORKER_KIND: 'foreign' },
    ]) {
      expect((await cliOf({ env })).proc.exitCode).toBe(1)
      expect((await cliOf({ env, verdict: { ok: true } })).proc.exitCode).toBe(0)
    }
    const env = { TELEMETRY_LONG_WINDOW: '1', TELEMETRY_WORKER_KIND: 'producer' }
    expect((await cliOf({ env, profile: 'idle' })).proc.exitCode).toBe(1)
    const failure = new Error('functional workload failed')
    const { proc, output } = await cliOf({ env, error: failure })
    expect(proc.exitCode).toBe(1)
    expect(output.error).toHaveBeenCalledWith(failure)
    expect(output.log).not.toHaveBeenCalled()
  })
})

describe('long-window sample timing', () => {
  test('never arms a negative sample delay when the clock crosses the grid deadline after an early wake', async () => {
    const { Soak } = require('../soak')
    const times = [0, 0, 59999.99, 60000.002508]
    const clock = { now: () => times.shift() ?? 60000.01 }
    let finish
    const workload = new Promise((resolve) => {
      finish = resolve
    })
    const run = new Soak({
      kafka: {},
      context: {},
      phases: [],
      sampleAt: [60000],
      settleMs: 0,
      clock,
      probe: { snapshot: async () => ({}) },
      workloads: { idle: () => workload },
      record: (row) => {
        if (row.kind === 'sample') {
          run.sampling = false
          finish()
        }
      },
    })
    run.nap = jest.fn(async () => {})
    await run.runPhase({ name: 'telemetry', workload: 'idle', ms: 1320000 }, 0)
    expect(run.nap.mock.calls.length).toBeGreaterThanOrEqual(1)
    for (const [delay] of run.nap.mock.calls) expect(delay).toBeGreaterThanOrEqual(0)
    expect(run.samples).toHaveLength(1)
    expect(run.samples[0].admittedPhaseMs).toBeGreaterThanOrEqual(60000)
    expect(run.skipped).toEqual([])
    expect(run.readFailures).toEqual([])
  })
})
