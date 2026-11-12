const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const yaml = require('js-yaml')
const { spawnSync } = require('node:child_process')
const { readSnapshot } = require('../../e2e/telemetry/retained/heapDiff')
const { installClosedDiagnostic, ClosedCapture } = require('../closedDiagnostic')
const { compareSnapshots, analyseWorker } = require('../closedAnalysis')
const { Soak, TelemetryResourceVerdict } = require('../soak')

const flags = { diagnostic: true, acceptanceEligible: false }
const reading = (at) => ({
  sockets: { open: 0 },
  memory: { at, heapUsed: at, external: 0, retainedBytes: at, gc: { observed: 2 } },
})

const fixture = (growth = 0) => {
  const strings = []
  const intern = (value) => {
    if (!strings.includes(value)) strings.push(value)
    return strings.indexOf(value)
  }
  const types = ['synthetic', 'object', 'array', 'code']
  const edgeTypes = ['property', 'internal', 'weak']
  const definitions = [
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', '__telemetryClosedRoots', 1],
        ['property', 'engine', 8],
      ],
    ],
    [
      'object',
      'ClosedRoots',
      3,
      10,
      [
        ['property', 'client', 2],
        ['property', 'registry', 4],
        ['property', 'harness', 5],
      ],
    ],
    ['object', 'Object', 5, 10, [['property', 'metrics', 3]]],
    ['object', 'TelemetryMetrics', 7, 20 + growth, []],
    [
      'object',
      'Registry',
      9,
      10,
      [
        ['property', 'registered', 7],
        ['property', 'client', 2],
      ],
    ],
    [
      'object',
      'TelemetryWorker',
      11,
      10,
      [
        ['property', 'claims', 6],
        ['property', 'client', 2],
        ['weak', 'weak', 9],
      ],
    ],
    ['array', 'Array', 13, 30 + 2 * growth, []],
    ['array', 'Array', 15, 10, []],
    ['code', 'system / FeedbackVector', 17, 40 + 3 * growth, [['internal', 'application', 10]]],
    ['object', 'WeakOnly', 19, 9999, []],
    ['object', 'Unresolved', 21, 5 + growth, []],
  ]
  const edges = []
  const nodes = definitions.flatMap(([type, name, id, bytes, links]) => {
    for (const [kind, label, target] of links)
      edges.push(edgeTypes.indexOf(kind), intern(label), target * 5)
    return [types.indexOf(type), intern(name), id, bytes, links.length]
  })
  return {
    snapshot: {
      node_count: definitions.length,
      edge_count: edges.length / 3,
      meta: {
        node_fields: ['type', 'name', 'id', 'self_size', 'edge_count'],
        node_types: [types],
        edge_fields: ['type', 'name_or_index', 'to_node'],
        edge_types: [edgeTypes],
      },
    },
    nodes,
    edges,
    strings,
  }
}

describe('closed transient diagnostic', () => {
  let directory
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'closed-diagnostic-'))
  })
  afterEach(() => {
    delete globalThis.__telemetryClosedRoots
    fs.rmSync(directory, { recursive: true, force: true })
  })

  test('leaves the switch-off path byte-identical without snapshots, readings or fields', async () => {
    const execute = async (install) => {
      const rows = []
      const probe = { snapshot: jest.fn(async () => reading(100)) }
      const worker = { kind: 'admin', soak: { Soak, TELEMETRY: { warmupMs: 60000 } } }
      const original = { ...worker.soak }
      const engine = { writeHeapSnapshot: jest.fn(), getHeapSpaceStatistics: jest.fn() }
      if (install) installClosedDiagnostic(worker, { TELEMETRY_CLOSED_DIAGNOSTIC: '0' }, { engine })
      expect(worker.soak).toEqual(original)
      const run = new worker.soak.Soak({
        kafka: {},
        probe,
        phases: [],
        settleMs: 0,
        record: (row) => rows.push(row),
        judge: () => ({ ok: true }),
      })
      await run.run()
      expect(engine.writeHeapSnapshot).not.toHaveBeenCalled()
      expect(engine.getHeapSpaceStatistics).not.toHaveBeenCalled()
      expect(probe.snapshot).toHaveBeenCalledTimes(2)
      return JSON.stringify(rows)
    }
    expect(await execute(true)).toBe(await execute(false))
    expect(fs.readdirSync(directory)).toEqual([])
  })

  test('captures original endpoints after settled readings and adds one warm reading with the same protocol', async () => {
    let at = 0
    const events = []
    const rows = []
    class FakeSoak {
      constructor(options) {
        Object.assign(this, options)
        this.phaseStartedAt = null
      }

      async settle() {
        events.push('settle')
      }

      async runPhase() {
        this.phaseStartedAt = 0
        at = 59999
        await this.probe.snapshot()
        at = 60000
        await this.probe.snapshot()
        at = 90000
        await this.probe.snapshot()
      }
    }
    const engine = {
      getHeapSpaceStatistics: jest.fn(() => [{ space_name: 'old_space', space_used_size: at }]),
      getHeapCodeStatistics: jest.fn(() => ({ code_and_metadata_size: at })),
      writeHeapSnapshot: jest.fn((file) => {
        events.push(`snapshot:${at}`)
        fs.writeFileSync(file, '{}')
      }),
    }
    const worker = { kind: 'admin', soak: { Soak: FakeSoak, TELEMETRY: { warmupMs: 60000 } } }
    installClosedDiagnostic(
      worker,
      {
        CI: 'true',
        TELEMETRY_CLOSED_DIAGNOSTIC: '1',
        TELEMETRY_CLOSED_ROOT: directory,
      },
      { engine }
    )
    const run = new worker.soak.Soak({
      context: { telemetryChurn: { client: {}, registry: {} } },
      warmupMs: 60000,
      clock: { now: () => at },
      probe: {
        snapshot: async () => {
          events.push(`gc-settled:${at}`)
          return reading(at)
        },
      },
      record: (row) => rows.push(row),
    })
    await run.settle()
    run.record({ kind: 'baseline', snapshot: await run.probe.snapshot() })
    await run.runPhase()
    at = 360000
    await run.settle()
    run.record({ kind: 'teardown', snapshot: await run.probe.snapshot() })
    expect(events).toEqual([
      'settle',
      'gc-settled:0',
      'snapshot:0',
      'gc-settled:59999',
      'gc-settled:60000',
      'settle',
      'gc-settled:60000',
      'snapshot:60000',
      'gc-settled:90000',
      'settle',
      'gc-settled:360000',
      'snapshot:360000',
    ])
    const captured = ['baseline', 'warm', 'teardown'].map((name) =>
      JSON.parse(fs.readFileSync(path.join(directory, 'admin', `${name}.json`)))
    )
    expect(captured.map((row) => row.memory.at)).toEqual([0, 60000, 360000])
    for (const row of captured) {
      expect(row).toMatchObject({
        ...flags,
        kind: 'admin',
        heapSpaces: [{ space_name: 'old_space' }],
        code: {},
      })
      expect(row.memory.gc.observed).toBe(2)
    }
    expect(rows.map((row) => row.kind)).toEqual(['baseline', 'teardown'])
    expect(worker.soak.TELEMETRY).toMatchObject(flags)
    const verdict = TelemetryResourceVerdict.evaluate({ clientCount: 1, samples: [], ...flags })
    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain('diagnostic inputs are not acceptance-eligible')
  })

  test('requires explicit diagnostic CI and refuses duplicate capture checkpoints', () => {
    const worker = { kind: 'admin', soak: { Soak } }
    expect(() => installClosedDiagnostic(worker, { TELEMETRY_CLOSED_DIAGNOSTIC: '1' })).toThrow(
      /diagnostic CI/
    )
    const capture = new ClosedCapture({
      directory,
      kind: 'admin',
      engine: {
        getHeapSpaceStatistics: () => [],
        getHeapCodeStatistics: () => ({}),
        writeHeapSnapshot: (file) => fs.writeFileSync(file, '{}'),
      },
    })
    capture.record('baseline', reading(0))
    expect(() => capture.record('baseline', reading(0))).toThrow(/duplicate/)
  })

  test('reconciles additive constructor and owner growth without hiding feedback-held application data', () => {
    const result = compareSnapshots(fixture(), fixture(10))
    expect(result.byScope.product.delta).toBe(10)
    expect(result.byScope.harness.delta).toBe(20)
    expect(result.byScope.engine.delta).toBe(30)
    expect(result.byScope.unresolved.delta).toBe(10)
    expect(result.selfDelta).toBe(70)
    expect(Object.values(result.byScope).reduce((sum, row) => sum + row.delta, 0)).toBe(70)
    expect(result.groups.find((row) => row.constructor === 'TelemetryMetrics')).toMatchObject({
      scope: 'product',
      delta: 10,
    })
    const claims = result.groups.find((row) => row.owner === 'harness.claims')
    expect(claims).toMatchObject({ scope: 'harness', delta: 20 })
    expect(claims.paths[0].trace.rootPath.map((row) => row.edge?.name)).toContain('claims')
    expect(JSON.stringify(result)).not.toContain('WeakOnly')
  })

  test('keeps product Map container bytes out of engine feedback storage', () => {
    const before = fixture()
    const after = fixture(10)
    for (const raw of [before, after]) raw.strings[raw.strings.indexOf('TelemetryMetrics')] = 'Map'
    const result = compareSnapshots(before, after)
    expect(result.groups.find((row) => row.constructor === 'Map')).toMatchObject({
      scope: 'product',
      delta: 10,
    })
    expect(result.byScope.engine.delta).toBe(30)
  })

  test('follows real V8 closure contexts only through matching product and harness source trees', () => {
    for (const [source, name] of [
      ['src', 'ClosedProductState'],
      ['scripts', 'ClosedHarnessState'],
    ]) {
      fs.mkdirSync(path.join(directory, source))
      fs.writeFileSync(
        path.join(directory, source, 'owner.js'),
        `
        class ${name} {}
        const state = []
        module.exports = {
          owner: { read: () => state },
          grow: () => { for (let i = 0; i < 100; i++) state.push(new ${name}()) },
        }
      `
      )
    }
    const child = spawnSync(
      process.execPath,
      [
        '--expose-gc',
        '-e',
        `
      const path = require('node:path')
      const v8 = require('node:v8')
      const directory = process.argv[1]
      const product = require(path.join(directory, 'src/owner.js'))
      const harness = require(path.join(directory, 'scripts/owner.js'))
      globalThis.__telemetryClosedRoots = { client: product.owner, harness: harness.owner }
      gc(); gc()
      v8.writeHeapSnapshot(path.join(directory, 'before.heapsnapshot'))
      product.grow(); harness.grow()
      gc(); gc()
      v8.writeHeapSnapshot(path.join(directory, 'after.heapsnapshot'))
    `,
        directory,
      ],
      { encoding: 'utf8', timeout: 30000 }
    )
    expect(child.error).toBeUndefined()
    expect(child.status).toBe(0)
    const result = compareSnapshots(
      readSnapshot(path.join(directory, 'before.heapsnapshot')),
      readSnapshot(path.join(directory, 'after.heapsnapshot'))
    )
    expect(result.groups.find((row) => row.constructor === 'ClosedProductState')).toMatchObject({
      scope: 'product',
      owner: 'client.read',
    })
    expect(result.groups.find((row) => row.constructor === 'ClosedHarnessState')).toMatchObject({
      scope: 'harness',
      owner: 'harness.read',
    })
  })

  test('emits both endpoint comparisons from the existing snapshot reader and bounds compressed evidence', async () => {
    for (const [name, growth] of [
      ['baseline', 0],
      ['warm', 5],
      ['teardown', 10],
    ]) {
      fs.writeFileSync(
        path.join(directory, `${name}.heapsnapshot`),
        JSON.stringify(fixture(growth))
      )
      fs.writeFileSync(
        path.join(directory, `${name}.json`),
        JSON.stringify({
          ...flags,
          kind: 'admin',
          checkpoint: name,
          memory: reading(growth).memory,
          heapSpaces: [],
          code: {},
        })
      )
    }
    const summary = await analyseWorker(directory, { kind: 'admin' })
    expect(summary).toMatchObject({ ...flags, kind: 'admin', complete: true })
    expect(summary.original.selfDelta).toBe(70)
    expect(summary.warm.selfDelta).toBe(35)
    expect(summary.original.retainedDelta).toBe(10)
    expect(summary.warm.retainedDelta).toBe(5)
    expect(fs.readdirSync(directory).filter((file) => file.endsWith('.heapsnapshot'))).toEqual([])
    expect(
      fs.readdirSync(directory).filter((file) => file.endsWith('.heapsnapshot.gz'))
    ).toHaveLength(3)
    expect(JSON.parse(fs.readFileSync(path.join(directory, 'summary.json')))).toEqual(summary)
  })

  test('archives incomplete captures and enforces the streaming compressed byte cap', async () => {
    fs.writeFileSync(path.join(directory, 'baseline.heapsnapshot'), JSON.stringify(fixture()))
    await expect(analyseWorker(directory, { kind: 'admin' })).rejects.toThrow()
    expect(JSON.parse(fs.readFileSync(path.join(directory, 'summary.json')))).toMatchObject({
      ...flags,
      complete: false,
    })
    expect(fs.existsSync(path.join(directory, 'baseline.heapsnapshot.gz'))).toBe(true)
    fs.unlinkSync(path.join(directory, 'baseline.heapsnapshot.gz'))
    for (const name of ['baseline', 'warm', 'teardown']) {
      fs.writeFileSync(path.join(directory, `${name}.heapsnapshot`), JSON.stringify(fixture()))
      fs.writeFileSync(
        path.join(directory, `${name}.json`),
        JSON.stringify({
          ...flags,
          kind: 'admin',
          checkpoint: name,
          memory: reading(0).memory,
          heapSpaces: [],
          code: {},
        })
      )
    }
    await expect(analyseWorker(directory, { kind: 'admin' }, { maxBytes: 1 })).rejects.toThrow(
      /compressed byte limit/
    )
    expect(fs.readdirSync(directory).filter((file) => file.endsWith('.gz'))).toEqual([])
    expect(fs.existsSync(path.join(directory, 'baseline.heapsnapshot'))).toBe(true)
    expect(JSON.parse(fs.readFileSync(path.join(directory, 'summary.json')))).toMatchObject({
      ...flags,
      complete: false,
      error: 'closed snapshots exceed compressed byte limit',
    })
  })

  test('refuses the job outside explicit CI or with mixed experiments before loading the live driver', async () => {
    const { main } = require('../closedDiagnosticJob')
    const env = { CI: 'true', TELEMETRY_CLOSED_DIAGNOSTIC: '1', KAFKA_VERSION: '4.3.1' }
    for (const override of [
      { CI: undefined },
      { TELEMETRY_CLOSED_DIAGNOSTIC: undefined },
      { KAFKA_VERSION: '4.2.2' },
      { TELEMETRY_LONG_WINDOW: '1' },
      { TELEMETRY_SEED: 'slope2x' },
      { SHARE_PEAK_DIAGNOSTIC: '1' },
      { LIVE_CENSUS_IDENTITY: 'capture' },
    ])
      await expect(main(['--diagnostic-only', directory], { ...env, ...override })).rejects.toThrow(
        /explicit closed diagnostic CI/
      )
    await expect(main([directory], env)).rejects.toThrow(/explicit closed diagnostic CI/)
    await expect(main(['--diagnostic-only', 'relative'], env)).rejects.toThrow(
      /explicit closed diagnostic CI/
    )
    fs.writeFileSync(path.join(directory, 'stale'), '')
    await expect(main(['--diagnostic-only', directory], env)).rejects.toThrow(/must be fresh/)
  })

  test('runs only on explicit dispatch within 45 minutes and outside every verdict dependency', () => {
    const workflow = yaml.load(
      fs.readFileSync(path.join(__dirname, '../../../.github/workflows/memory-gates.yml'), 'utf8')
    )
    const job = workflow.jobs['telemetry-closed-diagnostic']
    expect(job).toBeDefined()
    expect(job['timeout-minutes']).toBeLessThanOrEqual(45)
    expect(
      job.steps.reduce((total, step) => total + step['timeout-minutes'], 0)
    ).toBeLessThanOrEqual(45)
    expect(job.if).toContain("github.event_name == 'workflow_dispatch'")
    expect(job.if).toContain("inputs.only == 'telemetry-closed-diagnostic'")
    expect(job.strategy.matrix).toEqual({ node: [22, 24, 26] })
    expect(job.env.KAFKA_VERSION).toBe('4.3.1')
    expect(job.env.TELEMETRY_CLOSED_DIAGNOSTIC).toBe('1')
    for (const [name, candidate] of Object.entries(workflow.jobs))
      if (name.includes('verdict'))
        expect(candidate.needs || []).not.toContain('telemetry-closed-diagnostic')
    const upload = job.steps.find((step) => step.uses?.startsWith('actions/upload-artifact'))
    expect(upload.with.path).toContain('summary.json')
    expect(upload.with.path).toContain('*.heapsnapshot.gz')
    expect(upload.with.path).not.toContain('**')
    for (const step of job.steps) expect(step['timeout-minutes']).toBeGreaterThan(0)
  })
})
