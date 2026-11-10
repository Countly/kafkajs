const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const vm = require('node:vm')
const v8 = require('node:v8')
const { EventEmitter } = require('node:events')
const { PassThrough } = require('node:stream')
const {
  CAPACITY,
  CAPTURES,
  METHOD,
  CATALOGUE,
  DiagnosticLog,
  validateEvents,
  validateCatalogue,
  digest,
} = require('../retained/contract')
const { supervise, replay, screen, main, verifySources } = require('../retainedDiagnostic')
const { WeakOwners } = require('../retained/weakOwners')
const { DiagnosticMemory, DiagnosticSampler } = require('../retained/observer')
const TelemetryPayload = require('../../../../src/telemetry/payload')
const { MEMORY_FLAGS } = require('../../../../testHelpers/processMemory')
const { foregroundCatalogue } = require('../resourceArtifact')
const { STAGES } = require('../retained/settlement')
const {
  MemoryForeground,
  descriptor,
} = require('../../../../src/telemetry/__tests__/fixtures/memoryForeground')
const { maximum } = require('../../../../src/telemetry/__tests__/fixtures/maximum')

const turn = () => new Promise((resolve) => setImmediate(resolve))
const dirs = []
const temp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retained-control-'))
  dirs.push(dir)
  return dir
}
afterEach(() => {
  jest.useRealTimers()
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})

/** Deterministic real-shaped GC journal; malformed mutants must fail independent replay. */
const reading = (at = 1000, retained = 1000000) => {
  const scalar = {
    at,
    rss: 10000000,
    heapUsed: retained - 1000,
    external: 1000,
    arrayBuffers: 100,
    retained,
  }
  return {
    ...scalar,
    gc: {
      observed: 4,
      admittedAt: at - 250,
      completedAt: at,
      checkpoints: [at - 150, at].map((at) => ({
        ...scalar,
        at,
        admittedAt: at - 100,
        observed: 2,
      })),
    },
  }
}
const identity = () => ({
  schema: 1,
  method: METHOD,
  diagnostic: true,
  acceptanceEligible: false,
  arm: 'public',
  kind: 'producer',
  variant: 'scalar',
  childId: 'one',
  tree: 'a'.repeat(40),
  node: 'v24.22.2',
  v8: 'v8',
  runId: 'run',
  flags: MEMORY_FLAGS,
  sources: [{ file: 'fixture.js', bytes: 1, sha256: digest('x') }],
  capacity: CAPACITY,
  gc: { available: true, native: true },
})
const exit = () => ({
  code: 0,
  signal: null,
  timedOut: false,
  failure: null,
  stderrBytes: 0,
  stdoutBytes: 100,
  receipts: 1,
  results: 1,
  closedAfterReceiptMs: 100,
  elapsedMs: 1000,
})
/** Full scalar evidence through the same bounded writer used by the child. */
const evidence = (dir, id = identity()) => {
  const log = new DiagnosticLog(path.join(dir, 'events.jsonl'))
  log.write('manifest', { manifest: id })
  for (const phase of [
    'startup',
    'loaded',
    'C0',
    'C1',
    'disabled-released',
    'M',
    'closed:0',
    'closed:1',
    'closed:2',
    'closed:3',
    'closed:4',
    'closed:5',
    'cleanup',
  ])
    log.write('mark', {
      mark: { phase, vmhwm: 10240000, covered: 10000000, coveredExternal: 1000 },
      raw: {
        status: 'VmHWM: 10000 kB\nVmRSS: 9000 kB\n',
        parsed: { VmHWM: 10240000, VmRSS: 9216000 },
        current: { at: 1 },
        before: 2,
        after: 3,
        pid: 123,
        processStart: '1',
        kernel: 'kernel',
        os: 'Linux',
        arch: 'x64',
        flags: MEMORY_FLAGS,
      },
    })
  let tick = 1000
  let measurementId = 0
  const measured = () => {
    tick += 1000
    const result = reading(tick)
    result.gc.diagnosticId = ++measurementId
    log.write('settlement', {
      id: measurementId,
      phase: 'fixture',
      readingAt: tick,
      checkpoints: result.gc.checkpoints,
      observations: result.gc.checkpoints.flatMap((point, round) =>
        STAGES.map((stage, index) => ({
          ...point,
          round,
          stage,
          at: point.at - 90 + index * 10,
          submittedId: 0,
          submittedJobId: 0,
          submittedCodec: 0,
          settledId: 0,
          settledJobId: 0,
          settledCodec: 0,
          pending: 0,
        }))
      ),
    })
    return result
  }
  const checkpoint = (name, pre = measured()) => {
    log.write('checkpoint', {
      name,
      observedAt: pre.at,
      startedAt: pre.at + 1,
      endedAt: pre.at + 2,
      pre,
      post: { at: pre.at + 3 },
      file: null,
      bytes: 0,
      sha256: null,
    })
    return pre
  }
  const [c0, c1, released] = CAPTURES.slice(0, 3).map((name) => checkpoint(name))
  log.write('control', {
    c0,
    c1,
    released,
    facts: {
      enabled: false,
      api71: 0,
      api72: 0,
      nativeSubmissions: 0,
      probe: 'disabled',
      ops: foregroundCatalogue(id.kind, 5).ops,
      liveSockets: 0,
      liveRequests: 0,
    },
  })
  checkpoint('M')
  for (let index = 0; index < 6; index++) {
    const actives = [measured(), measured()]
    const closed = measured()
    if (index === 0 || index === 5) checkpoint(`closed-${index}`, closed)
    const rows = ['gzip', 'zstd'].flatMap((codec, c) =>
      [false, true].map((retired, r) => ({
        id: c * 2 + r + 1,
        codec,
        retired,
        outcome: 'resolved',
        inputBytes: 2000,
        resultBytes: 100,
        inputBackingId: 1,
        resultBackingId: 2,
        inputBackingBytes: 2000,
        resultBackingBytes: 100,
        distinctBackingBytes: 2100,
        submitted: { at: 10 + c * 2 + r },
        settled: { at: 20 + c * 2 + r },
      }))
    )
    log.write('block', {
      index,
      closed,
      actives,
      delta: 0,
      matchedRetained: 1000000,
      ops: foregroundCatalogue(id.kind, 5).ops,
      uuid: 'uuid',
      registry: { uuid: 'uuid', descriptors: 128 },
      decoded: [{ format: true }, { format: true }],
      native: { rows, live: 0, maxLive: 1, overflow: false },
      samples: {
        windows: 4,
        overflow: false,
        pendingWindows: rows.map((row) => ({
          begin: row.submitted.at,
          end: row.settled.at,
          count: 2,
        })),
      },
    })
  }
  checkpoint('all-released')
  log.write('terminal', { cleanup: true })
  log.write('complete', {})
  return fs.readFileSync(log.file, 'utf8').trim().split('\n').map(JSON.parse)
}

test('declares exactly eight identities with paired serial capture/scalar children', () => {
  expect(CATALOGUE).toHaveLength(16)
  expect(new Set(CATALOGUE.map((row) => `${row.arm}/${row.kind}/${row.variant}`)).size).toBe(16)
  expect(CAPTURES).toHaveLength(7)
})

test('records and independently replays complete scalar output without an acceptance ok', async () => {
  const dir = temp()
  expect(validateEvents(evidence(dir), identity(), exit())).toEqual([])
  const replayed = await replay(dir, identity(), exit())
  expect(replayed.complete).toBe(true)
  expect(replayed.acceptanceEligible).toBe(false)
  expect(replayed).not.toHaveProperty('ok')
})

test.each([
  [
    'foreign kind',
    (rows) => {
      rows[0].manifest.kind = 'admin'
    },
  ],
  [
    'codec substitution',
    (rows) => {
      rows.find((row) => row.type === 'block').native.rows[0].codec = 'none'
    },
  ],
  [
    'wrong source digest',
    (rows) => {
      rows[0].manifest.sources[0].sha256 = 'bad'
    },
  ],
  [
    'missing source digest',
    (rows) => {
      delete rows[0].manifest.sources
    },
  ],
  [
    'missing capture',
    (rows) => {
      rows.splice(
        rows.findIndex((row) => row.type === 'checkpoint'),
        1
      )
    },
  ],
  [
    'reordered captures',
    (rows) => {
      ;[rows[14], rows[15]] = [rows[15], rows[14]]
    },
  ],
  [
    'duplicate capture',
    (rows) => {
      rows.splice(
        2,
        0,
        rows.find((row) => row.type === 'checkpoint')
      )
    },
  ],
  [
    'missing block',
    (rows) => {
      rows.splice(
        rows.findIndex((row) => row.type === 'block'),
        1
      )
    },
  ],
  [
    'duplicate block',
    (rows) => {
      rows.find((row) => row.type === 'block').index = 1
    },
  ],
  [
    'forged completion',
    (rows) => {
      rows.find((row) => row.type === 'terminal').cleanup = false
    },
  ],
  [
    'missing GC journal',
    (rows) => {
      delete rows.find((row) => row.type === 'checkpoint').pre.gc
    },
  ],
  [
    'mocked GC',
    (rows) => {
      rows[0].manifest.gc.native = false
    },
  ],
  [
    'bad arithmetic',
    (rows) => {
      rows.find((row) => row.type === 'checkpoint').pre.retained++
    },
  ],
  [
    'rebased M',
    (rows) => {
      rows.find((row) => row.type === 'block').matchedRetained++
    },
  ],
  [
    'capacity overflow',
    (rows) => {
      rows[0].manifest.capacity = { ...CAPACITY, events: Infinity }
    },
  ],
  [
    'missing native edge',
    (rows) => {
      rows.find((row) => row.type === 'block').samples.pendingWindows.pop()
    },
  ],
  [
    'primary and cleanup errors',
    (rows) => {
      rows.push({ type: 'error', stage: 'capture' }, { type: 'error', stage: 'cleanup' })
    },
  ],
])('rejects broken variant: %s', (_name, mutate) => {
  const rows = evidence(temp())
  mutate(rows)
  expect(validateEvents(rows, identity(), exit()).length).toBeGreaterThan(0)
})

test.each([
  { signal: 'SIGTERM' },
  { timedOut: true },
  { code: 1 },
  { receipts: 2 },
  { results: 0 },
  { closedAfterReceiptMs: 10001 },
  { elapsedMs: 90001 },
])('rejects broken natural-exit variant %p', (broken) => {
  expect(validateEvents(evidence(temp()), identity(), { ...exit(), ...broken })).toContain(
    'natural child exit/receipt'
  )
})

test('fails finite transport capacity and preserves earlier completed scalar facts', () => {
  const log = new DiagnosticLog(path.join(temp(), 'events.jsonl'))
  log.write('first', { ok: true })
  expect(() => log.write('overflow', { text: 'x'.repeat(CAPACITY.eventBytes) })).toThrow('overflow')
  expect(fs.readFileSync(log.file, 'utf8')).toContain('first')
  expect(log.count).toBe(1)
})

test('rejects circular serialization while leaving primary scalar evidence available', () => {
  const log = new DiagnosticLog(path.join(temp(), 'events.jsonl'))
  const circular = {}
  circular.self = circular
  expect(() => log.write('bad', circular)).toThrow('circular')
  log.write('error', { stage: 'serialization' })
  log.write('error', { stage: 'cleanup' })
  expect(fs.readFileSync(log.file, 'utf8').match(/"type":"error"/g)).toHaveLength(2)
})

test('does not let decreasing sources or missing coverage become passes after clamping', () => {
  const rows = evidence(temp())
  rows.push(
    { type: 'mark', mark: { vmhwm: 1000, covered: 900 } },
    { type: 'mark', mark: { vmhwm: 999, covered: 1100 } }
  )
  expect(screen(rows)).toMatchObject({
    problems: ['authoritative peak decreased'],
    inconclusive: ['peak source does not cover a current reading'],
  })
})

test('preserves original-M step sensitivity and earlier active violations despite unstable controls', () => {
  const rows = evidence(temp())
  const blocks = rows.filter((row) => row.type === 'block')
  blocks[0].delta = 262144
  blocks[1].delta = 262145
  blocks[0].actives[0].gc.checkpoints[0].retained = blocks[0].matchedRetained + 8388609
  const control = rows.find((row) => row.type === 'control')
  control.c1.retained += 262145
  expect(screen(rows)).toMatchObject({
    problems: [
      'block 0: active retained bytes exceed 8 MiB',
      'block 1: closed transient bytes exceed 256 KiB',
    ],
    inconclusive: ['control retained bytes are unstable'],
  })
  const cycle = Array.from({ length: 11 }, (_, index) => index * 10 * 4096)
  expect(cycle.findIndex((value) => value > 262144) * 10).toBe(70)
})

/** Fake children exercise actual supervision with real streams and incremental output. */
const fakeChild = () => {
  const child = new EventEmitter()
  child.pid = 123
  child.stdout = new PassThrough()
  child.stderr = new PassThrough()
  child.kill = jest.fn(() => {
    child.stdout.end()
    child.stderr.end()
    child.emit('close', null, 'SIGKILL')
  })
  return child
}

test.each(['duplicate', 'late', 'early', 'watchdog', 'signal', 'overflow'])(
  'supervises broken child output: %s',
  async (mode) => {
    jest.useFakeTimers()
    const child = fakeChild()
    const run = supervise(path.join(temp(), 'child'), identity(), () => child)
    const emit = (row) => child.stdout.write(JSON.stringify({ childId: 'one', ...row }) + '\n')
    if (mode === 'watchdog') jest.advanceTimersByTime(90000)
    else if (mode === 'signal') child.emit('close', null, 'SIGTERM')
    else if (mode === 'overflow') child.stdout.write('x'.repeat(65537))
    else if (mode === 'early') emit({ result: true, failed: false })
    else {
      emit({ terminal: true })
      if (mode === 'late') emit({ result: true, failed: false })
      emit({ terminal: true })
    }
    const observed = await run
    expect(observed.failure !== null || observed.timedOut || observed.signal !== null).toBe(true)
    expect(child.kill.mock.calls.length > 0 || observed.signal === 'SIGTERM').toBe(true)
  }
)

test('joins result and pipes only after the terminal receipt, preserving natural exit timing', async () => {
  const child = fakeChild()
  const run = supervise(path.join(temp(), 'child'), identity(), () => child)
  child.stdout.write(
    '{"terminal":true,"childId":"one"}\n{"result":true,"failed":false,"childId":"one"}\n'
  )
  child.stdout.end()
  child.stderr.end()
  child.emit('close', 0, null)
  expect(await run).toMatchObject({
    code: 0,
    signal: null,
    receipts: 1,
    results: 1,
    failure: null,
    timedOut: false,
  })
})

test('refuses live/acceptance use before launching a diagnostic', async () => {
  await expect(main(['--resources', temp()])).rejects.toThrow('diagnostic-only')
})

test('diagnostic pending-window capacity is distinct from the frozen acceptance capacity', () => {
  let at = 0
  const sampler = new DiagnosticSampler({
    current: () => ({ at: ++at, rss: 1, heapUsed: 1, external: 1, arrayBuffers: 1 }),
  })
  for (let index = 0; index < 33; index++) {
    sampler.begin()
    sampler.end()
  }
  expect(sampler.summary()).toMatchObject({ windows: 33, overflow: false })
  expect(sampler.summary().pendingWindows).toHaveLength(33)
})

/** Injects a one-time product-owner retention while persistent public populations stay alive. */
test.each(['producer', 'consumer', 'admin', 'shareConsumer', 'collector'])(
  'weak observer detects a flat first-use backing store held by %s and genuine release',
  async (kind) => {
    v8.setFlagsFromString('--expose-gc')
    const gc = vm.runInNewContext('gc')
    const weak = new WeakOwners()
    const persistent =
      kind === 'collector'
        ? maximum('producer')
        : new MemoryForeground(kind, { enabled: true }).build()
    if (kind !== 'collector')
      for (let index = 0; index < 128; index++) persistent.register(descriptor(index))
    const owner = kind === 'collector' ? persistent.metrics : new TelemetryPayload(1n)
    let replacement
    const issue = () => {
      const old = Buffer.alloc(1048576)
      const alias = old.subarray(100, 200)
      owner.retainedOnce = { old, alias }
      weak.issue(old, 'view')
      weak.issue(alias, 'alias')
      weak.issue(old.buffer, 'store')
      replacement = Buffer.alloc(65536)
      owner.replacement = replacement
      weak.issue(replacement.buffer, 'replacement')
    }
    issue()
    await turn()
    gc()
    await turn()
    gc()
    expect(weak.retirement(['store']).outcome).toBe('unreleased')
    expect(owner.retainedOnce.old.buffer).toBe(owner.retainedOnce.alias.buffer)
    expect(owner.replacement.buffer).not.toBe(owner.retainedOnce.old.buffer)
    expect(kind === 'collector' ? persistent.metrics : persistent.registered).toBeDefined()
    delete owner.retainedOnce
    delete owner.replacement
    replacement = null
    await turn()
    gc()
    await turn()
    gc()
    await turn()
    expect(weak.retirement(['view', 'alias', 'store', 'replacement']).outcome).toBe('released')
    expect(new WeakOwners().retirement(['store']).outcome).toBe('incomplete')
    if (kind !== 'collector') persistent.release()
  }
)

test.each(['capture', 'serialization', 'decoder'])(
  'preserves actual %s failure with cleanup failure and joins remaining cleanup',
  async (stage) => {
    const { joinedCleanup } = require('../retained/cleanup')
    const errors = []
    const joined = []
    const complete = await joinedCleanup(
      async () => {
        if (stage === 'decoder') require('node:zlib').gunzipSync(Buffer.from('invalid gzip'))
        if (stage === 'serialization') {
          const circular = {}
          circular.self = circular
          new DiagnosticLog(path.join(temp(), 'events.jsonl')).write('bad', circular)
        }
        if (stage === 'capture') {
          const memory = new DiagnosticMemory({ write: () => {} }, temp(), 'capture')
          const capture = jest.spyOn(v8, 'writeHeapSnapshot').mockImplementation(() => {
            throw new Error('capture failed')
          })
          try {
            await memory.checkpoint('C0', reading())
          } finally {
            capture.mockRestore()
            memory.stop()
          }
        }
      },
      [
        {
          name: 'failing',
          run: async () => {
            throw new Error('cleanup failed')
          },
        },
        {
          name: 'pipes',
          run: async () => {
            await turn()
            joined.push('pipes')
          },
        },
      ],
      (where, error) => errors.push({ where, message: error.message })
    )
    expect(complete).toBe(false)
    expect(errors).toEqual([
      { where: 'workload', message: expect.any(String) },
      { where: 'cleanup:failing', message: 'cleanup failed' },
    ])
    expect(joined).toEqual(['pipes'])
  }
)

test('does not turn a fixture assertion closure retaining a target into a released proof', async () => {
  const weak = new WeakOwners()
  const make = () => {
    const target = Buffer.alloc(1048576)
    weak.issue(target, 'store')
    return () => target.length
  }
  const accidentalClosure = make()
  await turn()
  expect(weak.retirement(['store']).outcome).toBe('unreleased')
  expect(accidentalClosure()).toBe(1048576)
  expect(weak.rows.slice(0, weak.count).every((row) => row.weak instanceof WeakRef)).toBe(true)
  expect(weak.ids).toBeInstanceOf(WeakMap)
  expect(weak.restores).toEqual([])
  expect(() => weak.issue(null, 'store')).toThrow('missing target')
})

test('rejects duplicate and foreign run identities independently of per-child completion', () => {
  const rows = CATALOGUE.map((tuple, index) => ({
    ...tuple,
    childId: String(index),
    complete: true,
  }))
  expect(validateCatalogue(rows)).toEqual([])
  rows[1].childId = rows[0].childId
  expect(validateCatalogue(rows)).toContain('duplicate or missing child identity')
  rows[1].childId = 'unique'
  rows[1].kind = 'other'
  expect(validateCatalogue(rows)).toContain('missing or foreign catalogue child')
})

test('verifies actual archived source bytes and rejects missing and wrong digests', () => {
  const dir = temp()
  fs.mkdirSync(path.join(dir, 'sources'))
  fs.writeFileSync(path.join(dir, 'sources', 'fixture.js'), 'x')
  const sources = [{ file: 'fixture.js', bytes: 1, sha256: digest('x') }]
  expect(() => verifySources(dir, sources)).not.toThrow()
  fs.writeFileSync(path.join(dir, 'sources', 'fixture.js'), 'y')
  expect(() => verifySources(dir, sources)).toThrow('wrong source digest')
  fs.unlinkSync(path.join(dir, 'sources', 'fixture.js'))
  expect(() => verifySources(dir, sources)).toThrow()
})

test('reserves weak scalar history before issuance and fails before exceeding its capacity', () => {
  const weak = new WeakOwners(2)
  const slots = [...weak.rows]
  expect(slots).toHaveLength(2)
  expect(weak.read()).toEqual([])
  const first = {}
  const second = {}
  expect(weak.issue(first, 'snapshot')).toBe(1)
  expect(weak.issue(first, 'snapshot')).toBe(1)
  expect(weak.issue(second, 'point')).toBe(2)
  expect(() => weak.issue({}, 'point')).toThrow('weak observation overflow')
  expect(weak.rows).toEqual(slots)
  expect(weak.rows.every((row, index) => row === slots[index])).toBe(true)
  expect(weak.read()).toEqual([
    { id: 1, kind: 'snapshot', alive: true },
    { id: 2, kind: 'point', alive: true },
  ])
})

test('archives dead weak observations as scalars while continuing to detect a held target', async () => {
  v8.setFlagsFromString('--expose-gc')
  const gc = vm.runInNewContext('gc')
  const weak = new WeakOwners(8)
  const held = {}
  weak.issue(held, 'held')
  for (let index = 0; index < 6; index++) {
    weak.issue({}, 'point')
    await turn()
    gc()
    await turn()
    gc()
    await turn()
    const observations = weak.read()
    expect(observations[0]).toEqual({ id: 1, kind: 'held', alive: true })
    expect(observations.slice(1).every((row) => !row.alive)).toBe(true)
    expect(weak.rows.slice(1, index + 2).every((row) => row.weak === null)).toBe(true)
  }
  expect(weak.retirement(['held', 'point']).outcome).toBe('unreleased')
  expect(held).toEqual({})
})
