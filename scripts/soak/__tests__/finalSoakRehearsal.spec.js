const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn, spawnSync } = require('node:child_process')
const {
  StreamValidator,
  validateCapabilities,
  validateConfig,
  validateHeader,
  validateProvenance,
} = require('../finalSoakSchema')
const { planSchedule } = require('../finalSoakScheduler')

const ROOT = path.join(__dirname, '../../..')
const PRELOAD = path.join(ROOT, 'testHelpers/finalSoakRehearsal.js')
const MATRIX_FILE = 'scripts/soak/finalSoakMatrix.json'
const matrix = JSON.parse(fs.readFileSync(path.join(ROOT, MATRIX_FILE), 'utf8'))
const TREE = 'b'.repeat(40)
const JAR = 'c'.repeat(64)
const BROKERS = 'kafka1:9092,kafka2:9092,kafka3:9092'
/** The clock speed-up: the 30 min smoke schedule runs in about 30 s. */
const WARP = 60
/** Sixty-fold sampling is late by the real cost of two collections, which no broker-free run avoids. */
const EXPECTED_PROBLEM = /^step \S+ late$/

/**
 * A rehearsal workspace: the artifact directory with the reporter receipt the runbook's preparation
 * step leaves, the reporter load proofs of three nodes, supplied broker build evidence, and PATH
 * stubs for `docker` (answers only the mounted jar digest), `git` (a clean checkout at TREE) and
 * `npm` (installed dependencies).
 */
const workspace = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-rehearsal-'))
  const out = path.join(dir, 'artifact')
  const sink = path.join(dir, 'sink')
  const bin = path.join(dir, 'bin')
  for (const d of [path.join(out, 'reporter'), path.join(out, 'broker'), sink, bin])
    fs.mkdirSync(d, { recursive: true })
  const sinkBytes = matrix.host.brokerReporterFileBytes.value
  fs.writeFileSync(
    path.join(out, 'reporter/receipt.json'),
    JSON.stringify({ sinkBytes, sha256: JAR })
  )
  // What the runbook writes for a supplied cluster in place of the prepared Compose override.
  fs.writeFileSync(path.join(out, 'broker/compose.json'), '{"services":{}}\n')
  const nodes = [1, 2, 3].map((node) => {
    fs.writeFileSync(
      path.join(sink, `reporter-${node}.json`),
      JSON.stringify({ maxFileBytes: sinkBytes, loadedAtMs: 1 })
    )
    const file = path.join(dir, `build-${node}.txt`)
    fs.writeFileSync(file, `kafka 4.4.0 node ${node}\n`)
    return { node, version: '4.4.0', file }
  })
  const evidence = path.join(dir, 'evidence.json')
  fs.writeFileSync(evidence, JSON.stringify({ nodes }))
  fs.writeFileSync(path.join(bin, 'running'), '')
  const stubs = {
    docker: [
      `case "$*" in *sha256sum*) echo "${JAR}  /opt/kafka/libs/counting-telemetry-reporter.jar"; exit 0 ;; esac`,
      'state="$(dirname "$0")/running"',
      'case "$1 $2" in',
      '  "compose ps") echo rehearsal-container; exit 0 ;;',
      '  "compose stop" | "compose kill") rm -f "$state"; exit 0 ;;',
      '  "compose start") touch "$state"; exit 0 ;;',
      'esac',
      'if [ "$1" = inspect ]; then if [ -f "$state" ]; then echo 4242; else echo 0; fi; exit 0; fi',
      'echo "rehearsal: no docker" >&2; exit 1',
    ],
    git: [
      `case "$*" in "rev-parse HEAD") echo ${TREE} ;; "status --porcelain") ;; *) exit 1 ;; esac`,
    ],
    npm: ['exit 0'],
  }
  for (const [name, body] of Object.entries(stubs))
    fs.writeFileSync(path.join(bin, name), ['#!/bin/sh', ...body, ''].join('\n'), { mode: 0o755 })
  const env = {
    PATH: `${bin}:${path.dirname(process.execPath)}:/usr/bin:/bin`,
    KAFKA_BROKERS: BROKERS,
    TELEMETRY_EVIDENCE_DIR: sink,
    FINAL_SOAK_SUPPLIED_EVIDENCE: evidence,
    NODE_OPTIONS: `--require ${PRELOAD}`,
  }
  return { dir, out, env, remove: () => fs.rmSync(dir, { recursive: true, force: true }) }
}

/** @returns {Promise<{ code: number|null, signal: string|null, output: string }>} */
const runDriver = (args, env) =>
  new Promise((resolve) => {
    const driver = spawn(process.execPath, args, { cwd: ROOT, env })
    let output = ''
    driver.stdout.on('data', (b) => (output += b))
    driver.stderr.on('data', (b) => (output += b))
    driver.on('close', (code, signal) => resolve({ code, signal, output }))
  })

/** @param {string} out @returns {object[]} every stream record */
const recordsOf = (out) =>
  fs
    .readFileSync(path.join(out, 'stream.ndjson'), 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line))

describe('final soak rehearsal', () => {
  test('preflight and the sixteen-process smoke run start, connect, run every schedule step, sample and exit naturally without a broker', async () => {
    const ws = workspace()
    try {
      const preflight = spawnSync(
        process.execPath,
        [
          'scripts/soak/finalSoakPreflight.js',
          '--expect-sha',
          TREE,
          '--mode',
          'smoke',
          '--out',
          ws.out,
          '--diagnostic',
        ],
        { cwd: ROOT, env: { ...ws.env, FINAL_SOAK_REHEARSAL_PUSH: '1' }, encoding: 'utf8' }
      )
      const record = JSON.parse(fs.readFileSync(path.join(ws.out, 'preflight.json'), 'utf8'))
      expect(record.error).toBeUndefined()
      expect(preflight.status).toBe(0)
      const blocking = record.checks.filter((c) => c.required && !c.ok).map((c) => c.name)
      expect(blocking).toEqual([])
      expect(record.checks.filter((c) => !c.required).map((c) => c.name)).toEqual([
        'quiet-host',
        'memory',
        'disk',
        'load',
      ])
      const read = (file) => JSON.parse(fs.readFileSync(path.join(ws.out, file), 'utf8'))
      const capabilities = read('broker/capabilities.json')
      expect(validateCapabilities(capabilities)).toEqual([])
      expect(capabilities.nodes.map((n) => n.node)).toEqual([1, 2, 3])
      expect(validateProvenance(read('broker/provenance.json'), 'supplied')).toEqual([])
      expect(record.telemetry.pushes.map((p) => p.probe).sort()).toEqual(['consumer', 'producer'])

      // The outer observer's finalize witnesses the exit on the same (warped) run clock.
      const warped = {
        ...ws.env,
        FINAL_SOAK_REHEARSAL_WARP: String(WARP),
        FINAL_SOAK_REHEARSAL_ANCHOR: String(process.hrtime.bigint()),
      }
      const driver = await runDriver(
        [
          '--expose-gc',
          '--no-flush-bytecode',
          'scripts/soak/finalSoak.js',
          '--mode',
          'smoke',
          '--expect-sha',
          TREE,
          '--matrix',
          MATRIX_FILE,
          '--out',
          ws.out,
          '--diagnostic',
        ],
        warped
      )
      expect(driver.output).not.toMatch(/Error/)
      expect(driver).toMatchObject({ code: 0, signal: null })

      const config = read('config.json')
      expect(validateConfig(config, matrix)).toEqual([])
      const supervisor = read('supervisor.json')
      expect(supervisor).toMatchObject({
        aborted: null,
        incomplete: null,
        failures: [],
        forced: [],
      })
      expect(supervisor.problems.filter((p) => !EXPECTED_PROBLEM.test(p))).toEqual([])
      const records = recordsOf(ws.out)
      const slots = config.processes.map((p) => p.slot)
      expect(slots).toHaveLength(16)
      const of = (kind, test = () => true) => records.filter((r) => r.kind === kind && test(r))
      expect(new Set(of('process', (r) => r.event === 'start').map((r) => r.process))).toEqual(
        new Set(slots)
      )
      const exits = of('process', (r) => r.event === 'exit')
      expect(exits.map((r) => r.process).sort()).toEqual(slots.filter((s) => s !== 'p15').sort())
      expect(exits.every((r) => r.code === 0 && r.signal === null)).toBe(true)
      for (const slot of slots) {
        const roles = new Set(of('sample', (r) => r.process === slot).map((r) => r.role))
        expect([slot, ...['baseline', 'periodic', 'final'].filter((r) => roles.has(r))]).toEqual([
          slot,
          'baseline',
          'periodic',
          'final',
        ])
      }
      for (const slot of config.clients.filter((c) => c.standing)) {
        const client = `${slot.slot}.1`
        expect([
          client,
          of('client', (r) => r.event === 'construct' && r.client === client),
        ]).toEqual([
          client,
          [expect.objectContaining({ process: slot.process, clientKind: slot.clientKind })],
        ])
        expect([
          client,
          of(
            'client',
            (r) => r.event === 'connect' && r.client === client && r.phase === 'end'
          ).map((r) => r.result),
        ]).toEqual([client, ['ok']])
      }
      const notes = of('diagnostic').map((r) => r.message)
      const plan = planSchedule(matrix, 'smoke').steps
      for (const step of plan)
        expect([step.id, notes.some((n) => new RegExp(`^step ${step.id} \\w+$`).test(n))]).toEqual([
          step.id,
          true,
        ])
      expect(notes.filter((n) => /^step \S+ (failed|error)/.test(n))).toEqual([])
      const scans = of('scan')
      expect(scans.length).toBeGreaterThan(0)
      for (const scan of scans)
        expect([scan.scan, fs.existsSync(path.join(ws.out, scan.rawFile))]).toEqual([
          scan.scan,
          true,
        ])
      expect(notes.filter((n) => /^producer loop failed/.test(n))).toEqual([])
      const shareGroup = config.clients.find((c) => c.clientKind === 'share').group
      const configured = records.findIndex(
        (r) =>
          r.kind === 'diagnostic' &&
          r.message === `share group ${shareGroup} reads back share.auto.offset.reset=earliest`
      )
      const shareStart = records.findIndex(
        (r) => r.kind === 'client' && r.event === 'construct' && r.process === 'p07'
      )
      expect(configured).toBeGreaterThan(-1)
      expect(configured).toBeLessThan(shareStart)

      const finalize = spawnSync(
        process.execPath,
        [
          'scripts/soak/finalSoak.js',
          '--finalize',
          '--out',
          ws.out,
          '--status',
          String(driver.code),
        ],
        { cwd: ROOT, env: warped, encoding: 'utf8' }
      )
      expect(finalize.stderr).toBe('')
      expect(finalize.status).toBe(0)
      const header = read('header.json')
      expect(header.exit).toMatchObject({ supervisorProcess: 'p15', code: 0, signal: null })
      expect(validateHeader(header, matrix)).toEqual([])
      const validator = new StreamValidator({ matrix, config, header })
      for (const line of fs.readFileSync(path.join(ws.out, 'stream.ndjson'), 'utf8').split('\n'))
        if (line) validator.push(line)
      expect(validator.finish().slice(0, 30)).toEqual([])
      const summary = spawnSync(
        process.execPath,
        ['scripts/soak/finalSoak.js', '--summary', '--out', ws.out],
        { cwd: ROOT, env: ws.env, encoding: 'utf8' }
      )
      expect(summary.status).toBe(0)
      expect(JSON.parse(summary.stdout).exit).toMatchObject({ code: 0 })
    } finally {
      ws.remove()
    }
  }, 240000)

  test('the warp speeds the harness clock only: a raw reader connection outlives a 200 ms CPU stall in its setup', () => {
    // A stall the host can impose at any moment (a collection, a busy CPU) during the reader's
    // ApiVersions exchange; its 3000 ms setup bound must stay 3000 ms of real time.
    const script = `
      const { MonotonicClock } = require('./scripts/soak/finalSoakScheduler')
      const { ReaderWorker } = require('./scripts/soak/finalSoakWorkers')
      const stall = (ms) => { const end = performance.now() + ms; while (performance.now() < end); }
      ;(async () => {
        const before = MonotonicClock.raw()
        stall(20)
        const warped = MonotonicClock.raw() - before
        let stalled = false
        const broker = await ReaderWorker.prototype.openBroker({ host: 'kafka1', port: 9092 }, (e) => {
          if (e.apiKey === 18 && !stalled) { stalled = true; stall(200) }
        })
        await broker.disconnect()
        process.stdout.write(JSON.stringify({ fast: warped >= 20 * ${WARP} * 0.9, stalled }))
      })().catch((e) => { process.stdout.write(JSON.stringify({ error: e.message })) })
    `
    const run = spawnSync(process.execPath, ['-e', script], {
      cwd: ROOT,
      env: {
        PATH: process.env.PATH,
        KAFKA_BROKERS: BROKERS,
        NODE_OPTIONS: `--require ${PRELOAD}`,
        FINAL_SOAK_REHEARSAL_WARP: String(WARP),
        FINAL_SOAK_REHEARSAL_ANCHOR: String(process.hrtime.bigint()),
      },
      encoding: 'utf8',
    })
    expect([run.stderr, JSON.parse(run.stdout)]).toEqual(['', { fast: true, stalled: true }])
  }, 30000)
})

describe('final soak rehearsal heap snapshots', () => {
  const run = {}
  /**
   * The reviewed matrix with the smoke quiet readings moved from the idle window's last minute to
   * its middle, with the deadline at the window's end. A sixty-fold run pays the real cost of each
   * reading's collections sixty times over, about twenty harness seconds, so on a loaded host the
   * reviewed triplet's last reading would start past its fifty-second deadline or the window's end
   * and be skipped. These tests are about the snapshots; the supervisor's unit tests cover a
   * boundary whose last reading is skipped.
   */
  const snapshotMatrix = (() => {
    const { schedule } = matrix
    const idle = schedule.smoke.idleMs
    const first = (idle.from + idle.to) / 2
    const quietAtMs = schedule.smoke.quietAtMs.map((q) => q - schedule.smoke.quietAtMs[0] + first)
    const quietDeadlineMs = schedule.quietAtMs[0] + (idle.to - first)
    return {
      ...matrix,
      schedule: { ...schedule, quietDeadlineMs, smoke: { ...schedule.smoke, quietAtMs } },
    }
  })()

  beforeAll(async () => {
    run.ws = workspace()
    run.matrixFile = path.join(run.ws.dir, 'matrix.json')
    fs.writeFileSync(run.matrixFile, JSON.stringify(snapshotMatrix))
    // The decoder's snapshot path is a directory, so its snapshot fails and the others do not.
    fs.mkdirSync(path.join(run.ws.out, 'heap/p16.h00.heapsnapshot'), { recursive: true })
    const env = {
      ...run.ws.env,
      FINAL_SOAK_REHEARSAL_WARP: String(WARP),
      FINAL_SOAK_REHEARSAL_ANCHOR: String(process.hrtime.bigint()),
    }
    run.driver = await runDriver(
      [
        '--expose-gc',
        '--no-flush-bytecode',
        'scripts/soak/finalSoak.js',
        '--mode',
        'smoke',
        '--expect-sha',
        TREE,
        '--matrix',
        run.matrixFile,
        '--out',
        run.ws.out,
        '--diagnostic',
      ],
      env
    )
    run.finalize = spawnSync(
      process.execPath,
      ['scripts/soak/finalSoak.js', '--finalize', '--out', run.ws.out, '--status', '0'],
      { cwd: ROOT, env, encoding: 'utf8' }
    )
    const read = (file) => JSON.parse(fs.readFileSync(path.join(run.ws.out, file), 'utf8'))
    run.config = read('config.json')
    run.supervisor = read('supervisor.json')
    run.header = read('header.json')
    run.records = recordsOf(run.ws.out)
  }, 240000)

  afterAll(() => run.ws?.remove())

  test('each selected process writes its heap snapshot into the run directory only after every quiet reading of the boundary, and the sealed header binds the file', () => {
    expect(run.driver).toMatchObject({ code: 0, signal: null })
    expect(run.finalize.status).toBe(0)
    const { records, config, supervisor, header } = run
    const quiet = records.filter((r) => r.kind === 'sample' && r.role === 'quiet')
    expect(new Set(quiet.map((r) => r.reading))).toEqual(new Set([0, 1, 2]))
    const lastReading = Math.max(...quiet.map((r) => r.completedAt))
    const lastQuietLine = records.lastIndexOf(quiet.at(-1))
    expect(supervisor.heapSnapshots.map((s) => `${s.slot} ${s.boundary}`).sort()).toEqual(
      config.heapSnapshots.map((slot) => `${slot} h00`).sort()
    )
    for (const receipt of supervisor.heapSnapshots)
      expect([receipt.slot, receipt.at > lastReading]).toEqual([receipt.slot, true])
    const notes = records
      .map((r, line) => ({ r, line }))
      .filter(({ r }) => r.kind === 'diagnostic' && r.message.startsWith('heap snapshot '))
    expect(notes.map(({ r }) => r.snapshot)).toEqual(supervisor.heapSnapshots)
    for (const { r, line } of notes)
      expect([r.message, line > lastQuietLine]).toEqual([r.message, true])
    const written = supervisor.heapSnapshots.filter((s) => s.error === null)
    expect(written.map((s) => s.slot).sort()).toEqual(
      config.heapSnapshots.filter((s) => s !== 'p16').sort()
    )
    const bound = new Map(header.files.map((f) => [f.file, f.bytes]))
    for (const { file, bytes } of written) {
      expect(file).toMatch(/^heap\/p\d\d\.h00\.heapsnapshot$/)
      expect([file, fs.statSync(path.join(run.ws.out, file)).size, bound.get(file)]).toEqual([
        file,
        bytes,
        bytes,
      ])
      expect(bytes).toBeGreaterThan(1024 * 1024)
    }
  })

  test('without any opt-in every subject, the reader and the decoder write a snapshot, and finalize binds their summary, one record per receipt', () => {
    const { config, supervisor, header } = run
    expect(config.heapSnapshots).toEqual(
      config.processes
        .filter((p) => ['subject', 'reader', 'decoder'].includes(p.role))
        .map((p) => p.slot)
    )
    expect(header.files.map((f) => f.file)).toContain('heap/summary.json')
    const summary = JSON.parse(fs.readFileSync(path.join(run.ws.out, 'heap/summary.json'), 'utf8'))
    expect(summary.records.map((r) => `${r.slot} ${r.boundary} ${r.at}`).sort()).toEqual(
      supervisor.heapSnapshots.map((r) => `${r.slot} ${r.boundary} ${r.at}`).sort()
    )
    for (const r of summary.records.filter((x) => x.slot !== 'p16'))
      expect([r.slot, r.kept, r.judgedBytes > 1024 * 1024, r.excludedBytes > 0]).toEqual([
        r.slot,
        true,
        true,
        true,
      ])
    expect(summary.records.find((r) => r.slot === 'p16')).toMatchObject({ kept: false })
  })

  test('a heap snapshot that fails is recorded with its error and the run still ends naturally with no problem of its own', () => {
    const failed = run.supervisor.heapSnapshots.find((s) => s.slot === 'p16')
    expect(failed).toMatchObject({ file: 'heap/p16.h00.heapsnapshot', bytes: null })
    expect(failed.error).toMatch(/EISDIR/)
    expect(run.supervisor).toMatchObject({ aborted: null, incomplete: null, failures: [] })
    expect(run.supervisor.problems.filter((p) => !EXPECTED_PROBLEM.test(p))).toEqual([])
    // The finalize step appends the supervisor's exit as the outer observer saw it.
    const exits = run.records.filter((r) => r.kind === 'process' && r.event === 'exit')
    expect(exits.map((r) => `${r.process}:${r.code}`).sort()).toEqual(
      run.config.processes.map((p) => `${p.slot}:0`).sort()
    )
  })

  test('every sample is followed by its own heap record of V8 space and code statistics, and the stream stays valid', () => {
    const next = new Map()
    const unmatched = []
    for (const r of run.records) {
      const sample = next.get(r.source)
      if (sample && !(r.kind === 'heap' && r.sample === sample)) unmatched.push(sample)
      next.set(r.source, r.kind === 'sample' ? r.recordId : null)
    }
    expect(unmatched).toEqual([])
    const heaps = run.records.filter((r) => r.kind === 'heap')
    expect(heaps).toHaveLength(run.records.filter((r) => r.kind === 'sample').length)
    for (const h of heaps)
      expect([h.sample, h.spaces.old > 0, h.code.bytecode > 0]).toEqual([h.sample, true, true])
    const validator = new StreamValidator({
      matrix: snapshotMatrix,
      config: run.config,
      header: run.header,
    })
    for (const line of fs.readFileSync(path.join(run.ws.out, 'stream.ndjson'), 'utf8').split('\n'))
      if (line) validator.push(line)
    expect(validator.finish().slice(0, 30)).toEqual([])
  })
})
