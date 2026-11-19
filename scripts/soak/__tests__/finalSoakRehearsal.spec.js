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
/** The rehearsal's only failing steps: the raw reader has no broker to open a connection to. */
const UNREACHABLE = /^step scan\.\S+ failed: Connection timeout$/

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
      expect(
        notes.filter((n) => /^step \S+ (failed|error)/.test(n) && !UNREACHABLE.test(n))
      ).toEqual([])
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
})
