const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { runEntry } = require('../report')
const { JOBS, digest, schedule, minimumBudget, validateManifest } = require('../telemetry')
const adapter = require('../adapters/telemetrySession')

const ROOT = path.join(__dirname, '../../..')
const SCRIPT = 'scripts/benchmarks/adapters/telemetrySession.js'
const TELEMETRY = path.join(ROOT, 'scripts/benchmarks/telemetry.js')
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-row-'))

const ENTRY = {
  name: 'telemetrySession-delivery-control-eachMessage',
  command: [SCRIPT, '--job', 'delivery-control', '--raw', '{raw}', '--handler', 'eachMessage'],
  broker: true,
  metrics: [
    { key: 'messagesPerSecond', unit: 'msg/s' },
    { key: 'p99Ms', unit: 'ms' },
    { key: 'deliveryGapP99Ms', unit: 'ms' },
  ],
  arms: ['fork'],
  adapter: 'telemetrySession',
}

/** A small source tree with every path the snapshot copies. */
const fakeRoot = () => {
  const root = tmp()
  const files = {
    'index.js': 'module.exports = {}\n',
    'package.json': '{"name":"@countly/kafkajs"}\n',
    'src/index.js': '// client\n',
    'testHelpers/telemetryFixtures.js': '// fixtures\n',
    'scripts/benchmarks/consumeThroughput.js': '// consume driver\n',
  }
  for (const [file, text] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
    fs.writeFileSync(path.join(root, file), text)
  }
  return root
}

/** The complete delivery fixture: telemetry cluster, reporter sink and prepared decoder. */
const deliveryFixture = () => {
  const cache = tmp()
  const archive = path.join(cache, 'decoder-4.4.0.jsa')
  fs.writeFileSync(archive, 'archive')
  return {
    COMPOSE_FILE: 'docker-compose.telemetry.yml',
    COMPOSE_PROJECT_NAME: 'kafkajs-telemetry',
    KAFKA_VERSION: '4.4.0',
    TELEMETRY_EVIDENCE_DIR: tmp(),
    TELEMETRY_JAVA_CACHE: cache,
    TELEMETRY_DECODER_ARCHIVE: archive,
    RUNNER_TEMP: tmp(),
  }
}

/** Throughput per observation: the reference and its copy near 1000, the slow control near 500. */
const resultFor = (label, round) => ({
  messagesPerSecond: (label === 'slow' ? 500 : 1000) + round,
  p99Ms: (label === 'slow' ? 30 : 10) + round / 10,
  deliveryGapP99Ms: 2,
  gapSamples: 200000,
  elapsedNs: 1e9,
})

/**
 * A scripted telemetry controller: `tar` writes an archive, `--session` validates the manifest the
 * way the real controller does and writes the session's raw rows and receipt, `--verify-session`
 * repeats the verdict.
 * @param {{ exit?: number, verdict?: object|null, stderr?: string }} [script]
 */
const controller = ({ exit = 0, verdict, stderr = '' } = {}) => {
  const calls = []
  const job = JOBS['delivery-control']
  const result =
    verdict === undefined
      ? {
          disposition: 'pass',
          failures: [{ arm: 'slow', fields: ['messagesPerSecond', 'p99Ms'], expected: true }],
          receipt: { schemaVersion: 1, count: 30, chain: 'c'.repeat(64) },
        }
      : verdict
  const spawn = (command, args) => {
    if (command === 'tar') {
      fs.writeFileSync(args[args.indexOf('-cf') + 1], 'archive bytes')
      return { status: 0, signal: null, stdout: '', stderr: '' }
    }
    calls.push(args)
    if (args.includes('--session')) {
      const manifest = JSON.parse(fs.readFileSync(args[args.indexOf('--session') + 1], 'utf8'))
      validateManifest(manifest)
      fs.mkdirSync(manifest.directory)
      const rows = schedule(job.roles, job.reference, job.runs).rows.map((row, i) => ({
        ...row,
        result: resultFor(row.label, row.round),
        runId: `${manifest.runId}-${String(i).padStart(4, '0')}`,
        disposition: 'valid',
        envelopeSha256: 'e'.repeat(64),
        derived: {},
      }))
      const raw = path.join(manifest.directory, 'raw.jsonl')
      fs.writeFileSync(raw, rows.map((row) => `${JSON.stringify(row)}\n`).join(''))
    }
    const stdout = result ? JSON.stringify(result) : ''
    return { status: exit, signal: null, stdout, stderr }
  }
  return { spawn, calls }
}

/** Runs the row through the report core, which spawns the adapter's command in-process. */
const runRow = (env, inner, root = fakeRoot()) => {
  const rawDir = tmp()
  const outer = (execPath, args) => {
    expect(args[0]).toBe(SCRIPT)
    const out = []
    const err = []
    const status = adapter.main(args.slice(1), {
      env,
      spawn: inner,
      log: (line) => out.push(line),
      error: (line) => err.push(line),
      root,
    })
    return { status, signal: null, stdout: out.join('\n'), stderr: err.join('\n') }
  }
  const result = runEntry(ENTRY, {
    runs: 3,
    forkDir: '/fork',
    baselineDir: '/base',
    spawn: outer,
    rawDir,
  })
  return { ...result, rawDir, raw: path.join(rawDir, ENTRY.name, 'fork-run-1.jsonl') }
}

describe('scripts/benchmarks/adapters/telemetrySession', () => {
  test('the telemetry row refuses to run without its fixture', () => {
    const { spawn, calls } = controller()
    const bare = runRow({}, spawn)
    expect(calls).toEqual([])
    expect(bare.rows).toHaveLength(1)
    expect(bare.rows[0]).toMatchObject({ benchmark: ENTRY.name, status: 'error' })
    for (const name of [
      'COMPOSE_FILE',
      'COMPOSE_PROJECT_NAME',
      'KAFKA_VERSION',
      'TELEMETRY_EVIDENCE_DIR',
      'TELEMETRY_JAVA_CACHE',
      'TELEMETRY_DECODER_ARCHIVE',
    ])
      expect(bare.rows[0].reason).toContain(name)
    expect(bare.rows[0].reason).toMatch(/^run 1 of 1: the telemetry fixture is missing: /)
    expect(fs.existsSync(`${bare.raw}.manifest.json`)).toBe(false)

    const env = deliveryFixture()
    fs.rmSync(env.TELEMETRY_DECODER_ARCHIVE)
    const unprepared = runRow(env, spawn)
    expect(calls).toEqual([])
    expect(unprepared.rows[0].status).toBe('error')
    expect(unprepared.rows[0].reason).toContain('TELEMETRY_DECODER_ARCHIVE')
    expect(unprepared.rows[0].reason).not.toContain('COMPOSE_FILE')

    expect(adapter.fixtureProblems('requests-normal', {})).toEqual([
      expect.stringContaining('TELEMETRY_PREVIOUS_ROOT'),
      expect.stringContaining('TELEMETRY_PREVIOUS_REF'),
      expect.stringContaining('TELEMETRY_PREVIOUS_COMMIT'),
    ])
    expect(adapter.fixtureProblems('requests-control', {})).toEqual([])
  })

  test('the telemetry row keeps raw controls and the session manifest', () => {
    const env = deliveryFixture()
    const { spawn, calls } = controller()
    const { rows, details, raw, rawDir } = runRow(env, spawn)

    const manifestFile = `${raw}.manifest.json`
    const bytes = fs.readFileSync(manifestFile)
    const manifest = JSON.parse(bytes)
    const job = JOBS['delivery-control']
    expect(manifest).toMatchObject({
      schemaVersion: 1,
      job: 'delivery-control',
      directory: `${raw}.session`,
      reference: 'normal',
      runs: 9,
      budgetSeconds: minimumBudget('delivery-control'),
      fixture: { brokers: ['localhost:19092', 'localhost:19095', 'localhost:19098'] },
      driver: { path: TELEMETRY, sha256: sha256(fs.readFileSync(TELEMETRY)) },
    })
    expect(manifest.arms.map(({ label, argv }) => [label, argv])).toEqual([
      [
        'normal',
        [
          '--mode',
          'delivery',
          '--control',
          '0',
          '--handler',
          'eachMessage',
          '--telemetry',
          'active',
        ],
      ],
      [
        'slow',
        [
          '--mode',
          'delivery',
          '--control',
          '1',
          '--handler',
          'eachMessage',
          '--telemetry',
          'active',
        ],
      ],
    ])
    for (const arm of manifest.arms)
      expect(arm.configSha256).toBe(digest({ mode: job.mode, argv: arm.argv }))
    expect(calls).toEqual([
      ['--expose-gc', TELEMETRY, '--session', manifestFile],
      [TELEMETRY, '--verify-session', manifest.directory],
    ])
    expect(fs.readdirSync(env.RUNNER_TEMP)).toEqual([])

    const evidence = JSON.parse(fs.readFileSync(path.join(rawDir, ENTRY.name, 'fork.json'), 'utf8'))
    const report = JSON.parse(evidence.runs[0].stdout.trim().split('\n').pop())
    expect(report.observations).toHaveLength(30)
    expect(report.manifest).toEqual({ path: manifestFile, sha256: sha256(bytes) })

    expect(details).toHaveLength(1)
    expect(details[0]).toMatchObject({ status: 'valid', raw: `bench-raw/${ENTRY.name}/` })
    expect(details[0].controls).toEqual([
      {
        arm: 'session',
        name: 'declared observations',
        passed: true,
        detail: '30 observations, receipt 30, declared 30',
      },
      { arm: 'session', name: 'every observation verified', passed: true },
      {
        arm: 'session',
        name: 'session re-verified',
        passed: true,
        detail: 'exit 0, pass, chain matches',
      },
      {
        arm: 'slow',
        name: 'positive control fires',
        passed: true,
        detail: 'messagesPerSecond, p99Ms',
      },
      { arm: 'normal~AA', name: 'A/A spread', passed: true, detail: 'pass' },
    ])
    expect(details[0].notes).toContain(`session manifest sha256 ${sha256(bytes)}`)

    expect(rows).toHaveLength(9)
    expect(rows.map(({ arm, metric }) => `${arm} ${metric}`)).toEqual(
      ['normal', 'slow', 'normal~AA'].flatMap((label) =>
        ['messagesPerSecond', 'p99Ms', 'deliveryGapP99Ms'].map((key) => `${label} ${key}`)
      )
    )
    expect(rows[0]).toMatchObject({ status: 'valid', median: 1005, min: 1001, max: 1009, runs: 9 })
    expect(rows[3]).toMatchObject({ status: 'valid', median: 505, min: 501, max: 509, runs: 9 })
  })

  test('the telemetry row reports a void session as inconclusive and an ineffective control as invalid', () => {
    const voided = controller({
      exit: 3,
      verdict: { disposition: 'void', failures: [], receipt: { count: 30, chain: 'v'.repeat(64) } },
    })
    const inconclusive = runRow(deliveryFixture(), voided.spawn)
    expect(inconclusive.details[0].status).toBe('inconclusive')
    expect(inconclusive.details[0].reason).toBe('the A/A control is void')
    expect(inconclusive.details[0].controls.at(-1)).toEqual({
      arm: 'normal~AA',
      name: 'A/A spread',
      passed: false,
      detail: 'void',
    })
    expect(inconclusive.rows[0]).toMatchObject({ status: 'inconclusive', median: 1005 })

    const ineffective = controller({
      exit: 1,
      verdict: null,
      stderr:
        'AssertionError [ERR_ASSERTION]: the slow positive control is ineffective\n    at evaluate',
    })
    const invalid = runRow(deliveryFixture(), ineffective.spawn)
    expect(ineffective.calls).toHaveLength(1)
    expect(invalid.rows).toEqual([
      expect.objectContaining({
        status: 'invalid',
        reason: 'failed controls: slow positive control fires',
      }),
    ])
    expect(invalid.details[0].controls).toEqual([
      {
        arm: 'slow',
        name: 'positive control fires',
        passed: false,
        detail: 'the slow positive control is ineffective',
      },
    ])
    expect(invalid.rows[0]).not.toHaveProperty('median')
  })
})
