const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')
const { runEntry } = require('../report')
const {
  JOBS,
  childCount,
  digest,
  schedule,
  minimumBudget,
  parseWorkerArgs,
  validateManifest,
  PREPARE_SECONDS,
} = require('../telemetry')
const adapter = require('../adapters/telemetrySession')

const ROOT = path.join(__dirname, '../../..')
const SCRIPT = 'scripts/benchmarks/adapters/telemetrySession.js'
const TELEMETRY = path.join(ROOT, 'scripts/benchmarks/telemetry.js')
const REF = '3.0.0-alpha.9e'
const MILESTONE = adapter.ACCEPTED_BASELINE.milestone
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-row-'))
const PLENTY = () => ({ bavail: 2 ** 40, bsize: 1 })

/** @param {string} name a telemetry row @returns {object} its manifest entry */
const entryOf = (name) => adapter.manifestEntries().find((entry) => entry.name === name)

/** A small source tree with every path the snapshot copies. */
const fakeRoot = (client = '// client\n') => {
  const root = tmp()
  const files = {
    'index.js': 'module.exports = {}\n',
    'package.json': '{"name":"@countly/kafkajs"}\n',
    'src/index.js': client,
    'testHelpers/telemetryFixtures.js': '// fixtures\n',
    'scripts/benchmarks/consumeThroughput.js': '// consume driver\n',
  }
  for (const [file, text] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
    fs.writeFileSync(path.join(root, file), text)
  }
  return root
}

/** @param {string} root @param {...string} args @returns {string} trimmed stdout */
const git = (root, ...args) => {
  const result = spawnSync(
    'git',
    [
      '-C',
      root,
      '-c',
      'user.email=bench@example.invalid',
      '-c',
      'user.name=bench',
      '-c',
      'commit.gpgsign=false',
      '-c',
      'tag.gpgsign=false',
      ...args,
    ],
    { encoding: 'utf8' }
  )
  if (result.status !== 0) throw new Error(`git ${args.join(' ')}: ${result.stderr}`)
  return result.stdout.trim()
}

/** A committed and tagged previous checkout; its client differs from the candidate's. */
const gitBaseline = () => {
  const root = fakeRoot('// previous client\n')
  git(root, 'init', '-q')
  git(root, 'add', '.')
  git(root, 'commit', '-qm', 'baseline')
  git(root, 'tag', REF)
  const commit = git(root, 'rev-parse', 'HEAD')
  return { root, commit, accepted: { milestone: MILESTONE, ref: REF, commit } }
}

/** The complete delivery fixture: telemetry cluster, a fresh reporter sink and a prepared decoder. */
const deliveryFixture = (row) => {
  const cache = tmp()
  const archive = path.join(cache, 'decoder-4.4.0.jsa')
  fs.writeFileSync(archive, 'archive')
  fs.writeFileSync(
    `${archive}.json`,
    JSON.stringify({ verified: true, line: '4.4.0', archive, archiveSha256: sha256('archive') })
  )
  const sink = tmp()
  fs.writeFileSync(path.join(sink, 'reporter-1.json'), '{"loaded":true}\n')
  return {
    BENCH_TELEMETRY_ROW: row,
    COMPOSE_FILE: 'docker-compose.telemetry.yml',
    COMPOSE_PROJECT_NAME: 'kafkajs-telemetry',
    KAFKA_VERSION: '4.4.0',
    TELEMETRY_EVIDENCE_DIR: sink,
    TELEMETRY_JAVA_CACHE: cache,
    TELEMETRY_DECODER_ARCHIVE: archive,
  }
}

/** Numbers per observation: the reference and its copy near 1000, a control arm near 500. */
const resultFor = (label, round) => {
  const control = label === 'slow' || label === 'busy'
  return {
    messagesPerSecond: (control ? 500 : 1000) + round,
    p99Ms: (control ? 30 : 10) + round / 10,
    deliveryGapP99Ms: 2,
    gapSamples: 200000,
    elapsedNs: 1e9,
    cpuNsPerPair: (control ? 2000 : 1000) + round,
    elapsedNsPerPair: 1500 + round,
    heapExternalDeltaBytes: 4096,
    sliceP99Ms: (control ? 120 : 1) + round,
    cpuP99Ms: 1 + round,
    wallP99Ms: (control ? 120 : 1) + round,
    fittingPasses: 1,
    rawBytes: 1000,
    transmittedBytes: 500,
  }
}

/** @param {object} job a telemetry.js job @param {string} arm @returns {string[]} the fields its control moves */
const controlFields = (job, arm) =>
  (job.compare ?? job.absolute ?? []).find((check) => check.arm === arm)?.expect ?? []

/**
 * A scripted telemetry controller. `tar` and `git` run for real; `--session` validates the manifest
 * the way the real controller does and writes the session's raw rows, `--verify-session` repeats
 * the verdict.
 * @param {{ exit?: number, verdict?: object|null, stderr?: string, stop?: number,
 *   during?: (manifest: object) => void }} [script] `stop` ends the session as killed after that
 *   many observations; `during` runs inside the session
 */
const controller = ({ exit = 0, verdict, stderr = '', stop, during } = {}) => {
  const calls = []
  const options = []
  let manifest
  const spawn = (command, args, opts) => {
    if (command === 'tar' || command === 'git') return spawnSync(command, args, opts)
    calls.push(args)
    options.push(opts)
    if (args.includes('--session')) {
      manifest = JSON.parse(fs.readFileSync(args[args.indexOf('--session') + 1], 'utf8'))
      validateManifest(manifest)
      fs.mkdirSync(manifest.directory)
      const job = JOBS[manifest.job]
      const all = schedule(job.roles, job.reference, job.runs).rows.map((row, i) => ({
        ...row,
        result: resultFor(row.label, row.round),
        runId: `${manifest.runId}-${String(i).padStart(4, '0')}`,
        disposition: 'valid',
        envelopeSha256: 'e'.repeat(64),
        derived: {},
      }))
      const rows = stop === undefined ? all : all.slice(0, stop)
      const raw = path.join(manifest.directory, 'raw.jsonl')
      fs.writeFileSync(raw, rows.map((row) => `${JSON.stringify(row)}\n`).join(''))
      during?.(manifest)
      if (stop !== undefined) {
        const error = Object.assign(new Error('spawnSync node ETIMEDOUT'), { code: 'ETIMEDOUT' })
        return { status: null, signal: 'SIGTERM', error, stdout: '', stderr: '' }
      }
    }
    const job = JOBS[manifest.job]
    const result =
      verdict === undefined
        ? {
            disposition: 'pass',
            failures: job.controls.map((arm) => ({
              arm,
              fields: controlFields(job, arm),
              expected: true,
            })),
            receipt: { schemaVersion: 1, count: childCount(manifest.job), chain: 'c'.repeat(64) },
          }
        : verdict
    const stdout = result ? JSON.stringify(result) : ''
    return { status: exit, signal: null, stdout, stderr }
  }
  return { spawn, calls, options }
}

/** Runs the row through the report core, which spawns the adapter's command in-process. */
const runRow = (name, env, inner, deps = {}) => {
  const entry = entryOf(name)
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
      root: fakeRoot(),
      statfs: PLENTY,
      ...deps,
    })
    return { status, signal: null, stdout: out.join('\n'), stderr: err.join('\n') }
  }
  const result = runEntry(entry, {
    runs: 3,
    forkDir: '/fork',
    baselineDir: '/base',
    spawn: outer,
    rawDir,
  })
  return { ...result, rawDir, raw: path.join(rawDir, name, 'fork-run-1.jsonl') }
}

/** @param {string} dir @returns {string[]} every symbolic link below `dir` */
const linksBelow = (dir) =>
  fs
    .readdirSync(dir, { recursive: true })
    .filter((name) => fs.lstatSync(path.join(dir, name)).isSymbolicLink())

describe('Benchmarks > telemetrySession adapter', () => {
  const CONTROL = 'telemetrySession-delivery-control-eachMessage'

  test('the telemetry row refuses to run without its fixture', () => {
    const { spawn, calls } = controller()
    const bare = runRow(CONTROL, { BENCH_TELEMETRY_ROW: CONTROL }, spawn)
    expect(calls).toEqual([])
    expect(bare.rows).toHaveLength(1)
    expect(bare.rows[0]).toMatchObject({ benchmark: CONTROL, status: 'error' })
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

    const env = deliveryFixture(CONTROL)
    fs.rmSync(env.TELEMETRY_DECODER_ARCHIVE)
    const unprepared = runRow(CONTROL, env, spawn)
    expect(calls).toEqual([])
    expect(unprepared.rows[0].status).toBe('error')
    expect(unprepared.rows[0].reason).toContain('TELEMETRY_DECODER_ARCHIVE')
    expect(unprepared.rows[0].reason).not.toContain('COMPOSE_FILE')

    const unreceipted = deliveryFixture(CONTROL)
    fs.rmSync(`${unreceipted.TELEMETRY_DECODER_ARCHIVE}.json`)
    expect(runRow(CONTROL, unreceipted, spawn).rows[0].reason).toContain(
      'TELEMETRY_DECODER_ARCHIVE'
    )
    expect(calls).toEqual([])

    const deps = { raw: path.join(tmp(), 'run.jsonl'), statfs: PLENTY }
    const normal = 'telemetrySession-requests-normal'
    expect(adapter.fixtureProblems(normal, { BENCH_TELEMETRY_ROW: normal }, deps)).toEqual([
      expect.stringContaining('TELEMETRY_PREVIOUS_ROOT'),
    ])
    const control = 'telemetrySession-requests-control'
    expect(adapter.fixtureProblems(control, { BENCH_TELEMETRY_ROW: control }, deps)).toEqual([])
  })

  test('the telemetry row keeps raw controls and the session manifest', () => {
    const env = deliveryFixture(CONTROL)
    const { spawn, calls } = controller()
    const { rows, details, raw, rawDir } = runRow(CONTROL, env, spawn)

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
    expect(manifest.arms.map(({ label, argv }) => [label, argv])).toEqual(
      ['normal', 'slow'].map((label) => [
        label,
        [
          '--mode',
          'delivery',
          '--control',
          label === 'slow' ? '1' : '0',
          '--handler',
          'eachMessage',
          '--telemetry',
          'active',
        ],
      ])
    )
    for (const arm of manifest.arms)
      expect(arm.configSha256).toBe(digest({ mode: job.mode, argv: arm.argv }))
    expect(calls).toEqual([
      ['--expose-gc', TELEMETRY, '--session', manifestFile],
      [TELEMETRY, '--verify-session', manifest.directory],
    ])

    const evidence = JSON.parse(fs.readFileSync(path.join(rawDir, CONTROL, 'fork.json'), 'utf8'))
    const report = JSON.parse(evidence.runs[0].stdout.trim().split('\n').pop())
    expect(report.observations).toHaveLength(30)
    expect(report.manifest).toEqual({ path: manifestFile, sha256: sha256(bytes) })
    expect(fs.readFileSync(`${raw}.decoder.json`, 'utf8')).toBe(
      fs.readFileSync(`${env.TELEMETRY_DECODER_ARCHIVE}.json`, 'utf8')
    )

    expect(details).toHaveLength(1)
    expect(details[0]).toMatchObject({ status: 'valid', raw: `bench-raw/${CONTROL}/` })
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
        arm: 'session',
        name: 'reporter sink without overflow',
        passed: true,
        detail: '1 sink files',
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
    expect(details[0].notes).toContain(
      `decoder archive sha256 ${sha256('archive')} prepared; the session does not read it`
    )

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
    const inconclusive = runRow(CONTROL, deliveryFixture(CONTROL), voided.spawn)
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
    const invalid = runRow(CONTROL, deliveryFixture(CONTROL), ineffective.spawn)
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

  test('the telemetry rows cover every owed configuration and both delivery controls', () => {
    const payload = []
    for (const points of ['1', '257'])
      for (const temporality of ['delta', 'cumulative'])
        for (const codec of ['none', 'gzip', 'zstd'])
          for (const attributes of ['compressible', 'poor'])
            payload.push({ points, temporality, codec, attributes })
    const expected = [
      { job: 'requests-normal', flags: [] },
      { job: 'requests-control', flags: [] },
      ...payload.map(({ points, temporality, codec, attributes }) => ({
        job: 'payload',
        flags: [
          '--points',
          points,
          '--temporality',
          temporality,
          '--codec',
          codec,
          '--attributes',
          attributes,
        ],
      })),
      ...['delivery-normal', 'delivery-control'].flatMap((job) =>
        ['eachMessage', 'eachBatch'].map((handler) => ({ job, flags: ['--handler', handler] }))
      ),
    ]
    expect(adapter.ROWS.map(({ job, flags }) => ({ job, flags }))).toEqual(expected)
    expect(adapter.ROWS).toHaveLength(30)
    expect(new Set(adapter.ROWS.map(({ name }) => name)).size).toBe(30)
    expect(
      adapter.ROWS.filter(({ job }) => JOBS[job].controls.length).map(({ name }) => name)
    ).toEqual([
      'telemetrySession-requests-control',
      ...payload.map(
        ({ points, temporality, codec, attributes }) =>
          `telemetrySession-payload-${points}-${temporality}-${codec}-${attributes}`
      ),
      'telemetrySession-delivery-control-eachMessage',
      'telemetrySession-delivery-control-eachBatch',
    ])

    const metrics = {
      requests: ['cpuNsPerPair', 'elapsedNsPerPair', 'heapExternalDeltaBytes'],
      payload: [
        'sliceP99Ms',
        'cpuP99Ms',
        'wallP99Ms',
        'fittingPasses',
        'rawBytes',
        'transmittedBytes',
      ],
      delivery: ['messagesPerSecond', 'p99Ms', 'deliveryGapP99Ms'],
    }
    const entries = adapter.manifestEntries()
    expect(entries.map(({ name }) => name)).toEqual(adapter.ROWS.map(({ name }) => name))
    for (const [i, entry] of entries.entries()) {
      const { job: jobName, flags } = adapter.ROWS[i]
      const job = JOBS[jobName]
      expect(entry).toEqual({
        name: entry.name,
        adapter: 'telemetrySession',
        command: [SCRIPT, '--row', entry.name, '--raw', '{raw}'],
        broker: job.mode === 'delivery',
        metrics: metrics[job.mode].map((key) => ({ key, unit: expect.any(String) })),
        arms: ['fork'],
      })
      for (const label of job.roles) {
        const telemetry = job.telemetry[label] ? ['--telemetry', job.telemetry[label]] : []
        const control = job.controls.includes(label) ? '1' : '0'
        const parsed = parseWorkerArgs([
          '--mode',
          job.mode,
          '--control',
          control,
          ...flags,
          ...telemetry,
        ])
        expect(parsed).toMatchObject({ mode: job.mode, control: Number(control) })
      }
    }
  })

  test('the payload row reports an unavailable ZSTD codec as an error without numbers', () => {
    const name = 'telemetrySession-payload-257-cumulative-zstd-poor'
    const env = { BENCH_TELEMETRY_ROW: name }
    const unavailable = controller({ exit: 4, verdict: null, stderr: 'codec unavailable' })
    const result = runRow(name, env, unavailable.spawn, { zstd: false })
    const manifest = JSON.parse(fs.readFileSync(`${result.raw}.manifest.json`, 'utf8'))
    expect(manifest).toMatchObject({ optionalCodec: 'zstd', codecs: { zstd: false } })
    expect(manifest.arms[0].argv).toEqual(
      expect.arrayContaining(['--points', '257', '--temporality', 'cumulative', '--codec', 'zstd'])
    )
    expect(unavailable.calls).toHaveLength(1)
    expect(result.rows).toEqual([
      expect.objectContaining({
        status: 'error',
        reason: `ZSTD is not available on Node ${process.version}: the row has no numbers`,
      }),
    ])
    expect(result.rows[0]).not.toHaveProperty('median')

    const available = controller()
    const measured = runRow(name, env, available.spawn, { zstd: true })
    const declared = JSON.parse(fs.readFileSync(`${measured.raw}.manifest.json`, 'utf8'))
    expect(declared).not.toHaveProperty('optionalCodec')
    expect(declared.codecs.zstd).toBe(true)
    expect(measured.details[0].status).toBe('valid')
  })

  test('the telemetry row runs a previous arm only from a verified checkout of the accepted baseline', () => {
    expect(adapter.ACCEPTED_BASELINE).toEqual({
      milestone: '9E',
      ref: REF,
      commit: '5ed201d5acda199ae2af2f534c1216519f4b0344',
    })
    const name = 'telemetrySession-requests-normal'
    const refused = (mutate, accepted, baseline = gitBaseline()) => {
      mutate(baseline.root)
      const { spawn, calls } = controller()
      const env = { BENCH_TELEMETRY_ROW: name, TELEMETRY_PREVIOUS_ROOT: baseline.root }
      const { rows, raw } = runRow(name, env, spawn, {
        accepted: accepted ?? baseline.accepted,
      })
      expect(calls).toEqual([])
      expect(fs.existsSync(`${raw}.manifest.json`)).toBe(false)
      expect(rows).toEqual([expect.objectContaining({ status: 'error' })])
      expect(rows[0].reason).toMatch(/^run 1 of 1: the accepted baseline is not verified: /)
      return { reason: rows[0].reason, baseline }
    }

    const plain = { root: fakeRoot('// previous client\n') }
    const accepted = { milestone: MILESTONE, ref: REF, commit: 'a'.repeat(40) }
    const notGit = refused(() => {}, accepted, plain)
    expect(notGit.reason).toContain(`${plain.root} is not a git checkout`)

    let moved
    const wrongCommit = refused((root) => {
      git(root, 'commit', '-q', '--allow-empty', '-m', 'later')
      moved = git(root, 'rev-parse', 'HEAD')
    })
    expect(wrongCommit.reason).toContain(
      `the checkout is at ${moved}, not the accepted ${REF} commit ${wrongCommit.baseline.commit}`
    )

    const dirty = refused((root) => {
      fs.writeFileSync(path.join(root, 'src/index.js'), '// edited\n')
      fs.writeFileSync(path.join(root, 'src/extra.js'), '// untracked\n')
    })
    expect(dirty.reason).toContain('src/index.js differs from the accepted commit')
    expect(dirty.reason).toContain('src/extra.js is not in the accepted commit')

    const untagged = refused((root) => git(root, 'tag', '-d', REF))
    expect(untagged.reason).toContain(`tag ${REF} does not resolve to`)

    const mismatched = refused(() => {}, { milestone: MILESTONE, ref: REF, commit: 'f'.repeat(40) })
    expect(mismatched.reason).toContain(`not the accepted ${REF} commit ${'f'.repeat(40)}`)

    const genuine = gitBaseline()
    const { spawn, calls } = controller()
    const env = { BENCH_TELEMETRY_ROW: name, TELEMETRY_PREVIOUS_ROOT: genuine.root }
    const { rows, details, raw } = runRow(name, env, spawn, { accepted: genuine.accepted })
    expect(calls).toHaveLength(2)
    const manifest = JSON.parse(fs.readFileSync(`${raw}.manifest.json`, 'utf8'))
    const previous = manifest.arms.find(({ label }) => label === 'previous')
    expect(JSON.parse(fs.readFileSync(manifest.acceptedBaseline.path, 'utf8'))).toEqual({
      milestone: MILESTONE,
      status: 'accepted',
      ref: REF,
      commit: genuine.commit,
      verified: 'HEAD, tag and every frozen file equal the accepted commit',
      tree: previous.tree,
      sourceSha256: previous.source.sha256,
      tarballSha256: previous.tarball.sha256,
    })
    expect(details[0].status).toBe('valid')
    expect(details[0].notes).toContain(
      `previous arm ${REF} at ${genuine.commit}, verified against the checkout's HEAD, tag and files`
    )
    expect(rows).toHaveLength(15)
  })

  test('the telemetry row keeps its frozen sources so the session manifest validates after the command returns', () => {
    const name = 'telemetrySession-requests-normal'
    const baseline = gitBaseline()
    const { spawn } = controller()
    const env = { BENCH_TELEMETRY_ROW: name, TELEMETRY_PREVIOUS_ROOT: baseline.root }
    const { raw, details } = runRow(name, env, spawn, { accepted: baseline.accepted })
    expect(details[0].status).toBe('valid')

    const snapshots = `${raw}.snapshots`
    const manifest = JSON.parse(fs.readFileSync(`${raw}.manifest.json`, 'utf8'))
    for (const arm of manifest.arms) {
      expect(path.dirname(arm.root)).toBe(snapshots)
      expect(path.dirname(arm.tarball.path)).toBe(snapshots)
      expect(sha256(fs.readFileSync(arm.tarball.path))).toBe(arm.tarball.sha256)
    }
    expect(manifest.consume.path).toBe(
      path.join(snapshots, 'candidate/scripts/benchmarks/consumeThroughput.js')
    )
    expect(linksBelow(snapshots)).toEqual([])
    validateManifest(manifest)

    fs.appendFileSync(path.join(snapshots, 'previous/src/index.js'), '// changed\n')
    expect(() => validateManifest(manifest)).toThrow('frozen source catalogue differs')
  })

  test('the telemetry row runs only in its own job, within the job allowance and with the disk its ledgers need', () => {
    const budgets = Object.keys(JOBS).map(minimumBudget)
    expect(adapter.timeoutMs).toBeGreaterThanOrEqual(Math.max(...budgets) * 1000)

    const name = 'telemetrySession-requests-control'
    const budget = minimumBudget('requests-control')
    for (const env of [{}, { BENCH_TELEMETRY_ROW: CONTROL }]) {
      const elsewhere = controller()
      const shared = runRow(name, env, elsewhere.spawn)
      expect(elsewhere.calls).toEqual([])
      expect(shared.rows).toEqual([
        {
          benchmark: name,
          arm: '-',
          metric: '-',
          unit: '-',
          runs: 0,
          status: 'omitted',
          reason: `runs in its own telemetry-rows job of the benchmark workflow (BENCH_TELEMETRY_ROW=${name})`,
        },
      ])
      expect(fs.existsSync(`${shared.raw}.manifest.json`)).toBe(false)
    }

    const unbounded = controller()
    runRow(name, { BENCH_TELEMETRY_ROW: name }, unbounded.spawn)
    expect(unbounded.options[0].timeout).toBe(budget * 1000)

    const now = () => 1_000_000_000
    const deadline = String(1_000_000 + 7200)
    const stopped = controller({ stop: 12 })
    const cut = runRow(
      name,
      { BENCH_TELEMETRY_ROW: name, TELEMETRY_JOB_DEADLINE: deadline },
      stopped.spawn,
      { now }
    )
    expect(stopped.calls).toHaveLength(1)
    expect(stopped.options[0].timeout).toBe(6000 * 1000)
    expect(cut.rows).toEqual([
      expect.objectContaining({
        status: 'error',
        reason:
          `the session was cut off at its 6000 s limit after 0 s with 12 of 30 children completed ` +
          `(declared budget ${budget} s, job allowance 6000 s); the measurement stays owed and the partial evidence is kept`,
      }),
    ])
    expect(
      fs.readFileSync(`${cut.raw}.session/raw.jsonl`, 'utf8').split('\n').filter(Boolean)
    ).toHaveLength(12)

    const fits = controller()
    const short = runRow(
      name,
      { BENCH_TELEMETRY_ROW: name, TELEMETRY_JOB_DEADLINE: deadline },
      fits.spawn,
      { now }
    )
    expect(short.details[0].notes).toContain(
      `job allowance 6000 s is below the declared budget ${budget} s`
    )

    const late = controller()
    const expired = runRow(
      name,
      { BENCH_TELEMETRY_ROW: name, TELEMETRY_JOB_DEADLINE: String(1_000_000 + 600) },
      late.spawn,
      { now }
    )
    expect(late.calls).toEqual([])
    expect(expired.rows[0].reason).toContain('TELEMETRY_JOB_DEADLINE')

    const full = controller()
    const need = adapter.diskBytes('requests-control')
    expect(need).toBeGreaterThanOrEqual(childCount('requests-control') * 96 * 1024 * 1024)
    const cramped = runRow(name, { BENCH_TELEMETRY_ROW: name }, full.spawn, {
      statfs: () => ({ bavail: need - 1, bsize: 1 }),
    })
    expect(full.calls).toEqual([])
    expect(cramped.rows[0].reason).toContain(`disk: ${need - 1} bytes free under `)
    expect(cramped.rows[0].reason).toContain(`the session needs ${need}`)
  })

  test('the telemetry row keeps a cut-off session owed with its elapsed time and completed children when its last raw record is incomplete', () => {
    const name = 'telemetrySession-requests-control'
    const budget = minimumBudget('requests-control')
    const env = { BENCH_TELEMETRY_ROW: name, TELEMETRY_JOB_DEADLINE: String(1_000_000 + 7200) }
    const FRAGMENT = '{"round":'
    /** A session that runs 6000 s and rewrites its raw rows with `edit` before it returns. */
    const session = (script, edit) => {
      const clock = { t: 1_000_000_000 }
      const scripted = controller({
        ...script,
        during: (manifest) => {
          clock.t += 6000 * 1000
          const raw = path.join(manifest.directory, 'raw.jsonl')
          fs.writeFileSync(raw, edit(fs.readFileSync(raw, 'utf8')))
        },
      })
      return runRow(name, env, scripted.spawn, { now: () => clock.t })
    }

    const torn = session({ stop: 12 }, (text) => text + FRAGMENT)
    expect(torn.rows).toEqual([
      {
        benchmark: name,
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 1,
        status: 'error',
        reason:
          `the session was cut off at its 6000 s limit after 6000 s with 12 of 30 children completed ` +
          `(declared budget ${budget} s, job allowance 6000 s); the measurement stays owed and the partial evidence is kept; ` +
          `its raw evidence ends in an incomplete record of ${FRAGMENT.length} bytes`,
      },
    ])
    expect(torn.details ?? []).toEqual([])
    const kept = fs.readFileSync(`${torn.raw}.session/raw.jsonl`, 'utf8')
    expect(kept.endsWith(`}\n${FRAGMENT}`)).toBe(true)
    expect(kept.split('\n')).toHaveLength(13)

    const interior = session({ stop: 12 }, (text) => {
      const lines = text.split('\n')
      lines[5] = FRAGMENT
      return lines.join('\n')
    })
    expect(interior.rows).toEqual([expect.objectContaining({ runs: 0, status: 'error' })])
    expect(interior.rows[0].reason).not.toContain('children completed')
    expect(interior.rows[0]).not.toHaveProperty('median')

    const finished = session({}, (text) => text + FRAGMENT)
    expect(finished.rows).toEqual([expect.objectContaining({ runs: 0, status: 'error' })])
    expect(finished.details ?? []).toEqual([])
    expect(finished.rows[0]).not.toHaveProperty('median')
  })

  test('the long telemetry rows count a session that finishes inside the job and keep one the job cuts off owed with its elapsed time and completed children', () => {
    const allowance = 355 * 60 - 1200
    const start = 1_700_000_000_000
    const long = {
      'telemetrySession-requests-normal': 0,
      'telemetrySession-delivery-normal-eachMessage': 36,
      'telemetrySession-delivery-normal-eachBatch': 36,
      'telemetrySession-delivery-control-eachMessage': 30,
      'telemetrySession-delivery-control-eachBatch': 30,
    }
    for (const [name, resets] of Object.entries(long)) {
      const { job } = adapter.ROWS.find((row) => row.name === name)
      const total = childCount(job)
      const budget = minimumBudget(job)
      const preparation = resets * PREPARE_SECONDS
      expect(budget + preparation).toBeGreaterThan(allowance)
      const environment = () => {
        const fixture = JOBS[job].mode === 'delivery' ? deliveryFixture(name) : {}
        const baseline = JOBS[job].roles.includes('previous') ? gitBaseline() : null
        return {
          env: {
            ...fixture,
            BENCH_TELEMETRY_ROW: name,
            TELEMETRY_JOB_DEADLINE: String(start / 1000 + 355 * 60),
            ...(baseline && { TELEMETRY_PREVIOUS_ROOT: baseline.root }),
          },
          deps: baseline ? { accepted: baseline.accepted } : {},
        }
      }
      /** A session that takes `seconds`, with one 20 s reporter reset journalled per finished delivery child;
       * a cut-off session's journal ends in a torn line. */
      const timed = (seconds, script = {}) => {
        const clock = { t: start }
        const scripted = controller({
          ...script,
          during: (manifest) => {
            clock.t += seconds * 1000
            const done = script.stop ?? total
            const journal = Array.from({ length: resets ? done : 0 }, (_, ordinal) =>
              JSON.stringify({ ordinal, disposition: 'prepared', retained: [], elapsedMs: 20000 })
            )
            fs.writeFileSync(
              path.join(manifest.directory, 'journal.jsonl'),
              journal.map((line) => `${line}\n`).join('') +
                (script.stop === undefined ? '' : '{"ordinal":')
            )
          },
        })
        const { env, deps } = environment()
        return { ...runRow(name, env, scripted.spawn, { ...deps, now: () => clock.t }), scripted }
      }

      const fits = timed(19000)
      expect(fits.scripted.options[0].timeout).toBe(allowance * 1000)
      expect(fits.details[0].status).toBe('valid')
      expect(fits.rows.every(({ status }) => status === 'valid')).toBe(true)
      expect(fits.details[0].notes).toContain(
        `session elapsed 19000 s of its ${allowance} s limit, ${total} of ${total} children completed`
      )
      expect(fits.details[0].notes).toContain(
        `job allowance ${allowance} s is below the declared budget ${budget} s` +
          (preparation ? ` plus ${preparation} s of reporter resets` : '')
      )
      if (resets)
        expect(fits.details[0].notes).toContain(
          `reporter reset before each observation, outside its measured window: ${total} resets, ` +
            `${total * 20} s in total (bound ${PREPARE_SECONDS} s each, declared apart from the session budget)`
        )

      const done = Math.floor(total / 2)
      const cut = timed(allowance, { stop: done })
      const resetsNote = preparation ? `, reporter resets ${preparation} s` : ''
      const resetTime = resets ? `; ${done} reporter resets took ${done * 20} s` : ''
      expect(cut.rows).toEqual([
        {
          benchmark: name,
          arm: '-',
          metric: '-',
          unit: '-',
          runs: 1,
          status: 'error',
          reason:
            `the session was cut off at its ${allowance} s limit after ${allowance} s with ` +
            `${done} of ${total} children completed (declared budget ${budget} s${resetsNote}, ` +
            `job allowance ${allowance} s); the measurement stays owed and the partial evidence is kept${resetTime}`,
        },
      ])
      expect(cut.details ?? []).toEqual([])
      expect(
        fs.readFileSync(`${cut.raw}.session/raw.jsonl`, 'utf8').split('\n').filter(Boolean)
      ).toHaveLength(done)

      const clock = { t: start }
      const partial = controller({ stop: done, during: () => (clock.t += allowance * 1000) })
      const claimed = (command, args, opts) => {
        const result = partial.spawn(command, args, opts)
        if (!args.includes('--session')) return result
        const verdict = { disposition: 'pass', failures: [], receipt: { count: total } }
        return { ...result, stdout: JSON.stringify(verdict) }
      }
      const { env, deps } = environment()
      const late = runRow(name, env, claimed, { ...deps, now: () => clock.t })
      expect(late.rows).toEqual([expect.objectContaining({ status: 'error' })])
      expect(late.rows[0].reason).toContain('the measurement stays owed')
      expect(late.rows[0]).not.toHaveProperty('median')
    }
  })

  test('the delivery row refuses a reporter sink with an earlier session in it and keeps its own sink', () => {
    const name = 'telemetrySession-delivery-control-eachBatch'
    for (const stale of ['exports-1.jsonl', 'OVERFLOW-2.json']) {
      const env = deliveryFixture(name)
      fs.writeFileSync(path.join(env.TELEMETRY_EVIDENCE_DIR, stale), '{}\n')
      const { spawn, calls } = controller()
      const { rows } = runRow(name, env, spawn)
      expect(calls).toEqual([])
      expect(rows[0].reason).toContain(
        `TELEMETRY_EVIDENCE_DIR: a fresh reporter sink (it holds ${stale})`
      )
    }

    const env = deliveryFixture(name)
    const overflowing = controller({
      exit: 1,
      verdict: null,
      stderr: 'Error: Telemetry fixture overflow',
      during: () => {
        fs.writeFileSync(path.join(env.TELEMETRY_EVIDENCE_DIR, 'exports-3.jsonl'), '{"seq":1}\n')
        fs.writeFileSync(
          path.join(env.TELEMETRY_EVIDENCE_DIR, 'OVERFLOW-3.json'),
          '{"reason":"file-cap"}\n'
        )
      },
    })
    const { rows, raw } = runRow(name, env, overflowing.spawn)
    expect(overflowing.calls).toHaveLength(1)
    expect(rows[0].status).toBe('error')
    expect(rows[0].reason).toMatch(/; reporter sink overflow: OVERFLOW-3\.json$/)
    expect(fs.readdirSync(`${raw}.sink`).sort()).toEqual([
      'OVERFLOW-3.json',
      'exports-3.jsonl',
      'reporter-1.json',
    ])
    expect(fs.readFileSync(`${raw}.sink/OVERFLOW-3.json`, 'utf8')).toBe('{"reason":"file-cap"}\n')

    const batch = controller()
    const passed = runRow(name, deliveryFixture(name), batch.spawn)
    const manifest = JSON.parse(fs.readFileSync(`${passed.raw}.manifest.json`, 'utf8'))
    expect(manifest.arms.map(({ argv }) => argv[argv.indexOf('--handler') + 1])).toEqual([
      'eachBatch',
      'eachBatch',
    ])
    expect(passed.details[0].controls.find(({ arm }) => arm === 'slow')).toEqual({
      arm: 'slow',
      name: 'positive control fires',
      passed: true,
      detail: 'messagesPerSecond, p99Ms',
    })
  })
})
