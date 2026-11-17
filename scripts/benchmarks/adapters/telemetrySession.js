/**
 * Report adapter of the `telemetry.js --session` rows, and the command those rows run.
 *
 * Command: node scripts/benchmarks/adapters/telemetrySession.js --job <job> --raw <file> [worker flags]
 * It checks the job's fixture and refuses without it, freezes the candidate source (and, for a job
 * with a `previous` arm, the 3.0.0-alpha.9e checkout) into snapshots outside the evidence, writes the
 * session manifest beside `--raw`, runs the controller and then its `--verify-session`, and prints
 * one JSON line: the manifest's path and digest, both verdicts and every raw observation. The
 * session's evidence directory (`<raw>.session`) stays beside the manifest; the snapshots are removed.
 * Exit codes: 0 a report line was printed, 1 the command failed, 2 a usage error or a missing fixture.
 *
 * The row shows each arm's median and range of the manifest metrics over the measured observations,
 * and the controls: declared observation count, every observation verified, the independent
 * re-verification, each positive control and the A/A spread. A failed control makes the row
 * `invalid`, a void session `inconclusive`; the session's gate verdict is shown and never decides it.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { lastJsonLine, tail } = require('../report')
const { median } = require('../session')
const { JOBS, childCount, digest, minimumBudget } = require('../telemetry')
const { isSupported: zstd } = require('../../../src/protocol/message/compression/zstd')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const TELEMETRY = path.join(ROOT, 'scripts/benchmarks/telemetry.js')
const SESSION = path.join(ROOT, 'scripts/benchmarks/session.js')
/** What a worker loads from a source root; `consumeThroughput.js` is bound by the controller. */
const SNAPSHOT = [
  'index.js',
  'package.json',
  'src',
  'testHelpers',
  'scripts/benchmarks/consumeThroughput.js',
]
/** The published listeners of `docker-compose.telemetry.yml`. */
const BROKERS = ['localhost:19092', 'localhost:19095', 'localhost:19098']
/** The Kafka lines with a reporter jar for `docker-compose.telemetry.yml`. */
const LINES = ['4.4.0', '4.3.1', '4.2.2']
const PREVIOUS_REF = '3.0.0-alpha.9e'
const INEFFECTIVE = /the (\S+) positive control is ineffective/
const SPREAD = 'A/A spread'

/** @param {Buffer|string} bytes @returns {string} lowercase SHA-256 */
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/** @param {string} file absolute path @returns {{ path: string, sha256: string }} its identity */
const asset = (file) => ({ path: file, sha256: sha256(fs.readFileSync(file)) })

/** @param {unknown} value @param {'isDirectory'|'isFile'} kind @returns {boolean} an absolute existing path of that kind */
const exists = (value, kind) =>
  typeof value === 'string' &&
  path.isAbsolute(value) &&
  fs.statSync(value, { throwIfNoEntry: false })?.[kind]() === true

/**
 * Lists what the job's fixture lacks. A `previous` arm needs the 3.0.0-alpha.9e checkout; a
 * delivery job needs the telemetry cluster with its reporter, the reporter sink and the decoder
 * prepared by `scripts/e2e/telemetry/prepareDecoder.js`.
 * @param {string} jobName a `telemetry.js` job
 * @param {Record<string, string|undefined>} env
 * @returns {string[]} one line per missing part, empty when the fixture is complete
 */
const fixtureProblems = (jobName, env) => {
  const job = JOBS[jobName]
  const checks = []
  if (job.roles.includes('previous'))
    checks.push(
      [
        exists(env.TELEMETRY_PREVIOUS_ROOT, 'isDirectory'),
        `TELEMETRY_PREVIOUS_ROOT: an absolute checkout of ${PREVIOUS_REF}`,
      ],
      [env.TELEMETRY_PREVIOUS_REF === PREVIOUS_REF, `TELEMETRY_PREVIOUS_REF: ${PREVIOUS_REF}`],
      [
        /^[0-9a-f]{40}$/.test(env.TELEMETRY_PREVIOUS_COMMIT ?? ''),
        'TELEMETRY_PREVIOUS_COMMIT: the commit of that checkout',
      ]
    )
  if (job.mode === 'delivery')
    checks.push(
      [
        env.COMPOSE_FILE === 'docker-compose.telemetry.yml',
        'COMPOSE_FILE: docker-compose.telemetry.yml, the cluster with the telemetry reporter',
      ],
      [Boolean(env.COMPOSE_PROJECT_NAME), 'COMPOSE_PROJECT_NAME: the telemetry cluster project'],
      [LINES.includes(env.KAFKA_VERSION), `KAFKA_VERSION: one of ${LINES.join(', ')}`],
      [
        exists(env.TELEMETRY_EVIDENCE_DIR, 'isDirectory'),
        'TELEMETRY_EVIDENCE_DIR: the reporter sink directory',
      ],
      [
        exists(env.TELEMETRY_JAVA_CACHE, 'isDirectory'),
        'TELEMETRY_JAVA_CACHE: the verified decoder cache',
      ],
      [
        exists(env.TELEMETRY_DECODER_ARCHIVE, 'isFile'),
        'TELEMETRY_DECODER_ARCHIVE: the archive prepareDecoder.js wrote',
      ]
    )
  return checks.filter(([ok]) => !ok).map(([, problem]) => problem)
}

/**
 * Lists a root's files the way the controller's source check walks them: names sorted per
 * directory, depth first, `.git` and `node_modules` skipped.
 * @param {string} root
 * @returns {{ path: string, sha256: string }[]}
 */
const catalogue = (root) => {
  const files = []
  const visit = (relative) => {
    for (const name of fs.readdirSync(path.join(root, relative)).sort()) {
      if (name === '.git' || name === 'node_modules') continue
      const file = path.join(relative, name)
      if (fs.lstatSync(path.join(root, file)).isDirectory()) visit(file)
      else files.push({ path: file, sha256: sha256(fs.readFileSync(path.join(root, file))) })
    }
  }
  visit('')
  return files
}

/**
 * Copies a file or a directory tree; a link is copied as the file it points to.
 * @param {string} from
 * @param {string} to
 * @returns {void}
 */
const copy = (from, to) => {
  if (fs.lstatSync(from).isDirectory()) {
    fs.mkdirSync(to, { recursive: true })
    for (const name of fs.readdirSync(from)) copy(path.join(from, name), path.join(to, name))
  } else {
    fs.mkdirSync(path.dirname(to), { recursive: true })
    fs.copyFileSync(from, to)
  }
}

/**
 * Freezes the files a worker loads from `from` into `<work>/<label>` with its archive, and writes
 * the source receipt to `receipt`.
 * @param {string} from source root
 * @param {string} label `candidate` or `previous`
 * @param {{ work: string, receipt: string, spawn: Function }} options
 * @returns {{ root: string, tree: string, source: object, tarball: object }} the arm's identity
 * @throws {Error} when the archive cannot be written
 */
const freeze = (from, label, { work, receipt, spawn }) => {
  const root = path.join(work, label)
  for (const entry of SNAPSHOT) copy(path.join(from, entry), path.join(root, entry))
  if (fs.existsSync(path.join(ROOT, 'node_modules')))
    fs.symlinkSync(path.join(ROOT, 'node_modules'), path.join(root, 'node_modules'))
  const tarball = path.join(work, `${label}.tar`)
  const archived = spawn('tar', ['-cf', tarball, '--exclude=./node_modules', '-C', root, '.'], {
    encoding: 'utf8',
  })
  if (archived.error) throw archived.error
  if (archived.status !== 0)
    throw new Error(`tar of the ${label} snapshot failed: ${tail(archived.stderr)}`)
  const files = catalogue(root)
  const tree = digest(files)
  fs.writeFileSync(receipt, JSON.stringify({ tree, files, tarballSha256: asset(tarball).sha256 }))
  return { root, tree, source: asset(receipt), tarball: asset(tarball) }
}

/**
 * Builds the session manifest `telemetry.js --session` validates.
 * @param {{ jobName: string, extra: string[], raw: string, candidate: object, previous?: object,
 *   accepted?: object, root: string }} options `extra` are the worker flags every arm shares
 * @returns {object}
 */
const sessionManifest = ({ jobName, extra, raw, candidate, previous, accepted, root }) => {
  const job = JOBS[jobName]
  return {
    schemaVersion: 1,
    job: jobName,
    runId: `tsess-${Date.now().toString(36)}`,
    directory: `${raw}.session`,
    driver: asset(TELEMETRY),
    session: asset(SESSION),
    consume: asset(path.join(root, 'scripts/benchmarks/consumeThroughput.js')),
    node: {
      binary: process.execPath,
      sha256: asset(process.execPath).sha256,
      version: process.version,
      platform: process.platform,
      arch: process.arch,
    },
    codecs: { none: true, gzip: true, zstd },
    ...(accepted && { acceptedBaseline: accepted }),
    ...(job.mode === 'delivery' && { fixture: { brokers: BROKERS } }),
    arms: job.roles.map((label) => {
      const control = job.controls.includes(label) ? '1' : '0'
      const telemetry = job.telemetry[label] ? ['--telemetry', job.telemetry[label]] : []
      const argv = ['--mode', job.mode, '--control', control, ...extra, ...telemetry]
      const identity = label === 'previous' ? previous : candidate
      return { label, ...identity, argv, configSha256: digest({ mode: job.mode, argv }) }
    }),
    reference: job.reference,
    runs: job.runs,
    budgetSeconds: minimumBudget(jobName),
  }
}

/**
 * @param {object} result a controller run
 * @returns {{ status: number|null, signal: string|null, verdict: object|null,
 *   ineffective: string|null, stderr: string }} what the report keeps of it
 */
const outcomeOf = (result) => {
  let verdict = null
  try {
    const line = lastJsonLine(result.stdout ?? '')
    if (typeof line.disposition === 'string') verdict = line
  } catch {
    // No verdict line: the controller failed before its verdict.
  }
  return {
    status: result.status ?? null,
    signal: result.signal ?? null,
    verdict,
    ineffective: INEFFECTIVE.exec(result.stderr ?? '')?.[1] ?? null,
    stderr: tail(result.error?.message ?? result.stderr),
  }
}

/**
 * @param {string[]} argv `--job <job> --raw <absolute file>` and the shared worker flags
 * @returns {{ jobName: string, raw: string, extra: string[] }}
 * @throws {Error} for a missing or unknown job, a relative `--raw`, or a valueless flag
 */
const parseArgs = (argv) => {
  const flags = { extra: [] }
  for (let i = 0; i < argv.length; i += 2) {
    if (argv[i + 1] === undefined) throw new Error(`${argv[i]} needs a value`)
    if (argv[i] === '--job') flags.jobName = argv[i + 1]
    else if (argv[i] === '--raw') flags.raw = argv[i + 1]
    else flags.extra.push(argv[i], argv[i + 1])
  }
  if (!Object.hasOwn(JOBS, flags.jobName ?? ''))
    throw new Error(`--job must be one of ${Object.keys(JOBS).join('|')}`)
  if (!path.isAbsolute(flags.raw ?? '')) throw new Error('--raw takes an absolute file')
  return flags
}

/**
 * Runs one session for the report and prints its report line. The snapshots live in a temporary
 * directory under `RUNNER_TEMP` (or the system one) and are removed when the command ends.
 * @param {string[]} argv see `parseArgs`
 * @param {{ env?: object, spawn?: Function, log?: Function, error?: Function, root?: string }} [deps]
 *   `root` is the candidate source, the checkout by default
 * @returns {number} 0 once the report line is printed, 1 on a failure, 2 on a usage error or a
 *   missing fixture
 */
const main = (
  argv,
  {
    env = process.env,
    spawn = spawnSync,
    log = console.log,
    error = console.error,
    root = ROOT,
  } = {}
) => {
  let args
  try {
    args = parseArgs(argv)
  } catch (failure) {
    error(failure.message)
    return 2
  }
  const { jobName, raw, extra } = args
  const missing = fixtureProblems(jobName, env)
  if (missing.length) {
    log(JSON.stringify({ job: jobName, refused: 'fixture', missing }))
    return 2
  }
  const work = fs.mkdtempSync(path.join(env.RUNNER_TEMP || os.tmpdir(), 'telemetry-session-'))
  try {
    const candidate = freeze(root, 'candidate', {
      work,
      receipt: `${raw}.candidate-source.json`,
      spawn,
    })
    let previous
    let accepted
    if (JOBS[jobName].roles.includes('previous')) {
      previous = freeze(env.TELEMETRY_PREVIOUS_ROOT, 'previous', {
        work,
        receipt: `${raw}.previous-source.json`,
        spawn,
      })
      const proof = `${raw}.accepted-baseline.json`
      fs.writeFileSync(
        proof,
        JSON.stringify({
          milestone: '9E',
          status: 'accepted',
          tree: previous.tree,
          sourceSha256: previous.source.sha256,
          tarballSha256: previous.tarball.sha256,
          ref: env.TELEMETRY_PREVIOUS_REF,
          commit: env.TELEMETRY_PREVIOUS_COMMIT,
        })
      )
      accepted = asset(proof)
    }
    const manifest = sessionManifest({ jobName, extra, raw, candidate, previous, accepted, root })
    const manifestFile = `${raw}.manifest.json`
    fs.writeFileSync(manifestFile, JSON.stringify(manifest, null, 2), { flag: 'wx' })
    const options = { cwd: ROOT, env, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }
    const ran = spawn(
      process.execPath,
      ['--expose-gc', TELEMETRY, '--session', manifestFile],
      options
    )
    error(ran.stderr ?? '')
    const session = outcomeOf(ran)
    let verify = null
    if (session.verdict) {
      const checked = spawn(
        process.execPath,
        [TELEMETRY, '--verify-session', manifest.directory],
        options
      )
      error(checked.stderr ?? '')
      verify = outcomeOf(checked)
    }
    const rawRows = path.join(manifest.directory, 'raw.jsonl')
    const observations = fs.existsSync(rawRows)
      ? fs
          .readFileSync(rawRows, 'utf8')
          .split('\n')
          .filter(Boolean)
          .map((line) => JSON.parse(line))
      : []
    log(
      JSON.stringify({
        job: jobName,
        manifest: { path: manifestFile, sha256: asset(manifestFile).sha256 },
        declared: { observations: childCount(jobName), runs: manifest.runs },
        session,
        verify,
        observations,
      })
    )
    return 0
  } catch (failure) {
    error(failure.stack ?? String(failure))
    return 1
  } finally {
    fs.rmSync(work, { recursive: true, force: true })
  }
}

/**
 * Reads the command's report line.
 * @param {{ status: number|null, signal?: string, error?: Error, stdout?: string, stderr?: string }} result
 * @returns {object} the report line
 * @throws {Error} for a refused fixture, a spawn error, a signal, a non-zero exit or no report line
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  let line
  try {
    line = lastJsonLine(result.stdout ?? '')
  } catch {
    throw new Error(`exit ${result.status}: ${tail(result.stderr) || 'no session report'}`)
  }
  if (line.refused === 'fixture')
    throw new Error(`the telemetry fixture is missing: ${line.missing.join('; ')}`)
  if (result.status !== 0) throw new Error(`exit ${result.status}: ${tail(result.stderr)}`)
  if (!Object.hasOwn(JOBS, line.job) || !Array.isArray(line.observations) || !line.session) {
    throw new Error('the last JSON line is not a telemetry session report')
  }
  return line
}

/**
 * Turns one session into the row's lines and details.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm the report's arm, `fork`
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail?: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const one = (status, reason, runs = outputs.length) => ({
    rows: [{ benchmark: entry.name, arm: '-', metric: '-', unit: '-', runs, status, reason }],
  })
  if (error) return one('error', error)
  const [report] = outputs
  const declaredJob = entry.command[entry.command.indexOf('--job') + 1]
  if (report.job !== declaredJob)
    return one('error', `the session ran job ${report.job}, expected ${declaredJob}`)
  const job = JOBS[report.job]
  const { session, verify, observations } = report
  const notes = [`session manifest sha256 ${report.manifest.sha256}`]

  if (!session.verdict) {
    if (!session.ineffective) {
      return one(
        'error',
        `the session failed (exit ${session.status ?? session.signal}): ${session.stderr}`
      )
    }
    const control = {
      arm: session.ineffective,
      name: 'positive control fires',
      passed: false,
      detail: `the ${session.ineffective} positive control is ineffective`,
    }
    const reason = `failed controls: ${control.arm} ${control.name}`
    return {
      ...one('invalid', reason, 1),
      detail: { status: 'invalid', reason, controls: [control], notes },
    }
  }

  const { verdict } = session
  const total = childCount(report.job)
  const isVoid = verdict.disposition === 'void'
  const controls = [
    {
      arm: 'session',
      name: 'declared observations',
      passed: observations.length === total && verdict.receipt?.count === total,
      detail: `${observations.length} observations, receipt ${verdict.receipt?.count}, declared ${total}`,
    },
    {
      arm: 'session',
      name: 'every observation verified',
      passed: observations.every((row) => row.disposition === 'valid'),
    },
    {
      arm: 'session',
      name: 'session re-verified',
      passed:
        verify?.status === session.status &&
        verify.verdict?.disposition === verdict.disposition &&
        verify.verdict?.receipt?.chain === verdict.receipt?.chain,
      detail: verify?.verdict
        ? `exit ${verify.status}, ${verify.verdict.disposition}, chain ${verify.verdict.receipt?.chain === verdict.receipt?.chain ? 'matches' : 'differs'}`
        : `no verdict (exit ${verify?.status ?? 'none'}): ${verify?.stderr ?? ''}`,
    },
  ]
  if (!isVoid) {
    for (const label of job.controls) {
      const fired = verdict.failures.find(
        (failure) => failure.arm === label && failure.expected === true
      )
      controls.push({
        arm: label,
        name: 'positive control fires',
        passed: Boolean(fired),
        detail: fired ? fired.fields.join(', ') : 'no expected failure',
      })
    }
  }
  controls.push({
    arm: `${job.reference}~AA`,
    name: SPREAD,
    passed: !isVoid,
    detail: verdict.disposition,
  })
  const gates = verdict.failures.filter((failure) => failure.expected !== true)
  notes.push(
    `session verdict ${verdict.disposition} (exit ${session.status})` +
      `${gates.length ? `: ${gates.map(({ arm: label, fields }) => `${label} ${fields.join(', ')}`).join('; ')}` : ''}` +
      ' (information; it gates nothing)',
    `receipt chain ${verdict.receipt?.chain}`
  )

  const failed = controls.filter(({ name, passed }) => !passed && name !== SPREAD)
  let status = 'valid'
  let reason
  if (failed.length) {
    status = 'invalid'
    reason = `failed controls: ${failed.map(({ arm: label, name }) => `${label} ${name}`).join('; ')}`
  } else if (isVoid) {
    status = 'inconclusive'
    reason = 'the A/A control is void'
  }
  const detail = { status, ...(reason && { reason }), controls, notes }
  if (status === 'invalid') return { ...one(status, reason, 1), detail }

  const measured = observations.filter((row) => !row.warmup)
  const lines = []
  for (const label of [...job.roles, `${job.reference}~AA`]) {
    for (const { key, unit } of entry.metrics) {
      const values = measured.filter((row) => row.label === label).map((row) => row.result?.[key])
      if (values.length !== job.runs || !values.every(Number.isFinite)) {
        return one(
          'error',
          `${label} has ${values.length} observations of ${key}, expected ${job.runs} finite`
        )
      }
      lines.push({
        benchmark: entry.name,
        arm: label,
        metric: key,
        unit,
        runs: values.length,
        status,
        median: median(values),
        min: Math.min(...values),
        max: Math.max(...values),
      })
    }
  }
  return { rows: lines, detail }
}

/** One session per report arm: the session repeats every arm itself. */
module.exports = { runs: 1, timeoutMs: 3 * 60 * 60 * 1000, read, rows, main, fixtureProblems }

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
