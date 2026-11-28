/**
 * Report adapter of the telemetry session rows (`telemetry.js --session`), and the command they run.
 *
 * Command: node scripts/benchmarks/adapters/telemetrySession.js --row <name> --raw <file>
 * One row is one owed configuration: a `telemetry.js` job and its worker flags (`ROWS`). The command
 * runs only in the workflow job that owns the row (`BENCH_TELEMETRY_ROW`); anywhere else it prints
 * `{ row, omitted }` and the report shows the row as omitted with that reason. It checks the fixture and
 * the free disk and refuses without them, freezes the candidate source (and, for a job with a
 * `previous` arm, the accepted baseline checkout, verified against `ACCEPTED_BASELINE`) into
 * `<raw>.snapshots/`, writes the session manifest beside `--raw`, runs the controller within
 * min(declared session time, job allowance) and then its `--verify-session`, and prints one JSON line.
 * The declared session time is the controller's budget plus, for a delivery job, one bounded reporter
 * reset per observation. A session the job allowance cuts off is an `error` row with its elapsed time
 * and completed children: the measurement stays owed, and no partial session is accepted.
 * Exit codes: 0 a report or omitted line was printed, 1 the command failed, 2 a usage error, a
 * missing fixture or an unverified baseline.
 *
 * Evidence kept beside `--raw` for replay: the manifest, the session directory (`.session`), both
 * snapshot roots and their archives (`.snapshots`), the source and baseline receipts, the decoder
 * receipt (`.decoder.json`) and the reporter sink (`.sink`). Replay: restore the artifact at the same
 * absolute path, check out the same commit at the same path, use the same Node binary, then run
 * `node scripts/benchmarks/telemetry.js --verify-session <raw>.session`.
 *
 * The row shows each arm's median and range of the manifest metrics over the measured observations,
 * and the controls: declared observation count, every observation verified, the independent
 * re-verification, the reporter sink, each positive control and the A/A spread. A failed control
 * makes the row `invalid`, a void session `inconclusive`; the session's gate verdict is shown and
 * never decides it.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { lastJsonLine, tail } = require('../report')
const { median } = require('../session')
const { JOBS, PREPARE_SECONDS, childCount, digest, minimumBudget } = require('../telemetry')
const { isSupported } = require('../../../src/protocol/message/compression/zstd')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const SCRIPT = 'scripts/benchmarks/adapters/telemetrySession.js'
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
/** The only tree a `previous` arm may run: the accepted tag and the commit it was accepted at. */
const ACCEPTED_BASELINE = Object.freeze({
  milestone: '9E',
  ref: '3.0.0-alpha.9e',
  commit: '5ed201d5acda199ae2af2f534c1216519f4b0344',
})
const MIB = 1024 * 1024
/** Bytes one observation retains, per mode: its fixed-size ledgers plus payload or sink copies. */
const CHILD_BYTES = { requests: 112 * MIB, payload: 128 * MIB, delivery: 264 * MIB }
/** Seconds kept before the job deadline for `--verify-session` and the evidence upload. */
const AFTER_SESSION_SECONDS = 1200
const VERIFY_SECONDS = 900
const FREEZE_SECONDS = 300
const INEFFECTIVE = /the (\S+) positive control is ineffective/
const SPREAD = 'A/A spread'
const METRICS = {
  requests: [
    { key: 'cpuNsPerPair', unit: 'ns' },
    { key: 'elapsedNsPerPair', unit: 'ns' },
    { key: 'heapExternalDeltaBytes', unit: 'bytes' },
  ],
  payload: [
    { key: 'sliceP99Ms', unit: 'ms' },
    { key: 'cpuP99Ms', unit: 'ms' },
    { key: 'wallP99Ms', unit: 'ms' },
    { key: 'fittingPasses', unit: 'passes' },
    { key: 'rawBytes', unit: 'bytes' },
    { key: 'transmittedBytes', unit: 'bytes' },
  ],
  delivery: [
    { key: 'messagesPerSecond', unit: 'msg/s' },
    { key: 'p99Ms', unit: 'ms' },
    { key: 'deliveryGapP99Ms', unit: 'ms' },
  ],
}

/** @param {string[]} values @param {(value: string) => object[]} next @returns {object[]} */
const each = (values, next) => values.flatMap(next)

/**
 * Every owed telemetry configuration: both request jobs, the payload job over points ×
 * temporality × codec × attributes, and both delivery jobs over both handlers.
 * @type {{ name: string, job: string, flags: string[] }[]}
 */
const ROWS = [
  { name: 'telemetrySession-requests-normal', job: 'requests-normal', flags: [] },
  { name: 'telemetrySession-requests-control', job: 'requests-control', flags: [] },
  ...each(['1', '257'], (points) =>
    each(['delta', 'cumulative'], (temporality) =>
      each(['none', 'gzip', 'zstd'], (codec) =>
        ['compressible', 'poor'].map((attributes) => ({
          name: `telemetrySession-payload-${points}-${temporality}-${codec}-${attributes}`,
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
        }))
      )
    )
  ),
  ...each(['delivery-normal', 'delivery-control'], (job) =>
    ['eachMessage', 'eachBatch'].map((handler) => ({
      name: `telemetrySession-${job}-${handler}`,
      job,
      flags: ['--handler', handler],
    }))
  ),
]

/** @returns {object[]} the `scripts/benchmarks/manifest.json` entries of `ROWS` */
const manifestEntries = () =>
  ROWS.map(({ name, job }) => ({
    name,
    adapter: 'telemetrySession',
    command: [SCRIPT, '--row', name, '--raw', '{raw}'],
    broker: JOBS[job].mode === 'delivery',
    metrics: METRICS[JOBS[job].mode],
    arms: ['fork'],
  }))

/** @param {string} job a `telemetry.js` job @returns {number} free bytes its session needs */
const diskBytes = (job) => childCount(job) * CHILD_BYTES[JOBS[job].mode] + 1024 * MIB

/** @param {string} job a `telemetry.js` job @returns {number} seconds of reporter resets it declares */
const preparationSeconds = (job) =>
  JOBS[job].mode === 'delivery' ? childCount(job) * PREPARE_SECONDS : 0

/** One timeout over every row: the largest declared session time plus freezing and re-verification. */
const timeoutMs =
  (Math.max(...Object.keys(JOBS).map((job) => minimumBudget(job) + preparationSeconds(job))) +
    FREEZE_SECONDS +
    VERIFY_SECONDS) *
  1000

/** @param {Buffer|string} bytes @returns {string} lowercase SHA-256 */
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/** @param {string} file absolute path @returns {{ path: string, sha256: string }} its identity */
const asset = (file) => ({ path: file, sha256: sha256(fs.readFileSync(file)) })

/** @param {unknown} value @param {'isDirectory'|'isFile'} kind @returns {boolean} an absolute existing path of that kind */
const exists = (value, kind) =>
  typeof value === 'string' &&
  path.isAbsolute(value) &&
  fs.statSync(value, { throwIfNoEntry: false })?.[kind]() === true

/** @param {string} name a row @returns {object|undefined} its `ROWS` entry */
const rowOf = (name) => ROWS.find((row) => row.name === name)

/** @param {string} name a row @returns {string} why the row prints no session outside its own job */
const outside = (name) =>
  `runs in its own telemetry-rows job of the benchmark workflow (BENCH_TELEMETRY_ROW=${name})`

/**
 * The session's time limit: its declared budget plus its reporter resets, cut to the job allowance
 * when the workflow sets `TELEMETRY_JOB_DEADLINE` (epoch seconds), less the time kept for
 * re-verification and upload.
 * @param {string} job
 * @param {Record<string, string|undefined>} env
 * @param {() => number} now milliseconds
 * @returns {{ budget: number, preparation: number, allowance: number|null, limit: number }} seconds
 */
const limits = (job, env, now) => {
  const budget = minimumBudget(job)
  const preparation = preparationSeconds(job)
  const declared = budget + preparation
  if (env.TELEMETRY_JOB_DEADLINE === undefined)
    return { budget, preparation, allowance: null, limit: declared }
  const allowance =
    Math.floor(Number(env.TELEMETRY_JOB_DEADLINE) - now() / 1000) - AFTER_SESSION_SECONDS
  return { budget, preparation, allowance, limit: Math.min(declared, allowance) }
}

/**
 * @param {string|undefined} sink the reporter sink directory
 * @returns {string[]} files an earlier session left: exports or overflow markers
 */
const staleSink = (sink) =>
  fs.readdirSync(sink).filter((name) => /^(exports-|OVERFLOW-)/.test(name))

/**
 * @param {string|undefined} archive the decoder archive
 * @returns {boolean} the archive exists with a verified `<archive>.json` receipt of its bytes
 */
const preparedDecoder = (archive) => {
  if (!exists(archive, 'isFile') || !exists(`${archive}.json`, 'isFile')) return false
  try {
    const receipt = JSON.parse(fs.readFileSync(`${archive}.json`, 'utf8'))
    return receipt.verified === true && receipt.archiveSha256 === asset(archive).sha256
  } catch {
    return false
  }
}

/**
 * Lists what the row's fixture lacks: the free disk its ledgers need, a valid job
 * deadline when one is set, the baseline checkout for a `previous` arm, and for a delivery job the
 * telemetry cluster with its reporter, a fresh reporter sink and the decoder prepared by
 * `scripts/e2e/telemetry/prepareDecoder.js`.
 * @param {string} name a row of `ROWS`
 * @param {Record<string, string|undefined>} env
 * @param {{ raw: string, statfs?: Function, now?: () => number }} deps `raw` is the row's evidence file
 * @returns {string[]} one line per missing part, empty when the fixture is complete
 */
const fixtureProblems = (name, env, { raw, statfs = fs.statfsSync, now = Date.now }) => {
  const { job: jobName } = rowOf(name)
  const job = JOBS[jobName]
  const checks = []
  const directory = path.dirname(raw)
  const { bavail, bsize } = statfs(directory)
  const free = bavail * bsize
  const need = diskBytes(jobName)
  checks.push([
    free >= need,
    `disk: ${free} bytes free under ${directory}, the session needs ${need}`,
  ])
  if (env.TELEMETRY_JOB_DEADLINE !== undefined) {
    const { allowance } = limits(jobName, env, now)
    checks.push([
      /^\d+$/.test(env.TELEMETRY_JOB_DEADLINE) && allowance > 0,
      `TELEMETRY_JOB_DEADLINE: the job deadline in epoch seconds, more than ${AFTER_SESSION_SECONDS} s ahead`,
    ])
  }
  if (job.roles.includes('previous'))
    checks.push([
      exists(env.TELEMETRY_PREVIOUS_ROOT, 'isDirectory'),
      `TELEMETRY_PREVIOUS_ROOT: an absolute checkout of ${ACCEPTED_BASELINE.ref}`,
    ])
  if (job.mode === 'delivery') {
    const sink = exists(env.TELEMETRY_EVIDENCE_DIR, 'isDirectory')
    const stale = sink ? staleSink(env.TELEMETRY_EVIDENCE_DIR) : []
    checks.push(
      [
        env.COMPOSE_FILE === 'docker-compose.telemetry.yml',
        'COMPOSE_FILE: docker-compose.telemetry.yml, the cluster with the telemetry reporter',
      ],
      [Boolean(env.COMPOSE_PROJECT_NAME), 'COMPOSE_PROJECT_NAME: the telemetry cluster project'],
      [LINES.includes(env.KAFKA_VERSION), `KAFKA_VERSION: one of ${LINES.join(', ')}`],
      [sink, 'TELEMETRY_EVIDENCE_DIR: the reporter sink directory'],
      [
        stale.length === 0,
        `TELEMETRY_EVIDENCE_DIR: a fresh reporter sink (it holds ${stale.join(', ')})`,
      ],
      [
        exists(env.TELEMETRY_JAVA_CACHE, 'isDirectory'),
        'TELEMETRY_JAVA_CACHE: the verified decoder cache',
      ],
      [
        preparedDecoder(env.TELEMETRY_DECODER_ARCHIVE),
        'TELEMETRY_DECODER_ARCHIVE: the archive prepareDecoder.js wrote, with its verified receipt beside it',
      ]
    )
  }
  return checks.filter(([ok]) => !ok).map(([, problem]) => problem)
}

/**
 * Lists a root's files the way the controller's source check walks them: names sorted per
 * directory, depth first, `.git` and `node_modules` skipped.
 * @param {string} root
 * @param {(bytes: Buffer) => string} [hash] the file digest, SHA-256 by default
 * @returns {{ path: string, sha256: string }[]}
 */
const catalogue = (root, hash = sha256) => {
  const files = []
  const visit = (relative) => {
    for (const name of fs.readdirSync(path.join(root, relative)).sort()) {
      if (name === '.git' || name === 'node_modules') continue
      const file = path.join(relative, name)
      if (fs.lstatSync(path.join(root, file)).isDirectory()) visit(file)
      else files.push({ path: file, sha256: hash(fs.readFileSync(path.join(root, file))) })
    }
  }
  visit('')
  return files
}

/** @param {Buffer} bytes @returns {string} the git blob id of `bytes` */
const blobId = (bytes) =>
  crypto.createHash('sha1').update(`blob ${bytes.length}\0`).update(bytes).digest('hex')

/**
 * Checks that the baseline checkout is the accepted commit: its HEAD, the accepted tag, and every
 * frozen file against the commit's blobs (a modified, missing or untracked file fails).
 * @param {{ checkout: string, frozen: string, accepted: typeof ACCEPTED_BASELINE, spawn: Function }} options
 * @returns {string[]} one line per difference, empty when the frozen copy is the accepted tree
 */
const baselineProblems = ({ checkout, frozen, accepted, spawn }) => {
  const git = (...args) => spawn('git', ['-C', checkout, ...args], { encoding: 'utf8' })
  const head = git('rev-parse', '--verify', 'HEAD^{commit}')
  if (head.status !== 0) return [`${checkout} is not a git checkout: ${tail(head.stderr)}`]
  const commit = head.stdout.trim()
  const problems = []
  if (commit !== accepted.commit)
    problems.push(
      `the checkout is at ${commit}, not the accepted ${accepted.ref} commit ${accepted.commit}`
    )
  const tag = git('rev-parse', '--verify', `refs/tags/${accepted.ref}^{commit}`)
  if (tag.status !== 0 || tag.stdout.trim() !== accepted.commit)
    problems.push(`tag ${accepted.ref} does not resolve to ${accepted.commit} in the checkout`)
  const listed = git('ls-tree', '-r', '-z', 'HEAD', '--', ...SNAPSHOT)
  if (listed.status !== 0) return [...problems, `git ls-tree failed: ${tail(listed.stderr)}`]
  const expected = new Map()
  for (const line of listed.stdout.split('\0').filter(Boolean)) {
    const [meta, file] = line.split('\t')
    const [mode, , id] = meta.split(' ')
    if (mode === '100644' || mode === '100755') expected.set(file, id)
    else problems.push(`${file} has git mode ${mode}, not a regular file`)
  }
  for (const { path: file, sha256: id } of catalogue(frozen, blobId)) {
    if (!expected.has(file)) problems.push(`${file} is not in the accepted commit`)
    else if (expected.get(file) !== id) problems.push(`${file} differs from the accepted commit`)
    expected.delete(file)
  }
  for (const file of expected.keys()) problems.push(`${file} is missing from the checkout`)
  return problems
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
 * Freezes the files a worker loads from `from` into `<snapshots>/<label>` with its archive, links
 * the checkout's `node_modules` for the workers, and writes the source receipt to `receipt`.
 * @param {string} from source root
 * @param {string} label `candidate` or `previous`
 * @param {{ snapshots: string, receipt: string, spawn: Function }} options
 * @returns {{ root: string, tree: string, source: object, tarball: object }} the arm's identity
 * @throws {Error} when the archive cannot be written
 */
const freeze = (from, label, { snapshots, receipt, spawn }) => {
  const root = path.join(snapshots, label)
  for (const entry of SNAPSHOT) copy(path.join(from, entry), path.join(root, entry))
  if (fs.existsSync(path.join(ROOT, 'node_modules')))
    fs.symlinkSync(path.join(ROOT, 'node_modules'), path.join(root, 'node_modules'))
  const tarball = path.join(snapshots, `${label}.tar`)
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
 * Builds the session manifest `telemetry.js --session` validates. A delivery manifest names the
 * reporter sink and the telemetry cluster the controller resets before every observation.
 * @param {{ row: object, raw: string, env: Record<string, string|undefined>, candidate: object,
 *   previous?: object, accepted?: object, zstd: boolean }} options
 * @returns {object}
 */
const sessionManifest = ({ row, raw, env, candidate, previous, accepted, zstd }) => {
  const job = JOBS[row.job]
  const optional = row.flags.includes('zstd') && !zstd
  return {
    schemaVersion: 1,
    job: row.job,
    runId: `tsess-${Date.now().toString(36)}`,
    directory: `${raw}.session`,
    driver: asset(TELEMETRY),
    session: asset(SESSION),
    consume: asset(path.join(candidate.root, 'scripts/benchmarks/consumeThroughput.js')),
    node: {
      binary: process.execPath,
      sha256: asset(process.execPath).sha256,
      version: process.version,
      platform: process.platform,
      arch: process.arch,
    },
    codecs: { none: true, gzip: true, zstd },
    ...(optional && { optionalCodec: 'zstd' }),
    ...(accepted && { acceptedBaseline: accepted }),
    ...(job.mode === 'delivery' && {
      fixture: {
        brokers: BROKERS,
        sink: env.TELEMETRY_EVIDENCE_DIR,
        compose: { file: path.join(ROOT, env.COMPOSE_FILE), project: env.COMPOSE_PROJECT_NAME },
      },
    }),
    arms: job.roles.map((label) => {
      const control = job.controls.includes(label) ? '1' : '0'
      const telemetry = job.telemetry[label] ? ['--telemetry', job.telemetry[label]] : []
      const argv = ['--mode', job.mode, '--control', control, ...row.flags, ...telemetry]
      const identity = label === 'previous' ? previous : candidate
      return { label, ...identity, argv, configSha256: digest({ mode: job.mode, argv }) }
    }),
    reference: job.reference,
    runs: job.runs,
    budgetSeconds: minimumBudget(row.job),
  }
}

/**
 * @param {object} result a controller run
 * @returns {{ status: number|null, signal: string|null, timedOut: boolean, verdict: object|null,
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
    timedOut: result.error?.code === 'ETIMEDOUT',
    verdict,
    ineffective: INEFFECTIVE.exec(result.stderr ?? '')?.[1] ?? null,
    stderr: tail(result.error?.message ?? result.stderr),
  }
}

/**
 * Copies the reporter sink into the row's evidence.
 * @param {string} sink the reporter sink directory
 * @param {string} to the evidence copy
 * @returns {{ files: string[], overflow: string[], error?: string }}
 */
const keepSink = (sink, to) => {
  try {
    copy(sink, to)
    const files = fs.readdirSync(to).sort()
    return { files, overflow: files.filter((name) => name.startsWith('OVERFLOW-')) }
  } catch (failure) {
    return { files: [], overflow: [], error: failure.message }
  }
}

/** @param {string} line a journal line @returns {object|null} its entry, null for a torn line */
const entryOf = (line) => {
  try {
    return JSON.parse(line)
  } catch {
    return null
  }
}

/**
 * Sums the reporter resets the controller journalled; a line torn by a cut-off session is skipped.
 * @param {string} directory the session directory
 * @returns {{ count: number, elapsedMs: number }}
 */
const resetsOf = (directory) => {
  const file = path.join(directory, 'journal.jsonl')
  const prepared = fs.existsSync(file)
    ? fs
        .readFileSync(file, 'utf8')
        .split('\n')
        .map(entryOf)
        .filter((entry) => entry?.disposition === 'prepared')
    : []
  return {
    count: prepared.length,
    elapsedMs: prepared.reduce((sum, { elapsedMs }) => sum + elapsedMs, 0),
  }
}

/**
 * Reads the session's raw observations without changing the file. Every complete line must parse.
 * A session the job cut off may end in an unterminated record its last append left; that fragment
 * is kept on disk, left out of the observations and counted in `tornBytes`.
 * @param {string} file the session's `raw.jsonl`
 * @param {boolean} cutOff whether the job cut the session off
 * @returns {{ observations: object[], tornBytes: number }}
 * @throws {SyntaxError} for a malformed complete line, or any unterminated one when not cut off
 */
const observationsOf = (file, cutOff) => {
  const lines = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n') : []
  const tail = lines.pop() ?? ''
  const torn = cutOff ? tail : ''
  if (!torn && tail) lines.push(tail)
  return {
    observations: lines.filter(Boolean).map((line) => JSON.parse(line)),
    tornBytes: Buffer.byteLength(torn),
  }
}

/** @param {string} dir a snapshot directory @returns {void} removes its roots' `node_modules` links */
const unlinkModules = (dir) => {
  for (const label of ['candidate', 'previous']) {
    const link = path.join(dir, label, 'node_modules')
    if (fs.lstatSync(link, { throwIfNoEntry: false })?.isSymbolicLink()) fs.unlinkSync(link)
  }
}

/**
 * @param {string[]} argv `--row <name> --raw <absolute file>`
 * @returns {{ row: object, raw: string }}
 * @throws {Error} for a missing or unknown row, a relative `--raw`, or another flag
 */
const parseArgs = (argv) => {
  const flags = {}
  for (let i = 0; i < argv.length; i += 2) {
    if (argv[i] !== '--row' && argv[i] !== '--raw') throw new Error(`unknown flag ${argv[i]}`)
    if (argv[i + 1] === undefined) throw new Error(`${argv[i]} needs a value`)
    flags[argv[i]] = argv[i + 1]
  }
  const row = rowOf(flags['--row'])
  if (!row) throw new Error(`--row must be one of ${ROWS.map(({ name }) => name).join('|')}`)
  if (!path.isAbsolute(flags['--raw'] ?? '')) throw new Error('--raw takes an absolute file')
  return { row, raw: flags['--raw'] }
}

/**
 * Runs one session for the report and prints its report line; outside the row's own workflow job
 * prints an omitted line instead and runs nothing.
 * @param {string[]} argv see `parseArgs`
 * @param {{ env?: object, spawn?: Function, log?: Function, error?: Function, root?: string,
 *   statfs?: Function, now?: () => number, zstd?: boolean, accepted?: object }} [deps]
 *   `root` is the candidate source, the checkout by default; `accepted` replaces `ACCEPTED_BASELINE`
 * @returns {number} 0 once the report or omitted line is printed, 1 on a failure, 2 on a usage
 *   error, a missing fixture or an unverified baseline
 */
const main = (
  argv,
  {
    env = process.env,
    spawn = spawnSync,
    log = console.log,
    error = console.error,
    root = ROOT,
    statfs = fs.statfsSync,
    now = Date.now,
    zstd = isSupported,
    accepted = ACCEPTED_BASELINE,
  } = {}
) => {
  let args
  try {
    args = parseArgs(argv)
  } catch (failure) {
    error(failure.message)
    return 2
  }
  const { row, raw } = args
  if (env.BENCH_TELEMETRY_ROW !== row.name) {
    log(JSON.stringify({ row: row.name, omitted: outside(row.name) }))
    return 0
  }
  const missing = fixtureProblems(row.name, env, { raw, statfs, now })
  if (missing.length) {
    log(JSON.stringify({ row: row.name, refused: 'fixture', missing }))
    return 2
  }
  const snapshots = `${raw}.snapshots`
  try {
    fs.mkdirSync(snapshots)
    const candidate = freeze(root, 'candidate', {
      snapshots,
      receipt: `${raw}.candidate-source.json`,
      spawn,
    })
    let previous
    let proof
    if (JOBS[row.job].roles.includes('previous')) {
      previous = freeze(env.TELEMETRY_PREVIOUS_ROOT, 'previous', {
        snapshots,
        receipt: `${raw}.previous-source.json`,
        spawn,
      })
      const missingBaseline = baselineProblems({
        checkout: env.TELEMETRY_PREVIOUS_ROOT,
        frozen: previous.root,
        accepted,
        spawn,
      })
      if (missingBaseline.length) {
        log(JSON.stringify({ row: row.name, refused: 'baseline', missing: missingBaseline }))
        return 2
      }
      const receipt = `${raw}.accepted-baseline.json`
      fs.writeFileSync(
        receipt,
        JSON.stringify({
          milestone: accepted.milestone,
          status: 'accepted',
          ref: accepted.ref,
          commit: accepted.commit,
          verified: 'HEAD, tag and every frozen file equal the accepted commit',
          tree: previous.tree,
          sourceSha256: previous.source.sha256,
          tarballSha256: previous.tarball.sha256,
        })
      )
      proof = asset(receipt)
    }
    const delivery = JOBS[row.job].mode === 'delivery'
    let decoder = null
    if (delivery) {
      fs.copyFileSync(`${env.TELEMETRY_DECODER_ARCHIVE}.json`, `${raw}.decoder.json`)
      decoder = { archiveSha256: asset(env.TELEMETRY_DECODER_ARCHIVE).sha256 }
    }
    const manifest = sessionManifest({ row, raw, env, candidate, previous, accepted: proof, zstd })
    const manifestFile = `${raw}.manifest.json`
    fs.writeFileSync(manifestFile, JSON.stringify(manifest, null, 2), { flag: 'wx' })
    const { budget, preparation, allowance, limit } = limits(row.job, env, now)
    const options = { cwd: ROOT, env, encoding: 'utf8', maxBuffer: 64 * MIB }
    const started = now()
    const ran = spawn(process.execPath, ['--expose-gc', TELEMETRY, '--session', manifestFile], {
      ...options,
      timeout: limit * 1000,
    })
    const elapsedSeconds = Math.round((now() - started) / 1000)
    error(ran.stderr ?? '')
    const session = { ...outcomeOf(ran), elapsedSeconds }
    let verify = null
    if (session.verdict) {
      const checked = spawn(process.execPath, [TELEMETRY, '--verify-session', manifest.directory], {
        ...options,
        timeout: VERIFY_SECONDS * 1000,
      })
      error(checked.stderr ?? '')
      verify = outcomeOf(checked)
    }
    const sink = delivery ? keepSink(env.TELEMETRY_EVIDENCE_DIR, `${raw}.sink`) : null
    const { observations, tornBytes } = observationsOf(
      path.join(manifest.directory, 'raw.jsonl'),
      session.timedOut
    )
    log(
      JSON.stringify({
        row: row.name,
        job: row.job,
        manifest: { path: manifestFile, sha256: asset(manifestFile).sha256 },
        declared: {
          observations: childCount(row.job),
          runs: manifest.runs,
          budgetSeconds: budget,
          preparationSeconds: preparation,
          allowanceSeconds: allowance,
          limitSeconds: limit,
        },
        resets: delivery ? resetsOf(manifest.directory) : null,
        baseline: proof ? { ref: accepted.ref, commit: accepted.commit } : null,
        decoder,
        node: process.version,
        session,
        verify,
        sink,
        observations,
        tornBytes,
      })
    )
    return 0
  } catch (failure) {
    error(failure.stack ?? String(failure))
    return 1
  } finally {
    unlinkModules(snapshots)
  }
}

/**
 * Reads the command's report line.
 * @param {{ status: number|null, signal?: string, error?: Error, stdout?: string, stderr?: string }} result
 * @returns {object} the report line, or the omitted line of a row run outside its own job
 * @throws {Error} for a refused fixture or baseline, a spawn error, a signal, a non-zero exit or
 *   no report line
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
  if (result.status === 0 && typeof line.omitted === 'string') return line
  if (line.refused === 'fixture')
    throw new Error(`the telemetry fixture is missing: ${line.missing.join('; ')}`)
  if (line.refused === 'baseline')
    throw new Error(`the accepted baseline is not verified: ${line.missing.join('; ')}`)
  if (result.status !== 0) throw new Error(`exit ${result.status}: ${tail(result.stderr)}`)
  if (!rowOf(line.row) || !Array.isArray(line.observations) || !line.session) {
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
  if (report.omitted && report.row === entry.name) return one('omitted', report.omitted, 0)
  if (report.row !== entry.name)
    return one('error', `the session ran row ${report.row}, expected ${entry.name}`)
  const job = JOBS[report.job]
  const { session, verify, observations, declared, sink, resets } = report
  const total = childCount(report.job)
  const notes = [`session manifest sha256 ${report.manifest.sha256}`]
  if (report.baseline)
    notes.push(
      `previous arm ${report.baseline.ref} at ${report.baseline.commit}, verified against the checkout's HEAD, tag and files`
    )
  if (report.decoder)
    notes.push(
      `decoder archive sha256 ${report.decoder.archiveSha256} prepared; the session does not read it`
    )
  const prepared = declared.preparationSeconds
    ? ` plus ${declared.preparationSeconds} s of reporter resets`
    : ''
  if (
    declared.allowanceSeconds !== null &&
    declared.allowanceSeconds < declared.budgetSeconds + declared.preparationSeconds
  )
    notes.push(
      `job allowance ${declared.allowanceSeconds} s is below the declared budget ${declared.budgetSeconds} s${prepared}`
    )
  notes.push(
    `session elapsed ${session.elapsedSeconds} s of its ${declared.limitSeconds} s limit, ${observations.length} of ${total} children completed`
  )
  if (resets)
    notes.push(
      `reporter reset before each observation, outside its measured window: ${resets.count} resets, ` +
        `${Math.round(resets.elapsedMs / 1000)} s in total (bound ${PREPARE_SECONDS} s each, declared apart from the session budget)`
    )
  const overflow = sink?.overflow.length
    ? `; reporter sink overflow: ${sink.overflow.join(', ')}`
    : ''

  if (session.timedOut) {
    const resetBound = declared.preparationSeconds
      ? `, reporter resets ${declared.preparationSeconds} s`
      : ''
    const allowance =
      declared.allowanceSeconds === null ? '' : `, job allowance ${declared.allowanceSeconds} s`
    const resetTime = resets
      ? `; ${resets.count} reporter resets took ${Math.round(resets.elapsedMs / 1000)} s`
      : ''
    const torn = report.tornBytes
      ? `; its raw evidence ends in an incomplete record of ${report.tornBytes} bytes`
      : ''
    return one(
      'error',
      `the session was cut off at its ${declared.limitSeconds} s limit after ${session.elapsedSeconds} s with ` +
        `${observations.length} of ${total} children completed (declared budget ${declared.budgetSeconds} s${resetBound}${allowance}); ` +
        `the measurement stays owed and the partial evidence is kept${resetTime}${overflow}${torn}`
    )
  }
  if (!session.verdict) {
    if (session.status === 4)
      return one('error', `ZSTD is not available on Node ${report.node}: the row has no numbers`)
    if (!session.ineffective) {
      return one(
        'error',
        `the session failed (exit ${session.status ?? session.signal}): ${session.stderr}${overflow}`
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
  if (sink)
    controls.push({
      arm: 'session',
      name: 'reporter sink without overflow',
      passed: !sink.overflow.length && !sink.error,
      detail: sink.error ?? `${sink.files.length} sink files${overflow}`,
    })
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
module.exports = {
  runs: 1,
  timeoutMs,
  read,
  rows,
  main,
  fixtureProblems,
  outside,
  diskBytes,
  manifestEntries,
  ROWS,
  ACCEPTED_BASELINE,
}

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
