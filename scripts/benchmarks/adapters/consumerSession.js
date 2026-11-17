/**
 * Consumer measurement sessions: the inventory of the consumer rows of the benchmark report, the
 * controller that runs one row, and the report adapter of the row that audits all of them.
 *
 * A row runs its real driver as interleaved arms: every tree the row names (`head`, this
 * checkout, and the archived `alpha.9c` and `alpha.9b` trees from `BENCH_TREES`) times every
 * configuration of the row, each with an A/A copy (same tree, same configuration). Every round
 * runs every arm once, the order moving one place per round; the first round is discarded and the
 * next seven are measured. A driver that loads its tree by path (`placement: 'head'`) runs from
 * this checkout with `--tree <dir>`; any other driver runs as the copy inside each tree, whose
 * SHA-256 is printed beside the tree's commit so identical comparison drivers can be checked.
 *
 * Usage: node scripts/benchmarks/adapters/consumerSession.js --row <name> --raw <file>
 *        node scripts/benchmarks/adapters/consumerSession.js --audit --raw <file>
 * `--raw` is the JSONL file of the session's observations; profiles and evidence go beside it.
 * `BENCH_TREES` is a JSON object of archived tree directories by label. Prints one JSON line:
 * the session (`row`, `contracts`, `trees`, `arms`, `aa`, `controls`, `warmup`, `measured`,
 * `profiles`, `selfTime`, `nodes`) or `{ row, error }` when the row cannot start; exit 0 once a
 * line is printed, 2 for a bad argument.
 */
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawn, execFileSync } = require('node:child_process')
const { median } = require('../session')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const HEAD = 'head'
const FROZEN = ['alpha.9c', 'alpha.9b']
const ALL_TREES = [HEAD, ...FROZEN]
const AA = '~AA'
const AA_LIMIT_PERCENT = 3
const WARMUP = 1
const MEASURED = 7
const MIB = 1024 * 1024
const MINUTE = 60 * 1000
const MAX_OUTPUT = 16 * MIB
const CONTROL_INLINE = 64 * 1024

/**
 * @typedef {{ name: string, args: string[] }} Config
 * @typedef {{ key: string, unit: string }} Metric
 * @typedef {object} Row
 * @property {string[]} contracts the inherited contract ids the row answers
 * @property {string} adapter the module in `adapters/` that reads the row
 * @property {string} driver the driver under `scripts/benchmarks/`
 * @property {'head'|'tree'} placement `head`: this checkout's driver with `--tree <dir>`;
 *   `tree`: the driver copy inside each tree
 * @property {string[]} node node flags
 * @property {string[]} args driver arguments shared by every configuration
 * @property {Config[]} configs the configurations, each an arm per tree
 * @property {string[]} trees tree labels, `head` first
 * @property {Metric[]} metrics what the row shows
 * @property {string[]} aaOn the metrics the A/A rule judges
 * @property {string[]} controls the names of the controls its adapter checks
 * @property {string[]} artifacts what its raw evidence must hold
 * @property {boolean} broker whether it needs the cluster
 * @property {number} timeoutMs the limit of one run
 * @property {number} [warmup] discarded rounds, default 1
 * @property {number} [measured] measured rounds, default 7
 * @property {boolean} [aa] false only for a row outside the inherited contracts
 * @property {number[]} [exitCodes] the exits read as a finished run, default [0]
 * @property {string[]} [control] arguments of an untimed run per tree before the session
 * @property {boolean} [profile] archive a CPU profile of every run
 */

/** @param {string[]} args @returns {Config[]} one configuration named `default` */
const only = (args = []) => [{ name: 'default', args }]

const consume = (args, extra = {}) => ({
  adapter: 'consumeThroughput',
  driver: 'consumeThroughput.js',
  placement: 'tree',
  node: ['--expose-gc'],
  args,
  configs: only(),
  trees: ALL_TREES,
  metrics: [
    { key: 'messagesPerSecond', unit: 'msg/s' },
    { key: 'deliveryGapP99', unit: 'ms' },
    { key: 'heapRetainedBytes', unit: 'bytes' },
  ],
  aaOn: ['messagesPerSecond'],
  controls: ['handler entries', 'delivered messages'],
  artifacts: ['observations'],
  broker: true,
  timeoutMs: 15 * MINUTE,
  ...extra,
})

const sizing = (bytes, mib) => ({
  contracts: [`sizing/max-bytes-${mib}mib`],
  adapter: 'consumeSizing',
  driver: 'consumeThroughput.js',
  placement: 'tree',
  node: ['--expose-gc'],
  args: ['--partitions', '1000', '--max-bytes', String(bytes), '--rss'],
  configs: only(),
  trees: ALL_TREES,
  metrics: [
    { key: 'sizing.rssPeakBytes', unit: 'bytes' },
    { key: 'sizing.rssProcessPeakBytes', unit: 'bytes' },
    { key: 'sizing.heapRetainedBytes', unit: 'bytes' },
  ],
  aaOn: ['sizing.rssPeakBytes'],
  controls: ['every partition received data'],
  artifacts: ['observations'],
  broker: true,
  timeoutMs: 20 * MINUTE,
})

const sessions = (arm, contract) => ({
  contracts: [contract],
  adapter: 'fetchSessions',
  driver: 'fetchSessions.js',
  placement: 'tree',
  node: ['--expose-gc'],
  args: ['--arm', arm === 'Memory' ? 'L2' : arm],
  configs: [
    { name: 'sessions-false', args: ['--sessions', 'false'] },
    { name: 'sessions-true', args: ['--sessions', 'true'] },
  ],
  trees: ALL_TREES,
  metrics:
    arm === 'Memory'
      ? [{ key: 'sample.heapRetainedBytes', unit: 'bytes' }]
      : [
          { key: 'sample.bytes', unit: 'bytes' },
          { key: 'sample.bytesPerSecond', unit: 'bytes/s' },
          { key: 'sample.cpuMicrosPerSecond', unit: 'us/s' },
          ...(['S1', 'L1', 'L2'].includes(arm)
            ? [
                { key: 'sample.messagesPerSecond', unit: 'msg/s' },
                { key: 'sample.cpuMicrosPerMessage', unit: 'us' },
                { key: 'sample.bytesPerMessage', unit: 'bytes' },
              ]
            : []),
          ...(['S1', 'L1'].includes(arm) ? [{ key: 'sample.deliveryGapP99', unit: 'ms' }] : []),
          { key: 'sample.heapRetainedBytes', unit: 'bytes' },
        ],
  aaOn: [
    arm === 'Memory'
      ? 'sample.heapRetainedBytes'
      : ['S1', 'L1'].includes(arm)
        ? 'sample.messagesPerSecond'
        : 'sample.bytesPerSecond',
  ],
  controls: [
    'BENCH_NODES from the started containers',
    'full-cycle byte lower bound',
    'idle incremental bound',
    ...(arm === 'Memory' ? ['retained-memory positive control'] : []),
  ],
  artifacts: ['observations', 'frame journals'],
  broker: true,
  timeoutMs: 10 * MINUTE,
  control: ['--control'],
})

/** Every consumer row of the report by its manifest name. */
const ROWS = {
  'consumer-fetchRound': {
    contracts: ['performance/fetch-round'],
    adapter: 'fetchRound',
    driver: 'fetchRound.js',
    placement: 'head',
    node: ['--expose-gc', '--max-semi-space-size=256'],
    args: [],
    configs: [100, 1000, 5000].map((p) => ({ name: `p${p}`, args: ['--partitions', String(p)] })),
    trees: ALL_TREES,
    metrics: [
      { key: 'nsPerNodeRound', unit: 'ns' },
      { key: 'allocatedBytesPerRound', unit: 'bytes' },
      { key: 'retainedHeapDelta', unit: 'bytes' },
    ],
    aaOn: ['nsPerNodeRound'],
    controls: ['metadata lookups = topics x nodes', 'batches built = partitions', 'no-GC window'],
    artifacts: ['observations'],
    broker: false,
    timeoutMs: 5 * MINUTE,
  },
  'consumer-eachBatch': consume(['--handler', 'eachBatch', '--max-bytes-per-partition', '32768'], {
    contracts: ['performance/consume-each-batch'],
  }),
  'consumer-eachMessage': consume(['--handler', 'eachMessage', '--messages', '200001'], {
    contracts: ['performance/consume-each-message'],
  }),
  'consumer-autocommit': consume(
    ['--handler', 'eachMessage', '--messages', '200001', '--autocommit-threshold', '100'],
    { contracts: ['performance/consume-autocommit'] }
  ),
  'consumer-listener': consume(['--handler', 'eachBatch', '--max-bytes-per-partition', '32768'], {
    contracts: ['performance/consume-listener'],
    configs: [
      { name: 'listener-0', args: ['--listener', '0'] },
      { name: 'listener-1', args: ['--listener', '1'] },
    ],
    controls: ['handler entries', 'delivered messages', 'listener hooks once each'],
  }),
  'consumer-partitions-1000': consume(['--partitions', '1000'], {
    contracts: ['performance/consume-partitions-1000'],
    controls: ['handler entries', 'delivered messages', '1000 partitions'],
  }),
  'consumer-sessions-S1': sessions('S1', 'performance/fetch-sessions-s1'),
  'consumer-sessions-S2': sessions('S2', 'performance/fetch-sessions-s2'),
  'consumer-sessions-L1': sessions('L1', 'performance/fetch-sessions-l1'),
  'consumer-sessions-L2': sessions('L2', 'performance/fetch-sessions-l2'),
  'consumer-sessions-L3': sessions('L3', 'performance/fetch-sessions-l3'),
  'consumer-sessions-Memory': sessions('Memory', 'performance/fetch-sessions-memory'),
  'consumer-subscriptionResolve': {
    contracts: ['performance/subscription-resolve'],
    adapter: 'subscriptionResolve',
    driver: 'subscriptionResolve.js',
    placement: 'head',
    node: [],
    args: ['--iterations', '100'],
    configs: only(),
    trees: ALL_TREES,
    metrics: [{ key: 'evaluationMs', unit: 'ms' }],
    aaOn: ['evaluationMs'],
    controls: ['match count = generated expectation'],
    artifacts: ['observations'],
    broker: false,
    timeoutMs: 5 * MINUTE,
  },
  'consumer-stickyAssign-g6': {
    contracts: ['performance/sticky-assign-g6'],
    adapter: 'stickyAssign',
    driver: 'stickyAssign.js',
    placement: 'head',
    node: [],
    args: ['--worker', 'time', '--shape', 'stag-500+500x30k'],
    configs: only(),
    trees: ALL_TREES,
    metrics: [{ key: 'ms', unit: 'ms' }],
    aaOn: ['ms'],
    controls: ['assignment identical across trees', 'CPU profile per run'],
    artifacts: ['observations', 'CPU profiles'],
    broker: false,
    timeoutMs: 10 * MINUTE,
    control: ['--worker', 'bytes', '--shape', 'stag-500+500x30k'],
    profile: true,
  },
  'consumer-recordBatchDecode': {
    contracts: ['performance/record-batch-decode-no-change'],
    adapter: 'recordBatchDecode',
    driver: 'recordBatchDecode.js',
    placement: 'tree',
    node: [],
    args: [],
    configs: ['uncompressed', 'gzip', 'snappy'].map((arm) => ({ name: arm, args: ['--arm', arm] })),
    trees: ALL_TREES,
    metrics: [{ key: 'nsPerOp', unit: 'ns' }],
    aaOn: ['nsPerOp'],
    controls: ['decoded records equal across trees'],
    artifacts: ['observations'],
    broker: false,
    timeoutMs: 5 * MINUTE,
  },
  'consumer-sizing-10mib': sizing(10485760, 10),
  'consumer-sizing-25mib': sizing(26214400, 25),
  'consumer-sizing-50mib': sizing(52428800, 50),
  'consumer-rebalanceStorm-200': {
    contracts: [],
    adapter: 'rebalanceStorm',
    driver: 'rebalanceStorm.js',
    placement: 'tree',
    node: ['--expose-gc'],
    args: ['--rounds', '200'],
    configs: only(),
    trees: [HEAD],
    metrics: [
      { key: 'deltaBytes', unit: 'bytes' },
      { key: 'slopeBytesPerRound', unit: 'bytes/round' },
    ],
    aaOn: [],
    controls: ['200 cooperative rounds', 'five retained-heap samples'],
    artifacts: ['observations'],
    broker: true,
    timeoutMs: 60 * MINUTE,
    warmup: 0,
    measured: 1,
    aa: false,
    exitCodes: [0, 1],
  },
  'consumer-offsetCommit-retirement': consume(
    ['--handler', 'eachMessage', '--messages', '200001', '--autocommit-threshold', '100'],
    { contracts: [], adapter: 'offsetCommit', trees: [HEAD, 'before-retirement'] }
  ),
}

/** The audit row: the A/A arms, the discarded warm-ups and the frozen repeat of every row. */
const AUDIT = {
  name: 'consumer-session-audit',
  contracts: ['performance/controls-and-aa', 'performance/frozen-repetition'],
}

/**
 * @param {Row} row
 * @returns {{ label: string, tree: string, config: string, copy: boolean }[]} every arm of the
 *   row, each followed by its A/A copy unless the row has none
 */
const armsOf = (row) =>
  row.trees.flatMap((tree) =>
    row.configs.flatMap(({ name }) => {
      const label = row.configs.length === 1 ? tree : `${tree}/${name}`
      const arm = { label, tree, config: name, copy: false }
      return row.aa === false ? [arm] : [arm, { ...arm, label: label + AA, copy: true }]
    })
  )

/**
 * @returns {object[]} the manifest entries of the consumer rows and the audit row, in run order
 */
const manifestEntries = () => [
  ...Object.entries(ROWS).map(([name, row]) => ({
    name,
    adapter: row.adapter,
    contracts: row.contracts,
    command: ['scripts/benchmarks/adapters/consumerSession.js', '--row', name, '--raw', '{raw}'],
    broker: row.broker,
    metrics: row.metrics,
    arms: ['fork'],
  })),
  {
    name: AUDIT.name,
    adapter: 'consumerSession',
    contracts: AUDIT.contracts,
    command: ['scripts/benchmarks/adapters/consumerSession.js', '--audit', '--raw', '{raw}'],
    broker: false,
    metrics: [{ key: 'aaSpreadPercent', unit: '%' }],
    arms: ['fork'],
  },
]

/**
 * @param {object} obj
 * @param {string} dotted
 * @returns {unknown} the value at a dotted path, own keys only
 */
const at = (obj, dotted) =>
  dotted
    .split('.')
    .reduce(
      (value, key) =>
        value !== null && typeof value === 'object' && Object.hasOwn(value, key)
          ? value[key]
          : undefined,
      obj
    )

/**
 * @param {string} file
 * @returns {string|null} the SHA-256 of the file, or null when it does not exist
 */
const digest = (file) =>
  fs.existsSync(file)
    ? crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')
    : null

/**
 * @param {string} dir
 * @returns {string|null} the commit checked out in `dir`, or null
 */
const commitOf = (dir) => {
  const head = path.join(dir, '.git')
  try {
    return fs.existsSync(head)
      ? execFileSync('git', ['-C', dir, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
      : null
  } catch {
    return null
  }
}

/**
 * @param {string|undefined} text `BENCH_TREES`
 * @returns {Record<string, string>} archived tree directories by label
 * @throws {Error} when it is not a JSON object of absolute directories
 */
const parseTrees = (text) => {
  if (!text) return {}
  const trees = JSON.parse(text)
  if (trees === null || typeof trees !== 'object' || Array.isArray(trees)) {
    throw new Error('BENCH_TREES is not a JSON object of tree directories')
  }
  for (const [label, dir] of Object.entries(trees)) {
    if (typeof dir !== 'string' || !path.isAbsolute(dir)) {
      throw new Error(`BENCH_TREES ${label} is not an absolute directory`)
    }
  }
  return trees
}

/**
 * Describes each tree of a row: its directory, commit, the driver it runs and that driver's
 * SHA-256; a tree `BENCH_TREES` does not name, or whose driver is missing, carries `error`.
 * @param {Row} row
 * @param {Record<string, string>} dirs archived trees by label
 * @param {(dir: string) => string|null} commit reads a tree's commit
 * @returns {Record<string, { dir?: string, sha?: string|null, driver?: string,
 *   driverDigest?: string|null, error?: string }>}
 */
const describeTrees = (row, dirs, commit) =>
  Object.fromEntries(
    row.trees.map((label) => {
      const dir = label === HEAD ? ROOT : dirs[label]
      if (!dir) return [label, { error: `BENCH_TREES names no ${label} tree` }]
      const base = row.placement === 'head' ? ROOT : dir
      const driver = path.join(base, 'scripts', 'benchmarks', row.driver)
      const driverDigest = digest(driver)
      const tree = { dir, sha: commit(dir), driver, driverDigest }
      return [label, driverDigest ? tree : { ...tree, error: `no driver ${driver}` }]
    })
  )

/**
 * @param {string} stdout
 * @returns {object} the last line that parses as a JSON object
 * @throws {Error} when none does
 */
const lastJson = (stdout) => {
  const lines = String(stdout).trim().split('\n').reverse()
  for (const line of lines) {
    try {
      const value = JSON.parse(line)
      if (value && typeof value === 'object' && !Array.isArray(value)) return value
    } catch {
      // A log line.
    }
  }
  throw new Error('no JSON line')
}

/**
 * Runs one driver process to its end.
 * @param {string[]} args node arguments
 * @param {{ cwd: string, env: object, timeoutMs: number, onMessage?: Function }} options
 * @returns {Promise<{ status: number|null, signal: string|null, stdout: string, stderr: string,
 *   error?: Error }>}
 */
const runProcess = (args, { cwd, env, timeoutMs, onMessage }) =>
  new Promise((resolve) => {
    const child = spawn(process.execPath, args, {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe', ...(onMessage ? ['ipc'] : [])],
    })
    const out = { stdout: '', stderr: '' }
    const append = (key) => (chunk) => {
      if (out[key].length < MAX_OUTPUT) out[key] += chunk
    }
    child.stdout.setEncoding('utf8').on('data', append('stdout'))
    child.stderr.setEncoding('utf8').on('data', append('stderr'))
    if (onMessage) child.on('message', (message) => onMessage(message, child))
    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs)
    child.on('error', (error) => {
      clearTimeout(timer)
      resolve({ status: null, signal: null, ...out, error })
    })
    child.on('close', (status, signal) => {
      clearTimeout(timer)
      resolve({ status, signal, ...out })
    })
  })

/**
 * @param {{ status: number|null, signal: string|null, stdout: string, stderr: string,
 *   error?: Error }} result
 * @param {number[]} exitCodes
 * @returns {{ result?: object, error?: string }} the run's JSON line, or why there is none
 */
const readRun = (result, exitCodes) => {
  const stderr = result.stderr.replace(/\s+/g, ' ').trim().slice(-300)
  if (result.error) return { error: result.error.message }
  if (result.status === null) return { error: `killed by ${result.signal}` }
  if (!exitCodes.includes(result.status)) {
    return { error: [`exit ${result.status}`, stderr].filter(Boolean).join(': ') }
  }
  try {
    return { result: { ...lastJson(result.stdout), exitCode: result.status } }
  } catch {
    return { error: `exit ${result.status}: no JSON line${stderr ? `: ${stderr}` : ''}` }
  }
}

/**
 * Sums the self time of each function over CPU profiles.
 * @param {string[]} files `.cpuprofile` files
 * @param {number} [top] how many functions to keep
 * @returns {{ fn: string, ms: number }[]} the functions with the most self time
 */
const selfTimes = (files, top = 15) => {
  const totals = new Map()
  for (const file of files) {
    const { nodes, samples, timeDeltas } = JSON.parse(fs.readFileSync(file, 'utf8'))
    const byId = new Map(nodes.map((node) => [node.id, node]))
    samples.forEach((id, i) => {
      const { functionName, url, lineNumber } = byId.get(id).callFrame
      const fn = `${functionName || '(anonymous)'} ${path.basename(url || '')}:${lineNumber + 1}`
      totals.set(fn, (totals.get(fn) ?? 0) + (timeDeltas[i] ?? 0) / 1000)
    })
  }
  return [...totals]
    .map(([fn, ms]) => ({ fn, ms: Math.round(ms * 1000) / 1000 }))
    .sort((a, b) => b.ms - a.ms)
    .slice(0, top)
}

/**
 * @param {number[]} values
 * @returns {{ median: number, min: number, max: number, n: number }}
 */
const stat = (values) => ({
  median: median(values),
  min: Math.min(...values),
  max: Math.max(...values),
  n: values.length,
})

/**
 * Runs one row's session and returns its line. A row that cannot start (unknown name, an
 * unreadable `BENCH_TREES`, a failed preparation) returns `{ row, error }`.
 * @param {string} name the row
 * @param {string} raw the observations file
 * @param {object} [deps]
 * @param {Function} [deps.run] `runProcess`, replaceable in tests
 * @param {object} [deps.env] the environment, `process.env` by default
 * @param {object} [deps.hooks] the row adapter's `prepare`, replaceable in tests
 * @param {(dir: string) => string|null} [deps.commit] reads a tree's commit
 * @returns {Promise<object>} the session line
 */
const runRow = async (
  name,
  raw,
  { run = runProcess, env = process.env, hooks, commit = commitOf } = {}
) => {
  const row = Object.hasOwn(ROWS, name) ? ROWS[name] : undefined
  if (!row) return { row: name, error: `no consumer row ${name}` }
  const dir = path.dirname(raw)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(raw, '')
  let trees
  try {
    trees = describeTrees(row, parseTrees(env.BENCH_TREES), commit)
  } catch (error) {
    return { row: name, contracts: row.contracts, error: error.message }
  }
  const { prepare } = hooks ?? require(`./${row.adapter}`)
  let prepared = {}
  try {
    if (prepare) prepared = (await prepare({ row, name, dir, env })) ?? {}
  } catch (error) {
    return { row: name, contracts: row.contracts, trees, error: error.message }
  }
  const warmup = row.warmup ?? WARMUP
  const measured = row.measured ?? MEASURED
  const exitCodes = row.exitCodes ?? [0]
  try {
    const argsFor = (arm, extra, { timed = true, profileDir } = {}) => {
      const { dir: treeDir } = trees[arm.tree]
      const config = row.configs.find(({ name: c }) => c === arm.config)
      return {
        cwd: row.placement === 'head' ? ROOT : treeDir,
        args: [
          ...row.node,
          ...(profileDir ? ['--cpu-prof', `--cpu-prof-dir=${profileDir}`] : []),
          trees[arm.tree].driver,
          ...extra(config),
          ...(timed ? (prepared.args ?? []) : []),
          ...(row.placement === 'head' ? ['--tree', treeDir] : []),
        ],
      }
    }
    const runEnv = { ...env, ...(prepared.env ?? {}) }
    const controls = {}
    for (const [label, tree] of Object.entries(trees)) {
      if (!row.control || tree.error) continue
      const { cwd, args } = argsFor({ tree: label }, () => row.control, { timed: false })
      const outcome = readRun(await run(args, { cwd, env: runEnv, timeoutMs: row.timeoutMs }), [0])
      if (outcome.error) {
        controls[label] = { error: outcome.error }
        continue
      }
      const text = JSON.stringify(outcome.result)
      const file = path.join(dir, `control-${label}.json`)
      fs.writeFileSync(file, text)
      const hash = crypto.createHash('sha256').update(text).digest('hex')
      controls[label] = { digest: hash, file, ...(text.length <= CONTROL_INLINE && outcome.result) }
    }
    const arms = armsOf(row)
    const state = Object.fromEntries(arms.map((arm) => [arm.label, { results: [], errors: [] }]))
    const profiles = {}
    for (let round = 0; round < warmup + measured; round++) {
      const order = arms.map((_, i) => arms[(i + round) % arms.length])
      for (const [position, arm] of order.entries()) {
        const entry = { round, position, arm: arm.label, warmup: round < warmup }
        const armState = state[arm.label]
        if (trees[arm.tree].error) {
          entry.error = trees[arm.tree].error
        } else {
          const profileDir = row.profile
            ? path.join(dir, 'cpu-profiles', arm.label, `round-${round}`)
            : undefined
          if (profileDir) fs.rmSync(profileDir, { recursive: true, force: true })
          const config = (c) => [...row.args, ...c.args]
          const { cwd, args } = argsFor(arm, config, { profileDir })
          const outcome = readRun(
            await run(args, {
              cwd,
              env: runEnv,
              timeoutMs: row.timeoutMs,
              onMessage: prepared.onMessage,
            }),
            exitCodes
          )
          Object.assign(entry, outcome)
          if (profileDir && fs.existsSync(profileDir)) {
            const files = fs.readdirSync(profileDir).map((file) => path.join(profileDir, file))
            ;(profiles[arm.label] ??= []).push(...files.filter((f) => f.endsWith('.cpuprofile')))
          }
        }
        fs.appendFileSync(raw, `${JSON.stringify(entry)}\n`)
        if (entry.warmup) continue
        if (entry.error) armState.errors.push(`round ${round}: ${entry.error}`)
        else armState.results.push(entry.result)
      }
    }
    return summarize(row, name, { trees, state, controls, profiles, warmup, measured, prepared })
  } finally {
    await prepared.cleanup?.()
  }
}

/**
 * Reduces a row's measured runs to its session line.
 * @param {Row} row
 * @param {string} name
 * @param {object} session the trees, per-arm results and errors, untimed controls and profiles
 * @returns {object} the session line
 */
const summarize = (row, name, { trees, state, controls, profiles, warmup, measured, prepared }) => {
  const arms = {}
  for (const arm of armsOf(row)) {
    const { results, errors } = state[arm.label]
    const stats = {}
    for (const { key } of row.metrics) {
      const values = results.map((result) => at(result, key))
      if (values.length && values.every(Number.isFinite)) stats[key] = stat(values)
    }
    arms[arm.label] = {
      tree: arm.tree,
      config: arm.config,
      measured: results.length,
      errors,
      stats,
      results,
    }
  }
  const aa = {}
  for (const arm of armsOf(row).filter(({ copy }) => copy)) {
    const against = arm.label.slice(0, -AA.length)
    const percent = {}
    for (const key of row.aaOn) {
      const reference = arms[against].stats[key]?.median
      const copy = arms[arm.label].stats[key]?.median
      if (Number.isFinite(reference) && Number.isFinite(copy) && reference !== 0) {
        percent[key] = (copy / reference - 1) * 100
      }
    }
    aa[arm.label] = { against, percent }
  }
  const selfTime = Object.fromEntries(
    Object.entries(profiles).map(([label, files]) => [label, selfTimes(files)])
  )
  return {
    row: name,
    contracts: row.contracts,
    driver: row.driver,
    trees,
    warmup,
    measured,
    arms,
    aa,
    controls,
    profiles: Object.fromEntries(Object.entries(profiles).map(([l, f]) => [l, f.length])),
    selfTime,
    ...(prepared.nodes && { nodes: prepared.nodes }),
  }
}

/**
 * Reads one row's session run.
 * @param {object} result what `spawnSync` returned
 * @param {import('../report').ManifestEntry} entry
 * @returns {object} the session line
 * @throws {Error} for a failed run, no JSON line, a line of another row, or a row that could not
 *   start (its reason)
 */
const readSessionRun = (result, entry) => {
  if (result.error) throw result.error
  if (result.status !== 0) throw new Error(`exit ${result.status ?? result.signal}`)
  const line = lastJson(result.stdout ?? '')
  if (line.row !== entry.name) throw new Error(`the session line is of row ${line.row}`)
  if (line.error) throw new Error(line.error)
  if (!line.arms || !line.trees) throw new Error('the last JSON line is not a session')
  return line
}

/**
 * @typedef {{ arm: string, name: string, passed: boolean, detail?: string }} Control
 * @typedef {object} RowHooks what a driver's adapter adds to the shared session rules
 * @property {(session: object, row: Row) => Control[]} [controls] the driver's own controls
 * @property {(session: object, row: Row) => string|undefined} [inconclusive] why the row is
 *   inconclusive, if it is
 * @property {(result: object) => string} [corpus] the workload identity of one run
 * @property {(context: object) => Promise<object>} [prepare] run once before the session:
 *   returns `args`, `env`, `nodes`, `onMessage` and `cleanup` for the runs
 * @property {number} [timeoutMs] the limit of the whole session
 */

/**
 * Builds the report adapter of a consumer row. The row's status: `error` when the session did
 * not run or an arm of this tree failed; `invalid` when a control failed or an A/A copy differs
 * from its arm by more than 3 % on a judged metric; `inconclusive` for the driver's reason
 * (such as too few samples); otherwise `valid`. Each arm's rows carry their median and range; an
 * archived arm that failed is an `error` row with its reason beside the others.
 * @param {RowHooks} [hooks]
 * @returns {import('../report').Adapter & { prepare?: Function }}
 */
const rowAdapter = ({
  controls = () => [],
  inconclusive = () => undefined,
  corpus,
  prepare,
  timeoutMs = 330 * MINUTE,
} = {}) => ({
  runs: 1,
  timeoutMs,
  prepare,
  read: readSessionRun,
  rows: (entry, arm, { outputs, error }) => {
    const one = (status, reason) => ({
      rows: [{ benchmark: entry.name, arm: '-', metric: '-', unit: '-', runs: 0, status, reason }],
    })
    if (error) return one('error', error)
    const row = Object.hasOwn(ROWS, entry.name) ? ROWS[entry.name] : undefined
    if (!row) return one('error', `no consumer row ${entry.name}`)
    const session = outputs[0]
    const arms = armsOf(row)
    const missing = arms.find(({ label }) => !session.arms[label])
    if (missing) return one('error', `the session has no arm ${missing.label}`)

    const head = session.trees[HEAD] ?? {}
    const checks = []
    for (const [label, tree] of Object.entries(session.trees)) {
      if (tree.error) continue
      checks.push({
        arm: label,
        name: 'tree SHA and driver digest recorded',
        passed: Boolean(tree.sha) && Boolean(tree.driverDigest),
        detail: `${tree.sha ?? 'no SHA'}, driver ${tree.driverDigest ?? 'missing'}`,
      })
      if (label !== HEAD) {
        checks.push({
          arm: label,
          name: 'identical comparison driver',
          passed: tree.driverDigest === head.driverDigest,
          detail: `${tree.driverDigest} vs ${head.driverDigest}`,
        })
      }
    }
    for (const [label, { against, percent }] of Object.entries(session.aa)) {
      if (session.arms[label].errors.length || session.arms[against].errors.length) continue
      const spreads = row.aaOn.map((key) => [key, percent[key]])
      checks.push({
        arm: label,
        name: `A/A within ${AA_LIMIT_PERCENT} %`,
        passed: spreads.every(([, p]) => Number.isFinite(p) && Math.abs(p) <= AA_LIMIT_PERCENT),
        detail: spreads
          .map(([key, p]) => `${key} ${Number.isFinite(p) ? p.toFixed(2) : 'undefined'} %`)
          .join(', '),
      })
    }
    if (corpus) {
      const byConfig = new Map()
      for (const { config, results } of Object.values(session.arms)) {
        const set = byConfig.get(config) ?? new Set()
        for (const result of results) set.add(corpus(result))
        byConfig.set(config, set)
      }
      for (const [config, identities] of byConfig) {
        checks.push({
          arm: config,
          name: 'identical corpus on every tree',
          passed: identities.size === 1,
          detail: [...identities].join(' | ') || 'no measured run',
        })
      }
    }
    checks.push(...controls(session, row))

    const headFailure = arms
      .filter(({ tree }) => tree === HEAD)
      .map(({ label }) => [label, session.arms[label]])
      .find(([, a]) => a.errors.length || a.measured !== session.measured)
    const failed = checks.filter(({ passed }) => !passed)
    let status = 'valid'
    let reason
    if (headFailure) {
      const [label, a] = headFailure
      status = 'error'
      reason = `${label}: ${a.errors[0] ?? `${a.measured} of ${session.measured} measured runs`}`
    } else if (failed.length) {
      status = 'invalid'
      reason = `failed controls: ${failed.map(({ arm: a, name }) => `${a} ${name}`).join('; ')}`
    } else {
      reason = inconclusive(session, row)
      if (reason) status = 'inconclusive'
    }
    const notes = [
      `contracts: ${row.contracts.join(', ') || 'none (additional row)'}`,
      `driver ${row.driver}; ${session.warmup} discarded and ${session.measured} measured rounds`,
      ...row.configs.map(
        ({ name, args }) =>
          `workload ${name}: ${[...row.node, row.driver, ...row.args, ...args].join(' ')}`
      ),
      ...Object.entries(session.trees).map(
        ([label, t]) =>
          `tree ${label}: ${t.error ? `error: ${t.error}` : `${t.sha}, driver sha256 ${t.driverDigest}`}`
      ),
      ...(session.nodes ? [`nodes: ${JSON.stringify(session.nodes)}`] : []),
      ...Object.entries(session.profiles ?? {}).map(([l, n]) => `${l}: ${n} CPU profiles`),
      ...Object.entries(session.selfTime ?? {}).map(
        ([l, fns]) => `${l} self time: ${fns.map(({ fn, ms }) => `${fn} ${ms} ms`).join('; ')}`
      ),
    ]
    const out = arms.flatMap(({ label }) => {
      const a = session.arms[label]
      return row.metrics.map(({ key, unit }) => {
        const base = { benchmark: entry.name, arm: label, metric: key, unit, runs: a.measured }
        if (a.errors.length) return { ...base, status: 'error', reason: a.errors[0] }
        const s = a.stats[key]
        if (!['valid', 'inconclusive'].includes(status) || !s) {
          const r = s ? reason : `no ${key} in every measured run`
          return { ...base, status: s ? status : 'error', reason: r }
        }
        return {
          ...base,
          status,
          ...(reason && { reason }),
          median: s.median,
          min: s.min,
          max: s.max,
        }
      })
    })
    return { rows: out, detail: { status, ...(reason && { reason }), controls: checks, notes } }
  },
})

/**
 * @param {object} session
 * @param {(result: object, label: string) => boolean} test
 * @returns {string[]} the arms with a measured run that fails `test`
 */
const armsWhere = (session, test) =>
  Object.entries(session.arms)
    .filter(([label, { results }]) => results.some((result) => !test(result, label)))
    .map(([label]) => label)

/**
 * @param {string} rawDir the `bench-raw/` directory of the report
 * @param {string} name a row
 * @returns {object|{ error: string }} that row's session line, read from the report's evidence
 */
const readSession = (rawDir, name) => {
  const file = path.join(rawDir, name, 'fork.json')
  if (!fs.existsSync(file)) return { error: 'no evidence: the row did not run' }
  try {
    const { runs } = JSON.parse(fs.readFileSync(file, 'utf8'))
    return lastJson(runs.at(-1).stdout)
  } catch (error) {
    return { error: `unreadable evidence: ${error.message}` }
  }
}

/**
 * Audits every inherited row's session from the report's evidence: its A/A spreads, discarded
 * warm-up and measured counts, and, for the frozen repeat, both archived trees with their commits
 * and the comparison driver's digest against this tree's.
 * @param {string} raw the audit's observations file, inside `bench-raw/<audit>/`
 * @returns {object} `{ audit: true, rows }`
 */
const audit = (raw) => {
  const rawDir = path.dirname(path.dirname(raw))
  const rows = {}
  for (const [name, row] of Object.entries(ROWS)) {
    if (row.contracts.length === 0) continue
    const session = readSession(rawDir, name)
    if (session.error) {
      rows[name] = { error: session.error }
      continue
    }
    const spreads = Object.values(session.aa ?? {}).flatMap(({ percent }) =>
      Object.values(percent).map(Math.abs)
    )
    const head = session.trees?.[HEAD]
    rows[name] = {
      warmup: session.warmup,
      measured: session.measured,
      aaArms: Object.keys(session.aa ?? {}).length,
      aaSpreadPercent: spreads.length ? Math.max(...spreads) : null,
      frozen: Object.fromEntries(
        FROZEN.map((label) => {
          const tree = session.trees?.[label] ?? { error: 'not in the session' }
          return [
            label,
            {
              sha: tree.sha ?? null,
              driverDigest: tree.driverDigest ?? null,
              identicalDriver: !tree.error && tree.driverDigest === head?.driverDigest,
              ...(tree.error && { error: tree.error }),
              measuredArms: Object.values(session.arms ?? {}).filter(
                (arm) => arm.tree === label && arm.measured === session.measured
              ).length,
            },
          ]
        })
      ),
      configs: row.configs.length,
    }
  }
  return { audit: true, rows }
}

/**
 * Reads the audit line of a finished audit run.
 * @param {object} result what `spawnSync` returned
 * @returns {object} the audit line
 * @throws {Error} for a failed run or a line that is not an audit
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status !== 0) throw new Error(`exit ${result.status ?? result.signal}`)
  const line = lastJson(result.stdout ?? '')
  if (line.audit !== true || !line.rows) throw new Error('the last JSON line is not an audit')
  return line
}

/**
 * Turns the audit into one row per audited row: its widest A/A spread, `error` when the row left
 * no session, `invalid` when its warm-up, measured count, A/A arms or spread, or its frozen
 * repeat on both archived trees with the identical driver, fails.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const base = { arm: '-', metric: 'aaSpreadPercent', unit: '%', runs: 1 }
  if (error) {
    return {
      rows: [{ benchmark: entry.name, ...base, metric: '-', status: 'error', reason: error }],
    }
  }
  const [line] = outputs
  const controls = []
  const out = []
  for (const [name, audited] of Object.entries(line.rows)) {
    const benchmark = `${entry.name}/${name}`
    if (audited.error) {
      out.push({ benchmark, ...base, status: 'error', reason: audited.error })
      continue
    }
    const checks = [
      ['first run discarded', audited.warmup === WARMUP],
      [`${MEASURED} measured runs`, audited.measured === MEASURED],
      ['an A/A arm per arm', audited.aaArms === ALL_TREES.length * audited.configs],
      [
        `A/A within ${AA_LIMIT_PERCENT} %`,
        audited.aaSpreadPercent !== null && audited.aaSpreadPercent <= AA_LIMIT_PERCENT,
      ],
      ...FROZEN.map((label) => {
        const tree = audited.frozen[label]
        return [
          `frozen ${label} with its SHA and the identical driver`,
          Boolean(tree.sha) && tree.identicalDriver && tree.measuredArms === 2 * audited.configs,
        ]
      }),
    ]
    for (const [check, passed] of checks) controls.push({ arm: name, name: check, passed })
    const failed = checks.filter(([, passed]) => !passed).map(([check]) => check)
    const spread = audited.aaSpreadPercent
    out.push(
      failed.length || spread === null
        ? { benchmark, ...base, status: 'invalid', reason: `failed: ${failed.join('; ')}` }
        : { benchmark, ...base, status: 'valid', median: spread, min: spread, max: spread }
    )
  }
  const status = out.some((r) => r.status === 'error')
    ? 'error'
    : out.some((r) => r.status === 'invalid')
      ? 'invalid'
      : 'valid'
  return { rows: out, detail: { status, controls, notes: [] } }
}

/**
 * @param {string[]} argv
 * @returns {Promise<number>} 0 once a line is printed, 2 for a bad argument
 */
const main = async (argv) => {
  const flag = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined)
  const raw = flag('--raw')
  const name = flag('--row')
  if (!raw || (!name && !argv.includes('--audit'))) {
    console.error('usage: consumerSession.js --row <name> --raw <file> | --audit --raw <file>')
    return 2
  }
  const line = name ? await runRow(name, path.resolve(raw)) : audit(path.resolve(raw))
  console.log(JSON.stringify(line))
  return 0
}

module.exports = {
  ROWS,
  AUDIT,
  HEAD,
  FROZEN,
  AA,
  AA_LIMIT_PERCENT,
  WARMUP,
  MEASURED,
  MIB,
  armsOf,
  manifestEntries,
  at,
  lastJson,
  runRow,
  rowAdapter,
  armsWhere,
  audit,
  selfTimes,
  runs: 1,
  timeoutMs: 10 * MINUTE,
  read,
  rows,
  main,
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error) => {
      console.error(error.stack)
      process.exitCode = 1
    }
  )
}
