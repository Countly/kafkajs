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
 * Rows run only where `BENCH_CONSUMER_ROWS=1`: the workflow job that owns their cluster. Anywhere
 * else a row prints `{ row, omitted }` and the report shows it as omitted with that reason.
 * `BENCH_TREES` is a JSON object of archived tree directories by label; `BENCH_AUDIT_ROWS`, a
 * comma-separated list, limits the audit to the rows a dispatch selected. Prints one JSON line:
 * the session (`row`, `contracts`, `trees`, `arms`, `aa`, `controls`, `warmup`, `measured`,
 * `profiles`, `selfTime`, `nodes`) or `{ row, error }` when the row cannot start; exit 0 once a
 * line is printed, 2 for a bad argument. Every driver run, the untimed controls included, keeps
 * its arguments, exit and exact stdout and stderr under `runs/` beside the observations.
 *
 * Statuses, the same for every row: `error` when the work did not run or this tree's arm failed;
 * `invalid` when a correctness control failed; `inconclusive` when an A/A copy differs from its arm
 * by more than 3 % (a relative tolerance of 1e-9 absorbs rounding), samples are too few, or the row
 * declares a proof it owes; otherwise `valid`. The original bounds are printed as information and
 * never decide a status. An arm a tree cannot run (no such option or module) is labelled
 * unsupported and not run.
 */
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawn, execFileSync } = require('node:child_process')
const { median, beyond } = require('../session')

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
const VS_9B = 'alpha.9b'
/** Why a consumer row prints no session outside the workflow job that owns its cluster. */
const OUTSIDE = 'runs in its own job of the benchmark workflow (BENCH_CONSUMER_ROWS=1)'

/**
 * @typedef {{ name: string, args: string[], requires?: string }} Config `requires`: the
 *   capability a tree must have for this configuration to run there
 * @typedef {{ key: string, unit: string }} Metric
 * @typedef {object} Bound an original bound, printed as information and never a status
 * @property {string} metric
 * @property {string} against a tree label (every other tree is compared with it, per
 *   configuration) or a configuration name (`config` is compared with it, per tree)
 * @property {string} [config] the compared configuration when `against` is one
 * @property {'percent'|'bytes'|'ratio'} kind how the medians are compared
 * @property {string} bound the bound as declared
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
 * @property {string} [controlDriver] the script of that run, the row's driver by default
 * @property {boolean} [profile] archive a CPU profile of every run
 * @property {Bound[]} bounds the original bounds
 * @property {string[]} [rules] declared rules printed with the row
 * @property {string[]} [owed] proofs the row cannot give yet; they keep it inconclusive
 * @property {string} [requires] the capability every arm of a tree needs
 */

/** @param {string[]} args @returns {Config[]} one configuration named `default` */
const only = (args = []) => [{ name: 'default', args }]

/**
 * @param {string} metric
 * @param {Bound['kind']} kind
 * @param {string} text the bound as declared
 * @param {string} [against] a tree label or a configuration name
 * @param {string} [config] the compared configuration
 * @returns {Bound}
 */
const bound = (metric, kind, text, against = VS_9B, config) => ({
  metric,
  against,
  ...(config && { config }),
  kind,
  bound: text,
})

const CONSUME_BOUNDS = [
  bound('messagesPerSecond', 'percent', '-3 %'),
  bound('deliveryGapP99', 'percent', '+5 % (only with the declared gap samples)'),
  bound('heapRetainedBytes', 'bytes', '+2 MiB'),
]

/**
 * @param {string[]} args the throughput driver's arguments
 * @param {Partial<Row>} [extra]
 * @returns {Row} a `consumeThroughput.js` row on all three trees
 */
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
  bounds: CONSUME_BOUNDS,
  ...extra,
})

/**
 * @param {number} bytes the `--max-bytes` value
 * @param {number} mib the same in MiB, for the contract id
 * @returns {Row} a maxBytes sizing row
 */
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
  bounds: ['sizing.rssPeakBytes', 'sizing.heapRetainedBytes'].map((metric) =>
    bound(metric, 'bytes', 'not a gate: recorded as sizing advice')
  ),
  rules: ['maxBytes is a response target, not a heap ceiling'],
})

/** The same-tree bounds of each fetch-session arm: sessions on against sessionless. */
const SESSION_BOUNDS = {
  S1: [
    ['sample.messagesPerSecond', 'percent', '-3 %'],
    ['sample.deliveryGapP99', 'percent', '+5 %'],
    ['sample.cpuMicrosPerMessage', 'percent', '+3 %'],
  ],
  S2: [['sample.bytesPerSecond', 'percent', 'not greater (+0 %)']],
  L1: [
    ['sample.messagesPerSecond', 'percent', '-3 %'],
    ['sample.deliveryGapP99', 'percent', '+5 %'],
  ],
  L2: [
    ['sample.bytesPerMessage', 'ratio', 'at most 10 % of sessionless'],
    ['sample.cpuMicrosPerMessage', 'ratio', 'at most 75 % of sessionless'],
  ],
  L3: [['sample.bytesPerSecond', 'ratio', 'at most 10 % of sessionless']],
  Memory: [['sample.heapRetainedBytes', 'bytes', '+3 MiB over sessionless']],
}

/**
 * @param {string} arm S1, S2, L1, L2, L3 or Memory
 * @param {string} contract
 * @returns {Row} a fetch-session row
 */
const sessions = (arm, contract) => ({
  contracts: [contract],
  adapter: 'fetchSessions',
  driver: 'fetchSessions.js',
  placement: 'tree',
  node: ['--expose-gc'],
  args: ['--arm', arm === 'Memory' ? 'L2' : arm],
  configs: [
    { name: 'sessions-false', args: ['--sessions', 'false'] },
    { name: 'sessions-true', args: ['--sessions', 'true'], requires: 'fetchSessions option' },
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
    'remove-byte-counter mutant rejected',
    ...(arm === 'Memory' ? ['retained-memory positive control'] : []),
    ...(['L2', 'Memory'].includes(arm) ? ['L2 producer window complete'] : []),
  ],
  artifacts: ['observations', 'frame journals'],
  broker: true,
  timeoutMs: 10 * MINUTE,
  control: ['--control'],
  bounds: SESSION_BOUNDS[arm].map(([metric, kind, text]) =>
    bound(metric, kind, text, 'sessions-false', 'sessions-true')
  ),
  rules: [
    'fetchSessions stays false unless the correctness matrix passed and every fixed row of the default table passes with valid controls; alpha.9b is a separate sessionless comparator',
  ],
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
    bounds: [
      bound('nsPerNodeRound', 'percent', '+3 %'),
      bound('allocatedBytesPerRound', 'percent', '+3 %'),
      bound('retainedHeapDelta', 'bytes', '+1 MiB'),
    ],
    rules: [
      'an optional fetch-round site changes only if its isolated site time improves at least 2x at 1000 partitions; the whole round stays within +3 % at 100, 1000 and 5000 partitions',
    ],
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
      { name: 'listener-1', args: ['--listener', '1'], requires: 'rebalanceListener option' },
    ],
    controls: ['handler entries', 'delivered messages', 'listener hooks once each'],
    bounds: [
      ...CONSUME_BOUNDS,
      bound(
        'messagesPerSecond',
        'percent',
        'within +3 % of the no-listener arm',
        'listener-0',
        'listener-1'
      ),
    ],
  }),
  'consumer-partitions-1000': consume(['--partitions', '1000'], {
    contracts: ['performance/consume-partitions-1000'],
    controls: [
      'handler entries',
      'delivered messages',
      '1000 partitions',
      'data on every partition',
    ],
    control: ['--partitions', '1000'],
    controlDriver: 'consumeCoverage.js',
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
    requires: 'subscription module',
    bounds: [
      bound(
        'evaluationMs',
        'percent',
        'recorded: the first measured value is the ceiling and may only tighten'
      ),
    ],
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
    bounds: [bound('ms', 'percent', 'recorded')],
    rules: [
      'optimise or accept from the archived CPU-profile self times, a disposition the owner reviews',
    ],
    owed: [
      'Java oracle for stag-500+500x30k: cross-tree equality only; the Java assignment of this shape is unverified',
    ],
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
    controls: ['decoded records equal across trees', 'decoded checksum equal across trees'],
    artifacts: ['observations'],
    broker: false,
    timeoutMs: 5 * MINUTE,
    control: ['--checksum', '--arms', 'uncompressed,gzip,snappy'],
    bounds: [bound('nsPerOp', 'percent', 'no change: recorded')],
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
    bounds: [],
    rules: [
      'the driver exits 1 when its last-third slope passes 32 KB per round: information only',
    ],
  },
  'consumer-offsetCommit-retirement': consume(
    ['--handler', 'eachMessage', '--messages', '200001', '--autocommit-threshold', '100'],
    {
      contracts: [],
      adapter: 'offsetCommit',
      trees: [HEAD, 'before-retirement'],
      bounds: [bound('messagesPerSecond', 'percent', 'recorded', 'before-retirement')],
      owed: [
        'proof that the committed-offset retirement path ran, and the reviewed before and after SHAs: this row times the auto-commit throughput path',
      ],
    }
  ),
}

/** The audit row: the A/A arms, the discarded warm-ups and the frozen repeat of every row. */
const AUDIT = {
  name: 'consumer-session-audit',
  contracts: ['performance/controls-and-aa', 'performance/frozen-repetition'],
}

/**
 * @param {Row} row
 * @param {string} tree
 * @param {string} config
 * @returns {string} the arm label of that tree and configuration
 */
const labelOf = (row, tree, config) => (row.configs.length === 1 ? tree : `${tree}/${config}`)

/**
 * @param {Row} row
 * @returns {{ label: string, tree: string, config: string, copy: boolean }[]} every arm of the
 *   row, each followed by its A/A copy unless the row has none
 */
const armsOf = (row) =>
  row.trees.flatMap((tree) =>
    row.configs.flatMap(({ name }) => {
      const label = labelOf(row, tree, name)
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
 * @param {string} dir a tree
 * @returns {string} its consumer entry module's source, empty when it has none
 */
const consumerSource = (dir) => {
  try {
    return fs.readFileSync(path.join(dir, 'src', 'consumer', 'index.js'), 'utf8')
  } catch {
    return ''
  }
}

/**
 * What a configuration may need from a tree, each read from the tree's own sources: the archived
 * alpha.9b tree has neither consumer option nor the subscription module, so those arms cannot run
 * there.
 */
const CAPABILITIES = {
  'fetchSessions option': (dir) => consumerSource(dir).includes('fetchSessions'),
  'rebalanceListener option': (dir) => consumerSource(dir).includes('rebalanceListener'),
  'subscription module': (dir) =>
    fs.existsSync(path.join(dir, 'src', 'consumer', 'subscription.js')),
}

/**
 * @param {Row} row
 * @returns {string[]} every capability any arm of the row needs
 */
const requirements = (row) => [
  ...new Set([row.requires, ...row.configs.map(({ requires }) => requires)].filter(Boolean)),
]

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
 * Describes each tree of a row: its directory, commit, the driver (and control driver) it runs
 * with their SHA-256, and the capabilities it lacks; a tree `BENCH_TREES` does not name, or whose
 * driver is missing, carries `error`.
 * @param {Row} row
 * @param {Record<string, string>} dirs archived trees by label
 * @param {(dir: string) => string|null} commit reads a tree's commit
 * @returns {Record<string, { dir?: string, sha?: string|null, driver?: string,
 *   driverDigest?: string|null, controlDriver?: string, controlDigest?: string|null,
 *   lacks?: string[], error?: string }>}
 */
const describeTrees = (row, dirs, commit) =>
  Object.fromEntries(
    row.trees.map((label) => {
      const dir = label === HEAD ? ROOT : dirs[label]
      if (!dir) return [label, { error: `BENCH_TREES names no ${label} tree` }]
      const base = row.placement === 'head' ? ROOT : dir
      const script = (file) => path.join(base, 'scripts', 'benchmarks', file)
      const driver = script(row.driver)
      const tree = { dir, sha: commit(dir), driver, driverDigest: digest(driver) }
      if (row.controlDriver) {
        tree.controlDriver = script(row.controlDriver)
        tree.controlDigest = digest(tree.controlDriver)
      }
      const lacks = requirements(row).filter((name) => !CAPABILITIES[name](dir))
      if (lacks.length) tree.lacks = lacks
      const absent = [
        tree.driverDigest ? null : driver,
        tree.controlDriver && !tree.controlDigest ? tree.controlDriver : null,
      ].filter(Boolean)
      return [label, absent.length ? { ...tree, error: `no driver ${absent.join(', ')}` } : tree]
    })
  )

/**
 * @param {Row} row
 * @param {object} trees what `describeTrees` returned
 * @param {{ tree: string, config: string }} arm
 * @returns {string|undefined} why the arm's tree cannot run it, if it cannot
 */
const unsupported = (row, trees, arm) => {
  const config = row.configs.find(({ name }) => name === arm.config)
  const lacking = [row.requires, config.requires].filter(
    (name) => name && trees[arm.tree].lacks?.includes(name)
  )
  return lacking.length ? `${arm.tree} has no ${lacking.join(', ')}` : undefined
}

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
 * Runs one driver process to its end, keeping at most `maxOutput` characters of each stream.
 * @param {string[]} args node arguments
 * @param {{ cwd: string, env: object, timeoutMs: number, onMessage?: Function,
 *   maxOutput?: number }} options
 * @returns {Promise<{ status: number|null, signal: string|null, stdout: string, stderr: string,
 *   truncated?: { stdout?: true, stderr?: true }, error?: Error }>} `truncated` names a stream
 *   that was cut at `maxOutput`
 */
const runProcess = (args, { cwd, env, timeoutMs, onMessage, maxOutput = MAX_OUTPUT }) =>
  new Promise((resolve) => {
    const child = spawn(process.execPath, args, {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe', ...(onMessage ? ['ipc'] : [])],
    })
    const out = { stdout: '', stderr: '' }
    const truncated = {}
    const append = (key) => (chunk) => {
      const room = maxOutput - out[key].length
      if (chunk.length > room) truncated[key] = true
      if (room > 0) out[key] += chunk.slice(0, room)
    }
    child.stdout.setEncoding('utf8').on('data', append('stdout'))
    child.stderr.setEncoding('utf8').on('data', append('stderr'))
    if (onMessage) child.on('message', (message) => onMessage(message, child))
    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs)
    const cut = () => (Object.keys(truncated).length ? { truncated } : {})
    child.on('error', (error) => {
      clearTimeout(timer)
      resolve({ status: null, signal: null, ...out, ...cut(), error })
    })
    child.on('close', (status, signal) => {
      clearTimeout(timer)
      resolve({ status, signal, ...out, ...cut() })
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
 * Keeps each driver run's raw streams and invocation under `runs/` of a session's directory.
 * @param {string} dir the session's directory
 * @returns {(kind: string, label: string, args: string[], cwd: string, result: object) => object}
 *   writes one run's `.stdout` and `.stderr` files and returns its invocation, the files relative
 *   to `dir`
 */
const receipts = (dir) => {
  let sequence = 0
  return (kind, label, args, cwd, result) => {
    const name = `${String(++sequence).padStart(4, '0')}-${kind}-${label.replace(/[^\w.-]+/g, '_')}`
    const base = path.join(dir, 'runs', name)
    fs.mkdirSync(path.dirname(base), { recursive: true })
    fs.writeFileSync(`${base}.stdout`, result.stdout ?? '')
    fs.writeFileSync(`${base}.stderr`, result.stderr ?? '')
    return {
      args,
      cwd,
      status: result.status ?? null,
      signal: result.signal ?? null,
      ...(result.error && { error: result.error.message }),
      ...(result.truncated && { truncated: result.truncated }),
      stdout: path.relative(dir, `${base}.stdout`),
      stderr: path.relative(dir, `${base}.stderr`),
    }
  }
}

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
  const receipt = receipts(dir)
  try {
    const argsFor = (arm, extra, { timed = true, profileDir, driver } = {}) => {
      const { dir: treeDir } = trees[arm.tree]
      const config = row.configs.find(({ name: c }) => c === arm.config)
      return {
        cwd: row.placement === 'head' ? ROOT : treeDir,
        args: [
          ...row.node,
          ...(profileDir ? ['--cpu-prof', `--cpu-prof-dir=${profileDir}`] : []),
          driver ?? trees[arm.tree].driver,
          ...extra(config),
          ...(timed ? (prepared.args ?? []) : []),
          ...(row.placement === 'head' ? ['--tree', treeDir] : []),
        ],
      }
    }
    const runEnv = { ...env, ...(prepared.env ?? {}) }
    const execute = async (kind, label, { cwd, args }, codes, onMessage) => {
      const result = await run(args, { cwd, env: runEnv, timeoutMs: row.timeoutMs, onMessage })
      return { ...readRun(result, codes), invocation: receipt(kind, label, args, cwd, result) }
    }
    const controls = {}
    for (const [label, tree] of Object.entries(trees)) {
      if (!row.control || tree.error) continue
      const where = argsFor({ tree: label }, () => row.control, {
        timed: false,
        driver: tree.controlDriver,
      })
      const outcome = await execute('control', label, where, [0])
      if (outcome.error) {
        controls[label] = { error: outcome.error, invocation: outcome.invocation }
        continue
      }
      const text = JSON.stringify(outcome.result)
      const file = path.join(dir, `control-${label}.json`)
      fs.writeFileSync(file, text)
      const hash = crypto.createHash('sha256').update(text).digest('hex')
      controls[label] = {
        digest: hash,
        file,
        invocation: outcome.invocation,
        ...(text.length <= CONTROL_INLINE && outcome.result),
      }
    }
    const arms = armsOf(row)
    const state = Object.fromEntries(arms.map((arm) => [arm.label, { results: [], errors: [] }]))
    const profiles = {}
    let halt
    for (let round = 0; round < warmup + measured && !halt; round++) {
      const order = arms.map((_, i) => arms[(i + round) % arms.length])
      for (const [position, arm] of order.entries()) {
        if (halt) break
        const entry = { round, position, arm: arm.label, warmup: round < warmup }
        const armState = state[arm.label]
        const refusal = trees[arm.tree].error ? undefined : unsupported(row, trees, arm)
        if (trees[arm.tree].error) {
          entry.error = trees[arm.tree].error
        } else if (refusal) {
          entry.unsupported = refusal
        } else {
          const profileDir = row.profile
            ? path.join(dir, 'cpu-profiles', arm.label, `round-${round}`)
            : undefined
          if (profileDir) fs.rmSync(profileDir, { recursive: true, force: true })
          const config = (c) => [...row.args, ...c.args]
          const where = argsFor(arm, config, { profileDir })
          Object.assign(
            entry,
            await execute('run', arm.label, where, exitCodes, prepared.onMessage)
          )
          const producer = await prepared.afterRun?.()
          if (producer) (entry.result ?? entry).producer = producer
          halt = producer?.halt
          if (profileDir && fs.existsSync(profileDir)) {
            const files = fs.readdirSync(profileDir).map((file) => path.join(profileDir, file))
            ;(profiles[arm.label] ??= []).push(...files.filter((f) => f.endsWith('.cpuprofile')))
          }
        }
        fs.appendFileSync(raw, `${JSON.stringify(entry)}\n`)
        if (entry.unsupported) armState.unsupported = entry.unsupported
        if (entry.warmup || entry.unsupported) continue
        if (entry.error) armState.errors.push(`round ${round}: ${entry.error}`)
        else armState.results.push(entry.result)
      }
    }
    if (halt) for (const armState of Object.values(state)) armState.errors.push(`halted: ${halt}`)
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
    const { results, errors, unsupported: refusal } = state[arm.label]
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
      ...(refusal && { unsupported: refusal }),
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
 * @param {object} line the last JSON line of a row's run
 * @param {string} name the row it must belong to
 * @returns {object} the line: a session, or `{ row, omitted }`
 * @throws {Error} for a line of another row, a row that could not start (its reason), or a line
 *   that is no session
 */
const checkSessionLine = (line, name) => {
  if (line.row !== name) throw new Error(`the session line is of row ${line.row}`)
  if (line.error) throw new Error(line.error)
  if (typeof line.omitted === 'string') return line
  if (!line.arms || !line.trees) throw new Error('the last JSON line is not a session')
  return line
}

/**
 * Reads one row's session run.
 * @param {object} result what `spawnSync` returned
 * @param {import('../report').ManifestEntry} entry
 * @returns {object} the session line
 * @throws {Error} for a failed run, no JSON line, or a line `checkSessionLine` refuses
 */
const readSessionRun = (result, entry) => {
  if (result.error) throw result.error
  if (result.status !== 0) throw new Error(`exit ${result.status ?? result.signal}`)
  return checkSessionLine(lastJson(result.stdout ?? ''), entry.name)
}

/**
 * @param {number} p a percent
 * @returns {string} it signed with two decimals, such as `+3.20 %`
 */
const signed = (p) => `${p >= 0 ? '+' : '-'}${Math.abs(p).toFixed(2)} %`

/**
 * @param {Bound['kind']} kind
 * @param {number|undefined} value the compared arm's median
 * @param {number|undefined} reference the reference arm's median
 * @returns {string} the measured difference in the bound's terms, or `not measured`
 */
const measure = (kind, value, reference) => {
  if (!Number.isFinite(value) || !Number.isFinite(reference)) return 'not measured'
  if (kind === 'bytes') {
    const mib = (value - reference) / MIB
    return `${mib >= 0 ? '+' : '-'}${Math.abs(mib).toFixed(2)} MiB`
  }
  if (reference === 0) return 'not measured'
  if (kind === 'ratio') return `${((value / reference) * 100).toFixed(2)} % of the reference`
  return signed((value / reference - 1) * 100)
}

/**
 * @param {Row} row
 * @param {object} session
 * @returns {string[]} one note per original bound and compared pair of arms: the measured
 *   difference beside the declared bound, as information
 */
const boundNotes = (row, session) =>
  row.bounds.flatMap((b) => {
    const pairs = row.trees.includes(b.against)
      ? row.configs.flatMap(({ name }) =>
          row.trees
            .filter((tree) => tree !== b.against)
            .map((tree) => [labelOf(row, tree, name), labelOf(row, b.against, name)])
        )
      : row.trees.map((tree) => [labelOf(row, tree, b.config), labelOf(row, tree, b.against)])
    return pairs.map(([arm, reference]) => {
      const value = session.arms[arm]?.stats[b.metric]?.median
      const base = session.arms[reference]?.stats[b.metric]?.median
      return `bound ${b.metric} ${arm} vs ${reference} (information): ${measure(b.kind, value, base)} (original bound ${b.bound})`
    })
  })

/**
 * Judges every A/A copy whose arm and copy both completed: a copy without a judged metric is a
 * failed control, one beyond 3 % of its arm on a judged metric is noise.
 * @param {Row} row
 * @param {object} session
 * @returns {{ missing: Control[], spreads: Control[] }}
 */
const aaControls = (row, session) => {
  const missing = []
  const spreads = []
  for (const arm of armsOf(row).filter(({ copy }) => copy)) {
    const against = arm.label.slice(0, -AA.length)
    const pair = [session.arms[against], session.arms[arm.label]]
    if (pair.some((a) => a.errors.length || a.unsupported)) continue
    const values = row.aaOn.map((key) => [
      key,
      pair[0].stats[key]?.median,
      pair[1].stats[key]?.median,
    ])
    const absent = values.filter(([, r, c]) => !Number.isFinite(r) || !Number.isFinite(c))
    if (absent.length) {
      missing.push({
        arm: arm.label,
        name: 'A/A metric measured',
        passed: false,
        detail: `no ${absent.map(([key]) => key).join(', ')}`,
      })
      continue
    }
    spreads.push({
      arm: arm.label,
      name: `A/A within ${AA_LIMIT_PERCENT} %`,
      passed: values.every(([, r, c]) => !beyond(c, r, AA_LIMIT_PERCENT)),
      detail: values
        .map(([key, r, c]) => `${key} ${r === 0 ? 'n/a' : signed((c / r - 1) * 100)}`)
        .join(', '),
    })
  }
  return { missing, spreads }
}

/**
 * Checks a completed arm's identity and summaries against its slot and its own runs, so a session
 * line can never certify another arm's runs or numbers its runs do not carry.
 * @param {Row} row
 * @param {{ tree: string, config: string }} arm the arm this slot must hold
 * @param {object} a the arm of the session line
 * @param {number} measured the measured runs the arm must have
 * @returns {string|undefined} why the arm's evidence is incomplete or inconsistent, if it is
 */
const armEvidence = (row, arm, a, measured) => {
  if (a.tree !== arm.tree || a.config !== arm.config) {
    return `recorded as ${a.tree}/${a.config}, not ${arm.tree}/${arm.config}`
  }
  const results = Array.isArray(a.results) ? a.results : []
  if (results.length !== measured || a.measured !== measured) {
    return `${results.length} of ${measured} measured runs (${a.measured} reported)`
  }
  for (const { key } of row.metrics) {
    const values = results.map((result) => at(result, key))
    if (!values.every(Number.isFinite)) return `no ${key} in every measured run`
    const expected = stat(values)
    const s = a.stats?.[key]
    if (!s || Object.keys(expected).some((k) => s[k] !== expected[k])) {
      return `the ${key} summary disagrees with its measured runs`
    }
  }
  return undefined
}

/**
 * @param {Row} row
 * @param {object} session
 * @returns {string|undefined} why this tree's arms give no complete measurement, if they do not
 */
const headProblem = (row, session) => {
  for (const arm of armsOf(row).filter(({ tree }) => tree === HEAD)) {
    const { label } = arm
    const a = session.arms[label]
    if (a.unsupported) return `${label}: ${a.unsupported}`
    if (a.errors.length) return `${label}: ${a.errors[0]}`
    const problem = armEvidence(row, arm, a, session.measured)
    if (problem) return `${label}: ${problem}`
  }
  return undefined
}

/**
 * @typedef {{ arm: string, name: string, passed: boolean, detail?: string, fatal?: boolean }}
 *   Control `fatal`: its failure means the work did not happen, so the row is `error`
 * @typedef {object} RowHooks what a driver's adapter adds to the shared session rules
 * @property {(session: object, row: Row) => Control[]} [controls] the driver's own controls
 * @property {(session: object, row: Row) => string|undefined} [inconclusive] why the row is
 *   inconclusive, if it is
 * @property {(result: object) => string} [corpus] the workload identity of one run
 * @property {(context: object) => Promise<object>} [prepare] run once before the session:
 *   returns `args`, `env`, `nodes`, `onMessage`, `afterRun` and `cleanup` for the runs; a receipt
 *   from `afterRun` with a `halt` reason ends the session, every arm then carrying that error
 * @property {number} [timeoutMs] the limit of the whole session
 */

/**
 * Builds the report adapter of a consumer row, with the statuses of the header: `error` when the
 * session did not run, this tree's arm failed or a fatal control failed; `invalid` when another
 * control failed (an A/A copy without its metric included); `inconclusive` for A/A noise beyond
 * 3 %, the driver's reason (such as too few samples) or a proof the row owes; otherwise `valid`.
 * Each arm's rows carry their median and range; an archived arm that failed, or whose summaries
 * its runs do not carry, is an `error` row and an unsupported one an `omitted` row, each with its
 * reason (on this tree either makes the row `error`). The notes print the workload, trees,
 * original bounds, rules and owed proofs.
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
      detail: { status, reason, controls: [], notes: [] },
    })
    if (error) return one('error', error)
    if (outputs[0].omitted) return one('omitted', outputs[0].omitted)
    const row = Object.hasOwn(ROWS, entry.name) ? ROWS[entry.name] : undefined
    if (!row) return one('error', `no consumer row ${entry.name}`)
    const session = outputs[0]
    const arms = armsOf(row)
    const missingArm = arms.find(({ label }) => !session.arms[label])
    if (missingArm) return one('error', `the session has no arm ${missingArm.label}`)

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
      if (label === HEAD) continue
      checks.push({
        arm: label,
        name: 'identical comparison driver',
        passed: tree.driverDigest === head.driverDigest,
        detail: `${tree.driverDigest} vs ${head.driverDigest}`,
      })
      if (row.controlDriver) {
        checks.push({
          arm: label,
          name: 'identical control driver',
          passed: Boolean(tree.controlDigest) && tree.controlDigest === head.controlDigest,
          detail: `${tree.controlDigest} vs ${head.controlDigest}`,
        })
      }
    }
    const aa = aaControls(row, session)
    checks.push(...aa.missing)
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

    const failure = headProblem(row, session)
    const failed = checks.filter(({ passed }) => !passed)
    const named = (list) => list.map(({ arm: a, name }) => `${a} ${name}`).join('; ')
    let status = 'valid'
    let reason
    if (failure) {
      status = 'error'
      reason = failure
    } else if (failed.some(({ fatal }) => fatal)) {
      status = 'error'
      reason = `failed controls: ${named(failed.filter(({ fatal }) => fatal))}`
    } else if (failed.length) {
      status = 'invalid'
      reason = `failed controls: ${named(failed)}`
    } else {
      const noisy = aa.spreads.filter(({ passed }) => !passed)
      const reasons = [
        noisy.length &&
          `A/A beyond ${AA_LIMIT_PERCENT} %: ${noisy.map((c) => `${c.arm} ${c.detail}`).join('; ')}`,
        inconclusive(session, row),
        ...(row.owed ?? []).map((proof) => `owed: ${proof}`),
      ].filter(Boolean)
      if (reasons.length) {
        status = 'inconclusive'
        reason = reasons.join('; ')
      }
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
      ...arms
        .filter(({ copy, label }) => !copy && session.arms[label].unsupported)
        .map(({ label }) => `unsupported ${label}: ${session.arms[label].unsupported}`),
      ...(row.owed ?? []).map((proof) => `owed: ${proof}`),
      ...(row.rules ?? []).map((rule) => `rule (information): ${rule}`),
      ...boundNotes(row, session),
      ...(session.nodes ? [`nodes: ${JSON.stringify(session.nodes)}`] : []),
      ...Object.entries(session.profiles ?? {}).map(([l, n]) => `${l}: ${n} CPU profiles`),
      ...Object.entries(session.selfTime ?? {}).map(
        ([l, fns]) => `${l} self time: ${fns.map(({ fn, ms }) => `${fn} ${ms} ms`).join('; ')}`
      ),
    ]
    const out = arms.flatMap((arm) => {
      const { label } = arm
      const a = session.arms[label]
      const problem =
        !a.unsupported && !a.errors.length && armEvidence(row, arm, a, session.measured)
      return row.metrics.map(({ key, unit }) => {
        const base = { benchmark: entry.name, arm: label, metric: key, unit, runs: a.measured }
        if (a.unsupported) return { ...base, status: 'omitted', reason: a.unsupported }
        if (a.errors.length) return { ...base, status: 'error', reason: a.errors[0] }
        if (problem) return { ...base, status: 'error', reason: problem }
        const s = a.stats[key]
        if (!['valid', 'inconclusive'].includes(status)) return { ...base, status, reason }
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
    return {
      rows: out,
      detail: {
        status,
        ...(reason && { reason }),
        controls: [...checks, ...aa.spreads],
        notes,
      },
    }
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
    return { line: lastJson(runs.at(-1).stdout) }
  } catch (error) {
    return { error: `unreadable evidence: ${error.message}` }
  }
}

/** Audit statuses from the worst. */
const SEVERITY = ['error', 'invalid', 'inconclusive', 'valid']

/**
 * Audits one row's session: the row's own adapter verdict (this tree, controls, A/A, owed
 * proofs), then what the frozen repetition adds: one discarded and seven measured rounds, both
 * archived trees present, and every arm of every tree complete: seven measured runs, a finite
 * value of every metric in each and summaries that match them. An arm a tree cannot run is an
 * owed comparison, so the row is at best inconclusive.
 * @param {string} name
 * @param {Row} row
 * @param {object} line the row's session line
 * @returns {{ status: string, reason?: string, aaSpreadPercent: number|null }}
 */
const auditRow = (name, row, line) => {
  const found = { error: [], invalid: [], inconclusive: [] }
  let session
  let verdict
  try {
    session = checkSessionLine(line, name)
    if (session.omitted) throw new Error(`omitted: ${session.omitted}`)
    verdict = require(`./${row.adapter}`).rows({ name }, 'fork', { outputs: [session] }).detail
  } catch (error) {
    return { status: 'error', reason: error.message, aaSpreadPercent: null }
  }
  if (verdict.status !== 'valid') found[verdict.status].push(verdict.reason)
  if (session.warmup !== WARMUP || session.measured !== MEASURED) {
    found.invalid.push(
      `${session.warmup} discarded and ${session.measured} measured rounds, not ${WARMUP} and ${MEASURED}`
    )
  }
  const broken = FROZEN.filter((label) => !session.trees[label] || session.trees[label].error)
  for (const label of broken) {
    found.error.push(`${label}: ${session.trees[label]?.error ?? 'not in the session'}`)
  }
  for (const arm of armsOf(row)) {
    const a = session.arms[arm.label]
    if (broken.includes(arm.tree) || !a) continue
    if (a.unsupported) {
      if (!arm.copy) found.inconclusive.push(`owed: ${arm.label} ${a.unsupported}`)
    } else {
      const problem = a.errors[0] ?? armEvidence(row, arm, a, MEASURED)
      if (problem) found.error.push(`${arm.label}: ${problem}`)
    }
  }
  const spreads = Object.values(session.aa ?? {}).flatMap(({ percent }) =>
    Object.values(percent).filter(Number.isFinite).map(Math.abs)
  )
  const status = SEVERITY.find((s) => s === 'valid' || found[s].length)
  const reasons = [...found.error, ...found.invalid, ...found.inconclusive]
  return {
    status,
    ...(reasons.length && { reason: [...new Set(reasons)].join('; ') }),
    aaSpreadPercent: spreads.length ? Math.max(...spreads) : null,
  }
}

/**
 * Audits the inherited rows a dispatch selected (`BENCH_AUDIT_ROWS`, all by default) from the
 * report's evidence. A selected row without evidence is `error`; a row left out is `omitted`.
 * @param {string} raw the audit's observations file, inside `bench-raw/<audit>/`
 * @param {object} [env] the environment, `process.env` by default
 * @returns {object} `{ audit: true, scope, rows }`, or `{ row, error }` for an unknown selection
 */
const audit = (raw, env = process.env) => {
  const rawDir = path.dirname(path.dirname(raw))
  const inherited = Object.keys(ROWS).filter((name) => ROWS[name].contracts.length)
  const scope = env.BENCH_AUDIT_ROWS ? env.BENCH_AUDIT_ROWS.split(',') : inherited
  const unknown = scope.find((name) => !inherited.includes(name))
  if (unknown)
    return { row: AUDIT.name, error: `BENCH_AUDIT_ROWS names no consumer row ${unknown}` }
  const rows = {}
  for (const name of inherited) {
    if (!scope.includes(name)) {
      rows[name] = { status: 'omitted', reason: 'not selected in this dispatch' }
      continue
    }
    const { line, error } = readSession(rawDir, name)
    rows[name] = error
      ? { status: 'error', reason: error, aaSpreadPercent: null }
      : auditRow(name, ROWS[name], line)
  }
  return { audit: true, scope, rows }
}

/**
 * Reads the audit line of a finished audit run.
 * @param {object} result what `spawnSync` returned
 * @returns {object} the audit line
 * @throws {Error} for a failed run, an audit that could not start, or a line that is not an audit
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status !== 0) throw new Error(`exit ${result.status ?? result.signal}`)
  const line = lastJson(result.stdout ?? '')
  if (line.error) throw new Error(line.error)
  if (typeof line.omitted === 'string') return line
  if (line.audit !== true || !line.rows) throw new Error('the last JSON line is not an audit')
  return line
}

/**
 * Turns the audit into one row per inherited row: its status and reason as audited, with its
 * widest A/A spread as the number. The audit's own status is its worst row's; an audit that left
 * rows out is at best `inconclusive`, because the inherited controls-and-aa and frozen-repetition
 * contracts need every row.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const base = { arm: '-', metric: 'aaSpreadPercent', unit: '%', runs: 1 }
  const one = (status, reason) => ({
    rows: [{ benchmark: entry.name, ...base, metric: '-', status, reason }],
    detail: { status, reason, controls: [], notes: [] },
  })
  if (error) return one('error', error)
  const [line] = outputs
  if (line.omitted) return one('omitted', line.omitted)
  const controls = []
  const out = []
  for (const [name, audited] of Object.entries(line.rows)) {
    const benchmark = `${entry.name}/${name}`
    const { status, reason, aaSpreadPercent: spread } = audited
    const numbered = ['valid', 'inconclusive'].includes(status)
    if (status === 'omitted' || !numbered) {
      out.push({ benchmark, ...base, status, reason })
    } else if (!Number.isFinite(spread)) {
      out.push({
        benchmark,
        ...base,
        status: 'error',
        reason: [reason, 'no A/A spread'].filter(Boolean).join('; '),
      })
    } else {
      out.push({
        benchmark,
        ...base,
        status,
        ...(reason && { reason }),
        median: spread,
        min: spread,
        max: spread,
      })
    }
    if (status !== 'omitted') {
      controls.push({
        arm: name,
        name: 'audited',
        passed: status === 'valid',
        detail: reason ?? status,
      })
    }
  }
  const judged = out.filter((r) => r.status !== 'omitted')
  let status = SEVERITY.find((s) => s === 'valid' || judged.some((r) => r.status === s))
  const inherited = Object.keys(line.rows).length
  const partial = line.scope.length < inherited
  let reason =
    status === 'valid'
      ? undefined
      : `${status} rows: ${judged
          .filter((r) => r.status === status)
          .map((r) => r.benchmark.slice(entry.name.length + 1))
          .join(', ')}`
  if (partial && ['valid', 'inconclusive'].includes(status)) {
    status = 'inconclusive'
    reason = `partial scope: ${line.scope.length} of ${inherited} rows; the controls-and-aa and frozen-repetition contracts need every row`
  }
  return {
    rows: out,
    detail: {
      status,
      ...(reason && { reason }),
      controls,
      notes: [`scope: ${line.scope.join(', ')}`],
    },
  }
}

/**
 * @param {string[]} argv `--row <name> --raw <file>` or `--audit --raw <file>`
 * @param {{ env?: object, log?: (line: string) => void }} [io]
 * @returns {Promise<number>} 0 once a line is printed, 2 for a bad argument
 */
const main = async (argv, { env = process.env, log = console.log } = {}) => {
  const flag = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined)
  const raw = flag('--raw')
  const name = flag('--row')
  if (!raw || (!name && !argv.includes('--audit'))) {
    console.error('usage: consumerSession.js --row <name> --raw <file> | --audit --raw <file>')
    return 2
  }
  let line
  if (env.BENCH_CONSUMER_ROWS !== '1') line = { row: name ?? AUDIT.name, omitted: OUTSIDE }
  else line = name ? await runRow(name, path.resolve(raw), { env }) : audit(path.resolve(raw), env)
  log(JSON.stringify(line))
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
  CAPABILITIES,
  armsOf,
  labelOf,
  manifestEntries,
  OUTSIDE,
  at,
  lastJson,
  runProcess,
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
