const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const manifest = require('../manifest.json')
const session = require('../adapters/consumerSession')
const { ROOT, fakeRun, consumerFixture, writeEvidence } = require('./fixtures/consumerRows')

const { ROWS, AUDIT, FROZEN, HEAD, armsOf, manifestEntries, audit } = session
const TREES = ['head', 'alpha.9c', 'alpha.9b']
const CONSUME_BOUNDS = [
  'messagesPerSecond vs alpha.9b: -3 %',
  'deliveryGapP99 vs alpha.9b: +5 % (only with the declared gap samples)',
  'heapRetainedBytes vs alpha.9b: +2 MiB',
]

/**
 * @param {string[]} args
 * @param {string[]} [controls]
 * @returns {object} the contract of a throughput row
 */
const consume = (args, controls = ['handler entries', 'delivered messages']) => ({
  driver: 'consumeThroughput.js',
  args,
  configs: ['default'],
  controls,
  artifacts: ['observations'],
  metrics: ['messagesPerSecond', 'deliveryGapP99', 'heapRetainedBytes'],
  bounds: CONSUME_BOUNDS,
})

/**
 * @param {string} arm
 * @param {string[]} bounds the same-tree bounds of the arm
 * @param {string[]} [extra] more controls
 * @returns {object} the contract of a fetch-session row
 */
const sessionsArm = (arm, bounds, extra = []) => ({
  driver: 'fetchSessions.js',
  args: ['--arm', arm],
  configs: ['sessions-false', 'sessions-true'],
  controls: [
    'BENCH_NODES from the started containers',
    'full-cycle byte lower bound',
    'idle incremental bound',
    'remove-byte-counter mutant rejected',
    ...extra,
  ],
  artifacts: ['observations', 'frame journals'],
  metrics: ['sample.heapRetainedBytes'],
  bounds: bounds.map((bound) => bound.replace(':', ' vs sessions-false (sessions-true):')),
})

/**
 * @param {number} bytes
 * @returns {object} the contract of a sizing row
 */
const sizing = (bytes) => ({
  driver: 'consumeThroughput.js',
  args: ['--partitions', '1000', '--max-bytes', String(bytes), '--rss'],
  configs: ['default'],
  controls: ['every partition received data'],
  artifacts: ['observations'],
  metrics: ['sizing.rssPeakBytes', 'sizing.rssProcessPeakBytes', 'sizing.heapRetainedBytes'],
  bounds: [
    'sizing.rssPeakBytes vs alpha.9b: not a gate: recorded as sizing advice',
    'sizing.heapRetainedBytes vs alpha.9b: not a gate: recorded as sizing advice',
  ],
})

/**
 * The 20 inherited contracts with the arms, controls, artifacts and original bounds each must
 * keep, and the proofs a row declares owed instead of claiming them.
 */
const REQUIRED = {
  'performance/fetch-round': {
    driver: 'fetchRound.js',
    args: [],
    configs: ['p100', 'p1000', 'p5000'],
    controls: ['metadata lookups = topics x nodes', 'batches built = partitions', 'no-GC window'],
    artifacts: ['observations'],
    metrics: ['nsPerNodeRound', 'allocatedBytesPerRound', 'retainedHeapDelta'],
    bounds: [
      'nsPerNodeRound vs alpha.9b: +3 %',
      'allocatedBytesPerRound vs alpha.9b: +3 %',
      'retainedHeapDelta vs alpha.9b: +1 MiB',
    ],
  },
  'performance/consume-each-batch': consume([
    '--handler',
    'eachBatch',
    '--max-bytes-per-partition',
    '32768',
  ]),
  'performance/consume-each-message': consume(['--handler', 'eachMessage', '--messages', '200001']),
  'performance/consume-autocommit': consume([
    '--handler',
    'eachMessage',
    '--messages',
    '200001',
    '--autocommit-threshold',
    '100',
  ]),
  'performance/consume-listener': {
    ...consume(
      ['--handler', 'eachBatch', '--max-bytes-per-partition', '32768'],
      ['handler entries', 'delivered messages', 'listener hooks once each']
    ),
    configs: ['listener-0', 'listener-1'],
    bounds: [
      ...CONSUME_BOUNDS,
      'messagesPerSecond vs listener-0 (listener-1): within +3 % of the no-listener arm',
    ],
  },
  'performance/consume-partitions-1000': consume(
    ['--partitions', '1000'],
    ['handler entries', 'delivered messages', '1000 partitions', 'data on every partition']
  ),
  'performance/fetch-sessions-s1': sessionsArm('S1', [
    'sample.messagesPerSecond: -3 %',
    'sample.deliveryGapP99: +5 %',
    'sample.cpuMicrosPerMessage: +3 %',
  ]),
  'performance/fetch-sessions-s2': sessionsArm('S2', ['sample.bytesPerSecond: not greater (+0 %)']),
  'performance/fetch-sessions-l1': sessionsArm('L1', [
    'sample.messagesPerSecond: -3 %',
    'sample.deliveryGapP99: +5 %',
  ]),
  'performance/fetch-sessions-l2': sessionsArm(
    'L2',
    [
      'sample.bytesPerMessage: at most 10 % of sessionless',
      'sample.cpuMicrosPerMessage: at most 75 % of sessionless',
    ],
    ['L2 producer window complete']
  ),
  'performance/fetch-sessions-l3': sessionsArm('L3', [
    'sample.bytesPerSecond: at most 10 % of sessionless',
  ]),
  'performance/fetch-sessions-memory': sessionsArm(
    'L2',
    ['sample.heapRetainedBytes: +3 MiB over sessionless'],
    ['retained-memory positive control', 'L2 producer window complete']
  ),
  'performance/subscription-resolve': {
    driver: 'subscriptionResolve.js',
    args: ['--iterations', '100'],
    configs: ['default'],
    controls: ['match count = generated expectation'],
    artifacts: ['observations'],
    metrics: ['evaluationMs'],
    bounds: [
      'evaluationMs vs alpha.9b: recorded: the first measured value is the ceiling and may only tighten',
    ],
  },
  'performance/sticky-assign-g6': {
    driver: 'stickyAssign.js',
    args: ['--worker', 'time', '--shape', 'stag-500+500x30k'],
    configs: ['default'],
    controls: ['assignment identical across trees', 'CPU profile per run'],
    artifacts: ['observations', 'CPU profiles'],
    metrics: ['ms'],
    bounds: ['ms vs alpha.9b: recorded'],
    owed: [
      'Java oracle for stag-500+500x30k: cross-tree equality only; the Java assignment of this shape is unverified',
    ],
  },
  'performance/record-batch-decode-no-change': {
    driver: 'recordBatchDecode.js',
    args: [],
    configs: ['uncompressed', 'gzip', 'snappy'],
    controls: ['decoded records equal across trees', 'decoded checksum equal across trees'],
    artifacts: ['observations'],
    metrics: ['nsPerOp'],
    bounds: ['nsPerOp vs alpha.9b: no change: recorded'],
  },
  'sizing/max-bytes-10mib': sizing(10485760),
  'sizing/max-bytes-25mib': sizing(26214400),
  'sizing/max-bytes-50mib': sizing(52428800),
}
const AUDITED = ['performance/controls-and-aa', 'performance/frozen-repetition']

/**
 * @param {object} bound a row's bound
 * @returns {string} how the inventory names it
 */
const boundName = ({ metric, against, config, bound }) =>
  `${metric} vs ${against}${config ? ` (${config})` : ''}: ${bound}`

/**
 * @param {Record<string, object>} rows an inventory shaped like `ROWS`
 * @returns {string[]} every way the inventory falls short of `REQUIRED`
 */
const inventoryProblems = (rows) => {
  const problems = []
  for (const [contract, need] of Object.entries(REQUIRED)) {
    const owners = Object.entries(rows).filter(([, row]) => row.contracts.includes(contract))
    if (owners.length !== 1) {
      problems.push(`${contract} has ${owners.length} rows`)
      continue
    }
    const [name, row] = owners[0]
    const lacks = (what, have, want) => {
      const missing = want.filter((item) => !have.includes(item))
      if (missing.length) problems.push(`${name} lacks ${what} ${missing.join(', ')}`)
    }
    if (row.driver !== need.driver) problems.push(`${name} runs ${row.driver}`)
    if (JSON.stringify(row.args) !== JSON.stringify(need.args)) problems.push(`${name} args`)
    lacks('arm tree', row.trees, TREES)
    lacks(
      'arm',
      row.configs.map(({ name: c }) => c),
      need.configs
    )
    lacks('control', row.controls, need.controls)
    lacks('artifact', row.artifacts, need.artifacts)
    lacks(
      'metric',
      row.metrics.map(({ key }) => key),
      need.metrics
    )
    lacks('bound', (row.bounds ?? []).map(boundName), need.bounds)
    lacks('owed label', row.owed ?? [], need.owed ?? [])
    if (row.aa === false) problems.push(`${name} has no A/A arms`)
    if ((row.warmup ?? 1) !== 1 || (row.measured ?? 7) !== 7) problems.push(`${name} rounds`)
    const keys = row.metrics.map(({ key }) => key)
    if (!row.aaOn.length || row.aaOn.some((key) => !keys.includes(key))) {
      problems.push(`${name} judges no defined A/A metric`)
    }
    if (!fs.existsSync(path.join(__dirname, '..', 'adapters', `${row.adapter}.js`))) {
      problems.push(`${name} has no adapter ${row.adapter}`)
    }
  }
  const extra = Object.values(rows)
    .flatMap((row) => row.contracts)
    .filter((contract) => !Object.hasOwn(REQUIRED, contract))
  if (extra.length) problems.push(`unknown contracts ${extra.join(', ')}`)
  return problems
}

/**
 * @param {object[]} entries manifest entries
 * @returns {string[]} how the consumer entries among them differ from the controller's
 */
const manifestProblems = (entries) => {
  const consumer = entries.filter(
    (entry) => entry.command?.[0] === 'scripts/benchmarks/adapters/consumerSession.js'
  )
  const problems = []
  for (const want of manifestEntries()) {
    const have = consumer.filter((entry) => entry.name === want.name)
    if (have.length !== 1) problems.push(`${want.name} has ${have.length} entries`)
    else if (JSON.stringify(have[0]) !== JSON.stringify(want)) problems.push(`${want.name} differs`)
  }
  const known = new Set(manifestEntries().map(({ name }) => name))
  for (const entry of consumer) {
    if (!known.has(entry.name)) problems.push(`${entry.name} is no consumer row`)
  }
  return problems
}

/** @param {unknown} value @returns {any} a deep copy of a JSON value */
const clone = (value) => JSON.parse(JSON.stringify(value))

describe('consumer measurement inventory', () => {
  let tmp
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-audit-'))
  })
  afterEach(() => {
    fs.rmSync(tmp, { recursive: true, force: true })
  })

  test('the manifest maps each of the 20 inherited contract ids to exactly one row', () => {
    const entries = manifestEntries()
    const ids = entries.flatMap((entry) => entry.contracts)
    expect(ids).toHaveLength(20)
    expect([...ids].sort()).toEqual([...Object.keys(REQUIRED), ...AUDITED].sort())
    expect(new Set(ids).size).toBe(20)
    expect(inventoryProblems(ROWS)).toEqual([])
    const auditEntry = entries.find((entry) => entry.name === AUDIT.name)
    expect(auditEntry.contracts).toEqual(AUDITED)
    expect(entries.at(-1)).toBe(auditEntry)
    for (const entry of entries) {
      expect(entry.arms).toEqual(['fork'])
      expect(entry.command[0]).toBe('scripts/benchmarks/adapters/consumerSession.js')
      expect(entry.command).toContain('{raw}')
      if (entry !== auditEntry) {
        expect(entry.command.slice(1, 3)).toEqual(['--row', entry.name])
        expect(entry.metrics).toEqual(ROWS[entry.name].metrics)
      }
    }
    expect(manifestProblems(manifest.entries)).toEqual([])
  })

  test('the manifest check fails when one or every consumer entry is removed', () => {
    const others = manifest.entries.filter(
      (entry) => entry.command?.[0] !== 'scripts/benchmarks/adapters/consumerSession.js'
    )
    expect(manifestProblems(others)).toHaveLength(manifestEntries().length)
    for (const { name } of manifestEntries()) {
      const without = [...others, ...manifestEntries().filter((entry) => entry.name !== name)]
      expect(manifestProblems(without)).toEqual([`${name} has 0 entries`])
    }
    const changed = clone(manifestEntries())
    changed[1].metrics = []
    expect(manifestProblems([...others, ...changed])).toEqual([`${changed[1].name} differs`])
  })

  test('removing any required arm, control or artifact from a row fails the inventory', () => {
    for (const contract of Object.keys(REQUIRED)) {
      const [name] = Object.entries(ROWS).find(([, row]) => row.contracts.includes(contract))
      const mutations = [
        ...ROWS[name].trees.map((tree) => (row) => {
          row.trees = row.trees.filter((t) => t !== tree)
        }),
        ...ROWS[name].configs.map((_, i) => (row) => {
          row.configs.splice(i, 1)
        }),
        ...ROWS[name].controls.map((_, i) => (row) => {
          row.controls.splice(i, 1)
        }),
        ...ROWS[name].artifacts.map((_, i) => (row) => {
          row.artifacts.splice(i, 1)
        }),
        ...ROWS[name].bounds.map((_, i) => (row) => {
          row.bounds.splice(i, 1)
        }),
        ...(ROWS[name].owed ?? []).map((_, i) => (row) => {
          row.owed.splice(i, 1)
        }),
        (row) => {
          row.aa = false
        },
        (row) => {
          row.measured = 3
        },
        (row) => {
          row.aaOn = []
        },
        (row) => {
          row.contracts = []
        },
      ]
      for (const [i, mutate] of mutations.entries()) {
        const rows = clone(ROWS)
        mutate(rows[name])
        expect([contract, i, inventoryProblems(rows).length > 0]).toEqual([contract, i, true])
      }
    }
  })

  /**
   * Runs every inherited row with the fake drivers and keeps its evidence where the audit reads it.
   * @param {object} fixture `consumerFixture(tmp)`
   * @returns {Promise<{ rawDir: string, lines: Record<string, object>, read: Function }>}
   */
  const collect = async (fixture) => {
    const rawDir = path.join(tmp, 'bench-raw')
    const lines = {}
    const env = fixture.frozenEnv()
    for (const [name, row] of Object.entries(ROWS)) {
      if (!row.contracts.length) continue
      const { line } = await fixture.runSession(name, { env })
      lines[name] = line
      writeEvidence(rawDir, name, line)
    }
    const raw = path.join(rawDir, AUDIT.name, 'fork-run-1.jsonl')
    const entry = manifestEntries().find((e) => e.name === AUDIT.name)
    const read = (env = {}) =>
      session.rows(entry, 'fork', {
        outputs: [session.read({ status: 0, stdout: JSON.stringify(audit(raw, env)) })],
      })
    return { rawDir, lines, read }
  }

  /**
   * @param {object} report what the audit adapter returned
   * @param {string} name a row
   * @returns {object} that row's audit row
   */
  const auditedRow = (report, name) =>
    report.rows.find((r) => r.benchmark === `${AUDIT.name}/${name}`)

  test('the frozen repetition covers every row on both archived trees', async () => {
    const inherited = Object.entries(ROWS).filter(([, row]) => row.contracts.length)
    expect(inherited).toHaveLength(18)
    for (const [, row] of inherited) {
      const labels = armsOf(row).map(({ label }) => label)
      for (const tree of FROZEN) {
        for (const { name } of row.configs) {
          const label = row.configs.length === 1 ? tree : `${tree}/${name}`
          expect(labels).toEqual(expect.arrayContaining([label, `${label}~AA`]))
        }
      }
    }

    const fixture = consumerFixture(tmp)
    const { rawDir, lines, read } = await collect(fixture)
    const complete = read()
    expect(complete.rows).toHaveLength(18)
    for (const [name, row] of inherited) {
      const audited = auditedRow(complete, name)
      if (row.owed?.length) {
        expect([name, audited.status]).toEqual([name, 'inconclusive'])
        expect(audited.reason).toContain(`owed: ${row.owed[0]}`)
      } else {
        expect([name, audited.status, audited.reason]).toEqual([name, 'valid', undefined])
      }
      expect(Number.isFinite(audited.median)).toBe(true)
    }
    expect(complete.detail.status).toBe('inconclusive')

    const g6 = 'consumer-stickyAssign-g6'
    const without = clone(lines[g6])
    without.trees['alpha.9b'] = { error: 'BENCH_TREES names no alpha.9b tree' }
    writeEvidence(rawDir, g6, without)
    const partial = read()
    expect(partial.detail.status).toBe('error')
    expect(auditedRow(partial, g6)).toMatchObject({
      status: 'error',
      reason: expect.stringContaining('alpha.9b: BENCH_TREES names no alpha.9b tree'),
    })

    fs.rmSync(path.join(rawDir, 'consumer-recordBatchDecode'), { recursive: true })
    expect(auditedRow(read(), 'consumer-recordBatchDecode')).toMatchObject({
      status: 'error',
      reason: 'no evidence: the row did not run',
    })
  })

  test('the audit is never valid when a measurement failed, is missing, is unsupported or was weakened', async () => {
    const fixture = consumerFixture(tmp)
    const { rawDir, lines, read } = await collect(fixture)
    const statusAfter = (name, line) => {
      writeEvidence(rawDir, name, line)
      const row = auditedRow(read(), name)
      writeEvidence(rawDir, name, lines[name])
      return row
    }

    const headDown = fakeRun((args) =>
      args.find((a) => a.endsWith('.js')).startsWith(path.join(ROOT, 'scripts'))
        ? { status: 1, stderr: 'Error: broker unreachable' }
        : undefined
    )
    const headOnly = await fixture.runSession('consumer-eachBatch', { run: headDown })
    expect(headOnly.line.arms[HEAD].measured).toBe(0)
    expect(headOnly.line.arms['alpha.9c'].measured).toBe(7)
    expect(statusAfter('consumer-eachBatch', headOnly.line)).toMatchObject({
      status: 'error',
      reason: expect.stringContaining('broker unreachable'),
    })

    const noAa = clone(lines['consumer-subscriptionResolve'])
    delete noAa.arms['alpha.9c~AA'].stats.evaluationMs
    expect(['error', 'invalid']).toContain(statusAfter('consumer-subscriptionResolve', noAa).status)

    const wrongWork = clone(lines['consumer-subscriptionResolve'])
    wrongWork.arms['alpha.9b'].results[3].matches = 8999
    expect(statusAfter('consumer-subscriptionResolve', wrongWork)).toMatchObject({
      status: 'invalid',
      reason: expect.stringContaining('match count = generated expectation'),
    })

    const noProfile = clone(lines['consumer-stickyAssign-g6'])
    noProfile.profiles['alpha.9c'] = 0
    expect(statusAfter('consumer-stickyAssign-g6', noProfile)).toMatchObject({
      status: 'invalid',
      reason: expect.stringContaining('alpha.9c CPU profile per run'),
    })

    const short = clone(lines['consumer-fetchRound'])
    short.arms['alpha.9b/p100'].results.pop()
    short.arms['alpha.9b/p100'].measured = 6
    expect(statusAfter('consumer-fetchRound', short)).toMatchObject({
      status: 'error',
      reason: expect.stringContaining('alpha.9b/p100: 6 of 7 measured runs'),
    })

    const env = fixture.frozenEnv({
      BENCH_TREES: JSON.stringify({
        'alpha.9c': fixture.tree('alpha.9c'),
        'alpha.9b': fixture.tree('alpha.9b-old', { lacks: ['rebalanceListener option'] }),
      }),
    })
    const listener = await fixture.runSession('consumer-listener', { env })
    expect(statusAfter('consumer-listener', listener.line)).toMatchObject({
      status: 'inconclusive',
      reason: expect.stringContaining(
        'owed: alpha.9b/listener-1 alpha.9b has no rebalanceListener option'
      ),
    })
  })

  test('a row whose summaries its own measured runs do not carry is never valid in the report or the audit', async () => {
    const fixture = consumerFixture(tmp)
    const { rawDir, lines, read } = await collect(fixture)
    const cases = [
      ['consumer-subscriptionResolve', 'head', (a) => (a.results = [])],
      ['consumer-subscriptionResolve', 'head', (a) => a.results.pop()],
      ['consumer-subscriptionResolve', 'alpha.9c', (a) => (a.stats.evaluationMs.n = 0)],
      ['consumer-subscriptionResolve', 'alpha.9b', (a) => (a.results[2].evaluationMs = null)],
      ['consumer-fetchRound', 'alpha.9b/p1000', (a) => (a.results[4].retainedHeapDelta = 'NaN')],
      ['consumer-fetchRound', 'head/p100', (a) => (a.stats.retainedHeapDelta.median = null)],
    ]
    for (const [i, [name, arm, mutate]] of cases.entries()) {
      const line = clone(lines[name])
      mutate(line.arms[arm])
      expect(line.arms[arm].measured).toBe(7)
      const adapter = require(`../adapters/${ROWS[name].adapter}`)
      const { rows } = adapter.rows({ name }, 'fork', { outputs: [line] })
      expect([i, rows.filter((r) => r.arm === arm && r.status !== 'error')]).toEqual([i, []])
      expect([i, rows.filter((r) => 'median' in r && !Number.isFinite(r.median))]).toEqual([i, []])
      writeEvidence(rawDir, name, line)
      expect([i, auditedRow(read(), name).status]).toEqual([i, 'error'])
      writeEvidence(rawDir, name, lines[name])
    }
  })

  test('an arm whose recorded tree or configuration is not its own is never valid in the report or the audit', async () => {
    const fixture = consumerFixture(tmp)
    const { rawDir, lines, read } = await collect(fixture)
    const cases = [
      [
        'consumer-subscriptionResolve',
        'alpha.9c',
        (l) => (l.arms['alpha.9c'] = clone(l.arms.head)),
      ],
      ['consumer-subscriptionResolve', 'head', (l) => (l.arms.head.config = 'other-workload')],
      ['consumer-subscriptionResolve', 'head~AA', (l) => (l.arms['head~AA'].tree = 'alpha.9c')],
      [
        'consumer-fetchRound',
        'alpha.9b/p100~AA',
        (l) => (l.arms['alpha.9b/p100~AA'].config = 'p1000'),
      ],
    ]
    for (const [i, [name, arm, mutate]] of cases.entries()) {
      const line = clone(lines[name])
      mutate(line)
      expect(line.arms[arm].measured).toBe(7)
      const adapter = require(`../adapters/${ROWS[name].adapter}`)
      const { rows } = adapter.rows({ name }, 'fork', { outputs: [line] })
      expect([i, rows.filter((r) => r.arm === arm && r.status !== 'error')]).toEqual([i, []])
      expect([i, rows.find((r) => r.arm === arm).reason]).toEqual([
        i,
        expect.stringContaining('recorded as'),
      ])
      writeEvidence(rawDir, name, line)
      expect([i, auditedRow(read(), name)]).toEqual([
        i,
        expect.objectContaining({
          status: 'error',
          reason: expect.stringContaining(`${arm}: recorded as`),
        }),
      ])
      writeEvidence(rawDir, name, lines[name])
    }
  })

  test('an audit of only the selected rows says which it left out and never completes the inherited contracts', async () => {
    const fixture = consumerFixture(tmp)
    const { read } = await collect(fixture)
    const selected = read({ BENCH_AUDIT_ROWS: 'consumer-eachBatch,consumer-fetchRound' })
    expect(auditedRow(selected, 'consumer-eachBatch').status).toBe('valid')
    expect(auditedRow(selected, 'consumer-fetchRound').status).toBe('valid')
    expect(auditedRow(selected, 'consumer-sessions-S1')).toMatchObject({
      status: 'omitted',
      reason: 'not selected in this dispatch',
    })
    expect(selected.detail.status).toBe('inconclusive')
    expect(selected.detail.reason).toBe(
      'partial scope: 2 of 18 rows; the controls-and-aa and frozen-repetition contracts need every row'
    )
    const unknown = audit(path.join(tmp, 'bench-raw', AUDIT.name, 'r.jsonl'), {
      BENCH_AUDIT_ROWS: 'consumer-nope',
    })
    expect(unknown).toEqual({
      row: AUDIT.name,
      error: 'BENCH_AUDIT_ROWS names no consumer row consumer-nope',
    })
    expect(() => session.read({ status: 0, stdout: `${JSON.stringify(unknown)}\n` })).toThrow(
      'BENCH_AUDIT_ROWS names no consumer row consumer-nope'
    )
  })
})
