const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const manifest = require('../manifest.json')
const session = require('../adapters/consumerSession')

const { ROWS, AUDIT, FROZEN, armsOf, manifestEntries, audit } = session
const TREES = ['head', 'alpha.9c', 'alpha.9b']
const consume = (args, controls = ['handler entries', 'delivered messages']) => ({
  driver: 'consumeThroughput.js',
  args,
  configs: ['default'],
  controls,
  artifacts: ['observations'],
  metrics: ['messagesPerSecond', 'deliveryGapP99', 'heapRetainedBytes'],
})
const sessionsArm = (arm, extra = []) => ({
  driver: 'fetchSessions.js',
  args: ['--arm', arm],
  configs: ['sessions-false', 'sessions-true'],
  controls: [
    'BENCH_NODES from the started containers',
    'full-cycle byte lower bound',
    'idle incremental bound',
    ...extra,
  ],
  artifacts: ['observations', 'frame journals'],
  metrics: ['sample.heapRetainedBytes'],
})
const sizing = (bytes) => ({
  driver: 'consumeThroughput.js',
  args: ['--partitions', '1000', '--max-bytes', String(bytes), '--rss'],
  configs: ['default'],
  controls: ['every partition received data'],
  artifacts: ['observations'],
  metrics: ['sizing.rssPeakBytes', 'sizing.rssProcessPeakBytes', 'sizing.heapRetainedBytes'],
})

/** The 20 inherited contracts with the arms, controls and artifacts each must keep. */
const REQUIRED = {
  'performance/fetch-round': {
    driver: 'fetchRound.js',
    args: [],
    configs: ['p100', 'p1000', 'p5000'],
    controls: ['metadata lookups = topics x nodes', 'batches built = partitions', 'no-GC window'],
    artifacts: ['observations'],
    metrics: ['nsPerNodeRound', 'allocatedBytesPerRound', 'retainedHeapDelta'],
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
  },
  'performance/consume-partitions-1000': consume(
    ['--partitions', '1000'],
    ['handler entries', 'delivered messages', '1000 partitions']
  ),
  'performance/fetch-sessions-s1': sessionsArm('S1'),
  'performance/fetch-sessions-s2': sessionsArm('S2'),
  'performance/fetch-sessions-l1': sessionsArm('L1'),
  'performance/fetch-sessions-l2': sessionsArm('L2'),
  'performance/fetch-sessions-l3': sessionsArm('L3'),
  'performance/fetch-sessions-memory': sessionsArm('L2', ['retained-memory positive control']),
  'performance/subscription-resolve': {
    driver: 'subscriptionResolve.js',
    args: ['--iterations', '100'],
    configs: ['default'],
    controls: ['match count = generated expectation'],
    artifacts: ['observations'],
    metrics: ['evaluationMs'],
  },
  'performance/sticky-assign-g6': {
    driver: 'stickyAssign.js',
    args: ['--worker', 'time', '--shape', 'stag-500+500x30k'],
    configs: ['default'],
    controls: ['assignment identical across trees', 'CPU profile per run'],
    artifacts: ['observations', 'CPU profiles'],
    metrics: ['ms'],
  },
  'performance/record-batch-decode-no-change': {
    driver: 'recordBatchDecode.js',
    args: [],
    configs: ['uncompressed', 'gzip', 'snappy'],
    controls: ['decoded records equal across trees'],
    artifacts: ['observations'],
    metrics: ['nsPerOp'],
  },
  'sizing/max-bytes-10mib': sizing(10485760),
  'sizing/max-bytes-25mib': sizing(26214400),
  'sizing/max-bytes-50mib': sizing(52428800),
}
const AUDITED = ['performance/controls-and-aa', 'performance/frozen-repetition']

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

const clone = (value) => JSON.parse(JSON.stringify(value))

describe('consumer measurement inventory', () => {
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
    const applied = manifest.entries.filter((entry) => Array.isArray(entry.contracts))
    expect(applied).toEqual(applied.length ? entries : [])
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

  test('the frozen repetition covers every row on both archived trees', () => {
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

    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-audit-'))
    try {
      const rawDir = path.join(tmp, 'bench-raw')
      const evidence = (name, line) => {
        fs.mkdirSync(path.join(rawDir, name), { recursive: true })
        const runs = [{ run: 1, stdout: `${JSON.stringify(line)}\n` }]
        fs.writeFileSync(
          path.join(rawDir, name, 'fork.json'),
          JSON.stringify({ arm: 'fork', runs })
        )
      }
      const lineOf = (name, row) => ({
        row: name,
        warmup: 1,
        measured: 7,
        trees: Object.fromEntries(
          TREES.map((tree, i) => [
            tree,
            { sha: String(i).repeat(40), driverDigest: 'd'.repeat(64) },
          ])
        ),
        arms: Object.fromEntries(
          armsOf(row).map(({ label, tree }) => [label, { tree, measured: 7 }])
        ),
        aa: Object.fromEntries(
          armsOf(row)
            .filter(({ copy }) => copy)
            .map(({ label }) => [label, { percent: { [row.aaOn[0]]: 0.8 } }])
        ),
      })
      for (const [name, row] of inherited) evidence(name, lineOf(name, row))
      const raw = path.join(rawDir, AUDIT.name, 'fork-run-1.jsonl')
      const entry = manifestEntries().find((e) => e.name === AUDIT.name)
      const read = () =>
        session.rows(entry, 'fork', {
          outputs: [session.read({ status: 0, stdout: JSON.stringify(audit(raw)) })],
        })

      const complete = read()
      expect(complete.detail.status).toBe('valid')
      expect(complete.rows).toHaveLength(18)
      expect(complete.rows.every((r) => r.status === 'valid' && r.median === 0.8)).toBe(true)

      const [g6, g6Row] = inherited.find(([name]) => name === 'consumer-stickyAssign-g6')
      const without = lineOf(g6, g6Row)
      without.trees['alpha.9b'] = { error: 'BENCH_TREES names no alpha.9b tree' }
      evidence(g6, without)
      const partial = read()
      expect(partial.detail.status).toBe('invalid')
      expect(partial.rows.find((r) => r.benchmark.endsWith(g6))).toMatchObject({
        status: 'invalid',
        reason: 'failed: frozen alpha.9b with its SHA and the identical driver',
      })

      fs.rmSync(path.join(rawDir, 'consumer-recordBatchDecode'), { recursive: true })
      const missing = read()
      expect(missing.detail.status).toBe('error')
      expect(
        missing.rows.find((r) => r.benchmark.endsWith('consumer-recordBatchDecode'))
      ).toMatchObject({
        status: 'error',
        reason: 'no evidence: the row did not run',
      })
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true })
    }
  })
})
