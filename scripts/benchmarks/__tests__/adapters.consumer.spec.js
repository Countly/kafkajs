const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const session = require('../adapters/consumerSession')
const fetchSessions = require('../adapters/fetchSessions')

const { ROWS, HEAD, runRow, manifestEntries } = session
const ROOT = path.resolve(__dirname, '..', '..', '..')
const SHAS = { [ROOT]: 'a'.repeat(40) }

let tmp
beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-rows-'))
})
afterEach(() => {
  fs.rmSync(tmp, { recursive: true, force: true })
})

/**
 * @param {string} label
 * @param {{ driverBytes?: string }} [options] a different driver copy, for a mismatch
 * @returns {string} an archived tree holding this checkout's benchmark drivers
 */
const tree = (label, { driverBytes } = {}) => {
  const dir = path.join(tmp, label)
  const benchmarks = path.join(dir, 'scripts', 'benchmarks')
  fs.mkdirSync(benchmarks, { recursive: true })
  for (const file of fs.readdirSync(path.join(ROOT, 'scripts', 'benchmarks'))) {
    if (file.endsWith('.js')) {
      fs.copyFileSync(path.join(ROOT, 'scripts', 'benchmarks', file), path.join(benchmarks, file))
    }
  }
  if (driverBytes) fs.writeFileSync(path.join(benchmarks, 'consumeThroughput.js'), driverBytes)
  SHAS[dir] = crypto.createHash('sha1').update(label).digest('hex')
  return dir
}

const frozenEnv = (extra = {}) => ({
  BENCH_TREES: JSON.stringify({ 'alpha.9c': tree('alpha.9c'), 'alpha.9b': tree('alpha.9b') }),
  ...extra,
})

const flag = (args, name, fallback) =>
  args.includes(name) ? args[args.indexOf(name) + 1] : fallback

/**
 * What each real driver prints for one run, with a small deterministic spread.
 * @param {string[]} args the node arguments of the run
 * @param {number} n the call number
 * @returns {object} the driver's JSON line
 */
const output = (args, n) => {
  const jitter = 1 + (n % 3) / 1000
  const driver = path.basename(args.find((arg) => arg.endsWith('.js')))
  if (driver === 'consumeThroughput.js') {
    const handler = flag(args, '--handler', 'eachBatch')
    const messages = Number(flag(args, '--messages', '200000'))
    const partitions = Number(flag(args, '--partitions', '6'))
    const batches = handler === 'eachBatch' ? 1380 : 0
    const entries = handler === 'eachBatch' ? batches : messages
    return {
      messages,
      size: 200,
      partitions,
      handler,
      subscribeChannels: false,
      messagesPerSecond: 90000 * jitter,
      p50: 0.001,
      p90: 0.002,
      p99: 0.01,
      handlerP99: 0.01,
      deliveredMessages: messages,
      deliveredBatches: batches,
      handlerEntries: entries,
      gapSamples: entries - 1,
      deliveryGapP50: 0.01,
      deliveryGapP90: 0.05,
      deliveryGapP99: 0.4 * jitter,
      deliveredBytes: messages * 200,
      batches,
      heapRetainedBytes: 9.5e6 * jitter,
      ...(args.includes('--rss') && {
        sizing: {
          partitionsWithData: partitions,
          rssBeforeBytes: 1.2e8,
          rssPeakBytes: 3.1e8 * jitter,
          rssProcessPeakBytes: 3.3e8,
          rssGrowthBytes: 1.9e8,
          heapBeforeBytes: 2e7,
          heapPeakBytes: 9e7,
          heapRetainedBytes: 4.1e7 * jitter,
          heapRetainedGrowthBytes: 2.1e7,
        },
      }),
    }
  }
  if (driver === 'fetchRound.js') {
    const partitions = Number(flag(args, '--partitions'))
    const share = [0, 1, 2].map((node) => Math.ceil((partitions - node) / 3))
    return {
      lookups: 3,
      batches: partitions,
      requests: 3,
      lookupsByNode: [1, 1, 1],
      batchesByNode: share,
      nsPerNodeRound: 120 * partitions * jitter,
      allocatedBytesPerRound: 900 * partitions,
      retainedHeapDelta: 2048,
      windowAttempts: 1,
    }
  }
  if (driver === 'fetchSessions.js') {
    if (args.includes('--control')) {
      return {
        frames: {
          shape: [334, 333, 333],
          requestBytes: 33168,
          incrementalRequestBytes: 153,
          bytes: 33231,
          requests: 3,
        },
        delivery: [],
        retainedControlBytes: 70 * 1024 * 1024,
      }
    }
    const arm = flag(args, '--arm')
    const active = ['S1', 'L1'].includes(arm)
    return {
      arm,
      sessions: flag(args, '--sessions') === 'true',
      topic: flag(args, '--topic'),
      sample: {
        deliveredMessages: active ? 200000 : 0,
        deliveredBatches: active ? 1500 : 0,
        handlerEntries: active ? 1500 : 0,
        gapSamples: active ? 1499 : 0,
        deliveryGapP99: 3 * jitter,
        bytes: 4.2e6,
        elapsedMs: 60000,
        cpuMicros: 9e5,
        messagesPerSecond: 40000 * jitter,
        bytesPerSecond: 70000 * jitter,
        cpuMicrosPerSecond: 15000,
        bytesPerMessage: 21,
        cpuMicrosPerMessage: 4.5,
        heapRetainedBytes: 2.2e7 * jitter,
      },
      window: {},
      journal: '/evidence/fetch-frames.jsonl',
    }
  }
  if (driver === 'subscriptionResolve.js') {
    return { evaluationMs: 2.5 * jitter, matches: 9000, evaluations: 100 }
  }
  if (driver === 'stickyAssign.js') {
    if (flag(args, '--worker') === 'bytes') {
      return { sticky: ['m-1:00ff'], cooperative: ['m-1:00ff'], cooperativeCore: ['m-1:00ff'] }
    }
    const dirArg = args.find((arg) => arg.startsWith('--cpu-prof-dir='))
    if (dirArg) {
      const dir = dirArg.slice('--cpu-prof-dir='.length)
      fs.mkdirSync(dir, { recursive: true })
      const profile = {
        nodes: [
          { id: 1, callFrame: { functionName: '', url: '', lineNumber: -1 } },
          {
            id: 2,
            callFrame: { functionName: 'balance', url: '/t/stickyCore.js', lineNumber: 41 },
          },
        ],
        samples: [2, 2, 1],
        timeDeltas: [1500, 2500, 100],
      }
      fs.writeFileSync(path.join(dir, `CPU.${n}.cpuprofile`), JSON.stringify(profile))
    }
    return { ms: 4100 * jitter }
  }
  if (driver === 'recordBatchDecode.js') {
    return {
      available: 1,
      iterations: 20000,
      nsPerOp: 5100 * jitter,
      roundNsPerOp: [5100],
      opsPerSecond: 196000,
      records: 20,
      decompressCalls: 10,
    }
  }
  if (driver === 'rebalanceStorm.js') {
    return {
      groupProtocol: 'classic',
      rounds: 200,
      partitions: 6,
      samples: [1, 2, 3, 4, 5].map((i) => ({ round: i * 40, heapUsedBytes: 3e7 })),
      deltaBytes: 120000,
      slopeBytesPerRound: 400,
      ceilingBytes: 8e6,
      withinCeiling: true,
      slopeGateBytesPerRound: 32768,
      withinSlopeGate: true,
    }
  }
  throw new Error(`no fixture for ${driver}`)
}

/**
 * A `run` that answers like the real drivers and records every call.
 * @param {(args: string[], n: number) => object|{ status: number, stderr: string }} [override]
 * @returns {Function & { calls: object[] }}
 */
const fakeRun = (override) => {
  const calls = []
  const run = async (args, options) => {
    calls.push({ args, ...options })
    const n = calls.length
    const special = override?.(args, n)
    if (special && 'status' in special) return { signal: null, stdout: '', ...special }
    return {
      status: 0,
      signal: null,
      stdout: `log\n${JSON.stringify(special ?? output(args, n))}\n`,
      stderr: '',
    }
  }
  run.calls = calls
  return run
}

const sessionsHooks = {
  prepare: async () => ({
    args: ['--topic', 'bench-topic'],
    env: { BENCH_NODES: 'from-prepare' },
    nodes: [1, 2, 3].map((nodeId, i) => ({ nodeId, host: 'localhost', port: 9092 + 3 * i })),
  }),
}

/**
 * Runs one row's session with fakes and reads it the way the report does.
 * @param {string} name
 * @param {object} [options]
 * @returns {Promise<{ line: object, result: object, raw: string, run: Function }>}
 */
const runSession = async (name, { run = fakeRun(), env = frozenEnv(), hooks } = {}) => {
  const raw = path.join(tmp, 'bench-raw', name, 'fork-run-1.jsonl')
  const rowHooks = hooks ?? (ROWS[name].adapter === 'fetchSessions' ? sessionsHooks : {})
  const line = await runRow(name, raw, {
    run,
    env,
    hooks: rowHooks,
    commit: (dir) => SHAS[dir] ?? null,
  })
  const adapter = require(`../adapters/${ROWS[name].adapter}`)
  const entry = manifestEntries().find((e) => e.name === name)
  let outcome
  try {
    outcome = { outputs: [adapter.read({ status: 0, stdout: `${JSON.stringify(line)}\n` }, entry)] }
  } catch (error) {
    outcome = { outputs: [], error: error.message }
  }
  return { line, result: adapter.rows(entry, 'fork', outcome, 1), raw, run }
}

describe('consumer report rows', () => {
  test('every consumer row reports its controls and corpus identity', async () => {
    for (const name of Object.keys(ROWS)) {
      const env = frozenEnv({
        BENCH_TREES: JSON.stringify({
          'alpha.9c': tree('alpha.9c'),
          'alpha.9b': tree('alpha.9b'),
          'before-retirement': tree('before'),
        }),
      })
      const { result } = await runSession(name, { env })
      const row = ROWS[name]
      const { detail } = result
      expect([name, detail.status]).toEqual([name, 'valid'])
      const names = new Set(detail.controls.map((control) => control.name))
      for (const control of row.controls) expect([name, names.has(control)]).toEqual([name, true])
      expect(detail.controls.every(({ passed }) => passed)).toBe(true)
      for (const config of row.configs) {
        const workload = [...row.node, row.driver, ...row.args, ...config.args].join(' ')
        expect(detail.notes).toContain(`workload ${config.name}: ${workload}`)
      }
      if (!['consumer-stickyAssign-g6'].includes(name)) {
        const corpus = detail.controls.filter((c) => c.name === 'identical corpus on every tree')
        expect([name, corpus.length]).toEqual([name, row.configs.length])
      }
      expect(detail.notes).toContain(
        `contracts: ${row.contracts.join(', ') || 'none (additional row)'}`
      )
      const numbered = result.rows.filter((r) => r.status === 'valid')
      const arms = session.armsOf(row).length
      expect(numbered).toHaveLength(arms * row.metrics.length)
      expect(
        numbered.every((r) => Number.isFinite(r.median) && r.runs === (row.measured ?? 7))
      ).toBe(true)
    }
  })

  test('the sessions row reads BENCH_NODES from the started containers', async () => {
    const nodes = [
      { nodeId: 1, host: 'localhost', port: 9092 },
      { nodeId: 2, host: 'localhost', port: 9095 },
      { nodeId: 3, host: 'localhost', port: 9098 },
    ]
    const created = []
    const sent = []
    class Kafka {
      admin() {
        return {
          connect: async () => {},
          disconnect: async () => {},
          createTopics: async ({ topics }) => created.push(...topics),
          deleteTopics: async () => {},
        }
      }

      producer() {
        return {
          connect: async () => {},
          disconnect: async () => {},
          send: async ({ messages }) => sent.push(messages.length),
        }
      }
    }
    const hooks = { prepare: (context) => fetchSessions.prepare(context, { Kafka }) }
    const env = frozenEnv({ BENCH_NODES: JSON.stringify(nodes) })
    const { line, result, run } = await runSession('consumer-sessions-L1', { env, hooks })

    expect(line.nodes).toEqual(nodes)
    const timed = run.calls.filter(({ args }) => args.includes('--arm'))
    expect(timed.length).toBeGreaterThan(0)
    for (const call of timed) {
      expect(JSON.parse(call.env.BENCH_NODES)).toEqual(nodes)
      expect(path.isAbsolute(call.env.BENCH_EVIDENCE_DIR)).toBe(true)
      expect(call.args.slice(-6)).toEqual([
        '--arm',
        'L1',
        '--sessions',
        expect.stringMatching(/^(true|false)$/),
        '--topic',
        created[0].topic,
      ])
    }
    const leaders = [1, 2, 3].map(
      (id) => created[0].replicaAssignment.filter((p) => p.replicas[0] === id).length
    )
    expect(leaders).toEqual([334, 333, 333])
    expect(sent.reduce((a, b) => a + b, 0)).toBe(200000)
    expect(result.detail.controls).toContainEqual(
      expect.objectContaining({ name: 'BENCH_NODES from the started containers', passed: true })
    )

    const missing = await runSession('consumer-sessions-L1', { env: frozenEnv(), hooks })
    expect(missing.line.error).toBe(
      'BENCH_NODES is not set: the workflow reads it from the started containers'
    )
    expect(missing.result.rows).toEqual([
      expect.objectContaining({ status: 'error', reason: missing.line.error }),
    ])
  })

  test('the frozen alpha.9c and alpha.9b arms print their tree SHA and driver digest', async () => {
    const driver = path.join(ROOT, 'scripts', 'benchmarks', 'consumeThroughput.js')
    const digest = crypto.createHash('sha256').update(fs.readFileSync(driver)).digest('hex')
    const { result } = await runSession('consumer-eachBatch')
    for (const label of ['alpha.9c', 'alpha.9b']) {
      const sha = crypto.createHash('sha1').update(label).digest('hex')
      expect(result.detail.notes).toContain(`tree ${label}: ${sha}, driver sha256 ${digest}`)
      expect(result.detail.controls).toContainEqual({
        arm: label,
        name: 'identical comparison driver',
        passed: true,
        detail: `${digest} vs ${digest}`,
      })
      for (const arm of [label, `${label}~AA`]) {
        expect(result.rows.filter((r) => r.arm === arm && r.status === 'valid')).toHaveLength(3)
      }
    }

    const changed = frozenEnv()
    const trees = JSON.parse(changed.BENCH_TREES)
    trees['alpha.9b'] = tree('alpha.9b-changed', { driverBytes: '// another driver\n' })
    const mismatch = await runSession('consumer-eachBatch', {
      env: { BENCH_TREES: JSON.stringify(trees) },
    })
    expect(mismatch.result.detail.status).toBe('invalid')
    expect(mismatch.result.detail.reason).toContain('alpha.9b identical comparison driver')

    const absent = await runSession('consumer-eachBatch', { env: {} })
    expect(absent.result.detail.status).toBe('valid')
    expect(absent.result.rows.find((r) => r.arm === 'alpha.9c')).toMatchObject({
      status: 'error',
      reason: 'round 1: BENCH_TREES names no alpha.9c tree',
    })
  })

  test('a sizing run with a partition that received no data is error with its reason', async () => {
    const stderr = 'Error: Data reached 999 of 1000 partitions\n    at SizingObservation.finish'
    const run = fakeRun((args) => (args.includes('--rss') ? { status: 1, stderr } : undefined))
    const { result, line } = await runSession('consumer-sizing-25mib', { run })
    expect(line.arms[HEAD].errors[0]).toBe(
      'round 1: exit 1: Error: Data reached 999 of 1000 partitions at SizingObservation.finish'
    )
    expect(result.detail.status).toBe('error')
    expect(result.detail.reason).toContain('Data reached 999 of 1000 partitions')
    for (const row of result.rows) {
      expect(row.status).toBe('error')
      expect(row.reason).toContain('Data reached 999 of 1000 partitions')
      expect(row).not.toHaveProperty('median')
    }
  })

  test('the sizing rows come from consumeThroughput --rss, never from fetchRound', async () => {
    const sizes = { 10: 10485760, 25: 26214400, 50: 52428800 }
    for (const [mib, bytes] of Object.entries(sizes)) {
      const name = `consumer-sizing-${mib}mib`
      const row = ROWS[name]
      expect(row.contracts).toEqual([`sizing/max-bytes-${mib}mib`])
      expect(row.driver).toBe('consumeThroughput.js')
      expect(row.node).toContain('--expose-gc')
      expect(row.args).toEqual(['--partitions', '1000', '--max-bytes', String(bytes), '--rss'])
      const { run, result } = await runSession(name)
      expect(result.detail.status).toBe('valid')
      for (const { args } of run.calls) {
        expect(path.basename(args.find((arg) => arg.endsWith('.js')))).toBe('consumeThroughput.js')
        expect(args).toEqual(expect.arrayContaining(['--rss', '--max-bytes', String(bytes)]))
      }
    }
    const fetchRoundContracts = Object.values(ROWS)
      .filter((row) => row.driver === 'fetchRound.js')
      .flatMap((row) => row.contracts)
    expect(fetchRoundContracts).toEqual(['performance/fetch-round'])
  })

  test('the G6 row runs stag-500+500x30k with one discarded warm-up and seven measured runs and archives the CPU profiles', async () => {
    const run = fakeRun((args, n) => {
      const line = output(args, n)
      return flag(args, '--worker') === 'time' && n <= 9 ? { ms: 999999 } : line
    })
    const { line, result, raw } = await runSession('consumer-stickyAssign-g6', { run })
    const timed = run.calls.filter(({ args }) => flag(args, '--worker') === 'time')
    expect(timed).toHaveLength(6 * 8)
    for (const { args, cwd } of timed) {
      expect(cwd).toBe(ROOT)
      expect(flag(args, '--shape')).toBe('stag-500+500x30k')
      expect(args).toContain('--cpu-prof')
      expect(args.find((a) => a.startsWith('--cpu-prof-dir='))).toContain(path.dirname(raw))
      expect(path.isAbsolute(flag(args, '--tree'))).toBe(true)
    }
    const observations = fs
      .readFileSync(raw, 'utf8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l))
    expect(observations.filter((o) => o.warmup)).toHaveLength(6)
    expect(observations.filter((o) => !o.warmup)).toHaveLength(42)
    expect(line).toMatchObject({ warmup: 1, measured: 7 })
    for (const arm of Object.values(line.arms)) {
      expect(arm.stats.ms.n).toBe(7)
      expect(arm.stats.ms.max).toBeLessThan(999999)
    }
    expect(Object.values(line.profiles)).toEqual(Array(6).fill(8))
    expect(line.selfTime[HEAD][0]).toEqual({ fn: 'balance stickyCore.js:42', ms: 32 })
    expect(result.detail.status).toBe('valid')
    expect(
      result.detail.notes.some((n) => n.startsWith('head self time: balance stickyCore.js:42'))
    ).toBe(true)
    expect(result.detail.controls.filter((c) => c.name === 'CPU profile per run')).toHaveLength(6)

    const unprofiled = fakeRun((args) =>
      flag(args, '--worker') === 'time' ? { ms: 4100 } : undefined
    )
    const without = await runSession('consumer-stickyAssign-g6', {
      run: async (args, options) =>
        unprofiled(
          args.filter((a) => !a.startsWith('--cpu-prof')),
          options
        ),
    })
    expect(without.result.detail.status).toBe('invalid')
    expect(without.result.detail.reason).toContain('head CPU profile per run')
  })

  test('an A/A spread above 3 % makes that row invalid', async () => {
    const { line } = await runSession('consumer-subscriptionResolve')
    const adapter = require('../adapters/subscriptionResolve')
    const entry = manifestEntries().find((e) => e.name === 'consumer-subscriptionResolve')
    const withSpread = (percent) => {
      const copy = JSON.parse(JSON.stringify(line))
      copy.aa['alpha.9c~AA'].percent.evaluationMs = percent
      return adapter.rows(entry, 'fork', { outputs: [copy] }, 1)
    }
    const noisy = withSpread(3.2)
    expect(noisy.detail.status).toBe('invalid')
    expect(noisy.detail.reason).toBe('failed controls: alpha.9c~AA A/A within 3 %')
    expect(noisy.rows.every((r) => r.status === 'invalid' && r.median === undefined)).toBe(true)
    expect(withSpread(-3.2).detail.status).toBe('invalid')
    expect(withSpread(2.9).detail.status).toBe('valid')
  })
})
