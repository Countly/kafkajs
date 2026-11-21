const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { EventEmitter } = require('node:events')
const session = require('../../adapters/consumerSession')

const { ROWS, runRow, manifestEntries } = session
const ROOT = path.resolve(__dirname, '..', '..', '..', '..')

/** A complete L2 producer window: 200 messages per second for 60 s on 10 partitions. */
const FULL_WINDOW = {
  offered: 12000,
  acknowledged: 12000,
  failed: 0,
  unsettled: 0,
  partitions: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  ticks: 600,
  endedBy: 'window',
}

/**
 * @param {string[]} args
 * @param {string} name
 * @param {string} [fallback]
 * @returns {string|undefined} the value after `name`
 */
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
  if (driver === 'consumeCoverage.js') {
    const partitions = Number(flag(args, '--partitions', '6'))
    return {
      partitions,
      messages: 200000,
      populatedPartitions: partitions,
      partitionsWithData: partitions,
    }
  }
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
        mutant: { rejected: true, reason: 'Fetch bytes: expected 33231, received 0' },
      }
    }
    const arm = flag(args, '--arm')
    const active = ['S1', 'L1'].includes(arm)
    return {
      arm,
      sessions: flag(args, '--sessions') === 'true',
      topic: flag(args, '--topic'),
      sample: {
        deliveredMessages: active ? 200000 : arm === 'L2' ? 12000 : 0,
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
    if (args.includes('--checksum')) {
      const arms = flag(args, '--arms').split(',')
      return {
        checksums: Object.fromEntries(
          arms.map((arm) => [
            arm,
            {
              available: 1,
              records: 20,
              sha256: crypto.createHash('sha256').update(arm).digest('hex'),
            },
          ])
        ),
      }
    }
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
 * A `run` that answers like the real drivers and records every call. An override returns the
 * JSON line to print, or a whole process result (`{ status, stdout?, stderr? }`).
 * @param {(args: string[], n: number, options: object) => object|undefined} [override]
 * @returns {Function & { calls: object[] }}
 */
const fakeRun = (override) => {
  const calls = []
  const run = async (args, options) => {
    calls.push({ args, ...options })
    const n = calls.length
    const special = await override?.(args, n, options)
    if (special && 'status' in special) return { signal: null, stdout: '', stderr: '', ...special }
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

/** @returns {EventEmitter} a stand-in for the driver's child process */
const fakeChild = () => new EventEmitter()

/**
 * Builds the fixture of one test directory: archived trees holding this checkout's benchmark
 * drivers, and the sessions of rows run against them with fake drivers.
 * @param {string} tmp an empty directory the test owns
 * @returns {object} `tree`, `frozenEnv`, `runSession`, `sessionsHooks` and `SHAS`
 */
const consumerFixture = (tmp) => {
  const SHAS = { [ROOT]: 'a'.repeat(40) }

  /**
   * @param {string} label
   * @param {{ driverBytes?: string, lacks?: string[] }} [options] a different throughput driver
   *   copy, or the capabilities the tree lacks
   * @returns {string} an archived tree with this checkout's drivers and the consumer sources the
   *   capability probes read
   */
  const tree = (label, { driverBytes, lacks = [] } = {}) => {
    const dir = path.join(tmp, label)
    const benchmarks = path.join(dir, 'scripts', 'benchmarks')
    fs.mkdirSync(benchmarks, { recursive: true })
    for (const file of fs.readdirSync(path.join(ROOT, 'scripts', 'benchmarks'))) {
      if (file.endsWith('.js')) {
        fs.copyFileSync(path.join(ROOT, 'scripts', 'benchmarks', file), path.join(benchmarks, file))
      }
    }
    if (driverBytes) fs.writeFileSync(path.join(benchmarks, 'consumeThroughput.js'), driverBytes)
    const consumer = path.join(dir, 'src', 'consumer')
    fs.mkdirSync(consumer, { recursive: true })
    const options = ['fetchSessions', 'rebalanceListener'].filter(
      (option) => !lacks.includes(`${option} option`)
    )
    fs.writeFileSync(path.join(consumer, 'index.js'), `// ${options.join(', ')}\n`)
    if (!lacks.includes('subscription module')) {
      fs.writeFileSync(path.join(consumer, 'subscription.js'), '\n')
    }
    SHAS[dir] = crypto.createHash('sha1').update(label).digest('hex')
    return dir
  }

  /**
   * @param {object} [extra] more environment
   * @returns {object} an environment naming both archived trees and the pre-retirement tree
   */
  const frozenEnv = (extra = {}) => ({
    BENCH_TREES: JSON.stringify({
      'alpha.9c': tree('alpha.9c'),
      'alpha.9b': tree('alpha.9b'),
      'before-retirement': tree('before'),
    }),
    ...extra,
  })

  /** A fetch-session preparation without a cluster; L2 runs report a complete producer window. */
  const sessionsHooks = {
    prepare: async ({ row }) => ({
      args: ['--topic', 'bench-topic'],
      env: { BENCH_NODES: 'from-prepare' },
      nodes: [1, 2, 3].map((nodeId, i) => ({ nodeId, host: 'localhost', port: 9092 + 3 * i })),
      ...(row.args[1] === 'L2' && { afterRun: async () => ({ ...FULL_WINDOW }) }),
    }),
  }

  /**
   * Runs one row's session with fakes and reads it the way the report does.
   * @param {string} name
   * @param {{ run?: Function, env?: object, hooks?: object }} [options]
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
    const adapter = require(`../../adapters/${ROWS[name].adapter}`)
    const entry = manifestEntries().find((e) => e.name === name)
    let outcome
    try {
      outcome = {
        outputs: [adapter.read({ status: 0, stdout: `${JSON.stringify(line)}\n` }, entry)],
      }
    } catch (error) {
      outcome = { outputs: [], error: error.message }
    }
    return { line, result: adapter.rows(entry, 'fork', outcome, 1), raw, run }
  }

  return { SHAS, tree, frozenEnv, sessionsHooks, runSession }
}

/**
 * Writes a row's session line where the report keeps its evidence, as the audit reads it.
 * @param {string} rawDir the report's `bench-raw/` directory
 * @param {string} name the row
 * @param {object} line its session line
 * @returns {void}
 */
const writeEvidence = (rawDir, name, line) => {
  fs.mkdirSync(path.join(rawDir, name), { recursive: true })
  const runs = [{ run: 1, stdout: `${JSON.stringify(line)}\n` }]
  fs.writeFileSync(path.join(rawDir, name, 'fork.json'), JSON.stringify({ arm: 'fork', runs }))
}

module.exports = {
  ROOT,
  FULL_WINDOW,
  flag,
  output,
  fakeRun,
  fakeChild,
  consumerFixture,
  writeEvidence,
}
