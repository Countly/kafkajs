/**
 * The accumulator's cost on many partitions, topics or brokers, with no broker. One real
 * `Accumulator` of a tree is fed one record per send, round-robin over every topic-partition,
 * over a send path that answers at once; the event loop turns every `--yield-every` sends. The
 * leaders come from the tree's own metadata cache, as the producer reads them; a tree whose cache
 * has no `leader()` is fed none, which is how it ran. Counts the hand-offs and the Produce
 * requests (one per leader in a hand-off), and records the payload bytes of the largest request:
 * the keys and values of its batches, not the encoded request, which also holds headers and
 * framing.
 * Usage: node scripts/benchmarks/accumulatorScan.js [--topics 1] [--partitions 1000]
 *          [--brokers 3] [--linger 5] [--records 100000] [--yield-every 20] [--tree <dir>]
 *          [--baseline-tree <dir> --raw <file>] [--runs 9]
 * Without `--baseline-tree` it runs once and prints one JSON line: { tree, topics, partitions,
 * brokers, linger, records, wallMs, cpuMs, handOffs, produceRequests, largestPayloadBytes }. With
 * it, it runs a session: `--runs` measured runs of each of the two trees, and of a second copy of
 * the baseline (the A/A control), after one discarded warm-up run, every arm in every position,
 * each run in its own process. Every observation goes to `--raw`; the summary printed is a median
 * and a range per arm (`largestPayloadBytes`'s maximum is the session's largest), the percent
 * difference of the tree from the baseline, and the verdict: the session is void when the control's
 * `wallMs` or `cpuMs` differs from the baseline's by more than 3 % (exit 3), and fails (exit 1)
 * when the tree's `wallMs` or `cpuMs` is more than 10 % above the baseline's. `--runs` is a
 * multiple of 3, the number of arms, so that each takes every position equally often; any other
 * number exits 2. The two trees may be the same directory. A bad argument exits 2.
 */
const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')
const { runSession, summarize, format, exitCode } = require('./session')

/** The flags and their defaults; `tree` defaults to this repository. */
const DEFAULTS = {
  topics: 1,
  partitions: 1000,
  brokers: 3,
  linger: 5,
  records: 100000,
  yieldEvery: 20,
  runs: 9,
  tree: path.join(__dirname, '../..'),
  baselineTree: null,
  raw: null,
}

/** What a session reports; the A/A rule judges the two timings only, the counts depend on timers. */
const METRICS = ['wallMs', 'cpuMs', 'handOffs', 'produceRequests', 'largestPayloadBytes']
const TIMINGS = ['wallMs', 'cpuMs']

/**
 * Reads the flags of `argv` over `DEFAULTS`.
 * @param {string[]} argv
 * @returns {typeof DEFAULTS}
 * @throws {Error} for an unknown flag, a number that is not a positive integer, or a comparison
 *   whose `--runs` is not a multiple of 3 (each of its three arms must take every position equally
 *   often) or that has no `--raw` file
 */
const parseArgs = (argv) => {
  const options = { ...DEFAULTS }
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i].replace(/^--/, '').replace(/-(\w)/g, (_, c) => c.toUpperCase())
    if (!(name in DEFAULTS) || argv[i + 1] === undefined) {
      throw new Error(`unknown or incomplete flag ${argv[i]}`)
    }
    if (name === 'tree' || name === 'baselineTree' || name === 'raw') {
      options[name] = path.resolve(argv[i + 1])
      continue
    }
    const value = Number(argv[i + 1])
    if (!Number.isInteger(value) || value < 1) {
      throw new Error(`${argv[i]} needs a positive integer`)
    }
    options[name] = value
  }
  if (options.baselineTree !== null && options.runs % 3 !== 0) {
    throw new Error('--runs needs a multiple of 3 with --baseline-tree')
  }
  if (options.baselineTree !== null && options.raw === null) {
    throw new Error('--raw is needed with --baseline-tree')
  }
  return options
}

/**
 * Runs one arm in this process.
 * @param {typeof DEFAULTS} options
 * @returns {Promise<{ tree: string, topics: number, partitions: number, brokers: number,
 *   linger: number, records: number, wallMs: number, cpuMs: number, handOffs: number,
 *   produceRequests: number, largestPayloadBytes: number }>}
 */
const run = async ({ tree, topics, partitions, brokers, linger, records, yieldEvery }) => {
  const Accumulator = require(path.join(tree, 'src/producer/state/accumulator'))
  const MetadataCache = require(path.join(tree, 'src/cluster/metadataCache'))
  const nodeOf = (topicIndex, partition) => (topicIndex * partitions + partition) % brokers
  const cache = new MetadataCache({ logger: { namespace: () => ({ debug() {} }) } })
  cache.update({
    brokers: Array.from({ length: brokers }, (_, nodeId) => ({ nodeId })),
    topicMetadata: Array.from({ length: topics }, (_, t) => ({
      topic: `t${t}`,
      partitionMetadata: Array.from({ length: partitions }, (_, partitionId) => ({
        partitionId,
        leader: nodeOf(t, partitionId),
        leaderEpoch: 0,
      })),
    })),
  })

  let handOffs = 0
  let produceRequests = 0
  let largestPayloadBytes = 0
  const accumulator = new Accumulator({
    linger,
    batchSize: 16384,
    // An older tree hands the records themselves, this one the holder that carries them.
    partition: async (source) =>
      (Array.isArray(source) ? source : source.records).map(({ topic, messages }) => ({
        topic,
        partition: messages[0].partition,
        messages,
      })),
    sendPartitioned: async ({ batches }) => {
      handOffs += 1
      const bytesByNode = new Map()
      for (const batch of batches) {
        const node = nodeOf(Number(batch.topic.slice(1)), batch.partition)
        bytesByNode.set(node, (bytesByNode.get(node) ?? 0) + batch.bytes)
        batch.settlers.forEach((settler) => settler.resolve({ baseOffset: '0' }))
      }
      produceRequests += bytesByNode.size
      largestPayloadBytes = Math.max(largestPayloadBytes, ...bytesByNode.values())
    },
    ...(typeof cache.leader === 'function'
      ? { leaderOf: (topic, partition) => cache.leader(topic, partition) }
      : {}),
  })

  const value = Buffer.alloc(200, 'x')
  const sends = []
  const cpu = process.cpuUsage()
  const started = process.hrtime.bigint()
  for (let sent = 0; sent < records; sent += 1) {
    const pair = sent % (topics * partitions)
    sends.push(
      accumulator.append({
        acks: -1,
        timeout: 30000,
        compression: 0,
        topicMessages: [
          {
            topic: `t${Math.floor(pair / partitions)}`,
            messages: [{ partition: pair % partitions, value }],
          },
        ],
      })
    )
    if (sent % yieldEvery === yieldEvery - 1) {
      await new Promise(setImmediate)
    }
  }
  await Promise.all(sends)
  const used = process.cpuUsage(cpu)
  return {
    tree,
    topics,
    partitions,
    brokers,
    linger,
    records,
    wallMs: Number(process.hrtime.bigint() - started) / 1e6,
    cpuMs: (used.user + used.system) / 1000,
    handOffs,
    produceRequests,
    largestPayloadBytes,
  }
}

/**
 * Runs the comparison session: this tree, the baseline and a second copy of the baseline, each
 * run in its own process, every observation appended to `options.raw`.
 * @param {typeof DEFAULTS} options with `baselineTree` and `raw` set
 * @returns {import('./session').Summary} the arms are `tree`, `baseline` and `baseline~AA`; the
 *   pair `tree` against `baseline` is limited to +10 % on the two timings
 * @throws {Error} when a run exits with an error
 */
const compare = (options) => {
  const shape = ['topics', 'partitions', 'brokers', 'linger', 'records', 'yieldEvery'].flatMap(
    (name) => [`--${name.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)}`, String(options[name])]
  )
  const arm = (label, tree) => ({
    label,
    run: () => {
      const child = spawnSync(process.execPath, [__filename, ...shape, '--tree', tree], {
        encoding: 'utf8',
        timeout: 600000,
      })
      if (child.status !== 0) {
        throw new Error(`the run on ${tree} failed: ${child.stderr}`)
      }
      return JSON.parse(child.stdout)
    },
  })
  fs.writeFileSync(options.raw, '')
  const observations = runSession({
    arms: [arm('tree', options.tree), arm('baseline', options.baselineTree)],
    runs: options.runs,
    reference: 'baseline',
    onObservation: (observation) =>
      fs.appendFileSync(options.raw, `${JSON.stringify(observation)}\n`),
  })
  return summarize(observations, {
    metrics: METRICS,
    reference: 'baseline',
    pairs: [['tree', 'baseline', '+10']],
    controlOn: TIMINGS,
  })
}

module.exports = { parseArgs, run, compare }

if (require.main === module) {
  let options
  try {
    options = parseArgs(process.argv.slice(2))
  } catch (e) {
    console.error(e.message)
    process.exit(2)
  }
  if (options.baselineTree === null) {
    run(options).then(
      (result) => console.log(JSON.stringify(result)),
      (e) => {
        console.error(e)
        process.exit(1)
      }
    )
  } else {
    try {
      const summary = compare(options)
      format(summary, METRICS).forEach((line) => console.log(line))
      process.exitCode = exitCode(summary)
    } catch (e) {
      console.error(e)
      process.exit(1)
    }
  }
}
