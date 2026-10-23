/**
 * The accumulator's cost on many partitions, topics or brokers, with no broker: an internal-API
 * microbenchmark of the accumulator, not of the requests a producer writes. One real
 * `Accumulator` of a tree is fed `--per-send` records of `--value-bytes` per send for one
 * topic-partition, round-robin over every topic-partition, over a send path that answers at once;
 * the event loop turns every `--yield-every` sends. The leaders come from the tree's own metadata
 * cache, as the producer reads them; a tree whose cache has no `leader()` is fed none, which is
 * how it ran. A tree with a request-size plan (`RequestBudget`) gets one of `--max-request-size`
 * bytes, as its producer would, and its modelled requests take one batch per partition within
 * that plan; a tree without one models one request per leader in a hand-off, as it sent them.
 * Counts the hand-offs and the modelled Produce requests, and records two columns of the
 * largest modelled request: `largestPayloadBytes`, the keys and values of its batches (a payload,
 * not a frame), and `largestRequestEstimate`, the request-size plan of this script's tree for it
 * (framing, topics, batch headers and records' wire sizes): a model, not the frame a producer
 * writes - `produceFrameSize.js` measures that.
 * Usage: node scripts/benchmarks/accumulatorScan.js [--topics 1] [--partitions 1000]
 *          [--brokers 3] [--linger 5] [--records 100000] [--yield-every 20] [--tree <dir>]
 *          [--max-request-size 1048576] [--value-bytes 200] [--per-send 1]
 *          [--baseline-tree <dir> --raw <file>] [--runs 9]
 * Without `--baseline-tree` it runs once and prints one JSON line: { tree, topics, partitions,
 * brokers, linger, records, wallMs, cpuMs, handOffs, produceRequests, largestPayloadBytes,
 * largestRequestEstimate }. With
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
const {
  framing,
  topicFraming,
  recordsBytes,
  BATCH_OVERHEAD,
  PARTITION_FRAMING,
} = require('../../src/producer/state/recordSize')

/** The flags and their defaults; `tree` defaults to this repository. */
const DEFAULTS = {
  topics: 1,
  partitions: 1000,
  brokers: 3,
  linger: 5,
  records: 100000,
  yieldEvery: 20,
  maxRequestSize: 1048576,
  valueBytes: 200,
  perSend: 1,
  runs: 9,
  tree: path.join(__dirname, '../..'),
  baselineTree: null,
  raw: null,
}

/** What a session reports; the A/A rule judges the two timings only, the counts depend on timers. */
const METRICS = [
  'wallMs',
  'cpuMs',
  'handOffs',
  'produceRequests',
  'largestPayloadBytes',
  'largestRequestEstimate',
]
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
 * The request-size plan a tree's producer would build, when the tree has one.
 * @param {string} tree
 * @param {number} maxRequestSize
 * @returns {object|null} the tree's own `RequestBudget`, or null for a tree without one
 */
const budgetOf = (tree, maxRequestSize) => {
  try {
    const { RequestBudget } = require(path.join(tree, 'src/producer/state/recordSize'))
    return RequestBudget === undefined ? null : new RequestBudget({ maxRequestSize })
  } catch {
    return null
  }
}

/**
 * The request-size plan of a modelled request, on this script's tree: its framing, each topic's,
 * and per batch its partition's, its header and its records' wire sizes.
 * @param {{ topic: string, messages: object[] }[]} batches
 * @returns {number}
 */
const estimateOf = (batches) => {
  let bytes = framing()
  const topics = new Set()
  for (const { topic, messages } of batches) {
    if (!topics.has(topic)) bytes += topicFraming(topic)
    topics.add(topic)
    bytes += PARTITION_FRAMING + BATCH_OVERHEAD + recordsBytes(messages)
  }
  return bytes
}

/**
 * One leader's batches of a hand-off as the modelled requests that carry them: with a plan, one
 * batch per partition per request and each request within it; without one, one request.
 * @param {object[]} batches
 * @param {object|null} budget
 * @returns {object[][]}
 */
const requestsOf = (batches, budget) => {
  if (budget === null) return [batches]
  const requests = []
  let request = []
  let planned = 0
  let partitions = new Set()
  for (const batch of batches) {
    const bytes = budget.topicBytes(batch.topic) + budget.batchBytes(batch)
    const slot = `${batch.topic}/${batch.partition}`
    if (request.length > 0 && (partitions.has(slot) || planned + bytes > budget.maxRequestBytes)) {
      requests.push(request)
      request = []
      planned = 0
      partitions = new Set()
    }
    request.push(batch)
    planned += bytes
    partitions.add(slot)
  }
  requests.push(request)
  return requests
}

/**
 * Runs one arm in this process.
 * @param {typeof DEFAULTS} options
 * @returns {Promise<{ tree: string, topics: number, partitions: number, brokers: number,
 *   linger: number, records: number, wallMs: number, cpuMs: number, handOffs: number,
 *   produceRequests: number, largestPayloadBytes: number, largestRequestEstimate: number }>}
 */
const run = async ({
  tree,
  topics,
  partitions,
  brokers,
  linger,
  records,
  yieldEvery,
  maxRequestSize,
  valueBytes,
  perSend,
}) => {
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
  let largestRequestEstimate = 0
  const budget = budgetOf(tree, maxRequestSize)
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
      const byNode = new Map()
      for (const batch of batches) {
        const node = nodeOf(Number(batch.topic.slice(1)), batch.partition)
        byNode.set(node, [...(byNode.get(node) ?? []), batch])
      }
      for (const own of byNode.values()) {
        for (const request of requestsOf(own, budget)) {
          produceRequests += 1
          const payload = request.reduce((sum, batch) => sum + batch.bytes, 0)
          largestPayloadBytes = Math.max(largestPayloadBytes, payload)
          largestRequestEstimate = Math.max(largestRequestEstimate, estimateOf(request))
        }
      }
      for (const batch of batches) {
        batch.settlers.forEach((settler) =>
          settler.resolve({ topicName: batch.topic, partition: batch.partition, baseOffset: '0' })
        )
        batch.retire?.()
      }
    },
    ...(budget === null ? {} : { budget }),
    ...(typeof cache.leader === 'function'
      ? { leaderOf: (topic, partition) => cache.leader(topic, partition) }
      : {}),
  })

  const value = Buffer.alloc(valueBytes, 'x')
  const sends = []
  const cpu = process.cpuUsage()
  const started = process.hrtime.bigint()
  for (let sent = 0; sent * perSend < records; sent += 1) {
    const pair = sent % (topics * partitions)
    const count = Math.min(perSend, records - sent * perSend)
    sends.push(
      accumulator.append({
        acks: -1,
        timeout: 30000,
        compression: 0,
        topicMessages: [
          {
            topic: `t${Math.floor(pair / partitions)}`,
            messages: Array.from({ length: count }, () => ({
              partition: pair % partitions,
              value,
            })),
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
    largestRequestEstimate,
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
  const shape = [
    'topics',
    'partitions',
    'brokers',
    'linger',
    'records',
    'yieldEvery',
    'maxRequestSize',
    'valueBytes',
    'perSend',
  ].flatMap((name) => [
    `--${name.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)}`,
    String(options[name]),
  ])
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
