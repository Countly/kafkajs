/**
 * The gate of the producer's default partitioner, on the 3-broker cluster of `npm run test:kafka`:
 * keyless records over 6 partitions, one partition per broker pair, through three arms - round
 * robin (`DefaultPartitioner`, the comparator), uniform sticky and uniform sticky with adaptive
 * weighting - in two scenarios:
 * - `healthy`: 100 000 records of 200 B, `linger` 5, `--concurrency` sends in flight; reports the
 *   records per Produce request, the balance (max over min records per partition) and the p99 of
 *   a send's latency;
 * - `slow-broker`: one broker's answers delayed 300 ms by a chaos socket; 20 000 records/s offered
 *   for 6 s (120 000), the backlog of unsettled sends sampled at 6 s (and its maximum), then a
 *   drain until 30 s; every offered send counted as delivered, rejected or still unsettled; the p99
 *   over delivered sends.
 * Every run also reports its positive controls: the partitioner's calls (equal to the records),
 * the slow broker's median Produce answer time against the others', and whether keyed records map
 * as `DefaultPartitioner` maps them.
 * Usage: node scripts/benchmarks/stickyBalance.js --scenario healthy|slow-broker
 *          --arm round-robin|sticky|adaptive [--records 100000] [--concurrency 200]
 *          [--brokers localhost:9092,localhost:9095,localhost:9098] [--tree <dir>]
 *        node scripts/benchmarks/stickyBalance.js --session --scenario <scenario> --raw <file>
 *          [--runs 8]
 * A single run prints one JSON line. A session runs every arm of the scenario and an A/A copy of
 * its reference (round robin for `healthy`, sticky for `slow-broker`) in every position, each run
 * in its own process (`session.js`), keeps every observation in `--raw`, and prints the medians
 * and the verdict of `judge`: exit 0 passed, 1 failed, 3 void or inconclusive, 2 a bad argument.
 */
const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')
const { runSession, summarize, exitCode } = require('./session')

/** Retained heap ceiling and maximum allowed scheduling drift of the six-second sample. */
const HEAP_LIMIT = 64 * 1024 * 1024
const WINDOW_TOLERANCE_MS = 50
/** @returns {number} monotonic milliseconds */
const clock = () => Number(process.hrtime.bigint()) / 1e6
/** @param {number} ms @returns {Promise<void>} a timer wait */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** Offers fixed ticks while an independent timer samples the declared six-second instant. */
class OfferWindow {
  /**
   * @param {() => number} [now] monotonic milliseconds
   * @param {(ms: number) => Promise<void>} [wait] yields until the next tick
   */
  constructor(now = clock, wait = sleep) {
    this.now = now
    this.wait = wait
  }

  /**
   * Offers 200 records on each of 600 ticks. Missed windows are never extended to catch up.
   * @param {() => void} submit offers one operation without waiting for its answer
   * @param {() => number} backlog current unsettled operations
   * @returns {Promise<object>} actual offer count, sample time, drift and backlog measurements
   */
  async run(submit, backlog) {
    const started = this.now()
    let offered = 0
    let ticks = 0
    let maxTickDriftMs = 0
    let backlogMax = 0
    let lastOfferAtMs = 0
    let sampleTimer
    const sampled = new Promise((resolve) => {
      sampleTimer = setTimeout(() => {
        resolve({ sampleAtMs: this.now() - started, backlogAt6s: backlog() })
      }, 6000)
    })
    const sampler = setInterval(() => {
      backlogMax = Math.max(backlogMax, backlog())
    }, 10)
    try {
      for (let tick = 0; tick < 600; tick += 1) {
        const due = started + tick * 10
        if (this.now() < due) await this.wait(due - this.now())
        const elapsed = this.now() - started
        if (elapsed >= 6000) break
        maxTickDriftMs = Math.max(maxTickDriftMs, elapsed - tick * 10)
        for (let i = 0; i < 200; i += 1) {
          submit()
          offered += 1
        }
        lastOfferAtMs = this.now() - started
        ticks += 1
        backlogMax = Math.max(backlogMax, backlog())
      }
      return { offered, ticks, maxTickDriftMs, lastOfferAtMs, backlogMax, ...(await sampled) }
    } finally {
      clearInterval(sampler)
      clearTimeout(sampleTimer)
    }
  }
}

/**
 * Checks a single observation before any medians can hide invalid workload or missing controls.
 * @param {'healthy'|'slow-broker'} scenario
 * @param {object} result one arm's actual measurements
 * @returns {boolean} false for missing, non-finite or inconsistent evidence
 */
const validRun = (scenario, result) => {
  const expected = scenario === 'healthy' ? 100000 : 120000
  const counts = ['offered', 'delivered', 'rejected', 'unsettled', 'partitionerCalls']
  if (!counts.every((name) => Number.isInteger(result[name]) && result[name] >= 0)) return false
  if (result.records !== expected || result.offered !== expected) return false
  if (result.offered !== result.delivered + result.rejected + result.unsettled) return false
  if (result.partitionerCalls !== result.offered || result.keyedIdentical !== 1) return false
  if (result.gcAvailable !== 1 || result.topologyValid !== 1) return false
  if (
    !Number.isFinite(result.heapRetainedBytes) ||
    result.heapRetainedBytes < 0 ||
    result.heapRetainedBytes > HEAP_LIMIT
  )
    return false
  if (!(result.p99Ms > 0) || !(result.recordsPerRequest > 0)) return false
  if (scenario === 'healthy') return result.unsettled === 0 && result.rejected === 0
  return (
    result.ticks === 600 &&
    result.lastOfferAtMs >= 0 &&
    result.lastOfferAtMs <= 6000 &&
    Math.abs(result.sampleAtMs - 6000) <= WINDOW_TOLERANCE_MS &&
    result.maxTickDriftMs >= 0 &&
    result.maxTickDriftMs <= WINDOW_TOLERANCE_MS &&
    Number.isInteger(result.backlogAt6s) &&
    result.backlogAt6s >= 0 &&
    result.backlogAt6s <= result.offered &&
    Number.isInteger(result.backlogMax) &&
    result.backlogMax >= result.backlogAt6s &&
    result.backlogMax <= result.offered &&
    result.drainAtMs >= result.sampleAtMs &&
    result.drainAtMs <= 30000 + WINDOW_TOLERANCE_MS &&
    (result.unsettled === 0 || result.drainAtMs >= 30000) &&
    result.slowMedianMs > 0 &&
    result.otherMedianMs > 0 &&
    result.slowMedianMs >= 5 * result.otherMedianMs
  )
}

/**
 * Collects after yielding so completed promises are no longer on the current stack.
 * @returns {Promise<number>} collected heapUsed
 * @throws {Error} unless the process was launched with --expose-gc
 */
const retainedHeap = async () => {
  if (typeof global.gc !== 'function') throw new Error('launch stickyBalance with --expose-gc')
  global.gc()
  await new Promise((resolve) => setImmediate(resolve))
  global.gc()
  return process.memoryUsage().heapUsed
}

const TOPIC_PREFIX = 'sticky-balance-'
const ARMS = ['round-robin', 'sticky', 'adaptive']
const SCENARIOS = {
  healthy: { reference: 'round-robin', arms: ARMS },
  'slow-broker': { reference: 'sticky', arms: ['sticky', 'adaptive'] },
}
const METRICS = [
  'recordsPerRequest',
  'balance',
  'p99Ms',
  'delivered',
  'rejected',
  'unsettled',
  'backlogAt6s',
  'backlogMax',
  'partitionerCalls',
  'records',
  'slowMedianMs',
  'otherMedianMs',
  'keyedIdentical',
  'heapRetainedBytes',
  'valid',
  'offered',
  'gcAvailable',
  'topologyValid',
  'sampleAtMs',
  'maxTickDriftMs',
  'lastOfferAtMs',
  'ticks',
  'drainAtMs',
]
const DEFAULTS = {
  scenario: 'healthy',
  arm: 'sticky',
  records: 100000,
  concurrency: 200,
  brokers: 'localhost:9092,localhost:9095,localhost:9098',
  tree: path.join(__dirname, '../..'),
  session: false,
  raw: null,
  runs: 8,
}

/**
 * Reads the flags of `argv` over `DEFAULTS`; `--session` takes no value.
 * @param {string[]} argv
 * @returns {typeof DEFAULTS}
 * @throws {Error} for an unknown flag, scenario or arm, a count that is not a positive integer, or
 *   a session without `--raw` or with runs that are not a multiple of its arms (A/A included)
 */
const parseArgs = (argv) => {
  const options = { ...DEFAULTS }
  for (let i = 0; i < argv.length; i += 1) {
    const name = argv[i].replace(/^--/, '').replace(/-(\w)/g, (_, c) => c.toUpperCase())
    if (name === 'session') {
      options.session = true
      continue
    }
    const value = argv[++i]
    if (!(name in DEFAULTS) || value === undefined) {
      throw new Error(`unknown or incomplete flag ${argv[i - 1]}`)
    }
    if (name === 'scenario' && !(value in SCENARIOS))
      throw new Error('--scenario is healthy or slow-broker')
    if (name === 'arm' && !ARMS.includes(value))
      throw new Error(`--arm is one of ${ARMS.join(', ')}`)
    if (['records', 'concurrency', 'runs'].includes(name)) {
      const count = Number(value)
      if (!Number.isInteger(count) || count < 1)
        throw new Error(`--${argv[i - 1].slice(2)} needs a positive integer`)
      options[name] = count
    } else {
      options[name] = name === 'tree' || name === 'raw' ? path.resolve(value) : value
    }
  }
  if (options.session) {
    if (options.raw === null) throw new Error('--raw is needed with --session')
    const arms = SCENARIOS[options.scenario].arms.length + 1
    if (options.runs < 7) throw new Error('--runs needs at least seven measured runs per arm')
    if (options.runs % arms !== 0)
      throw new Error(`--runs needs a multiple of ${arms} with --session`)
  }
  return options
}

/**
 * @param {number[]} values
 * @param {number} q a quantile from 0 to 1
 * @returns {number} the nearest-rank quantile; 0 for no values
 */
const quantile = (values, q) => {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1)]
}

/**
 * @param {number[]} counts records per partition
 * @returns {number} the most over the fewest; Infinity when a partition got none
 */
const balanceOf = (counts) => Math.max(...counts) / Math.min(...counts)

/**
 * Whether an arm's partitioner maps keyed records exactly as `DefaultPartitioner` does, over
 * 10 000 keys and 1 to 64 partitions.
 * @param {object} Partitioners the tree's
 * @param {Function} createPartitioner the arm's
 * @returns {number} 1 when every key maps the same, else 0
 */
const keyedIdentical = (Partitioners, createPartitioner) => {
  const one = createPartitioner()
  const other = Partitioners.DefaultPartitioner()
  for (let i = 0; i < 10000; i += 1) {
    const partitionMetadata = Array.from({ length: 1 + (i % 64) }, (_, partitionId) => ({
      partitionId,
      leader: 0,
    }))
    const args = { topic: 'keyed', partitionMetadata, message: { key: `key-${i}`, value: 'v' } }
    if (one(args) !== other(args)) return 0
  }
  return 1
}

/**
 * The verdict of a session's medians, by the declared gate. `healthy`: sticky and adaptive each at
 * least 2 times round robin's records per request, a balance of at most 1.25, a p99 at most 3 %
 * above round robin's, keyed records mapped as by `DefaultPartitioner`, and the controls (round
 * robin's own balance near 1, the partitioner called once per record). `slow-broker`: adaptive's
 * p99 at most half of sticky's, its backlog at 6 s at most a quarter, delivered at least and
 * rejected at most sticky's, nothing unsettled at 30 s, and the slow broker answering at least 5
 * times slower than the others. A void A/A control, or unsettled work in the control as well, is
 * inconclusive; any other miss fails.
 * @param {'healthy'|'slow-broker'} scenario
 * @param {import('./session').Summary} summary
 * @returns {{ verdict: 'passed'|'failed'|'inconclusive', reasons: string[] }}
 */
const judge = (scenario, summary) => {
  if (summary.void) return { verdict: 'inconclusive', reasons: ['the A/A control is void'] }
  const m = (label, metric, stat = 'median') => summary.arms?.[label]?.[metric]?.[stat] ?? NaN
  const reasons = []
  const check = (ok, reason) => ok || reasons.push(reason)
  const reference = SCENARIOS[scenario]?.reference
  if (!reference) return { verdict: 'failed', reasons: ['unknown scenario'] }
  const labels = [...SCENARIOS[scenario].arms, reference + '~AA']
  const unsettled = labels.some((arm) => m(arm, 'unsettled', 'max') > 0)
  if (unsettled && m(reference + '~AA', 'unsettled', 'max') > 0) {
    return { verdict: 'inconclusive', reasons: ['unsettled work in the A/A control as well'] }
  }
  for (const arm of labels) {
    check(
      m(arm, 'valid', 'min') === 1,
      `${arm} has invalid workload, accounting, window or controls`
    )
    check(
      m(arm, 'heapRetainedBytes', 'max') <= HEAP_LIMIT,
      `${arm} retained heap above 64 MiB or missing`
    )
    check(m(arm, 'unsettled', 'max') === 0, `${arm} left work unsettled`)
  }
  if (scenario === 'healthy') {
    for (const arm of ['sticky', 'adaptive']) {
      check(
        m(arm, 'recordsPerRequest') >= 2 * m('round-robin', 'recordsPerRequest'),
        `${arm} batches less than 2x round robin`
      )
      check(m(arm, 'balance') <= 1.25, `${arm} balance above 1.25`)
      check(
        m(arm, 'p99Ms') <= 1.03 * m('round-robin', 'p99Ms'),
        `${arm} p99 more than 3 % above round robin`
      )
    }
    for (const arm of ARMS) {
      check(m(arm, 'keyedIdentical') === 1, `${arm} maps keyed records differently`)
      check(
        m(arm, 'partitionerCalls') === m(arm, 'records'),
        `${arm} partitioner calls differ from the records`
      )
    }
    check(m('round-robin', 'balance') <= 1.05, 'round robin did not split the records evenly')
  } else {
    check(m('adaptive', 'p99Ms') <= 0.5 * m('sticky', 'p99Ms'), 'adaptive p99 above half of sticky')
    check(
      m('adaptive', 'backlogAt6s') <= 0.25 * m('sticky', 'backlogAt6s'),
      'adaptive backlog at 6 s above a quarter of sticky'
    )
    check(
      m('adaptive', 'delivered') >= m('sticky', 'delivered'),
      'adaptive delivered less than sticky'
    )
    check(
      m('adaptive', 'rejected') <= m('sticky', 'rejected'),
      'adaptive rejected more than sticky'
    )
  }
  return { verdict: reasons.length === 0 ? 'passed' : 'failed', reasons }
}

/**
 * Runs one arm against the cluster.
 * @param {typeof DEFAULTS} options
 * @returns {Promise<Record<string, number>>}
 */
const run = async ({ scenario, arm, records, concurrency, brokers, tree }) => {
  if (typeof global.gc !== 'function') throw new Error('launch stickyBalance with --expose-gc')
  const { Kafka, Partitioners, logLevel } = require(path.join(tree, 'index.js'))
  const { createChaosSocketFactory } = require(path.join(tree, 'testHelpers/chaosSocket'))
  const defaultSocketFactory = require(path.join(tree, 'src/network/socketFactory'))
  const factory = createChaosSocketFactory(defaultSocketFactory())
  const endpoints = brokers.split(',')
  const kafka = new Kafka({
    clientId: 'sticky-balance',
    brokers: endpoints,
    socketFactory: factory,
    logLevel: logLevel.NOTHING,
  })
  const topic = `${TOPIC_PREFIX}${scenario}-${arm}-${Date.now()}`
  const admin = kafka.admin()
  await admin.connect()
  const { brokers: nodes } = await admin.describeCluster()
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic,
        replicaAssignment: Array.from({ length: 6 }, (_, partition) => ({
          partition,
          replicas: [nodes[partition % nodes.length].nodeId],
        })),
      },
    ],
  })
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] })
  const partitions = metadata.topics.find((entry) => entry.name === topic)?.partitions ?? []
  const leaderCounts = new Map()
  for (const { leader } of partitions) leaderCounts.set(leader, (leaderCounts.get(leader) ?? 0) + 1)
  const topologyValid =
    nodes.length === 3 &&
    endpoints.length === 3 &&
    partitions.length === 6 &&
    new Set(partitions.map(({ partitionId }) => partitionId)).size === 6 &&
    leaderCounts.size === 3 &&
    [...leaderCounts].every(([leader, count]) => leader >= 0 && count === 2)
      ? 1
      : 0
  await admin.disconnect()

  const base =
    arm === 'round-robin'
      ? Partitioners.DefaultPartitioner
      : Partitioners.UniformStickyPartitioner({ stickyBytes: 16384, adaptive: arm === 'adaptive' })
  let partitionerCalls = 0
  const createPartitioner = () => {
    const { observe } = require(path.join(tree, 'src/producer/partitioners/builtIn'))
    return observe(base(), () => {
      partitionerCalls += 1
    })
  }

  const producer = kafka.producer({ linger: 5, createPartitioner })
  const answers = new Map()
  producer.on(producer.events.REQUEST, ({ payload }) => {
    if (payload.apiName !== 'Produce') return
    answers.set(payload.broker, [...(answers.get(payload.broker) ?? []), payload.duration])
  })
  const slow = scenario === 'slow-broker' ? endpoints[1] : null
  if (slow !== null) {
    const [host, port] = slow.split(':')
    factory.arm({ host, port: Number(port) }, (socket) => socket.delay({ read: 300 }))
  }
  await producer.connect()

  const value = Buffer.alloc(200, 'x')
  const perPartition = new Array(6).fill(0)
  const latencies = []
  let delivered = 0
  let rejected = 0
  let unsettled = 0
  let backlogAt6s = 0
  let backlogMax = 0
  const sendOne = () => {
    const begun = process.hrtime.bigint()
    unsettled += 1
    return producer.send({ topic, messages: [{ value }] }).then(
      ([{ partition }]) => {
        unsettled -= 1
        delivered += 1
        perPartition[partition] += 1
        latencies.push(Number(process.hrtime.bigint() - begun) / 1e6)
      },
      () => {
        unsettled -= 1
        rejected += 1
      }
    )
  }
  for (let i = 0; i < 1000; i += 1) await producer.send({ topic, messages: [{ value }] })
  answers.clear()
  partitionerCalls = 0
  const before = await retainedHeap()
  const started = clock()
  let offered = 0
  let window = { sampleAtMs: 0, maxTickDriftMs: 0, lastOfferAtMs: 0, ticks: 0 }
  if (scenario === 'healthy') {
    const inFlight = new Set()
    for (let i = 0; i < records; i += 1) {
      offered += 1
      const sent = sendOne().finally(() => inFlight.delete(sent))
      inFlight.add(sent)
      if (inFlight.size >= concurrency) await Promise.race(inFlight)
    }
    await Promise.all(inFlight)
  } else {
    window = await new OfferWindow().run(sendOne, () => unsettled)
    offered = window.offered
    backlogAt6s = window.backlogAt6s
    backlogMax = window.backlogMax
    /** @returns {boolean} work still unsettled inside the absolute drain bound */
    const draining = () => unsettled > 0 && clock() - started < 30000
    while (draining()) await sleep(Math.min(10, Math.max(0, 30000 - (clock() - started))))
  }
  const drainAtMs = clock() - started
  const requests = [...answers.values()].reduce((sum, list) => sum + list.length, 0)
  const slowAnswers = slow === null ? [] : (answers.get(slow) ?? [])
  const otherAnswers = [...answers]
    .filter(([broker]) => broker !== slow)
    .flatMap(([, list]) => list)
  const result = {
    records: scenario === 'healthy' ? records : 120000,
    offered,
    ...window,
    drainAtMs,
    gcAvailable: 1,
    topologyValid,
    delivered,
    rejected,
    unsettled,
    recordsPerRequest: delivered / Math.max(1, requests),
    balance: balanceOf(perPartition),
    p99Ms: quantile(latencies, 0.99),
    backlogAt6s,
    backlogMax,
    partitionerCalls,
    slowMedianMs: quantile(slowAnswers, 0.5),
    otherMedianMs: quantile(otherAnswers, 0.5),
    keyedIdentical: keyedIdentical(Partitioners, base),
    heapRetainedBytes: Math.max(0, (await retainedHeap()) - before),
  }
  result.valid = validRun(scenario, result) ? 1 : 0
  if (unsettled === 0) await producer.disconnect()
  return result
}

/**
 * A session of a scenario: its arms and an A/A copy of its reference, each run in its own process.
 * @param {typeof DEFAULTS} options with `session` and `raw`
 * @returns {{ summary: import('./session').Summary, verdict: ReturnType<typeof judge> }}
 */
const compare = (options) => {
  const { arms, reference } = SCENARIOS[options.scenario]
  const shared = [
    '--scenario',
    options.scenario,
    '--records',
    String(options.records),
    '--concurrency',
    String(options.concurrency),
    '--brokers',
    options.brokers,
    '--tree',
    options.tree,
  ]
  fs.writeFileSync(options.raw, '')
  const observations = runSession({
    arms: arms.map((arm) => ({
      label: arm,
      run: () => {
        const child = spawnSync(
          process.execPath,
          ['--expose-gc', __filename, ...shared, '--arm', arm],
          {
            encoding: 'utf8',
            timeout: 600000,
          }
        )
        if (child.status !== 0) throw new Error(`the ${arm} run failed: ${child.stderr}`)
        const result = JSON.parse(child.stdout)
        result.valid = validRun(options.scenario, result) ? 1 : 0
        return result
      },
    })),
    runs: options.runs,
    reference,
    onObservation: (observation) =>
      fs.appendFileSync(options.raw, `${JSON.stringify(observation)}\n`),
  })
  const summary = summarize(observations, { metrics: METRICS, reference, controlOn: ['p99Ms'] })
  return { summary, verdict: judge(options.scenario, summary) }
}

module.exports = {
  OfferWindow,
  validRun,
  retainedHeap,
  HEAP_LIMIT,
  parseArgs,
  quantile,
  balanceOf,
  keyedIdentical,
  judge,
  run,
  compare,
  METRICS,
}

if (require.main === module) {
  let options
  try {
    options = parseArgs(process.argv.slice(2))
  } catch (e) {
    console.error(e.message)
    process.exit(2)
  }
  if (!options.session) {
    run(options).then(
      (result) => {
        console.log(JSON.stringify(result))
        process.exit(0)
      },
      (e) => {
        console.error(e)
        process.exit(1)
      }
    )
  } else {
    try {
      const { summary, verdict } = compare(options)
      console.log(JSON.stringify({ arms: summary.arms, void: summary.void, ...verdict }))
      process.exitCode =
        verdict.verdict === 'passed' ? exitCode(summary) : verdict.verdict === 'failed' ? 1 : 3
    } catch (e) {
      console.error(e)
      process.exit(1)
    }
  }
}
