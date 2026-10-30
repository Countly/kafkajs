const path = require('path')
const { performance, PerformanceObserver } = require('perf_hooks')

/**
 * Exercises the real internal ConsumerGroup fetch path on deterministic broker stubs.
 * All measurement wrappers live on this driver-owned instance, never production prototypes.
 * @class
 */
class FetchRoundDriver {
  /**
   * @param {{tree?:string,partitions?:number,data?:boolean}} [options]
   * @returns {FetchRoundDriver} primed classic assignment, no sockets or membership
   * @throws {Error} invalid partition count or incompatible internal baseline API
   */
  constructor({ tree = path.resolve(__dirname, '../..'), partitions = 1000, data = false } = {}) {
    if (![100, 1000, 5000].includes(partitions))
      throw new Error('partitions must be 100, 1000 or 5000')
    const ConsumerGroup = require(path.join(tree, 'src/consumer/consumerGroup'))
    const OffsetManager = require(path.join(tree, 'src/consumer/offsetManager'))
    const Emitter = require(path.join(tree, 'src/instrumentation/emitter'))
    const logger = {
      namespace() {
        return this
      },
      debugEnabled: () => false,
      debug() {},
      info() {},
      warn() {},
      error() {},
    }
    const metadata = Array.from({ length: partitions }, (_, partitionId) => ({
      partitionId,
      leader: partitionId % 3,
      replicas: [partitionId % 3],
    }))
    const cache = {}
    const topic = 'round'
    const ids = metadata.map((p) => p.partitionId)
    const responses = [0, 1, 2].map((nodeId) => ({
      errorCode: 0,
      responses: [
        {
          topicName: topic,
          partitions: ids
            .filter((id) => id % 3 === nodeId)
            .map((partition) => ({
              partition,
              errorCode: 0,
              highWatermark: '1',
              lastStableOffset: '1',
              logStartOffset: '0',
              preferredReadReplica: -1,
              abortedTransactions: [],
              messages: data
                ? [
                    {
                      offset: '0',
                      key: null,
                      value: Buffer.from('v'),
                      timestamp: '1',
                      attributes: 0,
                    },
                  ]
                : [],
            })),
        },
      ],
    }))
    const brokers = responses.map((response) => ({
      fetch: async () => {
        this.counts.requests++
        this.responseStarted = process.hrtime.bigint()
        return response
      },
    }))
    const cluster = {
      committedOffsets: () => cache,
      topicId: () => undefined,
      leaderEpoch: () => -1,
      refreshMetadataIfNecessary: async () => {},
      refreshMetadata: async () => {},
      findTopicPartitionMetadata: () => metadata,
      findBroker: async ({ nodeId }) => brokers[nodeId],
    }
    const options = {
      cluster,
      groupId: 'round-driver',
      topics: [topic],
      topicConfigurations: { [topic]: {} },
      logger,
      instrumentationEmitter: new Emitter(),
      assigners: [],
      retry: { retries: 0 },
      sessionTimeout: 45000,
      rebalanceTimeout: 60000,
      maxBytesPerPartition: 32768,
      minBytes: 1,
      maxBytes: 10485760,
      maxWaitTimeInMs: 5000,
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      isolationLevel: 1,
      rackId: '',
      checkCrcs: true,
    }
    this.group = new ConsumerGroup(options)
    this.group.subscriptionState.assign([{ topic, partitions: ids }])
    this.group.assignmentEpoch++
    for (const partition of ids) {
      const acquiredAt = ++this.group.acquireClock
      this.group.acquired.set(`${topic}|${partition}`, {
        acquiredAt,
        changedAt: acquiredAt,
      })
    }
    this.group.offsetManager = new OffsetManager({
      ...options,
      memberAssignment: { [topic]: ids },
      coordinator: {},
      generationId: 0,
      memberId: 'round',
    })
    for (const id of ids) this.group.offsetManager.committedOffsets()[topic][id] = '0'
    this.partitions = partitions
    this.expectedByNode = [0, 1, 2].map((nodeId) => Math.ceil((partitions - nodeId) / 3))
    this.restores = []
    this.reset()
    for (const [owner, key, site] of [
      [this.group, 'filterPartitionsByNode', 'groupingNs'],
      [cluster, 'findTopicPartitionMetadata', 'lookupNs'],
      [this.group, 'buildFetchRequests', 'buildNs'],
    ]) {
      const original = owner[key]
      const own = Object.hasOwn(owner, key)
      this.restores.push(() => {
        if (own) owner[key] = original
        else delete owner[key]
      })
      owner[key] = (...args) => {
        const started = process.hrtime.bigint()
        try {
          return original.apply(owner, args)
        } finally {
          this.counts[site] += Number(process.hrtime.bigint() - started)
          if (site === 'lookupNs') this.counts.lookups++
        }
      }
    }
  }

  /**
   * Removes the instance wrappers, leaving the group and stub cluster with their original methods.
   * Safe after a failed round and when called twice.
   * @returns {void} later rounds run unwrapped and count nothing
   * @throws Never.
   */
  close() {
    for (const restore of this.restores.splice(0)) restore()
  }

  /**
   * Clears only measurement counters between complete cycles.
   * @returns {void} assignment and validated positions stay warm
   * @throws Never.
   */
  reset() {
    this.counts = {
      lookups: 0,
      batches: 0,
      requests: 0,
      groupingNs: 0,
      lookupNs: 0,
      buildNs: 0,
      responseNs: 0,
    }
  }

  /**
   * Fetches one complete three-broker cycle and verifies required work.
   * Response timing includes outer request lookups, partition handlers and final stamps.
   * @returns {Promise<object>} counts, nested isolated intervals and per-node lookups and
   *   batches for this cycle
   * @throws {Error} skipped lookup, request or batch, or a node that built other than its share
   *   of the partitions; any underlying runtime error
   */
  async round() {
    this.reset()
    const lookupsByNode = []
    const batchesByNode = []
    for (const nodeId of [0, 1, 2]) {
      this.responseStarted = null
      const lookupsBefore = this.counts.lookups
      const batches = await this.group.fetch(nodeId)
      if (this.responseStarted !== null)
        this.counts.responseNs += Number(process.hrtime.bigint() - this.responseStarted)
      this.counts.batches += batches.length
      lookupsByNode.push(this.counts.lookups - lookupsBefore)
      batchesByNode.push(batches.length)
    }
    const result = { ...this.counts, lookupsByNode, batchesByNode }
    if (
      this.counts.lookups !== 3 ||
      this.counts.requests !== 3 ||
      this.counts.batches !== this.partitions ||
      lookupsByNode.some((lookups) => lookups !== 1) ||
      batchesByNode.some((batches, nodeId) => batches !== this.expectedByNode[nodeId])
    )
      throw new Error(`Incomplete round work: ${JSON.stringify(result)}`)
    return result
  }
}

/**
 * Measures one warmed complete round in an observed no-GC allocation window.
 * Site intervals overlap: lookup is nested in grouping; never sum the sites.
 * @param {FetchRoundDriver} driver identical driver bytes in both trees
 * @param {object} [env] replaceable clock sources for the validity controls
 * @param {typeof PerformanceObserver} [env.Observer] gc entry observer
 * @param {{now: function(): number}} [env.clock] window clock on the entries' time base
 * @param {function(): number} [env.heapUsed] heap reader
 * @returns {Promise<object>} ns/node/round, allocated bytes/round and retained heap
 * @throws {Error} missing exposed GC, skipped work, observed GC during the window or a negative
 *   allocation; the observer is disconnected on every path
 */
async function measure(
  driver,
  {
    Observer = PerformanceObserver,
    clock = performance,
    heapUsed = () => process.memoryUsage().heapUsed,
  } = {}
) {
  if (typeof global.gc !== 'function') throw new Error('Use --expose-gc')
  await driver.round()
  global.gc()
  const retainedBefore = heapUsed()
  const observed = []
  const observer = new Observer((list) => observed.push(...list.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  const began = clock.now()
  const heapBefore = heapUsed()
  const start = process.hrtime.bigint()
  let counts, elapsed, allocated, ended
  try {
    counts = await driver.round()
    elapsed = Number(process.hrtime.bigint() - start)
    allocated = heapUsed() - heapBefore
    ended = clock.now()
    await new Promise(setImmediate)
    observed.push(...observer.takeRecords())
  } finally {
    observer.disconnect()
  }
  if (
    observed.some((entry) => entry.startTime >= began && entry.startTime <= ended) ||
    allocated < 0
  )
    throw new Error('Void no-GC window')
  global.gc()
  return {
    ...counts,
    nsPerNodeRound: elapsed / 3,
    allocatedBytesPerRound: allocated,
    retainedHeapDelta: heapUsed() - retainedBefore,
  }
}

/**
 * Runs internal measurement or untimed work controls without opening a socket.
 * @param {string[]} args CLI flags --partitions, --tree, --data and --control
 * @returns {Promise<void>} one JSON record
 * @throws {Error} invalid arguments or failed work/GC control
 */
async function main(args = process.argv.slice(2)) {
  const options = {}
  const allowed = new Set(['--partitions', '--tree', '--data', '--control'])
  let control = false
  for (let i = 0; i < args.length; i++) {
    const flag = args[i]
    if (!allowed.has(flag)) throw new Error(`Unknown option ${flag}`)
    if (flag === '--control') control = true
    else if (flag === '--data') options.data = true
    else {
      if (!args[i + 1]) throw new Error(`Missing value ${flag}`)
      options[flag.slice(2)] = flag === '--partitions' ? Number(args[++i]) : args[++i]
    }
  }
  const driver = new FetchRoundDriver(options)
  try {
    process.stdout.write(
      JSON.stringify(control ? await driver.round() : await measure(driver)) + '\n'
    )
  } finally {
    driver.close()
  }
}
if (require.main === module)
  main().catch((error) => {
    process.stderr.write(error.stack + '\n')
    process.exitCode = 1
  })
module.exports = { FetchRoundDriver, measure, main }
