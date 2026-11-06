const Broker = require('../src/broker')
const Cluster = require('../src/cluster')
const ConnectionPool = require('../src/network/connectionPool')
const InstrumentationEventEmitter = require('../src/instrumentation/emitter')
const { ConsumerOperation } = require('../src/consumer/groupProtocol/interface')
const { ShareSession } = require('../src/consumer/share/shareSession')
const { ShareFetchLoop } = require('../src/consumer/share/shareFetchLoop')
const {
  batch,
  record,
} = require('../src/protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const { newLogger } = require('./index')
const { createScriptedCluster } = require('./scriptedCluster')
const { Schedule } = require('./schedule')

/** Two topic ids the share specs use as topics A and B. */
const A = '00112233-4455-6677-8899-aabbccddeeff'
const B = '10112233-4455-6677-8899-aabbccddeeff'

/**
 * @param {number|string|bigint} first First offset.
 * @param {number|string|bigint} last Last offset.
 * @param {number} [deliveryCount=1] Delivery count of the range.
 * @returns {object} One acquired range with the wire pair's literal string offsets.
 */
const acquired = (first, last, deliveryCount = 1) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  deliveryCount,
})

/**
 * @param {number|string|bigint} base Base offset of the batch.
 * @param {string[]} values Record values, one record each.
 * @returns {string} Hex of one valid record batch carrying those records.
 */
const recordsHex = (base, values) =>
  batch({
    baseOffset: BigInt(base),
    records: values.map((value, i) => record({ offsetDelta: i, key: `k${i}`, value })),
  }).toString('hex')

/**
 * @param {object} options Partition answer fields; every one has a quiet default.
 * @param {number} options.partition Partition index.
 * @returns {object} A complete ShareFetch partition answer.
 */
const part = ({
  topicId = A,
  partition,
  code = 0,
  ack = 0,
  leader = { leaderId: -1, leaderEpoch: -1 },
  acquire = [],
  records = '',
}) => ({
  topicId,
  partitionIndex: partition,
  errorCode: code,
  errorMessage: null,
  acknowledgeErrorCode: ack,
  acknowledgeErrorMessage: null,
  currentLeader: leader,
  acquiredRecords: acquire,
  records,
})

/**
 * @param {object[]} rows Partition answers that carry their `topicId`.
 * @returns {object[]} The rows grouped by topic id the way the wire answer lists them.
 */
const group = (rows) => {
  const topics = new Map()
  for (const { topicId, ...partition } of rows) {
    if (!topics.has(topicId)) topics.set(topicId, { topicId, partitions: [] })
    topics.get(topicId).partitions.push(partition)
  }
  return [...topics.values()]
}

/**
 * @param {object[]} rows Complete partition answers.
 * @param {object} [extra] Top-level fields laid over the defaults.
 * @returns {object} A ShareFetch answer.
 */
const fetchAnswer = (rows, extra = {}) => ({
  errorCode: 0,
  errorMessage: null,
  throttleTime: 0,
  acquisitionLockTimeoutMs: 15000,
  nodeEndpoints: [],
  responses: group(rows),
  ...extra,
})

/**
 * @param {Array<{topicId?: string, partition: number, code?: number}>} rows One row per partition.
 * @param {object} [extra] Top-level fields laid over the defaults.
 * @returns {object} A ShareAcknowledge answer.
 */
const ackAnswer = (rows, extra = {}) => ({
  errorCode: 0,
  errorMessage: null,
  throttleTime: 0,
  acquisitionLockTimeoutMs: 15000,
  nodeEndpoints: [],
  responses: group(
    rows.map(({ topicId = A, partition, code = 0 }) => ({
      topicId,
      partitionIndex: partition,
      errorCode: code,
      errorMessage: null,
      currentLeader: { leaderId: -1, leaderEpoch: -1 },
    }))
  ),
  ...extra,
})

/**
 * @param {number} code Top-level error code.
 * @returns {object} A top-level failure answer with no partitions.
 */
const topError = (code) => fetchAnswer([], { errorCode: code })

/**
 * @param {'ShareFetch'|'ShareAcknowledge'} api The request's API name.
 * @param {object} fields The request's decoded fields.
 * @returns {object} The answer a quiet broker gives: every listed partition confirmed, no records.
 */
const echo = (api, fields) => {
  const listed = fields.topics.flatMap((topic) =>
    topic.partitions.map((partition) => ({
      topicId: topic.topicId,
      partition: partition.partitionIndex,
    }))
  )
  return api === 'ShareFetch'
    ? fetchAnswer(listed.map(({ topicId, partition }) => part({ topicId, partition })))
    : ackAnswer(listed)
}

/**
 * @param {object} cluster Scripted cluster.
 * @param {...number} keys API keys; both share APIs when none is given.
 * @returns {object[]} The share frames the client wrote, oldest first.
 */
const frames = (cluster, ...keys) =>
  cluster
    .requests()
    .filter(({ apiKey }) => (keys.length ? keys.includes(apiKey) : apiKey >= 78 && apiKey <= 79))

/**
 * Lets queued socket and promise work run without any timer. Use it to prove that something does
 * not happen after the stage that would cause it was reached; wait for a stage with `until`.
 * @returns {Promise<void>}
 */
const flush = async () => {
  for (let turn = 0; turn < 20; turn++) await new Promise(setImmediate)
}

/**
 * Waits for a condition without a timer: one macrotask turn per check.
 * @param {() => boolean} predicate The reached-stage condition.
 * @param {string} what What is awaited, named by the failure.
 * @returns {Promise<void>}
 * @throws {Error} When the condition does not hold after 400 turns.
 */
const until = async (predicate, what) => {
  for (let turn = 0; turn < 400 && !predicate(); turn++) await new Promise(setImmediate)
  if (!predicate()) throw new Error(`${what} did not happen`)
}

/** @returns {{promise: Promise<void>, resolve: () => void}} A promise the test settles by hand. */
const deferred = () => {
  const gate = {}
  gate.promise = new Promise((resolve) => {
    gate.resolve = resolve
  })
  return gate
}

/**
 * @param {number} partition Partition index.
 * @param {number|string|bigint} base First offset acquired.
 * @param {string[]} values One record value per offset.
 * @param {object} [extra] Partition fields laid over the defaults.
 * @returns {object} The partition answer of a fetch that acquires `values` of `partition` from `base`.
 */
const acquire = (partition, base, values, extra = {}) => {
  const first = BigInt(base)
  return part({
    partition,
    acquire: [acquired(first, first + BigInt(values.length - 1))],
    records: recordsHex(first, values),
    ...extra,
  })
}

/**
 * A scripted cluster with one Broker, pool, operation, session and loop per node. Answers come
 * from `script`, falling back to the quiet broker. The loop's clock `clock.time` moves only when a
 * pacing sleep or the test moves it. A run's pacing sleeps are waits of its operation, as in
 * production, but their timers are the test's: call `run.holdSleeps()` and every later sleep waits
 * in `run.timers` until the test releases it, so the test decides when "timer delivery" happens
 * and what the clock says by then.
 * @param {object} [options] Harness options.
 * @param {number} [options.nodes=1] Node count, at most three.
 * @param {object} [options.loop] Overrides of the loop's options.
 * @param {object} [options.pool] Overrides of the connection pool's options.
 * @param {Map<string, string>} [options.names] Topic name by id for the mocked metadata.
 * @returns {Promise<object>} The harness: `cluster`, `schedule`, `clock`, `metadata`, `members`,
 *   `pools`, `newPool`, `runOn`, `gates`, `hold`, `script`, `scriptNodes`, `realCluster`, `close`.
 */
const harness = async ({
  nodes = 1,
  loop = {},
  pool: poolOptions = {},
  names = new Map([
    [A, 'topic-a'],
    [B, 'topic-b'],
  ]),
} = {}) => {
  const schedule = new Schedule()
  const topology = [0, 1, 2].slice(0, nodes).map((id) => ({ id, host: `node${id}`, port: 9092 }))
  const cluster = createScriptedCluster({ nodes: topology, profile: 'share', schedule })
  const clock = { time: 1000 }
  const gates = []
  const pools = []
  const runs = []
  const closers = []
  const metadata = {
    topicName: jest.fn((id) => names.get(id)),
    topicId: jest.fn((name) => [...names].find(([, candidate]) => candidate === name)?.[0]),
    hasNode: jest.fn(() => true),
    cachedLeader: jest.fn(() => 0),
    applyLeaderHint: jest.fn(async () => true),
    refreshMetadata: jest.fn(async () => {}),
  }
  const routing = { cluster: metadata }
  const newPool = (node) => {
    const created = new ConnectionPool({
      host: node.host,
      port: node.port,
      clientId: 'test-client',
      socketFactory: cluster.socketFactory,
      logger: newLogger(),
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      connectionsMaxIdle: 0,
      ...poolOptions,
    })
    pools.push(created)
    return created
  }
  /** One run on a node: its own operation, session, loop and observers, over the node's broker. */
  const runOn = ({ node, pool, broker }) => {
    const run = {
      node,
      pool,
      broker,
      want: [],
      handled: [],
      results: [],
      moves: [],
      fatals: [],
      resets: [],
      sleeps: [],
      timers: [],
    }
    let holding = false
    run.holdSleeps = () => {
      holding = true
    }
    /** @returns {Promise<{delay: number, release: () => void}>} The oldest held pacing timer. */
    run.nextTimer = async () => {
      await until(() => run.timers.length > 0, 'a pacing timer')
      return run.timers[0]
    }
    run.operation = new ConsumerOperation()
    jest.spyOn(run.operation, 'sleep').mockImplementation((delay) => {
      run.sleeps.push(delay)
      if (!holding) {
        clock.time += delay
        return Promise.resolve()
      }
      const gate = deferred()
      const timer = {
        delay,
        release: () => {
          run.timers.splice(run.timers.indexOf(timer), 1)
          gate.resolve()
        },
      }
      run.timers.push(timer)
      return run.operation.wait(() => gate.promise)
    })
    run.session = new ShareSession({ operation: run.operation })
    run.dispatch = async (rows) => {
      for (const row of rows) {
        const offsets = []
        row.walk.deliver.records.forEach((message, i) => {
          run.handled.push({
            partition: row.partition,
            topicId: row.topicId,
            offset: message.offset,
            value: message.value.toString(),
            deliveryCount: row.walk.deliver.deliveryCounts[i],
            token: row.token,
          })
          offsets.push(message.offset)
        })
        if (offsets.length) row.ledger.complete(offsets, 'accept')
      }
    }
    runs.push(run)
    run.loop = new ShareFetchLoop({
      session: run.session,
      groupId: 'group',
      memberId: 'member',
      minBytes: 1,
      maxBytes: 1048576,
      maxRecords: 500,
      acquireMode: 'record_limit',
      broker,
      cluster: routing.cluster,
      logger: newLogger(),
      now: () => clock.time,
      wanted: () => run.want,
      onResults: (results, row) =>
        run.results.push(
          ...results.map((outcome) => ({
            ...outcome,
            partition: row.partition,
            topicId: row.topicId,
          }))
        ),
      onMove: (move) => run.moves.push(move),
      onFatal: (error) => run.fatals.push(error),
      onReset: (open, reason, cause) => run.resets.push({ reason, cause }),
      dispatch: (rows) => run.dispatch(rows),
      ...loop,
    })
    return run
  }
  const members = []
  for (const node of topology) {
    const pool = newPool(node)
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    await broker.connect()
    members.push(runOn({ node, pool, broker }))
  }
  return {
    cluster,
    schedule,
    clock,
    metadata,
    members,
    pools,
    newPool,
    runOn,
    gates,
    hold(selector) {
      const gate = schedule.hold(selector)
      gates.push(gate)
      return gate
    },
    script(api, steps = []) {
      const queue = [...steps]
      cluster.answer(api, (fields, call) => {
        const step = queue.shift()
        const answer = typeof step === 'function' ? step(fields, call) : step
        return answer ?? echo(api, fields)
      })
    },
    scriptNodes(api, byNode) {
      cluster.answer(api, (fields, call) => {
        const step = byNode[call.node]?.shift()
        const answer = typeof step === 'function' ? step(fields, call) : step
        return answer ?? echo(api, fields)
      })
    },
    /**
     * Puts a real Cluster, with its brokers, connections and metadata cache, behind the scripted
     * wire and gives it to every run's loop, now and later, in place of the mocked metadata.
     * @param {Record<string, {topicId: string, partitions: Array<{leader: number, epoch: number}>}>} topics
     *   The metadata the wire serves, by topic name; the test edits the returned `table` to change it.
     * @returns {Promise<{cluster: Cluster, table: Map<string, object>, recreated: object[], refreshes: () => number}>}
     *   The cluster, the served table, every topic-recreated event and the count of Metadata
     *   requests written since the cluster finished connecting.
     */
    async realCluster(topics) {
      const table = new Map(Object.entries(topics))
      cluster.answer('Metadata', (fields) => ({
        brokers: topology.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
        controllerId: 0,
        topics: (fields.topics ?? [...table.keys()].map((name) => ({ name }))).map(({ name }) => {
          const entry = table.get(name)
          return {
            errorCode: entry ? 0 : 3,
            name,
            topicId: entry ? entry.topicId : '00000000-0000-0000-0000-000000000000',
            isInternal: false,
            topicAuthorizedOperations: -2147483648,
            partitions: (entry ? entry.partitions : []).map(
              ({ leader, epoch }, partitionIndex) => ({
                errorCode: 0,
                partitionIndex,
                leaderId: leader,
                leaderEpoch: epoch,
                replicas: [leader],
                isr: [leader],
                offlineReplicas: [],
              })
            ),
          }
        }),
      }))
      const { clientId, brokers, socketFactory } = cluster.kafkaOptions()
      const real = new Cluster({
        logger: newLogger(),
        clientId,
        brokers,
        socketFactory,
        connectionTimeout: 1000,
        requestTimeout: 30000,
        enforceRequestTimeout: false,
        allowAutoTopicCreation: false,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        retry: { retries: 0 },
      })
      await real.connect()
      for (const name of table.keys()) await real.addTargetTopic(name)
      const recreated = []
      real.onTopicRecreated((event) => recreated.push(event))
      const metadataFrames = () => cluster.requests().filter(({ api }) => api === 'Metadata').length
      const baseline = metadataFrames()
      closers.push(() => real.disconnect())
      routing.cluster = real
      for (const run of runs) run.loop.cluster = real
      return { cluster: real, table, recreated, refreshes: () => metadataFrames() - baseline }
    },
    async close() {
      for (const gate of gates) if (gate.settle) gate.release()
      for (const run of runs) {
        run.loop.dispose()
        for (const timer of [...run.timers]) timer.release()
      }
      await Promise.all(closers.map((close) => close().catch(() => {})))
      await Promise.all(pools.map((created) => created.destroy({ drainTimeout: 0 })))
      cluster.assertClean()
    },
  }
}

module.exports = {
  A,
  B,
  acquired,
  recordsHex,
  part,
  group,
  fetchAnswer,
  ackAnswer,
  topError,
  echo,
  frames,
  flush,
  until,
  deferred,
  acquire,
  harness,
}
