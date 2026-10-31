const { Kafka, logLevel } = require('../../../../index')
const Broker = require('../../../broker')
const {
  createConnectionPool,
  newLogger,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
} = require('testHelpers')
const { readLog } = require('../../../../testHelpers/rawLog')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')
const { runWithEvidence } = require('./evidenceCase')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  delay,
} = require('../../../producer/__tests__/fixtures/bounded')

const STEP_MS = 30000
const DISCONNECT_MS = 4 * STOP_GRACE_MS
const CLEANUP_BUDGET_MS = 15000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const MEMBERSHIP_EVENTS = ['GROUP_JOIN', 'REBALANCING', 'PARTITIONS_ASSIGNED', 'PARTITIONS_REVOKED']

/**
 * @param {{topic: string, partition: number, value: string}} record
 * @returns {string} the identity every set of a scenario is keyed by
 */
const identityOf = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * The acknowledged records a handler never saw.
 * @param {Array<{topic: string, partition: number, value: string}>} acknowledged what the broker acknowledged
 * @param {Array<{topic: string, partition: number, value: string}>} delivered what the handlers saw
 * @returns {string[]} identities, in acknowledgement order
 */
const missingFrom = (acknowledged, delivered) => {
  const seen = new Set(delivered.map(identityOf))
  return acknowledged.map(identityOf).filter((id) => !seen.has(id))
}

/**
 * The deliveries handed out again below the broker-confirmed committed offset of their partition:
 * a replay may start at the last confirmed commit and never before it.
 * @param {Array<{partition: number, offset: string, seq: number}>} delivered the deliveries of every member
 * @param {Map<number, number>} floors the committed next offset of each partition, read from the
 *   broker before the disturbance
 * @param {number} afterSeq only deliveries stamped after this can be replays
 * @returns {string[]} the partition and offset of each violation
 */
const replaysBelowFloor = (delivered, floors, afterSeq) => {
  const first = new Map()
  for (const { partition, offset, seq } of [...delivered].sort((a, b) => a.seq - b.seq)) {
    const key = `${partition}|${offset}`
    if (!first.has(key)) first.set(key, seq)
    else if (seq > afterSeq && Number(offset) < (floors.get(partition) ?? 0)) return [key]
  }
  return []
}

/**
 * @param {AbortSignal} scope the case scope
 * @param {string} label what the step is, for its time-out message
 * @param {() => Promise<T>} op
 * @param {number} [ms] the step's budget
 * @returns {Promise<T>} what `op` returned in time
 * @template T
 */
const step = (scope, label, op, ms = STEP_MS) => bounded(scope, ms, op, { label })

/**
 * A topic with a fixed acknowledged corpus, the case's own admin and producer, and independent
 * reads of the broker's committed offsets, group membership and raw log. Every client it makes is
 * closed by the case's cleanup stack, in separate bounded steps.
 */
class LiveCorpus {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope the case scope
   * @param {import('../../../producer/__tests__/fixtures/liveProbe').CleanupStack} options.cleanup
   * @param {string} options.prefix names the topic, groups and clients
   * @param {number} [options.partitions=3]
   * @param {number} [options.replicationFactor=3]
   * @param {Array<{name: string, value: string}>} [options.configEntries] topic configuration
   * @param {object} [options.producer] extra producer options
   */
  constructor({
    scope,
    cleanup,
    prefix,
    partitions = 3,
    replicationFactor = 3,
    configEntries = [],
    producer = {},
  }) {
    this.scope = scope
    this.cleanup = cleanup
    this.id = secureRandom()
    this.prefix = prefix
    this.partitions = partitions
    this.replicationFactor = replicationFactor
    this.configEntries = configEntries
    this.producerOptions = producer
    this.topic = `e2e-p9c-${prefix}-${this.id}`
    this.groupId = `e2e-p9c-${prefix}-group-${this.id}`
    this.brokers = plainTextBrokers()
    this.kafka = new Kafka({
      clientId: `e2e-p9c-${prefix}-${this.id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer(producer)
    /** @type {Array<{topic: string, partition: number, value: string}>} what the broker acknowledged */
    this.acknowledged = []
    this.sent = 0
    this.clock = 0
  }

  /** @returns {number} the next stamp of the case's one delivery order */
  tick() {
    return this.clock++
  }

  /**
   * Creates the topic and connects the admin and the producer, each as its own bounded step, and
   * registers their closes and the topic deletion as separate cleanups.
   * @returns {Promise<void>}
   */
  async prepare() {
    const { scope, cleanup, admin, producer, topic } = this
    cleanup.add('admin disconnect', () => bounded(NEVER, DISCONNECT_MS, () => admin.disconnect()))
    cleanup.add('topic deletion', (signal) => this.deleteTopic(signal))
    cleanup.add('producer disconnect', () =>
      bounded(NEVER, DISCONNECT_MS, () => producer.disconnect())
    )
    await step(scope, 'The admin connect', () => admin.connect())
    await step(scope, 'The topic creation', () =>
      admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic,
            numPartitions: this.partitions,
            replicationFactor: this.replicationFactor,
            configEntries: this.configEntries,
          },
        ],
      })
    )
    await step(scope, 'The producer connect', () => producer.connect())
  }

  /**
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>}
   * @throws {Error} a deletion failure other than a missing topic
   */
  async deleteTopic(signal) {
    try {
      await bounded(signal, CLEANUP_BUDGET_MS, () =>
        this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 })
      )
    } catch (error) {
      if (error.type !== MISSING_TOPIC) throw error
    }
  }

  /**
   * Produces records spread over the partitions with acks from all in-sync replicas. Each value is
   * unique across the case, and a record is acknowledged only once the broker answered.
   * @param {number} count how many records
   * @returns {Promise<Array<{topic: string, partition: number, value: string}>>} what was acknowledged
   */
  async produce(count) {
    const records = Array.from({ length: count }, () => {
      const index = this.sent++
      return {
        topic: this.topic,
        partition: index % this.partitions,
        value: `${this.prefix}-${index}`,
      }
    })
    await step(this.scope, 'The corpus production', () =>
      this.producer.send({
        topic: this.topic,
        acks: -1,
        messages: records.map(({ partition, value }) => ({ partition, value })),
      })
    )
    this.acknowledged.push(...records)
    return records
  }

  /**
   * An independent read of a group's committed next offsets, through the case's own admin.
   * @param {string} [groupId] the group, this case's by default
   * @returns {Promise<Map<number, number>>} by partition; -1 for none
   */
  async committed(groupId = this.groupId) {
    const [{ partitions }] = await step(this.scope, 'The independent offset read', () =>
      this.admin.fetchOffsets({ groupId, topics: [this.topic] })
    )
    return new Map(partitions.map(({ partition, offset }) => [partition, Number(offset)]))
  }

  /**
   * An independent read of a group's members, through the case's own admin.
   * @param {string} groupId
   * @returns {Promise<Array<{memberId: string, groupInstanceId: string|null, partitions: string[]}>>}
   *   each member's instance id and its assigned partitions as sorted `topic-partition` names
   */
  async members(groupId) {
    const { groups } = await step(this.scope, 'The independent group read', () =>
      this.admin.describeGroups([groupId])
    )
    return groups[0].members.map(({ memberId, groupInstanceId, assignment = [] }) => ({
      memberId,
      groupInstanceId: groupInstanceId ?? null,
      partitions: assignment
        .flatMap(({ topic, partitions }) => partitions.map((partition) => `${topic}-${partition}`))
        .sort(),
    }))
  }

  /**
   * Reads the whole log straight from the partition leaders, without a consumer group.
   * @param {number} [isolationLevel] 0 reads uncommitted, 1 committed
   * @returns {Promise<Map<number, {endOffset: string, records: object[], abortedTransactions: object[]}>>}
   */
  rawLog(isolationLevel = 0) {
    return step(this.scope, 'The raw log read', () =>
      readLog({ brokers: this.brokers, topic: this.topic, isolationLevel })
    )
  }

  /**
   * The node currently coordinating a group, found through a short-lived seed broker.
   * @param {string} groupId
   * @returns {Promise<number>} the coordinator's node id
   */
  coordinatorOf(groupId) {
    return step(this.scope, 'The coordinator lookup', async () => {
      const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
      try {
        await seed.connect()
        const { coordinator } = await seed.findGroupCoordinator({ groupId })
        return coordinator.nodeId
      } finally {
        await seed.disconnect()
      }
    })
  }

  /**
   * Finds a group id, derived from this case's, that the given node coordinates, so two groups of
   * different protocols share one broker and one outage touches both.
   * @param {number} nodeId the coordinator wanted
   * @param {string} stem the group id's stem
   * @returns {Promise<string>} the group id
   * @throws {Error} when no candidate lands on that node
   */
  async groupOn(nodeId, stem) {
    for (let candidate = 0; candidate < 200; candidate++) {
      const groupId = `${stem}-${candidate}`
      if ((await this.coordinatorOf(groupId)) === nodeId) return groupId
    }
    throw new Error(`No group id of ${stem} lands on node ${nodeId}`)
  }
}

/**
 * One consumer with a journal of what happened to it: deliveries with their offsets, membership
 * events, crashes and commits, each stamped with the wall clock so an outage window can be
 * compared with them.
 */
class JournalMember {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope the case scope
   * @param {import('../../../producer/__tests__/fixtures/liveProbe').CleanupStack} options.cleanup
   * @param {LiveCorpus} options.corpus
   * @param {string} options.name names the client and the member in the evidence
   * @param {string} options.groupId
   * @param {object} [options.consumer] consumer options: protocol, instance id, session timeout
   * @param {object} [options.run] run options
   * @param {Function} [options.socketFactory] the client's socket factory, to observe its frames
   * @param {(context: {topic: string, partition: number, message: object}) => void} [options.onMessage]
   *   sees each delivered message beside the journal
   */
  constructor({
    scope,
    cleanup,
    corpus,
    name,
    groupId,
    consumer = {},
    run = {},
    socketFactory,
    onMessage,
  }) {
    this.scope = scope
    this.corpus = corpus
    this.name = name
    this.groupId = groupId
    this.runOptions = run
    this.onMessage = onMessage
    this.client = new Kafka({
      clientId: `e2e-p9c-${corpus.prefix}-${name}-${corpus.id}`,
      brokers: corpus.brokers,
      logLevel: logLevel.NOTHING,
      ...(socketFactory ? { socketFactory } : {}),
    }).consumer({ groupId, maxWaitTimeInMs: 500, ...consumer })
    /** @type {Array<{partition: number, offset: string, value: string, topic: string, seq: number}>} */
    this.delivered = []
    /** @type {Array<{kind: string, at: number, [key: string]: unknown}>} */
    this.journal = []
    this.crashes = []
    this.stopped = false
    const { events } = this.client
    for (const kind of [...MEMBERSHIP_EVENTS, 'PARTITIONS_LOST', 'COMMIT_OFFSETS']) {
      this.client.on(events[kind], ({ payload }) =>
        this.journal.push({ kind, at: Date.now(), partitions: payload?.partitions ?? null })
      )
    }
    this.client.on(events.CRASH, ({ payload }) => {
      this.crashes.push({
        at: Date.now(),
        type: payload.error.type,
        code: payload.error.code,
        restart: payload.restart,
      })
      this.journal.push({ kind: 'CRASH', at: Date.now() })
    })
    cleanup.add(`consumer ${name}`, () =>
      bounded(NEVER, DISCONNECT_MS, () => this.client.disconnect({ timeout: 0 }))
    )
  }

  /**
   * Connects, subscribes from the beginning and runs.
   * @returns {Promise<void>}
   */
  async start() {
    const { scope, client, corpus, name } = this
    await step(scope, `The ${name} connect`, () => client.connect())
    await step(scope, `The ${name} subscribe`, () =>
      client.subscribe({ topic: corpus.topic, fromBeginning: true })
    )
    await step(scope, `The ${name} run`, () =>
      client.run({
        ...this.runOptions,
        eachMessage: async ({ topic, partition, message }) => {
          this.onMessage?.({ topic, partition, message })
          this.delivered.push({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
            seq: this.corpus.tick(),
          })
        },
      })
    )
  }

  /**
   * Stops the member, with or without leaving its group.
   * @param {{leaveGroup?: boolean}} [options]
   * @returns {Promise<void>}
   */
  async stop({ leaveGroup = true } = {}) {
    await step(this.scope, `The ${this.name} disconnect`, () =>
      this.client.disconnect({ leaveGroup })
    )
    this.stopped = true
  }

  /**
   * @param {number} from wall-clock ms
   * @param {number} [to=Infinity] wall-clock ms
   * @returns {Array<{kind: string, at: number}>} the membership events inside the window
   */
  membershipBetween(from, to = Infinity) {
    return this.journal.filter(
      ({ kind, at }) => MEMBERSHIP_EVENTS.includes(kind) && at >= from && at <= to
    )
  }

  /**
   * @param {string} instanceCode the error type to look for
   * @returns {Array<{type: string, restart: boolean}>} the crashes of that type
   */
  crashesOf(instanceCode) {
    return this.crashes.filter(({ type }) => type === instanceCode)
  }
}

/**
 * Waits until the members' deliveries cover the acknowledged records.
 * @param {AbortSignal} scope
 * @param {LiveCorpus} corpus
 * @param {JournalMember[]} members
 * @param {number} ms the budget of the wait
 * @returns {Promise<void>}
 * @throws {Error} naming the first missing identities when the budget passes
 */
const untilCovered = (scope, corpus, members, ms = STEP_MS * 2) =>
  bounded(
    scope,
    ms,
    async (inner) => {
      for (;;) {
        const missing = missingFrom(
          corpus.acknowledged,
          members.flatMap(({ delivered }) => delivered)
        )
        if (missing.length === 0) return
        await delay(inner, 250)
      }
    },
    { label: 'The delivery of the acknowledged records' }
  )

/**
 * Waits until no member journaled a membership event for a quiet interval.
 * @param {AbortSignal} scope
 * @param {JournalMember[]} members
 * @param {number} quietMs how long the journals must stay silent
 * @param {number} ms the budget of the wait
 * @returns {Promise<void>}
 */
const untilQuiet = (scope, members, quietMs, ms = STEP_MS * 2) =>
  bounded(
    scope,
    ms,
    async (inner) => {
      for (;;) {
        const last = Math.max(
          0,
          ...members.flatMap((member) => member.membershipBetween(0).map(({ at }) => at))
        )
        if (Date.now() - last >= quietMs) return
        await delay(inner, 250)
      }
    },
    { label: 'The group to stay stable' }
  )

/**
 * Runs one arm under its cleanup stack and writes its evidence row in a `finally`, before any
 * cleanup runs, with the log, broker and handler sets kept apart.
 * @param {object} options
 * @param {string} options.evidence the scenario's evidence name
 * @param {string} options.title the arm's title
 * @param {object} options.config what the row records
 * @param {number} options.budgetMs the arm's budget
 * @param {{log: string, offsets: string, handlers: string}} options.replay the replay sets the arm declares
 * @param {(ctx: {cleanup: CleanupStack, observed: object}) => Promise<LiveCorpus>} options.body
 *   runs the arm and returns the corpus whose acknowledged records the row counts
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, a failed evidence write, or the failed cleanups
 */
const runArm = async ({ evidence, title, config, budgetMs, replay, body }) => {
  const cleanup = new CleanupStack()
  const observed = { stage: 'starting' }
  let corpus = null
  await runWithEvidence(
    cleanup,
    async (failure) => {
      try {
        writeEvidence(evidence, {
          scenario: evidence,
          case: title,
          config: { ...config, budgetMs },
          replay,
          acknowledged: corpus?.acknowledged.length ?? 0,
          observed: failure ? { ...observed, error: describeError(failure) } : observed,
        })
        return null
      } catch (error) {
        return error
      }
    },
    async () => {
      observed.stage = 'body'
      corpus = await body({ cleanup, observed })
      observed.stage = 'passed'
    },
    CLEANUP_BUDGET_MS * 4
  )
}

module.exports = {
  CLEANUP_BUDGET_MS,
  JournalMember,
  LiveCorpus,
  STEP_MS,
  identityOf,
  missingFrom,
  replaysBelowFloor,
  runArm,
  step,
  untilCovered,
  untilQuiet,
}
