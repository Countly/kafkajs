const path = require('path')
const { spawn, spawnSync } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const Broker = require('../../broker')
const apiKeys = require('../../protocol/requests/apiKeys')
const defaultSocketFactory = require('../../network/socketFactory')
const { roundRobin, range } = require('../assigners')
const { plainTextBrokers, secureRandom, writeEvidence, wireVersionsOf } = require('testHelpers')
const { Schedule } = require('../../../testHelpers/schedule')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const {
  CleanupStack,
  describeError,
  traceCalls,
} = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: it needs a real cluster and a JVM. Run alone: `npm run test:e2e:leader-union`.
const GATED = process.env.LEADER_UNION_E2E === '1'
const describeLeaderUnion = GATED ? describe : describe.skip

const RUN_SH = path.join(__dirname, '..', '..', '..', 'scripts', 'e2e', 'javaConsumer', 'run.sh')

/**
 * Probes the Java member once, at collection time, on the controller's cached jars: a missing JDK,
 * jar directory or class is decided before the suite is built, offline, and never by a download.
 * @returns {{ ok: boolean, reason: string, workDir: string|undefined }}
 */
const preflight = () => {
  const workDir = process.env.JAVA_E2E_WORK_DIR
  if (!workDir) return { ok: false, reason: 'JAVA_E2E_WORK_DIR names no cached jars', workDir }
  const probe = spawnSync(RUN_SH, ['--probe'], {
    env: {
      ...process.env,
      JAVA_E2E_WORK_DIR: workDir,
      JAVA_E2E_OFFLINE: '1',
      KAFKA_CLIENTS_VERSION: process.env.KAFKA_CLIENTS_VERSION || '4.3.1',
    },
    encoding: 'utf8',
    timeout: 120000,
  })
  if (probe.error?.code === 'ENOENT')
    return { ok: false, reason: 'run.sh is not executable', workDir }
  if (probe.status === 0) return { ok: true, reason: '', workDir }
  const stderr = probe.stderr || ''
  const reason = /NoClassDefFoundError/.test(stderr)
    ? 'a class was missing from the classpath'
    : /command not found|javac/.test(stderr)
      ? 'no JDK on PATH'
      : 'the cached jars are missing'
  return { ok: false, reason, workDir }
}

const JAVA = GATED ? preflight() : { ok: false, reason: 'gated', workDir: undefined }
if (GATED && !JAVA.ok) {
  // A gated run that cannot start must not read as a pass.
  test('the Java member can be built and started', () => {
    throw new Error(`The leader-union scenario cannot run: ${JAVA.reason}`)
  })
}

const EVIDENCE = 'leader-union'
const CASE_BUDGET_MS = 120000
const JOIN_CASE_BUDGET_MS = 90000
const WAIT_MS = 30000
const JAVA_START_MS = 60000
const CLEANUP_BUDGET_MS = 15000
const PARTITIONS = 4
const GROWN_PARTITIONS = 8
const RECORDS_PER_PARTITION = 3
const MAX_WAIT_MS = 1000
const HEARTBEAT_MS = 1000
const METADATA_MAX_AGE_MS = 5000
// The consumer's own default, far beyond WAIT_MS.
const DEFAULT_METADATA_MAX_AGE_MS = 300000
const MAX_TRACED_CALLS = 500
const QUIET_MS = 2000
const SESSION_TIMEOUT_MS = 30000
const LOSSES = 3
const JOINER_START_MS = 45000
const ARMED_LIMIT = 10
const CHILD_RUN_MS = 100000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']
const JAVA_FAILURE = /NoClassDefFoundError|ClassNotFoundException|ConfigException/
const PASS = Symbol('pass')

/**
 * The assignors under test. `java` is the class the Java member loads; `wire` is the protocol name
 * both sides must offer, which for round robin is Java's `roundrobin`, not this client's own name.
 */
const ASSIGNORS = [
  {
    name: 'roundRobin',
    create: roundRobin,
    wire: 'roundrobin',
    java: 'org.apache.kafka.clients.consumer.RoundRobinAssignor',
  },
  {
    name: 'range',
    create: range,
    wire: 'range',
    java: 'org.apache.kafka.clients.consumer.RangeAssignor',
  },
]

/**
 * One raw client or admin operation, bounded by the case scope. It refuses to start under an
 * aborted scope, and an answer that arrives after the abort is dropped. An operation is one raw
 * call: a body that starts a second one is two steps.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the operation in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, WAIT_MS, op, { label })

/**
 * Offers an assigner under another protocol name.
 * @param {Function} create an assigner factory
 * @param {string} name the protocol name to send in JoinGroup
 * @returns {Function} a factory whose assigner differs only in its name
 */
const withWireName = (create, name) => (config) => ({ ...create(config), name })

/**
 * @param {string[]} topics
 * @returns {string[]} the `topic:partition` keys every partition of `topics` has
 */
const partitionKeys = (topics) =>
  topics.flatMap((topic) =>
    Array.from({ length: PARTITIONS }, (_, partition) => `${topic}:${partition}`)
  )

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the identity a delivery is compared by: the same value on another topic or
 *   partition is another record
 */
const identityOf = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * @param {Array<{ topic: string, partition: number, value: string }>} records
 * @returns {string[]} the sorted identities, duplicates kept
 */
const sortedIdentities = (records) => records.map(identityOf).sort()

/**
 * @param {object[]} seats every seat of a case
 * @returns {{ wireVersions?: Record<string, number> }} the version each API was sent at by the
 *   JavaScript members, or nothing when none of them sent a request
 */
const wireOf = (seats) => {
  const wireVersions = wireVersionsOf(
    seats
      .filter((seat) => seat instanceof Member)
      .flatMap(({ versions }) =>
        [...versions].map(([apiName, apiVersion]) => ({ apiName, apiVersion }))
      )
  )
  return Object.keys(wireVersions).length > 0 ? { wireVersions } : {}
}

/**
 * Collects who owns what.
 * @param {Array<{ name: string, topics: string[], assignment: () => Record<string, number[]> }>} seats
 * @returns {{ owners: Record<string, string[]>, stray: string[] }} the owners of every owned
 *   `topic:partition`, and the `seat/topic` pairs a seat holds without subscribing to the topic
 */
const ownershipOf = (seats) => {
  const owners = {}
  const stray = []
  for (const seat of seats) {
    for (const [topic, partitions] of Object.entries(seat.assignment())) {
      if (partitions.length > 0 && !seat.topics.includes(topic)) stray.push(`${seat.name}/${topic}`)
      for (const partition of partitions) (owners[`${topic}:${partition}`] ??= []).push(seat.name)
    }
  }
  return { owners, stray }
}

/**
 * @param {{ owners: Record<string, string[]>, stray: string[] }} ownership
 * @param {string[]} keys every `topic:partition` that must be owned
 * @returns {boolean} whether each key has exactly one owner, nothing else is owned and no seat
 *   holds a topic it did not subscribe to
 */
const isExclusive = ({ owners, stray }, keys) =>
  stray.length === 0 &&
  Object.keys(owners).length === keys.length &&
  keys.every((key) => owners[key]?.length === 1)

/**
 * Waits for the seats to hold every partition of `topics` exactly once, then for one quiet period
 * to show that no later round changes it.
 * @param {AbortSignal} scope the case scope
 * @param {object[]} seats
 * @param {string[]} topics
 * @param {string[]} [keys] the `topic:partition` keys to own, when the topics are not all
 *   `PARTITIONS` wide
 * @returns {Promise<{ owners: Record<string, string[]>, stray: string[] }>} what the seats hold
 * @throws {Error} when the seats do not settle exclusively within the wait
 */
const settleOwnership = async (scope, seats, topics, keys = partitionKeys(topics)) => {
  await pollUntil(scope, WAIT_MS, () => isExclusive(ownershipOf(seats), keys), {
    label: 'The exclusive ownership',
  })
  await delay(scope, QUIET_MS)
  return ownershipOf(seats)
}

/** One JavaScript consumer, recording what its handler was given and what it was told. */
class Member {
  /**
   * @param {object} options
   * @param {string} options.id the case's id
   * @param {string} options.name names the member in evidence and failures
   * @param {string[]} options.brokers
   * @param {string} options.groupId
   * @param {string[]} options.topics
   * @param {Function} options.assigner a partition assigner factory
   * @param {object} [options.consumerOptions] merged over the consumer's configuration
   * @param {object} [options.kafkaOptions] merged over the client's configuration
   */
  constructor({ id, name, brokers, groupId, topics, assigner, consumerOptions, kafkaOptions }) {
    this.name = name
    this.topics = topics
    this.clientId = `e2e-leader-union-${name}-${id}`
    this.kafka = new Kafka({
      clientId: this.clientId,
      brokers,
      logLevel: logLevel.NOTHING,
      ...kafkaOptions,
    })
    this.consumer = this.kafka.consumer({
      groupId,
      groupProtocol: 'classic',
      partitionAssigners: [assigner],
      maxWaitTimeInMs: MAX_WAIT_MS,
      heartbeatInterval: HEARTBEAT_MS,
      ...consumerOptions,
    })
    /** @type {Array<{ at: number, payload: object }>} */
    this.joins = []
    /** @type {Array<{ at: number, reason: string }>} when each rebalance began, `performance.now()` */
    this.rebalances = []
    this.unsubscribed = []
    /** @type {Array<{ at: number, error: Error }>} */
    this.crashes = []
    /** @type {Array<{ topic: string, type: string }>} subscriptions refused for a missing topic */
    this.refusedSubscriptions = []
    /** @type {Map<string, number>} the highest version each API was sent at */
    this.versions = new Map()
    /** @type {Array<{ at: number, duration: number, correlationId: number, broker: string }>} */
    this.metadataRequests = []
    /** @type {Array<{ topic: string, partition: number, value: string }>} */
    this.deliveries = []
  }

  /**
   * Connects, subscribes and starts delivering with manual commits of what the handler processed.
   * A delivery that arrives after the scope aborted is neither recorded nor committed.
   * @param {AbortSignal} scope the case scope
   * @param {object} [options]
   * @param {string[]} [options.missing] topics that may not exist yet; their subscription is
   *   allowed to fail with an unknown topic, which `subscribe` records before its metadata refresh
   * @param {number} [options.startMs] how long `run` may take: it resolves only once the first
   *   join has completed, so a join held up on purpose needs more than a step's usual wait
   * @returns {Promise<void>}
   */
  async begin(scope, { missing = [], startMs = WAIT_MS } = {}) {
    const { consumer } = this
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
      this.joins.push({ at: performance.now(), payload })
    )
    consumer.on(consumer.events.REBALANCING, ({ payload }) =>
      this.rebalances.push({ at: performance.now(), reason: payload.reason })
    )
    consumer.on(consumer.events.REQUEST, ({ payload }) => {
      const { apiName, apiVersion, correlationId, duration, broker } = payload
      this.versions.set(apiName, Math.max(this.versions.get(apiName) ?? apiVersion, apiVersion))
      if (apiName === 'Metadata') {
        this.metadataRequests.push({ at: performance.now(), duration, correlationId, broker })
      }
    })
    consumer.on(consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS, ({ payload }) =>
      this.unsubscribed.push(payload)
    )
    consumer.on(consumer.events.CRASH, ({ payload }) =>
      this.crashes.push({ at: performance.now(), error: payload.error })
    )
    await step(scope, 'The consumer connect', () => consumer.connect())
    for (const topic of this.topics) {
      await step(scope, 'The consumer subscription', () =>
        consumer.subscribe({ topic, fromBeginning: true }).catch((error) => {
          if (!missing.includes(topic) || error.type !== MISSING_TOPIC) throw error
          this.refusedSubscriptions.push({ topic, type: error.type })
        })
      )
    }
    await bounded(
      scope,
      startMs,
      () =>
        consumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            if (scope.aborted) return
            this.deliveries.push({ topic, partition, value: message.value.toString() })
            await step(scope, 'The offset commit', () =>
              consumer.commitOffsets([
                { topic, partition, offset: String(Number(message.offset) + 1) },
              ])
            )
          },
        }),
      { label: 'The consumer start' }
    )
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>} once the member has completed its first join
   */
  whenJoined(scope) {
    return pollUntil(scope, WAIT_MS, () => this.joins.length > 0, { label: 'The group join' })
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @param {object} [options] see `begin`
   * @returns {Promise<void>} once the member has started and joined
   */
  async start(scope, options) {
    await this.begin(scope, options)
    await this.whenJoined(scope)
  }

  /** @returns {object|undefined} the payload of the last completed join */
  lastJoin() {
    return this.joins.at(-1)?.payload
  }

  /** @returns {Record<string, number[]>} the partitions of the last completed join, by topic */
  assignment() {
    return this.lastJoin()?.memberAssignment ?? {}
  }

  /** @returns {string} the member id of the last completed join */
  memberId() {
    return this.lastJoin()?.memberId
  }

  /** @returns {string[]} the sorted identities the handler was given, duplicates kept */
  identities() {
    return sortedIdentities(this.deliveries)
  }

  /**
   * What this member recorded, for an evidence row. Reads only what the instrumentation events and
   * the Metadata trace kept, so it cannot fail a case that is already failing.
   * @param {MetadataTrace} trace
   * @param {number} since `performance.now()` from which Metadata is listed, 0 for all of it
   * @returns {object} the member's subscription, refusals, joins, rebalances with their reason,
   *   crashes, and the Metadata requests and answers since `since`
   */
  report(trace, since) {
    return {
      name: this.name,
      subscribed: this.topics,
      refusedSubscriptions: this.refusedSubscriptions,
      joins: this.joins.map(({ at, payload }) => ({
        at,
        memberId: payload.memberId,
        isLeader: payload.isLeader,
        memberAssignment: payload.memberAssignment,
      })),
      rebalances: this.rebalances,
      crashes: this.crashes.map(({ at, error }) => ({ at, error: describeError(error) })),
      metadataRequestsSinceCreation: this.metadataRequests.filter(({ at }) => at >= since),
      metadataCallsSinceCreation: trace.of(this.clientId, since),
    }
  }
}

/**
 * Records the Metadata calls the members of one case make, with the topics each asked for and the
 * topics, errors and partition counts each answer held, so a case that times out still shows what
 * every member saw. It wraps `Broker#metadata` and changes nothing about a call's result. At most
 * `MAX_TRACED_CALLS` calls are kept.
 */
class MetadataTrace {
  /** Starts with no members and no spy. */
  constructor() {
    /** @type {Set<string>} client ids of the members being traced */
    this.clientIds = new Set()
    /** @type {Array<object>} */
    this.calls = []
    this.spy = null
  }

  /**
   * @param {Member} member a member whose Metadata calls are kept
   * @returns {void}
   */
  watch(member) {
    this.clientIds.add(member.clientId)
  }

  /** @returns {void} begins recording; call `restore` to end it */
  start() {
    const original = Broker.prototype.metadata
    const trace = this
    this.spy = jest.spyOn(Broker.prototype, 'metadata').mockImplementation(async function (
      ...args
    ) {
      const clientId = this.connectionPool?.clientId
      if (!trace.clientIds.has(clientId) || trace.calls.length >= MAX_TRACED_CALLS) {
        return original.apply(this, args)
      }
      const [topics = [], options = {}] = args
      const call = {
        clientId,
        at: performance.now(),
        asked: topics.map((topic) => String(topic.name ?? topic.topicId ?? topic)),
        lenient: options.lenient === true,
        answered: null,
        error: null,
      }
      trace.calls.push(call)
      try {
        const response = await original.apply(this, args)
        call.answered = response.topicMetadata.map(
          ({ topic, topicErrorCode, partitionMetadata }) => ({
            topic,
            errorCode: topicErrorCode,
            partitions: partitionMetadata.map(({ partitionId }) => partitionId),
          })
        )
        return response
      } catch (error) {
        call.error = describeError(error)
        throw error
      }
    })
  }

  /** @returns {void} stops recording */
  restore() {
    this.spy?.mockRestore()
    this.spy = null
  }

  /**
   * @param {string} clientId
   * @param {number} since `performance.now()`
   * @returns {object[]} that member's calls that began at or after `since`, oldest first
   */
  of(clientId, since) {
    return this.calls
      .filter((call) => call.clientId === clientId && call.at >= since)
      .map(({ clientId: _, ...rest }) => rest)
  }
}

/** One Apache Kafka Java group member, run as a child process that is stopped with its scope. */
class JavaSeat {
  /**
   * @param {object} options
   * @param {string} options.name names the member in evidence and failures
   * @param {string[]} options.brokers
   * @param {string} options.groupId
   * @param {string[]} options.topics
   * @param {string} options.assignor the Java assignor class
   */
  constructor({ name, brokers, groupId, topics, assignor }) {
    this.name = name
    this.topics = topics
    this.args = [brokers.join(','), groupId, topics.join(','), assignor, String(CHILD_RUN_MS)]
    /** @type {object[]} the JSON lines the child printed */
    this.events = []
    this.stderr = []
    this.exitCode = null
    this.stopped = false
    this.child = null
    this.exited = null
  }

  /**
   * Spawns the child on the cached jars, offline, and waits for its first assignment. The child is
   * killed with its process group as soon as the scope aborts.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} when the child exits abnormally first, or prints no assignment in time
   */
  async start(scope) {
    scope.throwIfAborted()
    const child = spawn(RUN_SH, this.args, {
      env: {
        ...process.env,
        JAVA_E2E_WORK_DIR: JAVA.workDir,
        JAVA_E2E_OFFLINE: '1',
        KAFKA_CLIENTS_VERSION: process.env.KAFKA_CLIENTS_VERSION || '4.3.1',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: true,
    })
    this.child = child
    this.exited = new Promise((resolve) =>
      child.once('exit', (code, signal) => {
        this.exitCode = code ?? signal
        resolve()
      })
    )
    child.once('error', (error) => this.stderr.push(`spawn failed: ${error.message}\n`))
    let buffered = ''
    child.stdout.setEncoding('utf8')
    child.stdout.on('data', (chunk) => {
      const lines = (buffered + chunk).split('\n')
      buffered = lines.pop()
      for (const line of lines.filter((text) => text.startsWith('{'))) {
        try {
          this.events.push(JSON.parse(line))
        } catch {
          this.stderr.push(`unparsable stdout line: ${line}\n`)
        }
      }
    })
    child.stderr.setEncoding('utf8')
    child.stderr.on('data', (chunk) => this.stderr.push(chunk))
    scope.addEventListener('abort', () => this.kill(), { once: true })
    await pollUntil(
      scope,
      JAVA_START_MS,
      () => {
        if (this.exitCode !== null && this.exitCode !== 0) {
          throw new Error(
            `The Java member exited ${this.exitCode}: ${this.stderr.join('').slice(-1500)}`
          )
        }
        return this.events.some(({ event }) => event === 'assignment')
      },
      { label: 'The Java member assignment' }
    )
  }

  /** @returns {void} kills the child's process group; a child that is already gone is not an error */
  kill() {
    this.stopped = true
    try {
      if (this.child?.pid) process.kill(-this.child.pid, 'SIGKILL')
    } catch {
      // The group has already exited.
    }
  }

  /**
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>} once the child has exited
   */
  async stop(signal) {
    if (!this.child) return
    this.kill()
    await bounded(signal, Infinity, () => this.exited, { label: 'The Java member exit' })
  }

  /** @returns {Record<string, number[]>} the member's full owned set of the last callback */
  assignment() {
    const last = this.events.filter(({ event }) => event === 'assignment').at(-1)
    return last?.partitions ?? {}
  }

  /** @returns {string|undefined} the member id the Java client printed last */
  memberId() {
    return this.events.filter(({ event }) => event === 'assignment').at(-1)?.memberId
  }
}

/**
 * Sends a marked record to each partition of a topic for every record index. The value carries the
 * topic, so a record cannot stand in for the same record of another topic.
 * @param {AbortSignal} scope the case scope
 * @param {object} producer
 * @param {string} topic
 * @param {string} marker names the corpus, so no value repeats across episodes
 * @returns {Promise<Array<{ topic: string, partition: number, value: string }>>} what was sent
 */
const produce = async (scope, producer, topic, marker) => {
  const messages = Array.from({ length: PARTITIONS }, (_, partition) =>
    Array.from({ length: RECORDS_PER_PARTITION }, (_, n) => ({
      partition,
      value: `${marker}-${topic}-${partition}-${n}`,
    }))
  ).flat()
  await step(scope, 'The send', () => producer.send({ topic, acks: -1, messages }))
  return messages.map(({ partition, value }) => ({ topic, partition, value }))
}

/**
 * @param {AbortSignal} scope the case scope
 * @param {object} admin a connected admin client
 * @param {string} groupId
 * @param {string[]} topics
 * @returns {Promise<Record<string, Record<number, string>>>} the committed offset of each
 *   partition by topic, '-1' when none
 */
const committedOffsets = async (scope, admin, groupId, topics) => {
  const answers = await step(scope, 'The offset read', () =>
    admin.fetchOffsets({ groupId, topics })
  )
  return Object.fromEntries(
    answers.map(({ topic, partitions }) => [
      topic,
      Object.fromEntries(partitions.map(({ partition, offset }) => [partition, String(offset)])),
    ])
  )
}

/**
 * Waits until the broker holds a commit past every record of every partition of the topics.
 * @param {AbortSignal} scope the case scope
 * @param {object} admin a connected admin client
 * @param {string} groupId
 * @param {string[]} topics
 * @returns {Promise<Record<string, Record<number, string>>>} the committed offsets by topic
 */
const awaitCommitted = async (scope, admin, groupId, topics) => {
  const all = (committed) =>
    topics.every(
      (topic) =>
        Object.values(committed[topic] ?? {}).length === PARTITIONS &&
        Object.values(committed[topic]).every((offset) => offset === String(RECORDS_PER_PARTITION))
    )
  await pollUntil(
    scope,
    WAIT_MS,
    async () => all(await committedOffsets(scope, admin, groupId, topics)),
    {
      label: 'The broker commits',
    }
  )
  return committedOffsets(scope, admin, groupId, topics)
}

/**
 * Deletes a topic as a cleanup. Only a confirmed missing topic is success, so a topic the case
 * never created is not a failure. The connect and the deletion are separate steps of the cleanup's
 * own scope, so a connect that answers after the allowance ended starts no deletion; the
 * disconnect is bounded by the stop grace.
 * @param {Kafka} kafka
 * @param {string} topic
 * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopic = async (kafka, topic, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, CLEANUP_BUDGET_MS, () => admin.connect(), {
      label: 'The cleanup admin connect',
    })
    await bounded(
      signal,
      CLEANUP_BUDGET_MS,
      () => admin.deleteTopics({ topics: [topic], timeout: 10000 }),
      { label: 'The topic deletion' }
    )
  } catch (error) {
    if (error.type !== MISSING_TOPIC) throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
}

/**
 * Creates a topic through the case's own admin and waits until every partition has a leader. Each
 * request is one step of the case scope, so nothing is created or polled once the scope aborted.
 * @param {AbortSignal} scope the case scope
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @returns {Promise<void>}
 */
const createOwnedTopic = async (scope, admin, topic) => {
  await step(scope, 'The topic creation', () =>
    admin.createTopics({
      waitForLeaders: true,
      timeout: 20000,
      topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 2 }],
    })
  )
  await pollUntil(
    scope,
    WAIT_MS,
    async () => {
      try {
        const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
        const found = topics[0]?.partitions ?? []
        return found.length === PARTITIONS && found.every(({ leader }) => leader >= 0)
      } catch (error) {
        if (NOT_READY.includes(error.type)) return false
        throw error
      }
    },
    { label: 'The topic leaders' }
  )
}

/**
 * Runs one case body with a connected admin and producer, everything it creates registered for
 * cleanup before the operation that can create it partially, and the cleanup run under its own
 * allowance after the body, however the body ends.
 * @param {AbortSignal} scope the case scope
 * @param {(context: object) => Promise<void>} body gets `member`, `java`, `ownTopic`, `reserveTopic`
 *   and `createOwnedTopic`, each bound to the case
 * @returns {Promise<void>}
 */
const withClient = async (scope, body) => {
  const id = secureRandom()
  const brokers = plainTextBrokers()
  const groupId = `e2e-leader-union-group-${id}`
  const kafka = new Kafka({
    clientId: `e2e-leader-union-${id}`,
    brokers,
    logLevel: logLevel.NOTHING,
  })
  const cleanup = new CleanupStack()
  await cleanup.run(
    async () => {
      const admin = kafka.admin()
      cleanup.add('admin', () => admin.disconnect())
      await step(scope, 'The admin connect', () => admin.connect())
      const producer = kafka.producer()
      cleanup.add('producer', () => producer.disconnect())
      await step(scope, 'The producer connect', () => producer.connect())
      const reserveTopic = (name) => {
        const topic = `e2e-leader-union-${name}${id}`
        cleanup.add(`topic ${topic}`, (signal) => deleteTopic(kafka, topic, signal))
        return topic
      }
      const ownTopic = async (name) => {
        const topic = reserveTopic(name)
        await createOwnedTopic(scope, admin, topic)
        return topic
      }
      const member = (options) => {
        const created = new Member({ id, brokers, groupId, ...options })
        cleanup.add(`consumer ${options.name}`, () => created.consumer.disconnect())
        return created
      }
      const java = (options) => {
        const created = new JavaSeat({ brokers, groupId, ...options })
        cleanup.add(`java member ${options.name}`, (signal) => created.stop(signal))
        return created
      }
      await body({
        id,
        groupId,
        brokers,
        admin,
        producer,
        cleanup,
        member,
        java,
        ownTopic,
        reserveTopic,
        createOwnedTopic: (topic) => createOwnedTopic(scope, admin, topic),
      })
    },
    { budgetMs: CLEANUP_BUDGET_MS }
  )
}

/**
 * Fails a Java arm before it joins anything when the Java member could not be built.
 * @returns {void}
 * @throws {Error} naming the missing prerequisite
 */
const requireJava = () => {
  if (!JAVA.ok) throw new Error(`The Java member is a required prerequisite: ${JAVA.reason}`)
}

/**
 * What the first member of the group shows once it has joined alone: it leads, and its public
 * group metadata agrees with the event.
 * @param {Member} leader
 * @returns {{ isLeader: boolean, leaderIsSelf: boolean, metadataAgrees: boolean }}
 */
const leadershipOf = (leader) => {
  const join = leader.lastJoin()
  const { memberId, generationId } = leader.consumer.groupMetadata()
  return {
    isLeader: join.isLeader === true,
    leaderIsSelf: join.leaderId === join.memberId,
    metadataAgrees: memberId === join.memberId && memberId !== '' && generationId >= 0,
  }
}

/**
 * The scenario with only JavaScript members: the leader subscribes to A, the second member to B
 * and the third to both. Every partition of both topics is owned once by a subscriber, no member
 * is told of a topic it did not subscribe to, and a corpus produced once the group is stable is
 * delivered once, committed on the broker, with no rebalance in the window.
 * @param {{ name: string, create: Function }} assignor
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const unequalSubscriptions =
  ({ name, create }) =>
  async (scope) =>
    withClient(scope, async ({ id, groupId, admin, producer, member, ownTopic }) => {
      const a = await ownTopic('a-')
      const b = await ownTopic('b-')
      const leader = member({ name: 'leader', topics: [a], assigner: create })
      await leader.start(scope)
      const leadership = leadershipOf(leader)
      const second = member({ name: 'second', topics: [b], assigner: create })
      await second.start(scope)
      const third = member({ name: 'third', topics: [a, b], assigner: create })
      await third.start(scope)
      const seats = [leader, second, third]

      const ownership = await settleOwnership(scope, seats, [a, b])
      const rebalancesBefore = seats.map(({ rebalances }) => rebalances.length)
      const expected = (
        await Promise.all([a, b].map((topic) => produce(scope, producer, topic, `${name}-${id}`)))
      ).flat()
      await pollUntil(
        scope,
        WAIT_MS,
        () => seats.flatMap((seat) => seat.deliveries).length >= expected.length,
        { label: 'The deliveries' }
      )
      await delay(scope, QUIET_MS)
      const delivered = sortedIdentities(seats.flatMap((seat) => seat.deliveries))
      const committed = await awaitCommitted(scope, admin, groupId, [a, b])

      writeEvidence(EVIDENCE, {
        scenario: 'leader-union',
        case: `javascript-${name}`,
        config: { assignor: name, groupProtocol: 'classic', partitions: PARTITIONS },
        ...wireOf(seats),
        leadership,
        ownership,
        handlers: Object.fromEntries(seats.map((seat) => [seat.name, seat.identities()])),
        committed,
        rebalancesInWindow: seats.map(
          ({ rebalances }, i) => rebalances.length - rebalancesBefore[i]
        ),
      })
      expect(leadership).toEqual({ isLeader: true, leaderIsSelf: true, metadataAgrees: true })
      for (const seat of seats.slice(1)) expect(seat.lastJoin().leaderId).toBe(leader.memberId())
      expect(leader.lastJoin().isLeader).toBe(true)
      expect(isExclusive(ownership, partitionKeys([a, b]))).toBe(true)
      for (const seat of seats) expect(seat.unsubscribed).toEqual([])
      expect(delivered).toEqual(sortedIdentities(expected))
      for (const topic of [a, b]) {
        expect(Object.values(committed[topic])).toEqual(
          Array(PARTITIONS).fill(String(RECORDS_PER_PARTITION))
        )
      }
      expect(seats.map(({ rebalances }, i) => rebalances.length - rebalancesBefore[i])).toEqual([
        0, 0, 0,
      ])
      for (const seat of seats) expect(seat.crashes).toEqual([])
    })

/**
 * The scenario with a Java member in one seat: the leader subscribes to A only and a second member
 * to B only, the third being a JavaScript member of both. The Java member either forms the group
 * and leads, or follows a JavaScript leader. Every partition is owned once by a subscriber.
 * @param {{ name: string, create: Function, wire: string, java: string }} assignor
 * @param {boolean} javaLeads whether the Java member is the leader (subscribed to A) or the
 *   second member (subscribed to B)
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const javaSeat =
  ({ name, create, wire, java: assignor }, javaLeads) =>
  async (scope) => {
    requireJava()
    return withClient(scope, async ({ groupId, member, java, ownTopic }) => {
      const a = await ownTopic('a-')
      const b = await ownTopic('b-')
      const assigner = withWireName(create, wire)
      let javaMember
      let leaderId
      let leadership
      let seats
      if (javaLeads) {
        javaMember = java({ name: 'java-leader', topics: [a], assignor })
        await javaMember.start(scope)
        await pollUntil(
          scope,
          WAIT_MS,
          () => (javaMember.assignment()[a] ?? []).length === PARTITIONS,
          {
            label: 'The Java member forming the group',
          }
        )
        leaderId = javaMember.memberId()
        leadership = {
          first: 'java',
          formedAlone: (javaMember.assignment()[a] ?? []).length === PARTITIONS,
        }
        const second = member({ name: 'second', topics: [b], assigner })
        await second.start(scope)
        const third = member({ name: 'third', topics: [a, b], assigner })
        await third.start(scope)
        seats = [javaMember, second, third]
      } else {
        const leader = member({ name: 'leader', topics: [a], assigner })
        await leader.start(scope)
        leadership = { first: 'javascript', ...leadershipOf(leader) }
        leaderId = leader.memberId()
        javaMember = java({ name: 'java-second', topics: [b], assignor })
        await javaMember.start(scope)
        const third = member({ name: 'third', topics: [a, b], assigner })
        await third.start(scope)
        seats = [leader, javaMember, third]
      }
      const ownership = await settleOwnership(scope, seats, [a, b])
      const stderr = javaMember.stderr.join('')

      writeEvidence(EVIDENCE, {
        scenario: 'leader-union',
        case: `java-${javaLeads ? 'leads' : 'follows'}-${name}`,
        config: { assignor: name, javaAssignor: assignor, groupProtocol: 'classic', javaLeads },
        ...wireOf(seats),
        acceptance: 'assignment-only',
        delivery:
          'not exercised: the Java group member prints its assignments and neither prints nor commits records',
        leaderId,
        leadership,
        ownership,
        java: {
          memberId: javaMember.memberId(),
          events: javaMember.events.length,
          exitCode: javaMember.exitCode,
        },
      })
      expect(leadership).toEqual(
        javaLeads
          ? { first: 'java', formedAlone: true }
          : { first: 'javascript', isLeader: true, leaderIsSelf: true, metadataAgrees: true }
      )
      expect(stderr).not.toMatch(JAVA_FAILURE)
      expect(isExclusive(ownership, partitionKeys([a, b]))).toBe(true)
      expect(javaMember.exitCode).toBeNull()
      expect(leaderId).toBeTruthy()
      for (const seat of seats.filter((candidate) => candidate instanceof Member)) {
        expect(seat.lastJoin().leaderId).toBe(leaderId)
        expect(seat.lastJoin().isLeader).toBe(seat.memberId() === leaderId)
        expect(seat.unsubscribed).toEqual([])
        expect(seat.crashes).toEqual([])
      }
    })
  }

/**
 * Holds the first SyncGroup frame the sockets it is applied to write, until the gate it belongs to
 * is released. Every other frame, and every SyncGroup once the gate is used, goes straight out.
 * A frame released after the scope aborted is not written.
 * @param {Schedule} schedule holds `{ api: 'SyncGroup' }`
 * @param {AbortSignal} scope the case scope
 * @returns {(socket: object) => void} for `factory.arm`
 */
const holdSyncGroup = (schedule, scope) => (socket) => {
  const write = socket.write.bind(socket)
  socket.write = (data, encoding) => {
    const chunk = Buffer.from(data, encoding)
    if (chunk.length < 6 || chunk.readInt16BE(4) !== apiKeys.SyncGroup) return write(chunk)
    schedule
      .run({ api: 'SyncGroup' }, () => PASS)
      .then(() => {
        if (!scope.aborted) write(chunk)
      })
    return true
  }
}

/**
 * The scenario with a topic created while a round is in flight: the follower subscribes to C
 * before it exists and the leader's first SyncGroup after that is held. Once it arrives, the
 * leader's union snapshot is complete and lacks C; C is created, the round completes without it,
 * and the leader's union watch finds C and starts a later round, which assigns every partition of C
 * to its subscriber. No manual rejoin or stop is involved.
 *
 * The follower's metadata age exceeds the observation window, so its own discovery cannot start
 * the later round before the leader watch reads C. A range follower can change its subscription
 * protocol bytes on rejoin even with unchanged topics, because its generation has changed.
 * The follower's own rebalances are recorded as observations only. The evidence row is written in
 * a `finally`, so a case that fails or times out still records the stage it reached, each member's
 * Metadata requests and answers since the creation, and the join, rebalance and crash events with
 * their times.
 * @param {{ name: string, create: Function }} assignor
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const topicCreatedInFirstRound =
  ({ name, create }) =>
  async (scope) =>
    withClient(
      scope,
      async ({ admin, member, ownTopic, reserveTopic, cleanup, createOwnedTopic: createNow }) => {
        const a = await ownTopic('a-')
        const c = reserveTopic('c-')
        const schedule = new Schedule()
        const chaos = createChaosSocketFactory(defaultSocketFactory())
        const trace = new MetadataTrace()
        trace.start()
        cleanup.add('metadata trace', () => trace.restore())
        const seats = []
        const timeline = {}
        const state = { stage: 'starting', error: null, leadership: null, firstAssignment: null }
        let ownership = null
        let followerRebalances = []
        try {
          const leader = member({
            name: 'leader',
            topics: [a],
            assigner: create,
            consumerOptions: { allowAutoTopicCreation: false, metadataMaxAge: METADATA_MAX_AGE_MS },
            kafkaOptions: { socketFactory: chaos },
          })
          seats.push(leader)
          trace.watch(leader)
          await leader.start(scope)
          state.leadership = leadershipOf(leader)
          chaos.arm({}, holdSyncGroup(schedule, scope))
          const held = schedule.hold({ api: 'SyncGroup' })
          const follower = member({
            name: 'follower',
            topics: [c],
            assigner: create,
            consumerOptions: {
              allowAutoTopicCreation: false,
              metadataMaxAge: DEFAULT_METADATA_MAX_AGE_MS,
            },
          })
          seats.push(follower)
          trace.watch(follower)
          const joinsBefore = leader.joins.length
          const starting = follower.begin(scope, { missing: [c] })
          starting.catch(() => {})
          state.stage = 'waiting for the held SyncGroup'
          await bounded(scope, WAIT_MS, () => held.arrived(), { label: 'The held SyncGroup' })
          timeline.snapshotAt = performance.now()

          state.stage = 'creating the topic'
          timeline.creationStartedAt = performance.now()
          await createNow(c)
          timeline.createdAt = performance.now()
          held.release(PASS)
          timeline.releasedAt = performance.now()
          state.stage = 'waiting for the first round'
          await starting
          await pollUntil(scope, WAIT_MS, () => leader.joins.length > joinsBefore, {
            label: 'The first round completion',
          })
          const firstRound = leader.joins[joinsBefore]
          timeline.firstRoundAt = firstRound.at
          await follower.whenJoined(scope)
          state.firstAssignment = { ...follower.joins[0].payload.memberAssignment }

          state.stage = 'waiting for the later assignment'
          await pollUntil(
            scope,
            WAIT_MS,
            () => (follower.assignment()[c] ?? []).length === PARTITIONS,
            { label: 'The later assignment of the new topic' }
          )
          const detection = leader.rebalances.find(
            ({ at, reason }) => at > firstRound.at && reason === 'stale-assignment'
          )
          const later = follower.joins.find(
            ({ payload }) => (payload.memberAssignment[c] ?? []).length > 0
          )
          timeline.detectedAt = detection?.at
          timeline.detectedReason = detection?.reason
          timeline.laterAt = later?.at
          followerRebalances = follower.rebalances.map(({ at, reason }) => ({ at, reason }))
          state.stage = 'settling the ownership'
          ownership = await settleOwnership(scope, [leader, follower], [a, c])

          state.stage = 'asserting'
          expect(state.leadership).toEqual({
            isLeader: true,
            leaderIsSelf: true,
            metadataAgrees: true,
          })
          expect(state.firstAssignment[c] ?? []).toEqual([])
          expect(firstRound.payload.memberAssignment[c] ?? []).toEqual([])
          expect(Object.values(timeline).every(Boolean)).toBe(true)
          expect(timeline.snapshotAt).toBeLessThan(timeline.createdAt)
          expect(timeline.createdAt).toBeLessThan(timeline.releasedAt)
          expect(timeline.releasedAt).toBeLessThan(firstRound.at)
          expect(firstRound.at).toBeLessThan(detection.at)
          expect(detection.reason).toBe('stale-assignment')
          expect(detection.at).toBeLessThan(later.at)
          expect(later.payload.memberAssignment[c]).toEqual([0, 1, 2, 3])
          expect(isExclusive(ownership, partitionKeys([a, c]))).toBe(true)
          for (const seat of seats) {
            expect(seat.unsubscribed).toEqual([])
            expect(seat.crashes).toEqual([])
          }
          state.stage = 'passed'
        } catch (error) {
          state.error = describeError(error)
          throw error
        } finally {
          writeEvidence(EVIDENCE, {
            scenario: 'leader-union',
            case: `late-topic-${name}`,
            config: {
              assignor: name,
              groupProtocol: 'classic',
              allowAutoTopicCreation: false,
              leaderMetadataMaxAgeMs: METADATA_MAX_AGE_MS,
              followerMetadataMaxAgeMs: DEFAULT_METADATA_MAX_AGE_MS,
            },
            ...wireOf(seats),
            stage: state.stage,
            error: state.error,
            leadership: state.leadership,
            topicCreation: { topic: c, partitions: PARTITIONS },
            timeline,
            followerRebalances,
            firstAssignment: state.firstAssignment,
            ownership,
            members: seats.map((seat) => seat.report(trace, timeline.createdAt ?? 0)),
          })
        }
      }
    )

/**
 * The scenario with partitions added to a topic only the follower subscribes to. The leader
 * subscribes to A and has the short metadata age; the follower subscribes to C, which exists, and
 * has an age far beyond the wait, so that only the leader's union watch can notice the growth. Once
 * the assignment is stable, C grows; the leader must start a round for `stale-assignment`, and the
 * later assignment gives every new partition of C to its subscriber alone. The evidence row is
 * written in a `finally`, as in the late-topic scenario.
 * @param {{ name: string, create: Function }} assignor
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const partitionsAddedToFollowerTopic =
  ({ name, create }) =>
  async (scope) =>
    withClient(scope, async ({ admin, member, ownTopic, cleanup }) => {
      const a = await ownTopic('a-')
      const c = await ownTopic('c-')
      const keys = [
        ...partitionKeys([a]),
        ...Array.from({ length: GROWN_PARTITIONS }, (_, partition) => `${c}:${partition}`),
      ]
      const trace = new MetadataTrace()
      trace.start()
      cleanup.add('metadata trace', () => trace.restore())
      const seats = []
      const timeline = {}
      const state = { stage: 'starting', error: null, leadership: null, firstAssignment: null }
      let ownership = null
      try {
        const options = { allowAutoTopicCreation: false }
        const leader = member({
          name: 'leader',
          topics: [a],
          assigner: create,
          consumerOptions: { ...options, metadataMaxAge: METADATA_MAX_AGE_MS },
        })
        seats.push(leader)
        trace.watch(leader)
        state.stage = 'starting the leader'
        await leader.start(scope)
        state.leadership = leadershipOf(leader)
        const follower = member({
          name: 'follower',
          topics: [c],
          assigner: create,
          consumerOptions: { ...options, metadataMaxAge: DEFAULT_METADATA_MAX_AGE_MS },
        })
        seats.push(follower)
        trace.watch(follower)
        state.stage = 'starting the follower'
        await follower.start(scope)
        state.stage = 'settling the first assignment'
        const first = await settleOwnership(scope, seats, [a, c])
        state.firstAssignment = { ...follower.assignment() }
        const joinsBefore = leader.joins.length

        state.stage = 'adding partitions'
        timeline.growthStartedAt = performance.now()
        await step(scope, 'The partition creation', () =>
          admin.createPartitions({ topicPartitions: [{ topic: c, count: GROWN_PARTITIONS }] })
        )
        timeline.grownAt = performance.now()
        state.stage = 'waiting for the leader to start a round'
        await pollUntil(
          scope,
          WAIT_MS,
          () => leader.rebalances.some(({ at }) => at > timeline.grownAt),
          { label: 'The leader round for the grown topic' }
        )
        const detection = leader.rebalances.find(({ at }) => at > timeline.grownAt)
        timeline.detectedAt = detection.at
        timeline.detectedReason = detection.reason

        state.stage = 'waiting for the later assignment'
        await pollUntil(
          scope,
          WAIT_MS,
          () => (follower.assignment()[c] ?? []).length === GROWN_PARTITIONS,
          { label: 'The later assignment of the grown topic' }
        )
        const later = follower.joins.find(
          ({ payload }) => (payload.memberAssignment[c] ?? []).length === GROWN_PARTITIONS
        )
        timeline.laterAt = later.at
        state.stage = 'settling the ownership'
        ownership = await settleOwnership(scope, seats, [a, c], keys)

        state.stage = 'asserting'
        expect(state.leadership).toEqual({
          isLeader: true,
          leaderIsSelf: true,
          metadataAgrees: true,
        })
        expect(isExclusive(first, partitionKeys([a, c]))).toBe(true)
        expect(state.firstAssignment[c]).toHaveLength(PARTITIONS)
        expect(leader.joins.length).toBeGreaterThan(joinsBefore)
        expect(timeline.grownAt).toBeLessThan(timeline.detectedAt)
        expect(detection.reason).toBe('stale-assignment')
        expect(detection.at).toBeLessThan(later.at)
        expect([...later.payload.memberAssignment[c]].sort((x, y) => x - y)).toEqual(
          Array.from({ length: GROWN_PARTITIONS }, (_, partition) => partition)
        )
        expect(isExclusive(ownership, keys)).toBe(true)
        expect(leader.assignment()[c] ?? []).toEqual([])
        for (let partition = PARTITIONS; partition < GROWN_PARTITIONS; partition += 1) {
          expect(ownership.owners[`${c}:${partition}`]).toEqual(['follower'])
        }
        expect(follower.rebalances.map(({ reason }) => reason)).not.toContain('stale-assignment')
        for (const seat of seats) {
          expect(seat.unsubscribed).toEqual([])
          expect(seat.crashes).toEqual([])
        }
        state.stage = 'passed'
      } catch (error) {
        state.error = describeError(error)
        throw error
      } finally {
        writeEvidence(EVIDENCE, {
          scenario: 'leader-union',
          case: `follower-topic-growth-${name}`,
          config: {
            assignor: name,
            groupProtocol: 'classic',
            allowAutoTopicCreation: false,
            leaderMetadataMaxAgeMs: METADATA_MAX_AGE_MS,
            followerMetadataMaxAgeMs: DEFAULT_METADATA_MAX_AGE_MS,
          },
          ...wireOf(seats),
          stage: state.stage,
          error: state.error,
          leadership: state.leadership,
          growth: { topic: c, from: PARTITIONS, to: GROWN_PARTITIONS },
          timeline,
          firstAssignment: state.firstAssignment,
          ownership,
          members: seats.map((seat) => seat.report(trace, timeline.growthStartedAt ?? 0)),
        })
      }
    })

/**
 * Drops the next JoinGroup responses of the sockets it is applied to, a finite number of times, and
 * counts what it dropped. A dropped response is a complete frame the broker sent, so the broker has
 * accepted the join whose answer the client never sees. Each socket ends as a peer would right
 * after its dropped frame: a client whose JoinGroup is parked waits out the whole rebalance timeout
 * for an answer that never comes, and a join that expires that way is a failed round, not a retry,
 * so the loss it can retry is a lost connection.
 */
class JoinResponseLoss {
  /** @param {ReturnType<typeof createChaosSocketFactory>} chaos the joiner's socket factory */
  constructor(chaos) {
    this.chaos = chaos
    this.armed = false
    this.disarm = null
    this.tally = null
  }

  /** @returns {void} drops the next `ARMED_LIMIT` JoinGroup responses of every socket, once */
  arm() {
    if (this.armed) return
    this.armed = true
    this.disarm = this.chaos.arm({}, (socket) =>
      socket.dropResponses(ARMED_LIMIT, { apiKey: apiKeys.JoinGroup, thenClose: true })
    )
  }

  /** @returns {number} the responses dropped so far */
  dropped() {
    if (this.tally) return this.tally.dropped
    return this.chaos.sockets.reduce(
      (total, socket) => total + ARMED_LIMIT - (socket.responseRule?.count ?? ARMED_LIMIT),
      0
    )
  }

  /**
   * Ends the injection: nothing is dropped from now on, on open sockets or later ones.
   * @returns {{ dropped: number, endpoints: string[] }} the total, and the brokers that dropped
   */
  end() {
    const endpoints = this.chaos.sockets
      .filter((socket) => (socket.responseRule?.count ?? ARMED_LIMIT) < ARMED_LIMIT)
      .map(({ endpoint }) => `${endpoint.host}:${endpoint.port}`)
    this.tally = { dropped: this.dropped(), endpoints: [...new Set(endpoints)] }
    this.disarm?.()
    for (const socket of this.chaos.sockets) {
      socket.dropResponses(0, { apiKey: apiKeys.JoinGroup })
    }
    return this.tally
  }
}

/**
 * The scenario of a joiner that loses the responses to its follow-up JoinGroup requests. The
 * joiner receives its member id, then loses at least three complete JoinGroup responses the broker
 * sent, each with its connection; once the injection ends the same consumer finishes joining. It
 * must have asked for an id once and presented that id on every later attempt, so the group holds
 * exactly the expected members, as the broker reports them, long before an abandoned member could
 * age out.
 *
 * The connection ends with each dropped response because the JoinGroup request may block for the
 * rebalance timeout plus 5 s, whatever `requestTimeout` says, and the round is over at the
 * rebalance timeout itself: a response lost on a connection that stays open ends the round, not
 * the wait.
 *
 * The broker's own log is not read here: a controller that needs it takes the coordinator's log
 * between the `from` and `to` of the evidence line, for the group id and member ids it records.
 * The evidence row is written in a `finally`, so a case that fails or times out still records the
 * stage it reached, the member ids seen, each JoinGroup attempt with its time and the number of
 * responses dropped.
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const lostJoinResponses = () => async (scope) =>
  withClient(scope, async ({ groupId, admin, producer, member, ownTopic, cleanup, id }) => {
    const state = { stage: 'starting', error: null }
    const seats = []
    const wall = (at) => new Date(performance.timeOrigin + at).toISOString()
    let topic = null
    let stableIds = []
    let trace = null
    let loss = null
    let joiner = null
    let lost = null
    let described = null
    let ownership = null
    let committed = null
    let joinStartedAt = null
    let stableAt = null
    const joinCalls = () =>
      (trace?.entries ?? []).filter(({ memberId }) => !stableIds.includes(memberId))

    try {
      topic = await ownTopic('lost-')
      const options = {
        topics: [topic],
        assigner: roundRobin,
        consumerOptions: { sessionTimeout: SESSION_TIMEOUT_MS },
      }
      const stable = [
        member({ name: 'stable-1', ...options }),
        member({ name: 'stable-2', ...options }),
      ]
      seats.push(...stable)
      state.stage = 'starting the stable members'
      for (const seat of stable) await seat.start(scope)
      await settleOwnership(scope, stable, [topic])
      stableIds = stable.map((seat) => seat.consumer.groupMetadata().memberId)

      trace = traceCalls(Broker.prototype, 'joinGroup', ({ groupId: group, memberId }) =>
        group === groupId ? { memberId } : null
      )
      cleanup.add('join trace', () => trace.restore())
      const chaos = createChaosSocketFactory(defaultSocketFactory())
      loss = new JoinResponseLoss(chaos)
      joiner = member({
        name: 'joiner',
        ...options,
        kafkaOptions: { socketFactory: chaos },
      })
      seats.push(joiner)
      const { consumer } = joiner
      consumer.on(consumer.events.REQUEST, ({ payload }) => {
        if (payload.apiName === 'JoinGroup') loss.arm()
      })
      state.stage = 'losing the JoinGroup responses'
      const starting = joiner.begin(scope, { startMs: JOINER_START_MS })
      starting.catch(() => {})
      await Promise.race([
        pollUntil(scope, WAIT_MS, () => loss.dropped() >= LOSSES, {
          label: 'The lost JoinGroup responses',
        }),
        starting,
      ])
      lost = loss.end()
      state.stage = 'finishing the join'
      await starting
      await joiner.whenJoined(scope)

      state.stage = 'waiting for the stable group'
      const expectedIds = [...stableIds, joiner.memberId()].sort()
      await pollUntil(
        scope,
        WAIT_MS,
        async () => {
          const { groups } = await admin.describeGroups([groupId])
          described = groups[0]
          return (
            described.state === 'Stable' &&
            described.members
              .map(({ memberId }) => memberId)
              .sort()
              .join() === expectedIds.join()
          )
        },
        { label: 'The stable group of every expected member' }
      )
      stableAt = performance.now()
      const calls = joinCalls()
      joinStartedAt = calls[0]?.at ?? stableAt
      const recoveryMs = stableAt - joinStartedAt

      state.stage = 'settling the ownership'
      await pollUntil(
        scope,
        WAIT_MS,
        () => isExclusive(ownershipOf(seats), partitionKeys([topic])),
        { label: 'The exclusive ownership after recovery' }
      )
      state.stage = 'delivering'
      const expected = await produce(scope, producer, topic, `lost-${id}`)
      await pollUntil(
        scope,
        WAIT_MS,
        () => seats.flatMap((seat) => seat.deliveries).length >= expected.length,
        { label: 'The deliveries' }
      )
      await delay(scope, QUIET_MS)
      const delivered = sortedIdentities(seats.flatMap((seat) => seat.deliveries))
      committed = await awaitCommitted(scope, admin, groupId, [topic])
      ownership = ownershipOf(seats)

      state.stage = 'asserting'
      const allocations = calls.filter(({ memberId }) => memberId === '')
      expect(lost.dropped).toBeGreaterThanOrEqual(LOSSES)
      expect(allocations).toHaveLength(1)
      expect(calls[0]?.memberId).toBe('')
      expect(calls.length).toBeGreaterThanOrEqual(LOSSES + 1)
      expect(new Set(calls.slice(1).map(({ memberId }) => memberId))).toEqual(
        new Set([joiner.memberId()])
      )
      expect(joiner.memberId()).toBeTruthy()
      expect(stableIds).not.toContain(joiner.memberId())
      expect(calls.map(({ error }) => error?.type).filter(Boolean)).not.toContain(
        'UNKNOWN_MEMBER_ID'
      )
      expect(described.members.map(({ memberId }) => memberId).sort()).toEqual(expectedIds)
      expect(recoveryMs).toBeLessThan(SESSION_TIMEOUT_MS)
      expect(isExclusive(ownership, partitionKeys([topic]))).toBe(true)
      expect(joiner.deliveries.length).toBeGreaterThan(0)
      expect(delivered).toEqual(sortedIdentities(expected))
      expect(Object.values(committed[topic])).toEqual(
        Array(PARTITIONS).fill(String(RECORDS_PER_PARTITION))
      )
      for (const seat of seats) {
        expect(seat.unsubscribed).toEqual([])
        expect(seat.crashes).toEqual([])
      }
      state.stage = 'passed'
    } catch (error) {
      state.error = describeError(error)
      throw error
    } finally {
      lost ??= loss?.end() ?? null
      const calls = joinCalls()
      writeEvidence(EVIDENCE, {
        scenario: 'leader-union',
        case: 'lost-initial-join-group',
        config: {
          assignor: 'roundRobin',
          groupProtocol: 'classic',
          sessionTimeout: SESSION_TIMEOUT_MS,
        },
        ...wireOf(seats),
        stage: state.stage,
        error: state.error,
        groupId,
        runId: id,
        topic,
        joinerMemberId: joiner?.memberId() ?? null,
        stableIds,
        memberIds: [
          ...new Set([
            ...stableIds,
            ...calls.map(({ memberId }) => memberId).filter(Boolean),
            ...(joiner?.joins ?? []).map(({ payload }) => payload.memberId),
          ]),
        ],
        coordinators: lost?.endpoints ?? [],
        droppedResponses: lost?.dropped ?? 0,
        calls: calls.map(({ at, memberId, error }) => ({
          at: wall(at),
          memberId,
          error: describeError(error),
        })),
        from: joinStartedAt === null ? null : wall(joinStartedAt),
        to: stableAt === null ? null : wall(stableAt),
        recoveryMs: stableAt === null ? null : stableAt - joinStartedAt,
        group: described && {
          state: described.state,
          members: described.members.map(({ memberId }) => memberId),
        },
        ownership,
        committed,
        crashes: seats.flatMap(({ name, crashes }) =>
          crashes.map(({ at, error }) => ({ name, at: wall(at), error: describeError(error) }))
        ),
      })
    }
  })

describeLeaderUnion('Consumer > a leader with unequal subscriptions (live)', () => {
  for (const assignor of ASSIGNORS) {
    test(
      `assigns every partition of unequal JavaScript subscriptions with the ${assignor.name} assignor`,
      liveCase(CASE_BUDGET_MS, unequalSubscriptions(assignor)),
      caseTimeout(CASE_BUDGET_MS)
    )
  }

  for (const assignor of ASSIGNORS) {
    test(
      `assigns unequal subscriptions when a Java member leads and a JavaScript member follows with the ${assignor.name} assignor`,
      liveCase(CASE_BUDGET_MS, javaSeat(assignor, true)),
      caseTimeout(CASE_BUDGET_MS)
    )
  }

  for (const assignor of ASSIGNORS) {
    test(
      `assigns unequal subscriptions when a JavaScript member leads and a Java member follows with the ${assignor.name} assignor`,
      liveCase(CASE_BUDGET_MS, javaSeat(assignor, false)),
      caseTimeout(CASE_BUDGET_MS)
    )
  }

  for (const assignor of ASSIGNORS) {
    test(
      `assigns a topic created during the first round in a later round with the ${assignor.name} assignor`,
      liveCase(CASE_BUDGET_MS, topicCreatedInFirstRound(assignor)),
      caseTimeout(CASE_BUDGET_MS)
    )
  }

  test(
    'assigns partitions added to a follower-only topic in a later round with the range assignor',
    liveCase(
      JOIN_CASE_BUDGET_MS,
      partitionsAddedToFollowerTopic(ASSIGNORS.find(({ name }) => name === 'range'))
    ),
    caseTimeout(JOIN_CASE_BUDGET_MS)
  )

  test(
    'keeps one member id through lost initial JoinGroup responses',
    liveCase(JOIN_CASE_BUDGET_MS, lostJoinResponses()),
    caseTimeout(JOIN_CASE_BUDGET_MS)
  )
})
