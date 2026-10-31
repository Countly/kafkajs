const { Kafka, logLevel, PartitionAssigners } = require('../../../../index')
const { createCluster, plainTextBrokers } = require('testHelpers')
const { waitForFullIsr } = require('../../../../testHelpers/leaderElection')
const apiKeys = require('../../../protocol/requests/apiKeys')
const Decoder = require('../../../protocol/decoder')
const { requests } = require('../../../protocol/requests')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../../protocol/headers')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  delay,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { WireTranscript } = require('./rackWire')

const FETCH = apiKeys.Fetch
const STEP_MS = 30000
const POLL_MS = 100
const QUIET_MS = 1000
const DELIVERY_MS = 30000
const WAIT_MS = 500
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'

/** The assignment modes a Fetch session must serve, each with the consumer options it needs. */
const MODES = {
  'classic eager dynamic': {
    group: true,
    options: { groupProtocol: 'classic', partitionAssigners: [PartitionAssigners.roundRobin] },
  },
  'classic cooperative static': {
    group: true,
    static: true,
    options: {
      groupProtocol: 'classic',
      partitionAssigners: [PartitionAssigners.cooperativeSticky],
    },
  },
  'consumer protocol dynamic': { group: true, options: { groupProtocol: 'consumer' } },
  'consumer protocol static': { group: true, static: true, options: { groupProtocol: 'consumer' } },
  'manual assignment': { group: false, options: {} },
}

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the identity of a record: where it is and what it says
 */
const identity = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * One raw client or admin operation, bounded by the case scope.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the operation in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, STEP_MS, op, { label })

/**
 * Deletes topics as a cleanup. Only a confirmed missing topic is success.
 * @param {Kafka} kafka
 * @param {string[]} topics
 * @param {AbortSignal} signal the cleanup's own
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopics = async (kafka, topics, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, STEP_MS, () => admin.connect(), { label: 'The cleanup admin connect' })
    await bounded(signal, STEP_MS, () => admin.deleteTopics({ topics, timeout: 10000 }), {
      label: 'The topic deletion',
    })
  } catch (error) {
    if (error.type !== MISSING_TOPIC) throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
}

/**
 * Records the Fetch frames a client writes and the answers it reads, and reduces them to what a
 * Fetch session did on each node. It is a wire observer: the node of a frame is its socket's
 * endpoint and everything it reports is read from the frame's own bytes, never from the client's
 * state.
 */
class SessionWire extends WireTranscript {
  /**
   * @param {object} exchange a Fetch exchange whose answer was kept
   * @returns {Promise<object>} the answer's decoded body
   */
  async decode(exchange) {
    const { response } = requests.Fetch.protocol({ version: exchange.apiVersion })({
      maxWaitTime: 0,
      minBytes: 0,
      maxBytes: 0,
      topics: [],
      topicIds: new Map(),
      rackId: '',
    })
    const { frame } = exchange.answer
    const header = new Decoder(frame)
    header.readInt32()
    header.readInt32()
    if (responseHeaderVersion(FETCH, exchange.apiVersion) === RESPONSE_HEADER_V1) {
      header.readTaggedFields()
    }
    return response.decode(frame.subarray(header.offset))
  }

  /**
   * @returns {Promise<Array<{exchange: object, decoded: object}>>} every answered Fetch, oldest
   *   first, each decoded once
   */
  async answers() {
    const kept = this.exchanges.filter((x) => x.apiKey === FETCH && x.answer)
    return Promise.all(
      kept.map(async (exchange) => {
        exchange.decoded ??= this.decode(exchange)
        return { exchange, decoded: await exchange.decoded }
      })
    )
  }

  /**
   * Replays each node's session from the requests it was sent, so that every observation knows how
   * many partitions the session held when the broker answered.
   * @returns {Promise<Array<object>>} one record per Fetch frame, oldest first:
   *   `node`, `at`, `answeredAt`, `sessionId`, `sessionEpoch`, `cells` and `forgotten` as
   *   `topicId|partition` keys, `held` (how many partitions the session holds after the request),
   *   `heldKeys` (which, for a session of at most 256 partitions), `rackId`,
   *   `version`, `connection`, `correlationId` and `answer` (`errorCode`, `sessionId`, `rows` and
   *   `keys`, or null while unanswered)
   * @throws {Error} the decode failure of a request frame
   */
  async observations() {
    const answered = new Map((await this.answers()).map((a) => [a.exchange, a.decoded]))
    const held = new Map()
    const keyOf = (id, partition) => `${id}|${partition}`
    return this.exchanges
      .filter((x) => x.apiKey === FETCH)
      .map((exchange) => {
        if (exchange.fetchError) throw exchange.fetchError
        const request = exchange.fetch
        const node = this.nodeOf(exchange)
        const cells = request.topics.flatMap((t) =>
          t.partitions.map(({ partition }) => keyOf(t.topicId ?? t.topic, partition))
        )
        const forgotten = request.forgottenTopics.flatMap((t) =>
          t.partitions.map((partition) => keyOf(t.topicId ?? t.topic, partition))
        )
        let session = held.get(node) ?? new Set()
        if (request.sessionEpoch <= 0) session = new Set()
        if (request.sessionEpoch >= 0) {
          for (const key of forgotten) session.delete(key)
          for (const key of cells) session.add(key)
        }
        held.set(node, session)
        const decoded = answered.get(exchange)
        const keys = decoded
          ? decoded.responses.flatMap((t) =>
              t.partitions.map(({ partition }) => keyOf(t.topicId ?? t.topicName, partition))
            )
          : []
        return {
          node,
          endpoint: exchange.endpoint,
          connection: exchange.connection,
          correlationId: exchange.correlationId,
          version: exchange.apiVersion,
          at: exchange.at,
          answeredAt: exchange.answer?.at ?? null,
          sessionId: request.sessionId,
          sessionEpoch: request.sessionEpoch,
          rackId: request.rackId,
          maxWaitMs: request.maxWaitMs,
          cells,
          forgotten,
          held: session.size,
          heldKeys: session.size <= 256 ? [...session] : null,
          answer: decoded
            ? {
                errorCode: decoded.errorCode,
                sessionId: decoded.sessionId,
                rows: keys.length,
                keys,
              }
            : null,
        }
      })
  }
}

/**
 * @param {object} observation from `SessionWire#observations`
 * @param {string} key a `topicId|partition` key
 * @returns {boolean|undefined} whether the session held the partition after the request; unknown
 *   (undefined) for a session too large to list
 */
const holds = ({ heldKeys }, key) => heldKeys?.includes(key)

/**
 * Reduces observations to the session facts the oracles read.
 * @param {Array<object>} observations from `SessionWire#observations`
 * @returns {{ full: object[], created: object[], incremental: object[], sparse: object[],
 *   fallback: object[], refused: object[], closes: object[], nodes: number[] }} the frames of each
 *   kind: full requests at epoch 0; full requests the broker answered with a positive session ID;
 *   incremental requests it accepted; accepted ones answering fewer partitions than the session
 *   held; full requests answered with ID 0; answers carrying a session error; close requests; and
 *   the nodes seen
 */
const sessionFacts = (observations) => {
  const accepted = (o) => o.answer && o.answer.errorCode === 0
  return {
    full: observations.filter((o) => o.sessionEpoch === 0),
    created: observations.filter(
      (o) => o.sessionEpoch === 0 && accepted(o) && o.answer.sessionId > 0
    ),
    incremental: observations.filter((o) => o.sessionEpoch > 0 && accepted(o)),
    sparse: observations.filter((o) => o.sessionEpoch > 0 && accepted(o) && o.answer.rows < o.held),
    fallback: observations.filter(
      (o) => o.sessionEpoch === 0 && accepted(o) && o.answer.sessionId === 0
    ),
    refused: observations.filter((o) => o.answer && [70, 71, 106].includes(o.answer.errorCode)),
    closes: observations.filter((o) => o.sessionEpoch === -1 && o.sessionId > 0),
    nodes: [...new Set(observations.map((o) => o.node))].sort((a, b) => a - b),
  }
}

/**
 * @param {object[]} expected the records the sends acknowledged
 * @param {Array<{ topic: string, partition: number, offset: string, value: string }>} handled what
 *   the handlers were given, oldest first
 * @param {(key: string) => boolean} [replays] whether a record may be handled again: a declared
 *   seek interval, or a crash and rejoin after the last confirmed commit
 * @param {(record: object) => number} [incarnationOf] which incarnation of its topic a record
 *   belongs to; offsets restart in each one, and an incarnation above zero prefixes its keys
 * @returns {string[]} each record never handled, each handled record the sends never acknowledged,
 *   each record handled again where it may not be, and each partition whose offsets went backwards
 *   within one incarnation without a declared replay
 */
const corpusProblems = (expected, handled, replays = () => false, incarnationOf = () => 0) => {
  const tag = (record, text) => {
    const incarnation = incarnationOf(record)
    return incarnation ? `${incarnation}|${text}` : text
  }
  const keyOf = (record) => tag(record, identity(record))
  const acknowledged = new Set(expected.map(keyOf))
  const problems = []
  const seen = new Set()
  const last = new Map()
  for (const record of handled) {
    const key = keyOf(record)
    const place = tag(record, `${record.topic}|${record.partition}`)
    if (!acknowledged.has(key)) problems.push(`${key} was never acknowledged`)
    if (seen.has(key) && !replays(key)) problems.push(`${key} was handled again`)
    if (Number(record.offset) < (last.get(place) ?? -1) && !replays(key)) {
      problems.push(`${place} went back to offset ${record.offset}`)
    }
    last.set(place, Math.max(last.get(place) ?? -1, Number(record.offset)))
    seen.add(key)
  }
  for (const record of expected) {
    if (!seen.has(keyOf(record))) problems.push(`${keyOf(record)} was never handled`)
  }
  return problems
}

/**
 * @param {Array<{ topic: string, partition: number, offset: string }>} handled
 * @param {Map<string, number>} committed the next offset the broker holds for `topic|partition`
 * @returns {string[]} each partition committed past what its handlers processed
 */
const commitProblems = (handled, committed) =>
  [...committed].flatMap(([place, offset]) => {
    const processed = Math.max(
      -1,
      ...handled.filter((r) => `${r.topic}|${r.partition}` === place).map((r) => Number(r.offset))
    )
    return offset > processed + 1 ? [`${place} committed ${offset}, past ${processed + 1}`] : []
  })

/**
 * @param {ReturnType<typeof sessionFacts>} facts
 * @returns {string[]} how the facts break the incremental-path oracle: every node that carried
 *   partitions must show an initial full request, a created session and an accepted incremental
 *   request, and at least one accepted reply must have been sparse
 */
const sessionProblems = (facts) => {
  const problems = []
  const nodes = [...new Set(facts.full.filter((o) => o.cells.length).map((o) => o.node))]
  if (nodes.length === 0) problems.push('no full request carried a partition')
  for (const node of nodes) {
    if (!facts.full.some((o) => o.node === node && o.sessionId === 0)) {
      problems.push(`node ${node} had no initial full request`)
    }
    if (!facts.created.some((o) => o.node === node))
      problems.push(`node ${node} created no session`)
    if (!facts.incremental.some((o) => o.node === node)) {
      problems.push(`node ${node} had no accepted incremental request`)
    }
  }
  if (facts.sparse.length === 0) problems.push('no accepted reply was sparse')
  return problems
}

/**
 * @param {ReturnType<typeof sessionFacts>} facts
 * @returns {Record<string, number>} the counts of each kind of frame, for the evidence row
 */
const summarize = (facts) => ({
  full: facts.full.length,
  created: facts.created.length,
  incremental: facts.incremental.length,
  sparse: facts.sparse.length,
  fallback: facts.fallback.length,
  refused: facts.refused.length,
  closes: facts.closes.length,
})

/**
 * @param {object[]} observations
 * @param {string} key a `topicId|partition` key
 * @param {number} after `performance.now()` the moment is later than
 * @returns {object|undefined} the first request after `after` that leaves the session holding the
 *   partition
 */
const firstHolding = (observations, key, after) =>
  observations.find((o) => o.at > after && holds(o, key) === true)

/**
 * @param {number} wallMs a `Date.now()` moment
 * @param {number} [wallNow] `Date.now()` now
 * @param {number} [monotonicNow] `performance.now()` now
 * @returns {number} the same moment on the `performance.now()` clock the wire's frames carry
 */
const monotonicOf = (wallMs, wallNow = Date.now(), monotonicNow = performance.now()) =>
  monotonicNow - (wallNow - wallMs)

/**
 * @param {object[]} observations every Fetch frame of the member
 * @param {number} node
 * @returns {number|undefined} the connection of the node's newest frame, which a fault armed now
 *   reaches
 */
const connectionOf = (observations, node) =>
  observations.findLast((o) => o.node === node)?.connection

/**
 * Matches the Fetch a fault took. The request in flight when the fault is armed takes it, so the
 * frame may be older than the arming: it is told by its connection, not by its time.
 * @param {number} node
 * @param {number} connection the connection the fault was armed on
 * @returns {(o: object, index: number, all: object[]) => boolean} true for a frame sent on
 *   `connection` that was never answered, once the node shows a frame on another connection
 */
const lostOnConnection = (node, connection) => (o, index, all) =>
  o.node === node &&
  o.connection === connection &&
  o.answer === null &&
  all.some((later) => later.node === node && later.at > o.at && later.connection !== connection)

/**
 * @param {object} evidence
 * @param {object[]} evidence.observations every Fetch frame of the member
 * @param {string} evidence.key the moved partition's `topicId|partition`
 * @param {number} evidence.movedAt `performance.now()` when the move was issued
 * @param {number} evidence.oldNode
 * @param {number} evidence.newNode
 * @returns {string[]} how the evidence breaks the leader-move oracle: the first request holding the
 *   partition after the move must go to the new leader, and the old node must not hold it again
 */
const moveProblems = ({ observations, key, movedAt, oldNode, newNode }) => {
  const problems = []
  const arrived = observations.find((o) => o.at > movedAt && o.node === newNode && holds(o, key))
  if (!arrived) return ['no request held the partition on the new leader']
  const stale = observations.filter(
    (o) => o.at > arrived.at && o.node === oldNode && holds(o, key) === true
  )
  if (stale.length) problems.push(`the old leader held the partition ${stale.length} more times`)
  return problems
}

/**
 * @param {object} evidence
 * @param {object[]} evidence.observations every Fetch frame of the member
 * @param {{ node: number, at: number }} evidence.hint the answer that named the follower
 * @param {string} evidence.key the hinted partition's `topicId|partition`
 * @param {number} evidence.follower the node the answer named
 * @param {string} evidence.rackId the member's rack
 * @returns {string[]} how the evidence breaks the follower oracle: the first request after the hint
 *   whose session holds the partition must go to the follower, so a removal sent to the old node
 *   does not count; the follower must answer the partition; every frame carries the rack
 */
const followerProblems = ({ observations, hint, key, follower, rackId }) => {
  const problems = []
  const next = firstHolding(observations, key, hint.at)
  if (!next) problems.push('no later request held the hinted partition')
  else if (next.node !== follower) {
    problems.push(`the first request holding the partition went to node ${next.node}`)
  }
  if (!observations.some((o) => o.node === follower && o.answer?.keys.includes(key))) {
    problems.push('the follower never answered the partition')
  }
  const wrongRack = observations.filter((o) => o.rackId !== rackId)
  if (wrongRack.length) problems.push(`${wrongRack.length} frames carried another rack`)
  return problems
}

/**
 * @param {object} evidence
 * @param {object[]} evidence.before the frames before the close began; the IDs it held
 * @param {object[]} evidence.after the frames once the close and a quiet period are over
 * @param {number} evidence.startedAt `performance.now()` when the close was called
 * @param {number} evidence.endedAt `performance.now()` when it resolved
 * @param {number} evidence.budgetMs the most the close may take, grace included
 * @returns {string[]} how the evidence breaks the close oracle: one empty close per node holding a
 *   positive ID, naming that ID, written after the node's last ordinary Fetch was answered; the
 *   close within its budget; and no frame after it resolved
 */
const closeProblems = ({ before, after, startedAt, endedAt, budgetMs }) => {
  const problems = []
  const lastOf = new Map()
  for (const o of before) {
    if (o.sessionEpoch >= 0 && o.answer?.sessionId > 0) lastOf.set(o.node, o)
  }
  const closes = after.filter((o) => o.sessionEpoch === -1 && o.sessionId > 0)
  for (const [node, last] of lastOf) {
    const mine = closes.filter((o) => o.node === node)
    if (mine.length !== 1) problems.push(`node ${node} got ${mine.length} closes`)
    const [close] = mine
    if (!close) continue
    if (close.sessionId !== last.answer.sessionId) problems.push(`node ${node} closed another ID`)
    if (close.cells.length || close.forgotten.length) problems.push(`node ${node} close had topics`)
    const ordinary = after.filter((o) => o.node === node && o.sessionEpoch >= 0)
    if (ordinary.some((o) => o.answeredAt === null || o.answeredAt > close.at)) {
      problems.push(`node ${node} closed before its last Fetch was answered`)
    }
  }
  if (endedAt - startedAt > budgetMs) problems.push(`the close took ${endedAt - startedAt} ms`)
  const late = after.filter((o) => o.at > endedAt)
  if (late.length) problems.push(`${late.length} frames were written after the close resolved`)
  return problems
}

/**
 * @param {Array<Array<object>>} consumers each independent consumer's observations
 * @param {number} [slots=8] the broker's configured session cache slots per node
 * @returns {string[]} how the evidence breaks the capacity oracle: every node that carried
 *   partitions must have been asked by more consumers than it has slots, with an observed ID 0
 *   fallback, a session error or an eviction, and with positive sessions and accepted incremental
 *   requests beside it, so that neither the fallback path nor the session path is missing
 */
const capacityProblems = (consumers, slots = 8) => {
  const problems = []
  const all = consumers.flat()
  const nodes = [
    ...new Set(all.filter((o) => o.sessionEpoch === 0 && o.cells.length).map((o) => o.node)),
  ]
  if (nodes.length === 0) problems.push('no full request carried a partition')
  for (const node of nodes) {
    const asking = consumers.filter((own) =>
      own.some((o) => o.node === node && o.sessionEpoch === 0 && o.cells.length)
    ).length
    if (asking <= slots) problems.push(`node ${node} was asked by ${asking} consumers`)
  }
  const facts = sessionFacts(all)
  if (facts.fallback.length + facts.refused.length === 0) {
    problems.push('no fallback, rejection or eviction was observed')
  }
  if (facts.created.length === 0) problems.push('no consumer held a positive session')
  if (facts.incremental.length === 0) problems.push('no incremental request was accepted')
  return problems
}

/**
 * One consumer under test: its client, the wire its sockets showed, what its handlers were given,
 * and the membership events and commits it reported.
 */
class Member {
  /**
   * @param {object} options
   * @param {string} options.name names the member in client ids
   * @param {SessionWire} options.wire
   * @param {object} options.consumer
   * @param {boolean} options.group whether it has a group
   */
  constructor({ name, wire, consumer, group }) {
    this.name = name
    this.wire = wire
    this.consumer = consumer
    this.group = group
    /** @type {Array<{ topic: string, partition: number, offset: string, value: string, at: number }>} */
    this.handled = []
    /** @type {object[]} */
    this.joins = []
    /** @type {Error[]} */
    this.crashes = []
    /** @type {Array<{ topic: string, partition: number, offset: number, at: number }>} */
    this.commits = []
  }

  /**
   * @param {string} topic
   * @returns {number[]} the partitions of the topic in the member's last complete join, sorted
   */
  owned(topic) {
    return [...(this.joins.at(-1)?.memberAssignment?.[topic] ?? [])].sort((a, b) => a - b)
  }
}

/**
 * The topics, producer, admin and consumers of one live case on the compose cluster. It keeps
 * protocol work only: every wait goes through the case scope, and every client is registered for
 * cleanup before the operation that can make it partially.
 */
class SessionRig {
  /**
   * @param {AbortSignal} scope the case scope
   * @param {import('../../../producer/__tests__/fixtures/liveProbe').CleanupStack} cleanup
   * @param {string} id the case's id
   * @param {string} prefix the scenario's client and topic prefix
   */
  constructor(scope, cleanup, id, prefix) {
    this.scope = scope
    this.cleanup = cleanup
    this.id = id
    this.prefix = prefix
    this.brokers = plainTextBrokers()
    /** @type {Member[]} */
    this.members = []
    /** @type {Array<{ topic: string, partition: number, value: string }>} what the sends acknowledged */
    this.acknowledged = []
    /** @type {string[]} */
    this.topics = []
    /** @type {string} the step the rig reached, for the evidence of a failed case */
    this.stage = 'created'
    /** @type {Record<string, unknown>} what the case selected and measured */
    this.facts = {}
    this.nodes = new Map()
  }

  /**
   * Connects the admin and the producer and learns the cluster's nodes. Must run first.
   * @returns {Promise<void>}
   */
  async open() {
    const { scope, cleanup, id, prefix } = this
    this.stage = 'opening'
    this.kafka = new Kafka({
      clientId: `${prefix}-admin-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    this.admin = this.kafka.admin()
    cleanup.add('admin', () => this.admin.disconnect())
    await step(scope, 'The admin connect', () => this.admin.connect())
    this.producer = this.kafka.producer()
    cleanup.add('producer', () => this.producer.disconnect())
    await step(scope, 'The producer connect', () => this.producer.connect())
    const { brokers } = await step(scope, 'The cluster read', () => this.admin.describeCluster())
    this.nodeIds = brokers.map(({ nodeId }) => nodeId).sort((a, b) => a - b)
    this.endpoints = new Map(brokers.map(({ nodeId, host, port }) => [nodeId, `${host}:${port}`]))
    this.cluster = createCluster({}, this.brokers)
    cleanup.add('cluster', () => this.cluster.disconnect())
    await step(scope, 'The cluster connect', () => this.cluster.connect())
    const { brokers: racked } = await step(scope, 'The broker racks', () =>
      this.cluster.metadata({ topics: [] })
    )
    this.racks = new Map(racked.map(({ nodeId, rack }) => [nodeId, rack]))
  }

  /**
   * Creates a topic, registers its deletion, and waits for full replica sets.
   * @param {object} spec
   * @param {string} spec.topic
   * @param {number} [spec.partitions] with the default placement
   * @param {Array<{ partition: number, replicas: number[] }>} [spec.replicaAssignment] explicit
   *   placement; replaces `partitions`
   * @returns {Promise<string>} the topic's ID once the cluster knows it
   */
  async createTopic({ topic, partitions, replicaAssignment }) {
    const { scope, cleanup } = this
    this.stage = `creating ${topic}`
    this.topics.push(topic)
    cleanup.add(`topic ${topic}`, (signal) => deleteTopics(this.kafka, [topic], signal))
    await step(scope, 'The topic creation', () =>
      this.admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [
          replicaAssignment
            ? { topic, replicaAssignment }
            : { topic, numPartitions: partitions, replicationFactor: 1 },
        ],
      })
    )
    await step(scope, 'The full in-sync replica sets', () => waitForFullIsr(this.admin, [topic]))
    return this.topicId(topic)
  }

  /**
   * @param {string} topic
   * @returns {Promise<string>} the ID the cluster currently holds for the topic
   */
  async topicId(topic) {
    await step(this.scope, 'The target topic', () => this.cluster.addTargetTopic(topic))
    await step(this.scope, 'The topic ids', () => this.cluster.refreshMetadata())
    return this.cluster.topicId(topic)
  }

  /**
   * Registers every endpoint of the cluster with a wire, so its frames name their node.
   * @param {SessionWire} wire
   * @returns {void}
   */
  learnNodes(wire) {
    for (const [nodeId, endpoint] of this.endpoints) wire.nodes.set(endpoint, nodeId)
  }

  /**
   * Starts one consumer with Fetch sessions on and waits until it owns what it should.
   * @param {object} options
   * @param {string} options.name names the member
   * @param {string} options.mode a key of `MODES`
   * @param {string} options.topic
   * @param {number[]} options.partitions every partition the member must end up owning
   * @param {string} [options.groupId]
   * @param {string} [options.rackId]
   * @param {boolean} [options.rackAwareAssignment] the public option, when the case sets it
   * @param {boolean} [options.awaitOwnership=true] wait until the member owns every partition; a
   *   cohort that shares a group sets it false, because its members split them
   * @param {number} [options.requestTimeout] the client's request timeout, ms
   * @param {Function} [options.innerSocketFactory] what the wire observes, e.g. a fault injector;
   *   the default socket factory when absent
   * @returns {Promise<Member>}
   */
  async startMember({
    name,
    mode,
    topic,
    partitions,
    groupId,
    rackId,
    rackAwareAssignment,
    awaitOwnership = true,
    requestTimeout,
    innerSocketFactory,
  }) {
    const { scope, cleanup, id, prefix } = this
    const shape = MODES[mode]
    this.stage = `starting ${name}`
    const wire = new SessionWire(innerSocketFactory)
    this.learnNodes(wire)
    const kafka = new Kafka({
      clientId: `${prefix}-${name}-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: wire.socketFactory,
      ...(requestTimeout && { requestTimeout }),
    })
    const classic = shape.options.groupProtocol === 'classic'
    const consumer = kafka.consumer({
      ...shape.options,
      ...(shape.group && { groupId }),
      ...(shape.static && { groupInstanceId: `${prefix}-${name}-${id}` }),
      ...(classic && { heartbeatInterval: 1000, sessionTimeout: 30000 }),
      ...(rackId && { rackId }),
      ...(rackAwareAssignment !== undefined && { rackAwareAssignment }),
      maxWaitTimeInMs: WAIT_MS,
      fetchSessions: true,
    })
    const member = new Member({ name, wire, consumer, group: shape.group })
    this.members.push(member)
    const off = [
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => member.joins.push(payload)),
      consumer.on(consumer.events.CRASH, ({ payload }) => member.crashes.push(payload.error)),
      consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => {
        for (const entry of payload.topics) {
          for (const { partition, offset } of entry.partitions) {
            member.commits.push({
              topic: entry.topic,
              partition,
              offset: Number(offset),
              at: Date.now(),
            })
          }
        }
      }),
    ]
    cleanup.add(`${name} listeners`, () => off.forEach((remove) => remove()))
    cleanup.add(name, () => consumer.disconnect())
    await step(scope, 'The consumer connect', () => consumer.connect())
    if (shape.group) {
      await step(scope, 'The consumer subscription', () =>
        consumer.subscribe({ topics: [topic], fromBeginning: true })
      )
    } else {
      await step(scope, 'The manual assignment', () =>
        consumer.assign([{ topic, partitions }], { fromBeginning: true })
      )
    }
    await step(scope, 'The consumer start', () =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic: from, partition, message }) => {
          if (scope.aborted) return
          member.handled.push({
            topic: from,
            partition,
            offset: message.offset,
            value: message.value.toString(),
            at: Date.now(),
          })
          if (shape.group) {
            await consumer.commitOffsets([
              { topic: from, partition, offset: String(Number(message.offset) + 1) },
            ])
          }
        },
      })
    )
    if (shape.group && awaitOwnership) {
      await pollUntil(scope, STEP_MS, () => member.owned(topic).length === partitions.length, {
        delayMs: POLL_MS,
        label: `The ${name} assignment`,
      })
    }
    return member
  }

  /**
   * Sends records with acks -1, `perPartition` to each of the partitions.
   * @param {string} label names the corpus, so no value repeats
   * @param {string} topic
   * @param {number[]} partitions
   * @param {number} perPartition
   * @returns {Promise<Array<{ topic: string, partition: number, value: string }>>} the records the
   *   send acknowledged, kept in `acknowledged`
   */
  async produce(label, topic, partitions, perPartition) {
    this.stage = `sending ${label}`
    const messages = partitions.flatMap((partition) =>
      Array.from({ length: perPartition }, (_, n) => ({
        partition,
        value: `${label}-${partition}-${n}`,
      }))
    )
    await step(this.scope, 'The send', () => this.producer.send({ topic, acks: -1, messages }))
    const records = messages.map(({ partition, value }) => ({ topic, partition, value }))
    this.acknowledged.push(...records)
    return records
  }

  /**
   * Waits until every record has been given to a handler of the member.
   * @param {Member} member
   * @param {object[]} records
   * @returns {Promise<void>}
   * @throws {Error} when a record is not handled within the wait
   */
  async delivered(member, records) {
    this.stage = `waiting for ${member.name}'s deliveries`
    const have = () => new Set(member.handled.map(identity))
    await pollUntil(this.scope, DELIVERY_MS, () => records.every((r) => have().has(identity(r))), {
      delayMs: POLL_MS,
      label: 'The deliveries',
    })
  }

  /**
   * Waits one quiet period, in which an unexpected delivery or request would show.
   * @returns {Promise<void>}
   */
  quiet() {
    return delay(this.scope, QUIET_MS)
  }

  /**
   * Reads the offsets the broker holds for a group.
   * @param {string} groupId
   * @param {string} topic
   * @returns {Promise<Map<string, number>>} the next offset of `topic|partition`, -1 for none
   */
  async committed(groupId, topic) {
    const [{ partitions }] = await step(this.scope, 'The offset read', () =>
      this.admin.fetchOffsets({ groupId, topics: [topic] })
    )
    return new Map(
      partitions.map(({ partition, offset }) => [`${topic}|${partition}`, Number(offset)])
    )
  }

  /**
   * @returns {Promise<object>} what the rig retained, whatever point it reached: the stage, the
   *   facts, what was acknowledged and handled, and each member's last Fetch frames. A part that
   *   cannot be read is reported as unavailable, so the row is always written.
   */
  async evidence() {
    const members = []
    for (const member of this.members) {
      let wire
      try {
        wire = (await member.wire.observations()).slice(-200).map(({ heldKeys, ...rest }) => rest)
      } catch (error) {
        wire = { unavailable: error.message }
      }
      members.push({
        name: member.name,
        handled: member.handled,
        commits: member.commits,
        joins: member.joins.length,
        crashes: member.crashes.map((error) => error.message),
        wire,
      })
    }
    return {
      stage: this.stage,
      facts: this.facts,
      topics: this.topics,
      acknowledged: this.acknowledged,
      members,
    }
  }
}

module.exports = {
  FETCH,
  MODES,
  POLL_MS,
  QUIET_MS,
  SessionRig,
  SessionWire,
  STEP_MS,
  WAIT_MS,
  capacityProblems,
  commitProblems,
  corpusProblems,
  closeProblems,
  connectionOf,
  deleteTopics,
  followerProblems,
  holds,
  identity,
  lostOnConnection,
  monotonicOf,
  moveProblems,
  sessionProblems,
  summarize,
  sessionFacts,
  step,
}
