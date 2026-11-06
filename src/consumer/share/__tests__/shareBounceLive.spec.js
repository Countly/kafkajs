const net = require('net')
const { EventEmitter } = require('events')
const {
  Kafka,
  logLevel,
  AlterConfigOpTypes,
  ConfigResourceTypes,
  ConfigSource,
} = require('../../../../index')
const {
  createTopic,
  plainTextBrokers,
  readRequestHeader,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const {
  assertShareCleanup,
  setShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
  verifierMember,
} = require('../../../../testHelpers/shareFixtures')
const { BrokerBouncer, NODES, waitForFullIsr } = require('../../../../testHelpers/leaderElection')
const { readLog } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const {
  pollSharePosition,
  acknowledgementResults,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

// Gated: it needs the three-node share-enabled base cluster (docker-compose.yml, broker share
// session 45000 ms and heartbeat 5000 ms) and runs through the share-base lane or the guarded live
// entry with SHARE_BOUNCE_E2E=1.
const enabled = process.env.SHARE_BOUNCE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-bounce'
const PROFILE = 'tv2'
const TITLE = 'preserves share records across clean and hard rolling restarts'
const BODY_MS = 500000
const HOOK_MS = 10000
const CLEANUP_MS = 45000
const BUDGET_MS = BODY_MS + CLEANUP_MS
const PARTITIONS = 3
const RECORDS = 30000
const BURST_SIZE = 100
const BURSTS = RECORDS / BURST_SIZE
const BURST_INTERVAL_MS = 400
const LATE_BURST_MS = 1000
const WARMUP_APPENDS = 2000
const MAX_OUTAGE_MS = 45000
const MAX_HELD = 300
const MAX_REPLAY = 6 * MAX_HELD
const MEMBERSHIP_QUIET_MS = 3000
const OLD_TIMESTAMP = '1700000000000'
const SERVICES = ['kafka1', 'kafka2', 'kafka3']
const SHARE_STATE_TOPIC = '__share_group_state'
const OFFSETS_TOPIC = '__consumer_offsets'
const SHARE_APIS = new Set([76, 78, 79])
const SESSION_TIMEOUT = { name: 'group.share.session.timeout.ms', value: '45000' }
const HEARTBEAT_INTERVAL = { name: 'group.share.heartbeat.interval.ms', value: '5000' }
const GROUP_CONFIG = {
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': 30000,
}
const CLIENT_CONFIG = {
  acknowledgementMode: 'implicit',
  acquireMode: 'record_limit',
  maxRecords: 100,
  maxWaitTimeInMs: 250,
}
const RETRY = { retries: 40, initialRetryTime: 200, maxRetryTime: 2000 }

/**
 * Splits a byte stream into whole length-prefixed Kafka frames.
 */
class FrameSplitter {
  /** Starts with no partial frame. */
  constructor() {
    this.carry = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk the next bytes of the stream
   * @returns {Buffer[]} copies of the frames this chunk completed, each with its 4-byte length
   */
  push(chunk) {
    let bytes = this.carry.length === 0 ? chunk : Buffer.concat([this.carry, chunk])
    const frames = []
    while (bytes.length >= 4 && bytes.length >= 4 + bytes.readInt32BE(0)) {
      const end = 4 + bytes.readInt32BE(0)
      frames.push(Buffer.from(bytes.subarray(0, end)))
      bytes = bytes.subarray(end)
    }
    this.carry = Buffer.from(bytes)
    return frames
  }
}

/**
 * Wraps the plaintext socket factory of every client of one case. It counts sockets and unanswered
 * requests from the bytes actually written and read, and keeps the share frames each socket sent.
 * Bytes, order, return values and callbacks of every write are untouched.
 */
class ShareWireTap {
  /**
   * @param {Function} [connect] opens one socket; replaced by a fake in the offline checks
   */
  constructor(connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect)) {
    this.connect = connect
    this.sockets = []
    this.frames = []
  }

  /**
   * @param {string} label the client the sockets belong to
   * @returns {Function} a Kafka `socketFactory`
   */
  factory(label) {
    return (options) => {
      const socket = this.connect(options)
      socket.setKeepAlive?.(true, 60000)
      return this.wrap(label, socket)
    }
  }

  /**
   * @param {string} label the owning client
   * @param {object} socket the real socket
   * @returns {object} the same socket, observed
   */
  wrap(label, socket) {
    const entry = {
      label,
      socket,
      writer: new FrameSplitter(),
      reader: new FrameSplitter(),
      outstanding: new Map(),
    }
    this.sockets.push(entry)
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      const bytes = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
      entry.writer.push(bytes).forEach((frame) => this.record(entry, frame))
      return write(data, ...rest)
    }
    socket.on('data', (chunk) => {
      entry.reader.push(chunk).forEach((frame) => entry.outstanding.delete(frame.readInt32BE(4)))
    })
    return socket
  }

  /**
   * Notes one written request as unanswered, and keeps its header facts when it is a share API.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @returns {void}
   */
  record(entry, frame) {
    const { apiKey, apiVersion, correlationId } = readRequestHeader(frame)
    const fact = { label: entry.label, socket: this.sockets.indexOf(entry), apiKey, apiVersion }
    entry.outstanding.set(correlationId, { ...fact, correlationId })
    if (SHARE_APIS.has(apiKey)) this.frames.push({ ...fact, correlationId, at: performance.now() })
  }

  /** @returns {{ created: number, destroyed: number }} sockets opened and sockets since destroyed */
  socketCounts() {
    return {
      created: this.sockets.length,
      destroyed: this.sockets.filter(({ socket }) => socket.destroyed).length,
    }
  }

  /** @returns {number} requests written on live sockets that have no response yet */
  pending() {
    return this.liveOutstanding().length
  }

  /** @returns {object[]} the share requests on live sockets that have no response yet */
  inFlight() {
    return this.liveOutstanding().filter(({ apiKey }) => SHARE_APIS.has(apiKey))
  }

  /** @returns {object[]} every unanswered request on a socket that is still open */
  liveOutstanding() {
    return this.sockets
      .filter(({ socket }) => !socket.destroyed)
      .flatMap(({ outstanding }) => [...outstanding.values()])
  }
}

/**
 * @param {string[]} identities identities with repeats
 * @returns {Map<string, number>} how often each occurs
 */
const tally = (identities) =>
  identities.reduce((map, id) => map.set(id, (map.get(id) ?? 0) + 1), new Map())

/**
 * @param {Map<string, number>} map a tally
 * @returns {Array<[string, number]>} its entries in identity order
 */
const sortedEntries = (map) => [...map].sort(([a], [b]) => (a < b ? -1 : 1))

/**
 * @param {Map<string, number>} expected what must be seen, with multiplicity
 * @param {Map<string, number>} actual what was seen
 * @returns {{ missing: string[], extra: string[], count: string[] }} identities not seen,
 *   unexpected, or seen another number of times
 */
const multisetProblems = (expected, actual) => ({
  missing: [...expected.keys()].filter((id) => !actual.has(id)),
  extra: [...actual.keys()].filter((id) => !expected.has(id)),
  count: [...expected]
    .filter(([id, n]) => actual.has(id) && actual.get(id) !== n)
    .map(([id, n]) => `${id} expected ${n} saw ${actual.get(id)}`),
})

/**
 * @param {Array<{ partitions: number[], topic: string }>} assignment an assignment
 * @returns {string} a comparable form independent of order
 */
const normalizeAssignment = (assignment) =>
  JSON.stringify(
    assignment.map(({ topic, partitions }) => [topic, [...partitions].sort((a, b) => a - b)]).sort()
  )

/**
 * Charges every handler entry beyond an identity's first to the bounce that left it unresolved.
 * An entry needs an earlier bounce whose frozen held set named the identity, used at most once per
 * entry, and a delivery count that went up. Entries of an identity that were never held, and entries
 * beyond the bounce allowance, are reported as problems; none is ever excused by what was received.
 * @param {Array<{ identity: string, deliveryCount: number, at: number }>} entries every handler
 *   start, in start order
 * @param {Array<{ at: number, held: Set<string> }>} bounces the frozen held set of each bounce, in
 *   the order the bounces began
 * @returns {{ extras: Array<{ identity: string, bounce: number, deliveryCount: number, at: number }>,
 *   perBounce: number[], problems: string[] }} the charged entries, how many each bounce took, and
 *   what could not be charged
 */
const auditReplay = (entries, bounces) => {
  const byIdentity = new Map()
  for (const entry of entries) {
    if (!byIdentity.has(entry.identity)) byIdentity.set(entry.identity, [])
    byIdentity.get(entry.identity).push(entry)
  }
  const extras = []
  const problems = []
  for (const [identity, list] of byIdentity) {
    const eligible = bounces.flatMap((bounce, index) => (bounce.held.has(identity) ? [index] : []))
    let next = 0
    for (let i = 1; i < list.length; i++) {
      const { deliveryCount, at } = list[i]
      if (!(deliveryCount > list[i - 1].deliveryCount)) {
        problems.push(`${identity} was entered again without a higher delivery count`)
      }
      const bounce = eligible[next]
      if (bounce === undefined || bounces[bounce].at > at) {
        problems.push(`${identity} was entered ${list.length} times without a bounce holding it`)
        break
      }
      next++
      extras.push({ identity, bounce, deliveryCount, at })
    }
  }
  const perBounce = bounces.map(
    (_, index) => extras.filter(({ bounce }) => bounce === index).length
  )
  perBounce.forEach((count, index) => {
    if (count > MAX_HELD) problems.push(`bounce ${index} took ${count} extra entries`)
  })
  if (extras.length > MAX_REPLAY) problems.push(`${extras.length} extra entries in total`)
  return { extras, perBounce, problems }
}

/**
 * One live run of the bounce scenario: a share member reads three partitions of a replication
 * factor three, min-ISR two topic while a producer appends 30000 records, and every broker is taken
 * down in turn, cleanly and then hard. The log, the broker position and the handler entries are
 * checked as three independent oracles.
 */
class ShareBounceCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.topic = shareTopicName('bounce')
    this.group = `e2e-p9e-bounce-group-${this.nonce}`
    this.stack = new CleanupStack()
    this.tap = new ShareWireTap()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      queueDepth: () => this.tap.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.bouncer = new BrokerBouncer({ now: () => performance.now() })
    this.openHandlers = 0
    this.requests = []
    this.expected = []
    this.sends = []
    this.pendingSends = []
    this.appended = 0
    this.lateness = 0
    this.entries = []
    this.seen = new Set()
    this.confirmed = new Set()
    this.ackProblems = []
    this.joins = []
    this.changes = []
    this.resets = []
    this.crashes = []
    this.handlerErrors = []
    this.snapshots = []
    this.outages = []
    this.baselineMembership = null
    this.readiness = null
    this.shareState = null
    this.baseline = null
    this.beforeAdmin = null
    this.groupConfig = GROUP_CONFIG
    this.clientConfig = CLIENT_CONFIG
  }

  /**
   * @param {string} label the client label, also the start of its clientId
   * @returns {Kafka} a client whose sockets go through the tap and whose requests retry through an
   *   outage
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.factory(label),
      retry: RETRY,
    })
  }

  /**
   * Appends one evidence row.
   * @param {string} event the row kind
   * @param {object} [fields] the measured facts
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: TITLE,
      profile: PROFILE,
      scenario: this.nonce,
      groups: { bounce: this.group },
      budgets: { caseMs: BUDGET_MS, cleanupMs: CLEANUP_MS },
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
  }

  /** @returns {object} Progress and public outcomes before cleanup can change them. */
  progress() {
    return {
      appended: this.appended,
      handlerEntries: this.entries.length,
      distinctHandled: this.seen.size,
      confirmedOffsets: this.confirmed.size,
      sendFailures: this.sends.filter(({ error }) => error).length,
      crashes: this.crashes.map((error) => describeError(error)),
      handlerErrors: this.handlerErrors,
      resets: this.resets,
      acknowledgementProblems: this.ackProblems,
      outages: this.outages,
      bounces: this.snapshots.map(({ held, ...rest }) => ({ ...rest, held: held.size })),
    }
  }

  /**
   * @param {number} limit the phase's own allowance, ms
   * @param {number} [reserve] what later phases still need, ms
   * @returns {number} the allowance, cut to what the case budget has left
   */
  window(limit, reserve = CLEANUP_MS) {
    return Math.max(1000, Math.min(limit, this.deadline - performance.now() - reserve))
  }

  /**
   * Runs the whole case: setup, the concurrent produce and bounce, settlement, the three oracles
   * and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    this.deadline = performance.now() + BUDGET_MS
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await bounded(scope, this.window(330000, 100000), (inner) => this.drive(inner), {
            label: 'The produce and bounce',
          })
          await bounded(scope, this.window(60000, 70000), (inner) => this.drain(inner), {
            label: 'The settlement',
          })
          await this.observe(scope)
          await this.finish(scope)
        },
        { budgetMs: CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.row('case-failed', { error: describeError(error), ...this.progress() })
      throw error
    }
  }

  /**
   * Checks the cluster shape, creates the replication factor three topic, applies and reads back
   * the group config, measures the baseline, and starts a producer and the member, waiting until the
   * member and the admin API agree on its membership.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} when the cluster does not match the scenario or the member never joins
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    this.assertOwnership()
    await bounded(scope, 30000, () => this.bouncer.assertAllUp())
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    this.stack.add('restore every broker', (signal) => this.restoreBrokers(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 60000, () =>
      createTopic({
        topic: this.topic,
        partitions: PARTITIONS,
        replicas: 3,
        config: [
          { name: 'min.insync.replicas', value: '2' },
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 20000, (inner) => this.readReadiness(inner))
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, this.group, this.groupConfig)
    )
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({
      idempotent: true,
      maxInFlightRequests: 5,
      retry: RETRY,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
    await this.startMember(scope)
    await this.awaitBaseline(scope)
  }

  /**
   * Refuses a half-set controller ownership environment before any broker is touched.
   * @returns {void}
   * @throws {Error} when only one of the guard and its manifest is named
   */
  assertOwnership() {
    const { KAFKAJS_CLUSTER_GUARD: guard, KAFKAJS_CLUSTER_MANIFEST: manifest } = process.env
    if (Boolean(guard) !== Boolean(manifest)) {
      throw new Error('The cluster guard and its manifest must be named together')
    }
    this.guarded = Boolean(guard)
  }

  /**
   * Reads the topic and broker settings back and refuses a cluster that is not the scenario's: three
   * partitions of three replicas in sync, min-ISR two, and the base share session of 45000 ms and
   * heartbeat of 5000 ms on every node.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   * @throws {Error} naming the readiness facts when any differs
   */
  async readReadiness(scope) {
    const { resources } = await this.admin.describeConfigs({
      includeSynonyms: false,
      resources: [
        {
          type: ConfigResourceTypes.TOPIC,
          name: this.topic,
          configNames: ['min.insync.replicas', 'retention.ms', 'cleanup.policy'],
        },
        ...Object.values(NODES).map(({ nodeId }) => ({
          type: ConfigResourceTypes.BROKER,
          name: String(nodeId),
          configNames: [SESSION_TIMEOUT.name, HEARTBEAT_INTERVAL.name],
        })),
      ],
    })
    const valueOf = (type, name, config) =>
      resources
        .find(({ resourceType, resourceName }) => resourceType === type && resourceName === name)
        ?.configEntries.find(({ configName }) => configName === config)?.configValue
    scope.throwIfAborted()
    const { topics } = await this.admin.fetchTopicMetadata({ topics: [this.topic] })
    const partitions = topics[0].partitions
    this.readiness = {
      nodes: Object.keys(NODES).length,
      maxRecords: CLIENT_CONFIG.maxRecords,
      minIsr: Number(valueOf(ConfigResourceTypes.TOPIC, this.topic, 'min.insync.replicas')),
      retention: valueOf(ConfigResourceTypes.TOPIC, this.topic, 'retention.ms'),
      cleanupPolicy: valueOf(ConfigResourceTypes.TOPIC, this.topic, 'cleanup.policy'),
      partitions: partitions.length,
      replicas: partitions.map(({ replicas }) => replicas.length),
      isr: partitions.map(({ isr }) => isr.length),
      distinctReplicas: partitions.map(({ replicas }) => new Set(replicas).size),
      sessionMs: Object.values(NODES).map(({ nodeId }) =>
        valueOf(ConfigResourceTypes.BROKER, String(nodeId), SESSION_TIMEOUT.name)
      ),
      heartbeatMs: Object.values(NODES).map(({ nodeId }) =>
        valueOf(ConfigResourceTypes.BROKER, String(nodeId), HEARTBEAT_INTERVAL.name)
      ),
      guarded: this.guarded,
    }
    this.row('readiness', this.readiness)
    const r = this.readiness
    if (
      r.minIsr !== 2 ||
      r.retention !== '-1' ||
      r.cleanupPolicy !== 'delete' ||
      r.partitions !== PARTITIONS ||
      r.replicas.some((count) => count !== 3) ||
      r.isr.some((count) => count !== 3) ||
      r.distinctReplicas.some((count) => count !== 3) ||
      r.sessionMs.some((value) => value !== SESSION_TIMEOUT.value) ||
      r.heartbeatMs.some((value) => value !== HEARTBEAT_INTERVAL.value)
    ) {
      throw new Error(`The cluster does not match the scenario: ${JSON.stringify(r)}`)
    }
  }

  /**
   * Starts the share member: every listener first, then connect, subscribe and run.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   */
  async startMember(scope) {
    const consumer = this.client('member').shareConsumer({ groupId: this.group, ...CLIENT_CONFIG })
    this.consumer = consumer
    const { events } = consumer
    const at = () => performance.now()
    const removers = [
      consumer.on(events.GROUP_JOIN, ({ payload }) =>
        this.joins.push({ at: at(), memberId: payload.memberId })
      ),
      consumer.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        this.assignment = payload.assignment
        this.changes.push({
          at: at(),
          added: payload.added,
          removed: payload.removed,
          assignment: payload.assignment,
        })
      }),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => this.onAcknowledge(payload)),
      consumer.on(events.SESSION_RESET, ({ payload }) =>
        this.resets.push({ at: at(), nodeId: payload.nodeId, reason: payload.reason })
      ),
      consumer.on(events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
      consumer.on(events.HANDLER_ERROR, ({ payload }) =>
        this.handlerErrors.push({ at: at(), topic: payload.topic, partition: payload.partition })
      ),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
    ]
    this.stack.add('remove member listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect member', () => consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.openHandlers++
          const identity = shareRecordIdentity({ ...message, topic, partition })
          this.seen.add(identity)
          this.entries.push({
            identity,
            key: `${partition}:${message.offset}`,
            deliveryCount: message.deliveryCount,
            at: performance.now(),
          })
          this.openHandlers--
        },
      })
    )
  }

  /**
   * Keeps what a public acknowledgement outcome says: a null-error accept confirms its offsets; every
   * other outcome, and a range too wide for a plain-record case, is kept as a problem for the
   * evidence. It never throws, because a listener's failure would be contained and lost.
   * @param {object} payload the ACKNOWLEDGE event payload
   * @returns {void}
   */
  onAcknowledge(payload) {
    const { partition, firstOffset, lastOffset, type, error } = payload
    const last = BigInt(lastOffset)
    if (type !== 'accept' || error !== null || last - BigInt(firstOffset) > 10000n) {
      this.ackProblems.push(...acknowledgementResults([payload]))
      return
    }
    for (let offset = BigInt(firstOffset); offset <= last; offset++) {
      this.confirmed.add(`${partition}:${offset}`)
    }
  }

  /**
   * Waits until the member holds every partition and the admin API reports exactly that one member
   * with the same assignment; that membership is the baseline the bounces must leave unchanged.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   */
  async awaitBaseline(scope) {
    await pollTolerating(
      scope,
      30000,
      async (poll) => {
        const [join] = this.joins
        if (!join || !this.assignment) return false
        const [member, ...others] = await this.describeMembers(poll)
        if (!member || others.length) return false
        const assignment = normalizeAssignment(this.assignment)
        const agreed =
          member.memberId === join.memberId && normalizeAssignment(member.assignment) === assignment
        if (agreed) this.baselineMembership = { memberId: member.memberId, assignment }
        return agreed
      },
      { label: 'The baseline membership' }
    )
    const covered = new Set(this.assignment.flatMap(({ partitions }) => partitions))
    expect([...covered].sort()).toEqual([0, 1, 2])
    this.row('assignment', { ...this.baselineMembership, joins: this.joins.length })
  }

  /**
   * @param {AbortSignal} scope the caller's scope
   * @returns {Promise<object[]>} the group's members as the admin API describes them
   */
  async describeMembers(scope) {
    const { groups } = await bounded(scope, 8000, () =>
      this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
    )
    return groups[0].members
  }

  /**
   * Runs the producer and the two rolling bounces together. Both settle before this returns; a
   * failure of either aborts the other, and the first failure is the one thrown.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the producer or the bounce
   */
  async drive(scope) {
    const own = new AbortController()
    const inner = AbortSignal.any([scope, own.signal])
    const failures = []
    const guard = (task) =>
      task.catch((error) => {
        failures.push(error)
        own.abort(error)
      })
    this.bouncer.run = this.watchDown(this.bouncer.run)
    await Promise.all([guard(this.produce(inner)), guard(this.bounce(inner))])
    if (failures.length) throw failures[0]
    const failed = this.sends.filter(({ error }) => error)
    if (failed.length) {
      throw new Error(`${failed.length} sends were not acknowledged: ${JSON.stringify(failed[0])}`)
    }
    this.row('produced', {
      records: this.expected.length,
      bursts: this.sends.length,
      produceStart: this.produceStart,
      produceEnd: this.produceEnd,
      maxLatenessMs: this.lateness,
    })
  }

  /**
   * Wraps the bouncer's command runner so the held set and the in-flight requests are frozen in the
   * same turn that a node is told to go down, with nothing awaited between the two.
   * @param {Function} run the bouncer's own runner, which stays the one that reaches the cluster
   * @returns {Function} the runner with the freeze in front
   */
  watchDown(run) {
    return (kind, service) => {
      if (kind === 'stop' || kind === 'kill') this.freeze(kind, service)
      return run(kind, service)
    }
  }

  /**
   * Freezes what is unresolved right now: the handler starts whose acknowledgement is not yet
   * confirmed, and the share requests still waiting for an answer.
   * @param {string} kind `stop` or `kill`
   * @param {string} service the compose service about to go down
   * @returns {void}
   */
  freeze(kind, service) {
    const held = new Set(
      this.entries.filter(({ key }) => !this.confirmed.has(key)).map(({ identity }) => identity)
    )
    this.snapshots.push({
      service,
      kind,
      at: performance.now(),
      held,
      handlerEntries: this.entries.length,
      confirmedOffsets: this.confirmed.size,
      inFlight: this.tap.inFlight(),
    })
  }

  /**
   * Appends the corpus in 300 bursts of 100 records, one burst every 400 ms, never waiting for a
   * send before the next burst is due. A late burst fails the run rather than change the duration.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} when a burst starts more than a second late
   */
  async produce(scope) {
    const start = performance.now()
    this.produceStart = start
    for (let burst = 0; burst < BURSTS; burst++) {
      const wait = start + burst * BURST_INTERVAL_MS - performance.now()
      if (wait > 0) await delay(scope, wait)
      scope.throwIfAborted()
      const late = Math.max(0, -wait)
      if (late > LATE_BURST_MS) {
        throw new Error(`Burst ${burst} started ${Math.round(late)} ms late`)
      }
      this.lateness = Math.max(this.lateness, late)
      this.sendBurst(burst)
    }
    await Promise.all(this.pendingSends)
    this.produceEnd = performance.now()
  }

  /**
   * @param {number} index the record's position in the corpus
   * @returns {{ partition: number, key: Buffer, value: Buffer, timestamp: string }} the record
   */
  message(index) {
    return {
      partition: index % PARTITIONS,
      key: Buffer.from(`bounce:${String(index).padStart(5, '0')}`),
      value: Buffer.from(`bounce:value:${index}`),
      timestamp: OLD_TIMESTAMP,
    }
  }

  /**
   * Sends one burst and keeps its outcome. Both settlements are observed at once, so a rejection
   * cannot go unhandled while the schedule waits for the next burst.
   * @param {number} burst the burst's number
   * @returns {void}
   */
  sendBurst(burst) {
    const messages = Array.from({ length: BURST_SIZE }, (_, n) =>
      this.message(burst * BURST_SIZE + n)
    )
    const send = { burst, startedAt: performance.now(), settledAt: null, error: null }
    this.sends.push(send)
    this.pendingSends.push(
      this.producer.send({ topic: this.topic, acks: -1, messages }).then(
        (results) => {
          send.settledAt = performance.now()
          send.error = this.recordAppends(messages, results)
        },
        (error) => {
          send.settledAt = performance.now()
          send.error = describeError(error)
        }
      )
    )
  }

  /**
   * Turns the broker's acknowledgement into the expected identities: each partition's records take
   * consecutive offsets from the acknowledged base offset.
   * @param {object[]} messages the burst
   * @param {object[]} results the send's per-partition outcomes
   * @returns {object|null} what was not acknowledged, or null when every record was
   */
  recordAppends(messages, results) {
    const next = new Map()
    for (const { partition, errorCode, baseOffset } of results) {
      if (errorCode === 0) next.set(partition, BigInt(baseOffset))
    }
    const missing = new Set()
    for (const { partition, key, value } of messages) {
      const offset = next.get(partition)
      if (offset === undefined) {
        missing.add(partition)
        continue
      }
      next.set(partition, offset + 1n)
      this.expected.push(
        shareRecordIdentity({ topic: this.topic, partition, offset: String(offset), key, value })
      )
      this.appended++
    }
    return missing.size ? { name: 'NotAcknowledged', partitions: [...missing] } : null
  }

  /**
   * Waits for the warm-up, checks that the share state topic survives every single removal, then
   * rolls every broker cleanly and hard, each step restored and back in full ISR before the next.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   */
  async bounce(scope) {
    await pollUntil(
      scope,
      60000,
      () => this.appended >= WARMUP_APPENDS && this.entries.length > 0,
      { label: 'The first appends and a delivery' }
    )
    await this.captureInternalTopics(scope)
    const topics = [this.topic, this.shareState.topic]
    for (const hard of [false, true]) {
      const outages = await this.bouncer.rollingBounce({
        hard,
        between: 0,
        admin: this.admin,
        topics,
        services: SERVICES,
        signal: scope,
      })
      this.outages.push(...outages)
      await bounded(scope, 90000, () => waitForFullIsr(this.admin, topics), {
        label: 'The full ISR',
      })
      await this.awaitMembership(scope, hard ? 'after the hard bounce' : 'after the clean bounce')
    }
  }

  /**
   * Captures the share state and offsets topics' real names, replicas, in-sync sets and min-ISR, and
   * refuses a cluster where removing any one node would leave a partition below its min-ISR.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} when the share state topic is missing or cannot survive a planned removal
   */
  async captureInternalTopics(scope) {
    const names = (await bounded(scope, 10000, () => this.admin.listTopics())).filter(
      (name) => name === SHARE_STATE_TOPIC
    )
    if (names.length !== 1) throw new Error(`No ${SHARE_STATE_TOPIC} topic after the member joined`)
    const topics = [...names, OFFSETS_TOPIC]
    const { topics: described } = await bounded(scope, 10000, () =>
      this.admin.fetchTopicMetadata({ topics })
    )
    const { resources } = await bounded(scope, 10000, () =>
      this.admin.describeConfigs({
        includeSynonyms: false,
        resources: topics.map((name) => ({
          type: ConfigResourceTypes.TOPIC,
          name,
          configNames: ['min.insync.replicas'],
        })),
      })
    )
    const internal = described.map(({ name, partitions }) => ({
      topic: name,
      minIsr: Number(
        resources
          .find(({ resourceName }) => resourceName === name)
          ?.configEntries.find(({ configName }) => configName === 'min.insync.replicas')
          ?.configValue
      ),
      partitions: partitions.map(({ partitionId, leader, replicas, isr }) => ({
        partition: partitionId,
        leader,
        replicas,
        isr,
      })),
    }))
    const short = internal.flatMap(({ topic, minIsr, partitions }) =>
      Object.entries(NODES).flatMap(([service, { nodeId }]) =>
        partitions
          .filter(({ isr }) => !(isr.filter((id) => id !== nodeId).length >= minIsr))
          .map(({ partition }) => `${topic}-${partition} without ${service}`)
      )
    )
    this.shareState = internal.find(({ topic }) => topic === SHARE_STATE_TOPIC)
    this.row('internal-topics', { topics: internal, voters: 3, maxDown: 1, short })
    if (short.length) throw new Error(`An internal partition cannot survive a removal: ${short}`)
  }

  /**
   * Waits until the admin API reports exactly the baseline member and assignment again.
   * @param {AbortSignal} scope the phase scope
   * @param {string} stage what has just finished, for the failure message
   * @returns {Promise<void>}
   */
  async awaitMembership(scope, stage) {
    const { memberId, assignment } = this.baselineMembership
    await pollTolerating(
      scope,
      30000,
      async (poll) => {
        const members = await this.describeMembers(poll)
        return (
          members.length === 1 &&
          members[0].memberId === memberId &&
          normalizeAssignment(members[0].assignment) === assignment
        )
      },
      { label: `The baseline membership ${stage}` }
    )
  }

  /**
   * Waits until every fixed identity has been handled and its acknowledgement confirmed.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   */
  async drain(scope) {
    await pollUntil(
      scope,
      60000,
      () => this.seen.size >= RECORDS && this.confirmed.size >= RECORDS,
      { label: 'Every identity settling', delayMs: 500 }
    )
  }

  /**
   * Checks the six outages, the membership and replay allowances, and the three independent
   * oracles: raw log, handler entries and broker offsets.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    this.checkBounces()
    const log = await bounded(scope, 40000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    this.log = log
    const raw = []
    for (const [partition, { endOffset, records }] of log) {
      expect(endOffset).toBe(String(RECORDS / PARTITIONS))
      expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
      expect(records.map(({ offset }) => String(offset))).toEqual(
        Array.from({ length: RECORDS / PARTITIONS }, (_, n) => String(n))
      )
      for (const record of records) {
        raw.push(
          shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(record.offset),
            key: record.key,
            value: record.value,
          })
        )
      }
    }
    this.rawIdentities = raw
    expect(multisetProblems(tally(this.expected), tally(raw))).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    expect(this.expected).toHaveLength(RECORDS)

    this.handled = tally(this.entries.map(({ identity }) => identity))
    this.expectedHandled = tally(this.expected)
    for (const { identity } of this.replay.extras) {
      this.expectedHandled.set(identity, this.expectedHandled.get(identity) + 1)
    }
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    this.broker = await this.awaitBrokerEnd(scope)
  }

  /**
   * Checks the facts of the six outages against the plan's limits: each outage short of the session,
   * each node taken down while the producer ran, no crash and no handler failure, at least one
   * authentic session reset from a lost connection or a moved leader, no membership change, and
   * the frozen held sets and charged replay entries within their allowances.
   * @returns {void}
   * @throws {Error} the first way the evidence falls short
   */
  checkBounces() {
    expect(this.outages).toHaveLength(2 * SERVICES.length)
    expect(this.snapshots).toHaveLength(this.outages.length)
    expect(this.crashes.map((error) => error.message)).toEqual([])
    expect(this.handlerErrors).toEqual([])
    expect(this.resets.some(({ reason }) => ['connection', 'leader-moved'].includes(reason))).toBe(
      true
    )
    this.bounceFacts = this.outages.map((outage, index) => {
      const frozen = this.snapshots[index]
      const until = outage.tRestored + MEMBERSHIP_QUIET_MS
      const inside = ({ at }) => at >= frozen.at && at <= until
      return {
        ...outage,
        index,
        frozenAt: frozen.at,
        fullIsr: true,
        joins: this.joins.filter(inside).length,
        assignmentChanges: this.changes.filter(inside).length,
      }
    })
    for (const fact of this.bounceFacts) {
      expect(fact.outageMs).toBeLessThan(MAX_OUTAGE_MS)
      expect(fact.frozenAt).toBeLessThanOrEqual(this.produceEnd)
      expect({ index: fact.index, joins: fact.joins }).toEqual({ index: fact.index, joins: 0 })
      expect({ index: fact.index, changes: fact.assignmentChanges }).toEqual({
        index: fact.index,
        changes: 0,
      })
    }
    expect(this.joins).toHaveLength(1)
    for (const { held } of this.snapshots) expect(held.size).toBeLessThanOrEqual(MAX_HELD)
    this.replay = auditReplay(this.entries, this.snapshots)
    expect(this.replay.problems).toEqual([])
  }

  /**
   * Waits until every partition's share start offset equals the log end with known lag zero, in
   * both the named and the all-topics listing.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object[]>} one terminal row per partition
   */
  async awaitBrokerEnd(scope) {
    const partitions = [...this.log.keys()]
    let rows = []
    const last = { named: null, all: null }
    await pollSharePosition(
      scope,
      this.window(30000, 40000),
      async (poll) => {
        last.named = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({
            groupId: this.group,
            topics: [{ topic: this.topic, partitions }],
            apiTimeout: 8000,
          })
        )
        last.all = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({ groupId: this.group, apiTimeout: 8000 })
        )
        const pick = (listing) =>
          listing.find(({ topic }) => topic === this.topic)?.partitions ?? []
        rows = partitions.map((partition) => {
          const forms = [pick(last.named), pick(last.all)].map((list) =>
            list.find((p) => p.partition === partition)
          )
          const [first] = forms
          return {
            groupId: this.group,
            topic: this.topic,
            partition,
            errorCode: first?.errorCode,
            startOffset: first?.startOffset,
            logEnd: this.log.get(partition).endOffset,
            lag: first?.lag,
            agree: forms.every((form) => JSON.stringify(form) === JSON.stringify(first)),
          }
        })
        return rows.every(
          (r) => r.errorCode === 0 && r.startOffset === r.logEnd && r.lag === '0' && r.agree
        )
      },
      {
        label: 'The broker share offsets',
        snapshot: () => ({ groupId: this.group, topic: this.topic, rows, ...last }),
        record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
      }
    )
    return rows.map(({ agree: _agree, ...row }) => row)
  }

  /**
   * Disconnects the member, then lets a fresh member of the same group listen for five seconds.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    const verified = await bounded(scope, 30000, (inner) =>
      verifierMember(this.group, this.topic, { kafka: this.client('verifier'), signal: inner })
    )
    expect(verified).toEqual([])
    expect(this.crashes.map((error) => error.message)).toEqual([])
  }

  /**
   * Waits until every broker answers again and no bounce step is left unrestored.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async restoreBrokers(signal) {
    await pollUntil(
      signal,
      30000,
      async () =>
        this.bouncer.assertAllUp().then(
          () => true,
          () => false
        ),
      { label: 'Every broker answering again' }
    )
    if (this.readiness) await bounded(signal, 10000, () => waitForFullIsr(this.admin, [this.topic]))
  }

  /**
   * Proves owned objects are gone and resources are back at baseline, then disconnects the admin.
   * It runs last in the cleanup order.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async cleanup(signal) {
    try {
      if (this.baseline) {
        await pollUntil(
          signal,
          3000,
          () =>
            this.tap.socketCounts().created - this.tap.socketCounts().destroyed <=
            this.baseline.sockets.open,
          { delayMs: 50, label: 'The sockets closing' }
        )
        this.after = await assertShareCleanup(
          this.admin,
          [this.group],
          [this.topic],
          this.baseline,
          this.probe,
          { signal }
        )
      }
    } finally {
      await this.admin.disconnect()
    }
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the topic is still listed after deletion
   */
  async deleteTopic(signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
    await pollUntil(
      signal,
      10000,
      async () => !(await this.admin.listTopics()).includes(this.topic),
      { label: 'The topic disappearing' }
    )
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the group is still listed after deletion
   */
  async deleteGroup(signal) {
    await pollUntil(
      signal,
      10000,
      async () => {
        try {
          await this.admin.deleteGroups([this.group])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      },
      { label: 'The group deletion' }
    )
    await pollUntil(
      signal,
      10000,
      async () =>
        !(await this.admin.listGroups()).groups.some(({ groupId }) => groupId === this.group),
      { label: 'The group disappearing' }
    )
  }

  /**
   * Removes the owned group config overrides and proves none remain.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses or an override remains
   */
  async resetConfigs(signal) {
    const names = Object.keys(this.groupConfig)
    const { resources } = await bounded(signal, 8000, () =>
      this.admin.incrementalAlterConfigs({
        resources: [
          {
            type: ConfigResourceTypes.GROUP,
            name: this.group,
            configEntries: names.map((name) => ({ name, operation: AlterConfigOpTypes.DELETE })),
          },
        ],
      })
    )
    if (resources[0].errorCode !== 0) {
      throw new Error(`Config reset refused: ${resources[0].errorCode}`)
    }
    const read = await bounded(signal, 8000, () =>
      this.admin.describeConfigs({
        resources: [{ type: ConfigResourceTypes.GROUP, name: this.group, configNames: names }],
      })
    )
    const left = read.resources[0].configEntries.filter(
      ({ configSource }) => configSource === ConfigSource.GROUP_CONFIG
    )
    if (left.length > 0) {
      throw new Error(`Group config overrides remain: ${left.map((e) => e.configName)}`)
    }
  }

  /**
   * @param {{ sockets: { open: number }, activeResources: Record<string, number>, listeners: Record<string, number>, queueDepth: number, ledgerSize: number }} snapshot a measurement
   * @returns {object} the evidence form of the measurement
   */
  resourceSummary(snapshot) {
    return {
      openSockets: snapshot.sockets.open,
      timers: snapshot.activeResources.Timeout ?? 0,
      listeners: Object.values(snapshot.listeners).reduce((sum, n) => sum + n, 0),
      queueDepth: snapshot.queueDepth,
      ledgerSize: snapshot.ledgerSize,
    }
  }

  /**
   * Checks the admin's own sockets are closed, then records the single completion row.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async complete(scope) {
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    const final = await bounded(scope, 10000, () => this.probe.snapshot())
    this.row('cleanup', { snapshot: final })
    const { extras, perBounce } = this.replay
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: [...this.expected].sort(), actual: [...this.rawIdentities].sort() },
        handler: {
          expected: sortedEntries(this.expectedHandled),
          actual: sortedEntries(this.handled),
        },
        broker: this.broker,
      },
      resources: {
        before: this.resourceSummary(this.baseline),
        after: this.resourceSummary(this.after),
      },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      readiness: { ...this.readiness, internal: this.shareState },
      produceStart: this.produceStart,
      produceEnd: this.produceEnd,
      crashes: [],
      resets: this.resets,
      bounces: this.bounceFacts.map((fact, index) => ({
        ...fact,
        held: [...this.snapshots[index].held].sort(),
        inFlight: this.snapshots[index].inFlight,
        duplicates: extras.filter(({ bounce }) => bounce === index),
        extraEntries: perBounce[index],
      })),
      shareFrames: this.tap.frames.length,
      wireVersions: wireVersionsOf(this.requests),
      config: { client: this.clientConfig, group: this.groupConfig },
    })
  }
}

describe('share bounce wire tap', () => {
  const frame = (apiKey, correlationId) => {
    const header = Buffer.alloc(11)
    header.writeInt16BE(apiKey, 0)
    header.writeInt16BE(2, 2)
    header.writeInt32BE(correlationId, 4)
    header.writeInt16BE(1, 8)
    header.write('t', 10)
    const body = Buffer.concat([header, Buffer.from([0])])
    const bytes = Buffer.alloc(4 + body.length)
    bytes.writeInt32BE(body.length, 0)
    body.copy(bytes, 4)
    return bytes
  }
  const response = (correlationId) => {
    const bytes = Buffer.alloc(8)
    bytes.writeInt32BE(4, 0)
    bytes.writeInt32BE(correlationId, 4)
    return bytes
  }
  const open = () => {
    const written = []
    const socket = Object.assign(new EventEmitter(), {
      destroyed: false,
      write: (data) => {
        written.push(Buffer.from(data))
        return true
      },
    })
    const tap = new ShareWireTap(() => socket)
    return { tap, socket, written, wrapped: tap.factory('A')({}) }
  }

  it('splits fragmented and coalesced writes into whole frames and forwards every byte', () => {
    const splitter = new FrameSplitter()
    const first = frame(78, 7)
    const second = frame(79, 8)
    const stream = Buffer.concat([first, second])
    const seen = [
      stream.subarray(0, 3),
      stream.subarray(3, first.length + 2),
      stream.subarray(first.length + 2),
    ].flatMap((chunk) => splitter.push(chunk))
    expect(seen).toEqual([first, second])
  })

  it('counts a request as pending until its answer, and only on an open socket', () => {
    const { tap, socket, written, wrapped } = open()
    const stream = Buffer.concat([frame(78, 7), frame(0, 8)])
    wrapped.write(stream.subarray(0, 5))
    wrapped.write(stream.subarray(5))
    expect(Buffer.concat(written)).toEqual(stream)
    expect(tap.pending()).toBe(2)
    expect(tap.inFlight().map(({ apiKey, correlationId }) => [apiKey, correlationId])).toEqual([
      [78, 7],
    ])
    expect(tap.frames.map(({ apiKey }) => apiKey)).toEqual([78])
    socket.emit('data', response(7))
    expect(tap.pending()).toBe(1)
    socket.destroyed = true
    expect(tap.pending()).toBe(0)
    expect(tap.socketCounts()).toEqual({ created: 1, destroyed: 1 })
  })
})

describe('share bounce replay audit', () => {
  const entry = (identity, deliveryCount, at) => ({ identity, deliveryCount, at })
  const bounce = (at, held) => ({ at, held: new Set(held) })

  it('charges a repeated entry to the earlier bounce that held the identity', () => {
    const { extras, perBounce, problems } = auditReplay(
      [entry('a', 1, 1), entry('b', 1, 2), entry('a', 2, 12)],
      [bounce(5, ['a']), bounce(10, ['b'])]
    )
    expect(problems).toEqual([])
    expect(perBounce).toEqual([1, 0])
    expect(extras).toEqual([{ identity: 'a', bounce: 0, deliveryCount: 2, at: 12 }])
  })

  it('reports an identity entered again that no bounce held, or that came before its bounce', () => {
    const unheld = auditReplay([entry('a', 1, 1), entry('a', 2, 12)], [bounce(5, ['b'])])
    expect(unheld.problems).toHaveLength(1)
    const early = auditReplay([entry('a', 1, 1), entry('a', 2, 3)], [bounce(5, ['a'])])
    expect(early.problems).toHaveLength(1)
  })

  it('reports a repeat without a higher delivery count and charges each bounce once per entry', () => {
    const flat = auditReplay([entry('a', 2, 1), entry('a', 2, 12)], [bounce(5, ['a'])])
    expect(flat.problems).toEqual(['a was entered again without a higher delivery count'])
    const twice = auditReplay(
      [entry('a', 1, 1), entry('a', 2, 12), entry('a', 3, 14)],
      [bounce(5, ['a'])]
    )
    expect(twice.problems).toHaveLength(1)
  })
})

enabled('share bounce on a live group', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  it(
    TITLE,
    liveCase(BUDGET_MS, (scope) => new ShareBounceCase().run(scope)),
    caseTimeout(BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
