const {
  acknowledgementResults,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  ShareAcknowledgementObserver,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  Kafka,
  KafkaJSDeleteGroupsError,
  KafkaJSNonRetriableError,
  logLevel,
} = require('../../../../index')
const Broker = require('../../../broker')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { requests, selectVersion } = require('../../../protocol/requests')
const CONFIG_RESOURCE_TYPES = require('../../../protocol/configResourceTypes')
const CONFIG_SOURCE = require('../../../protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../../../protocol/alterConfigOpTypes')
const defaultSocketFactory = require('../../../network/socketFactory')
const { ShareAckLedger } = require('../shareAckLedger')
const {
  createCluster,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { readLog, endOffsets } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const {
  assertShareCleanup,
  captureShareCleanupBaseline,
  createShareTopic,
  setShareGroupConfigs,
  shareRecordIdentity,
} = require('../../../../testHelpers/shareFixtures')
const { CleanupStack } = require('../../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')

// Gated: it needs the default compose profile (a share-enabled broker, base lock minimum). Run it
// alone through the guarded live entry; `KAFKA_VERSION` names the cluster under test.
const describeAdmin = process.env.SHARE_ADMIN_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-admin'
const PROFILE = 'tv2'
const TITLE = 'captures share administration and preserves an ordinary group'
const CASE_BUDGET_MS = 100000
const HOOK_BUDGET_MS = 10000
const CLEANUP_BUDGET_MS = 10000
const API_TIMEOUT_MS = 10000
const STEP_MS = 15000
const RECORDS = 20
const ALTERED_START = 8
const TIMESTAMP = '1700000000000'
const GROUP_ALIAS = 'share-group'
const GROUP_ID_NOT_FOUND = 69
const NON_EMPTY_GROUP = 68
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

/**
 * @param {unknown} value
 * @returns {unknown} a JSON-safe copy: buffers become `{ $buffer: hex }`, errors their serialized
 *   form, and `undefined` properties stay absent
 * @throws {Error} for a value with no faithful JSON form (bigint, function, symbol)
 */
const serialize = (value) => {
  if (value === null || ['string', 'number', 'boolean'].includes(typeof value)) return value
  if (Buffer.isBuffer(value)) return { $buffer: value.toString('hex') }
  if (value instanceof Error) return serializeError(value)
  if (Array.isArray(value)) return value.map((item) => serialize(item) ?? null)
  if (typeof value === 'object') {
    const copy = {}
    for (const [key, item] of Object.entries(value)) {
      const serialized = serialize(item)
      if (serialized !== undefined) copy[key] = serialized
    }
    return copy
  }
  if (value === undefined) return undefined
  throw new Error(`A ${typeof value} has no capture form`)
}

/**
 * @param {Error} error
 * @returns {object} its name, message and every own property (code, type, retriable, cause, typed
 *   item errors), the stack left out; fields the error does not have stay absent
 */
const serializeError = (error) => {
  const copy = { name: error.name, message: error.message }
  for (const key of Object.getOwnPropertyNames(error)) {
    if (['stack', 'name', 'message'].includes(key)) continue
    const serialized = serialize(error[key])
    if (serialized !== undefined) copy[key] = serialized
  }
  return copy
}

/**
 * Runs `work` once and keeps what it returned or threw, in the capture's tagged form.
 * @param {() => Promise<unknown>} work
 * @returns {Promise<{ kind: 'return', value: unknown } | { kind: 'throw', error: object }>}
 */
const outcomeOf = async (work) => {
  try {
    return { kind: 'return', value: serialize(await work()) }
  } catch (error) {
    return { kind: 'throw', error: serializeError(error) }
  }
}

/**
 * A reversible rename of the identities one capture mentions (nonce group, topic, client, member
 * and topic ids), applied to string values only, so a code, an offset or any number stays as the
 * broker sent it.
 */
class Aliases {
  /** Starts with no rename. */
  constructor() {
    /** @type {Map<string, string>} real value to alias */
    this.map = new Map()
  }

  /**
   * @param {string} real the identity as the run used it
   * @param {string} alias the stable name the fixture shows
   * @returns {void}
   */
  add(real, alias) {
    if (real) this.map.set(real, alias)
  }

  /**
   * @param {unknown} value a serialized capture value
   * @returns {unknown} a copy with every alias applied inside its strings, longest identity first
   */
  apply(value) {
    const renames = [...this.map].sort(([a], [b]) => b.length - a.length)
    const rewrite = (item) => {
      if (typeof item === 'string') {
        return renames.reduce((text, [real, alias]) => text.split(real).join(alias), item)
      }
      if (Array.isArray(item)) return item.map(rewrite)
      if (item !== null && typeof item === 'object') {
        return Object.fromEntries(Object.entries(item).map(([key, inner]) => [key, rewrite(inner)]))
      }
      return item
    }
    return rewrite(value)
  }

  /** @returns {Record<string, string>} the rename as an object, for the raw evidence */
  toJSON() {
    return Object.fromEntries(this.map)
  }
}

/**
 * Captures what the public `admin.describeGroups` does with a share group id: every call it makes
 * to the coordinator's ConsumerGroupDescribe and DescribeGroups (version, request, response or
 * error) and its public result or throw, plus one raw DescribeGroups on the same id at its
 * negotiated version. The original methods always run; their errors are rethrown unchanged.
 */
class LegacyDescriptionCapture {
  /**
   * @param {AbortSignal} scope the case scope; nothing is installed once it aborted
   */
  constructor(scope) {
    this.scope = scope
  }

  /**
   * @param {object} admin the public admin client
   * @param {{ describeGroups: Function, versions: object }} brokerTarget the group's coordinator
   * @param {{ groupId: string, stage: 'member-present'|'empty' }} subject
   * @returns {Promise<{ stage: string, trace: object[], rawDescribeGroups: object, publicOutcome: object }>}
   */
  async capture(admin, brokerTarget, { groupId, stage }) {
    this.scope.throwIfAborted()
    const trace = []
    const restore = this.install(trace)
    let publicOutcome
    try {
      publicOutcome = await outcomeOf(() => admin.describeGroups([groupId]))
    } finally {
      restore()
    }
    const rawDescribeGroups = await this.raw(brokerTarget, groupId)

    return { stage, trace, rawDescribeGroups, publicOutcome }
  }

  /**
   * One raw DescribeGroups on the coordinator, labelled apart from the public trace.
   * @param {{ describeGroups: Function, versions: object }} broker
   * @param {string} groupId
   * @returns {Promise<object>} `{ method, apiKey, apiVersion, request, outcome }`
   */
  async raw(broker, groupId) {
    const request = { groupIds: [groupId], includeAuthorizedOperations: false }
    const apiVersion = selectVersion(
      broker.versions,
      apiKeys.DescribeGroups,
      requests.DescribeGroups
    )
    const outcome = await outcomeOf(() => broker.describeGroups(request))

    return {
      method: 'describeGroups',
      apiKey: apiKeys.DescribeGroups,
      apiVersion,
      request: serialize(request),
      outcome,
    }
  }

  /**
   * Replaces the two Broker methods with call-through wrappers that append to `trace`.
   * @param {object[]} trace filled in call order
   * @returns {() => void} puts the original methods back
   */
  install(trace) {
    const wrapped = [
      ['consumerGroupDescribe', apiKeys.ConsumerGroupDescribe, requests.ConsumerGroupDescribe],
      ['describeGroups', apiKeys.DescribeGroups, requests.DescribeGroups],
    ].map(([method, apiKey, definition]) => {
      const original = Broker.prototype[method]
      Broker.prototype[method] = async function traced(request) {
        let apiVersion
        try {
          apiVersion = selectVersion(this.versions, apiKey, definition)
        } catch {
          apiVersion = undefined
        }
        const step = {
          method,
          apiKey,
          ...(apiVersion !== undefined && { apiVersion }),
          request: serialize({
            groupIds: request.groupIds,
            includeAuthorizedOperations: request.includeAuthorizedOperations,
          }),
        }
        trace.push(step)
        let value
        try {
          value = await original.call(this, request)
        } catch (error) {
          step.outcome = { kind: 'throw', error: serializeError(error) }
          throw error
        }
        step.outcome = { kind: 'return', value: serialize(value) }
        return value
      }
      return () => {
        Broker.prototype[method] = original
      }
    })

    return () => wrapped.forEach((restore) => restore())
  }
}

/**
 * Counts the sockets a client opens and closes, for the resource probe.
 */
class SocketCounter {
  /** Starts at zero. */
  constructor() {
    this.created = 0
    this.destroyed = 0
  }

  /** @returns {Function} a socket factory that counts through the default one */
  factory() {
    const inner = defaultSocketFactory()
    return (options) => {
      const socket = inner(options)
      this.created += 1
      socket.once('close', () => {
        this.destroyed += 1
      })
      return socket
    }
  }
}

/**
 * Counts what the case's clients register on their public `on`, for the resource probe.
 */
class ListenerCounter {
  /** Starts empty. */
  constructor() {
    this.counts = new Map()
  }

  /** @param {string} event @param {number} step 1 for a new listener, -1 for a removed one @returns {void} */
  add(event, step = 1) {
    this.counts.set(event, (this.counts.get(event) ?? 0) + step)
  }

  /** @returns {string[]} the events that still have a listener */
  eventNames() {
    return [...this.counts].filter(([, count]) => count > 0).map(([event]) => event)
  }

  /** @param {string} event @returns {number} */
  listenerCount(event) {
    return this.counts.get(event) ?? 0
  }
}

/**
 * Watches the acknowledgement ledgers the case's share clients create, by recording each one when
 * one of its methods first runs, and reports how many entries they still hold.
 */
class LedgerWatch {
  /** Starts with nothing watched. */
  constructor() {
    this.ledgers = new Set()
    this.restores = []
  }

  /** @returns {void} replaces the ledger methods with call-through wrappers */
  install() {
    for (const name of [
      'canStart',
      'decide',
      'complete',
      'completeRange',
      'prepare',
      'settle',
      'invalidate',
    ]) {
      const original = ShareAckLedger.prototype[name]
      const watch = this
      ShareAckLedger.prototype[name] = function watched(...args) {
        watch.ledgers.add(this)
        return original.apply(this, args)
      }
      this.restores.push(() => {
        ShareAckLedger.prototype[name] = original
      })
    }
  }

  /** @returns {void} puts the original methods back */
  restore() {
    for (const restore of this.restores.splice(0)) restore()
  }

  /** @returns {number} entries held by every watched ledger */
  size() {
    let total = 0
    for (const ledger of this.ledgers) total += ledger.size
    return total
  }
}

/**
 * @param {number} index
 * @returns {{ key: Buffer, value: Buffer }} the indexed record of the fixed `admin` corpus
 */
const corpusRecord = (index) => ({
  key: Buffer.from(`admin:${String(index).padStart(4, '0')}`),
  value: Buffer.from(`admin:value:${index}`),
})

/**
 * One consumer of the case, share or ordinary, with the facts the case reads from its events.
 */
class Member {
  /**
   * @param {AdminCase} fixture the owning case
   * @param {string} label e.g. `share`
   * @param {object} consumer the client
   */
  constructor(fixture, label, consumer) {
    this.fixture = fixture
    this.label = label
    this.consumer = consumer
    this.memberId = null
    this.assignment = []
    this.delivered = []
    this.handled = 0
    this.acks = []
    this.crashes = []
    this.requests = []
    this.queueSizes = new Map()
    this.joined = false
    const { events } = consumer
    this.listen(events.GROUP_JOIN, ({ payload }) => {
      this.memberId = payload.memberId
      this.joined = true
    })
    if (events.ASSIGNMENT_CHANGED) {
      this.listen(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        this.assignment = payload.assignment
      })
    }
    if (events.ACKNOWLEDGE) {
      this.listen(events.ACKNOWLEDGE, ({ payload }) => this.acks.push(payload))
    }
    this.listen(events.CRASH, ({ payload }) => this.crashes.push(payload))
    this.listen(events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.listen(events.REQUEST_QUEUE_SIZE, ({ payload }) =>
      this.queueSizes.set(payload.broker, payload.queueSize)
    )
    fixture.cleanup.add(`disconnect ${label}`, () => consumer.disconnect({ timeout: 5000 }))
    fixture.members.push(this)
  }

  /**
   * @param {string} event
   * @param {(event: object) => void} listener
   * @returns {void} registers it and counts it for the resource probe
   */
  listen(event, listener) {
    const remove = this.consumer.on(event, listener)
    this.fixture.listeners.add(event)
    this.fixture.cleanup.add(`remove ${this.label} ${event} listener`, () => {
      remove()
      this.fixture.listeners.add(event, -1)
    })
  }

  /** @returns {number} requests queued or in flight, by the latest size each broker reported */
  queueDepth() {
    return [...this.queueSizes.values()].reduce((total, size) => total + size, 0)
  }

  /**
   * The share-partition assignment as `topic -> partitions`.
   * @returns {Array<{ topic: string, partitions: number[] }>}
   */
  assigned() {
    return this.assignment.map(({ topic, partitions }) => ({ topic, partitions: [...partitions] }))
  }
}

/**
 * The single administration case: a share group is altered, listed, described, refused and
 * deleted through the public admin client, the legacy `describeGroups` on a share id is
 * captured, and an ordinary consumer-protocol group sits beside it, intact, throughout.
 */
class AdminCase {
  /** Builds the identities and the resource probe; nothing is created yet. */
  constructor() {
    this.id = secureRandom()
    this.cleanup = new CleanupStack()
    this.members = []
    this.listeners = new ListenerCounter()
    this.ledgers = new LedgerWatch()
    this.sockets = new SocketCounter()
    this.groups = {
      share: `e2e-p9e-admin-share-${this.id}`,
      legacy: `e2e-p9e-admin-legacy-${this.id}`,
      ordinary: `e2e-p9e-admin-ordinary-${this.id}`,
    }
    this.topics = { share: null, ordinary: null }
    this.configured = []
    this.expected = []
    this.adminRequests = []
    this.oracles = {}
    this.probe = createResourceProbe({
      emitters: { listeners: this.listeners },
      sockets: () => ({ created: this.sockets.created, destroyed: this.sockets.destroyed }),
      queueDepth: () => this.members.reduce((total, member) => total + member.queueDepth(), 0),
      ledgerSize: () => this.ledgers.size(),
      activeResources: true,
    })
  }

  /**
   * Appends one evidence row carrying the case's identity.
   * @param {string} event the row kind
   * @param {object} [fields]
   * @returns {void}
   */
  note(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: TITLE,
      profile: PROFILE,
      nonce: this.id,
      groups: { share: GROUP_ALIAS, legacy: 'legacy-group', ordinary: 'ordinary-group' },
      budgets: { caseMs: CASE_BUDGET_MS, apiTimeoutMs: API_TIMEOUT_MS },
      wireVersions: wireVersionsOf([
        ...this.adminRequests,
        ...this.members.flatMap(({ requests }) => requests),
      ]),
      ...fields,
    })
  }

  /** @returns {object[]} Share handler progress and actual acknowledgement outcomes. */
  progress() {
    return this.members
      .filter(({ consumer }) => consumer.events.ACKNOWLEDGE)
      .map(({ label, memberId, delivered, handled, acks }) => ({
        label,
        memberId,
        delivered: delivered.length,
        handled,
        acknowledgements: acknowledgementResults(acks),
      }))
  }

  /**
   * Runs the stages between setup and strict cleanup and writes the case's row after both,
   * whichever way it ended.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the failure of a stage, or of the cleanup
   */
  async run(scope) {
    this.acknowledgements = new ShareAcknowledgementObserver()
    this.acknowledgements.install()
    try {
      await this.cleanup.run(
        async () => {
          await this.setup(scope)
          await this.alterAndList(scope)
          await this.describeAndList(scope)
          await this.refusals(scope)
          await this.emptyAndDelete(scope)
          await this.legacy(scope)
          await this.collision(scope)
          await this.ordinaryIntact(scope)
        },
        { budgetMs: CLEANUP_BUDGET_MS }
      )
    } catch (error) {
      this.note('case-failed', {
        error: String(error?.stack ?? error).slice(0, 4000),
        members: this.progress(),
      })
      throw error
    } finally {
      this.ledgers.restore()
      this.acknowledgements.restore()
    }
    this.note('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: this.oracles,
      resources: this.resources,
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      wireVersions: wireVersionsOf([
        ...this.adminRequests,
        ...this.members.flatMap(({ requests: seen }) => seen),
      ]),
      config: { groups: this.groups, apiTimeoutMs: API_TIMEOUT_MS },
    })
  }

  /**
   * Registers every cleanup before anything is created, then creates the share topic with its
   * twenty records, the ordinary topic with its group and one committed offset, and takes the
   * resource baseline before any test client connects.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    liveRun.assertClean()
    this.baseline = await captureShareCleanupBaseline(this.probe, { signal: scope })
    const common = { brokers: plainTextBrokers(), logLevel: logLevel.NOTHING, retry: { factor: 0 } }
    this.adminKafka = new Kafka({ ...common, clientId: `e2e-p9e-admin-${this.id}` })
    this.kafka = new Kafka({
      ...common,
      clientId: `e2e-p9e-admin-clients-${this.id}`,
      socketFactory: this.sockets.factory(),
    })
    this.admin = this.adminKafka.admin()
    this.cluster = createCluster()
    this.cleanup.add('disconnect admin', () => this.admin.disconnect())
    this.cleanup.add('prove cleanup', (signal) => this.proveCleanup(signal))
    this.cleanup.add('delete topics', (signal) => this.removeTopics(signal))
    this.cleanup.add('delete groups', (signal) => this.removeGroups(signal))
    this.cleanup.add('restore ledgers', () => this.ledgers.restore())
    this.cleanup.add('disconnect raw cluster', () => this.cluster.disconnect())
    await bounded(scope, 10000, () => this.admin.connect())
    const removeRequests = this.admin.on(this.admin.events.REQUEST, ({ payload }) =>
      this.adminRequests.push(payload)
    )
    this.cleanup.add('remove admin request listener', () => removeRequests())

    this.topics.share = await bounded(scope, 20000, () => createShareTopic({ scenario: 'admin' }))
    this.topics.ordinary = await bounded(scope, 20000, () =>
      createShareTopic({ scenario: 'admin-ordinary' })
    )
    await this.keepOldTimestamps(scope, this.topics.share)
    await this.keepOldTimestamps(scope, this.topics.ordinary)
    await this.configure(scope, this.groups.share)
    await this.configure(scope, this.groups.legacy)
    await this.produceCorpus(scope)
    await bounded(scope, 10000, () => this.admin.listGroups())
    this.ledgers.install()
    this.note('resource-baseline', { snapshot: this.baseline })
    await this.startOrdinary(scope)
  }

  /**
   * @param {AbortSignal} scope
   * @param {string} topic
   * @returns {Promise<void>} sets `retention.ms=-1` so the fixed old timestamps never expire
   */
  async keepOldTimestamps(scope, topic) {
    await bounded(scope, 10000, () =>
      this.admin.incrementalAlterConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: topic,
            configEntries: [
              { name: 'retention.ms', operation: ALTER_CONFIG_OP_TYPES.SET, value: '-1' },
            ],
          },
        ],
      })
    )
  }

  /**
   * Sets `share.auto.offset.reset=earliest` on a share group and reads it back before any join.
   * @param {AbortSignal} scope
   * @param {string} group
   * @returns {Promise<void>}
   */
  async configure(scope, group) {
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, group, { 'share.auto.offset.reset': 'earliest' })
    )
    this.configured.push(group)
    this.note('config-readback', { group, configs: { 'share.auto.offset.reset': 'earliest' } })
  }

  /**
   * Appends the twenty-record corpus to partition 0 with an idempotent producer and checks it
   * against the independently read log.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async produceCorpus(scope) {
    const [base] = await this.send(scope, this.topics.share, RECORDS, 0)
    this.expected = Array.from({ length: RECORDS }, (_, index) =>
      shareRecordIdentity({
        topic: this.topics.share,
        partition: 0,
        offset: String(BigInt(base) + BigInt(index)),
        ...corpusRecord(index),
      })
    )
    const log = await bounded(scope, 15000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topics.share })
    )
    const actual = log
      .get(0)
      .records.filter(({ isControlRecord }) => !isControlRecord)
      .map(({ offset, key, value }) =>
        shareRecordIdentity({
          topic: this.topics.share,
          partition: 0,
          offset: String(offset),
          key,
          value,
        })
      )
    expect(actual).toEqual(this.expected)
    expect(base).toBe('0')
    expect(log.get(0).endOffset).toBe(String(RECORDS))
    expect(log.get(0).records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    const metadata = await bounded(scope, Infinity, () =>
      this.admin.fetchTopicMetadata({ topics: [this.topics.share] })
    )
    this.topicId = metadata.topics.find(({ name }) => name === this.topics.share).topicId
    this.oracles.log = { expected: this.expected, actual }
    this.note('produced', { baseOffset: base, count: RECORDS })
  }

  /**
   * Sends records of the indexed corpus to one partition with an idempotent producer.
   * @param {AbortSignal} scope
   * @param {string} topic
   * @param {number} count how many records
   * @param {number} first index of the first record
   * @returns {Promise<[string]>} the base offset, as a decimal string
   */
  async send(scope, topic, count, first) {
    const producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
    try {
      await bounded(scope, 10000, () => producer.connect())
      const results = await bounded(scope, 10000, () =>
        producer.send({
          topic,
          acks: -1,
          messages: Array.from({ length: count }, (_, index) => ({
            ...corpusRecord(first + index),
            partition: 0,
            timestamp: TIMESTAMP,
          })),
        })
      )
      return [String(results[0].baseOffset)]
    } finally {
      await bounded(NEVER, 5000, () => producer.disconnect())
    }
  }

  /**
   * Starts the ordinary consumer-protocol member on its own topic with one committed offset.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async startOrdinary(scope) {
    await this.send(scope, this.topics.ordinary, 1, 0)
    const consumer = this.kafka.consumer({
      groupId: this.groups.ordinary,
      groupProtocol: 'consumer',
      maxWaitTimeInMs: 250,
    })
    this.ordinary = new Member(this, 'ordinary', consumer)
    await bounded(scope, 10000, () => consumer.connect())
    await bounded(scope, 10000, () =>
      consumer.subscribe({ topics: [this.topics.ordinary], fromBeginning: true })
    )
    await bounded(scope, 10000, () =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          this.ordinary.delivered.push(message.value.toString())
          await consumer.commitOffsets([
            { topic, partition, offset: String(BigInt(message.offset) + 1n) },
          ])
        },
      })
    )
    await pollUntil(scope, STEP_MS, () => this.ordinary.delivered.length === 1, {
      delayMs: 100,
      label: 'The ordinary delivery',
    })
    await pollUntil(scope, STEP_MS, async () => (await this.ordinaryOffset(scope)) === '1', {
      delayMs: 100,
      label: 'The ordinary committed offset',
    })
    this.ordinaryBefore = await this.ordinaryMembers(scope)
    expect(this.ordinaryBefore).toHaveLength(1)
  }

  /**
   * @param {AbortSignal} scope
   * @returns {Promise<Array<{ memberId: string, assignment: object[] }>>} the ordinary group's
   *   members as the public describe reports them
   */
  async ordinaryMembers(scope) {
    const { groups } = await bounded(scope, STEP_MS, () =>
      this.admin.describeGroups([this.groups.ordinary])
    )
    expect(groups[0].groupType).toBe('consumer')
    return groups[0].members.map(({ memberId, assignment }) => ({ memberId, assignment }))
  }

  /**
   * @param {AbortSignal} scope
   * @returns {Promise<string>} the ordinary group's committed offset on its topic, decimal
   */
  async ordinaryOffset(scope) {
    const [{ partitions }] = await bounded(scope, 10000, () =>
      this.admin.fetchOffsets({ groupId: this.groups.ordinary, topics: [this.topics.ordinary] })
    )
    return String(partitions[0].offset)
  }

  /**
   * Starts a share member on the shared topic and waits for its first accepted assignment.
   * @param {AbortSignal} scope
   * @param {string} label
   * @param {string} group
   * @param {string} [topic] defaults to the share topic
   * @returns {Promise<Member>} its handler records each delivered identity and delivery count
   */
  async startShare(scope, label, group, topic = this.topics.share) {
    const consumer = this.kafka.shareConsumer({
      groupId: group,
      maxRecords: 100,
      maxWaitTimeInMs: 250,
      acknowledgementMode: 'implicit',
    })
    const member = new Member(this, label, consumer)
    await bounded(scope, 10000, () => consumer.connect())
    await bounded(scope, 10000, () => consumer.subscribe({ topics: [topic] }))
    await bounded(scope, 10000, () =>
      consumer.run({
        eachMessage: async ({ topic: name, partition, message }) => {
          member.delivered.push({
            identity: shareRecordIdentity({ ...message, topic: name, partition }),
            deliveryCount: message.deliveryCount,
          })
          member.handled++
        },
      })
    )
    await pollUntil(scope, STEP_MS, () => member.assignment.length > 0, {
      delayMs: 50,
      label: `The ${label} assignment`,
    })
    return member
  }

  /**
   * Alters a never-used empty group to start at offset 8, lists it in both forms, then consumes
   * the twelve remaining records exactly once and requires terminal broker settlement.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async alterAndList(scope) {
    const group = this.groups.share
    const topic = this.topics.share
    const altered = await bounded(scope, STEP_MS, () =>
      this.admin.alterShareGroupOffsets({
        groupId: group,
        topics: [{ topic, partitions: [{ partition: 0, offset: String(ALTERED_START) }] }],
        apiTimeout: API_TIMEOUT_MS,
      })
    )
    expect(altered).toHaveLength(1)
    expect(altered[0].partitions.map(({ errorCode }) => errorCode)).toEqual([0])
    this.note('acknowledgement', {
      stage: 'alter-empty',
      group: GROUP_ALIAS,
      altered: serialize(altered),
    })

    const all = await this.listOffsets(scope, group)
    const named = await this.listOffsets(scope, group, [{ topic, partitions: [0] }])
    expect(all.map(({ topic: name }) => name)).toContain(topic)
    expect(all.find(({ topic: name }) => name === topic).partitions[0].startOffset).toBe(
      String(ALTERED_START)
    )
    expect(named[0].partitions[0].startOffset).toBe(String(ALTERED_START))
    const lags = [all, named].map(
      (rows) => rows.find(({ topic: name }) => name === topic).partitions[0].lag
    )
    this.note('acknowledgement', {
      stage: 'list-before-consume',
      allTopicsLag: lags[0],
      namedLag: lags[1],
    })

    let lag
    await pollUntil(
      scope,
      STEP_MS,
      async () => {
        const named = await this.listOffsets(scope, group, [{ topic, partitions: [0] }])
        const all = await this.listOffsets(scope, group)
        const pick = (listing) => listing.filter((row) => row.topic === topic)
        const forms = [named, all].map(pick)
        if (
          forms.some(
            (rows) =>
              rows.length !== 1 ||
              rows[0].topicId !== this.topicId ||
              rows[0].partitions.length !== 1
          )
        )
          return false
        const [first, second] = forms.map(([row]) => row.partitions[0])
        lag = first.lag
        this.note('broker-position', {
          stage: 'reset',
          groupId: group,
          topic,
          named,
          all,
          logEnd: String(RECORDS),
        })
        return (
          first.partition === 0 &&
          first.errorCode === 0 &&
          first.startOffset === String(ALTERED_START) &&
          lag === String(RECORDS - ALTERED_START) &&
          second.partition === first.partition &&
          second.errorCode === first.errorCode &&
          second.startOffset === first.startOffset &&
          second.lag === first.lag &&
          second.leaderEpoch === first.leaderEpoch
        )
      },
      { delayMs: 200, label: 'The known lag of 12' }
    )
    this.note('acknowledgement', { stage: 'lag-known', lag })

    const member = await this.startShare(scope, 'share', group)
    this.share = member
    await pollUntil(scope, STEP_MS * 2, () => member.delivered.length >= RECORDS - ALTERED_START, {
      delayMs: 100,
      label: 'The twelve deliveries',
    })
    const expected = this.expected.slice(ALTERED_START)
    expect(member.delivered.map(({ identity }) => identity)).toEqual(expected)
    expect(member.delivered.every(({ deliveryCount }) => deliveryCount === 1)).toBe(true)
    this.oracles.handler = { expected, actual: member.delivered.map(({ identity }) => identity) }

    await this.reachesEnd(scope, group)
  }

  /** @returns {boolean} Exactly the twelve fixed suffix identities have successful original Accepts. */
  successfulAccepts() {
    const types = this.acknowledgements.types(this.groups.share, this.topicId, 0)
    return (
      types.size === RECORDS - ALTERED_START &&
      this.expected
        .slice(ALTERED_START)
        .every((identity) => JSON.stringify(types.get(JSON.parse(identity)[2])) === '[1]')
    )
  }

  /**
   * Requires successful suffix Accepts, then agreeing terminal listings within the existing poll bound.
   * @param {AbortSignal} scope Case scope.
   * @param {string} group Workload group.
   * @returns {Promise<void>} Records actual broker and acknowledgement provenance before deletion.
   */
  async reachesEnd(scope, group) {
    await bounded(scope, STEP_MS, async (inner) => {
      await pollUntil(inner, Infinity, () => this.successfulAccepts(), {
        label: 'The twelve broker Accepts',
      })
      expect(this.share.acks.filter(({ error }) => error !== null)).toEqual([])
      const position = await awaitTerminalSharePosition(
        this.admin,
        {
          groupId: group,
          topic: this.topics.share,
          partitions: [{ partition: 0, floor: String(ALTERED_START), end: String(RECORDS) }],
        },
        {
          signal: inner,
          timeoutMs: STEP_MS,
          record: (fields) =>
            this.note('broker-position', {
              ...fields,
              members: this.progress(),
              acknowledgements: this.acknowledgements.evidence(group),
            }),
        }
      )
      const [logEnd] = [
        ...(
          await bounded(inner, 10000, () =>
            endOffsets({
              brokers: plainTextBrokers(),
              topic: this.topics.share,
            })
          )
        ).values(),
      ]
      expect(logEnd).toBe(String(RECORDS))
      this.oracles.broker = position.rows
      this.oracles.acknowledgements = this.acknowledgements.evidence(group)
    })
  }

  /**
   * @param {AbortSignal} scope
   * @param {string} group
   * @param {Array<{ topic: string, partitions: number[] }>} [topics] omitted: every topic
   * @returns {Promise<object[]>} the public `listShareGroupOffsets` result
   */
  listOffsets(scope, group, topics) {
    return bounded(scope, STEP_MS, () =>
      this.admin.listShareGroupOffsets({
        groupId: group,
        ...(topics && { topics }),
        apiTimeout: API_TIMEOUT_MS,
      })
    )
  }

  /**
   * With the member present: the dedicated describe matches its events, and the group listings
   * show the share group by type beside the ordinary one.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async describeAndList(scope) {
    const group = this.groups.share
    const member = this.share
    const { groups } = await bounded(scope, STEP_MS, () =>
      this.admin.describeShareGroups([group], { apiTimeout: API_TIMEOUT_MS })
    )
    const [described] = groups
    expect(described.groupId).toBe(group)
    expect(described.errorCode).toBe(0)
    expect(described.state).toBe('Stable')
    expect(Number.isInteger(described.groupEpoch) && described.groupEpoch >= 1).toBe(true)
    expect(Number.isInteger(described.assignmentEpoch) && described.assignmentEpoch >= 1).toBe(true)
    expect(described.members).toHaveLength(1)
    const [row] = described.members
    expect(row.memberId).toBe(member.memberId)
    expect(row.clientId).toBe(`e2e-p9e-admin-clients-${this.id}`)
    expect(row.subscribedTopics).toEqual([this.topics.share])
    expect(row.assignment.map(({ topic, partitions }) => ({ topic, partitions }))).toEqual(
      member.assigned()
    )
    expect(row.assignment.every(({ topicId }) => UUID.test(topicId))).toBe(true)
    expect(described.authorizedOperations).toBeNull()
    const masked = await bounded(scope, STEP_MS, () =>
      this.admin.describeShareGroups([group], {
        includeAuthorizedOperations: true,
        apiTimeout: API_TIMEOUT_MS,
      })
    )
    const mask = masked.groups[0].authorizedOperations
    expect(mask === null || Number.isInteger(mask)).toBe(true)
    this.note('assignment', {
      stage: 'describe-with-member',
      described: serialize(described),
      authorizedOperationsMask: mask,
    })

    const listed = async (options) =>
      (await bounded(scope, STEP_MS, () => this.admin.listGroups(options))).groups
    const everything = await listed()
    expect(everything.find(({ groupId }) => groupId === group)?.groupType).toBe('share')
    const shares = await listed({ types: ['share'] })
    expect(shares.map(({ groupId }) => groupId)).toContain(group)
    const consumers = await listed({ types: ['consumer'] })
    expect(consumers.map(({ groupId }) => groupId)).not.toContain(group)
    expect(consumers.map(({ groupId }) => groupId)).toContain(this.groups.ordinary)
  }

  /**
   * With the workload member present: records the actual refusal of an offset alter and
   * requires NON_EMPTY_GROUP (68) for offset deletion and group deletion.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async refusals(scope) {
    const group = this.groups.share
    const topic = this.topics.share
    const alterRefusal = await outcomeOf(() =>
      this.admin.alterShareGroupOffsets({
        groupId: group,
        topics: [{ topic, partitions: [{ partition: 0, offset: String(RECORDS) }] }],
        apiTimeout: API_TIMEOUT_MS,
      })
    )
    this.note('fault', { stage: 'alter-with-member', group: GROUP_ALIAS, outcome: alterRefusal })

    const subscribed = await outcomeOf(() =>
      bounded(scope, STEP_MS, () =>
        this.admin.deleteShareGroupOffsets({
          groupId: group,
          topics: [topic],
          apiTimeout: API_TIMEOUT_MS,
        })
      )
    )
    expect(subscribed).toMatchObject({
      kind: 'throw',
      error: { name: 'KafkaJSProtocolError', type: 'NON_EMPTY_GROUP', code: NON_EMPTY_GROUP },
    })
    this.note('fault', {
      stage: 'delete-offsets-subscribed',
      memberId: this.share.memberId,
      outcome: subscribed,
    })

    let refusal
    try {
      await bounded(scope, STEP_MS, () => this.admin.deleteGroups([group]))
    } catch (error) {
      refusal = error
    }
    expect(refusal).toBeInstanceOf(KafkaJSDeleteGroupsError)
    expect(refusal.groups.map(({ groupId, errorCode }) => ({ groupId, errorCode }))).toEqual([
      { groupId: group, errorCode: NON_EMPTY_GROUP },
    ])
    const version = wireVersionsOf(this.adminRequests).DeleteGroups
    if (version >= 3) expect('errorMessage' in refusal.groups[0]).toBe(true)
    this.note('fault', {
      stage: 'delete-groups-with-member',
      deleteGroupsVersion: version,
      errorMessage: refusal.groups[0].errorMessage,
      error: serializeError(refusal),
    })

    const { groups } = await bounded(scope, STEP_MS, () => this.admin.listGroups())
    expect(groups.map(({ groupId }) => groupId)).toContain(group)
    expect(this.share.crashes).toEqual([])
    expect(this.share.assigned()).toEqual([{ topic, partitions: [0] }])
  }

  /**
   * Stops the member, waits until the group describes as empty, deletes its offsets for the
   * topic and then the empty group itself, each with its own proof.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async emptyAndDelete(scope) {
    const group = this.groups.share
    const topic = this.topics.share
    await bounded(scope, 10000, () => this.share.consumer.disconnect({ timeout: 5000 }))
    await this.untilEmpty(scope, group)
    const quiet = await assertQuietTerminalGroup({
      kafka: this.kafka,
      admin: this.admin,
      groupId: group,
      topic,
      partitions: [{ partition: 0, floor: String(ALTERED_START), end: String(RECORDS) }],
      signal: scope,
      timeoutMs: STEP_MS,
      record: (fields) => this.note('quiet-verifier', fields),
    })
    expect(quiet.delivered).toEqual([])
    expect(this.successfulAccepts()).toBe(true)
    expect(this.share.delivered.map(({ identity }) => identity)).toEqual(
      this.expected.slice(ALTERED_START)
    )
    this.oracles.broker = quiet.position.rows
    this.oracles.quiet = quiet
    this.oracles.acknowledgements = this.acknowledgements.evidence(group)
    await this.untilEmpty(scope, group)
    const removed = await bounded(scope, STEP_MS, () =>
      this.admin.deleteShareGroupOffsets({
        groupId: group,
        topics: [topic],
        apiTimeout: API_TIMEOUT_MS,
      })
    )
    expect(removed.map(({ topic: name, errorCode }) => ({ topic: name, errorCode }))).toEqual([
      { topic, errorCode: 0 },
    ])
    const all = await this.listOffsets(scope, group)
    expect(all.map(({ topic: name }) => name)).not.toContain(topic)
    const named = await this.listOffsets(scope, group, [{ topic, partitions: [0] }])
    const claimsOld = named.some(({ partitions }) =>
      partitions.some(
        ({ errorCode, startOffset }) => errorCode === 0 && startOffset === String(RECORDS)
      )
    )
    expect(claimsOld).toBe(false)
    this.note('acknowledgement', {
      stage: 'delete-offsets-empty',
      results: serialize(removed),
      namedAfterDelete: serialize(named),
    })

    const deleted = await bounded(scope, STEP_MS, () => this.admin.deleteGroups([group]))
    expect(deleted.map(({ groupId, errorCode }) => ({ groupId, errorCode }))).toEqual([
      { groupId: group, errorCode: 0 },
    ])
    await pollUntil(
      scope,
      STEP_MS,
      async () => !(await this.admin.listGroups()).groups.some(({ groupId }) => groupId === group),
      { delayMs: 200, label: 'The group deletion' }
    )
    this.note('acknowledgement', { stage: 'delete-empty-group', results: serialize(deleted) })
  }

  /**
   * @param {AbortSignal} scope
   * @param {string} group
   * @returns {Promise<void>} once the dedicated describe lists no member
   */
  untilEmpty(scope, group) {
    return pollUntil(
      scope,
      STEP_MS,
      async () => {
        const { groups } = await this.admin.describeShareGroups([group], {
          apiTimeout: API_TIMEOUT_MS,
        })
        const [described] = groups
        expect(groups).toHaveLength(1)
        expect(described.groupId).toBe(group)
        expect(described.errorCode).toBe(0)
        return described.state === 'Empty' && described.members.length === 0
      },
      { delayMs: 200, label: 'The empty share group' }
    )
  }

  /**
   * Captures the legacy `describeGroups` outcome on a share group id with a member present and
   * again when it is empty, each written to the evidence both as it was observed and normalized
   * into the fixture's schema.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async legacy(scope) {
    const group = this.groups.legacy
    const member = await this.startShare(scope, 'legacy', group)
    await pollUntil(scope, STEP_MS * 2, () => member.delivered.length >= RECORDS, {
      delayMs: 100,
      label: 'The legacy group deliveries',
    })
    await this.cluster.connect()
    await this.cluster.refreshMetadata()
    const coordinator = await this.cluster.findGroupCoordinator({ groupId: group })
    const capture = new LegacyDescriptionCapture(scope)
    const present = await bounded(scope, STEP_MS, () =>
      capture.capture(this.admin, coordinator, { groupId: group, stage: 'member-present' })
    )
    this.aliases = new Aliases()
    this.aliases.add(group, GROUP_ALIAS)
    this.aliases.add(this.topics.share, 'topic-1')
    this.aliases.add(`e2e-p9e-admin-clients-${this.id}`, 'client-1')
    this.aliases.add(member.memberId, 'member-1')
    this.record(present)

    await bounded(scope, 10000, () => member.consumer.disconnect({ timeout: 5000 }))
    await this.untilEmpty(scope, group)
    const empty = await bounded(scope, STEP_MS, () =>
      capture.capture(this.admin, coordinator, { groupId: group, stage: 'empty' })
    )
    this.record(empty)
  }

  /**
   * Writes one capture twice: as observed with the real identities and their rename, then as the
   * fixture entry, so a controller commits the authentic capture from the artifact.
   * @param {{ stage: string, trace: object[], rawDescribeGroups: object, publicOutcome: object }} captured
   * @returns {void}
   */
  record(captured) {
    const wireVersions = wireVersionsOf(this.adminRequests)
    this.note('legacy-description', {
      stage: captured.stage,
      observed: captured,
      aliasMap: this.aliases,
      wireVersions,
    })
    this.note('legacy-capture', {
      stage: captured.stage,
      groupAlias: GROUP_ALIAS,
      wireVersions,
      capture: {
        stage: captured.stage,
        groupAlias: GROUP_ALIAS,
        wireVersions,
        trace: this.aliases.apply(captured.trace),
        rawDescribeGroups: this.aliases.apply(captured.rawDescribeGroups),
        publicOutcome: this.aliases.apply(captured.publicOutcome),
      },
    })
  }

  /**
   * A share consumer on the ordinary group's id: its first join must fail loudly with a fatal
   * typed error that keeps the broker's own code, restart nothing and send no further share frame.
   * The exact class, reason and code are recorded, never assumed.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async collision(scope) {
    const consumer = this.kafka.shareConsumer({
      groupId: this.groups.ordinary,
      maxWaitTimeInMs: 250,
    })
    const member = new Member(this, 'collision', consumer)
    await bounded(scope, 10000, () => consumer.connect())
    await bounded(scope, 10000, () => consumer.subscribe({ topics: [this.topics.ordinary] }))
    const outcome = await bounded(scope, STEP_MS, async () => {
      try {
        await consumer.run({ eachMessage: async () => {} })
        return { kind: 'return' }
      } catch (error) {
        return { kind: 'throw', error }
      }
    })
    expect(outcome.kind).toBe('throw')
    const { error } = outcome
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.retriable).toBe(false)
    const code = error.cause?.code ?? error.code
    expect(Number.isInteger(code)).toBe(true)
    this.note('fault', {
      stage: 'consumer-group-id-collision',
      error: serializeError(error),
      brokerCode: code,
      crashes: serialize(member.crashes),
    })
    await delay(scope, 500)
    expect(member.crashes.every(({ restart }) => restart === false)).toBe(true)

    const shareFrames = () => member.requests.filter(({ apiName }) => /^Share/.test(apiName)).length
    const before = shareFrames()
    await delay(scope, 1500)
    expect(shareFrames()).toBe(before)
    expect(member.requests.some(({ apiName }) => apiName === 'ShareFetch')).toBe(false)
  }

  /**
   * The ordinary group kept its member, assignment and committed offset through everything, and a
   * new record reaches it and is committed; the unrelated group is present before its own cleanup.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async ordinaryIntact(scope) {
    const { groups } = await bounded(scope, STEP_MS, () => this.admin.listGroups())
    expect(groups.map(({ groupId }) => groupId)).toContain(this.groups.ordinary)
    expect(await this.ordinaryMembers(scope)).toEqual(this.ordinaryBefore)
    expect(await this.ordinaryOffset(scope)).toBe('1')
    expect(this.ordinary.crashes).toEqual([])

    await this.send(scope, this.topics.ordinary, 1, 1)
    await pollUntil(scope, STEP_MS, () => this.ordinary.delivered.length === 2, {
      delayMs: 100,
      label: 'The ordinary sentinel',
    })
    await pollUntil(scope, STEP_MS, async () => (await this.ordinaryOffset(scope)) === '2', {
      delayMs: 100,
      label: 'The ordinary sentinel commit',
    })
    this.note('acknowledgement', { stage: 'ordinary-intact', delivered: this.ordinary.delivered })
  }

  /**
   * Proves the owned groups and topics are gone and the clients' resources are back to baseline.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async proveCleanup(signal) {
    if (!this.baseline) return
    this.resources = {
      before: this.baseline,
      after: await assertShareCleanup(
        this.admin,
        Object.values(this.groups),
        Object.values(this.topics).filter(Boolean),
        this.baseline,
        this.probe,
        { signal, disconnectAdmin: true }
      ),
    }
  }

  /**
   * Deletes both topics and waits for them to be gone.
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   */
  async removeTopics(signal) {
    const owned = Object.values(this.topics).filter(Boolean)
    if (owned.length === 0) return
    await this.admin.deleteTopics({ topics: owned, timeout: 10000 })
    await pollUntil(
      signal,
      8000,
      async () => {
        const left = await this.admin.listTopics()
        return owned.every((topic) => !left.includes(topic))
      },
      { delayMs: 100, label: 'The topic deletion' }
    )
  }

  /**
   * Deletes every owned group (the ordinary one only here, after its consumer has gone), retrying
   * while one still counts a member, removes the config overrides this case set and waits until
   * no owned group and no override remains.
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   */
  async removeGroups(signal) {
    const owned = Object.values(this.groups)
    await pollUntil(
      signal,
      9000,
      async () => {
        let busy = false
        for (const id of owned) {
          try {
            await this.admin.deleteGroups([id])
          } catch (error) {
            const [item] = error.groups ?? []
            if (item?.errorCode === NON_EMPTY_GROUP) busy = true
            else if (item?.errorCode !== GROUP_ID_NOT_FOUND) throw error
          }
        }
        if (busy) return false
        const listed = (await this.admin.listGroups()).groups.map(({ groupId }) => groupId)
        return owned.every((id) => !listed.includes(id))
      },
      { delayMs: 200, label: 'The group deletion' }
    )
    for (const group of this.configured) {
      const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
      await this.admin.incrementalAlterConfigs({
        resources: [
          {
            ...resource,
            configEntries: [
              { name: 'share.auto.offset.reset', operation: ALTER_CONFIG_OP_TYPES.DELETE },
            ],
          },
        ],
      })
      const { resources } = await this.admin.describeConfigs({
        resources: [{ ...resource, configNames: ['share.auto.offset.reset'] }],
      })
      const left = (resources[0]?.configEntries ?? []).filter(
        ({ configSource }) => configSource === CONFIG_SOURCE.GROUP_CONFIG
      )
      if (left.length > 0) throw new Error(`Group config override remains on ${group}`)
    }
  }
}

describeAdmin('Share consumer > administration on a live group', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_BUDGET_MS - 2000, async () => {
        liveRun.assertClean()
      }),
    HOOK_BUDGET_MS
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })

  it(
    TITLE,
    liveCase(CASE_BUDGET_MS, (scope) => new AdminCase().run(scope)),
    caseTimeout(CASE_BUDGET_MS)
  )
})

describe('Share consumer > administration capture helpers', () => {
  it('turns buffers into hex objects and keeps errors with their codes and causes', () => {
    const cause = Object.assign(new Error('inner'), { code: 69, retriable: false })
    const error = Object.assign(new Error('outer', { cause }), { code: 35, type: 'X' })

    expect(serialize({ id: Buffer.from([0, 255]), list: [1, 'a'], none: undefined })).toEqual({
      id: { $buffer: '00ff' },
      list: [1, 'a'],
    })
    expect(serialize(error)).toEqual({
      name: 'Error',
      message: 'outer',
      code: 35,
      type: 'X',
      cause: { name: 'Error', message: 'inner', code: 69, retriable: false },
    })
    expect(() => serialize({ big: 1n })).toThrow('bigint')
  })

  it('renames identities inside strings only and never a number', () => {
    const aliases = new Aliases()
    aliases.add('real-group-1', GROUP_ALIAS)

    expect(
      aliases.apply({
        groupId: 'real-group-1',
        errorCode: 69,
        errorMessage: 'Group real-group-1 not found',
        offset: '69',
      })
    ).toEqual({
      groupId: GROUP_ALIAS,
      errorCode: 69,
      errorMessage: `Group ${GROUP_ALIAS} not found`,
      offset: '69',
    })
    expect(JSON.parse(JSON.stringify(aliases))).toEqual({ 'real-group-1': GROUP_ALIAS })
  })

  it('counts listeners by registration and removal', () => {
    const listeners = new ListenerCounter()
    listeners.add('a')
    listeners.add('a')
    listeners.add('b')
    listeners.add('b', -1)
    listeners.add('a', -1)

    expect(listeners.eventNames()).toEqual(['a'])
    expect(listeners.listenerCount('a')).toBe(1)
    expect(listeners.listenerCount('b')).toBe(0)
  })
})
