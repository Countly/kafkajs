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
const { readLog } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const shareFetchV2 = require('../../../../testHelpers/wire/shareFetchV2')
const shareAcknowledgeV2 = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const {
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

// Gated: it needs the share-enabled base cluster (docker-compose.yml) and runs through the share
// lane or the guarded live entry with SHARE_ACK_E2E=1.
const enabled = process.env.SHARE_ACK_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-ack'
const PROFILE = 'tv2'
const TITLE = 'settles all four acknowledgement classes and control gaps'
const BUDGET_MS = 90000
const HOOK_MS = 10000
const CLEANUP_MS = 15000
const RECORDS = 200
const MARKER_OFFSET = '200'
const OLD_TIMESTAMP = '1700000000000'
const DELIVERY_LIMIT = '5'
const SHARE_FETCH = 78
const SHARE_ACKNOWLEDGE = 79
const SHARE_APIS = new Set([76, 78, 79])
const GAP = 0
const ACCEPT = 1
const RELEASE = 2
const REJECT = 3
const GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const CLIENT_CONFIG = {
  acknowledgementMode: 'explicit',
  acquireMode: 'record_limit',
  maxRecords: 100,
  maxWaitTimeInMs: 250,
}

/**
 * What the handler decides for each record class (index modulo four) at its n-th delivery, the
 * terminal event types and wire types that must result, and the delivery counts it must see.
 */
const CLASSES = [
  { decide: () => 'accept', events: ['accept'], wire: [ACCEPT], counts: [1] },
  {
    decide: (n) => (n === 1 ? 'release' : 'accept'),
    events: ['release', 'accept'],
    wire: [RELEASE, ACCEPT],
    counts: [1, 2],
  },
  {
    decide: () => 'release',
    events: Array(5).fill('release'),
    wire: Array(5).fill(RELEASE),
    counts: [1, 2, 3, 4, 5],
  },
  { decide: () => 'reject', events: ['reject'], wire: [REJECT], counts: [1] },
]

const EXPECTED_ENTRIES =
  (RECORDS / CLASSES.length) * CLASSES.reduce((sum, { counts }) => sum + counts.length, 0)

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
 * One request frame withheld from the broker until it is released.
 */
class Hold {
  /**
   * @param {string} label the client whose frame is withheld
   * @param {number} apiKey the API of that frame
   */
  constructor(label, apiKey) {
    this.label = label
    this.apiKey = apiKey
    this.pending = null
    this.released = false
  }

  /**
   * @param {string} label the writing client
   * @param {number} apiKey the written API
   * @returns {boolean} whether this hold still waits for that client's next frame of the API
   */
  matches(label, apiKey) {
    return !this.released && this.pending === null && this.label === label && this.apiKey === apiKey
  }

  /**
   * Forwards the withheld frame, or lets the next one pass when none was withheld yet.
   * @returns {void}
   */
  release() {
    this.released = true
    const forward = this.pending
    this.pending = null
    if (forward) forward()
  }

  /**
   * Drops the hold without forwarding anything.
   * @returns {void}
   */
  discard() {
    this.released = true
    this.pending = null
  }
}

/**
 * Wraps the plaintext socket factory of every client of one case: counts sockets and unanswered
 * requests from the bytes actually written and read, and can withhold one chosen request frame.
 * Bytes, order, return values and callbacks of every other write are untouched.
 */
class ShareWireTap {
  /**
   * @param {Function} [connect] opens one socket; replaced by a fake in the offline checks
   */
  constructor(connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect)) {
    this.connect = connect
    this.sockets = []
    this.frames = []
    this.holds = []
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
      sent: 0,
      answered: 0,
    }
    this.sockets.push(entry)
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => this.write(entry, write, data, rest)
    socket.on('data', (chunk) => {
      entry.answered += entry.reader.push(chunk).length
    })
    return socket
  }

  /**
   * @param {object} entry the observed socket
   * @param {Function} write the original write
   * @param {Buffer|string} data the bytes the client wrote
   * @param {unknown[]} rest the remaining write arguments
   * @returns {boolean} what the original write returned, or true for a withheld frame
   */
  write(entry, write, data, rest) {
    const bytes = Buffer.isBuffer(data)
      ? data
      : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
    const frames = entry.writer.push(bytes)
    entry.sent += frames.length
    const headers = frames.map((frame) => this.record(entry, frame))
    const whole = frames.length === 1 && frames[0].length === bytes.length
    const hold = whole && this.holds.find((h) => h.matches(entry.label, headers[0].apiKey))
    if (!hold) return write(data, ...rest)
    hold.pending = () => write(data, ...rest)
    return true
  }

  /**
   * Keeps every share request frame with its header facts and write time.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @returns {{ apiKey: number }} the decoded request header
   */
  record(entry, frame) {
    const header = readRequestHeader(frame)
    if (SHARE_APIS.has(header.apiKey)) {
      this.frames.push({
        label: entry.label,
        socket: this.sockets.indexOf(entry),
        at: performance.now(),
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
        correlationId: header.correlationId,
        headerBytes: header.byteLength,
        bytes: frame,
      })
    }
    return header
  }

  /** @returns {Record<string, Record<number, number>>} written share frames by client and API */
  frameCounts() {
    const counts = {}
    for (const { label, apiKey } of this.frames) {
      counts[label] ??= {}
      counts[label][apiKey] = (counts[label][apiKey] ?? 0) + 1
    }
    return counts
  }

  /**
   * @param {string} label the client
   * @param {number} apiKey the API
   * @returns {Hold} withholds that client's next frame of the API until released
   */
  hold(label, apiKey) {
    const hold = new Hold(label, apiKey)
    this.holds.push(hold)
    return hold
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
    return this.sockets
      .filter(({ socket }) => !socket.destroyed)
      .reduce((total, { sent, answered }) => total + sent - answered, 0)
  }

  /**
   * Drops every hold without forwarding.
   * @returns {void}
   */
  discardHolds() {
    this.holds.forEach((hold) => hold.discard())
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
 * @param {string} first first offset, a decimal string
 * @param {string} last last offset, inclusive
 * @returns {string[]} every offset of the range
 * @throws {Error} for a range too large to be a fixture range
 */
const offsetsOf = (first, last) => {
  const span = BigInt(last) - BigInt(first) + 1n
  if (span < 1n || span > 10000n) throw new Error(`Unexpected offset range ${first}..${last}`)
  return Array.from({ length: Number(span) }, (_, n) => String(BigInt(first) + BigInt(n)))
}

/**
 * Appends to the list a map keeps under a key.
 * @param {Map<string, unknown[]>} map the map
 * @param {string} key the key
 * @param {unknown} value the value to append
 * @returns {void}
 */
const append = (map, key, value) => map.set(key, [...(map.get(key) ?? []), value])

/**
 * One live run of the acknowledgement scenario: one explicit member settles a committed
 * transaction of two hundred records in four decision classes plus its commit marker, with the
 * log, broker, handler and wire checked independently.
 */
class AckTypesCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.topic = shareTopicName('ack')
    this.group = `e2e-p9e-ack-group-${this.nonce}`
    this.stack = new CleanupStack()
    this.tap = new ShareWireTap()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      queueDepth: () => this.tap.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.member = null
    this.entries = []
    this.deliveries = new Map()
    this.openHandlers = 0
    this.requests = []
    this.corpus = []
    this.expected = []
    this.baseline = null
    this.beforeAdmin = null
    this.groupConfig = GROUP_CONFIG
    this.clientConfig = CLIENT_CONFIG
  }

  /**
   * @param {string} label the client label, also its clientId
   * @returns {Kafka} a client whose sockets go through the tap
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.factory(label),
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
      groups: { ack: this.group },
      budgets: { caseMs: BUDGET_MS, cleanupMs: CLEANUP_MS },
      ...fields,
    })
  }

  /**
   * Runs the whole case: setup, settlement, observation, final verification and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await this.settle(scope)
          await this.observe(scope)
          await this.finish(scope)
        },
        { budgetMs: CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.row('case-failed', { error: describeError(error) })
      throw error
    }
  }

  /**
   * Creates the topic and group config, checks the broker's delivery limit, connects the admin and
   * the transactional producer, and measures the baseline later resource checks compare against.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 30000, () =>
      createTopic({
        topic: this.topic,
        partitions: 1,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, this.group, this.groupConfig)
    )
    await this.requireDeliveryLimit(scope)
    await bounded(scope, 10000, () => this.admin.listGroups())
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({
      transactionalId: `e2e-p9e-ack-${this.nonce}`,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
  }

  /**
   * Requires the broker's effective delivery limit to be the one the release-every class relies
   * on; the expectation is never rewritten to what was observed.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async requireDeliveryLimit(scope) {
    const name = 'group.share.delivery.count.limit'
    const { brokers } = await bounded(scope, 10000, () => this.admin.describeCluster())
    const { resources } = await bounded(scope, 10000, () =>
      this.admin.describeConfigs({
        resources: [
          {
            type: ConfigResourceTypes.BROKER,
            name: String(brokers[0].nodeId),
            configNames: [name],
          },
        ],
      })
    )
    const entry = resources[0].configEntries.find(({ configName }) => configName === name)
    this.row('config-readback', { broker: { [name]: entry?.configValue ?? null } })
    expect(entry?.configValue).toBe(DELIVERY_LIMIT)
  }

  /**
   * Produces the committed transaction, joins the member and waits until the handler has seen
   * every expected delivery.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async settle(scope) {
    await this.produceTransaction(scope)
    await this.startMember(scope)
    await this.awaitAssignment(scope)
    await pollUntil(scope, 45000, () => this.entries.length >= EXPECTED_ENTRIES, {
      label: 'All deliveries',
    })
  }

  /**
   * Sends the fixed corpus in one transaction and commits it; the expected identities come from
   * the corpus and the broker's base offset, never from what the handler later sees.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async produceTransaction(scope) {
    const messages = Array.from({ length: RECORDS }, (_, index) => ({
      partition: 0,
      key: Buffer.from(`ack:${String(index).padStart(4, '0')}`),
      value: Buffer.from(`ack:value:${index}`),
      timestamp: OLD_TIMESTAMP,
    }))
    const transaction = await bounded(scope, 15000, () => this.producer.transaction())
    let sent
    try {
      const results = await bounded(scope, 20000, () =>
        transaction.send({ topic: this.topic, acks: -1, messages })
      )
      sent = results[0]
      await bounded(scope, 20000, () => transaction.commit())
    } catch (error) {
      await transaction.abort().catch(() => {})
      throw error
    }
    expect(sent.errorCode).toBe(0)
    messages.forEach((message, index) => {
      const offset = String(BigInt(sent.baseOffset) + BigInt(index))
      const identity = shareRecordIdentity({
        topic: this.topic,
        partition: 0,
        offset,
        key: message.key,
        value: message.value,
      })
      this.corpus.push({ identity, offset, index })
      this.expected.push(identity)
    })
    this.row('produced', {
      records: RECORDS,
      baseOffset: sent.baseOffset,
      markerOffset: MARKER_OFFSET,
    })
  }

  /**
   * Starts the single explicit member: listeners first, then connect, subscribe and run.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async startMember(scope) {
    const consumer = this.client('A').shareConsumer({ groupId: this.group, ...this.clientConfig })
    const member = { consumer, memberId: null, assignment: [], acks: [], crashes: [] }
    this.member = member
    const { events } = consumer
    const removers = [
      consumer.on(events.GROUP_JOIN, ({ payload }) => {
        member.memberId = payload.memberId
      }),
      consumer.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        member.assignment = payload.assignment
      }),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => member.acks.push(payload)),
      consumer.on(events.CRASH, ({ payload }) => member.crashes.push(payload.error)),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
    ]
    this.stack.add('remove member listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect member', () => consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message, acknowledge }) => {
          const identity = shareRecordIdentity({ ...message, topic, partition })
          const seen = this.deliveries.get(identity) ?? []
          seen.push(message.deliveryCount)
          this.deliveries.set(identity, seen)
          this.entries.push({ identity, deliveryCount: message.deliveryCount })
          const index = Number(message.key.toString().split(':')[1])
          this.openHandlers++
          try {
            acknowledge(CLASSES[index % CLASSES.length].decide(seen.length))
          } finally {
            this.openHandlers--
          }
        },
      })
    )
  }

  /**
   * Waits until the member holds the topic's partition and the admin API reports the same member
   * and assignment.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async awaitAssignment(scope) {
    await pollUntil(
      scope,
      20000,
      async (poll) => {
        if (this.member.assignment.length === 0) return false
        const { groups } = await bounded(poll, 8000, () =>
          this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
        )
        const [only] = groups[0].members
        return (
          groups[0].members.length === 1 &&
          only.memberId === this.member.memberId &&
          normalizeAssignment(only.assignment) === normalizeAssignment(this.member.assignment)
        )
      },
      { label: 'The assignment barrier' }
    )
    expect(JSON.parse(normalizeAssignment(this.member.assignment))).toEqual([[this.topic, [0]]])
    this.row('assignment', { memberId: this.member.memberId, assignment: [[this.topic, [0]]] })
  }

  /**
   * Checks the independent oracles: raw log, handler entries, broker offsets, terminal events and
   * written ranges.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    await this.checkLog(scope)
    this.checkHandler()
    this.broker = await this.awaitBrokerEnd(scope)
    await pollUntil(scope, 20000, () => this.eventsMatch(), { label: 'All terminal events' })
    this.checkAcknowledgements()
  }

  /**
   * Reads the log straight from the leader: two hundred data records, one commit marker, nothing
   * else.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async checkLog(scope) {
    this.log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = this.log.get(0)
    expect(endOffset).toBe('201')
    const control = records.filter(({ batchContext }) => batchContext.isControlBatch)
    const data = records.filter(({ batchContext }) => !batchContext.isControlBatch)
    expect(control.map(({ offset }) => String(offset))).toEqual([MARKER_OFFSET])
    expect(control[0].key.readInt16BE(2)).toBe(1)
    expect(data.map(({ offset }) => String(offset))).toEqual(
      Array.from({ length: RECORDS }, (_, n) => String(n))
    )
    this.rawIdentities = data.map((record) =>
      shareRecordIdentity({
        topic: this.topic,
        partition: 0,
        offset: String(record.offset),
        key: record.key,
        value: record.value,
      })
    )
    expect(multisetProblems(tally(this.expected), tally(this.rawIdentities))).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
  }

  /**
   * Checks every identity was handled as often as its class requires, with the delivery counts the
   * broker reported.
   * @returns {void}
   */
  checkHandler() {
    this.handled = tally(this.entries.map(({ identity }) => identity))
    this.expectedHandled = new Map(
      this.corpus.map(({ identity, index }) => [identity, CLASSES[index % 4].counts.length])
    )
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    const wrong = this.corpus
      .filter(
        ({ identity, index }) =>
          JSON.stringify(this.deliveries.get(identity)) !==
          JSON.stringify(CLASSES[index % 4].counts)
      )
      .map(({ offset }) => offset)
    expect(wrong).toEqual([])
  }

  /**
   * @returns {Map<string, Array<{ type: string, error: Error|null }>>} the terminal acknowledgement
   *   events per offset, in order, with ranges expanded
   */
  terminalEvents() {
    const byOffset = new Map()
    for (const { type, error, firstOffset, lastOffset } of this.member.acks) {
      if (type === 'renew') continue
      for (const offset of offsetsOf(firstOffset, lastOffset))
        append(byOffset, offset, { type, error })
    }
    return byOffset
  }

  /**
   * @returns {Map<string, number[]>} the acknowledge type written for each offset, in order, over
   *   every ShareFetch and ShareAcknowledge request, decoded by the independent readers
   */
  wireTypes() {
    const byOffset = new Map()
    for (const frame of this.tap.frames) {
      if (frame.apiKey !== SHARE_FETCH && frame.apiKey !== SHARE_ACKNOWLEDGE) continue
      expect(frame.apiVersion).toBe(2)
      const reader = frame.apiKey === SHARE_FETCH ? shareFetchV2 : shareAcknowledgeV2
      const request = reader.readRequest(frame.bytes.subarray(frame.headerBytes))
      for (const { partitions } of request.topics ?? []) {
        for (const { acknowledgementBatches } of partitions ?? []) {
          for (const batch of acknowledgementBatches ?? []) {
            const types = batch.acknowledgeTypes
            offsetsOf(batch.firstOffset, batch.lastOffset).forEach((offset, n) =>
              append(byOffset, offset, types.length === 1 ? types[0] : types[n])
            )
          }
        }
      }
    }
    return byOffset
  }

  /**
   * @returns {Array<[string, string[], number[]]>} each offset with the terminal event types and
   *   wire types it must show, the marker last
   */
  expectations() {
    return [
      ...this.corpus.map(({ offset, index }) => [
        offset,
        CLASSES[index % CLASSES.length].events,
        CLASSES[index % CLASSES.length].wire,
      ]),
      [MARKER_OFFSET, ['gap'], [GAP]],
    ]
  }

  /** @returns {boolean} whether every offset already shows exactly its expected terminal events */
  eventsMatch() {
    const events = this.terminalEvents()
    return this.expectations().every(
      ([offset, types]) =>
        JSON.stringify((events.get(offset) ?? []).map(({ type }) => type)) === JSON.stringify(types)
    )
  }

  /**
   * Checks the terminal events and the written ranges against each class: confirmed results,
   * application types on the wire, and a Gap only for the marker.
   * @returns {void}
   */
  checkAcknowledgements() {
    const events = this.terminalEvents()
    const wire = this.wireTypes()
    const wrong = this.expectations()
      .filter(
        ([offset, types, written]) =>
          JSON.stringify((events.get(offset) ?? []).map(({ type }) => type)) !==
            JSON.stringify(types) ||
          JSON.stringify(wire.get(offset) ?? []) !== JSON.stringify(written)
      )
      .map(([offset]) => offset)
    expect(wrong).toEqual([])
    expect([...events.values()].flat().filter(({ error }) => error !== null)).toEqual([])
    expect([...wire].filter(([, types]) => types.includes(GAP)).map(([offset]) => offset)).toEqual([
      MARKER_OFFSET,
    ])
    this.row('frame', { shareFrames: this.tap.frameCounts() })
    this.row('acknowledgement', {
      results: this.member.acks.map(({ type, firstOffset, lastOffset, error }) => ({
        type,
        firstOffset,
        lastOffset,
        error: error ? describeError(error) : null,
      })),
    })
  }

  /**
   * Waits until the share start offset equals the log end and the lag is zero or unknown, in both
   * the named and the all-topics listing.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object[]>} one terminal row for the partition
   */
  async awaitBrokerEnd(scope) {
    let row
    await pollUntil(
      scope,
      20000,
      async (poll) => {
        const named = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({
            groupId: this.group,
            topics: [{ topic: this.topic, partitions: [0] }],
            apiTimeout: 8000,
          })
        )
        const all = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({ groupId: this.group, apiTimeout: 8000 })
        )
        const pick = (listing) =>
          listing
            .find(({ topic }) => topic === this.topic)
            ?.partitions.find((p) => p.partition === 0)
        const first = pick(named)
        row = {
          groupId: this.group,
          topic: this.topic,
          partition: 0,
          errorCode: first?.errorCode,
          startOffset: first?.startOffset,
          logEnd: this.log.get(0).endOffset,
          lag: first?.lag,
        }
        return (
          row.errorCode === 0 &&
          row.startOffset === row.logEnd &&
          (row.lag === '0' || row.lag === null) &&
          JSON.stringify(pick(all)) === JSON.stringify(first)
        )
      },
      { label: 'The broker share offsets' }
    )
    return [row]
  }

  /**
   * Disconnects the member, then lets a fresh member of the same group listen for five seconds.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.member.consumer.disconnect({ timeout: 5000 }))
    const verified = await bounded(scope, 30000, (inner) =>
      verifierMember(this.group, this.topic, { kafka: this.client('verifier'), signal: inner })
    )
    expect(verified).toEqual([])
    expect(this.entries).toHaveLength(EXPECTED_ENTRIES)
    expect(this.member.crashes.map((error) => error.message)).toEqual([])
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
          {
            delayMs: 50,
            label: 'The sockets closing',
          }
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
      this.tap.discardHolds()
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
      {
        label: 'The topic disappearing',
      }
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
    if (resources[0].errorCode !== 0)
      throw new Error(`Config reset refused: ${resources[0].errorCode}`)
    const read = await bounded(signal, 8000, () =>
      this.admin.describeConfigs({
        resources: [{ type: ConfigResourceTypes.GROUP, name: this.group, configNames: names }],
      })
    )
    const left = read.resources[0].configEntries.filter(
      ({ configSource }) => configSource === ConfigSource.GROUP_CONFIG
    )
    if (left.length > 0)
      throw new Error(`Group config overrides remain: ${left.map((e) => e.configName)}`)
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
      wireVersions: wireVersionsOf(this.requests),
      config: { client: this.clientConfig, group: this.groupConfig },
    })
  }
}

describe('share wire tap', () => {
  const frame = (apiKey, payload) => {
    const header = Buffer.alloc(11)
    header.writeInt16BE(apiKey, 0)
    header.writeInt16BE(2, 2)
    header.writeInt32BE(7, 4)
    header.writeInt16BE(1, 8)
    header.write('t', 10)
    const body = Buffer.concat([header, Buffer.from([0]), Buffer.from(payload)])
    const bytes = Buffer.alloc(4 + body.length)
    bytes.writeInt32BE(body.length, 0)
    body.copy(bytes, 4)
    return bytes
  }

  it('splits fragmented and coalesced writes into whole frames and forwards every byte', () => {
    const splitter = new FrameSplitter()
    const first = frame(78, 'abcdef')
    const second = frame(79, 'xyz')
    const stream = Buffer.concat([first, second])
    const seen = [
      stream.subarray(0, 3),
      stream.subarray(3, first.length + 2),
      stream.subarray(first.length + 2),
    ].flatMap((chunk) => splitter.push(chunk))
    expect(seen).toEqual([first, second])
  })

  it('withholds only the chosen whole frame and keeps every other write unchanged', () => {
    const written = []
    const inner = () =>
      Object.assign(new EventEmitter(), {
        write: (data) => {
          written.push(Buffer.from(data))
          return true
        },
      })
    const tap = new ShareWireTap(inner)
    const socket = tap.factory('A')({})
    const hold = tap.hold('A', 78)
    expect(socket.write(frame(78, 'one'))).toBe(true)
    expect(written).toEqual([])
    socket.write(frame(78, 'two'))
    hold.release()
    expect(written).toEqual([frame(78, 'two'), frame(78, 'one')])
    expect(tap.pending()).toBe(2)
    expect(tap.frameCounts()).toEqual({ A: { 78: 2 } })
    socket.emit('data', frame(0, 'ok'))
    expect(tap.pending()).toBe(1)
  })
})

enabled('share acknowledgement classes on a live group', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  it(
    TITLE,
    liveCase(BUDGET_MS, (scope) => new AckTypesCase().run(scope)),
    caseTimeout(BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
