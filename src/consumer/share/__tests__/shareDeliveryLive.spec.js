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
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

// Gated: it needs the share-enabled base cluster (docker-compose.yml) and runs through the share
// lane or the guarded live entry with SHARE_DELIVERY_E2E=1.
const enabled = process.env.SHARE_DELIVERY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-delivery'
const PROFILE = 'tv2'
const TITLE = 'delivers six hundred records once across two members'
const BUDGET_MS = 90000
const HOOK_MS = 10000
const CLEANUP_MS = 15000
const PARTITIONS = 3
const RECORDS = 600
const PER_PARTITION = RECORDS / PARTITIONS
const OLD_TIMESTAMP = '1700000000000'
const HANDLER_MS = 5
const SHARE_FETCH = 78
const SHARE_APIS = new Set([76, 78, 79])
const GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const CLIENT_CONFIG = { acquireMode: 'record_limit', maxRecords: 100, maxWaitTimeInMs: 250 }

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
 * One live run of the delivery scenario: two share members read a three-partition topic in one
 * group and must see every record exactly once, with the log, broker and handler checked
 * independently.
 */
class DeliveryCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.topic = shareTopicName('delivery')
    this.group = `e2e-p9e-delivery-group-${this.nonce}`
    this.stack = new CleanupStack()
    this.tap = new ShareWireTap()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      queueDepth: () => this.tap.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.members = new Map()
    this.entries = []
    this.openHandlers = 0
    this.requests = []
    this.expected = []
    this.firstAcquirer = null
    this.gate = null
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
      groups: { delivery: this.group },
      budgets: { caseMs: BUDGET_MS, cleanupMs: CLEANUP_MS },
      ...fields,
    })
  }

  /**
   * Runs the whole case: setup, delivery, observation, final verification and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await this.deliver(scope)
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
   * Creates the topic and group config, connects the admin and producer, and measures the baseline
   * every later resource check compares against.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    const kafka = this.client('admin')
    this.admin = kafka.admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 30000, () =>
      createTopic({
        topic: this.topic,
        partitions: PARTITIONS,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, this.group, this.groupConfig)
    )
    await bounded(scope, 10000, () => this.admin.listGroups())
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())

    await this.startMember(scope, 'A')
    await this.startMember(scope, 'B')
    await this.awaitAssignments(scope)
  }

  /**
   * Starts one share member: listeners first, then connect, subscribe and run.
   * @param {AbortSignal} scope the case scope
   * @param {string} label the member label
   * @returns {Promise<void>}
   */
  async startMember(scope, label) {
    const consumer = this.client(label).shareConsumer({ groupId: this.group, ...this.clientConfig })
    const member = { label, consumer, memberId: null, assignment: [], acks: [], crashes: [] }
    this.members.set(label, member)
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
      consumer.on(events.FETCH, ({ payload }) => this.onFetch(member, payload)),
    ]
    this.stack.add(`remove ${label} listeners`, () => removers.forEach((remove) => remove()))
    this.stack.add(`disconnect ${label}`, () => consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.entries.push({
            member: label,
            identity: shareRecordIdentity({ ...message, topic, partition }),
            deliveryCount: message.deliveryCount,
          })
          this.openHandlers++
          try {
            await delay(scope, HANDLER_MS)
          } finally {
            this.openHandlers--
          }
        },
      })
    )
  }

  /**
   * Holds the next fetch of the first member that completes an acquisition, so the other member
   * is certain to receive part of the corpus.
   * @param {object} member the member that fetched
   * @param {{ numberOfRecords: number }} payload the fetch event
   * @returns {void}
   */
  onFetch(member, payload) {
    if (this.firstAcquirer || payload.numberOfRecords === 0) return
    this.firstAcquirer = member.label
    this.gate = this.tap.hold(member.label, SHARE_FETCH)
  }

  /**
   * Waits until both members hold an assignment that the admin API reports identically.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async awaitAssignments(scope) {
    const members = [...this.members.values()]
    await pollUntil(
      scope,
      20000,
      async (poll) => {
        if (members.some(({ assignment }) => assignment.length === 0)) return false
        const { groups } = await bounded(poll, 8000, () =>
          this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
        )
        const described = groups[0].members
        return (
          described.length === members.length &&
          members.every((member) => {
            const found = described.find(({ memberId }) => memberId === member.memberId)
            return (
              found &&
              normalizeAssignment(found.assignment) === normalizeAssignment(member.assignment)
            )
          })
        )
      },
      { label: 'The assignment barrier' }
    )
    const covered = new Set(
      members.flatMap(({ assignment }) => assignment.flatMap((a) => a.partitions))
    )
    expect([...covered].sort()).toEqual([0, 1, 2])
    this.row('assignment', {
      members: members.map(({ label, memberId, assignment }) => ({
        label,
        memberId,
        assignment: JSON.parse(normalizeAssignment(assignment)),
      })),
    })
  }

  /**
   * Produces the fixed corpus, one acknowledged send per partition, then waits for all deliveries
   * and all terminal accepts while the gate gives both members a share.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async deliver(scope) {
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const indexes = Array.from({ length: PER_PARTITION }, (_, n) => n * PARTITIONS + partition)
      const messages = indexes.map((i) => ({
        partition,
        key: Buffer.from(`delivery:${String(i).padStart(4, '0')}`),
        value: Buffer.from(`delivery:value:${i}`),
        timestamp: OLD_TIMESTAMP,
      }))
      const [sent] = await bounded(scope, 15000, () =>
        this.producer.send({ topic: this.topic, acks: -1, messages })
      )
      expect(sent.errorCode).toBe(0)
      expect(sent.partition).toBe(partition)
      messages.forEach((message, position) =>
        this.expected.push(
          shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(BigInt(sent.baseOffset) + BigInt(position)),
            key: message.key,
            value: message.value,
          })
        )
      )
    }
    this.row('produced', { records: this.expected.length })

    await pollUntil(scope, 15000, () => this.firstAcquirer !== null, {
      label: 'The first acquisition',
    })
    const other = [...this.members.keys()].find((label) => label !== this.firstAcquirer)
    await pollUntil(scope, 15000, () => this.entries.some(({ member }) => member === other), {
      label: 'The second member receiving a record',
    })
    this.gate.release()

    await pollUntil(scope, 40000, () => this.entries.length >= RECORDS, { label: 'All deliveries' })
    await pollUntil(scope, 15000, () => this.acceptedOffsets() >= BigInt(RECORDS), {
      label: 'All terminal accepts',
    })
  }

  /**
   * @returns {bigint} offsets confirmed accepted by the broker, over every member
   * @throws {Error} on any terminal event that is not a confirmed accept
   */
  acceptedOffsets() {
    let total = 0n
    for (const { acks, label } of this.members.values()) {
      for (const { type, error, firstOffset, lastOffset } of acks) {
        if (type === 'renew') continue
        if (type !== 'accept' || error !== null) {
          throw new Error(`Member ${label} reported ${type} with ${error?.message ?? 'no error'}`)
        }
        total += BigInt(lastOffset) - BigInt(firstOffset) + 1n
      }
    }
    return total
  }

  /**
   * Checks the three independent oracles: raw log, handler entries and broker offsets.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    const log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    this.log = log
    const raw = []
    for (const [partition, { endOffset, records }] of log) {
      expect(endOffset).toBe(String(PER_PARTITION))
      expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
      expect(records.map(({ offset }) => String(offset))).toEqual(
        Array.from({ length: PER_PARTITION }, (_, n) => String(n))
      )
      records.forEach((record) =>
        raw.push(
          shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(record.offset),
            key: record.key,
            value: record.value,
          })
        )
      )
    }
    this.rawIdentities = raw
    expect(multisetProblems(tally(this.expected), tally(raw))).toEqual({
      missing: [],
      extra: [],
      count: [],
    })

    this.handled = tally(this.entries.map(({ identity }) => identity))
    this.expectedHandled = tally(this.expected)
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    expect(this.entries.every(({ deliveryCount }) => deliveryCount === 1)).toBe(true)
    for (const { label } of this.members.values()) {
      expect(this.entries.filter(({ member }) => member === label).length).toBeGreaterThan(0)
    }

    this.broker = await this.awaitBrokerEnd(scope)
    this.row('frame', { shareFrames: this.tap.frameCounts() })
    this.row('acknowledgement', {
      accepted: String(this.acceptedOffsets()),
      perMember: [...this.members.values()].map(({ label, acks }) => ({
        label,
        events: acks.length,
      })),
    })
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
    await pollUntil(
      scope,
      20000,
      async (poll) => {
        const named = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({
            groupId: this.group,
            topics: [{ topic: this.topic, partitions }],
            apiTimeout: 8000,
          })
        )
        const all = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({ groupId: this.group, apiTimeout: 8000 })
        )
        const pick = (listing) =>
          listing.find(({ topic }) => topic === this.topic)?.partitions ?? []
        rows = partitions.map((partition) => {
          const forms = [pick(named), pick(all)].map((list) =>
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
      { label: 'The broker share offsets' }
    )
    return rows.map(({ agree: _agree, ...row }) => row)
  }

  /**
   * Disconnects both members, then lets a fresh member of the same group listen for five seconds.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    for (const { consumer } of this.members.values()) {
      await bounded(scope, 10000, () => consumer.disconnect({ timeout: 5000 }))
    }
    const verified = await bounded(scope, 30000, (inner) =>
      verifierMember(this.group, this.topic, { kafka: this.client('verifier'), signal: inner })
    )
    expect(verified).toEqual([])
    expect(this.entries).toHaveLength(RECORDS)
    for (const { crashes, label } of this.members.values()) {
      expect({ label, crashes: crashes.map((e) => e.message) }).toEqual({ label, crashes: [] })
    }
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

enabled('share delivery on a live group', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  it(
    TITLE,
    liveCase(BUDGET_MS, (scope) => new DeliveryCase().run(scope)),
    caseTimeout(BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
