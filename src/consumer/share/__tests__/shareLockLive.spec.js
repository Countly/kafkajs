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
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

// Gated: it needs the short-lock share cluster (docker-compose.share.yml) and runs through the
// share lane or the guarded live entry with SHARE_LOCK_E2E=1.
const enabled = process.env.SHARE_LOCK_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-lock'
const PROFILE = 'share'
const FILE_MS = 150000
const JOIN_MS = 2000
const HOOK_MS = 10000
const CLEANUP_MS = 8000
const LOCK_MS = 3000
const REDELIVERY_SLACK_MS = 2000
const RENEW_AT_MS = [1500, 3000, 4500, 6000, 7500]
const LATE_ACCEPT_AT_MS = { expire: 6000, renew: 8000, disabled: 6000 }
const OLD_TIMESTAMP = '1700000000000'
const SHARE_FETCH = 78
const SHARE_ACKNOWLEDGE = 79
const SHARE_APIS = new Set([76, 78, 79])
const RENEW = 4
const ACCEPT = 1
const INVALID_RECORD_STATE = 121
const BASE_GROUP = {
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': LOCK_MS,
}

/**
 * The four lock scenarios: each runs on a fresh topic and group, with the lock fixed at three
 * seconds before either member joins.
 */
const SCENARIOS = {
  expire: {
    title: 'expires a held lock without crashing the late handler',
    budgetMs: 26000,
    key: 'expiry',
    records: 1,
    mode: 'explicit',
    maxRecords: 10,
    group: BASE_GROUP,
  },
  renew: {
    title: 'renews only held records without redelivery',
    budgetMs: 26000,
    key: 'renew',
    records: 1,
    mode: 'explicit',
    maxRecords: 10,
    group: BASE_GROUP,
  },
  disabled: {
    title: 'reports a disabled renewal and recovers the record',
    budgetMs: 26000,
    key: 'disabled',
    records: 1,
    mode: 'explicit',
    maxRecords: 10,
    group: { ...BASE_GROUP, 'share.renew.acknowledge.enable': false },
  },
  gate: {
    title: 'gates unstarted records and flushes completed handlers',
    budgetMs: 36000,
    key: 'gate',
    records: 50,
    mode: 'implicit',
    maxRecords: 50,
    group: BASE_GROUP,
  },
}

let fileStart = 0

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
 * @param {Error|undefined} error an error with an optional cause chain
 * @returns {number[]} the protocol codes found along the chain
 */
const causeCodes = (error) => {
  const codes = []
  for (let current = error; current; current = current.cause) {
    if (typeof current.code === 'number') codes.push(current.code)
  }
  return codes
}

/**
 * One live run of a lock scenario: two members share one partition under a three second lock,
 * a first-fetch gate gives member A the targeted acquisition, and the log, broker and handler
 * are checked independently.
 */
class LockCase {
  /**
   * @param {'expire'|'renew'|'disabled'|'gate'} kind the scenario; also the name of its action
   *   method
   */
  constructor(kind) {
    this.kind = kind
    this.scenario = SCENARIOS[kind]
    this.nonce = secureRandom(6)
    this.topic = shareTopicName(`lock-${kind}`)
    this.group = `e2e-p9e-lock-${kind}-group-${this.nonce}`
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
    this.corpus = []
    this.expected = []
    this.renewals = []
    this.renewOutcome = null
    this.fetched = null
    this.gateB = null
    this.baseline = null
    this.beforeAdmin = null
    this.groupConfig = this.scenario.group
    this.clientConfig = {
      acknowledgementMode: this.scenario.mode,
      acquireMode: 'record_limit',
      maxRecords: this.scenario.maxRecords,
      maxWaitTimeInMs: 250,
    }
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
      case: this.scenario.title,
      profile: PROFILE,
      scenario: this.nonce,
      groups: { lock: this.group },
      budgets: { caseMs: this.scenario.budgetMs, fileMs: FILE_MS, cleanupMs: CLEANUP_MS },
      ...fields,
    })
  }

  /**
   * Runs the whole case: setup, the scenario's action, final verification and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      const left = FILE_MS - (performance.now() - fileStart)
      if (left < this.scenario.budgetMs + JOIN_MS) {
        throw new Error(`Only ${Math.round(left)} ms of the file deadline remain for this case`)
      }
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await this[this.kind](scope)
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
   * Creates the topic and group configs, connects the admin and producer, measures the baseline,
   * then joins both members with member B's first fetch held.
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
    await bounded(scope, 10000, () => this.admin.connect())
    await bounded(scope, 20000, () =>
      createTopic({
        topic: this.topic,
        partitions: 1,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 10000, () =>
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
    await bounded(scope, 10000, () => this.producer.connect())

    this.gateB = this.tap.hold('B', SHARE_FETCH)
    await Promise.all([this.startMember(scope, 'A'), this.startMember(scope, 'B')])
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
    const member = {
      label,
      consumer,
      memberId: null,
      assignment: [],
      acks: [],
      crashes: [],
      expired: [],
    }
    this.members.set(label, member)
    const { events } = consumer
    const removers = [
      consumer.on(events.GROUP_JOIN, ({ payload }) => {
        member.memberId = payload.memberId
      }),
      consumer.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        member.assignment = payload.assignment
      }),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) =>
        member.acks.push({ ...payload, at: performance.now() })
      ),
      consumer.on(events.LOCK_EXPIRED, ({ payload }) => member.expired.push(payload)),
      consumer.on(events.CRASH, ({ payload }) => member.crashes.push(payload.error)),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
      consumer.on(events.FETCH, ({ payload }) => this.onFetch(member, payload)),
    ]
    this.stack.add(`remove ${label} listeners`, () => removers.forEach((remove) => remove()))
    this.stack.add(`disconnect ${label}`, () => consumer.disconnect({ timeout: 2000 }))
    await bounded(scope, 10000, () => consumer.connect())
    await bounded(scope, 10000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 15000, () => consumer.run({ eachMessage: this.handler(label, scope) }))
  }

  /**
   * In the gate scenario, lets member B fetch as soon as member A has captured its acquisition.
   * @param {object} member the member that fetched
   * @param {{ numberOfRecords: number }} payload the fetch event
   * @returns {void}
   */
  onFetch(member, payload) {
    if (this.kind !== 'gate' || member.label !== 'A' || this.fetched !== null) return
    if (payload.numberOfRecords === 0) return
    this.fetched = payload.numberOfRecords
    this.gateB.release()
  }

  /**
   * @param {string} label the member label
   * @param {AbortSignal} scope the case scope
   * @returns {Function} the member's eachMessage handler, which records every entry at once
   */
  handler(label, scope) {
    return async (payload) => {
      const { topic, partition, message } = payload
      const entry = {
        member: label,
        identity: shareRecordIdentity({ ...message, topic, partition }),
        deliveryCount: message.deliveryCount,
        at: performance.now(),
        returnedAt: null,
      }
      this.entries.push(entry)
      this.openHandlers++
      try {
        await this.behave(entry, payload, scope)
      } finally {
        entry.returnedAt = performance.now()
        this.openHandlers--
      }
    }
  }

  /**
   * The scenario's fixed handler behaviour. Member B accepts at once; member A holds, renews or
   * works as its scenario prescribes, on a schedule counted from its own first handler start.
   * @param {object} entry the recorded entry of this invocation
   * @param {object} payload the eachMessage payload
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async behave(entry, payload, scope) {
    if (entry.member === 'B') {
      if (this.scenario.mode === 'explicit') payload.acknowledge('accept')
      return
    }
    if (this.kind === 'gate') {
      await delay(scope, 200)
      return
    }
    this.gateB.release()
    const wait = (until) => delay(scope, Math.max(0, entry.at + until - performance.now()))
    const settle = (promise) =>
      promise.then(
        (value) => ({ value }),
        (error) => ({ error })
      )
    if (this.kind === 'renew') {
      for (const at of RENEW_AT_MS) {
        await wait(at)
        this.renewals.push(await settle(payload.renew()))
      }
    }
    if (this.kind === 'disabled') {
      await wait(1500)
      this.renewOutcome = await settle(payload.renew())
    }
    await wait(LATE_ACCEPT_AT_MS[this.kind])
    payload.acknowledge('accept')
  }

  /**
   * Waits until both members hold the partition and the admin API reports the same members,
   * assignments and a topic id.
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
    for (const { assignment } of members) {
      expect(JSON.parse(normalizeAssignment(assignment))).toEqual([[this.topic, [0]]])
    }
    this.row('assignment', {
      members: members.map(({ label, memberId }) => ({ label, memberId, partitions: [0] })),
    })
  }

  /**
   * Produces the scenario's fixed records in one acknowledged send, then reads the log back.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async produce(scope) {
    const { key, records } = this.scenario
    const messages = Array.from({ length: records }, (_, index) => ({
      partition: 0,
      key: Buffer.from(`${key}:${String(index).padStart(4, '0')}`),
      value: Buffer.from(`${key}:value:${index}`),
      timestamp: OLD_TIMESTAMP,
    }))
    const [sent] = await bounded(scope, 15000, () =>
      this.producer.send({ topic: this.topic, acks: -1, messages })
    )
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
    this.row('produced', { records, baseOffset: sent.baseOffset })
    await this.checkLog(scope)
  }

  /**
   * @param {string} label the member label
   * @returns {object[]} that member's handler entries in order
   */
  entriesOf(label) {
    return this.entries.filter(({ member }) => member === label)
  }

  /**
   * Waits for member A's first delivery and checks the lock it runs under is the configured one.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object>} that first entry
   */
  async firstDelivery(scope) {
    await pollUntil(scope, 15000, () => this.entriesOf('A').length > 0, {
      label: 'The first delivery',
    })
    expect(this.members.get('A').consumer.acquisitionLockTimeoutMs()).toBe(LOCK_MS)
    return this.entriesOf('A')[0]
  }

  /**
   * The acquisition's send time, taken from the last ShareFetch frame the member wrote before its
   * first handler started: a node fetches again only after its handlers are done, so that frame is
   * the one whose answer carried the records.
   * @param {string} label the member label
   * @returns {number} `performance.now()` at that frame's write
   */
  acquiredAt(label) {
    const [first] = this.entriesOf(label)
    const frames = this.tap.frames.filter(
      (frame) => frame.label === label && frame.apiKey === SHARE_FETCH && frame.at <= first.at
    )
    return frames[frames.length - 1].at
  }

  /**
   * Waits for member B's redelivery, which must arrive within the slack after the original lock
   * deadline.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object>} B's first entry
   */
  async awaitRedelivery(scope) {
    const deadline = this.acquiredAt('A') + LOCK_MS
    await pollUntil(
      scope,
      Math.max(1, deadline + REDELIVERY_SLACK_MS - performance.now()),
      () => this.entriesOf('B').length > 0,
      { label: 'The redelivery to the second member' }
    )
    return this.entriesOf('B')[0]
  }

  /**
   * A's lock expires while its handler is still held: B receives the same record at delivery count
   * two, and A's late accept is refused by the broker without crashing A.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async expire(scope) {
    await this.produce(scope)
    const first = await this.firstDelivery(scope)
    const second = await this.awaitRedelivery(scope)
    expect([first.deliveryCount, second.deliveryCount]).toEqual([1, 2])
    expect(second.identity).toBe(first.identity)
    const a = this.members.get('A')
    await this.observe(scope, 'The late accept result', 15000, () => this.accepts(a).length > 0)
    const [late] = this.accepts(a)
    expect(late.error).not.toBeNull()
    expect(late.error.reason).toBe('rejected-by-broker')
    expect(causeCodes(late.error)).toContain(INVALID_RECORD_STATE)
    expect(a.crashes).toEqual([])
    expect(a.consumer.assignment()).not.toEqual([])
    this.expectedHandled = new Map([[first.identity, 2]])
    this.recordAcknowledgements()
  }

  /**
   * A renews its held record five times; the lock never lapses, B never sees the record, and every
   * renewal is a ShareAcknowledge v2 renew frame for the single offset.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async renew(scope) {
    await this.produce(scope)
    const first = await this.firstDelivery(scope)
    const a = this.members.get('A')
    await this.observe(
      scope,
      'The renewals and the final accept',
      15000,
      () => this.renewals.length === RENEW_AT_MS.length && this.accepts(a).length > 0
    )
    expect(this.renewals).toEqual(RENEW_AT_MS.map(() => ({ value: LOCK_MS })))
    expect(this.entriesOf('B')).toEqual([])
    expect(this.entriesOf('A').map(({ deliveryCount }) => deliveryCount)).toEqual([1])
    const frames = this.decoded('A')
    const renewFrames = frames.filter(
      ({ frame, request }) => frame.apiKey === SHARE_ACKNOWLEDGE && request.isRenewAck
    )
    expect(renewFrames).toHaveLength(RENEW_AT_MS.length)
    for (const { request } of renewFrames) {
      expect(
        request.topics.flatMap(({ partitions }) => partitions.map((p) => p.acknowledgementBatches))
      ).toEqual([[{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [RENEW] }]])
    }
    expect(this.writtenTypes(frames).get('0')).toEqual([...RENEW_AT_MS.map(() => RENEW), ACCEPT])
    const epochs = frames.map(({ request }) => request.shareSessionEpoch)
    expect(epochs.every((epoch, n) => n === 0 || epoch > epochs[n - 1])).toBe(true)
    expect(a.acks.filter(({ type }) => type === 'renew').map(({ error }) => error)).toEqual(
      RENEW_AT_MS.map(() => null)
    )
    expect(a.acks.filter(({ type }) => type !== 'renew')).toHaveLength(1)
    expect(this.accepts(a)[0].error).toBeNull()
    this.expectedHandled = new Map([[first.identity, 1]])
    this.recordAcknowledgements()
  }

  /**
   * With renewal disabled for the group, A's renew is refused and does not extend the lock: B
   * receives the record at count two, and the broker accepts only one terminal decision for it.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async disabled(scope) {
    await this.produce(scope)
    const first = await this.firstDelivery(scope)
    await this.observe(scope, 'The renewal result', 10000, () => this.renewOutcome !== null)
    const { error } = this.renewOutcome
    expect(error?.name).toBe('KafkaJSShareAcknowledgementError')
    expect(error.reason).toBe('rejected-by-broker')
    expect(causeCodes(error)).toContain(INVALID_RECORD_STATE)
    const second = await this.awaitRedelivery(scope)
    expect([first.deliveryCount, second.deliveryCount]).toEqual([1, 2])
    expect(second.identity).toBe(first.identity)
    const a = this.members.get('A')
    await this.observe(scope, 'The late accept result', 15000, () => this.accepts(a).length > 0)
    const confirmed = [...this.members.values()].flatMap((member) =>
      this.accepts(member).filter(({ error: failure }) => failure === null)
    )
    expect(confirmed).toHaveLength(1)
    expect(a.crashes).toEqual([])
    this.expectedHandled = new Map([[first.identity, 2]])
    this.recordAcknowledgements()
  }

  /**
   * A acquires all fifty records at once but starts handlers only until the lock lapses: the
   * unstarted ones are dropped and redelivered to B at count two, and A's finished records are
   * flushed before the lapse.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async gate(scope) {
    await this.produce(scope)
    await this.observe(scope, 'The first acquisition', 15000, () => this.fetched !== null)
    expect(this.fetched).toBe(this.scenario.records)
    await this.awaitBrokerEnd(scope)
    await this.observe(scope, 'Every record delivered and A idle', 25000, () => {
      const delivered = new Set(this.entries.map(({ identity }) => identity))
      return (
        delivered.size === this.scenario.records &&
        this.entriesOf('A').every(({ returnedAt }) => returnedAt !== null)
      )
    })
    const a = this.members.get('A')
    const own = this.entriesOf('A')
    const second = this.entriesOf('B')
    const firstStart = own[0].at
    expect(own.filter(({ at }) => at > firstStart + LOCK_MS)).toEqual([])
    expect(own.every(({ deliveryCount }) => deliveryCount === 1)).toBe(true)
    const processed = new Set(own.map(({ identity }) => identity))
    expect(processed.size).toBe(own.length)
    const dropped = this.expected.filter((identity) => !processed.has(identity))
    const received = tally(second.map(({ identity }) => identity))
    expect(second.every(({ deliveryCount }) => deliveryCount === 2)).toBe(true)
    expect([...received.values()].every((n) => n === 1)).toBe(true)
    const inFlight = [...received.keys()].filter((identity) => processed.has(identity))
    expect(inFlight.length).toBeLessThanOrEqual(1)
    expect(dropped.filter((identity) => !received.has(identity))).toEqual([])
    expect(received.size).toBe(dropped.length + inFlight.length)
    this.expectedHandled = new Map(
      this.expected.map((identity) => [identity, inFlight.includes(identity) ? 2 : 1])
    )
    this.row('held-set', {
      acquiredAt: this.acquiredAt('A'),
      processed: [...processed],
      dropped,
      inFlight,
      firstStart,
    })
    const offsetOf = new Map(this.corpus.map(({ identity, offset }) => [identity, offset]))
    const before = new Set(
      a.expired.filter(({ stage }) => stage === 'before-handler').flatMap(({ offsets }) => offsets)
    )
    expect(dropped.filter((identity) => !before.has(offsetOf.get(identity)))).toEqual([])
    expect([...processed].filter((identity) => before.has(offsetOf.get(identity)))).toEqual([])
    const accepted = () => a.acks.filter(({ type, error }) => type === 'accept' && error === null)
    const lapse = this.acquiredAt('A') + LOCK_MS
    const lastReturn = Math.max(...own.map(({ returnedAt }) => returnedAt))
    expect(accepted().some(({ at }) => at < Math.min(lapse, lastReturn))).toBe(true)
    await this.observe(scope, 'Every finished record accepted', 15000, () => {
      const offsets = new Set(
        accepted().flatMap(({ firstOffset, lastOffset }) => offsetsOf(firstOffset, lastOffset))
      )
      return [...processed].every(
        (identity) => inFlight.includes(identity) || offsets.has(offsetOf.get(identity))
      )
    })
    this.recordAcknowledgements()
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @param {string} label names the wait in its time-out message
   * @param {number} ms the longest the wait may take
   * @param {() => boolean} check true once the observation holds
   * @returns {Promise<void>}
   */
  observe(scope, label, ms, check) {
    return pollUntil(scope, ms, check, { label })
  }

  /**
   * @param {object} member a member
   * @returns {object[]} its terminal accept acknowledgement events
   */
  accepts(member) {
    return member.acks.filter(({ type }) => type === 'accept')
  }

  /**
   * @param {string} label the member label
   * @returns {Array<{ frame: object, request: object }>} every ShareFetch and ShareAcknowledge
   *   request the member wrote, decoded by the independent readers
   */
  decoded(label) {
    return this.tap.frames
      .filter((frame) => frame.label === label && frame.apiKey !== 76)
      .map((frame) => {
        expect(frame.apiVersion).toBe(2)
        const reader = frame.apiKey === SHARE_FETCH ? shareFetchV2 : shareAcknowledgeV2
        return { frame, request: reader.readRequest(frame.bytes.subarray(frame.headerBytes)) }
      })
  }

  /**
   * @param {Array<{ request: object }>} frames decoded requests
   * @returns {Map<string, number[]>} the acknowledge type written for each offset, in order
   */
  writtenTypes(frames) {
    const byOffset = new Map()
    for (const { request } of frames) {
      for (const { partitions } of request.topics ?? []) {
        for (const { acknowledgementBatches } of partitions ?? []) {
          for (const batch of acknowledgementBatches ?? []) {
            const types = batch.acknowledgeTypes
            offsetsOf(batch.firstOffset, batch.lastOffset).forEach((offset, n) =>
              byOffset.set(offset, [
                ...(byOffset.get(offset) ?? []),
                types.length === 1 ? types[0] : types[n],
              ])
            )
          }
        }
      }
    }
    return byOffset
  }

  /**
   * Writes the scenario's acknowledgement results and frame counts as evidence.
   * @returns {void}
   */
  recordAcknowledgements() {
    this.row('frame', { shareFrames: this.tap.frameCounts() })
    this.row('acknowledgement', {
      results: [...this.members.values()].flatMap(({ label, acks }) =>
        acks.map(({ type, firstOffset, lastOffset, error, at }) => ({
          member: label,
          type,
          firstOffset,
          lastOffset,
          at,
          error: error ? { ...describeError(error), causeCodes: causeCodes(error) } : null,
        }))
      ),
      entries: this.entries.map(({ member, identity, deliveryCount, at, returnedAt }) => ({
        member,
        identity,
        deliveryCount,
        at,
        returnedAt,
      })),
    })
  }

  /**
   * Reads the log straight from the leader and compares it with the corpus; the corpus is one
   * acknowledged send, so the log is final once it is produced.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async checkLog(scope) {
    this.log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = this.log.get(0)
    expect(endOffset).toBe(String(this.scenario.records))
    expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    expect(records.map(({ offset }) => String(offset))).toEqual(
      Array.from({ length: this.scenario.records }, (_, n) => String(n))
    )
    this.rawIdentities = records.map((record) =>
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
   * Waits until the share start offset equals the log end in both the named and the all-topics
   * listing; the lag is recorded as the broker reports it.
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
          JSON.stringify(pick(all)) === JSON.stringify(first)
        )
      },
      { label: 'The broker share offsets' }
    )
    return [row]
  }

  /**
   * Checks the broker oracle, disconnects both members, lets a fresh member of the same group
   * listen for five seconds, then compares the final handler multiset with the frozen one.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    this.broker = await this.awaitBrokerEnd(scope)
    await Promise.all(
      [...this.members.values()].map(({ consumer }) =>
        bounded(scope, 10000, () => consumer.disconnect({ timeout: 2000 }))
      )
    )
    const verified = await bounded(scope, 30000, (inner) =>
      verifierMember(this.group, this.topic, { kafka: this.client('verifier'), signal: inner })
    )
    expect(verified).toEqual([])
    this.handled = tally(this.entries.map(({ identity }) => identity))
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    for (const { label, crashes } of this.members.values()) {
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

enabled('share record locks on a live group', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
        fileStart = performance.now()
      }),
    HOOK_MS
  )

  it(
    SCENARIOS.expire.title,
    liveCase(SCENARIOS.expire.budgetMs, (scope) => new LockCase('expire').run(scope)),
    caseTimeout(SCENARIOS.expire.budgetMs)
  )

  it(
    SCENARIOS.renew.title,
    liveCase(SCENARIOS.renew.budgetMs, (scope) => new LockCase('renew').run(scope)),
    caseTimeout(SCENARIOS.renew.budgetMs)
  )

  it(
    SCENARIOS.disabled.title,
    liveCase(SCENARIOS.disabled.budgetMs, (scope) => new LockCase('disabled').run(scope)),
    caseTimeout(SCENARIOS.disabled.budgetMs)
  )

  it(
    SCENARIOS.gate.title,
    liveCase(SCENARIOS.gate.budgetMs, (scope) => new LockCase('gate').run(scope)),
    caseTimeout(SCENARIOS.gate.budgetMs)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
