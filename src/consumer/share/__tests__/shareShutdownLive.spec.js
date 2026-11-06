const { EventEmitter } = require('events')
const { Kafka, KafkaJSNonRetriableError, logLevel } = require('../../../../index')
const CONFIG_RESOURCE_TYPES = require('../../../protocol/configResourceTypes')
const CONFIG_SOURCE = require('../../../protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../../../protocol/alterConfigOpTypes')
const defaultSocketFactory = require('../../../network/socketFactory')
const { ShareAckLedger } = require('../shareAckLedger')
const { plainTextBrokers, secureRandom, writeEvidence, wireVersionsOf } = require('testHelpers')
const { readLog, endOffsets } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const { readRequestHeader } = require('../../../../testHelpers/fakeSocket')
const heartbeatWire = require('../../../../testHelpers/wire/shareGroupHeartbeatV1')
const fetchWire = require('../../../../testHelpers/wire/shareFetchV2')
const acknowledgeWire = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const {
  assertShareCleanup,
  createShareTopic,
  setShareGroupConfigs,
  shareRecordIdentity,
  verifierMember,
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

// Gated: it needs the `share` compose profile (a lowered lock minimum, share.version 1 or later).
// Run it alone through the guarded live entry; `KAFKA_VERSION` names the cluster under test.
const describeShutdown = process.env.SHARE_SHUTDOWN_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-shutdown'
const PROFILE = 'share'
const LOCK_MS = 15000
const FORCED_BUDGET_MS = 40000
const ZERO_BUDGET_MS = 50000
const STOP_BUDGET_MS = 30000
const HOOK_BUDGET_MS = 10000
const CLEANUP_BUDGET_MS = 8000
const JOIN_BUDGET_MS = 10000
const DISCONNECT_MS = 5000
/** The product's forced boundary plus the time its final answer may take. */
const FINAL_ALLOWANCE_MS = 1000 + 4000 + 2000
const RELEASE_ALLOWANCE_MS = 3000
const QUIET_WINDOW_MS = 5000
const TIMESTAMP = '1700000000000'
const API = { heartbeat: 76, fetch: 78, acknowledge: 79 }
const SHARE_APIS = new Set(Object.values(API))
const ACK_TYPES = { accept: 1, release: 2 }
const READERS = new Map(
  [heartbeatWire, fetchWire, acknowledgeWire].map((pair) => [pair.apiKey, pair])
)

/**
 * Splits a byte stream of size-prefixed Kafka frames into whole frames, across any chunk boundary.
 */
class FrameReader {
  /** Starts with no partial frame. */
  constructor() {
    this.buffer = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk bytes as one socket write carried them
   * @returns {Buffer[]} every frame the bytes so far complete, size prefix included
   */
  push(chunk) {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk])
    const frames = []
    while (this.buffer.length >= 4) {
      const size = this.buffer.readInt32BE(0)
      if (size < 0) {
        this.buffer = Buffer.alloc(0)
        break
      }
      if (this.buffer.length < 4 + size) break
      frames.push(this.buffer.subarray(0, 4 + size))
      this.buffer = this.buffer.subarray(4 + size)
    }
    return frames
  }
}

/**
 * Wraps the plaintext socket factory of one client to record every complete request frame it
 * writes, decoded by the independent wire readers, and to hold back the first fetch on request.
 * It preserves the bytes, their order and the write's return value, and never answers anything.
 */
class ShareWireTap {
  /** @param {string} label the client the tap belongs to, e.g. `a` */
  constructor(label) {
    this.label = label
    /** @type {Array<{ at: number, apiKey: number, apiVersion: number, correlationId: number, fields?: object, decodeError?: string }>} */
    this.frames = []
    this.created = 0
    this.destroyed = 0
    this.armed = false
    this.queue = []
  }

  /**
   * @returns {Function} a socket factory for the `Kafka` client's `socketFactory` option
   */
  socketFactory() {
    const inner = defaultSocketFactory()
    return (options) => this.wrap(inner(options))
  }

  /**
   * @param {import('net').Socket} socket a socket the client just opened
   * @returns {import('net').Socket} the same socket, its `write` observed
   */
  wrap(socket) {
    this.created += 1
    socket.once('close', () => {
      this.destroyed += 1
    })
    const reader = new FrameReader()
    const write = socket.write.bind(socket)
    const state = { held: false }
    socket.write = (chunk, ...rest) => {
      const frames = reader.push(chunk)
      const fetches = frames.some((frame) => frame.readInt16BE(4) === API.fetch)
      if (state.held || (this.armed && fetches)) {
        this.armed = false
        state.held = true
        this.queue.push({ socket, chunk, rest, frames, write, state })
        return true
      }
      this.record(frames)
      return write(chunk, ...rest)
    }
    return socket
  }

  /**
   * Decodes frames with the independent readers and appends them, stamped with the write time.
   * @param {Buffer[]} frames whole request frames
   * @returns {void}
   */
  record(frames) {
    const at = performance.now()
    for (const frame of frames) {
      let header
      try {
        header = readRequestHeader(frame)
      } catch (error) {
        this.frames.push({
          at,
          apiKey: frame.readInt16BE(4),
          apiVersion: frame.readInt16BE(6),
          correlationId: frame.readInt32BE(8),
          decodeError: error.message,
        })
        continue
      }
      const entry = {
        at,
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
        correlationId: header.correlationId,
      }
      const reader = READERS.get(header.apiKey)
      if (reader && reader.version === header.apiVersion) {
        try {
          entry.fields = reader.readRequest(frame.subarray(header.byteLength))
        } catch (error) {
          entry.decodeError = error.message
        }
      }
      this.frames.push(entry)
    }
  }

  /**
   * Holds the next fetch this client writes, and every write behind it on that socket, until
   * `release`; heartbeats on other sockets keep flowing.
   * @returns {void}
   */
  holdFirstFetch() {
    this.armed = true
  }

  /**
   * Writes what `holdFirstFetch` held, in order, unless its socket is gone.
   * @returns {number} how many writes it released
   */
  release() {
    const held = this.queue.splice(0)
    for (const { socket, chunk, rest, frames, write, state } of held) {
      state.held = false
      if (socket.destroyed) continue
      this.record(frames)
      write(chunk, ...rest)
    }
    return held.length
  }

  /**
   * @param {number} [from=0] index of the first frame to look at
   * @returns {object[]} the share frames (heartbeat, fetch, acknowledge) written from there on
   */
  shareFrames(from = 0) {
    return this.frames.slice(from).filter(({ apiKey }) => SHARE_APIS.has(apiKey))
  }
}

/**
 * Counts what a case's clients register on their public `on`, so a probe can compare it.
 */
class ListenerCounter {
  /** Starts empty. */
  constructor() {
    this.counts = new Map()
  }

  /** @param {string} event @returns {void} */
  add(event) {
    this.counts.set(event, (this.counts.get(event) ?? 0) + 1)
  }

  /** @param {string} event @returns {void} */
  remove(event) {
    this.counts.set(event, (this.counts.get(event) ?? 1) - 1)
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
 * Watches the acknowledgement ledgers the case's clients create, by recording each one the first
 * time one of its methods runs, and reports how many entries they still hold.
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
 * A promise with its resolver, so one part of a case can wait for another.
 * @template T
 * @returns {{ promise: Promise<T>, resolve: (value?: T) => void }}
 */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/**
 * @param {string} scenario
 * @param {number} index
 * @returns {{ key: Buffer, value: Buffer }} the indexed record of the fixed corpus
 */
const corpusRecord = (scenario, index) => ({
  key: Buffer.from(`${scenario}:${String(index).padStart(4, '0')}`),
  value: Buffer.from(`${scenario}:value:${index}`),
})

/**
 * What each offset was told to become, from the batches of a ShareAcknowledge request.
 * @param {{ topics: Array<{ partitions: Array<{ acknowledgementBatches: object[] }> }> }} fields
 * @returns {Record<string, number>} the acknowledge type by decimal offset
 */
const typesByOffset = ({ topics }) => {
  const types = {}
  for (const { partitions } of topics) {
    for (const { acknowledgementBatches } of partitions) {
      for (const { firstOffset, lastOffset, acknowledgeTypes } of acknowledgementBatches) {
        for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
          types[String(offset)] =
            acknowledgeTypes.length === 1
              ? acknowledgeTypes[0]
              : acknowledgeTypes[Number(offset - BigInt(firstOffset))]
        }
      }
    }
  }
  return types
}

/**
 * @param {number} accepted offsets `0..accepted-1` are accepted
 * @param {number} total offsets `accepted..total-1` are released
 * @returns {Record<string, number>} the expected acknowledge type by decimal offset
 */
const wireDecisions = (accepted, total) =>
  Object.fromEntries(
    Array.from({ length: total }, (_, offset) => [
      String(offset),
      offset < accepted ? ACK_TYPES.accept : ACK_TYPES.release,
    ])
  )

/**
 * @param {number} accepted offsets `0..accepted-1` settle as accepts
 * @param {number} total offsets `accepted..total-1` settle as releases
 * @returns {Record<string, string>} the expected settled type by decimal offset
 */
const settledDecisions = (accepted, total) =>
  Object.fromEntries(
    Array.from({ length: total }, (_, offset) => [
      String(offset),
      offset < accepted ? 'accept' : 'release',
    ])
  )

/**
 * The acknowledgement results of the public events, by decimal offset.
 * @param {Array<{ firstOffset: string, lastOffset: string, type: string }>} events
 * @returns {Record<string, string>} the settled type by decimal offset
 */
const settledByOffset = (events) => {
  const types = {}
  for (const { firstOffset, lastOffset, type } of events) {
    for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
      types[String(offset)] = type
    }
  }
  return types
}

/**
 * One share consumer of a case: its own client, wire tap and observed facts.
 */
class ShareMember {
  /**
   * @param {ShutdownCase} fixture the case that owns it
   * @param {string} label e.g. `a`
   * @param {object} config share consumer options besides the group id
   */
  constructor(fixture, label, config) {
    this.fixture = fixture
    this.label = label
    this.tap = new ShareWireTap(label)
    this.clientId = `e2e-p9e-shutdown-${label}-${fixture.id}`
    this.config = { groupId: fixture.group, ...config }
    this.consumer = new Kafka({
      clientId: this.clientId,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.socketFactory(),
      retry: { factor: 0 },
    }).shareConsumer(this.config)
    this.memberId = null
    this.assigned = deferred()
    this.entered = deferred()
    this.delivered = []
    this.acknowledged = []
    this.requests = []
    this.problems = []
    this.queueSizes = new Map()
    this.stopped = null
    this.listen(this.consumer.events.GROUP_JOIN, ({ payload }) => {
      this.memberId = payload.memberId
    })
    this.listen(this.consumer.events.ASSIGNMENT_CHANGED, ({ payload }) => {
      if (
        payload.assignment.some(
          ({ topic, partitions }) => topic === fixture.topic && partitions.includes(0)
        )
      ) {
        this.assigned.resolve()
      }
    })
    this.listen(this.consumer.events.ACKNOWLEDGE, ({ payload }) => {
      this.acknowledged.push({ at: performance.now(), ...payload })
    })
    this.listen(this.consumer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.listen(this.consumer.events.REQUEST_QUEUE_SIZE, ({ payload }) =>
      this.queueSizes.set(payload.broker, payload.queueSize)
    )
    this.listen(this.consumer.events.STOP, () => {
      this.stopped = performance.now()
    })
    for (const name of ['CRASH', 'HANDLER_ERROR', 'LOCK_EXPIRED', 'SESSION_RESET']) {
      this.listen(this.consumer.events[name], ({ payload }) =>
        this.problems.push({ event: name, payload })
      )
    }
    fixture.cleanup.add(`disconnect ${label}`, () =>
      this.consumer.disconnect({ timeout: DISCONNECT_MS })
    )
    fixture.members.push(this)
  }

  /**
   * Registers a listener on the client and counts it for the resource probe.
   * @param {string} event
   * @param {(event: object) => void} listener
   * @returns {void}
   */
  listen(event, listener) {
    const remove = this.consumer.on(event, listener)
    this.fixture.listeners.add(event)
    this.fixture.cleanup.add(`remove ${this.label} ${event} listener`, () => {
      remove()
      this.fixture.listeners.remove(event)
    })
  }

  /**
   * Connects, subscribes, runs and waits for the first accepted assignment of partition 0.
   * @param {AbortSignal} scope
   * @param {object} handler `{ eachMessage }` or `{ eachBatch }`
   * @returns {Promise<void>}
   */
  async start(scope, handler) {
    await bounded(scope, JOIN_BUDGET_MS, () => this.consumer.connect(), {
      label: `${this.label} connect`,
    })
    await bounded(scope, JOIN_BUDGET_MS, () =>
      this.consumer.subscribe({ topics: [this.fixture.topic] })
    )
    await bounded(scope, JOIN_BUDGET_MS, () => this.consumer.run(handler), {
      label: `${this.label} run`,
    })
    await bounded(scope, JOIN_BUDGET_MS, () => this.assigned.promise, {
      label: `${this.label} assignment`,
    })
  }

  /**
   * An implicit-mode handler that records what it is given and accepts it.
   * @returns {{ eachMessage: Function }}
   */
  counting() {
    return {
      eachMessage: async ({ topic, partition, message }) => {
        this.delivered.push({
          at: performance.now(),
          identity: shareRecordIdentity({ ...message, topic, partition }),
          deliveryCount: message.deliveryCount,
        })
      },
    }
  }

  /** @returns {number} requests queued or in flight, by the latest size each broker reported */
  queueDepth() {
    return [...this.queueSizes.values()].reduce((total, size) => total + size, 0)
  }
}

/**
 * The fixture of the three shutdown cases: a fresh topic and group per case, the members, the
 * independent oracles and the strict cleanup that runs even when an assertion fails.
 */
class ShutdownCase {
  /** @param {string} name the case's short name, used in resource names */
  constructor(name) {
    this.name = name
    this.id = secureRandom()
    this.cleanup = new CleanupStack()
    this.members = []
    this.listeners = new ListenerCounter()
    this.ledgers = new LedgerWatch()
    this.group = `e2e-p9e-shutdown-${name}-group-${this.id}`
    this.topic = null
    this.expected = []
    this.facts = { oracles: {}, wireVersions: {}, config: {} }
    this.probe = createResourceProbe({
      emitters: { listeners: this.listeners },
      sockets: () => ({
        created: this.members.reduce((total, { tap }) => total + tap.created, 0),
        destroyed: this.members.reduce((total, { tap }) => total + tap.destroyed, 0),
      }),
      queueDepth: () => this.members.reduce((total, member) => total + member.queueDepth(), 0),
      ledgerSize: () => this.ledgers.size(),
      activeResources: true,
    })
  }

  /**
   * Appends one evidence row carrying the case's identity.
   * @param {string} title the case title
   * @param {string} event the row kind
   * @param {object} [fields]
   * @returns {void}
   */
  note(title, event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: title,
      profile: PROFILE,
      nonce: this.id,
      groups: { share: 'share-group' },
      budgets: { lockMs: LOCK_MS, finalAllowanceMs: FINAL_ALLOWANCE_MS },
      ...fields,
    })
  }

  /**
   * Runs `act` between setup and strict cleanup and writes the case's evidence row after both,
   * whichever way it ended.
   * @param {AbortSignal} scope the case scope
   * @param {string} title the case title
   * @param {(scope: AbortSignal) => Promise<void>} act the case's own steps
   * @returns {Promise<void>}
   * @throws {Error} the failure of the steps, or of the cleanup
   */
  async run(scope, title, act) {
    try {
      await this.cleanup.run(
        async () => {
          await this.setup(scope, title)
          await act(scope)
        },
        { budgetMs: CLEANUP_BUDGET_MS }
      )
    } catch (error) {
      this.note(title, 'case-failed', { error: String(error?.stack ?? error).slice(0, 4000) })
      throw error
    } finally {
      this.ledgers.restore()
    }
    this.note(title, 'case-complete', {
      status: 'passed',
      tainted: false,
      oracles: this.facts.oracles,
      resources: this.facts.resources,
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      wireVersions: wireVersionsOf(this.members.flatMap(({ requests }) => requests)),
      config: { lockMs: LOCK_MS, members: this.members.map(({ config }) => config) },
    })
  }

  /**
   * Registers every cleanup before anything is created, creates the topic and the group's
   * configuration, and takes the resource baseline before any client exists.
   * @param {AbortSignal} scope
   * @param {string} title
   * @returns {Promise<void>}
   */
  async setup(scope, title) {
    liveRun.assertClean()
    const kafka = new Kafka({
      clientId: `e2e-p9e-shutdown-admin-${this.id}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    this.kafka = kafka
    this.admin = kafka.admin()
    this.cleanup.add('disconnect admin', () => this.admin.disconnect())
    this.cleanup.add('prove cleanup', (signal) => this.proveCleanup(signal))
    this.cleanup.add('delete topic', (signal) => this.removeTopic(signal))
    this.cleanup.add('delete group', (signal) => this.removeGroup(signal))
    this.cleanup.add('watch ledgers', () => this.ledgers.restore())
    await bounded(scope, 10000, () => this.admin.connect())
    this.topic = await bounded(scope, 20000, () =>
      createShareTopic({ scenario: `shutdown-${this.name}` })
    )
    await bounded(scope, 10000, () =>
      this.admin.incrementalAlterConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: this.topic,
            configEntries: [
              { name: 'retention.ms', operation: ALTER_CONFIG_OP_TYPES.SET, value: '-1' },
            ],
          },
        ],
      })
    )
    const configs = {
      'share.auto.offset.reset': 'earliest',
      'share.record.lock.duration.ms': LOCK_MS,
    }
    await bounded(scope, 15000, () => setShareGroupConfigs(this.admin, this.group, configs))
    this.configKeys = Object.keys(configs)
    this.note(title, 'config-readback', { groupConfigs: configs })
    await bounded(scope, 10000, () => this.admin.listGroups())
    this.ledgers.install()
    this.baseline = await bounded(scope, 10000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.note(title, 'resource-baseline', { snapshot: this.baseline })
  }

  /**
   * @param {string} label
   * @param {object} config share consumer options besides the group id
   * @returns {ShareMember}
   */
  member(label, config) {
    return new ShareMember(this, label, {
      acquireMode: 'record_limit',
      maxWaitTimeInMs: 250,
      ...config,
    })
  }

  /**
   * Waits until the broker lists exactly these members, each holding partition 0.
   * @param {AbortSignal} scope
   * @param {string} title
   * @param {ShareMember[]} members
   * @returns {Promise<void>}
   */
  async assignmentBarrier(scope, title, members) {
    const wanted = members.map(({ memberId }) => memberId).sort()
    let seen
    await pollUntil(
      scope,
      10000,
      async () => {
        const { groups } = await this.admin.describeShareGroups([this.group], { apiTimeout: 5000 })
        seen = groups[0].members
        const held = seen.every(({ assignment }) =>
          assignment.some(({ topic, partitions }) => topic === this.topic && partitions.includes(0))
        )
        return (
          held &&
          JSON.stringify(seen.map(({ memberId }) => memberId).sort()) === JSON.stringify(wanted)
        )
      },
      { delayMs: 100, label: 'The broker member set' }
    )
    this.note(title, 'assignment', {
      members: seen.map(({ memberId, clientId, assignment }) => ({
        memberId,
        clientId,
        assignment,
      })),
    })
  }

  /**
   * Appends the corpus to partition 0 with an idempotent producer, one send, and derives the
   * expected identities from the corpus and the broker's base offset.
   * @param {AbortSignal} scope
   * @param {string} title
   * @param {number} count how many records
   * @returns {Promise<void>}
   */
  async produce(scope, title, count) {
    const producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
    let results
    try {
      await bounded(scope, 10000, () => producer.connect())
      results = await bounded(scope, 10000, () =>
        producer.send({
          topic: this.topic,
          acks: -1,
          messages: Array.from({ length: count }, (_, index) => ({
            ...corpusRecord('shutdown', index),
            partition: 0,
            timestamp: TIMESTAMP,
          })),
        })
      )
    } finally {
      await bounded(NEVER, 5000, () => producer.disconnect())
    }
    const base = BigInt(results[0].baseOffset)
    this.expected = Array.from({ length: count }, (_, index) =>
      shareRecordIdentity({
        topic: this.topic,
        partition: 0,
        offset: String(base + BigInt(index)),
        ...corpusRecord('shutdown', index),
      })
    )
    expect(String(base)).toBe('0')
    this.note(title, 'produced', { baseOffset: String(base), count })
  }

  /**
   * Reads the whole log straight from the leader and requires its data records to be the fixed
   * corpus, once. It runs after the timing-sensitive steps: reading takes long enough to matter.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async verifyLog(scope) {
    const log = await bounded(scope, 15000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const actual = log
      .get(0)
      .records.filter(({ isControlRecord }) => !isControlRecord)
      .map(({ offset, key, value }) =>
        shareRecordIdentity({ topic: this.topic, partition: 0, offset: String(offset), key, value })
      )
    expect(actual).toEqual(this.expected)
    this.facts.oracles.log = { expected: this.expected, actual }
  }

  /**
   * @param {number} first first offset, inclusive
   * @param {number} last last offset, inclusive
   * @returns {string[]} the expected identities of those offsets
   */
  identities(first, last) {
    return this.expected.slice(first, last + 1)
  }

  /**
   * Reads the broker's position for the group: where the share-partition starts and its lag.
   * @param {AbortSignal} scope
   * @returns {Promise<{ startOffset: string, lag: string|null, errorCode: number }>}
   */
  async position(scope) {
    const [{ partitions }] = await bounded(scope, 10000, () =>
      this.admin.listShareGroupOffsets({
        groupId: this.group,
        topics: [{ topic: this.topic, partitions: [0] }],
        apiTimeout: 5000,
      })
    )
    return partitions[0]
  }

  /**
   * Waits for the share-partition's start offset to reach at least `offset`.
   * @param {AbortSignal} scope
   * @param {number} offset
   * @param {number} [ms=8000]
   * @returns {Promise<void>}
   */
  async startReaches(scope, offset, ms = 8000) {
    await pollUntil(
      scope,
      ms,
      async () => {
        const { startOffset, errorCode } = await this.position(scope)
        return errorCode === 0 && BigInt(startOffset) >= BigInt(offset)
      },
      { delayMs: 100, label: `The share start offset ${offset}` }
    )
  }

  /**
   * Records the broker's terminal position, requires start equal to the log end and a known lag
   * of zero, then proves a fresh member of the same group receives nothing.
   * @param {AbortSignal} scope
   * @param {string} title
   * @param {number} end the expected log end
   * @returns {Promise<void>}
   */
  async quiescent(scope, title, end) {
    await this.verifyLog(scope)
    await this.startReaches(scope, end)
    const [logEnd] = [
      ...(
        await bounded(scope, 10000, () =>
          endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
        )
      ).values(),
    ]
    const { startOffset, lag, errorCode } = await this.position(scope)
    expect({ errorCode, startOffset, lag, logEnd }).toEqual({
      errorCode: 0,
      startOffset: String(end),
      lag: '0',
      logEnd: String(end),
    })
    this.facts.oracles.broker = [
      { groupId: this.group, topic: this.topic, partition: 0, errorCode, startOffset, logEnd, lag },
    ]
    this.note(title, 'broker-position', { startOffset, lag, logEnd })
    const received = await verifierMember(this.group, this.topic, {
      signal: scope,
      windowMs: QUIET_WINDOW_MS,
    })
    expect(received).toEqual([])
  }

  /**
   * Compares the handlers' combined entries with the expected multiset and records both.
   * @param {string[]} expected identities with their multiplicity, in any order
   * @returns {void}
   */
  handlerMultiset(expected) {
    const actual = this.members
      .flatMap(({ delivered }) => delivered.map(({ identity }) => identity))
      .sort()
    expect(actual).toEqual([...expected].sort())
    this.facts.oracles.handler = { expected: [...expected].sort(), actual }
  }

  /**
   * Proves owned resources are gone and the clients' resources are back to the baseline.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async proveCleanup(signal) {
    if (!this.baseline) return
    this.facts.resources = {
      before: this.baseline,
      after: await assertShareCleanup(
        this.admin,
        [this.group],
        [this.topic],
        this.baseline,
        this.probe,
        {
          signal,
        }
      ),
    }
  }

  /**
   * Deletes the topic and waits for it to be gone.
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   */
  async removeTopic(signal) {
    if (!this.topic) return
    await this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 })
    await pollUntil(
      signal,
      6000,
      async () => !(await this.admin.listTopics()).includes(this.topic),
      {
        delayMs: 100,
        label: 'The topic deletion',
      }
    )
  }

  /**
   * Deletes the group, retrying while it still counts a member, removes the group configs this
   * case set and waits until the group is gone and no owned override remains.
   * @param {AbortSignal} signal
   * @returns {Promise<void>}
   */
  async removeGroup(signal) {
    const listed = async () =>
      (await this.admin.listGroups()).groups.some(({ groupId }) => groupId === this.group)
    await pollUntil(
      signal,
      8000,
      async () => {
        try {
          await this.admin.deleteGroups([this.group])
        } catch (error) {
          const [item] = error.groups ?? []
          if (item?.errorCode === 69) return !(await listed())
          if (item?.errorCode === 68) return false
          throw error
        }
        return !(await listed())
      },
      { delayMs: 200, label: 'The group deletion' }
    )
    if (!this.configKeys) return
    const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: this.group }
    await this.admin.incrementalAlterConfigs({
      resources: [
        {
          ...resource,
          configEntries: this.configKeys.map((name) => ({
            name,
            operation: ALTER_CONFIG_OP_TYPES.DELETE,
          })),
        },
      ],
    })
    const { resources } = await this.admin.describeConfigs({
      resources: [{ ...resource, configNames: this.configKeys }],
    })
    const left = (resources[0]?.configEntries ?? []).filter(
      ({ configSource }) => configSource === CONFIG_SOURCE.GROUP_CONFIG
    )
    if (left.length > 0)
      throw new Error(`Group config override remains: ${left.map((e) => e.configName)}`)
  }

  /**
   * Requires that no member crashed, and records every other event the clients reported.
   * @param {string} title
   * @param {ShareMember[]} members
   * @returns {void}
   */
  noCrash(title, members) {
    const reported = members.map(({ label, problems }) => ({ label, problems }))
    this.note(title, 'fault', { reported })
    expect(
      members.flatMap(({ problems }) => problems.filter(({ event }) => event === 'CRASH'))
    ).toEqual([])
  }

  /**
   * The identities one batch handler was given, with a record of when and how often.
   * @param {ShareMember} member
   * @param {object} batch the handler's batch
   * @returns {void}
   */
  enter(member, batch) {
    for (const message of batch.messages) {
      member.delivered.push({
        at: performance.now(),
        identity: shareRecordIdentity({
          ...message,
          topic: batch.topic,
          partition: batch.partition,
        }),
        deliveryCount: message.deliveryCount,
      })
    }
  }

  /**
   * The write time of the fetch that gave `member` its acquisition: the last fetch frame written
   * before its handler was entered.
   * @param {ShareMember} member
   * @param {number} enteredAt `performance.now()` when its handler began
   * @returns {number}
   */
  acquisitionPrewrite(member, enteredAt) {
    const fetches = member.tap.frames.filter(
      ({ at, apiKey }) => apiKey === API.fetch && at <= enteredAt
    )
    expect(fetches.length).toBeGreaterThan(0)
    return fetches[fetches.length - 1].at
  }

  /**
   * Starts the explicit-mode member that acquires `count` records, decides the first `decided` of
   * them and then holds until released, so it is blocked when its shutdown begins.
   * @param {ShareMember} a
   * @param {number} decided how many leading messages it accepts before blocking
   * @param {{ promise: Promise<void> }} gate what releases the blocked handler
   * @param {(payload: object) => Promise<void>} [after] runs once released, with the batch payload
   * @returns {object} the `run` config
   */
  blockingHandler(a, decided, gate, after) {
    return {
      eachBatch: async (payload) => {
        this.enter(a, payload.batch)
        a.enteredAt = performance.now()
        a.batchSize = payload.batch.messages.length
        for (const message of payload.batch.messages.slice(0, decided)) {
          payload.acknowledge(message, 'accept')
        }
        a.processed = payload.batch.messages.slice(0, decided).map(({ offset }) => offset)
        a.entered.resolve()
        await gate.promise
        await after?.(payload)
      },
    }
  }

  /**
   * Forced disconnect: after four accepted decisions the holder is shut down with a 5 s bound and
   * the product must send one captured final (four accepts, six releases) and then leave.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async forced(scope) {
    const title = 'sends four decisions and releases six held records before lock expiry'
    const gate = deferred()
    const a = this.member('a', { acknowledgementMode: 'explicit', maxRecords: 10 })
    const b = this.member('b', { acknowledgementMode: 'implicit', maxRecords: 100 })
    this.cleanup.add('release held handler', () => gate.resolve())
    this.cleanup.add('release held fetch', () => b.tap.release())
    b.tap.holdFirstFetch()
    await a.start(
      scope,
      this.blockingHandler(a, 4, gate, async ({ batch, acknowledge }) => {
        try {
          acknowledge(batch.messages[4], 'accept')
        } catch (error) {
          a.lateError = error
          throw error
        }
      })
    )
    await b.start(scope, b.counting())
    await this.assignmentBarrier(scope, title, [a, b])
    await this.produce(scope, title, 10)
    await bounded(scope, 8000, () => a.entered.promise, { label: 'The holder acquisition' })
    const prewrite = this.acquisitionPrewrite(a, a.enteredAt)
    expect(a.consumer.acquisitionLockTimeoutMs()).toBe(LOCK_MS)
    b.tap.release()

    const aFrames = a.tap.frames.length
    const calledAt = performance.now()
    if (calledAt - prewrite > 1000) {
      throw new Error(`The fixture took ${calledAt - prewrite} ms from acquisition to disconnect`)
    }
    const closing = a.consumer.disconnect({ timeout: DISCONNECT_MS })
    await bounded(scope, DISCONNECT_MS + 2000, () => closing, { label: 'The forced disconnect' })
    const settledAt = performance.now()

    const [final, ...extra] = a.tap
      .shareFrames(aFrames)
      .filter(
        ({ apiKey, fields }) => apiKey === API.acknowledge && fields?.shareSessionEpoch === -1
      )
    expect(final).toBeDefined()
    expect(extra).toEqual([])
    expect(final.decodeError).toBeUndefined()
    expect(typesByOffset(final.fields)).toEqual(wireDecisions(4, 10))
    const leave = a.tap
      .shareFrames(aFrames)
      .find(({ apiKey, fields }) => apiKey === API.heartbeat && fields?.memberEpoch === -1)
    expect(leave).toBeDefined()
    expect(leave.at).toBeGreaterThanOrEqual(final.at)
    expect(final.at - calledAt).toBeLessThanOrEqual(DISCONNECT_MS)
    expect(settledAt - calledAt).toBeLessThanOrEqual(DISCONNECT_MS + 2000)

    const settled = a.acknowledged.filter(({ type }) => type === 'accept' || type === 'release')
    expect(settled.every(({ error }) => error === null)).toBe(true)
    expect(settledByOffset(settled)).toEqual(settledDecisions(4, 10))
    const resultAt = Math.max(...settled.map(({ at }) => at))
    expect(resultAt - prewrite).toBeLessThanOrEqual(FINAL_ALLOWANCE_MS)
    this.note(title, 'frame', {
      prewriteToCall: calledAt - prewrite,
      callToFinalWrite: final.at - calledAt,
      finalWriteToResult: resultAt - final.at,
      callToSettled: settledAt - calledAt,
      finalDecisions: wireDecisions(4, 10),
      leaveAfterFinal: leave.at - final.at,
    })
    this.note(title, 'acknowledgement', { events: settled })

    await this.startReaches(scope, 4)
    await pollUntil(scope, RELEASE_ALLOWANCE_MS + 2000, () => b.delivered.length >= 6, {
      delayMs: 50,
      label: 'The released records',
    })
    expect(b.delivered.map(({ identity }) => identity)).toEqual(this.identities(4, 9))
    expect(b.delivered.every(({ deliveryCount }) => deliveryCount === 2)).toBe(true)
    expect(Math.max(...b.delivered.map(({ at }) => at)) - final.at).toBeLessThanOrEqual(
      RELEASE_ALLOWANCE_MS
    )
    this.note(title, 'fault', {
      releaseLatenciesAfterFinalWrite: b.delivered.map(({ at }) => at - final.at),
    })

    const framesBefore = a.tap.frames.length
    const acksBefore = a.acknowledged.length
    gate.resolve()
    await pollUntil(scope, 3000, () => a.lateError !== undefined, {
      delayMs: 25,
      label: 'The late callback',
    })
    expect(a.lateError).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(a.lateError.retriable).toBe(false)
    await delay(scope, 250)
    expect(a.tap.frames.length).toBe(framesBefore)
    expect(a.acknowledged.length).toBe(acksBefore)
    this.noCrash(title, [a, b])

    await bounded(scope, DISCONNECT_MS + 2000, () =>
      b.consumer.disconnect({ timeout: DISCONNECT_MS })
    )
    this.handlerMultiset([...this.identities(0, 9), ...this.identities(4, 9)])
    expect(a.delivered.map(({ deliveryCount }) => deliveryCount)).toEqual(Array(10).fill(1))
    await this.quiescent(scope, title, 10)
  }

  /**
   * Timeout zero: the holder's disconnect is the whole of its shutdown, and it writes no share
   * frame after the call, while the other member receives the released records.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async zero(scope) {
    const title = 'writes no share frame after a fresh timeout zero call'
    const gate = deferred()
    const a = this.member('a', { acknowledgementMode: 'explicit', maxRecords: 10 })
    const b = this.member('b', { acknowledgementMode: 'implicit', maxRecords: 100 })
    this.cleanup.add('release held handler', () => gate.resolve())
    this.cleanup.add('release held fetch', () => b.tap.release())
    b.tap.holdFirstFetch()
    await a.start(
      scope,
      this.blockingHandler(a, 0, gate, async ({ batch, acknowledge }) => {
        try {
          acknowledge(batch.messages[0], 'accept')
        } catch (error) {
          a.lateError = error
          throw error
        }
      })
    )
    await b.start(scope, b.counting())
    await this.assignmentBarrier(scope, title, [a, b])
    await this.produce(scope, title, 10)
    await bounded(scope, 8000, () => a.entered.promise, { label: 'The holder acquisition' })
    const prewrite = this.acquisitionPrewrite(a, a.enteredAt)
    expect(a.consumer.acquisitionLockTimeoutMs()).toBe(LOCK_MS)
    b.tap.release()

    const mark = a.tap.frames.length
    const calledAt = performance.now()
    const closing = a.consumer.disconnect({ timeout: 0 })
    expect(a.tap.shareFrames(mark)).toEqual([])
    await bounded(scope, 5000, () => closing, { label: 'The timeout zero disconnect' })
    await pollUntil(scope, 5000, () => a.tap.destroyed === a.tap.created, {
      delayMs: 50,
      label: 'The holder sockets',
    })
    expect(a.tap.shareFrames(mark)).toEqual([])

    await pollUntil(scope, LOCK_MS + 5000, () => b.delivered.length >= 10, {
      delayMs: 100,
      label: 'The released records',
    })
    const lockDeadline = prewrite + LOCK_MS
    expect(b.delivered.map(({ identity }) => identity)).toEqual(this.identities(0, 9))
    expect(b.delivered.every(({ deliveryCount }) => deliveryCount === 2)).toBe(true)
    expect(Math.max(...b.delivered.map(({ at }) => at))).toBeLessThanOrEqual(lockDeadline + 2000)
    this.note(title, 'fault', {
      callToFirstRelease: b.delivered[0].at - calledAt,
      releaseLatenciesAfterCall: b.delivered.map(({ at }) => at - calledAt),
      lockDeadlineMargin: b.delivered.map(({ at }) => lockDeadline - at),
    })

    gate.resolve()
    await pollUntil(scope, 3000, () => a.lateError !== undefined, {
      delayMs: 25,
      label: 'The late callback',
    })
    expect(a.lateError).toBeInstanceOf(KafkaJSNonRetriableError)
    await delay(scope, 250)
    expect(a.tap.shareFrames(mark)).toEqual([])
    this.noCrash(title, [a, b])

    await bounded(scope, DISCONNECT_MS + 2000, () =>
      b.consumer.disconnect({ timeout: DISCONNECT_MS })
    )
    this.handlerMultiset([...this.identities(0, 9), ...this.identities(0, 9)])
    await this.quiescent(scope, title, 10)
  }

  /**
   * A clean stop in explicit mode: the application stops after four decisions, and the
   * unstarted records go to the other member exactly once.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async stop(scope) {
    const title = 'stops cleanly and releases unstarted records'
    const gate = deferred()
    const a = this.member('a', { acknowledgementMode: 'explicit', maxRecords: 50 })
    const b = this.member('b', { acknowledgementMode: 'implicit', maxRecords: 100 })
    let callbacks = 0
    this.cleanup.add('release held handler', () => gate.resolve())
    this.cleanup.add('release held fetch', () => b.tap.release())
    b.tap.holdFirstFetch()
    const blocking = this.blockingHandler(a, 4, gate)
    await a.start(scope, {
      eachBatch: async (payload) => {
        callbacks += 1
        await blocking.eachBatch(payload)
        while (payload.isRunning()) await delay(scope, 10)
        a.sawStop = true
        a.handlerDoneAt = performance.now()
      },
    })
    await b.start(scope, b.counting())
    await this.assignmentBarrier(scope, title, [a, b])
    await this.produce(scope, title, 50)
    await bounded(scope, 8000, () => a.entered.promise, { label: 'The holder acquisition' })
    b.tap.release()

    const aFrames = a.tap.frames.length
    const stopping = a.consumer.stop()
    gate.resolve()
    await bounded(scope, 10000, () => stopping, { label: 'The stop' })
    expect(a.sawStop).toBe(true)
    expect(a.stopped).not.toBeNull()
    const own = a.tap.shareFrames(aFrames)
    const final = own.find(
      ({ apiKey, fields }) => apiKey === API.acknowledge && fields?.shareSessionEpoch === -1
    )
    expect(final).toBeDefined()
    expect(typesByOffset(final.fields)).toEqual(wireDecisions(4, 50))
    const leave = own.find(
      ({ apiKey, fields }) => apiKey === API.heartbeat && fields?.memberEpoch === -1
    )
    expect(leave).toBeDefined()
    expect(a.handlerDoneAt).toBeLessThanOrEqual(final.at)
    expect(final.at).toBeLessThanOrEqual(leave.at)
    expect(leave.at).toBeLessThanOrEqual(a.stopped)
    expect(own.filter(({ apiKey, at }) => apiKey === API.fetch && at > a.handlerDoneAt)).toEqual([])
    expect(a.consumer.subscription()).toEqual([this.topic])
    expect(a.consumer.assignment()).toEqual([])
    this.note(title, 'frame', {
      callbackCount: callbacks,
      batchIdentities: a.delivered.map(({ identity }) => identity),
      processedOffsets: a.processed,
      handlerDoneToFinal: final.at - a.handlerDoneAt,
      finalToLeave: leave.at - final.at,
      leaveToStop: a.stopped - leave.at,
    })
    expect(callbacks).toBe(1)
    expect(a.batchSize).toBe(50)

    await pollUntil(scope, RELEASE_ALLOWANCE_MS + 2000, () => b.delivered.length >= 46, {
      delayMs: 50,
      label: 'The released records',
    })
    expect(b.delivered.map(({ identity }) => identity)).toEqual(this.identities(4, 49))
    expect(b.delivered.every(({ deliveryCount }) => deliveryCount === 2)).toBe(true)

    const framesBefore = a.tap.frames.length
    await a.consumer.disconnect()
    await a.consumer.disconnect()
    expect(a.tap.frames.length).toBe(framesBefore)
    expect(
      a.tap.shareFrames(aFrames).filter(({ fields }) => fields?.shareSessionEpoch === -1)
    ).toHaveLength(1)
    this.noCrash(title, [a, b])

    await bounded(scope, DISCONNECT_MS + 2000, () =>
      b.consumer.disconnect({ timeout: DISCONNECT_MS })
    )
    this.handlerMultiset([...this.identities(0, 49), ...this.identities(4, 49)])
    await this.quiescent(scope, title, 50)
  }
}

describeShutdown('Share consumer > shutdown on a live group', () => {
  /**
   * Runs one case under its own fixture, whatever happens to it.
   * @param {string} name resource-name part, e.g. `forced`
   * @param {string} method the `ShutdownCase` method that holds the steps
   * @param {string} title the case title
   * @returns {(scope: AbortSignal) => Promise<void>}
   */
  const shutdownCase = (name, method, title) => async (scope) => {
    const fixture = new ShutdownCase(name)
    await fixture.run(scope, title, (inner) => fixture[method](inner))
  }

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
    'sends four decisions and releases six held records before lock expiry',
    liveCase(
      FORCED_BUDGET_MS,
      shutdownCase(
        'forced',
        'forced',
        'sends four decisions and releases six held records before lock expiry'
      )
    ),
    caseTimeout(FORCED_BUDGET_MS)
  )

  it(
    'writes no share frame after a fresh timeout zero call',
    liveCase(
      ZERO_BUDGET_MS,
      shutdownCase('zero', 'zero', 'writes no share frame after a fresh timeout zero call')
    ),
    caseTimeout(ZERO_BUDGET_MS)
  )

  it(
    'stops cleanly and releases unstarted records',
    liveCase(
      STOP_BUDGET_MS,
      shutdownCase('stop', 'stop', 'stops cleanly and releases unstarted records')
    ),
    caseTimeout(STOP_BUDGET_MS)
  )
})

describe('Share consumer > shutdown wire tap framing', () => {
  /**
   * @param {number} apiKey
   * @param {number} correlationId
   * @returns {Buffer} a request frame: header version 2, an empty tag buffer and no body
   */
  const frame = (apiKey, correlationId) => {
    const clientId = Buffer.from('tap')
    const bytes = Buffer.alloc(4 + 2 + 2 + 4 + 2 + clientId.length + 1)
    bytes.writeInt32BE(bytes.length - 4, 0)
    bytes.writeInt16BE(apiKey, 4)
    bytes.writeInt16BE(apiKey === API.heartbeat ? 1 : 2, 6)
    bytes.writeInt32BE(correlationId, 8)
    bytes.writeInt16BE(clientId.length, 12)
    clientId.copy(bytes, 14)
    return bytes
  }
  /** @returns {{ socket: EventEmitter & object, tap: ShareWireTap, writes: Buffer[] }} */
  const tapped = () => {
    const writes = []
    const socket = Object.assign(new EventEmitter(), {
      destroyed: false,
      write: (chunk) => {
        writes.push(chunk)
        return true
      },
    })
    const tap = new ShareWireTap('probe')
    tap.wrap(socket)
    return { socket, tap, writes }
  }

  it('reads a frame split across writes and two frames in one write', () => {
    const { socket, tap, writes } = tapped()
    const first = frame(API.heartbeat, 1)
    socket.write(first.subarray(0, 5))
    socket.write(first.subarray(5))
    socket.write(Buffer.concat([frame(API.acknowledge, 2), frame(API.fetch, 3)]))

    expect(tap.frames.map(({ apiKey, correlationId }) => [apiKey, correlationId])).toEqual([
      [API.heartbeat, 1],
      [API.acknowledge, 2],
      [API.fetch, 3],
    ])
    expect(Buffer.concat(writes)).toEqual(
      Buffer.concat([first, frame(API.acknowledge, 2), frame(API.fetch, 3)])
    )
  })

  it('holds the first fetch and what follows it, then writes them in order', () => {
    const { socket, tap, writes } = tapped()
    tap.holdFirstFetch()
    socket.write(frame(API.heartbeat, 1))
    socket.write(frame(API.fetch, 2))
    socket.write(frame(API.acknowledge, 3))

    expect(writes).toHaveLength(1)
    expect(tap.release()).toBe(2)
    expect(writes.map((chunk) => chunk.readInt32BE(8))).toEqual([1, 2, 3])
    expect(tap.shareFrames().map(({ correlationId }) => correlationId)).toEqual([1, 2, 3])
    socket.write(frame(API.fetch, 4))
    expect(writes).toHaveLength(4)
  })

  it('counts sockets by their close', () => {
    const { socket, tap } = tapped()

    expect([tap.created, tap.destroyed]).toEqual([1, 0])
    socket.emit('close')
    expect([tap.created, tap.destroyed]).toEqual([1, 1])
  })
})
