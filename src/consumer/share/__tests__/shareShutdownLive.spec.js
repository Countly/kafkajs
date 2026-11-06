const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const {
  acknowledgementResults,
  acknowledgementRequests,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  pollSharePosition,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const { EventEmitter } = require('events')
const {
  Kafka,
  KafkaJSNonRetriableError,
  KafkaJSShareAcknowledgementError,
  logLevel,
} = require('../../../../index')
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
  captureShareCleanupBaseline,
  createShareTopic,
  setShareGroupConfigs,
  shareRecordIdentity,
} = require('../../../../testHelpers/shareFixtures')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')
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

/** Records original acknowledgement requests with their same-socket broker responses. */
class AcknowledgementTrace {
  /** @param {Function} [record] Receives completed request/response facts. */
  constructor(record = () => {}) {
    this.frames = []
    this.sockets = 0
    this.record = record
  }

  /**
   * Observes bytes without changing write results, callbacks or ordering.
   * @param {object} socket Original socket.
   * @param {string} label Owning member.
   * @returns {object} The same socket.
   */
  wrap(socket, label) {
    const pending = new Map()
    const ordinal = ++this.sockets
    const buffers = { request: Buffer.alloc(0), response: Buffer.alloc(0) }
    /** @param {string} kind Stream direction. @param {Buffer} chunk Original bytes. @returns {Buffer[]} Complete frames. */
    const split = (kind, chunk) => {
      let bytes = Buffer.concat([buffers[kind], chunk])
      const frames = []
      while (bytes.length >= 4 && bytes.length >= bytes.readInt32BE(0) + 4) {
        const size = bytes.readInt32BE(0) + 4
        if (size < 4) throw new Error('Invalid Kafka diagnostic frame size')
        frames.push(Buffer.from(bytes.subarray(0, size)))
        bytes = bytes.subarray(size)
      }
      buffers[kind] = bytes
      return frames
    }
    const write = socket.write.bind(socket)
    socket.write = (chunk, ...rest) => {
      for (const bytes of split('request', Buffer.from(chunk))) {
        const header = readRequestHeader(bytes)
        if (![78, 79].includes(header.apiKey)) continue
        const fact = {
          ...header,
          label,
          socket: ordinal,
          bytes,
          headerBytes: header.byteLength,
          at: performance.now(),
        }
        const [request] = acknowledgementRequests([fact])
        if (!request) continue
        fact.request = request
        this.frames.push(fact)
        pending.set(header.correlationId, fact)
      }
      return write(chunk, ...rest)
    }
    socket.on('data', (chunk) => {
      for (const bytes of split('response', chunk)) {
        const fact = pending.get(bytes.readInt32BE(4))
        if (!fact) continue
        pending.delete(fact.correlationId)
        const decoder = fact.apiKey === 78 ? fetchAcknowledgementResponse : acknowledgeResponse
        fact.decoding = Promise.resolve()
          .then(() => {
            if (bytes[8] !== 0) throw new Error('Unexpected response header tags')
            return decoder.decode(bytes.subarray(9))
          })
          .then(({ errorCode, responses }) => {
            fact.response = {
              errorCode,
              at: performance.now(),
              topics: responses.map(({ topicId, partitions }) => ({
                topicId,
                partitions: partitions.map(
                  ({ partitionIndex, errorCode, acknowledgeErrorCode, currentLeader }) => ({
                    partitionIndex,
                    errorCode,
                    acknowledgeErrorCode,
                    currentLeader,
                  })
                ),
              })),
            }
          })
          .then(() => this.record({ ...fact.request, at: fact.at, response: fact.response }))
          .catch((error) => {
            fact.decodeError = describeError(error)
          })
      }
    })
    return socket
  }

  /** @returns {Promise<void>} Joins every response decoder before its owning clients leave. */
  async join() {
    await Promise.all(this.frames.map(({ decoding }) => decoding))
  }

  /** @returns {object[]} Original requests and correlated partition outcomes, without byte buffers. */
  snapshot() {
    return this.frames.map(({ request, at, response, decodeError }) => ({
      ...request,
      at,
      response: response ?? null,
      decodeError,
    }))
  }

  /**
   * Returns only offsets whose original requested decision succeeded at group and partition level.
   * @param {string} group Group id.
   * @param {string} topicId Original topic UUID.
   * @param {number} partition Partition index.
   * @param {number} [type=1] Required wire acknowledgement type.
   * @returns {Set<string>} Confirmed decimal offsets.
   */
  confirmed(group, topicId, partition, type = 1) {
    const offsets = new Set()
    for (const { request, response } of this.frames) {
      if (request.groupId !== group || request.isRenewAck || response?.errorCode !== 0) continue
      const replies = response.topics
        .filter((topic) => topic.topicId === topicId)
        .flatMap((topic) => topic.partitions)
        .filter((row) => row.partitionIndex === partition)
      if (replies.length !== 1) continue
      const code = request.apiKey === 78 ? replies[0].acknowledgeErrorCode : replies[0].errorCode
      if (code !== 0) continue
      for (const topic of request.topics ?? []) {
        if (topic.topicId !== topicId) continue
        for (const row of topic.partitions) {
          if (row.partitionIndex !== partition) continue
          for (const { firstOffset, lastOffset, acknowledgeTypes } of row.acknowledgementBatches) {
            for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
              if (
                (acknowledgeTypes.length === 1
                  ? acknowledgeTypes[0]
                  : acknowledgeTypes[Number(offset - BigInt(firstOffset))]) === type
              )
                offsets.add(String(offset))
            }
          }
        }
      }
    }
    return offsets
  }
}

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
    this.acknowledgements = new AcknowledgementTrace()
  }

  /**
   * @returns {Function} a socket factory for the `Kafka` client's `socketFactory` option
   */
  socketFactory() {
    const inner = defaultSocketFactory()
    return (options) => this.wrap(this.acknowledgements.wrap(inner(options), this.label))
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
    this.handled = 0
    this.acknowledged = []
    this.fetches = []
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
    this.listen(this.consumer.events.FETCH, ({ payload }) =>
      this.fetches.push({ at: performance.now(), ...payload })
    )
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
        this.handled++
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
      groups: { share: this.group },
      wireVersions: wireVersionsOf(this.members.flatMap(({ requests }) => requests)),
      budgets: { lockMs: LOCK_MS, finalAllowanceMs: FINAL_ALLOWANCE_MS },
      ...fields,
    })
  }

  /** @returns {object} Delivered and handled counts, public results, sessions and wire decisions. */
  progress() {
    return {
      members: this.members.map(
        ({
          label,
          memberId,
          delivered,
          handled,
          processed,
          acknowledged,
          fetches,
          problems,
          tap,
        }) => ({
          label,
          memberId,
          delivered: delivered.length,
          handled,
          processed,
          fetches,
          identities: delivered,
          acknowledgements: acknowledgementResults(acknowledged),
          problems,
          shareRequests: tap.shareFrames(),
          wireAcknowledgements: tap.acknowledgements.snapshot(),
        })
      ),
      position: this.lastPosition,
    }
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
          try {
            await this.setup(scope, title)
            await act(scope)
          } catch (error) {
            this.note(title, 'work-failed', { error: describeError(error), ...this.progress() })
            throw error
          }
        },
        { budgetMs: CLEANUP_BUDGET_MS }
      )
    } catch (error) {
      this.note(title, 'case-failed', {
        error: String(error?.stack ?? error).slice(0, 4000),
        ...this.progress(),
      })
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
    this.baseline = await captureShareCleanupBaseline(this.probe, { signal: scope })
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
        this.topicId = seen
          .flatMap(({ assignment }) => assignment)
          .find(({ topic }) => topic === this.topic)?.topicId
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
    expect([...log.keys()]).toEqual([0])
    expect(log.get(0).endOffset).toBe(String(this.expected.length))
    expect(log.get(0).records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    this.logEnd = log.get(0).endOffset
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
   * Checks public decisions against their successful original request and partition response.
   * @param {ShareMember} member Member whose results are being checked.
   * @param {Record<string, string>} expected Exact required offset decisions.
   * @returns {boolean} Whether every expected decision succeeded.
   */
  decisionsSucceeded(member, expected) {
    const publicResults = settledByOffset(member.acknowledged.filter(({ error }) => error === null))
    return Object.entries(expected).every(
      ([offset, type]) =>
        publicResults[offset] === type &&
        member.tap.acknowledgements
          .confirmed(this.group, this.topicId, 0, ACK_TYPES[type])
          .has(offset)
    )
  }

  /**
   * Checks each original held offset once, before any range projection can hide duplicates.
   * @param {ShareMember} member Holder of the original ten-record acquisition.
   * @returns {boolean} Whether four Accepts succeeded and six undecided records lost authority.
   */
  forcedOutcomesMatch(member) {
    const outcomes = Array.from({ length: 10 }, () => [])
    for (const event of member.acknowledged) {
      const { topic, partition, firstOffset, lastOffset } = event
      if (topic !== this.topic || partition !== 0) return false
      const first = BigInt(firstOffset)
      const last = BigInt(lastOffset)
      if (first < 0n || last > 9n || first > last) return false
      for (let offset = first; offset <= last; offset++) outcomes[Number(offset)].push(event)
    }
    return outcomes.every((events, offset) => {
      if (events.length !== 1) return false
      const { type, error } = events[0]
      return offset < 4
        ? type === 'accept' && error === null
        : type === 'release' &&
            error instanceof KafkaJSShareAcknowledgementError &&
            error.reason === 'membership-lost' &&
            error.retriable === false &&
            error.topic === this.topic &&
            error.partition === 0
    })
  }

  /**
   * Observes the accepted prefix and released suffix while B continues fetching.
   * @param {AbortSignal} scope Case scope.
   * @param {string} title Case title.
   * @param {ShareMember} a Original holder.
   * @param {ShareMember} b Redelivery member.
   * @returns {Promise<void>} After both listings agree with actual successful suffix progress.
   */
  async prefixSettled(scope, title, a, b) {
    await this.verifyLog(scope)
    /** @returns {string[]} Suffix offsets with both public and wire-confirmed Accepts. */
    const accepted = () =>
      Object.keys(
        settledByOffset(
          b.acknowledged.filter(({ type, error }) => type === 'accept' && error === null)
        )
      ).filter(
        (offset) =>
          BigInt(offset) >= 4n &&
          BigInt(offset) < 10n &&
          b.tap.acknowledgements.confirmed(this.group, this.topicId, 0).has(offset)
      )
    await pollSharePosition(
      scope,
      8000,
      async (poll) => {
        const before = accepted()
        const named = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({
            groupId: this.group,
            topics: [{ topic: this.topic, partitions: [0] }],
            apiTimeout: 5000,
          })
        )
        const all = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({ groupId: this.group, apiTimeout: 5000 })
        )
        const after = accepted()
        /** @param {object[]} listing Actual broker listing. @returns {object[]} Owned partition rows. */
        const pick = (listing) =>
          listing
            .filter(({ topic, topicId }) => topic === this.topic && topicId === this.topicId)
            .flatMap(({ partitions }) => partitions)
            .filter(({ partition }) => partition === 0)
        const [left, right] = [pick(named), pick(all)]
        const row = left[0]
        this.lastPosition = {
          named,
          all,
          logEnd: this.logEnd,
          acceptedBefore: before,
          acceptedAfter: after,
        }
        /** @param {unknown} value Broker offset or lag. @returns {boolean} Whether it is a canonical decimal string. */
        const canonical = (value) => typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value)
        return (
          this.forcedOutcomesMatch(a) &&
          this.decisionsSucceeded(a, settledDecisions(4, 4)) &&
          left.length === 1 &&
          right.length === 1 &&
          ['startOffset', 'lag', 'errorCode', 'leaderEpoch'].every(
            (key) => row[key] === right[0][key]
          ) &&
          row.errorCode === 0 &&
          canonical(row.startOffset) &&
          BigInt(row.startOffset) <= 10n &&
          canonical(row.lag) &&
          BigInt(row.lag) >= BigInt(6 - after.length) &&
          BigInt(row.lag) <= BigInt(6 - before.length)
        )
      },
      {
        delayMs: 100,
        label: 'The accepted prefix and released suffix',
        snapshot: () => this.lastPosition,
        record: (fields) => this.note(title, 'broker-position', { ...fields, ...this.progress() }),
      }
    )
  }

  /**
   * Requires complete successful settlement, terminal listings and a quiet same-group verifier.
   * @param {AbortSignal} scope Case scope.
   * @param {string} title Case title.
   * @param {number} end Fixed corpus end.
   * @returns {Promise<void>} After final verification, before state deletion.
   */
  async quiescent(scope, title, end) {
    for (const { consumer } of this.members) {
      await bounded(scope, DISCONNECT_MS + 2000, () =>
        consumer.disconnect({ timeout: DISCONNECT_MS })
      )
    }
    await Promise.all(this.members.map(({ tap }) => tap.acknowledgements.join()))
    await this.verifyLog(scope)
    expect(this.logEnd).toBe(String(end))
    const [a, b] = this.members
    const prefix = this.name === 'zero' ? 0 : 4
    if (this.name === 'forced') {
      expect(this.forcedOutcomesMatch(a)).toBe(true)
      expect(this.decisionsSucceeded(a, settledDecisions(4, 4))).toBe(true)
    } else if (this.name === 'stop') {
      expect(this.decisionsSucceeded(a, settledDecisions(4, 50))).toBe(true)
    } else {
      expect(a.acknowledged.filter(({ error }) => error === null)).toEqual([])
    }
    const suffix = Object.fromEntries(
      Array.from({ length: end - prefix }, (_, n) => [String(n + prefix), 'accept'])
    )
    expect(this.decisionsSucceeded(b, suffix)).toBe(true)
    const partitions = [{ partition: 0, floor: '0', end: this.logEnd }]
    await awaitTerminalSharePosition(
      this.admin,
      { groupId: this.group, topic: this.topic, partitions },
      {
        signal: scope,
        timeoutMs: 8000,
        record: (fields) => this.note(title, 'broker-position', { ...fields, ...this.progress() }),
      }
    )
    const quiet = await assertQuietTerminalGroup({
      kafka: this.kafka,
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions,
      signal: scope,
      windowMs: QUIET_WINDOW_MS,
      timeoutMs: 8000,
      record: (fields) => this.note(title, 'quiet-verifier', fields),
    })
    this.facts.oracles.broker = quiet.position.rows
    this.facts.oracles.quiet = quiet
    this.facts.oracles.acknowledgements = this.progress()
    const ends = await bounded(scope, 10000, () =>
      endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect(ends.get(0)).toBe(this.logEnd)
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
          disconnectAdmin: true,
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
   * Confirms the holder actually acquired the complete fixed batch before shutdown starts.
   * @param {string} title Case title.
   * @param {ShareMember} member Original holder.
   * @param {number} count Required acquisition size.
   * @returns {void}
   */
  checkAcquisition(title, member, count) {
    this.note(title, 'acquisition', { batchSize: member.batchSize, ...this.progress() })
    expect(member.batchSize).toBe(count)
    expect(member.delivered.map(({ identity }) => identity)).toEqual(this.identities(0, count - 1))
    expect(member.delivered.map(({ deliveryCount }) => deliveryCount)).toEqual(Array(count).fill(1))
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
        a.handled = decided
      },
    }
  }

  /**
   * Forced disconnect: after four accepted decisions the holder is shut down with a 5 s bound and
   * the product must send one captured final with four Accepts and then leave. The six undecided
   * records settle locally as release/membership-lost; B must independently acquire them again.
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
    this.checkAcquisition(title, a, 10)
    b.tap.release()

    const aFrames = a.tap.frames.length
    const originalMemberId = a.memberId
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
    expect(final.fields).toMatchObject({
      groupId: this.group,
      memberId: originalMemberId,
      isRenewAck: false,
    })
    expect(final.fields.topics).toHaveLength(1)
    expect(final.fields.topics[0].topicId).toBe(this.topicId)
    expect(final.fields.topics[0].partitions).toHaveLength(1)
    expect(final.fields.topics[0].partitions[0].partitionIndex).toBe(0)
    expect(typesByOffset(final.fields)).toEqual(wireDecisions(4, 4))
    const leave = a.tap
      .shareFrames(aFrames)
      .find(({ apiKey, fields }) => apiKey === API.heartbeat && fields?.memberEpoch === -1)
    expect(leave).toBeDefined()
    expect(leave.at).toBeGreaterThanOrEqual(final.at)
    expect(final.at - calledAt).toBeGreaterThanOrEqual(DISCONNECT_MS - 1000)
    expect(final.at - calledAt).toBeLessThanOrEqual(DISCONNECT_MS)
    expect(settledAt - calledAt).toBeLessThanOrEqual(DISCONNECT_MS + 2000)

    await a.tap.acknowledgements.join()
    const finalFacts = a.tap.acknowledgements.frames.filter(
      ({ apiKey, correlationId, request }) =>
        apiKey === API.acknowledge &&
        correlationId === final.correlationId &&
        request.memberId === originalMemberId &&
        request.shareSessionEpoch === -1
    )
    expect(finalFacts).toHaveLength(1)
    const [finalFact] = finalFacts
    expect(finalFact.decodeError).toBeUndefined()
    expect(finalFact.response?.errorCode).toBe(0)
    expect(finalFact.response.topics).toEqual([
      {
        topicId: this.topicId,
        partitions: [expect.objectContaining({ partitionIndex: 0, errorCode: 0 })],
      },
    ])
    expect(this.forcedOutcomesMatch(a)).toBe(true)
    expect(this.decisionsSucceeded(a, settledDecisions(4, 4))).toBe(true)
    const accepts = a.acknowledged.filter(({ type }) => type === 'accept')
    expect(settledByOffset(accepts)).toEqual(settledDecisions(4, 4))
    const resultAt = Math.max(finalFact.response.at, ...accepts.map(({ at }) => at))
    const suffixLossAt = Math.max(
      ...a.acknowledged.filter(({ type }) => type === 'release').map(({ at }) => at)
    )
    expect(resultAt - calledAt - 4000).toBeLessThanOrEqual(2000)
    expect(resultAt - prewrite).toBeLessThanOrEqual(FINAL_ALLOWANCE_MS)
    this.note(title, 'frame', {
      prewriteToCall: calledAt - prewrite,
      callToFinalWrite: final.at - calledAt,
      finalWriteToResult: resultAt - final.at,
      callToSettled: settledAt - calledAt,
      finalDecisions: wireDecisions(4, 4),
      resultAt,
      finalResponseAt: finalFact.response.at,
      suffixLossAt,
      leaveAfterFinal: leave.at - final.at,
    })
    this.note(title, 'acknowledgement', { events: a.acknowledged })

    await this.prefixSettled(scope, title, a, b)
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
    this.checkAcquisition(title, a, 10)
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
    this.checkAcquisition(title, a, 50)
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
