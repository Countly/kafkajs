const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const {
  acknowledgementResults,
  acknowledgementRequests,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const path = require('path')
const { spawn } = require('child_process')
const { isDeepStrictEqual } = require('util')
const { Kafka, logLevel } = require('../../../../index')
const { createChaosSocketFactory } = require('../../../../testHelpers/chaosSocket')
const { readRequestHeader } = require('../../../../testHelpers/fakeSocket')
const { waitForFullIsr } = require('../../../../testHelpers/leaderElection')
const { readLog, endOffsets } = require('../../../../testHelpers/rawLog')
const shareFetchWire = require('../../../../testHelpers/wire/shareFetchV2')
const shareAcknowledgeWire = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const {
  SHARE_TOPIC_PREFIX,
  shareTopicName,
  setShareGroupConfigs,
  shareRecordIdentity,
  assertShareCleanup,
} = require('../../../../testHelpers/shareFixtures')
const {
  createResourceProbe,
  createTopic,
  plainTextBrokers,
  secureRandom,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const defaultSocketFactory = require('../../../network/socketFactory')
const RequestQueue = require('../../../network/requestQueue')
const ALTER_CONFIG_OP_TYPES = require('../../../protocol/alterConfigOpTypes')
const CONFIG_RESOURCE_TYPES = require('../../../protocol/configResourceTypes')
const CONFIG_SOURCE = require('../../../protocol/configSource')
const shareFetchResponse = require('../../../protocol/requests/shareFetch/v2/response')
const { ShareAckLedger } = require('../shareAckLedger')
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

const LIMIT = process.env.SHARE_SESSION_LIMIT_E2E === '1'
const enabled = process.env.SHARE_SESSION_E2E === '1' ? describe : describe.skip

const TITLES = {
  timeout: 'recovers after a lost fetch response',
  leader: 'opens on the new leader after a move',
  limit: 'backs off a third group until a leader session is freed',
}
const ROOT = path.resolve(__dirname, '..', '..', '..', '..')
const API_SHARE_FETCH = 78
const API_SHARE_ACKNOWLEDGE = 79
const SESSION_LIMIT_REACHED = 133
const LOCK_MS = 3000
const REQUEST_TIMEOUT_MS = 3000
const RECORDS = 300
const SEND_SIZE = 100
const PREFIX = 100
const BACKOFF_MS = 500
const BACKOFF_WINDOW_MS = 5000
const MAX_FRAMES_IN_WINDOW = 20
const TIMESTAMP = '1700000000000'
const STEP_MS = 20000
const DELIVERY_MS = 45000
const MOVE_MS = 60000
const CLEANUP_MS = 40000
const PROOF_MS = 10000
const HOOK_BUDGET_MS = 10000
const SHARE_CASE_BUDGET_MS = 90000
const LIMIT_CASE_BUDGET_MS = 190000
const GROUP_KEYS = ['share.auto.offset.reset', 'share.record.lock.duration.ms']
const LEDGER_METHODS = [
  'canStart',
  'decide',
  'complete',
  'completeRange',
  'dropExpired',
  'needsFlush',
  'prepare',
  'settle',
  'invalidate',
]

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

/** Runs the blocking leader-election helpers away from this process's event loop. */
const HELPER_SOURCE = `
const helpers = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/leaderElection')
const [topic, replicas, leader] = JSON.parse(process.argv[1])
helpers.reassignPreferredReplica(topic, replicas, leader)
helpers.waitForReassignmentToComplete(45000)
  .then(() => helpers.triggerPreferredElection(topic, 0))
  .catch((error) => {
    console.error(error && error.stack ? error.stack : String(error))
    process.exitCode = 1
  })
`

/**
 * @param {number} index
 * @returns {string} the index as four digits
 */
const pad = (index) => String(index).padStart(4, '0')

/**
 * @returns {{ promise: Promise<void>, open: () => void }} a gate that opens once
 */
const gate = () => {
  let open
  const promise = new Promise((resolve) => {
    open = resolve
  })
  return { promise, open }
}

/**
 * Splits a byte stream into complete length-prefixed Kafka frames, whatever the chunking.
 */
class FrameReader {
  /** Starts empty. */
  constructor() {
    this.pending = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk the next bytes of the stream
   * @returns {Buffer[]} the frames that are now complete, each with its 4-byte size prefix
   * @throws {Error} when a frame announces a negative size
   */
  push(chunk) {
    this.pending = this.pending.length ? Buffer.concat([this.pending, chunk]) : chunk
    const frames = []
    while (this.pending.length >= 4) {
      const size = this.pending.readInt32BE(0)
      if (size < 0) throw new Error(`A frame announced a negative size: ${size}`)
      if (this.pending.length < 4 + size) break
      frames.push(Buffer.from(this.pending.subarray(0, 4 + size)))
      this.pending = this.pending.subarray(4 + size)
    }
    return frames
  }
}

/**
 * Watches every complete request and response frame of the sockets it wraps, below any fault
 * injection, without changing a byte: writes keep their order, return value and callback, and the
 * response bytes are only read. A hook sees each frame as it happens.
 */
class ShareWireTap {
  /** Starts with no sockets, frames or hooks. */
  constructor() {
    /** @type {object[]} */
    this.frames = []
    this.hooks = new Set()
    this.sockets = 0
  }

  /**
   * @param {Function} inner the socket factory to observe
   * @param {string} label which client the sockets belong to
   * @returns {Function} a socket factory of the same kind
   */
  wrap(inner, label) {
    return (options) => {
      const socket = inner(options)
      const meta = {
        label,
        ordinal: ++this.sockets,
        host: options.host,
        port: options.port,
        socket,
        closedAt: null,
      }
      const requests = new FrameReader()
      const responses = new FrameReader()
      const write = socket.write.bind(socket)
      socket.write = (data, ...rest) => {
        const result = write(data, ...rest)
        for (const frame of requests.push(Buffer.from(data))) this.record('request', meta, frame)
        return result
      }
      socket.on('data', (chunk) => {
        for (const frame of responses.push(chunk)) this.record('response', meta, frame)
      })
      socket.on('close', () => {
        meta.closedAt = Date.now()
      })
      return socket
    }
  }

  /**
   * @param {'request'|'response'} kind
   * @param {object} meta the socket's identity
   * @param {Buffer} frame one complete frame
   * @returns {void}
   */
  record(kind, meta, frame) {
    const fact = {
      kind,
      frame,
      at: Date.now(),
      label: meta.label,
      socket: meta.ordinal,
      host: meta.host,
      port: meta.port,
      ref: meta,
    }
    if (kind === 'request') {
      const { apiKey, apiVersion, correlationId, byteLength } = readRequestHeader(frame)
      Object.assign(fact, { apiKey, apiVersion, correlationId, byteLength })
    } else {
      fact.correlationId = frame.readInt32BE(4)
    }
    this.frames.push(fact)
    for (const hook of this.hooks) hook(fact)
  }

  /**
   * @param {object} request a request fact
   * @returns {object|undefined} the response to it on the same socket, once it arrived
   */
  responseTo(request) {
    return this.frames.find(
      ({ kind, socket, correlationId }) =>
        kind === 'response' && socket === request.socket && correlationId === request.correlationId
    )
  }

  /**
   * Reads a ShareFetch or ShareAcknowledge request with the independent wire reader.
   * @param {object} request a request fact for API 78 or 79
   * @returns {{ epoch: number, acks: Array<{partition: number, firstOffset: string, lastOffset: string, types: number[]}> }}
   * @throws {Error} for another API or a version the independent reader does not know
   */
  decodeRequest(request) {
    const reader = {
      [API_SHARE_FETCH]: shareFetchWire,
      [API_SHARE_ACKNOWLEDGE]: shareAcknowledgeWire,
    }[request.apiKey]
    if (!reader || request.apiVersion !== reader.version) {
      throw new Error(`No independent reader for API ${request.apiKey} v${request.apiVersion}`)
    }
    const fields = reader.readRequest(request.frame.subarray(request.byteLength))
    return {
      epoch: fields.shareSessionEpoch,
      acks: fields.topics.flatMap(({ partitions }) =>
        partitions.flatMap(({ partitionIndex, acknowledgementBatches }) =>
          acknowledgementBatches.map(({ firstOffset, lastOffset, acknowledgeTypes }) => ({
            partition: partitionIndex,
            firstOffset,
            lastOffset,
            types: acknowledgeTypes,
          }))
        )
      ),
    }
  }

  /**
   * Decodes a ShareFetch response frame with the product's response decoder, for evidence only.
   * @param {object} response a response fact
   * @returns {Promise<object>} the decoded body
   * @throws {Error} on a response header with tagged fields or a malformed body
   */
  async decodeResponse(response) {
    const body = response.frame.subarray(8)
    if (body[0] !== 0) throw new Error('A response header carries tagged fields')
    return shareFetchResponse.decode(body.subarray(1))
  }

  /**
   * @param {string} label which client
   * @param {number} apiKey
   * @returns {object[]} that client's request facts for the API, oldest first
   */
  requestsOf(label, apiKey) {
    return this.frames.filter(
      (fact) => fact.kind === 'request' && fact.label === label && fact.apiKey === apiKey
    )
  }
}

/**
 * Counts sockets through the factory a client was built with, so a probe can compare open sockets.
 */
class SocketCounter {
  /** Starts with no sockets. */
  constructor() {
    /** @type {import('net').Socket[]} */
    this.sockets = []
    this.acknowledgements = new AcknowledgementTrace()
  }

  /**
   * @param {string} [label='client'] Owning member label.
   * @returns {Function} a socket factory that remembers every socket it creates
   */
  factory(label = 'client') {
    const create = defaultSocketFactory()
    return (options) => {
      const socket = create(options)
      this.sockets.push(socket)
      return this.acknowledgements.wrap(socket, label)
    }
  }

  /**
   * @returns {{ created: number, destroyed: number }} sockets created and how many are destroyed
   */
  counts() {
    return {
      created: this.sockets.length,
      destroyed: this.sockets.filter((socket) => socket.destroyed).length,
    }
  }
}

/**
 * Reads the product's own acknowledgement ledgers and request queues through call-through spies on
 * their prototypes: the spy runs the original with its own `this` and returns its result, and only
 * remembers which instance it saw. Nothing is altered or answered.
 */
class ResourceGauge {
  /** Starts with no spy installed. */
  constructor() {
    /** @type {Set<ShareAckLedger>} */
    this.ledgers = new Set()
    /** @type {Set<RequestQueue>} */
    this.queues = new Set()
    this.restores = []
  }

  /**
   * Installs the spies.
   * @returns {void}
   */
  install() {
    this.watch(ShareAckLedger.prototype, LEDGER_METHODS, this.ledgers)
    this.watch(RequestQueue.prototype, ['push'], this.queues)
  }

  /**
   * @param {object} prototype class prototype to spy on
   * @param {string[]} names methods that remember their receiver
   * @param {Set<object>} seen where receivers are remembered
   * @returns {void}
   */
  watch(prototype, names, seen) {
    for (const name of names) {
      const original = prototype[name]
      prototype[name] = function (...args) {
        seen.add(this)
        return original.apply(this, args)
      }
      this.restores.push(() => {
        prototype[name] = original
      })
    }
  }

  /**
   * Restores every original method and forgets what was seen.
   * @returns {void}
   */
  uninstall() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.ledgers.clear()
    this.queues.clear()
  }

  /**
   * @returns {number} requests pending or in flight on every queue seen
   */
  queueDepth() {
    return [...this.queues].reduce(
      (total, queue) => total + queue.pending.length + queue.inflight.size,
      0
    )
  }

  /**
   * @returns {number} entries every ledger seen still holds
   */
  ledgerSize() {
    return [...this.ledgers].reduce((total, ledger) => total + ledger.size, 0)
  }
}

/**
 * Reduces a probe snapshot to the integers the evidence verifier compares.
 * @param {object} snapshot a `createResourceProbe` snapshot with every source
 * @returns {{openSockets: number, timers: number, listeners: number, queueDepth: number, ledgerSize: number}}
 */
const summarize = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((total, count) => total + count, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * One share member with everything it observed, kept per fact and never reconstructed from a count.
 */
class Member {
  /**
   * @param {object} options
   * @param {Kafka} options.kafka the client factory this member's sockets come from
   * @param {string} options.label short name in evidence
   * @param {string} options.groupId the share group
   * @param {object} [options.config] share consumer options beyond the group
   * @param {(entry: object, payload: object) => Promise<void>} [options.onEntry] decides the
   *   record's fate; without it an explicit-mode member accepts and an implicit one returns
   */
  constructor({ kafka, label, groupId, config = {}, onEntry }) {
    this.label = label
    this.groupId = groupId
    this.config = { maxRecords: 100, maxWaitTimeInMs: 250, ...config }
    this.client = kafka.shareConsumer({ groupId, ...this.config })
    this.onEntry = onEntry
    /** @type {Array<{identity: string, deliveryCount: number, offset: string, at: number}>} */
    this.entries = []
    this.finished = 0
    this.acks = []
    this.requests = []
    this.timeouts = []
    this.resets = []
    this.fetches = []
    this.assignments = []
    this.crashes = []
    this.removers = []
  }

  /**
   * Starts observing; call before `start`, so no event can be missed.
   * @returns {void}
   */
  listen() {
    const { events } = this.client
    const keep = (name, list) =>
      this.removers.push(
        this.client.on(events[name], ({ payload }) => list.push({ ...payload, at: Date.now() }))
      )
    keep('ACKNOWLEDGE', this.acks)
    keep('REQUEST', this.requests)
    keep('REQUEST_TIMEOUT', this.timeouts)
    keep('SESSION_RESET', this.resets)
    keep('FETCH', this.fetches)
    keep('ASSIGNMENT_CHANGED', this.assignments)
    keep('CRASH', this.crashes)
  }

  /**
   * Connects, subscribes and starts the run, each step bounded.
   * @param {AbortSignal} scope
   * @param {string} topic
   * @returns {Promise<void>}
   * @throws {Error} a failed or slow step
   */
  async start(scope, topic) {
    await bounded(scope, STEP_MS, () => this.client.connect())
    await bounded(scope, STEP_MS, () => this.client.subscribe({ topics: [topic] }))
    await bounded(scope, STEP_MS, () =>
      this.client.run({ eachMessage: (payload) => this.handle(payload) })
    )
  }

  /**
   * Records the delivery before anything else can happen to it, then lets the case decide.
   * @param {object} payload the `eachMessage` payload
   * @returns {Promise<void>}
   */
  async handle(payload) {
    const { topic, partition, message } = payload
    const entry = {
      identity: shareRecordIdentity({ ...message, topic, partition }),
      deliveryCount: message.deliveryCount,
      offset: message.offset,
      at: Date.now(),
    }
    this.entries.push(entry)
    try {
      if (this.onEntry) await this.onEntry(entry, payload)
      else if (this.config.acknowledgementMode === 'explicit') payload.acknowledge('accept')
    } finally {
      this.finished += 1
    }
  }

  /**
   * @returns {string[]} every delivered identity, in delivery order, one element per delivery
   */
  identities() {
    return this.entries.map(({ identity }) => identity)
  }

  /**
   * @returns {void} removes every listener this member added
   */
  unlisten() {
    for (const remove of this.removers.splice(0)) remove()
  }
}

/**
 * Offsets inside inclusive decimal ranges.
 * @param {Array<{firstOffset: string, lastOffset: string}>} ranges
 * @returns {string[]} decimal offsets, in range order
 */
const offsetsOf = (ranges) => {
  const offsets = []
  for (const { firstOffset, lastOffset } of ranges) {
    for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
      offsets.push(String(offset))
    }
  }
  return offsets
}

/**
 * Deletes a topic and waits until the broker no longer lists it.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @returns {Promise<void>}
 * @throws {Error} a delete other than "unknown topic", or the topic still listed at the bound
 */
const dropTopic = async (scope, admin, topic) => {
  try {
    await bounded(scope, STEP_MS, () => admin.deleteTopics({ topics: [topic], timeout: 15000 }))
  } catch (error) {
    if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
  }
  await pollUntil(scope, STEP_MS, async () => !(await admin.listTopics()).includes(topic), {
    delayMs: 500,
    label: `Deleting ${topic}`,
  })
}

/**
 * Deletes a group and waits until the broker no longer lists it. A member that has just left keeps
 * the group non-empty for a moment (68), which is retried; "not found" counts only once a listing
 * confirms the absence.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} group
 * @returns {Promise<void>}
 * @throws {Error} any other group error, or the group still listed at the bound
 */
const dropGroup = (scope, admin, group) =>
  pollUntil(
    scope,
    STEP_MS,
    async () => {
      try {
        await admin.deleteGroups([group])
      } catch (error) {
        const code = error.groups?.find(({ groupId }) => groupId === group)?.errorCode
        if (code === 68) return false
        if (code !== 69) throw error
      }
      const { groups } = await admin.listGroups()
      return !groups.some(({ groupId }) => groupId === group)
    },
    { delayMs: 500, label: `Deleting ${group}` }
  )

/**
 * Removes the dynamic GROUP configs a case set and verifies none is left.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} group
 * @param {string[]} names the owned config keys
 * @returns {Promise<void>}
 * @throws {Error} a refused change, or an owned override still present
 */
const resetGroupConfig = async (scope, admin, group, names) => {
  const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
  const configEntries = names.map((name) => ({ name, operation: ALTER_CONFIG_OP_TYPES.DELETE }))
  const { resources } = await bounded(scope, STEP_MS, () =>
    admin.incrementalAlterConfigs({ resources: [{ ...resource, configEntries }] })
  )
  if (resources[0].errorCode !== 0) {
    throw new Error(`Group ${group} refused a config reset: error ${resources[0].errorCode}`)
  }
  await pollUntil(
    scope,
    STEP_MS,
    async () => {
      const read = await admin.describeConfigs({ resources: [{ ...resource, configNames: names }] })
      return read.resources[0].configEntries.every(
        ({ configSource }) => configSource !== CONFIG_SOURCE.GROUP_CONFIG
      )
    },
    { delayMs: 500, label: `Resetting ${group}` }
  )
}

/**
 * Runs the blocking leader-election helpers in a short-lived child process, so this process's
 * event loop (and the share client living in it) keeps running while they work.
 * @param {AbortSignal} scope the case scope; the child is killed when it aborts
 * @param {[string, number[], number]} args topic, the partition's replicas and the wanted leader
 * @returns {Promise<void>}
 * @throws {Error} a non-zero exit, with the end of the child's error output
 */
const moveLeadership = (scope, args) =>
  bounded(
    scope,
    MOVE_MS,
    (inner) =>
      new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ['-e', HELPER_SOURCE, JSON.stringify(args)], {
          env: { ...process.env, SHARE_LIVE_ROOT: ROOT },
          stdio: ['ignore', 'ignore', 'pipe'],
        })
        let errors = ''
        child.stderr.on('data', (chunk) => {
          errors += chunk
        })
        child.once('error', reject)
        child.once('close', (code, signal) =>
          code === 0
            ? resolve()
            : reject(new Error(`Leadership helper ended ${code ?? signal}: ${errors.slice(-600)}`))
        )
        inner.addEventListener('abort', () => child.kill('SIGKILL'), { once: true })
      })
  )

/**
 * The live cases of one session scenario family: lost fetch response, leader move and the session
 * limit. Each action owns its own topic, groups, members, taps and cleanup.
 */
class SessionCase {
  /**
   * @param {'timeout'|'leader'|'limit'} action which case this instance runs
   */
  constructor(action) {
    this.action = action
    this.title = TITLES[action]
    this.profile = action === 'limit' ? 'share-limit' : 'share'
    this.evidence = action === 'limit' ? 'share-session-limit' : 'share-session'
    this.budget = action === 'limit' ? LIMIT_CASE_BUDGET_MS : SHARE_CASE_BUDGET_MS
    this.nonce = secureRandom()
    this.topic = shareTopicName(`session-${action}`)
    this.groups = {}
    this.sockets = new SocketCounter()
    this.tap = new ShareWireTap()
    this.chaos = []
    this.gauge = new ResourceGauge()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.sockets.counts(),
      queueDepth: () => this.gauge.queueDepth(),
      ledgerSize: () => this.gauge.ledgerSize(),
      activeResources: true,
    })
    this.admin = new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      clientId: `share-session-admin-${this.nonce}`,
    }).admin()
    this.stack = new CleanupStack()
    this.members = {}
    this.expected = []
    this.handlerExpected = []
    this.handlers = {}
    this.log = []
    this.broker = []
    this.config = {}
  }

  /**
   * Appends one evidence row stamped with the case, profile, nonce, group aliases and budgets.
   * @param {string} event the row's event name
   * @param {object} [fields] what was measured
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(this.evidence, {
      event,
      case: this.title,
      profile: this.profile,
      scenario: this.nonce,
      groups: this.groups,
      budgets: { caseMs: this.budget, stepMs: STEP_MS, deliveryMs: DELIVERY_MS },
      wireVersions: wireVersionsOf(Object.values(this.members).flatMap(({ requests }) => requests)),
      ...fields,
    })
  }

  /** @returns {object} Per-member deliveries, completions and original acknowledgement requests. */
  progress() {
    return {
      members: Object.values(this.members).map(
        ({ label, groupId, entries, finished, acks, crashes, resets }) => ({
          label,
          groupId,
          delivered: entries.length,
          handled: finished,
          acknowledgements: acknowledgementResults(acks),
          crashes: crashes.map(({ error }) => describeError(error)),
          resets,
        })
      ),
      wireAcknowledgements: this.sockets.acknowledgements.snapshot(),
      acknowledgementRequests: acknowledgementRequests(
        this.tap.frames
          .filter(({ kind }) => kind === 'request')
          .map(({ frame, byteLength, ...fact }) => ({
            ...fact,
            bytes: frame,
            headerBytes: byteLength,
          }))
      ),
    }
  }

  /**
   * Runs the whole case: the action, strict cleanup, the cleanup proof and, only if all of it
   * held, one completion row.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure; cleanup failures ride on it
   */
  async run(scope) {
    const outer = new CleanupStack()
    outer.add('release resource gauge', () => this.gauge.uninstall())
    outer.add('disconnect admin', () => this.admin.disconnect())
    let pre
    let base
    let after
    try {
      await outer.run(
        async () => {
          this.gauge.install()
          pre = await this.sample(scope)
          await bounded(scope, STEP_MS, () => this.admin.connect())
          await bounded(scope, STEP_MS, async () => {
            await this.admin.describeCluster()
            await this.admin.listGroups()
          })
          base = await this.sample(scope)
          this.row('resource-baseline', { resources: summarize(base) })
          await this.stack.run(
            async () => {
              try {
                await this[this.action](scope)
              } catch (error) {
                this.row('work-failed', { error: describeError(error), ...this.progress() })
                throw error
              }
            },
            { budgetMs: CLEANUP_MS }
          )
          after = await assertShareCleanup(
            this.admin,
            Object.values(this.groups),
            [this.topic],
            base,
            this.probe,
            { signal: scope }
          )
        },
        { budgetMs: PROOF_MS }
      )
      await this.settleProcess(scope, pre)
      this.complete(base, after)
    } catch (error) {
      this.row('case-failed', { status: 'failed', error: describeError(error), ...this.progress() })
      throw error
    }
  }

  /**
   * One resource sample, taken under the same bounds `assertShareCleanup` samples under, so the
   * timers a bound itself holds appear in the baseline too.
   * @param {AbortSignal} scope
   * @returns {Promise<object>} a probe snapshot
   */
  sample(scope) {
    return bounded(scope, PROOF_MS, (inner) =>
      bounded(inner, Infinity, () => this.probe.snapshot())
    )
  }

  /**
   * After the admin client is gone, the process holds the handles it held before the case began.
   * Timers are left out: the poll's own bounds hold some.
   * @param {AbortSignal} scope
   * @param {object} base the sample taken before the admin client connected
   * @returns {Promise<void>}
   * @throws {Error} event-loop handles still differ at the bound
   */
  settleProcess(scope, base) {
    const handles = ({ Timeout: _timers, ...rest }) => rest
    return pollUntil(
      scope,
      PROOF_MS,
      async () =>
        isDeepStrictEqual(
          handles((await this.sample(scope)).activeResources),
          handles(base.activeResources)
        ),
      { delayMs: 250, label: 'Event-loop handles returning to their baseline' }
    )
  }

  /**
   * A client factory whose sockets are counted, observed on the wire and, below the product, can be
   * made to lose a response. Faults are seeded and armed only by the case.
   * @param {string} label which member it is for
   * @param {object} [options] Kafka options beyond brokers and logging
   * @returns {Kafka}
   */
  kafka(label, options = {}) {
    const observed = this.tap.wrap(this.sockets.factory(label), label)
    const chaos = createChaosSocketFactory(observed, { seed: 932 })
    this.chaos.push(chaos)
    this.stack.add(`stop fault injection of ${label}`, () => {
      for (const socket of chaos.sockets) socket.stop()
    })
    return new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      clientId: `share-session-${label.toLowerCase()}-${this.nonce}`,
      socketFactory: chaos,
      ...options,
    })
  }

  /**
   * Registers a group for deletion and config reset, then sets its configs and reads them back
   * before any member can join.
   * @param {AbortSignal} scope
   * @param {string} alias the group's short name in evidence
   * @returns {Promise<string>} the group id
   */
  async group(scope, alias) {
    const id = `${SHARE_TOPIC_PREFIX}session-${this.action}-${alias}-${this.nonce}`
    const { stack, admin } = this
    this.groups[alias] = id
    stack.add(`delete group ${id}`, (signal) => dropGroup(signal, admin, id))
    stack.add(`reset config of ${id}`, (signal) => resetGroupConfig(signal, admin, id, GROUP_KEYS))
    const configs = {
      'share.auto.offset.reset': 'earliest',
      'share.record.lock.duration.ms': LOCK_MS,
    }
    await bounded(scope, STEP_MS, () => setShareGroupConfigs(admin, id, configs))
    this.config[alias] = configs
    this.row('config-readback', { alias, configs })
    return id
  }

  /**
   * Registers and creates the case's one-partition topic.
   * @param {AbortSignal} scope
   * @param {{replicas?: number, minIsr?: number}} [options]
   * @returns {Promise<void>}
   */
  async createTopic(scope, { replicas = 1, minIsr } = {}) {
    const { stack, admin, topic } = this
    stack.add('delete topic', (signal) => dropTopic(signal, admin, topic))
    const config = [
      { name: 'retention.ms', value: '-1' },
      { name: 'cleanup.policy', value: 'delete' },
      ...(minIsr ? [{ name: 'min.insync.replicas', value: String(minIsr) }] : []),
    ]
    await bounded(scope, STEP_MS, () => createTopic({ topic, partitions: 1, replicas, config }))
  }

  /**
   * Appends the fixed corpus once, in awaited sends, and returns the identities from the broker's
   * own base offsets.
   * @param {AbortSignal} scope
   * @returns {Promise<string[]>} identities in append order
   * @throws {Error} an append that did not land contiguously from offset 0
   */
  async produce(scope) {
    const producer = this.kafka('P').producer({ idempotent: true, maxInFlightRequests: 1 })
    this.stack.add('disconnect producer', () => producer.disconnect())
    await bounded(scope, STEP_MS, () => producer.connect())
    const identities = []
    for (let first = 0; first < RECORDS; first += SEND_SIZE) {
      const messages = Array.from({ length: SEND_SIZE }, (_, offset) => ({
        partition: 0,
        key: Buffer.from(`${this.action}:${pad(first + offset)}`),
        value: Buffer.from(`${this.action}:value:${first + offset}`),
        timestamp: TIMESTAMP,
      }))
      const [meta] = await bounded(scope, STEP_MS, () =>
        producer.send({ topic: this.topic, acks: -1, messages })
      )
      expect(meta.errorCode).toBe(0)
      expect(meta.baseOffset).toBe(String(first))
      messages.forEach(({ key, value }, offset) =>
        identities.push(
          shareRecordIdentity({
            topic: this.topic,
            partition: 0,
            offset: String(first + offset),
            key,
            value,
          })
        )
      )
    }
    await bounded(scope, STEP_MS, () => producer.disconnect())
    this.row('produced', { records: RECORDS, partition: 0, end: String(RECORDS) })
    this.expected = identities
    return identities
  }

  /**
   * @param {AbortSignal} scope
   * @returns {Promise<{ topicId: string, leader: number, leaderEpoch: number, replicas: number[], isr: number[], endpoint: string }>}
   *   the partition's current leadership, with the leader's advertised `host:port`
   */
  async leadership(scope) {
    const { topics } = await bounded(scope, STEP_MS, () =>
      this.admin.fetchTopicMetadata({ topics: [this.topic], useDescribeTopicPartitions: true })
    )
    const [{ topicId, partitions }] = topics
    const [{ leader, leaderEpoch, replicas, isr }] = partitions
    const { brokers } = await bounded(scope, STEP_MS, () => this.admin.describeCluster())
    const node = brokers.find(({ nodeId }) => nodeId === leader)
    return {
      topicId,
      leader,
      leaderEpoch,
      replicas,
      isr,
      endpoint: `${node.host}:${node.port}`,
    }
  }

  /**
   * Registers a member's cleanup, listens and starts it.
   * @param {AbortSignal} scope
   * @param {Member} member
   * @returns {Promise<Member>} the same member, running
   */
  async launch(scope, member) {
    this.members[member.label] = member
    member.listen()
    this.stack.add(`remove ${member.label} listeners`, () => member.unlisten())
    this.stack.add(`disconnect ${member.label}`, () => member.client.disconnect({ timeout: 5000 }))
    await member.start(scope, this.topic)
    return member
  }

  /**
   * Waits for a member's deliveries to reach `count`, failing at once on a crash.
   * @param {AbortSignal} scope
   * @param {Member} member
   * @param {number} count
   * @returns {Promise<void>}
   * @throws {Error} a crash, or too few deliveries at the bound
   */
  awaitDelivered(scope, member, count) {
    return pollUntil(
      scope,
      DELIVERY_MS,
      () => {
        expect(member.crashes).toEqual([])
        return member.entries.length >= count
      },
      { delayMs: 100, label: `${member.label} receiving ${count} deliveries` }
    )
  }

  /**
   * Checks successful Accept coverage for this group without combining independent groups.
   * @param {string} group Group being observed.
   * @returns {boolean} Whether the group's successful results cover all 300 identities.
   */
  acceptedGroup(group) {
    const accepted = new Set()
    for (const { groupId, acks } of Object.values(this.members)) {
      if (groupId !== group) continue
      for (const { topic, partition, type, error, firstOffset, lastOffset } of acks) {
        if (topic !== this.topic || partition !== 0 || type !== 'accept' || error !== null) continue
        for (const offset of offsetsOf([{ firstOffset, lastOffset }])) accepted.add(offset)
      }
    }
    const wire = this.sockets.acknowledgements.confirmed(group, this.topicId, 0)
    return this.expected.every(
      (identity) => accepted.has(JSON.parse(identity)[2]) && wire.has(JSON.parse(identity)[2])
    )
  }

  /**
   * Polls successful settlement and terminal positions against the independent raw-log end.
   * @param {AbortSignal} scope Case scope.
   * @param {string} group Group being observed.
   * @returns {Promise<object>} Terminal partition row.
   */
  async awaitStart(scope, group) {
    return bounded(
      scope,
      DELIVERY_MS,
      async (poll) => {
        await pollUntil(poll, DELIVERY_MS, () => this.acceptedGroup(group), {
          label: `${group} successful Accept coverage`,
        })
        const { rows } = await awaitTerminalSharePosition(
          this.admin,
          {
            groupId: group,
            topic: this.topic,
            partitions: [{ partition: 0, floor: '0', end: this.logEnd }],
          },
          {
            signal: poll,
            timeoutMs: DELIVERY_MS,
            record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
          }
        )
        return rows[0]
      },
      { label: `${group} terminal settlement` }
    )
  }

  /**
   * Reads the partition straight from its leader and checks it holds exactly the fixed corpus.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} a missing, extra, duplicate or mutated record, or a control record
   */
  async verifyLog(scope) {
    const log = await bounded(scope, STEP_MS, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = log.get(0)
    expect(endOffset).toBe(String(RECORDS))
    this.logEnd = endOffset
    const { topics } = await bounded(scope, STEP_MS, () =>
      this.admin.fetchTopicMetadata({ topics: [this.topic], useDescribeTopicPartitions: true })
    )
    this.topicId = topics.find(({ name }) => name === this.topic).topicId
    expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    const actual = records.map(({ offset, key, value }) =>
      shareRecordIdentity({ topic: this.topic, partition: 0, offset, key, value })
    )
    expect(actual).toEqual(this.expected)
    this.log = actual
  }

  /**
   * Runs the same-group verifiers one after another; each must see nothing new.
   * @param {AbortSignal} scope
   * @param {string[]} groups
   * @returns {Promise<void>}
   * @throws {Error} a delivery to a verifier
   */
  async verifyQuiet(scope, groups) {
    for (const member of Object.values(this.members)) {
      await bounded(scope, STEP_MS, () => member.client.disconnect({ timeout: 5000 }))
    }
    await this.sockets.acknowledgements.join()
    this.quiet = []
    this.broker = []
    for (const group of groups) {
      const verified = await assertQuietTerminalGroup({
        kafka: this.kafka('V'),
        admin: this.admin,
        groupId: group,
        topic: this.topic,
        partitions: [{ partition: 0, floor: '0', end: this.logEnd }],
        signal: scope,
        timeoutMs: DELIVERY_MS,
        record: (fields) => this.row('quiet-verifier', { groupId: group, ...fields }),
      })
      this.quiet.push({ groupId: group, ...verified })
      this.broker.push(...verified.position.rows)
    }
    const ends = await bounded(scope, STEP_MS, () =>
      endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect(ends.get(0)).toBe(this.logEnd)
  }

  /**
   * A lost ShareFetch response. The leader endpoint's next ShareFetch answer, which carries a
   * nonempty acquisition, is read from the wire and then swallowed below the product, so the
   * product sees a request that never completes.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} a fixture that cannot prove the dropped answer acquired records, or an oracle
   *   that does not hold
   */
  async timeout(scope) {
    await this.createTopic(scope)
    const group = await this.group(scope, 'main')
    const expected = await this.produce(scope)
    const { endpoint } = await this.leadership(scope)

    const member = new Member({
      kafka: this.kafka('A', { requestTimeout: REQUEST_TIMEOUT_MS }),
      label: 'A',
      groupId: group,
    })
    let dropped = null
    const arm = (fact) => {
      if (
        dropped ||
        fact.kind !== 'request' ||
        fact.label !== 'A' ||
        fact.apiKey !== API_SHARE_FETCH ||
        `${fact.host}:${fact.port}` !== endpoint ||
        member.finished < PREFIX
      ) {
        return
      }
      dropped = fact
      this.chaos
        .flatMap(({ sockets }) => sockets)
        .find(({ inner }) => inner === fact.ref.socket)
        .dropResponses(1, { apiKey: API_SHARE_FETCH })
    }
    this.tap.hooks.add(arm)
    this.stack.add('remove the arming hook', () => this.tap.hooks.delete(arm))
    await this.launch(scope, member)

    await pollUntil(scope, STEP_MS, () => dropped !== null, {
      delayMs: 50,
      label: 'The next fetch after the first hundred records',
    })
    const startedHeld = member.entries.slice(member.finished).map(({ identity }) => identity)
    const carried = this.tap.decodeRequest(dropped)
    expect(carried.epoch).toBeGreaterThan(0)
    await pollUntil(scope, STEP_MS, () => this.tap.responseTo(dropped) !== undefined, {
      delayMs: 50,
      label: 'The leader answering the swallowed fetch',
    })
    const response = this.tap.responseTo(dropped)
    const answer = await this.tap.decodeResponse(response)
    expect(answer.errorCode).toBe(0)
    const acquired = answer.responses.flatMap(({ partitions }) =>
      partitions.flatMap(({ acquiredRecords }) => acquiredRecords ?? [])
    )
    const droppedOffsets = offsetsOf(acquired)
    expect(droppedOffsets.length).toBeGreaterThan(0)
    const droppedIdentities = droppedOffsets.map((offset) => expected[Number(offset)])
    this.row('frame', {
      swallowed: {
        correlationId: dropped.correlationId,
        socket: dropped.socket,
        epoch: carried.epoch,
        carriedAcknowledgements: carried.acks,
        acquired,
        sizeBytes: response.frame.length,
      },
    })

    await this.awaitDelivered(scope, member, RECORDS)
    await this.verifyLog(scope)
    const record = await this.awaitStart(scope, group)
    const closedAt = dropped.ref.closedAt
    this.assertRecovery(member, { dropped, carried, droppedOffsets, droppedIdentities, closedAt })

    expect(member.crashes).toEqual([])
    expect([...member.identities()].sort()).toEqual([...expected].sort())
    this.row('held-set', {
      startedHeldAtFault: startedHeld,
      acquiredNeverDelivered: droppedIdentities,
    })
    this.handlerExpected = expected
    this.handlers = { A: member.identities() }
    this.broker = [record]
    await bounded(scope, STEP_MS, () => member.client.disconnect({ timeout: 5000 }))
    await this.verifyQuiet(scope, [group])
  }

  /**
   * The oracles that tie the swallowed answer to the product's recovery.
   * @param {Member} member the consuming member
   * @param {object} fault what was swallowed
   * @param {object} fault.dropped the request fact whose answer was swallowed
   * @param {object} fault.carried the decoded request: its epoch and piggybacked acknowledgements
   * @param {string[]} fault.droppedOffsets offsets the swallowed answer acquired
   * @param {string[]} fault.droppedIdentities their identities
   * @param {number|null} fault.closedAt when the original socket closed
   * @returns {void}
   * @throws {Error} an oracle that does not hold
   */
  assertRecovery(member, { dropped, carried, droppedOffsets, droppedIdentities, closedAt }) {
    const timedOut = member.timeouts.filter(
      ({ apiName, correlationId }) =>
        apiName === 'ShareFetch' && correlationId === dropped.correlationId
    )
    expect(timedOut).toHaveLength(1)
    expect(dropped.ref.socket.destroyed).toBe(true)
    expect(closedAt).not.toBeNull()
    const reset = member.resets.find(({ reason }) => reason === 'connection')
    expect(reset).toBeDefined()

    const lost = offsetsOf(carried.acks)
    const outcomes = new Map()
    for (const { type, error, firstOffset, lastOffset } of member.acks) {
      if (error === null || type === 'gap') continue
      for (const offset of offsetsOf([{ firstOffset, lastOffset }])) {
        outcomes.set(offset, [...(outcomes.get(offset) ?? []), error.reason])
      }
    }
    for (const offset of lost) expect(outcomes.get(offset)).toEqual(['session-lost'])

    const later = this.tap
      .requestsOf('A', API_SHARE_FETCH)
      .filter(({ at, socket }) => at > dropped.at && socket !== dropped.socket)
    const [opening, next] = later.map((fact) => this.tap.decodeRequest(fact))
    expect(opening.epoch).toBe(0)
    expect(opening.acks).toEqual([])
    expect(next.epoch).toBe(1)
    const redelivered = new Set(
      member.entries.filter(({ at }) => at > closedAt).map(({ offset }) => offset)
    )
    for (const fact of this.tap.frames) {
      if (fact.kind !== 'request' || fact.label !== 'A' || fact.at <= closedAt) continue
      if (![API_SHARE_FETCH, API_SHARE_ACKNOWLEDGE].includes(fact.apiKey)) continue
      for (const offset of offsetsOf(this.tap.decodeRequest(fact).acks)) {
        expect(redelivered.has(offset)).toBe(true)
      }
    }

    const counts = new Map()
    for (const { identity, deliveryCount } of member.entries) {
      counts.set(identity, [...(counts.get(identity) ?? []), deliveryCount])
    }
    for (const identity of droppedIdentities) expect(counts.get(identity)).toEqual([2])
    const firstSecond = member.entries.find(({ deliveryCount }) => deliveryCount === 2)
    expect(firstSecond.at - closedAt).toBeLessThanOrEqual(LOCK_MS + 2000)
    this.row('fault', {
      timeoutEvent: timedOut[0].correlationId,
      closedAt,
      reset: { reason: reset.reason, recordsDropped: reset.recordsDropped },
      lostAcknowledgements: lost,
      replacement: { socket: later[0].socket, firstEpoch: opening.epoch, nextEpoch: next.epoch },
      reacquiredAfterMs: firstSecond.at - closedAt,
      reacquired: droppedOffsets.length,
    })
  }

  /**
   * A leader move under a held acquisition. The partition's leadership is moved with the guarded
   * helpers while the held record is renewed; then the handler is released and the client must
   * discover the new leader by itself.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async leader(scope) {
    await this.createTopic(scope, { replicas: 3, minIsr: 2 })
    const group = await this.group(scope, 'main')
    const expected = await this.produce(scope)
    const before = await this.leadership(scope)
    expect(before.isr).toHaveLength(3)
    const target = Math.min(...before.replicas.filter((id) => id !== before.leader))
    const moved = { done: false }
    this.stack.add('restore the preferred leader', async (signal) => {
      if (!moved.done) return
      await moveLeadership(signal, [this.topic, before.replicas, before.leader])
      await waitForFullIsr(this.admin, [this.topic], { budgetMs: 30000 })
    })

    const hold = gate()
    const held = { payload: null, outcome: null, startedAt: null, releasedAt: null }
    const member = new Member({
      kafka: this.kafka('A'),
      label: 'A',
      groupId: group,
      config: { acknowledgementMode: 'explicit', maxRecords: 1 },
      onEntry: async (entry, payload) => {
        if (held.payload) {
          payload.acknowledge('accept')
          return
        }
        Object.assign(held, { payload, entry, startedAt: Date.now() })
        await hold.promise
        try {
          payload.acknowledge('release')
          held.outcome = { decision: 'release' }
        } catch (error) {
          held.outcome = { lost: describeError(error) }
        }
      },
    })
    await this.launch(scope, member)
    this.stack.add('open the held handler', () => hold.open())
    await pollUntil(scope, STEP_MS, () => held.payload !== null, {
      delayMs: 50,
      label: 'The first record reaching its handler',
    })
    expect(member.entries).toHaveLength(1)
    const heldIdentity = held.entry.identity

    const renewals = []
    const renewing = { on: true }
    const schedule = (async () => {
      while (renewing.on) {
        await delay(scope, 1500)
        if (!renewing.on) break
        renewals.push(await this.renewal(held.payload))
      }
    })()
    schedule.catch(() => {})
    this.stack.add('stop the renewal schedule', async () => {
      renewing.on = false
      await schedule.catch(() => {})
    })

    moved.done = true
    await moveLeadership(scope, [this.topic, before.replicas, target])
    let after
    await pollTolerating(
      scope,
      STEP_MS,
      async () => {
        after = await this.leadership(scope)
        return after.leader === target && after.isr.length === 3
      },
      { delayMs: 500, label: 'The new leader with a full ISR' }
    )
    expect(after.leaderEpoch).toBeGreaterThan(before.leaderEpoch)
    renewing.on = false
    await schedule
    const finalRenewal = await this.renewal(held.payload)
    this.row('fault', {
      before,
      after,
      scheduleRenewals: renewals,
      finalRenewal,
      frozenDuplicateAllowance: [heldIdentity],
    })
    if (finalRenewal.error) expect(finalRenewal.error.name).toBe('KafkaJSShareAcknowledgementError')

    const fetchesBefore = this.tap.requestsOf('A', API_SHARE_FETCH).length
    held.releasedAt = Date.now()
    hold.open()
    await this.awaitDelivered(scope, member, RECORDS + 1)
    expect(member.crashes).toEqual([])
    const roundWrites = this.tap
      .requestsOf('A', API_SHARE_FETCH)
      .slice(0, fetchesBefore)
      .filter(({ at }) => at > held.startedAt && at < held.releasedAt)
    expect(roundWrites).toEqual([])
    expect(held.outcome).not.toBeNull()

    const newNode = after.endpoint
    const opening = this.tap
      .requestsOf('A', API_SHARE_FETCH)
      .find(({ host, port, at }) => `${host}:${port}` === newNode && at > held.releasedAt)
    expect(opening).toBeDefined()
    const openingFields = this.tap.decodeRequest(opening)
    expect(openingFields.epoch).toBe(0)
    expect(openingFields.acks).toEqual([])

    const counts = new Map()
    for (const { identity, deliveryCount } of member.entries) {
      counts.set(identity, [...(counts.get(identity) ?? []), deliveryCount])
    }
    expect(counts.get(heldIdentity).length).toBeGreaterThanOrEqual(2)
    for (const identity of expected.filter((id) => id !== heldIdentity)) {
      expect(counts.get(identity)).toHaveLength(1)
    }
    this.handlerExpected = [...expected, heldIdentity]
    expect([...member.identities()].sort()).toEqual([...this.handlerExpected].sort())
    this.row('held-set', {
      identity: heldIdentity,
      deliveryCounts: counts.get(heldIdentity),
      handlerOutcome: held.outcome,
      resets: member.resets.map(({ reason, nodeId, recordsDropped }) => ({
        reason,
        nodeId,
        recordsDropped,
      })),
      replacementOpening: { socket: opening.socket, endpoint: newNode, epoch: openingFields.epoch },
    })
    this.handlers = { A: member.identities() }
    await this.verifyLog(scope)
    this.broker = [await this.awaitStart(scope, group)]
    await bounded(scope, STEP_MS, () => member.client.disconnect({ timeout: 5000 }))
    await this.verifyQuiet(scope, [group])
  }

  /**
   * One renewal of the held record through its original invocation.
   * @param {{ renew: () => Promise<number> }} payload the held handler's payload
   * @returns {Promise<{ at: number, lockMs?: number, error?: object }>} the actual result
   */
  async renewal(payload) {
    const at = Date.now()
    try {
      return { at, lockMs: await payload.renew() }
    } catch (error) {
      return { at, error: describeError(error) }
    }
  }

  /**
   * Three groups on one leader under a two-session limit: the third group is told 133, backs off
   * on its configured schedule and gets a session once one is freed.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async limit(scope) {
    await this.createTopic(scope)
    await this.verifyBrokerLimits(scope)
    const groups = {}
    for (const alias of ['g1', 'g2', 'g3']) groups[alias] = await this.group(scope, alias)
    const expected = await this.produce(scope)

    const first = await this.launch(scope, this.member('G1', groups.g1))
    await this.awaitDelivered(scope, first, RECORDS)
    const second = await this.launch(scope, this.member('G2', groups.g2))
    await this.awaitDelivered(scope, second, RECORDS)
    for (const member of [first, second]) {
      expect(this.tap.requestsOf(member.label, API_SHARE_FETCH).length).toBeGreaterThan(0)
    }

    const third = await this.launch(
      scope,
      this.member('G3', groups.g3, {
        retry: { initialRetryTime: BACKOFF_MS, maxRetryTime: BACKOFF_MS, factor: 0, retries: 100 },
      })
    )
    let limitedAt = null
    await pollUntil(
      scope,
      STEP_MS,
      async () => {
        limitedAt = await this.firstSessionLimit('G3')
        return limitedAt !== null
      },
      { delayMs: 50, label: 'The third group being told the session limit' }
    )
    await delay(scope, BACKOFF_WINDOW_MS)
    const window = await this.backoffWindow('G3', limitedAt)
    expect(window.frames.length).toBeLessThanOrEqual(MAX_FRAMES_IN_WINDOW)
    expect(window.answered).toBeGreaterThan(1)
    expect(window.refusals).toBe(window.answered)
    for (const gap of window.gaps) expect(gap).toBeGreaterThanOrEqual(BACKOFF_MS - 100)
    expect(third.entries).toHaveLength(0)
    this.row('frame', { limit: window })

    await bounded(scope, STEP_MS, () => first.client.disconnect({ timeout: 5000 }))
    await this.awaitDelivered(scope, third, RECORDS)
    const accepted = await this.firstAcceptedOpening('G3')
    expect(accepted).not.toBeNull()
    expect(accepted.at).toBeGreaterThan(limitedAt + BACKOFF_WINDOW_MS)
    this.row('frame', { freed: accepted })

    await this.verifyLog(scope)
    this.broker = []
    for (const alias of ['g1', 'g2', 'g3']) {
      this.broker.push(await this.awaitStart(scope, groups[alias]))
    }
    for (const member of [first, second, third]) {
      expect(member.crashes).toEqual([])
      expect([...member.identities()].sort()).toEqual([...expected].sort())
    }
    this.handlerExpected = [...expected, ...expected, ...expected]
    this.handlers = {
      G1: first.identities(),
      G2: second.identities(),
      G3: third.identities(),
    }
    for (const member of [second, third]) {
      await bounded(scope, STEP_MS, () => member.client.disconnect({ timeout: 5000 }))
    }
    await this.verifyQuiet(scope, Object.values(groups))
  }

  /**
   * @param {string} label the member's name
   * @param {string} group its group
   * @param {object} [config] share consumer options beyond the default
   * @returns {Member}
   */
  member(label, group, config = {}) {
    return new Member({ kafka: this.kafka(label), label, groupId: group, config })
  }

  /**
   * Checks the limits on every broker, not only in the compose file.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} a broker whose effective share-session or group-size limit is not 2
   */
  async verifyBrokerLimits(scope) {
    const { brokers } = await bounded(scope, STEP_MS, () => this.admin.describeCluster())
    expect(brokers).toHaveLength(3)
    const effective = {}
    for (const { nodeId } of brokers) {
      const { resources } = await bounded(scope, STEP_MS, () =>
        this.admin.describeConfigs({
          resources: [
            {
              type: CONFIG_RESOURCE_TYPES.BROKER,
              name: String(nodeId),
              configNames: ['group.share.max.share.sessions', 'group.share.max.size'],
            },
          ],
        })
      )
      const values = Object.fromEntries(
        resources[0].configEntries.map(({ configName, configValue }) => [configName, configValue])
      )
      expect(values).toEqual({ 'group.share.max.share.sessions': '2', 'group.share.max.size': '2' })
      effective[nodeId] = values
    }
    this.row('config-readback', { brokers: effective })
  }

  /**
   * @param {string} label the member
   * @returns {Promise<number|null>} when the member first got a top-level 133, or null
   */
  async firstSessionLimit(label) {
    for (const request of this.tap.requestsOf(label, API_SHARE_FETCH)) {
      const response = this.tap.responseTo(request)
      if (
        response &&
        (await this.tap.decodeResponse(response)).errorCode === SESSION_LIMIT_REACHED
      ) {
        return response.at
      }
    }
    return null
  }

  /**
   * @param {string} label the member
   * @returns {Promise<{correlationId: number, at: number, epoch: number}|null>} the first ShareFetch
   *   that opened a session (epoch 0) and was answered without a top-level error, or null
   */
  async firstAcceptedOpening(label) {
    for (const request of this.tap.requestsOf(label, API_SHARE_FETCH)) {
      const response = this.tap.responseTo(request)
      const { epoch } = this.tap.decodeRequest(request)
      if (response && epoch === 0 && (await this.tap.decodeResponse(response)).errorCode === 0) {
        return { correlationId: request.correlationId, at: request.at, epoch }
      }
    }
    return null
  }

  /**
   * The ShareFetch frames a member wrote in the window that began with its first 133.
   * @param {string} label the member
   * @param {number} from the window's start
   * @returns {Promise<{frames: number[], gaps: number[], answered: number, refusals: number}>}
   *   write times, the spacing between them, how many were answered and how many of those 133
   */
  async backoffWindow(label, from) {
    const requests = this.tap
      .requestsOf(label, API_SHARE_FETCH)
      .filter(({ at }) => at >= from && at <= from + BACKOFF_WINDOW_MS)
    let answered = 0
    let refusals = 0
    for (const request of requests) {
      const response = this.tap.responseTo(request)
      if (!response) continue
      answered += 1
      if ((await this.tap.decodeResponse(response)).errorCode === SESSION_LIMIT_REACHED)
        refusals += 1
    }
    const frames = requests.map(({ at }) => at)
    return {
      frames,
      gaps: frames.slice(1).map((at, index) => at - frames[index]),
      answered,
      refusals,
    }
  }

  /**
   * Writes the one completion row, only after every assertion and the strict cleanup passed.
   * @param {object} before the baseline snapshot
   * @param {object} after the snapshot after cleanup
   * @returns {void}
   */
  complete(before, after) {
    const requests = Object.values(this.members).flatMap(({ requests: list }) => list)
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: this.expected, actual: this.log },
        handler: {
          expected: this.handlerExpected,
          actual: Object.values(this.handlers).flat(),
        },
        handlerByGroup: Object.fromEntries(
          Object.entries(this.handlers).map(([label, actual]) => [label, { actual }])
        ),
        broker: this.broker,
        acknowledgements: this.progress(),
        quiet: this.quiet,
      },
      resources: { before: summarize(before), after: summarize(after) },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      wireVersions: wireVersionsOf(requests),
      config: {
        groups: this.config,
        members: Object.fromEntries(
          Object.values(this.members).map(({ label, config }) => [label, config])
        ),
      },
    })
  }
}

/**
 * Declares one live case of the file under the enclosing selection's gate.
 * @param {'timeout'|'leader'|'limit'} action
 * @returns {void}
 */
const declare = (action) => {
  const budget = action === 'limit' ? LIMIT_CASE_BUDGET_MS : SHARE_CASE_BUDGET_MS
  it(
    TITLES[action],
    liveCase(budget, (scope) => new SessionCase(action).run(scope)),
    caseTimeout(budget)
  )
}

/**
 * Installs the hooks every live case of this file shares: a clean-run check before the file and
 * after each case that was admitted.
 * @returns {void}
 */
const hooks = () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_BUDGET_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_BUDGET_MS
  )
  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
}

enabled(`share session ${LIMIT ? 'limit' : 'recovery'} on a live broker`, () => {
  hooks()
  if (LIMIT) {
    declare('limit')
  } else {
    declare('timeout')
    declare('leader')
  }
})

describe('share wire tap framing', () => {
  const frame = (...bytes) => {
    const body = Buffer.from(bytes)
    const head = Buffer.alloc(4)
    head.writeInt32BE(body.length)
    return Buffer.concat([head, body])
  }

  it('joins a frame split across chunks', () => {
    const reader = new FrameReader()
    const whole = frame(1, 2, 3, 4, 5)
    expect(reader.push(whole.subarray(0, 2))).toEqual([])
    expect(reader.push(whole.subarray(2, 6))).toEqual([])
    expect(reader.push(whole.subarray(6))).toEqual([whole])
  })

  it('splits several frames that arrive in one chunk', () => {
    const reader = new FrameReader()
    const first = frame(1)
    const second = frame(2, 3)
    expect(reader.push(Buffer.concat([first, second, frame(4).subarray(0, 3)]))).toEqual([
      first,
      second,
    ])
    expect(reader.push(frame(4).subarray(3))).toEqual([frame(4)])
  })

  it('refuses a frame that announces a negative size', () => {
    const negative = Buffer.alloc(4)
    negative.writeInt32BE(-1)
    expect(() => new FrameReader().push(negative)).toThrow('negative size')
  })
})
