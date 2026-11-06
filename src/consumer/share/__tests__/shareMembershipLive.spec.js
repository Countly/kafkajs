const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const { readRequestHeader } = require('../../../../testHelpers/fakeSocket')
const {
  acknowledgementResults,
  acknowledgementRequests,
  pollSharePosition,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const { spawn } = require('child_process')
const { isDeepStrictEqual } = require('util')
const { runInNewContext } = require('vm')
const { Kafka, logLevel } = require('../../../../index')
const { readLog, endOffsets } = require('../../../../testHelpers/rawLog')
const {
  SHARE_TOPIC_PREFIX,
  ShareHandlerLog,
  shareTopicName,
  setShareGroupConfigs,
  shareRecordIdentity,
  assertShareCleanup,
  captureShareCleanupBaseline,
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
const { ShareAckLedger } = require('../shareAckLedger')
const {
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

const enabled = process.env.SHARE_MEMBERSHIP_E2E === '1' ? describe : describe.skip

const TITLE = 'preserves identities across joins clean leave and child death'
const EVIDENCE = 'share-membership'
const PROFILE = 'share'
const ROOT = path.resolve(__dirname, '..', '..', '..', '..')
const CASE_BUDGET_MS = 200000
const HOOK_BUDGET_MS = 10000
const STEP_MS = 20000
const STAGE_MS = 60000
const CLEANUP_MS = 40000
const PROOF_MS = 10000
const LOCK_MS = 3000
const SESSION_MS = 6000
const HEARTBEAT_MS = 1000
const RENEW_MS = 1000
const PARTITIONS = 3
const STAGES = 6
const STAGE_RECORDS = 300
const PER_PARTITION = STAGE_RECORDS / PARTITIONS
const TIMESTAMP = '1700000000000'
const CLEAN_LEAVE_MS = 1000
const RELEASE_WITHIN_MS = 2000
const CHILD_RETURN_MS = LOCK_MS + SESSION_MS + 3000
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

/**
 * The child member's whole program. It writes each invocation's start to a synchronous journal
 * before it can block or be killed, reports over IPC only after that, and keeps each held
 * invocation's continuation bound to the invocation itself.
 */
const CHILD_SOURCE = `
const { acknowledgementRequests } = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/shareLiveDiagnostics')
const { readRequestHeader } = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/fakeSocket')
const { describeError } = require(process.env.SHARE_LIVE_ROOT + '/src/producer/__tests__/fixtures/liveProbe')
const fetchAcknowledgementResponse = require(process.env.SHARE_LIVE_ROOT + '/src/protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require(process.env.SHARE_LIVE_ROOT + '/src/protocol/requests/shareAcknowledge/v2/response')
const defaultSocketFactory = require(process.env.SHARE_LIVE_ROOT + '/src/network/socketFactory')
const AcknowledgementTrace = ${AcknowledgementTrace.toString()}
const { Kafka, logLevel } = require(process.env.SHARE_LIVE_ROOT + '/index')
const { ShareHandlerLog, shareRecordIdentity } = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/shareFixtures')
const config = JSON.parse(process.env.SHARE_LIVE_CHILD)
const journal = new ShareHandlerLog(config.journal, config.clientId)
const report = (message) => process.send(message)
let hold = null
let current = null
const trace = new AcknowledgementTrace((row) => report({ event: 'wire', row }))
const socketFactory = defaultSocketFactory()
const kafka = new Kafka({ brokers: config.brokers, clientId: config.clientId, logLevel: logLevel.NOTHING,
  socketFactory: (options) => trace.wrap(socketFactory(options), 'C'),
})
const client = kafka.shareConsumer({
  groupId: config.groupId,
  acknowledgementMode: 'implicit',
  maxRecords: 1,
  maxWaitTimeInMs: 250,
})
const { events } = client
client.on(events.GROUP_JOIN, ({ payload }) =>
  report({ event: 'group_join', memberId: payload.memberId, memberEpoch: payload.memberEpoch })
)
client.on(events.ASSIGNMENT_CHANGED, ({ payload }) =>
  report({
    event: 'assignment',
    memberId: payload.memberId,
    memberEpoch: payload.memberEpoch,
    assignment: payload.assignment,
  })
)
client.on(events.HEARTBEAT, ({ payload }) =>
  report({ event: 'heartbeat', ...payload, assignment: client.assignment(), subscription: client.subscription() })
)
client.on(events.ACKNOWLEDGE, ({ payload }) =>
  report({
    event: 'acknowledge',
    topic: payload.topic,
    partition: payload.partition,
    error: payload.error ? { name: payload.error.name, reason: payload.error.reason, code: payload.error.code, cause: payload.error.cause && { code: payload.error.cause.code } } : null,
    type: payload.type,
    firstOffset: payload.firstOffset,
    lastOffset: payload.lastOffset,
    reason: payload.error ? payload.error.reason : null,
  })
)
client.on(events.CRASH, ({ payload }) =>
  report({ event: 'crash', message: String(payload.error && payload.error.message) })
)
const handle = async ({ topic, partition, message, renew }) => {
  const record = {
    topic,
    partition,
    offset: message.offset,
    key: message.key,
    value: message.value,
    deliveryCount: message.deliveryCount,
  }
  const invocation = journal.start(record)
  const identity = shareRecordIdentity(record)
  report({ event: 'start', invocation, identity, deliveryCount: message.deliveryCount })
  if (hold) {
    const mine = hold
    hold = null
    let timer = null
    await new Promise((resolve) => {
      current = { invocation, release: resolve }
      if (mine.renew) timer = setInterval(() => renew().catch(() => {}), ${RENEW_MS})
      report({ event: 'held', invocation, identity, deliveryCount: message.deliveryCount })
    })
    clearInterval(timer)
    current = null
  } else {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  journal.finish(invocation)
  report({ event: 'finish', invocation })
}
const stop = async () => {
  try {
    if (current) current.release()
    await client.disconnect({ timeout: 3000 })
    await trace.join()
  } finally {
    journal.close()
    report({ event: 'stopped' })
    process.disconnect()
  }
}
process.on('message', (message) => {
  if (message.cmd === 'hold') hold = { renew: Boolean(message.renew) }
  else if (message.cmd === 'release' && current) current.release()
  else if (message.cmd === 'stop') stop()
})
report({ event: 'ready', nonce: config.nonce, pid: process.pid, clientId: config.clientId })
;(async () => {
  await client.connect()
  await client.subscribe({ topics: [config.topic] })
  await client.run({ eachMessage: handle })
  report({ event: 'running' })
})().catch((error) => {
  report({ event: 'failed', message: String(error && error.stack) })
  process.exitCode = 1
  process.disconnect()
})
`

/**
 * @param {Error} error a failed admin call
 * @returns {boolean} whether it only says the group coordinator is loading or moving (14, 15, 16)
 */
const moving = (error) => [14, 15, 16].includes(error?.code ?? error?.cause?.code)

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
 * @param {Array<{topic: string, partitions: number[]}>} assignment
 * @returns {Array<{topic: string, partitions: number[]}>} Local arrays with sorted partitions
 *   and topics, so IPC and in-process sources compare by value across realms.
 */
const normalize = (assignment) =>
  [...assignment]
    .map(({ topic, partitions }) => ({ topic, partitions: [...partitions].sort((a, b) => a - b) }))
    .sort((a, b) => a.topic.localeCompare(b.topic))

/** Compares change-only assignment events with a current admin member description.
 * @param {object} described Current admin member.
 * @param {object} event Last actual assignment-change event.
 * @param {string} topicId Owned topic UUID.
 * @returns {boolean} Exact member/assignment equality with a nonfuture event epoch.
 */
const assignmentAgrees = (described, event, topicId) =>
  Boolean(
    described &&
    event &&
    described.memberId === event.memberId &&
    Number.isInteger(event.memberEpoch) &&
    event.memberEpoch > 0 &&
    Number.isInteger(described.memberEpoch) &&
    event.memberEpoch <= described.memberEpoch &&
    described.assignment.every((row) => row.topicId === topicId) &&
    isDeepStrictEqual(normalize(event.assignment), normalize(described.assignment))
  )

/**
 * One in-process share member with everything it observed. Its handler does a little work, or, once
 * armed, holds its next invocation (renewing the lock) until the case opens the gate.
 */
class Member {
  /**
   * @param {object} options
   * @param {Kafka} options.kafka the client factory, whose sockets the case counts
   * @param {string} options.label short name in evidence
   * @param {string} options.groupId the share group
   * @param {number} [options.workMs=5] how long an ordinary handler takes
   */
  constructor({ kafka, label, groupId, workMs = 5 }) {
    this.label = label
    this.groupId = groupId
    this.workMs = workMs
    this.config = { maxRecords: 1, maxWaitTimeInMs: 250, acknowledgementMode: 'implicit' }
    this.client = kafka.shareConsumer({ groupId, ...this.config })
    /** @type {Array<{identity: string, deliveryCount: number, offset: string, at: number}>} */
    this.entries = []
    this.inflightEntries = new Set()
    this.handled = 0
    this.gates = []
    this.hold = null
    this.held = null
    this.acks = []
    this.requests = []
    this.assignments = []
    this.joins = []
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
    keep('ASSIGNMENT_CHANGED', this.assignments)
    keep('GROUP_JOIN', this.joins)
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
   * Arms the member to hold its next invocation until the returned gate opens.
   * @returns {{ promise: Promise<void>, open: () => void }} the gate that releases it
   */
  arm() {
    const armed = gate()
    this.gates.push(armed)
    this.hold = armed
    this.held = null
    return armed
  }

  /**
   * Records the delivery before anything else can happen to it, then works or holds.
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
    this.inflightEntries.add(entry)
    try {
      const { hold } = this
      if (hold) {
        this.hold = null
        this.held = entry
        const timer = setInterval(() => {
          try {
            payload.renew().catch(() => {})
          } catch {
            // A revoked call refuses renewal synchronously; there is nothing left to renew.
            clearInterval(timer)
          }
        }, RENEW_MS)
        try {
          await hold.promise
        } finally {
          clearInterval(timer)
        }
      } else {
        await new Promise((resolve) => setTimeout(resolve, this.workMs))
      }
      this.handled++
    } finally {
      this.inflightEntries.delete(entry)
    }
  }

  /**
   * @returns {string[]} identities whose handler has started and not returned
   */
  inflight() {
    return [...this.inflightEntries].map(({ identity }) => identity)
  }

  /**
   * @returns {string[]} every delivered identity, in delivery order, one element per delivery
   */
  identities() {
    return this.entries.map(({ identity }) => identity)
  }

  /**
   * @returns {void} opens every gate this member armed, so no handler is left pending
   */
  releaseAll() {
    for (const armed of this.gates) armed.open()
  }

  /**
   * @returns {void} removes every listener this member added
   */
  unlisten() {
    for (const remove of this.removers.splice(0)) remove()
  }
}

/**
 * The member that runs in its own process, so the case can kill it for real. Everything it reports
 * is kept as a fact; its durable journal, not its IPC, is what a held set is read from.
 */
class ChildMember {
  /**
   * @param {object} options
   * @param {string} options.label short name in evidence
   * @param {string} options.groupId the share group
   * @param {string} options.topic the topic to subscribe to
   * @param {string} options.clientId the client id the broker sees
   * @param {string} options.journal fresh path of the child's handler journal
   * @param {string} options.nonce proves a message came from this child
   */
  constructor({ label, groupId, topic, clientId, journal, nonce }) {
    Object.assign(this, { label, groupId, topic, clientId, journal, nonce })
    this.messages = []
    this.unfinished = new Map()
    this.starts = []
    this.assignments = []
    this.acks = []
    this.wireAcknowledgements = []
    this.failure = null
    this.exit = null
    this.child = null
    this.output = { stdout: '', stderr: '' }
    this.closed = null
    this.expectedExit = false
  }

  /**
   * Spawns the child with an IPC channel and attaches every handler before anything can fail.
   * @returns {void}
   */
  spawn() {
    const config = {
      brokers: plainTextBrokers(),
      groupId: this.groupId,
      topic: this.topic,
      clientId: this.clientId,
      journal: this.journal,
      nonce: this.nonce,
    }
    const child = spawn(process.execPath, ['-e', CHILD_SOURCE], {
      env: {
        ...process.env,
        NODE_PATH: [ROOT, process.env.NODE_PATH].filter(Boolean).join(path.delimiter),
        SHARE_LIVE_ROOT: ROOT,
        SHARE_LIVE_CHILD: JSON.stringify(config),
      },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    })
    this.child = child
    this.closed = new Promise((resolve) =>
      child.once('close', (code, signal) => resolve({ code, signal }))
    )
    child.once('error', (error) => {
      this.failure = error
    })
    child.once('exit', (code, signal) => {
      this.exit = { code, signal }
    })
    for (const stream of ['stdout', 'stderr']) {
      child[stream].on('data', (chunk) => {
        this.output[stream] = (this.output[stream] + chunk).slice(-2000)
      })
    }
    child.on('message', (message) => this.receive(message))
  }

  /**
   * Keeps one IPC message and updates the child's running facts.
   * @param {object} message what the child reported
   * @returns {void}
   */
  receive(message) {
    const fact = { ...message, at: Date.now() }
    this.messages.push(fact)
    if (message.event === 'ready') {
      if (message.nonce !== this.nonce || message.pid !== this.child.pid) {
        this.failure = new Error('The child did not identify itself with its nonce and pid')
      }
    } else if (message.event === 'start') {
      this.starts.push(fact)
      this.unfinished.set(message.invocation, message.identity)
    } else if (message.event === 'finish') {
      this.unfinished.delete(message.invocation)
    } else if (message.event === 'assignment') {
      this.assignments.push(fact)
    } else if (message.event === 'wire') {
      this.wireAcknowledgements.push(message.row)
    } else if (message.event === 'acknowledge') {
      this.acks.push(fact)
    } else if (message.event === 'failed' || message.event === 'crash') {
      this.failure = new Error(`The child reported ${message.event}: ${message.message}`)
    }
  }

  /**
   * @param {string} event
   * @returns {object[]} the child's messages of that kind
   */
  of(event) {
    return this.messages.filter((message) => message.event === event)
  }

  /**
   * Waits for a condition on the child's facts, failing at once if the child failed or died.
   * @param {AbortSignal} scope
   * @param {() => boolean} condition
   * @param {string} label what is awaited
   * @param {number} [ms=STEP_MS]
   * @returns {Promise<void>}
   * @throws {Error} the child's failure, an unexpected exit, or the bound
   */
  waitFor(scope, condition, label, ms = STEP_MS) {
    return pollUntil(
      scope,
      ms,
      () => {
        if (this.failure) throw this.failure
        if (this.exit && !this.expectedExit) {
          throw new Error(
            `The child exited early: ${JSON.stringify(this.exit)} ${this.output.stderr}`
          )
        }
        return condition()
      },
      { delayMs: 50, label }
    )
  }

  /**
   * @param {object} command what the child should do
   * @returns {void}
   */
  command(command) {
    this.child.send(command)
  }

  /**
   * Kills this retained child process, once, and waits for it and its streams to close.
   * @param {AbortSignal} scope
   * @returns {Promise<{code: number|null, signal: string|null}>} how it ended
   */
  async kill(scope) {
    this.expectedExit = true
    this.child.kill('SIGKILL')
    return bounded(scope, STEP_MS, () => this.closed)
  }

  /**
   * Ends the child on every path: asks it to stop, then kills it if it does not close in time.
   * @returns {Promise<void>}
   */
  async end() {
    if (!this.child || this.exit) {
      await this.closed
      return
    }
    this.expectedExit = true
    if (this.child.connected) this.child.send({ cmd: 'stop' })
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 8000)
    try {
      await this.closed
    } finally {
      clearTimeout(timer)
    }
  }
}

/**
 * @param {number} stage which 300-record stage
 * @returns {Array<Array<{partition: number, key: Buffer, value: Buffer, timestamp: string}>>} the
 *   stage's records per partition, 100 each, in ascending index order
 */
const stageRecords = (stage) =>
  Array.from({ length: PARTITIONS }, (_, partition) =>
    Array.from({ length: PER_PARTITION }, (_, slot) => {
      const index = stage * STAGE_RECORDS + slot * PARTITIONS + partition
      return {
        partition,
        key: Buffer.from(`membership:${pad(index)}`),
        value: Buffer.from(`membership:value:${index}`),
        timestamp: TIMESTAMP,
      }
    })
  )

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
 * The live case: three members over three partitions with a join, a clean leave while a record is
 * held, and the death of a real child process while it holds one, across 1800 unique records.
 */
class MembershipCase {
  /** Names every owned resource and builds the clients; nothing is created on the cluster yet. */
  constructor() {
    this.nonce = secureRandom()
    this.topic = shareTopicName('membership')
    this.group = `${SHARE_TOPIC_PREFIX}membership-${this.nonce}`
    this.groups = { main: this.group }
    this.sockets = new SocketCounter()
    this.gauge = new ResourceGauge()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.sockets.counts(),
      queueDepth: () => this.gauge.queueDepth(),
      ledgerSize: () => this.gauge.ledgerSize(),
      activeResources: true,
    })
    this.kafka = new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.sockets.factory(),
    })
    this.admin = new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      clientId: `share-membership-admin-${this.nonce}`,
    }).admin()
    this.stack = new CleanupStack()
    this.clientIds = {}
    this.members = {}
    this.expected = []
    this.frozen = {}
    this.dir = null
    this.log = []
    this.handlerExpected = []
    this.handlerActual = []
    this.broker = []
  }

  /**
   * Appends one evidence row stamped with the case, profile, nonce, group alias and budgets.
   * @param {string} event the row's event name
   * @param {object} [fields] what was measured
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: TITLE,
      profile: PROFILE,
      scenario: this.nonce,
      groups: this.groups,
      budgets: { caseMs: CASE_BUDGET_MS, stepMs: STEP_MS, stageMs: STAGE_MS },
      wireVersions: wireVersionsOf(
        Object.values(this.members).flatMap(({ requests }) => requests ?? [])
      ),
      ...fields,
    })
  }

  /** @returns {object} Per-member handler progress and broker acknowledgement outcomes. */
  progress() {
    return {
      members: Object.values(this.members).map((member) => ({
        label: member.label,
        delivered: member.entries?.length ?? member.starts.length,
        handled: member.entries ? member.handled : member.of('finish').length,
        acknowledgements: acknowledgementResults(member.acks),
        crashes: member.crashes?.map(({ error }) => describeError(error)),
      })),
      wireAcknowledgements: this.sockets.acknowledgements.snapshot(),
      childWireAcknowledgements: this.members.C?.wireAcknowledgements ?? [],
      membershipObservation: this.membershipObservation,
    }
  }

  /**
   * Runs the whole case: bounded setup, the six stages, the final oracles, strict cleanup and,
   * only if all of it held, one completion row.
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
          pre = await captureShareCleanupBaseline(this.probe, {
            signal: scope,
            cleanupStack: false,
          })
          await bounded(scope, STEP_MS, () => this.admin.connect())
          await bounded(scope, STEP_MS, async () => {
            await this.admin.describeCluster()
            await this.admin.listGroups()
          })
          base = pre
          this.row('resource-baseline', { resources: summarize(base) })
          await this.stack.run(
            async () => {
              try {
                await this.setup(scope)
                await this.membership(scope)
                await this.finish(scope)
              } catch (error) {
                this.row('work-failed', { error: describeError(error), ...this.progress() })
                throw error
              }
            },
            { budgetMs: CLEANUP_MS }
          )
          after = await assertShareCleanup(
            this.admin,
            [this.group],
            [this.topic],
            base,
            this.probe,
            { signal: scope, disconnectAdmin: true }
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
   * Creates the topic and group, reads the lock, session and heartbeat back before any member can
   * join, and prepares the child's journal directory. Every owned resource is registered for
   * deletion before it is created.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} a refused create or a config not read back
   */
  async setup(scope) {
    const { stack, admin, topic, group } = this
    stack.add('delete topic', (signal) => dropTopic(signal, admin, topic))
    stack.add(`delete group ${group}`, (signal) => dropGroup(signal, admin, group))
    stack.add(`reset config of ${group}`, (signal) =>
      resetGroupConfig(signal, admin, group, GROUP_KEYS)
    )
    this.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'share-membership-'))
    stack.add('remove the journal directory', () =>
      fs.rmSync(this.dir, { recursive: true, force: true })
    )
    await bounded(scope, STEP_MS, () =>
      createTopic({
        topic,
        partitions: PARTITIONS,
        replicas: 1,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    const configs = {
      'share.auto.offset.reset': 'earliest',
      'share.record.lock.duration.ms': LOCK_MS,
    }
    await bounded(scope, STEP_MS, () => setShareGroupConfigs(admin, group, configs))
    const read = await bounded(scope, STEP_MS, () =>
      admin.describeConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.GROUP,
            name: group,
            configNames: ['share.session.timeout.ms', 'share.heartbeat.interval.ms'],
          },
        ],
      })
    )
    const effective = Object.fromEntries(
      read.resources[0].configEntries.map(({ configName, configValue }) => [
        configName,
        configValue,
      ])
    )
    expect(effective).toEqual({
      'share.session.timeout.ms': String(SESSION_MS),
      'share.heartbeat.interval.ms': String(HEARTBEAT_MS),
    })
    this.config = { ...configs, ...effective }
    this.row('config-readback', { configs: this.config })
    const { topics } = await bounded(scope, STEP_MS, () =>
      admin.fetchTopicMetadata({ topics: [topic], useDescribeTopicPartitions: true })
    )
    this.topicId = topics[0].topicId
  }

  /**
   * @param {string} label
   * @returns {string} the client id the broker sees for that member
   */
  clientIdOf(label) {
    this.clientIds[label] = `share-membership-${label.toLowerCase()}-${this.nonce}`
    return this.clientIds[label]
  }

  /**
   * Builds, registers and starts an in-process member.
   * @param {AbortSignal} scope
   * @param {string} label
   * @param {number} [workMs] how long its ordinary handlers take
   * @returns {Promise<Member>} the running member
   */
  async join(scope, label, workMs) {
    const kafka = new Kafka({
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      clientId: this.clientIdOf(label),
      socketFactory: this.sockets.factory(label),
    })
    const member = new Member({ kafka, label, groupId: this.group, workMs })
    this.members[label] = member
    member.listen()
    this.stack.add(`remove ${label} listeners`, () => member.unlisten())
    this.stack.add(`disconnect ${label}`, () => member.client.disconnect({ timeout: 5000 }))
    this.stack.add(`release ${label} gates`, () => member.releaseAll())
    await member.start(scope, this.topic)
    return member
  }

  /**
   * Builds, registers and starts the child member, and checks it identified itself.
   * @param {AbortSignal} scope
   * @returns {Promise<ChildMember>} the running child
   * @throws {Error} a child that never reported ready and running
   */
  async joinChild(scope) {
    const child = new ChildMember({
      label: 'C',
      groupId: this.group,
      topic: this.topic,
      clientId: this.clientIdOf('C'),
      journal: path.join(this.dir, 'child.jsonl'),
      nonce: this.nonce,
    })
    this.members.C = child
    this.stack.add('end child', () => child.end())
    child.spawn()
    await child.waitFor(scope, () => child.of('ready').length === 1, 'The child reporting ready')
    await child.waitFor(scope, () => child.of('running').length === 1, 'The child running')
    return child
  }

  /**
   * Appends one stage, one awaited send per partition, and records the identities from the
   * broker's own base offsets.
   * @param {AbortSignal} scope
   * @param {number} stage
   * @returns {Promise<string[]>} the stage's identities, in index order
   * @throws {Error} an append that did not land where the fixed layout puts it
   */
  async produce(scope, stage) {
    if (!this.producer) {
      this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
      this.stack.add('disconnect producer', () => this.producer.disconnect())
      await bounded(scope, STEP_MS, () => this.producer.connect())
    }
    const identities = []
    for (const messages of stageRecords(stage)) {
      const [meta] = await bounded(scope, STEP_MS, () =>
        this.producer.send({ topic: this.topic, acks: -1, messages })
      )
      expect(meta.errorCode).toBe(0)
      expect(meta.baseOffset).toBe(String(stage * PER_PARTITION))
      messages.forEach(({ key, value, partition }, slot) => {
        const identity = shareRecordIdentity({
          topic: this.topic,
          partition,
          offset: String(stage * PER_PARTITION + slot),
          key,
          value,
        })
        identities.push(identity)
      })
    }
    this.expected.push(...identities)
    this.row('produced', { stage, records: identities.length, perPartition: PER_PARTITION })
    return identities
  }

  /**
   * Requires successful Accept coverage for every identity in the completed append prefix.
   * @returns {boolean} Whether every produced identity has a successful broker outcome.
   */
  acceptedPrefix() {
    const accepted = new Set()
    for (const { acks } of Object.values(this.members)) {
      for (const { topic, partition, firstOffset, lastOffset, type, error } of acks) {
        if (topic !== this.topic || type !== 'accept' || error !== null) continue
        for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
          accepted.add(JSON.stringify([partition, String(offset)]))
        }
      }
    }
    const child = new AcknowledgementTrace()
    child.frames = (this.members.C?.wireAcknowledgements ?? []).map(({ response, ...request }) => ({
      request,
      response,
    }))
    const confirmed = new Map(
      Array.from({ length: PARTITIONS }, (_, partition) => [
        partition,
        new Set([
          ...this.sockets.acknowledgements.confirmed(this.group, this.topicId, partition),
          ...child.confirmed(this.group, this.topicId, partition),
        ]),
      ])
    )
    return this.expected.every((identity) => {
      const [, partition, offset] = JSON.parse(identity)
      return (
        accepted.has(JSON.stringify([partition, offset])) && confirmed.get(partition).has(offset)
      )
    })
  }

  /**
   * Checks the completed raw-log prefix and successful Accepts before polling terminal positions.
   * @param {AbortSignal} scope Case scope.
   * @param {number} end Completed records per partition.
   * @returns {Promise<void>} After every partition has known zero lag in both listings.
   */
  async settled(scope, end) {
    await bounded(
      scope,
      STAGE_MS,
      async (stage) => {
        this.assertHealthy()
        const log = await bounded(stage, STEP_MS, () =>
          readLog({ brokers: plainTextBrokers(), topic: this.topic })
        )
        const raw = []
        expect([...log.keys()].sort()).toEqual([0, 1, 2])
        for (const [partition, { endOffset, records }] of log) {
          expect(endOffset).toBe(String(end))
          expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
          raw.push(
            ...records.map(({ offset, key, value }) =>
              shareRecordIdentity({ topic: this.topic, partition, offset, key, value })
            )
          )
        }
        expect(raw.sort()).toEqual([...this.expected].sort())
        await pollUntil(
          stage,
          STAGE_MS,
          () => {
            this.assertHealthy()
            return this.acceptedPrefix()
          },
          { label: 'Successful prefix Accept coverage' }
        )
        const position = await awaitTerminalSharePosition(
          this.admin,
          {
            groupId: this.group,
            topic: this.topic,
            partitions: [...log].map(([partition, { endOffset }]) => ({
              partition,
              floor: '0',
              end: endOffset,
            })),
          },
          {
            signal: stage,
            timeoutMs: STAGE_MS,
            record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
          }
        )
        this.broker = position.rows
      },
      { label: `Every partition settling through ${end}` }
    )
  }

  /**
   * @returns {void}
   * @throws {Error} a crash of an in-process member or any failure of the child
   */
  assertHealthy() {
    for (const member of Object.values(this.members)) {
      if (member instanceof ChildMember) {
        if (member.failure) throw member.failure
      } else {
        expect(member.crashes).toEqual([])
      }
    }
  }

  /**
   * Waits until the admin description and every live member's own events name the same members
   * and the same assignments, then records them. Assignments are compared per member and may
   * overlap. Assignment events occur only when the resolved set changes, so their epochs may lag.
   * @param {AbortSignal} scope
   * @param {string[]} labels the members that must be in the group, and only those
   * @param {string} stage what this convergence is for
   * @returns {Promise<object>} per member: broker member id, epoch and normalized assignment
   * @throws {Error} sources that never agree
   */
  async converge(scope, labels, stage) {
    let seen
    const last = {
      stage,
      expectedClients: Object.fromEntries(labels.map((label) => [label, this.clientIds[label]])),
      description: null,
      members: {},
      error: null,
    }
    this.membershipObservation = last
    await pollSharePosition(
      scope,
      STAGE_MS,
      async () => {
        this.assertHealthy()
        let members
        last.error = null
        last.members = Object.fromEntries(
          labels.map((label) => {
            const member = this.members[label]
            return [
              label,
              {
                join:
                  member instanceof ChildMember
                    ? member.of('group_join').at(-1)
                    : member.joins.at(-1),
                event: member.assignments.at(-1),
                publicAssignment:
                  member instanceof ChildMember
                    ? member.of('heartbeat').at(-1)?.assignment
                    : member.client.assignment(),
                heartbeat:
                  member instanceof ChildMember ? member.of('heartbeat').at(-1) : undefined,
              },
            ]
          })
        )
        try {
          last.description = await this.admin.describeShareGroups([this.group])
          members = last.description.groups[0].members
        } catch (error) {
          last.error = describeError(error)
          if (moving(error)) return false
          throw error
        }
        const byClient = new Map(members.map((member) => [member.clientId, member]))
        if (members.length !== labels.length) return false
        const result = {}
        for (const label of labels) {
          const described = byClient.get(this.clientIds[label])
          const event = this.members[label].assignments.at(-1)
          if (!assignmentAgrees(described, event, this.topicId)) return false
          const fromAdmin = normalize(described.assignment)
          result[label] = {
            memberId: described.memberId,
            epoch: described.memberEpoch,
            assignmentEventEpoch: event.memberEpoch,
            assignment: fromAdmin,
          }
        }
        const owned = new Set(
          Object.values(result).flatMap(({ assignment }) =>
            assignment.flatMap(({ partitions }) => partitions)
          )
        )
        seen = result
        return owned.size === PARTITIONS
      },
      {
        delayMs: 250,
        label: `Admin and events agreeing on ${labels.join('+')}`,
        snapshot: () => last,
        record: (fields) => this.row('membership-observation', fields),
      }
    )
    this.row('assignment', { stage, members: seen })
    return seen
  }

  /**
   * The six stages: a lone member, a join, a third member that is a child process, a clean leave
   * with a held record, the child's death with a held record, and the survivor finishing alone.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async membership(scope) {
    const a = await this.join(scope, 'A', 20)
    await this.converge(scope, ['A'], 'A alone')
    await this.produce(scope, 0)
    await this.settled(scope, PER_PARTITION)

    const b = await this.join(scope, 'B', 20)
    await this.converge(scope, ['A', 'B'], 'B joined')
    await this.produce(scope, 1)
    await this.settled(scope, 2 * PER_PARTITION)
    expect(a.entries.length).toBeGreaterThan(0)
    expect(b.entries.length).toBeGreaterThan(0)

    const c = await this.joinChild(scope)
    await this.converge(scope, ['A', 'B', 'C'], 'C joined')
    const bGate = b.arm()
    c.command({ cmd: 'hold', renew: true })
    await this.produce(scope, 2)
    await c.waitFor(
      scope,
      () => c.of('held').length === 1,
      'The child holding an invocation',
      STAGE_MS
    )
    await pollUntil(scope, STAGE_MS, () => b.held !== null, {
      delayMs: 50,
      label: 'B holding an invocation',
    })

    this.frozen.cleanHandoverHeld = [b.held.identity]
    this.frozen.otherAtLeave = [
      ...b.inflight().filter((identity) => identity !== b.held.identity),
      ...a.inflight(),
      ...c.unfinished.values(),
    ]
    this.row('held-set', {
      name: 'cleanHandoverHeld',
      identities: this.frozen.cleanHandoverHeld,
      otherHeldAtRecordedHandover: this.frozen.otherAtLeave,
    })
    const leaveCalledAt = Date.now()
    await bounded(scope, STEP_MS, () => b.client.disconnect({ timeout: CLEAN_LEAVE_MS }))
    const leftAt = Date.now()
    this.row('acknowledgement', {
      name: 'cleanLeaveChronology',
      leaveCalledAt,
      leftAt,
      acknowledgements: b.acks
        .filter(({ at }) => at >= leaveCalledAt - 100)
        .map(({ type, firstOffset, lastOffset, error, at }) => ({
          type,
          firstOffset,
          lastOffset,
          at,
          error: describeError(error),
        })),
    })
    await this.converge(scope, ['A', 'C'], 'B left')
    c.command({ cmd: 'release' })
    await c.waitFor(scope, () => c.unfinished.size === 0, 'The child finishing its held invocation')
    const releasedAt = Date.now()
    bGate.open()
    await this.awaitReleased(scope, this.frozen.cleanHandoverHeld, Math.max(leftAt, releasedAt))
    await this.settled(scope, 3 * PER_PARTITION)

    c.command({ cmd: 'hold', renew: false })
    const heldBefore = c.of('held').length
    await this.produce(scope, 3)
    await c.waitFor(
      scope,
      () => c.of('held').length === heldBefore + 1,
      'The child holding before death',
      STAGE_MS
    )
    const fault = await this.killChild(scope, a, c)
    await this.converge(scope, ['A'], 'C dead')
    await this.awaitReturned(scope, a, fault)
    await this.settled(scope, 4 * PER_PARTITION)

    await this.produce(scope, 4)
    await this.produce(scope, 5)
    await this.settled(scope, STAGES * PER_PARTITION)
  }

  /**
   * Waits until every identity a clean leave released is delivered again, to A or to the child,
   * within the allowance once nothing blocks the survivors.
   * @param {AbortSignal} scope
   * @param {string[]} identities the identities B held when it left
   * @param {number} from the moment nothing blocks the survivors
   * @returns {Promise<void>}
   * @throws {Error} an identity not delivered again in time
   */
  async awaitReleased(scope, identities, from) {
    const delivered = () => [
      ...this.members.A.entries,
      ...this.members.C.starts.map(({ identity, deliveryCount, at }) => ({
        identity,
        deliveryCount,
        at,
      })),
    ]
    await pollUntil(
      scope,
      RELEASE_WITHIN_MS + 4000,
      () =>
        identities.every((identity) =>
          delivered().some((entry) => entry.identity === identity && entry.deliveryCount >= 2)
        ),
      { delayMs: 50, label: 'The records B held being delivered again' }
    )
    const latest = Math.max(
      ...identities.map((identity) =>
        Math.min(
          ...delivered()
            .filter((entry) => entry.identity === identity && entry.deliveryCount >= 2)
            .map((entry) => entry.at)
        )
      )
    )
    this.row('held-set', { name: 'cleanLeaveRelease', identities, latencyMs: latest - from })
    expect(latest - from).toBeLessThanOrEqual(RELEASE_WITHIN_MS)
  }

  /**
   * Freezes what the child holds from its durable journal once it agrees with the child's own
   * reports, kills that exact child process once, then reads the journal again: what a dead child
   * held is what its journal says, and it must include what was frozen.
   * @param {AbortSignal} scope
   * @param {Member} a the survivor
   * @param {ChildMember} c the child
   * @returns {Promise<{killedAt: number, held: string[]}>} the kill time and the identities the
   *   journal says the child held when it died
   * @throws {Error} a journal that never agrees with the child's reports, or a child that did not
   *   die by SIGKILL
   */
  async killChild(scope, a, c) {
    let frozen
    await pollUntil(
      scope,
      STEP_MS,
      () => {
        this.assertHealthy()
        try {
          frozen = ShareHandlerLog.read(c.journal)
        } catch {
          return false
        }
        const reported = [...c.unfinished.values()]
        return frozen.held.length > 0 && isDeepStrictEqual(frozen.held, reported)
      },
      { delayMs: 25, label: 'The child journal and its reports agreeing on what it holds' }
    )
    const ready = c.of('ready')[0]
    expect(ready.nonce).toBe(this.nonce)
    expect(ready.pid).toBe(c.child.pid)
    this.frozen.otherAtKill = a.inflight()
    const killedAt = Date.now()
    const outcome = await c.kill(scope)
    expect(outcome.signal).toBe('SIGKILL')
    const final = ShareHandlerLog.read(c.journal)
    for (const identity of frozen.held) expect(final.held).toContain(identity)
    this.frozen.killedHeld = final.held
    this.row('held-set', {
      name: 'killedHeld',
      frozenBeforeKill: frozen.held,
      identities: final.held,
      otherHeldAtRecordedHandover: this.frozen.otherAtKill,
      journalRows: final.rows.length,
    })
    this.row('fault', { pid: c.child.pid, nonce: this.nonce, signal: outcome.signal, killedAt })
    return { killedAt, held: final.held }
  }

  /**
   * Waits until every identity the dead child held is delivered to A again, and records how long
   * that took after the kill.
   * @param {AbortSignal} scope
   * @param {Member} a the survivor
   * @param {{killedAt: number, held: string[]}} fault what was killed
   * @returns {Promise<void>}
   * @throws {Error} an identity not delivered again within the lock, the session timeout and slack
   */
  async awaitReturned(scope, a, { killedAt, held }) {
    const back = (identity) =>
      a.entries.find((entry) => entry.identity === identity && entry.at > killedAt)
    await pollUntil(scope, CHILD_RETURN_MS + 5000, () => held.every((identity) => back(identity)), {
      delayMs: 100,
      label: 'The identities the dead child held returning to A',
    })
    const latencies = held.map((identity) => back(identity).at - killedAt)
    this.row('held-set', { name: 'childReplay', identities: held, latencyMs: latencies })
    for (const latency of latencies) expect(latency).toBeLessThanOrEqual(CHILD_RETURN_MS)
    for (const identity of held) expect(back(identity).deliveryCount).toBeGreaterThanOrEqual(2)
  }

  /**
   * The final oracles, before cleanup: raw log, broker positions and handler multiplicities with
   * the duplicates limited to the frozen held sets, then a quiet same-group verifier.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async finish(scope) {
    const { A: a, B: b, C: c } = this.members
    expect(this.expected).toHaveLength(STAGES * STAGE_RECORDS)
    const log = await bounded(scope, STEP_MS, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const appended = []
    for (const [partition, { endOffset, records }] of log) {
      expect(endOffset).toBe(String(STAGES * PER_PARTITION))
      expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
      for (const { offset, key, value } of records) {
        appended.push(shareRecordIdentity({ topic: this.topic, partition, offset, key, value }))
      }
    }
    expect([...appended].sort()).toEqual([...this.expected].sort())
    this.log = appended

    const journal = ShareHandlerLog.read(c.journal)
    const fromChild = journal.rows
      .filter(({ event }) => event === 'start')
      .map(({ identity }) => identity)
    expect(fromChild.length).toBeGreaterThanOrEqual(c.starts.length)
    this.row('held-set', {
      name: 'childJournal',
      sha256: crypto.createHash('sha256').update(fs.readFileSync(c.journal)).digest('hex'),
      rows: journal.rows,
    })
    const delivered = [...a.identities(), ...b.identities(), ...fromChild]
    const counts = new Map()
    for (const identity of delivered) counts.set(identity, (counts.get(identity) ?? 0) + 1)
    const allowed = new Set([
      ...this.frozen.killedHeld,
      ...this.frozen.cleanHandoverHeld,
      ...this.frozen.otherAtLeave,
      ...this.frozen.otherAtKill,
    ])
    for (const identity of this.expected) {
      const count = counts.get(identity)
      expect(count).toBeGreaterThanOrEqual(1)
      expect(count).toBeLessThanOrEqual(allowed.has(identity) ? 2 : 1)
    }
    expect(counts.size).toBe(this.expected.length)
    const duplicated = this.expected.filter((identity) => counts.get(identity) === 2)
    for (const identity of [...this.frozen.killedHeld, ...this.frozen.cleanHandoverHeld]) {
      expect(duplicated).toContain(identity)
    }
    this.handlerExpected = [...this.expected, ...duplicated]
    this.handlerActual = delivered
    for (const member of [a, b]) expect(member.crashes).toEqual([])
    this.row('held-set', {
      name: 'allowedDuplicates',
      killedHeld: this.frozen.killedHeld,
      cleanHandoverHeld: this.frozen.cleanHandoverHeld,
      otherHeldAtRecordedHandover: [...this.frozen.otherAtLeave, ...this.frozen.otherAtKill],
      duplicated,
    })

    this.row('acknowledgement', {
      A: a.acks
        .filter(({ error }) => error !== null)
        .map(({ firstOffset, lastOffset, error }) => ({
          firstOffset,
          lastOffset,
          error: describeError(error),
        })),
      B: b.acks
        .filter(({ error }) => error !== null)
        .map(({ firstOffset, lastOffset, error }) => ({
          firstOffset,
          lastOffset,
          error: describeError(error),
        })),
      C: c.acks.filter(({ reason }) => reason !== null),
    })

    await bounded(scope, STEP_MS, () => a.client.disconnect({ timeout: 5000 }))
    await bounded(scope, STEP_MS, () => b.client.disconnect({ timeout: 5000 }))
    await bounded(scope, STEP_MS, () => c.end())
    await this.sockets.acknowledgements.join()
    this.quiet = await assertQuietTerminalGroup({
      kafka: this.kafka,
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions: [...log].map(([partition, { endOffset }]) => ({
        partition,
        floor: '0',
        end: endOffset,
      })),
      signal: scope,
      timeoutMs: STAGE_MS,
      record: (fields) => this.row('quiet-verifier', fields),
    })
    this.broker = this.quiet.position.rows
    const ends = await bounded(scope, STEP_MS, () =>
      endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect([...ends]).toEqual([...log].map(([partition, { endOffset }]) => [partition, endOffset]))
  }

  /**
   * Writes the one completion row, only after every assertion and the strict cleanup passed.
   * @param {object} before the baseline snapshot
   * @param {object} after the snapshot after cleanup
   * @returns {void}
   */
  complete(before, after) {
    const requests = [...this.members.A.requests, ...this.members.B.requests]
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: this.expected, actual: this.log },
        handler: { expected: this.handlerExpected, actual: this.handlerActual },
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
        group: this.config,
        members: { A: this.members.A.config, B: this.members.B.config, C: { maxRecords: 1 } },
      },
    })
  }
}

enabled('share membership on a live broker', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_BUDGET_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_BUDGET_MS
  )

  it(
    TITLE,
    liveCase(CASE_BUDGET_MS, (scope) => new MembershipCase().run(scope)),
    caseTimeout(CASE_BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})

describe('membership assignment observation', () => {
  const member = () => ({
    memberId: 'original-member',
    memberEpoch: 5,
    assignment: [{ topic: 'records', topicId: 'original-topic', partitions: [2] }],
  })
  const event = () => ({
    memberId: 'original-member',
    memberEpoch: 4,
    assignment: [{ topic: 'records', partitions: [2] }],
  })

  it('accepts an unchanged assignment whose last event precedes the admin epoch', () => {
    expect(assignmentAgrees(member(), event(), 'original-topic')).toBe(true)
    expect(assignmentAgrees(member(), { ...event(), memberEpoch: 5 }, 'original-topic')).toBe(true)
  })

  it('compares child IPC assignments by value across realms', () => {
    const child = new ChildMember({ label: 'C' })
    const message = runInNewContext('JSON.parse(input)', {
      input: JSON.stringify({ ...event(), event: 'assignment' }),
    })
    child.receive(message)
    const observed = child.assignments.at(-1)

    expect(assignmentAgrees(member(), observed, 'original-topic')).toBe(true)
    observed.assignment[0].partitions = [0]
    expect(assignmentAgrees(member(), observed, 'original-topic')).toBe(false)
  })

  it.each([
    [
      'future epoch',
      (row) => {
        row.memberEpoch = 6
      },
    ],
    [
      'missing epoch',
      (row) => {
        delete row.memberEpoch
      },
    ],
    [
      'different member',
      (row) => {
        row.memberId = 'replacement-member'
      },
    ],
    [
      'different assignment',
      (row) => {
        row.assignment[0].partitions = [0]
      },
    ],
  ])('rejects %s', (name, mutate) => {
    const observed = event()
    mutate(observed)
    expect(assignmentAgrees(member(), observed, 'original-topic')).toBe(false)
  })

  it('rejects a foreign topic UUID', () => {
    expect(assignmentAgrees(member(), event(), 'foreign-topic')).toBe(false)
  })
})
