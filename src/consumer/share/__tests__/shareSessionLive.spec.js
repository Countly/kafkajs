const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const {
  acknowledgementResults,
  acknowledgementRequests,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  terminalSharePosition,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const path = require('path')
const { spawn } = require('child_process')
const { isDeepStrictEqual } = require('util')
const { AsyncLocalStorage } = require('async_hooks')
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
const shareFetchResponse = require('../../../protocol/requests/shareFetch/v2/response')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareRun } = require('../shareRun')
const { ShareRunner } = require('../shareRunner')
const { KafkaJSShareAcknowledgementError, KafkaJSRequestTimeoutError } = require('../../../errors')
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
      const { apiKey, apiVersion, correlationId, clientId, byteLength } = readRequestHeader(frame)
      Object.assign(fact, { apiKey, apiVersion, correlationId, clientId, byteLength })
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

/** Expands a validated acknowledgement projection without losing duplicate offset charges.
 * @param {object[]} batches Original acknowledgement batches.
 * @param {number} type Required wire type.
 * @returns {string[]} Decimal offsets, including multiplicity.
 */
const decisionOffsets = (batches, type) => {
  const offsets = []
  for (const { firstOffset, lastOffset, acknowledgeTypes } of batches) {
    if (![firstOffset, lastOffset].every((value) => /^(0|[1-9][0-9]*)$/.test(value)))
      throw new Error('Noncanonical acknowledgement offset')
    const size = BigInt(lastOffset) - BigInt(firstOffset) + 1n
    if (size < 1n || size > BigInt(RECORDS)) throw new Error('Invalid acknowledgement width')
    if (acknowledgeTypes.length !== 1 && BigInt(acknowledgeTypes.length) !== size)
      throw new Error('Incomplete acknowledgement types')
    for (let index = 0; BigInt(index) < size; index++) {
      const decision = acknowledgeTypes.length === 1 ? acknowledgeTypes[0] : acknowledgeTypes[index]
      if (![0, 1, 2, 3, 4].includes(decision)) throw new Error('Invalid acknowledgement type')
      if (decision === type) offsets.push(String(BigInt(firstOffset) + BigInt(index)))
    }
  }
  if (new Set(offsets).size !== offsets.length) throw new Error('Duplicate acknowledgement offset')
  return offsets
}

/** Observes original authority and public results; every wrapper calls the original unchanged. */
class SettlementCapture {
  /** @param {SessionCase} owner Owning isolated arm. */
  constructor(owner) {
    this.owner = owner
    this.context = new AsyncLocalStorage()
    this.labels = new Map()
    this.restores = []
    this.handlers = []
    this.loops = new Map()
    this.assignments = []
    this.decisions = []
    this.outcomes = []
    this.public = []
    this.writes = []
    this.acquisitions = []
    this.losses = []
    this.faults = []
    this.sequence = 0
  }

  /** @param {*} original Original object or token. @returns {string|null} Stable local label. */
  id(original) {
    if (original == null) return null
    if (!this.labels.has(original)) this.labels.set(original, `original-${this.labels.size + 1}`)
    return this.labels.get(original)
  }

  /** @returns {object} Ordered observation time, independent of millisecond ties. */
  stamp() {
    return { sequence: ++this.sequence, at: Date.now(), monotonic: performance.now() }
  }

  /** @param {object} target Original provider. @param {string} name Method. @param {Function} observe Call-through observer. */
  watch(target, name, observe) {
    const original = target[name]
    target[name] = function (...args) {
      return observe(this, args, () => original.apply(this, args))
    }
    this.restores.push(() => {
      target[name] = original
    })
  }

  /** @returns {void} Installs observations before any workload client starts. */
  install() {
    this.watch(ShareRun.prototype, 'createLoop', (run, [node], next) => {
      const loop = next()
      this.loops.set(loop, { run, node })
      return loop
    })
    this.watch(ShareRun.prototype, 'assignmentChanged', (run, [snapshot], next) => {
      const result = next()
      this.assignments.push({
        run,
        groupId: run.options.groupId,
        snapshot: {
          ...snapshot,
          token: this.id(snapshot.token),
          targetRevision: this.id(snapshot.targetRevision),
          assignment: structuredClone(snapshot.assignment),
          unresolved: [...snapshot.unresolved],
        },
        ...this.stamp(),
      })
      return result
    })
    this.watch(ShareFetchLoop.prototype, 'send', (loop, [exchange], next) =>
      this.context.run({ loop, exchange }, next)
    )
    this.watch(ShareFetchLoop.prototype, 'writeFinal', (loop, [operation, guard, captured], next) =>
      this.context.run({ loop, captured }, next)
    )
    this.watch(ShareFetchLoop.prototype, 'adopt', (loop, [exchange], next) => {
      const cohort = next()
      if (Object.values(this.owner.groups).includes(loop.groupId))
        this.acquisitions.push({ cohort, exchange, ...this.stamp() })
      return cohort
    })
    this.watch(ShareRun.prototype, 'invoke', (run, [node, cohort, call], next) =>
      this.context.run({ run, node, cohort, call }, next)
    )
    this.watch(ShareRunner.prototype, 'newRun', (runner, args, next) => {
      const run = next()
      if (!Object.values(this.owner.groups).includes(run.options.groupId)) return run
      const handler = run.handler
      run.handler = (payload) => {
        const context = this.context.getStore()
        const { topic, partition, message } = payload
        this.handlers.push({
          ...context,
          groupId: run.options.groupId,
          clientId: run.options.clientId,
          memberId: run.membership.memberId,
          identity: shareRecordIdentity({ ...message, topic, partition }),
          offset: message.offset,
          deliveryCount: message.deliveryCount,
          ...this.stamp(),
        })
        return handler.call(run, payload)
      }
      this.restores.push(() => {
        run.handler = handler
      })
      return run
    })
    for (const method of ['decide', 'complete', 'completeRange']) {
      this.watch(ShareAckLedger.prototype, method, (ledger, args, next) => {
        const result = next()
        const offsets =
          method === 'decide'
            ? [args[0]]
            : method === 'complete'
              ? args[0]
              : args[0].slice(args[1], args[2]).map(({ offset }) => offset)
        const type = method === 'completeRange' ? args[3] : (args[1] ?? 'accept')
        for (const offset of offsets)
          this.decisions.push({ ledger, offset, type, method, ...this.stamp() })
        return result
      })
    }
    this.watch(ShareRunner.prototype, 'publishResults', (runner, [cohort, results], next) => {
      if (Object.values(this.owner.groups).includes(runner.options.groupId))
        for (const result of results)
          this.outcomes.push({ cohort, result, groupId: runner.options.groupId, ...this.stamp() })
      return next()
    })
    this.watch(ShareFetchLoop.prototype, 'failExchange', (loop, [exchange, cause], next) => {
      for (const write of this.writes.filter((write) => write.exchange === exchange))
        write.failure = cause
      return next()
    })
    this.watch(ShareFetchLoop.prototype, 'onSessionReset', (loop, [open, reason, cause], next) => {
      this.losses.push({ loop, open, reason, cause, ...this.stamp() })
      return next()
    })
    const hook = (fact) => this.frame(fact)
    this.owner.tap.hooks.add(hook)
    this.restores.push(() => this.owner.tap.hooks.delete(hook))
  }

  /** @param {object} fact Complete original frame below the fault layer. @returns {void} */
  frame(fact) {
    Object.assign(fact, this.stamp())
    if (fact.kind !== 'request' || ![78, 79].includes(fact.apiKey)) return
    const context = this.context.getStore()
    if (!context?.loop) throw new Error('Share frame has no original exchange')
    const { loop, captured } = context
    const exchange =
      context.exchange ??
      [...loop.exchanges].find(
        (entry) =>
          entry.captured &&
          entry.prepared === captured.prepared &&
          entry.binding === captured.binding
      )
    if (!exchange) throw new Error('Missing original captured final exchange')
    const descriptor = captured?.descriptor ?? exchange.descriptor
    const binding = exchange.binding
    const socket = binding.connection.socket
    if ((socket.inner ?? socket) !== fact.ref.socket) throw new Error('Foreign exchange socket')
    const reader = fact.apiKey === 78 ? shareFetchWire : shareAcknowledgeWire
    const fields = reader.readRequest(fact.frame.subarray(fact.byteLength))
    expect(fields.groupId).toBe(loop.groupId)
    expect(fields.memberId).toBe(loop.memberId)
    expect(fields.shareSessionEpoch).toBe(descriptor.epoch)
    expect(fields.topics).toEqual((captured?.fields ?? exchange.fields).topics)
    const write = {
      loop,
      ...this.loops.get(loop),
      exchange,
      descriptor,
      fact,
      fields,
      binding: { ...binding },
      socket,
      open: captured?.descriptor.token ?? exchange.open,
      membership: loop.authority.membershipToken,
      prepared: exchange.prepared.map((entry) => ({ ...entry })),
      ...this.stamp(),
    }
    fact.write = write
    this.writes.push(write)
  }

  /** @param {Member} member Owning public listener. @param {object} payload Actual event. @returns {void} */
  publicResult(member, payload) {
    const match = this.outcomes.find(
      (outcome) =>
        outcome.groupId === member.groupId &&
        !this.public.some((row) => row.outcome === outcome) &&
        ['type', 'firstOffset', 'lastOffset', 'error'].every(
          (key) => outcome.result[key] === payload[key]
        ) &&
        outcome.cohort.topic === payload.topic &&
        outcome.cohort.partition === payload.partition
    )
    this.public.push({ outcome: match, payload, label: member.label, ...this.stamp() })
  }

  /** Freezes unresolved actual handler attempts and the exact in-flight written exchanges.
   * @param {string} kind Prescribed interruption.
   * @param {object[]} writes Original affected requests, never a later offset-derived pool.
   * @returns {object} Immutable fault eligibility observations.
   */
  freeze(kind, writes) {
    const handlers = this.handlers.filter(
      (handler) =>
        !this.outcomes.some(
          ({ cohort, result }) =>
            cohort === handler.cohort &&
            result.type !== 'renew' &&
            offsetsOf([result]).includes(handler.offset)
        )
    )
    const fault = { kind, handlers: [...handlers], writes: [...writes], ...this.stamp() }
    this.faults.push(fault)
    return fault
  }

  /** @returns {Promise<void>} Decodes every complete original answer, including swallowed acquisitions. */
  async refresh() {
    for (const write of this.writes) {
      const response = this.owner.tap.responseTo(write.fact)
      if (!response || write.answer) continue
      const decoder = write.fact.apiKey === 78 ? fetchAcknowledgementResponse : acknowledgeResponse
      if (response.frame[8] !== 0) throw new Error('Unexpected response header tags')
      write.response = response
      write.answer = await decoder.decode(response.frame.subarray(9))
    }
  }

  /** @returns {void} Restores every original provider after clients have joined. */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
  }
}

/** Returns the independent acknowledgement verdict; fetch status cannot replace it.
 * @param {object} write Original request, prepared ticket and response.
 * @param {object} cohort Original acquisition.
 * @returns {number|undefined} Known broker code, or absent when no answer exists.
 */
const acknowledgementCode = (write, cohort) => {
  if (!write.answer) return undefined
  if (!Number.isInteger(write.answer.errorCode))
    throw new Error('Missing top-level acknowledgement result')
  if (write.answer.errorCode !== 0) return write.answer.errorCode
  const topics = write.answer.responses.filter(({ topicId }) => topicId === cohort.topicId)
  if (topics.length !== 1) throw new Error('Missing or duplicate acknowledgement topic')
  const partitions = topics[0].partitions.filter(
    ({ partitionIndex }) => partitionIndex === cohort.partition
  )
  if (partitions.length !== 1) throw new Error('Missing or duplicate acknowledgement partition')
  const code =
    write.fact.apiKey === 78 ? partitions[0].acknowledgeErrorCode : partitions[0].errorCode
  if (!Number.isInteger(code)) throw new Error('Missing acknowledgement partition result')
  return code
}

/** Validates the full physical request against its original ticket and authority.
 * @param {object} write Captured transmitted exchange.
 * @param {object} prepared Original prepared cohort/ledger/ticket link.
 * @param {object} handler Actual handler attempt.
 * @returns {boolean} Whether that exact written request carried this Accept.
 */
const writtenAccept = (write, prepared, handler) => {
  const { cohort, offset, groupId } = handler
  const { fact } = write
  if (
    !Buffer.isBuffer(fact.frame) ||
    fact.frame.length < 12 ||
    fact.frame.length !== fact.frame.readInt32BE(0) + 4 ||
    ![78, 79].includes(fact.apiKey) ||
    fact.apiVersion !== 2 ||
    !write.prepared.includes(prepared) ||
    prepared.row !== cohort ||
    prepared.ledger !== cohort.ledger ||
    (write.socket.inner ?? write.socket) !== fact.ref.socket ||
    (write.exchange.open ?? write.descriptor.token) !== write.open ||
    write.exchange.binding.connection !== write.binding.connection ||
    write.exchange.sentAt === undefined ||
    write.fields.groupId !== groupId ||
    write.fields.memberId !== write.loop.memberId ||
    prepared.ticket.isRenewAck ||
    write.fields.isRenewAck
  )
    return false
  const header = readRequestHeader(fact.frame)
  if (
    header.apiKey !== fact.apiKey ||
    header.apiVersion !== fact.apiVersion ||
    header.correlationId !== fact.correlationId ||
    header.byteLength !== fact.byteLength
  )
    return false
  const reader = fact.apiKey === 78 ? shareFetchWire : shareAcknowledgeWire
  const decoded = reader.readRequest(fact.frame.subarray(header.byteLength))
  if (
    !isDeepStrictEqual(decoded, write.fields) ||
    decoded.shareSessionEpoch !== write.descriptor.epoch
  )
    return false
  if (
    decoded.topics.length !== 1 ||
    decoded.topics[0].topicId !== cohort.topicId ||
    decoded.topics[0].partitions.length !== 1
  )
    return false
  const [partition] = decoded.topics[0].partitions
  return (
    partition.partitionIndex === cohort.partition &&
    isDeepStrictEqual(partition.acknowledgementBatches, prepared.ticket.acknowledgementBatches) &&
    decisionOffsets(partition.acknowledgementBatches, 1).includes(offset)
  )
}

/** Checks the original written-Accept chain without converting an unknown outcome to success.
 * @param {object} attempt Original handler, decision, ticket, write, fault and public outcome.
 * @param {object[]} handlers Entire workload/verifier handler journal.
 * @returns {boolean} Provisional eligibility only; terminal proof remains independent.
 */
const qualifyingUnknown = (
  { handler, decision, prepared, write, outcome, publication, fault, loss },
  handlers
) => {
  if (!handler || !decision || !prepared || !write || !outcome || !publication || !fault || !loss)
    return false
  const { cohort, offset } = handler
  const error = outcome.result.error
  return (
    writtenAccept(write, prepared, handler) &&
    fault.handlers.includes(handler) &&
    fault.writes.includes(write) &&
    loss.loop === write.loop &&
    loss.open === write.open &&
    loss.cause === write.failure &&
    loss.reason === 'connection' &&
    loss.sequence > fault.sequence &&
    loss.sequence < outcome.sequence &&
    decision.ledger === cohort.ledger &&
    decision.offset === offset &&
    decision.type === 'accept' &&
    decision.method !== 'decide' &&
    decision.sequence < write.sequence &&
    write.membership === write.loop.authority.membershipToken &&
    handler.sequence < fault.sequence &&
    write.sequence < fault.sequence &&
    outcome.sequence > fault.sequence &&
    outcome.cohort === cohort &&
    publication.outcome === outcome &&
    outcome.result.type === 'accept' &&
    offsetsOf([outcome.result]).includes(offset) &&
    error instanceof KafkaJSShareAcknowledgementError &&
    error.reason === 'session-lost' &&
    error.cause === write.failure &&
    write.failure !== undefined &&
    fault.kind === 'lost-response' &&
    (!write.response || write.response.sequence > fault.sequence) &&
    write.fact.apiKey === 78 &&
    write.failure instanceof KafkaJSRequestTimeoutError &&
    write.failure.correlationId === write.fact.correlationId &&
    write.failure.broker === `${write.fact.host}:${write.fact.port}` &&
    (acknowledgementCode(write, cohort) === undefined ||
      acknowledgementCode(write, cohort) === 0) &&
    !handlers.some(
      (later) =>
        later.groupId === handler.groupId &&
        later.identity === handler.identity &&
        later.sequence > handler.sequence
    )
  )
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
    this.removers.push(
      this.client.on(events.ACKNOWLEDGE, ({ payload }) => {
        this.capture?.publicResult(this, payload)
        this.acks.push({ ...payload, at: Date.now() })
      })
    )
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
    this.capture = new SettlementCapture(this)
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
      faultSettlement: this.settlement,
      archival: this.archival,
      capture: this.captureEvidence(),
      settlement: Object.values(this.groups).map((groupId) => ({
        groupId,
        ...this.missingAccepts(groupId),
      })),
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
    outer.add('release settlement observers', () => this.capture.restore())
    outer.add('disconnect admin', () => this.admin.disconnect())
    let pre
    let base
    let after
    try {
      await outer.run(
        async () => {
          this.gauge.install()
          this.capture.install()
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
    member.capture = this.capture
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

  /** Reads the effective delivery limit on every broker without changing it.
   * @param {AbortSignal} scope Case scope.
   * @returns {Promise<void>} Retained archival-exclusion configuration.
   */
  async deliveryLimits(scope) {
    const { brokers } = await bounded(scope, STEP_MS, () => this.admin.describeCluster())
    this.limits = []
    for (const { nodeId } of brokers) {
      const name = 'group.share.delivery.count.limit'
      const response = await bounded(scope, STEP_MS, () =>
        this.admin.describeConfigs({
          resources: [
            { type: CONFIG_RESOURCE_TYPES.BROKER, name: String(nodeId), configNames: [name] },
          ],
        })
      )
      expect(response.resources).toHaveLength(1)
      expect(response.resources[0].errorCode).toBe(0)
      const entries = response.resources[0].configEntries.filter(
        ({ configName }) => configName === name
      )
      expect(entries).toHaveLength(1)
      expect(entries[0].configValue).toMatch(/^[1-9][0-9]*$/)
      this.limits.push({ nodeId, response, limit: Number(entries[0].configValue) })
    }
    expect(this.limits.length).toBeGreaterThan(0)
    this.row('config-readback', { deliveryLimits: this.limits })
  }

  /** Requires one matching original acquisition before using a handler in a settlement proof.
   * @param {object} handler Actual handler entry.
   * @returns {void} Fails on a fabricated cohort or response-only acquisition.
   */
  assertAcquisition(handler) {
    const { cohort, offset } = handler
    const adopted = this.capture.acquisitions.filter((row) => row.cohort === cohort)
    expect(adopted).toHaveLength(1)
    const writes = this.capture.writes.filter(({ exchange }) => exchange === adopted[0].exchange)
    expect(writes).toHaveLength(1)
    const write = writes[0]
    expect(write.fields.groupId).toBe(handler.groupId)
    expect(write.answer?.errorCode).toBe(0)
    const topics = write.answer.responses.filter(({ topicId }) => topicId === this.topicId)
    expect(topics).toHaveLength(1)
    const partitions = topics[0].partitions.filter(({ partitionIndex }) => partitionIndex === 0)
    expect(partitions).toHaveLength(1)
    expect(partitions[0].errorCode).toBe(0)
    const ranges = partitions[0].acquiredRecords.filter(
      ({ firstOffset, lastOffset }) =>
        BigInt(firstOffset) <= BigInt(offset) && BigInt(offset) <= BigInt(lastOffset)
    )
    expect(ranges).toHaveLength(1)
    expect(ranges[0].deliveryCount).toBe(handler.deliveryCount)
    expect(cohort.topicId).toBe(this.topicId)
    expect(cohort.partition).toBe(0)
    const original = cohort.walk.deliver.records.filter((record) => record.offset === offset)
    expect(original).toHaveLength(1)
    expect(
      shareRecordIdentity({ ...original[0], topic: cohort.topic, partition: cohort.partition })
    ).toBe(handler.identity)
    expect(write.response.sequence).toBeLessThan(adopted[0].sequence)
    expect(adopted[0].sequence).toBeLessThan(handler.sequence)
  }

  /** Computes disjoint real-success and provisional-unknown classes from original observations.
   * @param {string} group Original workload group.
   * @returns {object} Exact corpus, attempts, classifications and ineligible identities.
   */
  partitionSettlement(group) {
    const { capture } = this
    const successful = []
    const candidates = []
    const missing = []
    const identityOf = (identity) => ({ groupId: group, topicId: this.topicId, identity })
    const handlers = capture.handlers.filter(({ groupId }) => groupId === group)
    expect(handlers.every(({ identity }) => this.expected.includes(identity))).toBe(true)
    expect(capture.public.every(({ outcome }) => outcome !== undefined)).toBe(true)
    const faults = capture.faults.filter(({ kind }) => kind === 'lost-response')
    expect(faults).toHaveLength(1)
    const [fault] = faults
    expect(fault.writes).toHaveLength(1)
    const [dropped] = fault.writes
    const written = dropped.prepared.flatMap(({ ticket }) =>
      decisionOffsets(ticket.acknowledgementBatches, 1)
    )
    expect(written.length).toBeLessThanOrEqual(PREFIX)
    expect(written.every((offset) => BigInt(offset) < BigInt(PREFIX))).toBe(true)
    for (const identity of this.expected) {
      const entries = handlers.filter((handler) => handler.identity === identity)
      expect(entries.length).toBeLessThanOrEqual(1)
      const handler = entries[0]
      if (!handler) {
        missing.push({ ...identityOf(identity), reason: 'missing-handler' })
        continue
      }
      this.assertAcquisition(handler)
      const terminal = capture.outcomes.filter(
        ({ cohort, result }) =>
          cohort === handler.cohort &&
          result.type !== 'renew' &&
          offsetsOf([result]).includes(handler.offset)
      )
      if (!terminal.length) {
        missing.push({ ...identityOf(identity), reason: 'missing-outcome' })
        continue
      }
      expect(terminal).toHaveLength(1)
      const [outcome] = terminal
      const publications = capture.public.filter((row) => row.outcome === outcome)
      expect(publications.length).toBeLessThanOrEqual(1)
      const decisions = capture.decisions.filter(
        (row) =>
          row.ledger === handler.cohort.ledger &&
          row.offset === handler.offset &&
          row.method !== 'decide' &&
          row.sequence < outcome.sequence
      )
      const decision = decisions.at(-1)
      const attempts = capture.writes.flatMap((write) =>
        write.prepared
          .filter(
            ({ row, ticket }) =>
              row === handler.cohort &&
              !ticket.isRenewAck &&
              decisionOffsets(ticket.acknowledgementBatches, 1).includes(handler.offset)
          )
          .map((prepared) => ({ write, prepared }))
      )
      expect(attempts.length).toBeLessThanOrEqual(1)
      const attempt = attempts[0]
      const evidence = {
        ...identityOf(identity),
        handler: capture.id(handler),
        cohort: capture.id(handler.cohort),
        ledger: capture.id(handler.cohort.ledger),
        ticket: capture.id(attempt?.prepared.ticket),
        exchange: capture.id(attempt?.write.exchange),
        outcome: capture.id(outcome),
        fault: capture.id(fault),
      }
      if (!attempt || !decision || publications.length !== 1) {
        missing.push({ ...evidence, reason: 'missing-original-written-attempt-or-public-outcome' })
        continue
      }
      const { write, prepared } = attempt
      expect(writtenAccept(write, prepared, handler)).toBe(true)
      expect(write.fields.topics).toHaveLength(1)
      expect(write.fields.topics[0].topicId).toBe(this.topicId)
      expect(write.fields.topics[0].partitions).toHaveLength(1)
      expect(write.fields.topics[0].partitions[0]).toEqual({
        partitionIndex: 0,
        acknowledgementBatches: prepared.ticket.acknowledgementBatches,
      })
      const reused = capture.writes
        .flatMap((entry) => entry.prepared)
        .filter(({ ticket }) => ticket === prepared.ticket)
      expect(reused).toHaveLength(1)
      if (
        outcome.result.type === 'accept' &&
        outcome.result.error === null &&
        acknowledgementCode(write, handler.cohort) === 0 &&
        decision.type === 'accept' &&
        decision.sequence < write.sequence &&
        write.response.sequence < outcome.sequence
      ) {
        successful.push(evidence)
      } else if (
        qualifyingUnknown(
          {
            handler,
            decision,
            prepared,
            write,
            outcome,
            publication: publications[0],
            fault,
            loss: capture.losses.find(
              (loss) =>
                loss.loop === write.loop && loss.open === write.open && loss.cause === write.failure
            ),
          },
          handlers
        )
      ) {
        candidates.push(evidence)
      } else missing.push({ ...evidence, reason: 'ineligible-outcome' })
    }
    expect(candidates.length).toBeLessThanOrEqual(written.length)
    if (!missing.length) expect(successful.length).toBeGreaterThanOrEqual(RECORDS - PREFIX)
    return {
      groupId: group,
      successful,
      provisional: candidates,
      unknown: [],
      missing,
      written: written.map((offset) => identityOf(this.expected[Number(offset)])),
    }
  }

  /** Excludes archive paths using every acquisition, including conservatively unanswered fetches.
   * @param {object} settlement Current candidate partition.
   * @returns {object[]} Per-candidate acquisition bounds and actual delivery limits.
   */
  excludeArchival(settlement) {
    const limit = Math.min(...this.limits.map(({ limit }) => limit))
    const bounds = []
    for (const candidate of settlement.provisional) {
      const offset = JSON.parse(candidate.identity)[2]
      let upper = 0
      for (const write of this.capture.writes.filter(
        ({ fields }) => fields.groupId === settlement.groupId
      )) {
        if (write.fact.apiKey === 78) {
          if (!write.answer) upper += 1
          else {
            for (const topic of write.answer.responses) {
              if (topic.topicId !== this.topicId) continue
              for (const partition of topic.partitions) {
                if (partition.partitionIndex !== 0) continue
                for (const acquired of partition.acquiredRecords ?? [])
                  if (
                    BigInt(acquired.firstOffset) <= BigInt(offset) &&
                    BigInt(offset) <= BigInt(acquired.lastOffset)
                  ) {
                    expect(Number.isInteger(acquired.deliveryCount)).toBe(true)
                    expect(acquired.deliveryCount).toBeGreaterThan(0)
                    upper += acquired.deliveryCount
                  }
              }
            }
          }
        }
        for (const topic of write.fields.topics) {
          if (topic.topicId !== this.topicId) continue
          for (const partition of topic.partitions) {
            if (partition.partitionIndex !== 0) continue
            for (const type of [0, 3])
              expect(decisionOffsets(partition.acknowledgementBatches, type)).not.toContain(offset)
          }
        }
      }
      expect(upper).toBeGreaterThan(0)
      expect(upper).toBeLessThan(limit)
      expect(
        this.capture.decisions.some(
          (decision) =>
            decision.ledger.topicId === this.topicId &&
            decision.offset === offset &&
            decision.type === 'reject'
        )
      ).toBe(false)
      bounds.push({ ...candidate, acquisitionUpperBound: upper, deliveryLimit: limit })
    }
    return bounds
  }

  /** Requires independent terminal and active-verifier evidence before promoting candidates.
   * @param {object} settlement Recomputed provisional partition.
   * @returns {void} Q alone can never complete the case.
   */
  assertUnknownTerminal(settlement) {
    expect(settlement.missing).toEqual([])
    expect(
      [...settlement.successful, ...settlement.provisional].map(({ identity }) => identity).sort()
    ).toEqual([...this.expected].sort())
    expect(this.log).toEqual(this.expected)
    expect(this.logEnd).toBe(String(RECORDS))
    expect(this.recovery).toBeDefined()
    expect(this.recovery.sequence).toBeLessThan(this.admission.sequence)
    expect(this.quiet).toHaveLength(1)
    const [quiet] = this.quiet
    expect(quiet.groupId).toBe(settlement.groupId)
    expect(quiet.delivered).toEqual([])
    expect(quiet.activeFetches.length).toBeGreaterThan(1)
    expect(
      quiet.assignmentObservations.some(
        ({ groupId, snapshot }) =>
          groupId === settlement.groupId &&
          snapshot.assignment.some(
            ({ topicId, partitions }) => topicId === this.topicId && partitions.includes(0)
          )
      )
    ).toBe(true)
    expect(
      this.quietObservations.filter(
        (row) =>
          row.groupId === settlement.groupId &&
          row.status === 'quiet' &&
          row.windowMs === 5000 &&
          row.delivered.length === 0
      )
    ).toHaveLength(1)
    const target = {
      groupId: settlement.groupId,
      topic: this.topic,
      partitions: [{ partition: 0, floor: '0', end: String(RECORDS) }],
    }
    for (const position of [this.terminalBefore, quiet.position]) {
      expect(position).toBeDefined()
      expect(
        terminalSharePosition(target, {
          ...position,
          topicId: this.topicId,
          ends: [{ partition: 0, high: this.logEnd }],
        }).settled
      ).toBe(true)
    }
    expect(this.archival.map(({ identity }) => identity).sort()).toEqual(
      settlement.provisional.map(({ identity }) => identity).sort()
    )
    for (const { acquisitionUpperBound, deliveryLimit } of this.archival) {
      expect(acquisitionUpperBound).toBeGreaterThan(0)
      expect(acquisitionUpperBound).toBeLessThan(deliveryLimit)
    }
  }

  /** Archives primitive observations and stable labels of original objects, never replacement authority.
   * @returns {object} Lossless frame references and complete acquisition/handler/decision/outcome journals.
   */
  captureEvidence() {
    const capture = this.capture
    const id = (value) => capture.id(value)
    const authority = (write) => ({
      exchange: id(write.exchange),
      loop: id(write.loop),
      run: id(write.run),
      nodeId: write.node?.id,
      open: id(write.open),
      membership: id(write.membership),
      pool: id(write.binding.pool),
      generation: write.binding.generation,
      connection: id(write.binding.connection),
      prepared: write.prepared.map(({ ledger, ticket, row }) => ({
        ledger: id(ledger),
        ticket: id(ticket),
        cohort: id(row),
        acquisition: id(row.token),
        batches: ticket.acknowledgementBatches,
        isRenewAck: ticket.isRenewAck,
      })),
    })
    return {
      frames: this.tap.frames.map(({ kind, frame, ref: _ref, write, ...fact }) => ({
        ...fact,
        kind,
        bytes: frame.toString('base64'),
        ...(write && {
          ...authority(write),
          fields: write.fields,
          failure: describeError(write.failure),
          cause: id(write.failure),
        }),
      })),
      assignments: capture.assignments.map(({ run, ...fact }) => ({ ...fact, run: id(run) })),
      acquisitions: capture.acquisitions.map(({ cohort, exchange, ...fact }) => ({
        ...fact,
        cohort: id(cohort),
        acquisition: id(cohort.token),
        ledger: id(cohort.ledger),
        exchange: id(exchange),
        topicId: cohort.topicId,
        partition: cohort.partition,
        groupId: capture.writes.find((write) => write.exchange === exchange)?.fields.groupId,
        records: cohort.walk.deliver.records.map(({ offset, key, value }) => ({
          offset,
          key: key?.toString('base64'),
          value: value?.toString('base64'),
        })),
        deliveryCounts: cohort.walk.deliver.deliveryCounts,
      })),
      handlers: capture.handlers.map(({ run, node, cohort, call, ...fact }) => ({
        ...fact,
        handler: id(capture.handlers.find((entry) => entry.sequence === fact.sequence)),
        run: id(run),
        node: node.id,
        cohort: id(cohort),
        call: id(call),
        ledger: id(cohort.ledger),
      })),
      decisions: capture.decisions.map(({ ledger, ...fact }) => ({ ...fact, ledger: id(ledger) })),
      outcomes: capture.outcomes.map((outcome) => ({
        outcome: id(outcome),
        cohort: id(outcome.cohort),
        groupId: outcome.groupId,
        sequence: outcome.sequence,
        at: outcome.at,
        ...outcome.result,
        error: describeError(outcome.result.error),
        cause: id(outcome.result.error?.cause),
        public: capture.public
          .filter((entry) => entry.outcome === outcome)
          .map(({ payload: _payload, outcome: _outcome, ...entry }) => entry),
      })),
      losses: capture.losses.map(({ loop, open, cause, ...fact }) => ({
        ...fact,
        loop: id(loop),
        open: id(open),
        cause: id(cause),
        error: describeError(cause),
      })),
      faults: capture.faults.map(({ handlers, writes, ...fact }) => ({
        ...fact,
        fault: id(capture.faults.find((entry) => entry.sequence === fact.sequence)),
        handlers: handlers.map(id),
        exchanges: writes.map(({ exchange }) => id(exchange)),
      })),
    }
  }

  /**
   * Identifies missing successful Accepts without combining independent groups or replacing a
   * lost public outcome with a response observed below the fault layer.
   * @param {string} group Group being observed.
   * @returns {{missingPublic: string[], missingWire: string[]}} Unconfirmed corpus offsets.
   */
  missingAccepts(group) {
    const accepted = new Set()
    for (const { groupId, acks } of Object.values(this.members)) {
      if (groupId !== group) continue
      for (const { topic, partition, type, error, firstOffset, lastOffset } of acks) {
        if (topic !== this.topic || partition !== 0 || type !== 'accept' || error !== null) continue
        for (const offset of offsetsOf([{ firstOffset, lastOffset }])) accepted.add(offset)
      }
    }
    const wire = this.sockets.acknowledgements.confirmed(group, this.topicId, 0)
    const expected = this.expected.map((identity) => JSON.parse(identity)[2])
    return {
      missingPublic: expected.filter((offset) => !accepted.has(offset)),
      missingWire: expected.filter((offset) => !wire.has(offset)),
    }
  }

  /**
   * Checks successful public and wire Accept coverage for this group.
   * @param {string} group Group being observed.
   * @returns {boolean} Whether both sources cover the entire immutable corpus.
   */
  acceptedGroup(group) {
    const { missingPublic, missingWire } = this.missingAccepts(group)
    return missingPublic.length === 0 && missingWire.length === 0
  }

  /**
   * Polls successful settlement and terminal positions against the independent raw-log end.
   * @param {AbortSignal} scope Case scope.
   * @param {string} group Group being observed.
   * @param {boolean} [faultSettlement=false] Admit only evidenced written-Accept candidates.
   * @returns {Promise<object>} Terminal partition row.
   */
  async awaitStart(scope, group, faultSettlement = false) {
    if (faultSettlement) {
      expect(this.recovery).toBeDefined()
      this.admission = this.capture.stamp()
      expect(this.recovery.sequence).toBeLessThan(this.admission.sequence)
    }
    return bounded(
      scope,
      DELIVERY_MS,
      async (poll) => {
        await pollUntil(
          poll,
          DELIVERY_MS,
          async () => {
            if (!faultSettlement) return this.acceptedGroup(group)
            await this.capture.refresh()
            this.settlement = this.partitionSettlement(group)
            return this.settlement.missing.length === 0
          },
          {
            label: `${group} terminal Accept eligibility`,
          }
        )
        if (faultSettlement) {
          this.archival = this.excludeArchival(this.settlement)
          this.row('settlement-candidates', {
            settlement: this.settlement,
            archival: this.archival,
            capture: this.captureEvidence(),
          })
        }
        const position = await awaitTerminalSharePosition(
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
        if (faultSettlement) this.terminalBefore = position
        return position.rows[0]
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
    const topicId = topics.find(({ name }) => name === this.topic).topicId
    if (this.topicId !== undefined) expect(topicId).toBe(this.topicId)
    this.topicId = topicId
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
    this.quietObservations = []
    for (const group of groups) {
      const verified = await assertQuietTerminalGroup({
        kafka: this.kafka('V'),
        admin: this.admin,
        groupId: group,
        topic: this.topic,
        partitions: [{ partition: 0, floor: '0', end: this.logEnd }],
        signal: scope,
        timeoutMs: DELIVERY_MS,
        record: (fields) => {
          this.quietObservations.push({ groupId: group, ...fields })
          this.row('quiet-verifier', { groupId: group, ...fields })
        },
      })
      const active = this.tap
        .requestsOf('V', API_SHARE_FETCH)
        .filter(({ write }) => write.fields.groupId === group)
      expect(active.length).toBeGreaterThan(1)
      const completed = active.filter((request) => this.tap.responseTo(request))
      expect(completed.length).toBeGreaterThan(1)
      const assignments = this.capture.assignments.filter(({ run }) => run === active[0].write.run)
      expect(
        assignments.some(({ snapshot }) =>
          snapshot.assignment.some(
            ({ topicId, partitions }) => topicId === this.topicId && partitions.includes(0)
          )
        )
      ).toBe(true)
      this.quiet.push({
        assignmentObservations: assignments.map(({ run: _run, ...fact }) => fact),
        groupId: group,
        ...verified,
        activeFetches: completed.map(({ socket, correlationId, at }) => ({
          socket,
          correlationId,
          at,
        })),
      })
      this.broker.push(...verified.position.rows)
    }
    const ends = await bounded(scope, STEP_MS, () =>
      endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect(ends.get(0)).toBe(this.logEnd)
    if (this.action === 'timeout') {
      await this.verifyLog(scope)
      await this.capture.refresh()
      const final = this.partitionSettlement(groups[0])
      expect(final.missing).toEqual([])
      this.archival = this.excludeArchival(final)
      this.assertUnknownTerminal(final)
      this.settlement = { ...final, unknown: final.provisional, provisional: [] }
      this.row('settlement-terminal', {
        settlement: this.settlement,
        archival: this.archival,
        capture: this.captureEvidence(),
        quiet: this.quiet,
      })
    }
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
    const { endpoint, topicId } = await this.leadership(scope)
    this.topicId = topicId
    await this.deliveryLimits(scope)

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
      this.capture.freeze('lost-response', [fact.write])
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
    const startedHeld = this.capture.faults[0].handlers.map(({ identity }) => identity)
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
    expect(droppedOffsets).toEqual(
      Array.from({ length: PREFIX }, (_, index) => String(PREFIX + index))
    )
    expect(member.entries.some(({ offset }) => droppedOffsets.includes(offset))).toBe(false)
    expect(answer.responses).toHaveLength(1)
    expect(answer.responses[0].topicId).toBe(topicId)
    expect(answer.responses[0].partitions).toHaveLength(1)
    expect(answer.responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 0,
      acknowledgeErrorCode: 0,
    })
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
    const closedAt = dropped.ref.closedAt
    this.assertRecovery(member, { dropped, carried, droppedOffsets, droppedIdentities, closedAt })

    expect(member.crashes).toEqual([])
    expect([...member.identities()].sort()).toEqual([...expected].sort())
    this.row('held-set', {
      startedHeldAtFault: startedHeld,
      acquiredNeverDelivered: droppedIdentities,
    })
    const record = await this.awaitStart(scope, group, true)
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
    const write = dropped.write
    const timedOut = member.timeouts.filter(
      ({ apiName, correlationId, broker, sentAt }) =>
        apiName === 'ShareFetch' &&
        correlationId === dropped.correlationId &&
        broker === `${dropped.host}:${dropped.port}` &&
        sentAt === write.failure?.sentAt
    )
    expect(timedOut).toHaveLength(1)
    expect(write.failure).toBeInstanceOf(KafkaJSRequestTimeoutError)
    expect(timedOut[0].at - timedOut[0].sentAt).toBeGreaterThanOrEqual(REQUEST_TIMEOUT_MS)
    expect(dropped.ref.socket.destroyed).toBe(true)
    expect(closedAt).not.toBeNull()
    const reset = member.resets.find(
      ({ reason, at }) => reason === 'connection' && at >= dropped.at
    )
    expect(reset).toBeDefined()
    const originalLoss = this.capture.losses.filter(
      ({ loop, open, cause }) =>
        loop === write.loop && open === write.open && cause === write.failure
    )
    expect(originalLoss).toHaveLength(1)
    expect(originalLoss[0].reason).toBe('connection')

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
    expect(later.length).toBeGreaterThanOrEqual(2)
    const replacement = later[0].write
    expect(replacement.binding.pool).toBe(write.binding.pool)
    expect(replacement.binding.generation).not.toBe(write.binding.generation)
    expect(replacement.open).not.toBe(write.open)
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
      expect(
        fact.write.prepared.some(({ ticket }) =>
          write.prepared.some((old) => old.ticket === ticket)
        )
      ).toBe(false)
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
    this.recovery = {
      socket: dropped.socket,
      exchange: this.capture.id(write.exchange),
      timeout: timedOut[0],
      closedAt,
      loss: this.capture.id(originalLoss[0]),
      ...this.capture.stamp(),
    }
    this.row('fault', {
      recovery: this.recovery,
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

    const moving = this.capture.writes.filter(
      ({ loop, exchange }) =>
        loop.groupId === group && loop.exchanges.has(exchange) && exchange.prepared.length > 0
    )
    const moveFault = this.capture.freeze('leader-move', moving)
    expect(
      moveFault.writes.flatMap(({ prepared }) =>
        prepared.flatMap(({ ticket }) => decisionOffsets(ticket.acknowledgementBatches, 1))
      )
    ).toEqual([])
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
    expect(counts.get(heldIdentity)).toHaveLength(2)
    expect(counts.get(heldIdentity)[1]).toBe(counts.get(heldIdentity)[0] + 1)
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
    if (this.action === 'timeout') {
      expect(this.settlement.provisional).toEqual([])
      expect(this.settlement.missing).toEqual([])
      expect(this.settlement.successful.length + this.settlement.unknown.length).toBe(RECORDS)
      const final = this.partitionSettlement(this.groups.main)
      this.assertUnknownTerminal(final)
      expect(final.successful).toEqual(this.settlement.successful)
      expect(final.provisional).toEqual(this.settlement.unknown)
      expect(this.quiet).toHaveLength(1)
      expect(this.recovery).toBeDefined()
    }
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

/** Builds original-authority doubles with real ledger decisions and complete scripted frames.
 * @returns {Promise<SessionCase>} Applied Accepts with a diagnostic answer and typed public unknowns.
 */
const scriptedSettlement = async () => {
  const Encoder = require('../../../protocol/encoder')
  const encodeFetch = require('../../../protocol/requests/shareFetch/v2/request')
  const owner = Object.create(SessionCase.prototype)
  Object.assign(owner, {
    topic: 'records',
    topicId: '12345678-1234-1234-1234-123456789abc',
    action: 'timeout',
    groups: { main: 'original-group' },
    expected: [],
    logEnd: '300',
    limits: [{ nodeId: 1, limit: 5 }],
    tap: { frames: [] },
    recovery: { socket: 1, sequence: 1 },
    admission: { sequence: 2 },
  })
  const capture = new SettlementCapture(owner)
  owner.capture = capture
  const records = Array.from({ length: RECORDS }, (_, index) => ({
    offset: String(index),
    key: Buffer.from(`key-${index}`),
    value: Buffer.from(`value-${index}`),
  }))
  owner.expected = records.map((record) =>
    shareRecordIdentity({ ...record, topic: owner.topic, partition: 0 })
  )
  owner.log = [...owner.expected]
  const loop = { memberId: 'original-member', authority: { membershipToken: Symbol('membership') } }
  const socket = {}
  const binding = { connection: {}, generation: 1, pool: {} }
  let correlationId = 0
  /** @param {object[]} prepared Original ticket links. @param {object[]} acquiredRecords Scripted acquisitions. @returns {Promise<object>} Complete original exchange. */
  const write = async (prepared, acquiredRecords) => {
    const fields = shareFetchWire.readRequest(
      (
        await encodeFetch({
          groupId: owner.groups.main,
          memberId: loop.memberId,
          shareSessionEpoch: ++correlationId,
          maxRecords: PREFIX,
          topics: [
            {
              topicId: owner.topicId,
              partitions: [
                {
                  partitionIndex: 0,
                  acknowledgementBatches: prepared[0]?.ticket.acknowledgementBatches ?? [],
                },
              ],
            },
          ],
        }).encode()
      ).buffer
    )
    const body = (await encodeFetch(fields).encode()).buffer
    const header = new Encoder()
      .writeInt16(78)
      .writeInt16(2)
      .writeInt32(correlationId)
      .writeString('original-client')
      .writeTaggedFields().buffer
    const bytes = Buffer.concat([header, body])
    const frame = new Encoder().writeInt32(bytes.length).writeBuffer(bytes).buffer
    const fact = {
      ...readRequestHeader(frame),
      frame,
      ref: { socket },
      kind: 'request',
      socket: 1,
      host: 'broker',
      port: 9092,
      ...capture.stamp(),
    }
    const open = Symbol('open')
    const exchange = {
      open,
      binding,
      sentAt: fact.at,
      descriptor: { epoch: fields.shareSessionEpoch },
    }
    const result = {
      fields,
      fact,
      socket,
      loop,
      open,
      binding,
      exchange,
      descriptor: exchange.descriptor,
      prepared,
      membership: loop.authority.membershipToken,
      ...capture.stamp(),
      answer: {
        errorCode: 0,
        responses: [
          {
            topicId: owner.topicId,
            partitions: [
              { partitionIndex: 0, errorCode: 0, acknowledgeErrorCode: 0, acquiredRecords },
            ],
          },
        ],
      },
    }
    result.response = { ...capture.stamp() }
    fact.write = result
    capture.writes.push(result)
    owner.tap.frames.push(fact)
    return result
  }
  owner.scriptedWrite = write
  for (let first = 0; first < RECORDS; first += PREFIX) {
    const selected = records.slice(first, first + PREFIX)
    const source = await write(
      [],
      [
        {
          firstOffset: String(first),
          lastOffset: String(first + PREFIX - 1),
          deliveryCount: first === PREFIX ? 2 : 1,
        },
      ]
    )
    const token = Symbol('acquisition')
    const walk = {
      deliver: {
        records: selected,
        deliveryCounts: selected.map(() => (first === PREFIX ? 2 : 1)),
      },
      gaps: [],
      release: [],
      reject: [],
    }
    const ledger = new ShareAckLedger({
      topic: owner.topic,
      topicId: owner.topicId,
      partition: 0,
      token,
      sentAt: 0,
      lockDuration: LOCK_MS,
      walk,
    })
    const cohort = { topic: owner.topic, topicId: owner.topicId, partition: 0, ledger, token, walk }
    capture.acquisitions.push({ cohort, exchange: source.exchange, ...capture.stamp() })
    for (const record of selected) {
      capture.handlers.push({
        cohort,
        groupId: owner.groups.main,
        identity: owner.expected[Number(record.offset)],
        offset: record.offset,
        deliveryCount: first === PREFIX ? 2 : 1,
        ...capture.stamp(),
      })
    }
    ledger.complete(
      selected.map(({ offset }) => offset),
      'accept'
    )
    for (const { offset } of selected)
      capture.decisions.push({
        ledger,
        offset,
        type: 'accept',
        method: 'complete',
        ...capture.stamp(),
      })
    const { ticket } = ledger.prepare({ token })
    const prepared = { row: cohort, ledger, ticket }
    const sent = await write(
      [prepared],
      first === 0 ? [{ firstOffset: '100', lastOffset: '199', deliveryCount: 1 }] : []
    )
    let results
    if (first === 0) {
      const fault = capture.freeze('lost-response', [sent])
      sent.response = { ...capture.stamp() }
      const cause = new KafkaJSRequestTimeoutError('ShareFetch timed out', {
        broker: 'broker:9092',
        correlationId: sent.fact.correlationId,
        sentAt: sent.fact.at,
      })
      sent.failure = cause
      capture.losses.push({
        loop,
        open: sent.open,
        reason: 'connection',
        cause,
        ...capture.stamp(),
      })
      results = ledger.invalidate('session-lost', null, cause)
      expect(ledger.invalidate('session-lost', null, cause)).toEqual([])
      owner.scriptedFault = fault
    } else results = ledger.settle(ticket, { token })
    for (const result of results) {
      const outcome = { cohort, result, groupId: owner.groups.main, ...capture.stamp() }
      capture.outcomes.push(outcome)
      capture.public.push({ outcome, payload: { ...result }, ...capture.stamp() })
    }
  }
  const listing = () => [
    {
      topic: owner.topic,
      topicId: owner.topicId,
      partitions: [{ partition: 0, errorCode: 0, lag: '0', startOffset: '100', leaderEpoch: 1 }],
    },
  ]
  owner.terminalBefore = { named: listing(), all: listing() }
  owner.quiet = [
    {
      groupId: owner.groups.main,
      delivered: [],
      activeFetches: [{ at: 0 }, { at: 5000 }],
      assignmentObservations: [
        {
          groupId: owner.groups.main,
          snapshot: {
            assignment: [{ topicId: owner.topicId, partitions: [0] }],
          },
        },
      ],
      position: { named: listing(), all: listing() },
    },
  ]
  owner.quietObservations = [
    { groupId: owner.groups.main, status: 'quiet', windowMs: 5000, delivered: [] },
  ]
  return owner
}

describe('written Accept settlement sensitivity', () => {
  it('keeps diagnostic broker success separate from public unknown and admits only a provisional partition', async () => {
    const owner = await scriptedSettlement()
    const partition = owner.partitionSettlement(owner.groups.main)
    expect(partition.successful).toHaveLength(200)
    expect(partition.provisional).toHaveLength(100)
    expect(partition.unknown).toEqual([])
    expect(partition.missing).toEqual([])
    const failed = owner.capture.outcomes.filter(({ result }) => result.error !== null)
    expect(failed).toHaveLength(1)
    expect(failed[0].result.error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
    expect(
      owner.capture.outcomes
        .filter(({ result }) => result.type === 'accept' && result.error === null)
        .flatMap(({ result }) => offsetsOf([result]))
    ).toHaveLength(200)
    expect(
      owner.capture.writes.find(({ failure }) => failure).answer.responses[0].partitions[0]
        .acknowledgeErrorCode
    ).toBe(0)
    owner.archival = owner.excludeArchival(partition)
    expect(() => owner.assertUnknownTerminal(partition)).not.toThrow()
  })

  it.each([
    [
      'plain error without the typed outcome',
      (owner) => {
        owner.capture.outcomes[0].result.error = { reason: 'session-lost' }
      },
    ],
    [
      'duplicate fault charge',
      (owner) => {
        owner.capture.faults.push(owner.scriptedFault)
      },
    ],
    [
      'recovery after settlement admission',
      (owner) => {
        owner.recovery.sequence = owner.admission.sequence + 1
      },
    ],
    [
      'lost prefix beyond the actual request bound',
      (owner) => {
        owner.scriptedFault.writes[0].prepared[0].ticket = {
          isRenewAck: false,
          acknowledgementBatches: [{ firstOffset: '0', lastOffset: '100', acknowledgeTypes: [1] }],
        }
      },
    ],
    [
      'swallowed acquisition used as written Accepts',
      (owner) => {
        owner.scriptedFault.writes[0].prepared[0].ticket = {
          isRenewAck: false,
          acknowledgementBatches: [
            { firstOffset: '100', lastOffset: '199', acknowledgeTypes: [1] },
          ],
        }
      },
    ],
    [
      'last-count-only archival exclusion',
      async (owner) => {
        for (let index = 0; index < 5; index++) {
          const unanswered = await owner.scriptedWrite([], [])
          unanswered.answer = undefined
        }
      },
    ],
    [
      'foreign terminal UUID',
      (owner) => {
        owner.quiet[0].position.named[0].topicId = 'foreign'
      },
    ],
    [
      'terminal partition error',
      (owner) => {
        owner.quiet[0].position.named[0].partitions[0].errorCode = 6
      },
    ],
    [
      'no write',
      (owner) => {
        owner.scriptedFault.writes[0].exchange.sentAt = undefined
      },
    ],
    [
      'queued ticket',
      (owner) => {
        owner.scriptedFault.writes = []
      },
    ],
    [
      'held before decision',
      (owner) => {
        owner.capture.decisions = []
      },
    ],
    [
      'missing handler',
      (owner) => {
        owner.capture.handlers.shift()
      },
    ],
    [
      'missing typed outcome',
      (owner) => {
        owner.capture.outcomes.shift()
      },
    ],
    [
      'foreign group',
      (owner) => {
        owner.capture.handlers[0].groupId = 'foreign'
      },
    ],
    [
      'foreign UUID',
      (owner) => {
        owner.capture.handlers[0].cohort.topicId = 'foreign'
      },
    ],
    [
      'changed bytes',
      (owner) => {
        owner.capture.handlers[0].identity = 'changed'
      },
    ],
    [
      'replacement ticket',
      (owner) => {
        owner.scriptedFault.writes[0].prepared[0].ticket = { acknowledgementBatches: [] }
      },
    ],
    [
      'replacement cohort',
      (owner) => {
        owner.scriptedFault.writes[0].prepared[0].row = {}
      },
    ],
    [
      'wrong socket',
      (owner) => {
        owner.scriptedFault.writes[0].fact.ref.socket = {}
      },
    ],
    [
      'reused correlation',
      (owner) => {
        owner.scriptedFault.writes[0].fact.correlationId += 1
      },
    ],
    [
      'incomplete frame',
      (owner) => {
        owner.scriptedFault.writes[0].fact.frame = Buffer.alloc(0)
      },
    ],
    [
      'unrelated fault',
      (owner) => {
        owner.scriptedFault.kind = 'unrelated'
      },
    ],
    [
      'unrelated timeout',
      (owner) => {
        owner.scriptedFault.writes[0].failure = new KafkaJSRequestTimeoutError('other')
      },
    ],
    [
      'known rejection',
      (owner) => {
        owner.scriptedFault.writes[0].answer.responses[0].partitions[0].acknowledgeErrorCode = 6
      },
    ],
    [
      'duplicate public outcome',
      (owner) => {
        owner.capture.public.push({ ...owner.capture.public[0] })
      },
    ],
    [
      'duplicate compact outcome',
      (owner) => {
        owner.capture.outcomes.push({ ...owner.capture.outcomes[0] })
      },
    ],
    [
      'stale ticket resend',
      (owner) => {
        owner.capture.writes.push({ ...owner.scriptedFault.writes[0] })
      },
    ],
    [
      'unknown outside H',
      (owner) => {
        owner.scriptedFault.handlers.shift()
      },
    ],
    [
      'later workload entry',
      (owner) => {
        owner.capture.handlers.push({ ...owner.capture.handlers[0], ...owner.capture.stamp() })
      },
    ],
    [
      'missing recovery',
      (owner) => {
        owner.recovery = undefined
      },
    ],
    [
      'missing post-verifier T',
      (owner) => {
        owner.quiet[0].position = undefined
      },
    ],
    [
      'inactive verifier',
      (owner) => {
        owner.quiet[0].activeFetches = []
      },
    ],
    [
      'verifier delivery',
      (owner) => {
        owner.quiet[0].delivered = [owner.expected[0]]
      },
    ],
    [
      'archival exhaustion',
      (owner) => {
        owner.limits[0].limit = 1
      },
    ],
    [
      'changed raw corpus',
      (owner) => {
        owner.log.pop()
      },
    ],
    [
      'changed end',
      (owner) => {
        owner.logEnd = '301'
      },
    ],
    [
      'positive lag',
      (owner) => {
        owner.quiet[0].position.named[0].partitions[0].lag = '1'
      },
    ],
    [
      'null lag',
      (owner) => {
        owner.quiet[0].position.named[0].partitions[0].lag = null
      },
    ],
    [
      'missing lag',
      (owner) => {
        delete owner.quiet[0].position.named[0].partitions[0].lag
      },
    ],
    [
      'listing disagreement',
      (owner) => {
        owner.quiet[0].position.all[0].partitions[0].startOffset = '0'
      },
    ],
    [
      'invalid start',
      (owner) => {
        owner.quiet[0].position.named[0].partitions[0].startOffset = '-1'
      },
    ],
    [
      'missing partition',
      (owner) => {
        owner.quiet[0].position.named[0].partitions = []
      },
    ],
  ])('rejects %s with the independent corpus and other evidence retained', async (name, mutate) => {
    const owner = await scriptedSettlement()
    await mutate(owner)
    expect(() => {
      const partition = owner.partitionSettlement(owner.groups.main)
      expect(partition.missing).toEqual([])
      owner.archival = owner.excludeArchival(partition)
      owner.assertUnknownTerminal(partition)
    }).toThrow()
  })

  it('cannot promote a written but unapplied Accept while the broker still has work', async () => {
    const owner = await scriptedSettlement()
    owner.scriptedFault.writes[0].answer = undefined
    const partition = owner.partitionSettlement(owner.groups.main)
    expect(partition.provisional).toHaveLength(100)
    owner.archival = owner.excludeArchival(partition)
    for (const position of [owner.terminalBefore, owner.quiet[0].position]) {
      position.named[0].partitions[0].lag = '100'
      position.all[0].partitions[0].lag = '100'
    }
    expect(() => owner.assertUnknownTerminal(partition)).toThrow()
    expect(owner.capture.outcomes[0].result.error.reason).toBe('session-lost')
  })

  it.each(['renew', 'release', 'reject', 'gap'])(
    'cannot replace a terminal Accept with %s',
    async (type) => {
      const owner = await scriptedSettlement()
      owner.capture.outcomes[0].result.type = type
      expect(owner.partitionSettlement(owner.groups.main).missing.length).toBeGreaterThan(0)
    }
  )

  it.each(['not-sent', 'membership-lost', 'rejected-by-broker'])(
    'rejects typed %s even with terminal evidence',
    async (reason) => {
      const owner = await scriptedSettlement()
      owner.capture.outcomes[0].result.error.reason = reason
      expect(owner.partitionSettlement(owner.groups.main).missing.length).toBeGreaterThan(0)
    }
  )

  it('does not inherit the unknown exception in the held-only leader or normal arms', async () => {
    const owner = await scriptedSettlement()
    owner.scriptedFault.kind = 'leader-move'
    expect(() => owner.partitionSettlement(owner.groups.main)).toThrow()
    owner.members = {
      A: {
        groupId: owner.groups.main,
        acks: owner.capture.outcomes.map(({ result }) => ({
          ...result,
          topic: owner.topic,
          partition: 0,
        })),
      },
    }
    owner.sockets = {
      acknowledgements: {
        confirmed: () => new Set(Array.from({ length: RECORDS }, (_, index) => String(index))),
      },
    }
    expect(owner.acceptedGroup(owner.groups.main)).toBe(false)
  })

  it('keeps fetch and acknowledgement errors independent', () => {
    const cohort = { topicId: 'uuid', partition: 0 }
    const write = {
      fact: { apiKey: 78 },
      answer: {
        errorCode: 0,
        responses: [
          {
            topicId: 'uuid',
            partitions: [{ partitionIndex: 0, errorCode: 6, acknowledgeErrorCode: 0 }],
          },
        ],
      },
    }
    expect(acknowledgementCode(write, cohort)).toBe(0)
    write.fact.apiKey = 79
    expect(acknowledgementCode(write, cohort)).toBe(6)
  })
})
