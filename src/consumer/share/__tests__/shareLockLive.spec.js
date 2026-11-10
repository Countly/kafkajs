const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const Decoder = require('../../../protocol/decoder')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../../protocol/headers')
const {
  acknowledgementResults,
  acknowledgementRequests,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const net = require('net')
const { EventEmitter } = require('events')
const { Kafka, logLevel } = require('../../../../index')
const {
  createTopic,
  newLogger,
  plainTextBrokers,
  readRequestHeader,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const {
  assertShareCleanup,
  captureShareCleanupBaseline,
  waitForShareFixtureAbsence,
  setShareGroupConfigs,
  resetShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
} = require('../../../../testHelpers/shareFixtures')
const { readLog, endOffsets } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const RequestQueue = require('../../../network/requestQueue')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareSession } = require('../shareSession')
const { ShareRun } = require('../shareRun')
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
const CLEANUP_MS = 4000
const LOCK_MS = 3000
const REDELIVERY_SLACK_MS = 2000
const RENEW_AT_MS = [1500, 3000, 4500, 6000, 7500]
const LATE_ACCEPT_AT_MS = { expire: 6000, renew: 8000, disabled: 6000 }
const OLD_TIMESTAMP = '1700000000000'
const SHARE_FETCH = 78
const SHARE_ACKNOWLEDGE = 79
const SHARE_APIS = new Set([76, 78, 79])
const ADMIN_TRACE_APIS = {
  3: require('../../../protocol/requests/metadata'),
  10: require('../../../protocol/requests/findCoordinator'),
  16: require('../../../protocol/requests/listGroups'),
  18: require('../../../protocol/requests/apiVersions'),
  20: require('../../../protocol/requests/deleteTopics'),
  33: require('../../../protocol/requests/describeConfigs'),
  42: require('../../../protocol/requests/deleteGroups'),
  44: require('../../../protocol/requests/incrementalAlterConfigs'),
}
const RENEW = 4
const ACCEPT = 1
const INVALID_RECORD_STATE = 121
const BASE_GROUP = {
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': LOCK_MS,
}

/**
 * Validates the controlled expiry sequence from portable original observations.
 * @param {object} proof Acquisitions, invocations, tickets, wire responses and public results.
 * @param {boolean} [complete=true] Also require B's terminal settlement after opening its gate.
 * @returns {void} Throws for a missing, duplicate, uncorrelated or permissive expiry result.
 */
const assertControlledExpiry = (proof, complete = true) => {
  const {
    group,
    topic,
    topicId,
    members,
    acquisitions,
    entries,
    wire,
    results,
    tickets,
    renewals,
  } = proof
  expect(entries).toHaveLength(2)
  const [a, b] = entries
  expect([a.member, b.member]).toEqual(['A', 'B'])
  expect([a.deliveryCount, b.deliveryCount]).toEqual([1, 2])
  expect(b.identity).toBe(a.identity)
  expect(acquisitions).toHaveLength(2)
  expect(acquisitions[0].id).not.toBe(acquisitions[1].id)
  for (const [index, entry] of entries.entries()) {
    const acquisition = acquisitions[index]
    expect(acquisition).toMatchObject({
      member: entry.member,
      group,
      topicId,
      partition: 0,
      identities: [entry.identity],
      counts: [entry.deliveryCount],
      lockDuration: LOCK_MS,
    })
    expect(entry.acquisition).toBe(acquisition.id)
    expect(acquisition.sentAt).toBeLessThanOrEqual(entry.at)
    expect(acquisition.deadline).toBe(acquisition.sentAt + LOCK_MS)
  }
  expect(b.at).toBeLessThanOrEqual(acquisitions[0].deadline + REDELIVERY_SLACK_MS)
  expect(a.decidedAt).toBeGreaterThanOrEqual(a.at + LATE_ACCEPT_AT_MS.expire)
  expect(wire.filter((frame) => frame.label === 'A' && frame.isRenewAck)).toEqual([])
  /** @param {string} label Member label. @param {string} type Public result type. @param {number} code Required ack code. @returns {object[]} Correlated exchanges. */
  const check = (label, type, code) => {
    const frames = wire.filter(
      (frame) => frame.label === label && frame.isRenewAck === (type === 'renew')
    )
    const outcomes = results.filter((result) => result.member === label && result.type === type)
    expect(frames.length).toBeGreaterThan(0)
    expect(outcomes).toHaveLength(frames.length)
    const seen = new Set()
    for (const [index, frame] of frames.entries()) {
      expect([78, 79]).toContain(frame.apiKey)
      expect(frame).toMatchObject({ apiVersion: 2, groupId: group, memberId: members[label] })
      expect(Number.isInteger(frame.socket)).toBe(true)
      expect(Number.isInteger(frame.correlationId)).toBe(true)
      const exchange = `${frame.socket}:${frame.correlationId}`
      expect(seen.has(exchange)).toBe(false)
      seen.add(exchange)
      expect(frame.topics).toEqual([
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [
                {
                  firstOffset: '0',
                  lastOffset: '0',
                  acknowledgeTypes: [type === 'renew' ? RENEW : ACCEPT],
                },
              ],
            },
          ],
        },
      ])
      expect(frame.response?.errorCode).toBe(0)
      expect(frame.response.topics).toHaveLength(1)
      expect(frame.response.topics[0].topicId).toBe(topicId)
      expect(frame.response.topics[0].partitions).toHaveLength(1)
      const [partition] = frame.response.topics[0].partitions
      expect(partition.partitionIndex).toBe(0)
      expect(
        frame.apiKey === SHARE_FETCH ? partition.acknowledgeErrorCode : partition.errorCode
      ).toBe(code)
      expect(frame.response.at).toBeGreaterThanOrEqual(frame.at)
      const outcome = outcomes[index]
      expect(outcome).toMatchObject({
        topic,
        partition: 0,
        firstOffset: '0',
        lastOffset: '0',
        type,
      })
      if (code === 0) expect(outcome.error).toBeNull()
      else {
        expect(outcome.error).toMatchObject({
          name: 'KafkaJSShareAcknowledgementError',
          reason: 'rejected-by-broker',
        })
        expect(causeCodes(outcome.error)).toContain(code)
      }
      const matching = tickets.filter(
        (ticket) =>
          ticket.member === label &&
          ticket.isRenewAck === frame.isRenewAck &&
          ticket.preparedAt <= frame.at &&
          ticket.settledAt >= frame.at &&
          ticket.settledAt <= outcome.at
      )
      expect(matching).toHaveLength(1)
      expect(matching[0].settlements).toBe(1)
      expect(matching[0].acquisition).toBe(label === 'A' ? a.acquisition : b.acquisition)
      expect(matching[0].batches).toEqual(frame.topics[0].partitions[0].acknowledgementBatches)
      expect(matching[0].results).toEqual([
        { firstOffset: '0', lastOffset: '0', type, error: outcome.error },
      ])
    }
    return frames
  }
  const aFrames = check('A', 'accept', INVALID_RECORD_STATE)
  expect(aFrames).toHaveLength(1)
  expect(results.filter(({ member }) => member === 'A')).toHaveLength(1)
  const verdictAt = Math.max(
    aFrames[0].response.at,
    results.find(({ member }) => member === 'A').at
  )
  expect(aFrames[0].at).toBeGreaterThanOrEqual(a.decidedAt)
  const renewalFrames = check('B', 'renew', 0)
  expect(renewals).toHaveLength(renewalFrames.length)
  expect(renewals.length).toBeGreaterThan(0)
  let deadline = acquisitions[1].deadline
  for (const [index, renewal] of renewals.entries()) {
    expect(renewal.duration).toBe(LOCK_MS)
    expect(renewal.error).toBeNull()
    expect(renewal.calledAt).toBeLessThan(deadline)
    expect(renewal.completedAt).toBeLessThanOrEqual(deadline)
    expect(renewalFrames[index].at).toBeGreaterThanOrEqual(renewal.calledAt)
    expect(renewalFrames[index].response.at).toBeLessThanOrEqual(renewal.completedAt)
    deadline = renewalFrames[index].at + LOCK_MS
  }
  expect(deadline).toBeGreaterThan(verdictAt)
  if (!complete) {
    expect(b.decidedAt).toBeUndefined()
    expect(results.filter(({ member, type }) => member === 'B' && type !== 'renew')).toEqual([])
    expect(wire.filter(({ label, isRenewAck }) => label === 'B' && !isRenewAck)).toEqual([])
    return
  }
  expect(proof.gateOpenedAt).toBeGreaterThanOrEqual(verdictAt)
  expect(b.decidedAt).toBeGreaterThanOrEqual(proof.gateOpenedAt)
  expect(b.decidedAt).toBeLessThan(deadline)
  const bFrames = check('B', 'accept', 0)
  expect(bFrames).toHaveLength(1)
  expect(bFrames[0].at).toBeGreaterThanOrEqual(b.decidedAt)
  expect(results.filter(({ member, type }) => member === 'B' && type !== 'renew')).toHaveLength(1)
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
          requestHex: bytes.toString('hex'),
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
        fact.receivedAt = performance.now()
        fact.responseHex = bytes.toString('hex')
        fact.decoding = Promise.resolve()
          .then(() => {
            if (bytes[8] !== 0) throw new Error('Unexpected response header tags')
            return decoder.decode(bytes.subarray(9))
          })
          .then(({ errorCode, responses }) => {
            fact.response = {
              errorCode,
              at: fact.receivedAt,
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
    return this.frames.map(({ request, at, response, decodeError, requestHex, responseHex }) => ({
      ...request,
      requestHex,
      responseHex,
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

let fileStart = 0

/**
 * Preserves nested cleanup failures and their timings without following repeated causes.
 * @param {Error} error Original failure.
 * @param {Set<Error>} [seen] Ancestors already described.
 * @returns {object|null} Original error details, including aggregate and cleanup failures.
 */
const cleanupError = (error, seen = new Set()) => {
  if (error == null) return null
  const detail = {
    ...describeError(error),
    message: String(error.message),
    step: error.step,
    elapsedMs: error.elapsedMs,
  }
  if (seen.has(error)) return detail
  const ancestors = new Set([...seen, error])
  return {
    ...detail,
    cause: error.cause ? cleanupError(error.cause, ancestors) : undefined,
    errors: error.errors?.map((failure) => cleanupError(failure, ancestors)),
    cleanupErrors: error.cleanupErrors?.map((failure) => cleanupError(failure, ancestors)),
  }
}

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
   * @param {Function} [recordAdmin] Receives admin request and response frame observations.
   */
  constructor(
    connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect),
    recordAdmin = () => {}
  ) {
    this.connect = connect
    this.recordAdmin = recordAdmin
    this.traceAdmin = false
    this.adminFrames = []
    this.sockets = []
    this.frames = []
    this.holds = []
    this.acknowledgements = new AcknowledgementTrace()
  }

  /**
   * @param {string} label the client the sockets belong to
   * @returns {Function} a Kafka `socketFactory`
   */
  factory(label) {
    return (options) => {
      const socket = this.connect(options)
      socket.setKeepAlive?.(true, 60000)
      return this.wrap(label, this.acknowledgements.wrap(socket, label), options)
    }
  }

  /**
   * @param {string} label the owning client
   * @param {object} socket the real socket
   * @param {{host?:string, port?:number}} [address] Original connection destination.
   * @returns {object} the same socket, observed
   */
  wrap(label, socket, address = {}) {
    const entry = {
      label,
      socket,
      writer: new FrameSplitter(),
      reader: new FrameSplitter(),
      sent: 0,
      answered: 0,
      broker: `${address.host}:${address.port}`,
      pendingAdmin: new Map(),
    }
    this.sockets.push(entry)
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => this.write(entry, write, data, rest)
    socket.on('data', (chunk) => {
      const frames = entry.reader.push(chunk)
      entry.answered += frames.length
      for (const frame of frames) this.adminResponse(entry, frame)
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
    if (!hold) {
      const result = write(data, ...rest)
      for (const { correlationId } of headers) {
        const fact = entry.pendingAdmin.get(correlationId)
        if (!fact) continue
        fact.writeReturnedAt = performance.now()
        this.recordAdmin({ status: 'written', ...fact })
      }
      return result
    }
    hold.pending = () => write(data, ...rest)
    return true
  }

  /**
   * Keeps share frames and cleanup admin frames with their original headers and write times.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @returns {{ apiKey: number }} the decoded request header
   */
  record(entry, frame) {
    const header = readRequestHeader(frame)
    if (entry.label === 'admin' && this.traceAdmin) {
      const fact = {
        broker: entry.broker,
        socket: this.sockets.indexOf(entry),
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
        correlationId: header.correlationId,
        writtenAt: performance.now(),
        writeReturnedAt: null,
        answeredAt: null,
        requestHex: frame.toString('hex'),
        responseHex: null,
        response: null,
      }
      this.adminFrames.push(fact)
      entry.pendingAdmin.set(header.correlationId, fact)
      this.recordAdmin({ status: 'write-started', ...fact })
    }
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

  /**
   * Pairs an admin answer with its original socket and request, retaining undecoded bytes on error.
   * @param {object} entry Observed socket and its unanswered admin requests.
   * @param {Buffer} frame Complete response frame including length and correlation ID.
   * @returns {void} Decoding is observed without delaying the client.
   */
  adminResponse(entry, frame) {
    const fact = entry.pendingAdmin.get(frame.readInt32BE(4))
    if (!fact) return
    entry.pendingAdmin.delete(fact.correlationId)
    fact.answeredAt = performance.now()
    fact.responseHex = frame.toString('hex')
    this.recordAdmin({ status: 'answered', ...fact })
    Promise.resolve()
      .then(async () => {
        const definitions = ADMIN_TRACE_APIS[fact.apiKey]
        if (!definitions) return
        const payload = frame.subarray(8)
        const decoder = new Decoder(payload)
        if (responseHeaderVersion(fact.apiKey, fact.apiVersion) === RESPONSE_HEADER_V1) {
          decoder.readTaggedFields()
        }
        const { response } = definitions.protocol({ version: fact.apiVersion })({})
        fact.response = await response.decode(payload.subarray(decoder.offset))
        this.recordAdmin({ status: 'decoded', ...fact })
      })
      .catch((error) => {
        fact.decodeError = describeError(error)
        this.recordAdmin({ status: 'decode-failed', ...fact })
      })
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

/** Observes original resource owners without changing method results or receiver identities. */
class ResourceGauge {
  /** Retains owners until after the cleanup comparison, including destroyed connections. */
  constructor() {
    this.ledgers = new Set()
    this.queues = new Set()
    this.emitters = new Set()
    this.restores = []
  }

  /** @returns {void} Installs call-through observers before clients are constructed. */
  install() {
    this.watch(
      ShareAckLedger.prototype,
      [
        'canStart',
        'decide',
        'complete',
        'completeRange',
        'dropExpired',
        'needsFlush',
        'prepare',
        'settle',
        'invalidate',
      ],
      this.ledgers
    )
    this.watch(
      RequestQueue.prototype,
      ['push', 'scheduleRequestTimeoutCheck', 'waitForPendingRequests', 'destroy'],
      this.queues
    )
    this.watch(InstrumentationEventEmitter.prototype, ['addListener', 'emit'], this.emitters)
  }

  /**
   * @param {object} prototype Original class prototype.
   * @param {string[]} names Methods to observe.
   * @param {Set<object>} seen Original receivers retained for measurement.
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

  /** @returns {void} Restores observers only after the final resource proof. */
  uninstall() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.ledgers.clear()
    this.queues.clear()
    this.emitters.clear()
  }

  /** @returns {number} Entries still held by original ledgers, including retired acquisitions. */
  ledgerSize() {
    return [...this.ledgers].reduce((total, ledger) => total + ledger.size, 0)
  }

  /** @returns {number} Pending and in-flight requests, regardless of socket destruction. */
  queueDepth() {
    return [...this.queues].reduce(
      (total, queue) => total + queue.pending.length + queue.inflight.size,
      0
    )
  }

  /** @returns {object[]} Original admin queue entries and throttle/timer state without more I/O. */
  adminQueues() {
    return [...this.queues]
      .filter(({ clientId }) => clientId?.startsWith('admin-'))
      .map((queue) => ({
        broker: queue.broker,
        throttledUntil: queue.throttledUntil,
        pendingCheckScheduled: Boolean(queue.throttleCheckTimeoutId),
        timeoutCheckScheduled: Boolean(queue.requestTimeoutIntervalId),
        pending: queue.pending.map(
          ({ entry: { apiKey, apiVersion, correlationId }, createdAt }) => ({
            apiKey,
            apiVersion,
            correlationId,
            createdAt,
          })
        ),
        inflight: [...queue.inflight.values()].map(({ entry, createdAt, sentAt }) => ({
          apiKey: entry.apiKey,
          apiVersion: entry.apiVersion,
          correlationId: entry.correlationId,
          createdAt,
          sentAt,
        })),
      }))
  }

  /**
   * @param {Iterable<import('events').EventEmitter>} emitters Original client or queue emitters.
   * @returns {number} Listeners over every event of every retained emitter.
   */
  listenerCount(emitters) {
    let total = 0
    for (const emitter of emitters)
      for (const event of emitter.eventNames()) total += emitter.listenerCount(event)
    return total
  }

  /**
   * @param {object} sources Socket, process-listener and event-loop measurements.
   * @returns {{ snapshot: () => Promise<object> }} Probe with fixed keys and measured owner totals.
   */
  probe(sources) {
    const probe = createResourceProbe({
      ...sources,
      queueDepth: () => this.queueDepth(),
      ledgerSize: () => this.ledgerSize(),
    })
    return {
      snapshot: async () => {
        const snapshot = await probe.snapshot()
        return {
          ...snapshot,
          listeners: {
            ...snapshot.listeners,
            clients: this.listenerCount([...this.emitters].map(({ emitter }) => emitter)),
            queues: this.listenerCount(this.queues),
          },
        }
      },
    }
  }
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
    this.cleanupSteps = []
    this.tap = new ShareWireTap(undefined, (frame) =>
      this.row('cleanup-frame', {
        steps: this.cleanupSteps
          .filter(({ elapsedMs }) => elapsedMs === null)
          .map(({ step }) => step),
        ...frame,
        writtenAtMs: frame.writtenAt - this.startedAt,
        answeredAtMs: frame.answeredAt === null ? null : frame.answeredAt - this.startedAt,
      })
    )
    this.gauge = new ResourceGauge()
    this.probe = this.gauge.probe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      activeResources: true,
    })
    this.members = new Map()
    this.entries = []
    this.handlerWork = new Set()
    this.startedAt = performance.now()
    this.requests = []
    this.corpus = []
    this.expected = []
    this.renewals = []
    this.renewOutcome = null
    this.expiryRenewals = []
    this.acquisitions = []
    this.tickets = []
    this.ledgerAcquisitions = new WeakMap()
    this.ticketFacts = new WeakMap()
    this.expiryGate = null
    this.fetched = null
    this.gateAcquisitions = []
    this.gateExpiry = null
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
      elapsedMs: performance.now() - this.startedAt,
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
  }

  /**
   * Registers a cleanup with its original order, scope and deadline, adding step diagnostics.
   * @param {string} step Operation name.
   * @param {Function} close Cleanup operation receiving the stack's signal.
   * @returns {void}
   */
  addCleanup(step, close) {
    this.stack.add(step, (signal) => this.cleanupStep(step, () => close(signal)))
  }

  /**
   * Records the start, duration and original outcome of one cleanup operation.
   * @param {string} step Operation name.
   * @param {Function} work Operation to run and join.
   * @returns {Promise<*>} Original result.
   */
  async cleanupStep(step, work) {
    this.tap.traceAdmin = true
    const startedAt = performance.now()
    const entry = { step, startedAtMs: startedAt - this.startedAt, elapsedMs: null, error: null }
    this.cleanupSteps.push(entry)
    this.row('cleanup-step', { status: 'started', step, startedAtMs: entry.startedAtMs })
    try {
      return await work()
    } catch (error) {
      error.step ??= step
      error.elapsedMs ??= performance.now() - startedAt
      entry.error = cleanupError(error)
      throw error
    } finally {
      entry.elapsedMs = performance.now() - startedAt
      this.row('cleanup-step', {
        status: entry.error ? 'failed' : 'settled',
        ...entry,
        adminQueues: this.gauge.adminQueues(),
      })
    }
  }

  /** @returns {object} Handler counts, actual outcomes and original wire acknowledgements. */
  progress() {
    return {
      members: [...this.members.values()].map(({ label, memberId, acks, crashes, expired }) => ({
        label,
        memberId,
        delivered: this.entriesOf(label).length,
        handled: this.entriesOf(label).filter(({ handled }) => handled).length,
        acknowledgements: acknowledgementResults(acks),
        crashes: crashes.map((error) => describeError(error)),
        expired,
      })),
      entries: this.entries,
      renewals: this.renewals,
      controlledExpiry: this.kind === 'expire' ? this.expiryProof() : undefined,
      renewOutcome: this.renewOutcome && {
        ...this.renewOutcome,
        error: describeError(this.renewOutcome.error),
      },
      acknowledgementRequests: acknowledgementRequests(this.tap.frames),
      wireAcknowledgements: this.tap.acknowledgements.snapshot(),
      cleanupSteps: this.cleanupSteps,
      adminCleanupRequests: this.tap.adminFrames,
    }
  }

  /**
   * Runs the whole case: setup, the scenario's action, final verification and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    this.gauge.install()
    if (this.kind === 'gate') this.watchGateAcquisition()
    if (this.kind === 'expire') this.watchExpiryAcquisitions()
    try {
      const left = FILE_MS - (performance.now() - fileStart)
      if (left < this.scenario.budgetMs + JOIN_MS) {
        throw new Error(`Only ${Math.round(left)} ms of the file deadline remain for this case`)
      }
      this.beforeAdmin = await captureShareCleanupBaseline(this.probe, { signal: scope })
      this.baseline = this.beforeAdmin
      await this.stack.run(
        () =>
          bounded(
            scope,
            this.scenario.budgetMs - CLEANUP_MS - JOIN_MS,
            async (work) => {
              try {
                this.row('phase', { phase: 'setup' })
                await this.setup(work)
                this.row('phase', { phase: this.kind })
                await this[this.kind](work)
                this.row('phase', { phase: 'finish' })
                await this.finish(work)
              } catch (error) {
                this.row('work-failed', { error: describeError(error), ...this.progress() })
                throw error
              }
            },
            { label: 'The lock workload and verification' }
          ),
        { budgetMs: CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.row('case-failed', { error: cleanupError(error), ...this.progress() })
      throw error
    } finally {
      this.gauge.uninstall()
    }
  }

  /**
   * Creates the topic and group configs, connects the admin and producer, measures the baseline,
   * then joins both members with member B's first fetch held.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.admin = this.client('admin').admin()
    this.addCleanup('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.addCleanup(
      'remove admin listeners',
      this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    )
    this.addCleanup('delete fixtures', (signal) => this.deleteFixtures(signal))
    this.addCleanup('reset group configs', (signal) => this.resetConfigs(signal))
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
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.addCleanup(
      'remove producer listeners',
      this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    )
    this.addCleanup('disconnect producer', () => this.producer.disconnect())
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
      consumer.on(events.LOCK_EXPIRED, ({ payload }) => {
        member.expired.push(payload)
        this.freezeGateExpiry()
      }),
      consumer.on(events.CRASH, ({ payload }) => member.crashes.push(payload.error)),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
      consumer.on(events.FETCH, ({ payload }) => this.onFetch(member, payload)),
    ]
    this.addCleanup(`remove ${label} listeners`, () => removers.forEach((remove) => remove()))
    this.addCleanup(`disconnect and join ${label}`, async () => {
      this.tap.discardHolds()
      await consumer.disconnect({ timeout: 2000 })
      await Promise.allSettled([...this.handlerWork])
      await this.tap.acknowledgements.join()
    })
    await bounded(scope, 10000, () => consumer.connect())
    await bounded(scope, 10000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 15000, () => consumer.run({ eachMessage: this.handler(label, scope) }))
  }

  /** @returns {void} Observes validated parallel acquisition counts and once-only ticket ownership. */
  watchExpiryAcquisitions() {
    const owner = this
    const adopt = ShareFetchLoop.prototype.adopt
    ShareFetchLoop.prototype.adopt = function (...args) {
      const cohort = adopt.apply(this, args)
      if (this.groupId === owner.group && cohort.walk.deliver.records.length) {
        const { records, deliveryCounts } = cohort.walk.deliver
        if (!deliveryCounts || deliveryCounts.length !== records.length) {
          throw new Error('Acquisition delivery counts must match records')
        }
        const counts = Array.from(deliveryCounts)
        if (counts.some((count) => !Number.isInteger(count) || count < 1 || count > 32767)) {
          throw new Error('Acquisition delivery counts must be integers in 1–32767')
        }
        const [exchange, topicId, partition, lockDuration] = args
        const member = [...owner.members.values()].find(
          ({ memberId }) => memberId === this.memberId
        )?.label
        const acquisition = {
          id: owner.acquisitions.length + 1,
          member,
          group: this.groupId,
          memberId: this.memberId,
          topicId,
          partition: partition.partitionIndex,
          sentAt: exchange.sentAt,
          deadline: exchange.sentAt + lockDuration,
          lockDuration,
          identities: records.map((message) =>
            shareRecordIdentity({
              ...message,
              topic: cohort.topic,
              partition: cohort.partition,
            })
          ),
          counts,
          request: JSON.parse(JSON.stringify(exchange.fields)),
          acquiredRecords: partition.acquiredRecords,
          errorCode: partition.errorCode,
        }
        owner.acquisitions.push(acquisition)
        owner.ledgerAcquisitions.set(cohort.ledger, acquisition)
      }
      return cohort
    }
    const prepare = ShareAckLedger.prototype.prepare
    ShareAckLedger.prototype.prepare = function (...args) {
      const result = prepare.apply(this, args)
      const acquisition = owner.ledgerAcquisitions.get(this)
      if (acquisition && result.ticket) {
        const fact = {
          id: owner.tickets.length + 1,
          acquisition: acquisition.id,
          member: acquisition.member,
          preparedAt: performance.now(),
          isRenewAck: result.ticket.isRenewAck,
          batches: result.ticket.acknowledgementBatches,
        }
        owner.tickets.push(fact)
        owner.ticketFacts.set(result.ticket, fact)
      }
      return result
    }
    const settle = ShareAckLedger.prototype.settle
    ShareAckLedger.prototype.settle = function (ticket, ...args) {
      const result = settle.call(this, ticket, ...args)
      const fact = owner.ticketFacts.get(ticket)
      if (fact) {
        fact.settlements = (fact.settlements ?? 0) + 1
        fact.settledAt = performance.now()
        fact.results = result.map(({ error, ...fields }) => ({
          ...fields,
          error: describeError(error),
        }))
      }
      return result
    }
    this.gauge.restores.push(() => {
      ShareFetchLoop.prototype.adopt = adopt
      ShareAckLedger.prototype.prepare = prepare
      ShareAckLedger.prototype.settle = settle
    })
  }

  /** @returns {object} Complete JSON-safe expiry observations for independent replay. */
  expiryProof() {
    return {
      group: this.group,
      topic: this.topic,
      topicId: this.topicId,
      members: Object.fromEntries(
        [...this.members.values()].map(({ label, memberId }) => [label, memberId])
      ),
      acquisitions: this.acquisitions,
      entries: this.entries,
      tickets: this.tickets,
      gateOpenedAt: this.expiryGate?.openedAt,
      renewals: this.expiryRenewals,
      wire: this.tap.acknowledgements.snapshot(),
      results: [...this.members.values()].flatMap(({ label, acks }) =>
        acks.map(({ error, ...fields }) => ({
          ...fields,
          member: label,
          error: describeError(error),
        }))
      ),
    }
  }

  /** @returns {void} Records the original acquisition and its conservative expiry boundary. */
  watchGateAcquisition() {
    const owner = this
    const original = ShareFetchLoop.prototype.adopt
    ShareFetchLoop.prototype.adopt = function (...args) {
      const cohort = original.apply(this, args)
      if (
        this.groupId === owner.group &&
        this.memberId === owner.members.get('A')?.memberId &&
        cohort.walk.deliver.records.length > 0
      ) {
        const [exchange, , , lockDuration] = args
        owner.gateAcquisitions.push({
          cohort,
          sentAt: exchange.sentAt,
          lockDuration,
          deadline: exchange.sentAt + lockDuration,
          identities: cohort.walk.deliver.records.map((message) =>
            shareRecordIdentity({ ...message, topic: cohort.topic, partition: cohort.partition })
          ),
        })
      }
      return cohort
    }
    this.gauge.restores.push(() => {
      ShareFetchLoop.prototype.adopt = original
    })
  }

  /**
   * Freezes original invocation intervals at the first callback crossing acquisition expiry.
   * Handler entry/return times preserve the exact boundary even if the event loop resumes later.
   * @returns {void}
   */
  freezeGateExpiry() {
    const acquisition = this.gateAcquisitions[0]
    if (this.gateExpiry || !acquisition || performance.now() < acquisition.deadline) return
    const { deadline, identities, sentAt, lockDuration } = acquisition
    const own = this.entriesOf('A').filter(({ at }) => at <= deadline)
    const processed = own.map(({ identity }) => identity)
    const active = own.filter(({ returnedAt }) => returnedAt === null || returnedAt > deadline)
    this.gateExpiry = Object.freeze({
      capturedAt: performance.now(),
      deadline,
      sentAt,
      lockDuration,
      processed: Object.freeze(processed),
      dropped: Object.freeze(identities.filter((identity) => !processed.includes(identity))),
      inFlight: Object.freeze(
        active.map(({ invocation, identity, at }) => Object.freeze({ invocation, identity, at }))
      ),
      expected: Object.freeze(
        [null, ...active.map(({ identity }) => identity)].map((duplicate) =>
          Object.freeze(
            identities.map((identity) => Object.freeze([identity, identity === duplicate ? 2 : 1]))
          )
        )
      ),
    })
    this.row('held-set', { expiry: this.gateExpiry })
  }

  /**
   * @returns {Map<string, number>} The sole matching multiset declared by the original expiry set.
   * @throws {Error} For any replay or omission outside those predeclared alternatives.
   */
  gateExpectation() {
    const actual = tally(this.entries.map(({ identity }) => identity))
    const matching = this.gateExpiry.expected
      .map((entries) => new Map(entries))
      .filter((expected) =>
        Object.values(multisetProblems(expected, actual)).every((problems) => problems.length === 0)
      )
    expect(matching).toHaveLength(1)
    return matching[0]
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
    return (payload) => {
      const work = this.handle(label, payload, scope)
      this.handlerWork.add(work)
      work.then(
        () => this.handlerWork.delete(work),
        () => this.handlerWork.delete(work)
      )
      return work
    }
  }

  /**
   * Records and joins one handler invocation under the workload scope.
   * @param {string} label Member label.
   * @param {object} payload Handler payload.
   * @param {AbortSignal} scope Workload scope.
   * @returns {Promise<void>} When the invocation returns.
   */
  async handle(label, payload, scope) {
    const { topic, partition, message } = payload
    this.freezeGateExpiry()
    const entry = {
      invocation: this.entries.length + 1,
      member: label,
      identity: shareRecordIdentity({ ...message, topic, partition }),
      deliveryCount: message.deliveryCount,
      at: performance.now(),
      returnedAt: null,
      handled: false,
      ...(this.kind === 'expire' && {
        acquisition: this.acquisitions.find(
          (row) =>
            row.member === label &&
            row.identities.includes(shareRecordIdentity({ ...message, topic, partition })) &&
            row.counts.includes(message.deliveryCount)
        )?.id,
      }),
    }
    this.entries.push(entry)
    try {
      await this.behave(entry, payload, scope)
      entry.handled = true
    } finally {
      entry.returnedAt = performance.now()
      this.freezeGateExpiry()
    }
  }

  /**
   * The scenario's fixed handler behaviour. B holds and renews during controlled expiry; other
   * B invocations accept at once. A follows the schedule from its original handler start.
   * @param {object} entry the recorded entry of this invocation
   * @param {object} payload the eachMessage payload
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async behave(entry, payload, scope) {
    if (entry.member === 'B') {
      if (this.kind === 'expire') {
        let next = entry.at + 1500
        while (!this.expiryGate.openedAt) {
          const opened = await bounded(
            scope,
            Infinity,
            () =>
              new Promise((resolve) => {
                const timer = setTimeout(
                  () => resolve(false),
                  Math.max(0, next - performance.now())
                )
                this.expiryGate.promise.then(() => {
                  clearTimeout(timer)
                  resolve(true)
                })
              })
          )
          if (opened) break
          if (performance.now() < next) continue
          const renewal = {
            scheduledAt: next,
            calledAt: performance.now(),
            duration: null,
            error: null,
          }
          this.expiryRenewals.push(renewal)
          try {
            renewal.duration = await payload.renew()
            expect(renewal.duration).toBe(LOCK_MS)
          } catch (error) {
            renewal.error = describeError(error)
            throw error
          } finally {
            renewal.completedAt = performance.now()
          }
          next += 1500
        }
      }
      entry.decidedAt = performance.now()
      if (this.scenario.mode === 'explicit') payload.acknowledge('accept')
      return
    }
    if (this.kind === 'gate') {
      await delay(scope, 200)
      return
    }
    this.gateB.release()
    const wait = async (until) => {
      while (performance.now() < entry.at + until) {
        await delay(scope, Math.max(1, entry.at + until - performance.now()))
      }
    }
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
    entry.decidedAt = performance.now()
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
        this.topicId = described
          .flatMap(({ assignment }) => assignment)
          .find(({ topic }) => topic === this.topic)?.topicId
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
   * two, and A's late Accept records the broker verdict without crashing A.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async expire(scope) {
    let open
    const promise = new Promise((resolve) => {
      open = resolve
    })
    this.expiryGate = { promise, openedAt: null }
    this.addCleanup('release the expiry handler', () => {
      this.expiryGate.openedAt ??= performance.now()
      open()
    })
    await this.produce(scope)
    const first = await this.firstDelivery(scope)
    const second = await this.awaitRedelivery(scope)
    expect([first.deliveryCount, second.deliveryCount]).toEqual([1, 2])
    expect(second.identity).toBe(first.identity)
    const a = this.members.get('A')
    await this.observe(scope, 'The late accept result', 15000, () => this.accepts(a).length > 0)
    await this.observe(
      scope,
      'The original B renewal schedule settling',
      3000,
      () =>
        this.expiryRenewals.length > 0 &&
        this.expiryRenewals.every(({ completedAt }) => completedAt !== undefined)
    )
    await this.tap.acknowledgements.join()
    assertControlledExpiry(JSON.parse(JSON.stringify(this.expiryProof())), false)
    this.expiryGate.openedAt = performance.now()
    open()
    await this.observe(
      scope,
      'B accepting after the late rejection',
      5000,
      () => this.accepts(this.members.get('B')).length > 0 && second.returnedAt !== null
    )
    await this.tap.acknowledgements.join()
    assertControlledExpiry(JSON.parse(JSON.stringify(this.expiryProof())))
    this.row('controlled-expiry', { proof: this.expiryProof() })
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
   * receives and accepts the record at count two; a skipped late Accept is recorded separately.
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
    this.checkLateAccept(this.accepts(a)[0])
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
    expect(this.gateAcquisitions).toHaveLength(1)
    expect(this.gateAcquisitions[0].lockDuration).toBe(LOCK_MS)
    expect([...this.gateAcquisitions[0].identities].sort()).toEqual([...this.expected].sort())
    expect(this.gateExpiry).not.toBeNull()
    const { processed: frozenProcessed, dropped, inFlight: frozenInvocations } = this.gateExpiry
    const inFlight = frozenInvocations.map(({ identity }) => identity)
    expect(inFlight.length).toBeLessThanOrEqual(1)
    const firstStart = own[0].at
    expect(own.filter(({ at }) => at > firstStart + LOCK_MS)).toEqual([])
    expect(own.every(({ deliveryCount }) => deliveryCount === 1)).toBe(true)
    const processed = new Set(frozenProcessed)
    expect(processed.size).toBe(own.length)
    expect([...processed].sort()).toEqual(own.map(({ identity }) => identity).sort())
    expect([...processed, ...dropped].sort()).toEqual([...this.expected].sort())
    const received = tally(second.map(({ identity }) => identity))
    expect(second.every(({ deliveryCount }) => deliveryCount === 2)).toBe(true)
    expect([...received.values()].every((n) => n === 1)).toBe(true)
    const duplicated = [...received.keys()].filter((identity) => processed.has(identity))
    expect(duplicated.filter((identity) => !inFlight.includes(identity))).toEqual([])
    expect(duplicated.length).toBeLessThanOrEqual(1)
    expect(dropped.filter((identity) => !received.has(identity))).toEqual([])
    expect(received.size).toBe(dropped.length + duplicated.length)
    this.expectedHandled = this.gateExpectation()
    this.row('held-set', {
      acquiredAt: this.acquiredAt('A'),
      processed: [...processed],
      dropped,
      inFlight,
      duplicated,
      expiry: this.gateExpiry,
      firstStart,
    })
    const offsetOf = new Map(this.corpus.map(({ identity, offset }) => [identity, offset]))
    const before = new Set(
      a.expired.filter(({ stage }) => stage === 'before-handler').flatMap(({ offsets }) => offsets)
    )
    expect(dropped.filter((identity) => !before.has(offsetOf.get(identity)))).toEqual([])
    expect([...processed].filter((identity) => before.has(offsetOf.get(identity)))).toEqual([])
    const accepted = () => a.acks.filter(({ type, error }) => type === 'accept' && error === null)
    const lapse = this.gateExpiry.deadline
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
   * Requires the late broker verdict, allowing a successful skip only after B settled the record.
   * @param {object} late A's actual late Accept outcome.
   * @returns {void}
   */
  checkLateAccept(late) {
    this.recordAcknowledgements()
    const successful = this.accepts(this.members.get('B')).filter(({ error }) => error === null)
    expect(successful).toHaveLength(1)
    expect(successful[0]).toMatchObject({
      topic: this.topic,
      partition: 0,
      firstOffset: '0',
      lastOffset: '0',
    })
    const trace = this.tap.acknowledgements
    const b = new AcknowledgementTrace()
    b.frames = trace.frames.filter(({ request }) => request.label === 'B')
    expect(b.confirmed(this.group, this.topicId, 0).has('0')).toBe(true)
    const a = trace.frames.filter(({ request }) => request.label === 'A' && !request.isRenewAck)
    expect(a).toHaveLength(1)
    expect(a[0].request.topics).toEqual([
      {
        topicId: this.topicId,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [
              { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [ACCEPT] },
            ],
          },
        ],
      },
    ])
    expect(a[0].response?.errorCode).toBe(0)
    expect(a[0].response.topics).toHaveLength(1)
    const [reply] = a[0].response.topics
    expect(reply.topicId).toBe(this.topicId)
    expect(reply.partitions).toHaveLength(1)
    const code =
      a[0].request.apiKey === SHARE_FETCH
        ? reply.partitions[0].acknowledgeErrorCode
        : reply.partitions[0].errorCode
    expect(code).toBe(late.error === null ? 0 : INVALID_RECORD_STATE)
    if (late.error === null) {
      expect(successful[0].at).toBeLessThan(this.entriesOf('A')[0].decidedAt)
      this.row('late-accept', {
        verdict: 'success-after-prior-accept',
        priorAccept: acknowledgementResults(successful),
        ...this.progress(),
      })
    } else {
      expect(late.error.reason).toBe('rejected-by-broker')
      expect(causeCodes(late.error)).toContain(INVALID_RECORD_STATE)
    }
  }

  /** @returns {boolean} Whether successful Accept results cover the immutable corpus. */
  acceptedCorpus() {
    const accepted = new Set(
      [...this.members.values()].flatMap(({ acks }) =>
        acks
          .filter(
            ({ topic, partition, type, error }) =>
              topic === this.topic && partition === 0 && type === 'accept' && error === null
          )
          .flatMap(({ firstOffset, lastOffset }) => offsetsOf(firstOffset, lastOffset))
      )
    )
    const wire = this.tap.acknowledgements.confirmed(this.group, this.topicId, 0)
    return this.corpus.every(({ offset }) => accepted.has(offset) && wire.has(offset))
  }

  /**
   * Polls agreeing listings with known zero lag against the independently read log end.
   * @param {AbortSignal} scope Workload scope.
   * @returns {Promise<object[]>} Terminal partition rows.
   */
  async awaitBrokerEnd(scope) {
    return bounded(
      scope,
      20000,
      async (poll) => {
        await pollUntil(poll, Infinity, () => this.acceptedCorpus(), {
          label: 'Successful Accept coverage',
        })
        const { rows } = await awaitTerminalSharePosition(
          this.admin,
          {
            groupId: this.group,
            topic: this.topic,
            partitions: [{ partition: 0, floor: '0', end: this.log.get(0).endOffset }],
          },
          {
            signal: poll,
            timeoutMs: 20000,
            record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
          }
        )
        return rows
      },
      { label: 'The terminal broker share position' }
    )
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
    await Promise.allSettled([...this.handlerWork])
    const verified = await assertQuietTerminalGroup({
      kafka: this.client('verifier'),
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions: [{ partition: 0, floor: '0', end: this.log.get(0).endOffset }],
      signal: scope,
      timeoutMs: 20000,
      record: (fields) => this.row('quiet-verifier', fields),
    })
    this.broker = verified.position.rows
    this.quiet = verified
    if (this.kind === 'expire')
      assertControlledExpiry(JSON.parse(JSON.stringify(this.expiryProof())))
    const ends = await bounded(scope, 20000, () =>
      endOffsets({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect(ends.get(0)).toBe(this.log.get(0).endOffset)
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
    this.tap.discardHolds()
    const failures = []
    const steps = [
      [
        'prove fixture absence on every broker',
        () =>
          waitForShareFixtureAbsence(this.admin, [this.group], [this.topic], {
            signal,
            unrelated: [],
            delayMs: 50,
            record: (event, fields) => this.row(event, fields),
          }),
      ],
      [
        'prove resources',
        async () => {
          if (this.baseline) {
            this.after = await assertShareCleanup(this.admin, [], [], this.baseline, this.probe, {
              signal,
              disconnectAdmin: true,
              record: (event, fields) => this.row(event, fields),
            })
          }
        },
      ],
      ['disconnect admin', () => this.admin.disconnect()],
    ]
    for (const [step, work] of steps) {
      try {
        await this.cleanupStep(step, work)
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw new AggregateError(failures, 'Cleanup proof failed')
  }

  /**
   * Deletes share group state before its topic, then proves their absence together in cleanup.
   * @param {AbortSignal} signal Cleanup scope shared by both operations.
   * @returns {Promise<void>} Once both deletion operations have settled.
   */
  async deleteFixtures(signal) {
    const failures = []
    for (const [step, work] of [
      ['delete group', () => this.deleteGroup(signal)],
      ['delete topic', () => this.deleteTopic(signal)],
    ]) {
      try {
        signal.throwIfAborted()
        await this.cleanupStep(step, work)
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw new AggregateError(failures, 'Fixture deletion failed')
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} When deletion is refused; the final cleanup proof checks absence.
   */
  async deleteTopic(signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} When deletion is refused; the final cleanup proof checks absence.
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
      { delayMs: 50, label: 'The group deletion' }
    )
  }

  /**
   * Removes the owned group config overrides and proves none remain.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses or an override remains
   */
  async resetConfigs(signal) {
    await resetShareGroupConfigs(this.admin, this.group, Object.keys(this.groupConfig), { signal })
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
        acknowledgements: this.progress(),
        quiet: this.quiet,
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

describe('lock acquisition observation', () => {
  let observer
  let run
  let node
  let partition
  let exchange
  let handled
  const topicId = '00112233-4455-6677-8899-aabbccddeeff'

  beforeEach(() => {
    handled = []
    run = new ShareRun({
      runner: {
        subscription: ['records'],
        options: {
          groupId: 'group',
          acknowledgementMode: 'implicit',
          maxPollInterval: 30000,
          logger: newLogger(),
          cluster: { topicName: () => 'records', onRoutingChange: () => () => {} },
        },
      },
      handler: ({ message }) => handled.push(message.deliveryCount),
      batch: false,
    })
    node = {
      issued: run.authorityToken,
      session: new ShareSession({ operation: run.operation }),
      keys: new Set([`${topicId}:0`]),
      lockDuration: LOCK_MS,
    }
    node.loop = run.createLoop(node)
    exchange = { open: node.session.token, sentAt: performance.now(), fields: { groupId: 'group' } }
    partition = {
      partitionIndex: 0,
      errorCode: 0,
      messages: [
        { offset: '0', key: null, value: null },
        { offset: '1', key: null, value: null },
      ],
      acquiredRecords: [
        { firstOffset: '0', lastOffset: '0', deliveryCount: 1 },
        { firstOffset: '1', lastOffset: '1', deliveryCount: 32767 },
      ],
    }
    observer = new LockCase('expire')
    observer.group = 'group'
    observer.members.set('A', { label: 'A', memberId: run.membership.memberId })
  })

  afterEach(() => {
    observer.gauge.uninstall()
    jest.restoreAllMocks()
    node.session.retire()
    run.manager.shutdown()
    run.poll.stop()
    run.unrouted()
  })

  it('copies parallel counts before dispatch attaches them to the original records', async () => {
    observer.watchExpiryAcquisitions()
    expect(partition.messages.every((message) => !Object.hasOwn(message, 'deliveryCount'))).toBe(
      true
    )
    const cohort = node.loop.adopt(exchange, topicId, partition, LOCK_MS)
    expect(partition.messages.every((message) => !Object.hasOwn(message, 'deliveryCount'))).toBe(
      true
    )
    expect(observer.acquisitions).toHaveLength(1)
    const [acquisition] = observer.acquisitions
    expect(acquisition.identities).toEqual([
      '["records",0,"0",null,null]',
      '["records",0,"1",null,null]',
    ])
    expect(Array.isArray(acquisition.counts)).toBe(true)
    expect(acquisition.counts).toEqual([1, 32767])
    await node.loop.dispatchCohorts([cohort])
    expect(handled).toEqual([1, 32767])
    cohort.walk.deliver.deliveryCounts.fill(2)
    partition.messages.forEach((message) => (message.deliveryCount = 3))
    expect(JSON.parse(JSON.stringify(acquisition)).counts).toEqual([1, 32767])
  })

  it.each([
    ['missing', undefined],
    ['short', new Uint16Array([1])],
    ['extra', new Uint16Array([1, 2, 3])],
    ['zero', [1, 0]],
    ['over-limit', [1, 32768]],
    ['fractional', [1, 1.5]],
    ['non-number', [1, '2']],
    ['NaN', [1, NaN]],
  ])('rejects %s parallel counts at adoption', (name, deliveryCounts) => {
    const adopt = ShareFetchLoop.prototype.adopt
    jest.spyOn(ShareFetchLoop.prototype, 'adopt').mockImplementation(function (...args) {
      const cohort = adopt.apply(this, args)
      cohort.walk.deliver.deliveryCounts = deliveryCounts
      return cohort
    })
    observer.watchExpiryAcquisitions()
    expect(() => node.loop.adopt(exchange, topicId, partition, LOCK_MS)).toThrow(
      'Acquisition delivery counts'
    )
    expect(observer.acquisitions).toEqual([])
  })
})

describe('share gate expiry snapshot', () => {
  /** @returns {LockCase} Original invocation intervals spanning a past acquisition deadline. */
  const fixture = () => {
    const run = new LockCase('gate')
    const deadline = performance.now() - 100
    run.row = () => {}
    run.gateAcquisitions.push({
      sentAt: deadline - LOCK_MS,
      lockDuration: LOCK_MS,
      deadline,
      identities: ['completed', 'active', 'dropped'],
    })
    run.entries.push(
      {
        invocation: 1,
        member: 'A',
        identity: 'completed',
        at: deadline - 400,
        returnedAt: deadline - 200,
      },
      {
        invocation: 2,
        member: 'A',
        identity: 'active',
        at: deadline - 200,
        returnedAt: deadline + 50,
      }
    )
    run.freezeGateExpiry()
    return run
  }

  it('freezes the active original at expiry even when the callback resumes after its return', () => {
    const run = fixture()
    const frozen = run.gateExpiry
    expect(frozen.processed).toEqual(['completed', 'active'])
    expect(frozen.dropped).toEqual(['dropped'])
    expect(frozen.inFlight.map(({ invocation, identity }) => [invocation, identity])).toEqual([
      [2, 'active'],
    ])
    run.entries.push({ member: 'B', identity: 'completed' })
    run.freezeGateExpiry()
    expect(run.gateExpiry).toBe(frozen)
    expect(frozen.inFlight.map(({ identity }) => identity)).toEqual(['active'])
  })

  it.each([null, 'active'])('admits the predeclared optional replay %s', (duplicate) => {
    const run = fixture()
    run.entries.push({ member: 'B', identity: 'dropped' })
    if (duplicate) run.entries.push({ member: 'B', identity: duplicate })
    expect(run.gateExpectation().get('active')).toBe(duplicate ? 2 : 1)
  })

  it.each(['completed', 'foreign', 'third-delivery', 'missing-dropped'])(
    'rejects %s without changing the frozen allowance',
    (fault) => {
      const run = fixture()
      if (fault !== 'missing-dropped') run.entries.push({ member: 'B', identity: 'dropped' })
      if (fault === 'third-delivery') {
        run.entries.push({ member: 'B', identity: 'active' }, { member: 'B', identity: 'active' })
      } else if (fault !== 'missing-dropped') {
        run.entries.push({ member: 'B', identity: fault })
      }
      expect(() => run.gateExpectation()).toThrow()
      expect(run.gateExpiry.inFlight.map(({ identity }) => identity)).toEqual(['active'])
    }
  )
})

describe('original share resource gauges', () => {
  let gauge
  let probe

  beforeEach(() => {
    gauge = new ResourceGauge()
    gauge.install()
    probe = gauge.probe({})
  })

  afterEach(() => gauge.uninstall())

  it('detects a ledger entry after the application handler has returned', async () => {
    const before = await probe.snapshot()
    const ledger = new ShareAckLedger({
      topic: 'resource-probe',
      topicId: 'resource-probe-id',
      partition: 0,
      token: Symbol('original-acquisition'),
      sentAt: 0,
      lockDuration: 3000,
      walk: {
        deliver: { records: [{ offset: '0' }], deliveryCounts: [1] },
        gaps: [],
        release: [],
        reject: [],
      },
    })
    ledger.complete(['0'], 'accept')
    expect((await probe.snapshot()).ledgerSize).toBe(before.ledgerSize + 1)
    expect(await probe.snapshot()).not.toEqual(before)
    ledger.invalidate('membership-lost')
    expect(await probe.snapshot()).toEqual(before)
  })

  it.each([
    ['pending', 0],
    ['inflight', 1],
  ])(
    'detects an original %s request after its socket and queue are destroyed',
    async (population, maxInFlightRequests) => {
      const before = await probe.snapshot()
      const socket = new net.Socket()
      const queue = new RequestQueue({
        maxInFlightRequests,
        requestTimeout: 30000,
        enforceRequestTimeout: false,
        logger: { debug: () => {}, error: () => {} },
        isConnected: () => !socket.destroyed,
      })
      try {
        queue.push({
          entry: { correlationId: 1, resolve: () => {}, reject: () => {} },
          expectResponse: true,
          sendRequest: () => {},
        })
        socket.destroy()
        queue.destroy()
        expect(population === 'pending' ? queue.pending.length : queue.inflight.size).toBe(1)
        expect((await probe.snapshot()).queueDepth).toBe(before.queueDepth + 1)
        expect(await probe.snapshot()).not.toEqual(before)
        queue.settleAll(new Error('cleanup'))
        expect(await probe.snapshot()).toEqual(before)
      } finally {
        socket.destroy()
        queue.settleAll(new Error('cleanup'))
        queue.destroy()
      }
    }
  )

  it('detects listeners on original client and destroyed queue emitters', async () => {
    const before = await probe.snapshot()
    const emitter = new InstrumentationEventEmitter()
    const remove = emitter.addListener('client-listener', () => {})
    expect((await probe.snapshot()).listeners.clients).toBe(before.listeners.clients + 1)
    expect(await probe.snapshot()).not.toEqual(before)
    remove()
    expect(await probe.snapshot()).toEqual(before)
    const queue = new RequestQueue({})
    queue.on('queue-listener', () => {})
    queue.destroy()
    expect((await probe.snapshot()).listeners.queues).toBe(before.listeners.queues + 1)
    expect(await probe.snapshot()).not.toEqual(before)
    queue.removeAllListeners()
    expect(await probe.snapshot()).toEqual(before)
  })
})

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

describe('scripted controlled expiry evidence', () => {
  /** @returns {object} Labelled observations; no broker state is simulated. */
  const proof = () => {
    const group = 'scripted-group'
    const topic = 'scripted-topic'
    const topicId = '12345678-1234-1234-1234-123456789abc'
    const identity = JSON.stringify([topic, 0, '0', 'key', 'value'])
    const error = {
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'rejected-by-broker',
      cause: { code: 121 },
    }
    const entries = [
      { member: 'A', identity, acquisition: 1, deliveryCount: 1, at: 100, decidedAt: 6100 },
      { member: 'B', identity, acquisition: 2, deliveryCount: 2, at: 3100, decidedAt: 6140 },
    ]
    const results = []
    const tickets = []
    const wire = []
    /** @param {string} label Owner. @param {boolean} renew Renewal flag. @param {number} at Original write. @param {number} code Ack code. @returns {void} Appends one complete exchange. */
    const add = (label, renew, at, code) => {
      const type = renew ? 'renew' : 'accept'
      const batches = [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [renew ? 4 : 1] }]
      wire.push({
        label,
        apiKey: 79,
        apiVersion: 2,
        socket: label === 'A' ? 1 : 2,
        correlationId: wire.length,
        groupId: group,
        memberId: label,
        isRenewAck: renew,
        at,
        topics: [{ topicId, partitions: [{ partitionIndex: 0, acknowledgementBatches: batches }] }],
        response: {
          errorCode: 0,
          at: at + 10,
          topics: [{ topicId, partitions: [{ partitionIndex: 0, errorCode: code }] }],
        },
      })
      results.push({
        member: label,
        topic,
        partition: 0,
        firstOffset: '0',
        lastOffset: '0',
        type,
        at: at + 20,
        error: code ? error : null,
      })
      tickets.push({
        id: tickets.length + 1,
        member: label,
        acquisition: label === 'A' ? 1 : 2,
        isRenewAck: renew,
        batches,
        preparedAt: at - 1,
        settledAt: at + 15,
        settlements: 1,
        results: [{ firstOffset: '0', lastOffset: '0', type, error: code ? error : null }],
      })
    }
    add('B', true, 4601, 0)
    add('B', true, 6101, 0)
    add('A', false, 6102, 121)
    add('B', false, 6141, 0)
    return {
      source: 'scripted',
      group,
      topic,
      topicId,
      members: { A: 'A', B: 'B' },
      entries,
      acquisitions: entries.map((entry, index) => ({
        id: index + 1,
        member: entry.member,
        group,
        topicId,
        partition: 0,
        sentAt: index ? 3000 : 0,
        lockDuration: LOCK_MS,
        deadline: (index ? 3000 : 0) + LOCK_MS,
        identities: [identity],
        counts: [index + 1],
      })),
      results,
      tickets,
      wire,
      gateOpenedAt: 6130,
      renewals: [4600, 6100].map((calledAt) => ({
        calledAt,
        completedAt: calledAt + 25,
        duration: 3000,
        error: null,
      })),
    }
  }

  it.each(['4.3.1', '4.4.0'])('requires the same controlled121 sequence for %s', (version) => {
    assertControlledExpiry(JSON.parse(JSON.stringify({ ...proof(), version })))
  })

  it.each([
    [
      'successful skip after prior B Accept',
      (row) => {
        row.wire[2].response.topics[0].partitions[0].errorCode = 0
        row.results[2].error = null
        row.tickets[2].results[0].error = null
        row.entries[1].decidedAt = 6000
        row.gateOpenedAt = 5999
        row.wire[3].at = 6001
        row.wire[3].response.at = 6011
        row.results[3].at = 6021
        row.tickets[3].preparedAt = 6000
        row.tickets[3].settledAt = 6016
        row.diagnostic = 'success-after-prior-accept'
      },
    ],
    [
      'no wire write',
      (row) => {
        row.wire.splice(2, 1)
      },
    ],
    [
      'foreign owner',
      (row) => {
        row.wire[2].memberId = 'foreign'
      },
    ],
    [
      'foreign group',
      (row) => {
        row.wire[2].groupId = 'foreign'
      },
    ],
    [
      'foreign UUID',
      (row) => {
        row.wire[2].topics[0].topicId = 'foreign'
      },
    ],
    [
      'foreign ticket',
      (row) => {
        row.tickets[2].acquisition = 2
      },
    ],
    [
      'fetch121 with ack0',
      (row) => {
        row.wire[2].apiKey = 78
        row.wire[2].response.topics[0].partitions[0] = {
          partitionIndex: 0,
          errorCode: 121,
          acknowledgeErrorCode: 0,
        }
      },
    ],
    [
      'group error',
      (row) => {
        row.wire[2].response.errorCode = 121
      },
    ],
    [
      'wrong typed reason',
      (row) => {
        row.results[2].error.reason = 'unacknowledged'
      },
    ],
    [
      'wrong cause',
      (row) => {
        row.results[2].error.cause.code = 122
      },
    ],
    [
      'duplicate outcome',
      (row) => {
        row.results.push(row.results[2])
      },
    ],
    [
      'duplicate settlement',
      (row) => {
        row.tickets[2].settlements = 2
      },
    ],
    [
      'B decides before A verdict',
      (row) => {
        row.entries[1].decidedAt = 6100
      },
    ],
    [
      'failed renewal',
      (row) => {
        row.renewals[0].error = { code: 121 }
      },
    ],
    [
      'missing renewal',
      (row) => {
        row.renewals = []
      },
    ],
    [
      'missing B settlement',
      (row) => {
        row.tickets.pop()
      },
    ],
    [
      'third delivery',
      (row) => {
        row.entries.push({ ...row.entries[1], deliveryCount: 3 })
      },
    ],
  ])('rejects %s', (_label, mutate) => {
    const input = JSON.parse(JSON.stringify(proof()))
    mutate(input)
    expect(() => assertControlledExpiry(input)).toThrow()
  })
})
