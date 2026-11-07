const shareFetchWire = require('../../../../testHelpers/wire/shareFetchV2')
const shareAckWire = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const heartbeatWire = require('../../../../testHelpers/wire/shareGroupHeartbeatV1')
const heartbeatResponse = require('../../../protocol/requests/shareGroupHeartbeat/v1/response')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareSession } = require('../shareSession')
const { ShareRun } = require('../shareRun')
const fetchAcknowledgementResponse = require('../../../protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require('../../../protocol/requests/shareAcknowledge/v2/response')
const { readRequestHeader } = require('../../../../testHelpers/fakeSocket')
const {
  acknowledgementResults,
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
  resetShareGroupConfigs,
  shareRecordIdentity,
  assertShareCleanup,
  captureShareCleanupBaseline,
} = require('../../../../testHelpers/shareFixtures')
const {
  createResourceProbe,
  createTopic,
  newLogger,
  plainTextBrokers,
  secureRandom,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const defaultSocketFactory = require('../../../network/socketFactory')
const RequestQueue = require('../../../network/requestQueue')
const CONFIG_RESOURCE_TYPES = require('../../../protocol/configResourceTypes')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareHandlerCall } = require('../shareHandlerCall')
const { KafkaJSNonRetriableError } = require('../../../errors')
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
const CHILD_RETURN_MS = Math.max(LOCK_MS, SESSION_MS) + 3000
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

/**
 * Requires configured concurrent timers and proved assignment throttling before joining.
 * @param {object} facts Version and original effective configuration readbacks.
 * @returns {number} The fixed child allowance, never selected from replay latency.
 */
const childAllowance = ({ version, configs, assignment }) => {
  expect(version).toMatch(/^4\.(3\.1|4\.0)(?:$|[-.])/)
  expect(String(configs['share.record.lock.duration.ms'])).toBe(String(LOCK_MS))
  expect(String(configs['share.session.timeout.ms'])).toBe(String(SESSION_MS))
  expect(String(configs['share.heartbeat.interval.ms'])).toBe(String(HEARTBEAT_MS))
  if (version.startsWith('4.4.0')) {
    expect(['group', 'broker']).toContain(assignment?.source)
    expect(assignment.readback.length).toBeGreaterThan(0)
    expect(Number.isInteger(assignment.value)).toBe(true)
    expect(assignment.value).toBeGreaterThanOrEqual(0)
    expect(assignment.value).toBeLessThanOrEqual(1000)
    for (const value of assignment.readback) {
      expect(typeof value).toBe('string')
      expect(value).toMatch(/^(0|[1-9][0-9]*)$/)
      expect(Number(value)).toBe(assignment.value)
    }
  }
  return Math.max(LOCK_MS, SESSION_MS) + 3000
}

/**
 * Computes only the remaining original budget; an exhausted deadline cannot start another poll.
 * @param {number} deadline Original absolute parent monotonic deadline.
 * @param {number} [now=performance.now()] Current parent monotonic clock.
 * @returns {number} Remaining milliseconds.
 */
const remainingRecovery = (deadline, now = performance.now()) => {
  if (!Number.isFinite(deadline) || !Number.isFinite(now) || now >= deadline) {
    throw new Error('The original recovery deadline is exhausted or missing')
  }
  return deadline - now
}

/**
 * Checks an original release-linked or kill-linked replay without consulting later gates/polls.
 * @param {object} facts Original boundary, release, acquisition and replay observations.
 * @returns {number} Signed latency in the parent's original monotonic domain.
 */
const assertRecoveryTiming = ({
  kind,
  boundary,
  releaseAt,
  replayAt,
  count,
  clock,
  acquisition,
  originalAcquisition,
}) => {
  expect(clock).toBe('parent-monotonic')
  for (const value of [boundary, releaseAt, replayAt]) {
    expect(Number.isFinite(value)).toBe(true)
    expect(value).toBeGreaterThanOrEqual(0)
  }
  expect(['child', 'leave']).toContain(kind)
  expect(count).toBeGreaterThanOrEqual(2)
  expect(typeof acquisition).toBe('string')
  expect(typeof originalAcquisition).toBe('string')
  expect(acquisition).not.toBe(originalAcquisition)
  expect(replayAt).toBeGreaterThan(releaseAt)
  if (kind === 'child') {
    expect(releaseAt).toBe(boundary)
    expect(replayAt).toBeGreaterThan(boundary)
  }
  const latency = replayAt - boundary
  expect(latency).toBeLessThanOrEqual(kind === 'child' ? CHILD_RETURN_MS : RELEASE_WITHIN_MS)
  return latency
}

/**
 * Requires the undecided held record's single local loss, without conflating other partitions.
 * @param {object} held Original held handler entry with its full immutable identity.
 * @param {object[]} outcomes The leaving member's public acknowledgement outcomes.
 * @returns {void} Throws for missing, repeated, successful or incorrectly classified settlement.
 */
const assertHeldLeaveOutcome = (held, outcomes) => {
  const [topic, partition, offset] = JSON.parse(held.identity)
  const matching = outcomes.filter(
    (row) =>
      row.topic === topic &&
      row.partition === partition &&
      BigInt(row.firstOffset) <= BigInt(offset) &&
      BigInt(row.lastOffset) >= BigInt(offset)
  )
  expect(matching).toHaveLength(1)
  expect(matching[0]).toMatchObject({
    type: 'release',
    error: {
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'membership-lost',
      retriable: false,
    },
  })
}

/**
 * Validates frozen kill provenance before any replay can satisfy recovery.
 * @param {object} fault Original parent boundary and durable child observations.
 * @returns {void} Throws for replaced children, invented clocks or omitted held starts.
 */
const assertKilledProvenance = (fault) => {
  expect(fault.clock).toBe('parent-monotonic')
  expect(Number.isFinite(fault.killedAt)).toBe(true)
  expect(fault.boundary).toBe(fault.killedAt)
  expect(fault.deadline).toBe(fault.killedAt + CHILD_RETURN_MS)
  expect(fault.ready.pid).toBe(fault.pid)
  expect(fault.ready.nonce).toBe(fault.nonce)
  expect(fault.ready.at).toBeLessThan(fault.killedAt)
  expect(fault.exit.signal).toBe('SIGKILL')
  expect(fault.held.length).toBeGreaterThan(0)
  for (const identity of fault.held) {
    const original = fault.frozenRows.filter(
      (row) => row.event === 'start' && row.identity === identity
    )
    expect(original).toHaveLength(1)
    expect(fault.finalRows).toContainEqual(original[0])
    expect(fault.finalHeld).toContain(identity)
  }
}

/**
 * Requires successful replay settlement from that acquisition, before projecting ranges to sets.
 * @param {object} evidence Original replay, owner acquisition, public result and wire exchange.
 * @returns {void} Throws for a missing, repeated, foreign or failed held-replay receipt.
 */
const assertHeldReplaySettlement = ({
  groupId,
  topicId,
  replay,
  acquisition,
  outcomes,
  frames,
}) => {
  const [topic, partition, offset] = JSON.parse(replay.identity)
  expect(replay.acquisition).toBe(acquisition.id)
  expect(acquisition).toMatchObject({ groupId, topicId, partition, errorCode: 0 })
  expect(acquisition.records).toContainEqual({
    identity: replay.identity,
    deliveryCount: replay.deliveryCount,
  })
  expect(outcomes).toHaveLength(1)
  expect(outcomes[0]).toMatchObject({ topic, partition, type: 'accept', error: null })
  for (const value of [outcomes[0].firstOffset, outcomes[0].lastOffset]) {
    expect(value).toMatch(/^(0|[1-9][0-9]*)$/)
  }
  expect(BigInt(outcomes[0].firstOffset) <= BigInt(offset)).toBe(true)
  expect(BigInt(outcomes[0].lastOffset) >= BigInt(offset)).toBe(true)
  expect(outcomes[0].at).toBeGreaterThanOrEqual(replay.at)
  expect(frames).toHaveLength(1)
  const [frame] = frames
  expect([78, 79]).toContain(frame.apiKey)
  expect(frame).toMatchObject({ groupId, memberId: acquisition.memberId, isRenewAck: false })
  expect(frame.at).toBeGreaterThanOrEqual(acquisition.adoptedAt)
  expect(frame.response?.errorCode).toBe(0)
  const topics = frame.response.topics.filter((row) => row.topicId === topicId)
  expect(topics).toHaveLength(1)
  const replies = topics[0].partitions.filter((row) => row.partitionIndex === partition)
  expect(replies).toHaveLength(1)
  expect(frame.apiKey === 78 ? replies[0].acknowledgeErrorCode : replies[0].errorCode).toBe(0)
  const batches = frame.topics
    .filter((row) => row.topicId === topicId)
    .flatMap((row) => row.partitions)
    .filter((row) => row.partitionIndex === partition)
    .flatMap((row) => row.acknowledgementBatches)
    .filter(
      (batch) =>
        BigInt(batch.firstOffset) <= BigInt(offset) && BigInt(batch.lastOffset) >= BigInt(offset)
    )
  expect(batches).toHaveLength(1)
  const batch = batches[0]
  expect(
    batch.acknowledgeTypes.length === 1
      ? batch.acknowledgeTypes[0]
      : batch.acknowledgeTypes[Number(BigInt(offset) - BigInt(batch.firstOffset))]
  ).toBe(1)
}

/**
 * Requires all original completed-prefix barriers, including the prefix before the last append.
 * @param {object[]} prefixes Original raw-log, expected corpus and terminal position reads.
 * @returns {void} Throws for an omitted, reordered or invalid completed prefix.
 */
const assertCompletedPrefixes = (prefixes) => {
  expect(prefixes.map(({ end }) => end)).toEqual([100, 200, 300, 400, 500, 600])
  for (const { end, raw, expected, broker } of prefixes) {
    expect(raw).toHaveLength(end * PARTITIONS)
    expect([...raw].sort()).toEqual([...expected].sort())
    expect(broker).toHaveLength(PARTITIONS)
    for (const row of broker) {
      expect(row).toMatchObject({
        errorCode: 0,
        lag: '0',
        agree: true,
        logEnd: String(end),
        expectedEnd: String(end),
      })
    }
  }
}

/** Observes successful original acquisitions, preserving each process's own clock origin. */
class AcquisitionTrace {
  /** @param {Function} label Resolves original member id to label. @param {Function} [record] Original-fact sink. */
  constructor(label, record = () => {}) {
    this.label = label
    this.record = record
    this.rows = []
  }

  /** @returns {void} Installs a call-through observer that validates parallel acquisition counts. */
  install() {
    const original = ShareFetchLoop.prototype.adopt
    const trace = this
    ShareFetchLoop.prototype.adopt = function (exchange, topicId, partition, lockDuration) {
      const cohort = original.call(this, exchange, topicId, partition, lockDuration)
      if (cohort.walk.deliver.records.length) {
        const { records, deliveryCounts } = cohort.walk.deliver
        if (!deliveryCounts || deliveryCounts.length !== records.length) {
          throw new Error('Acquisition delivery counts must match records')
        }
        const counts = Array.from(deliveryCounts)
        if (counts.some((count) => !Number.isInteger(count) || count < 1 || count > 32767)) {
          throw new Error('Acquisition delivery counts must be integers in 1–32767')
        }
        const label = trace.label(this.memberId)
        const row = {
          id: `${label}:${trace.rows.length + 1}`,
          label,
          groupId: this.groupId,
          memberId: this.memberId,
          topicId,
          partition: partition.partitionIndex,
          sentAt: exchange.sentAt,
          adoptedAt: performance.now(),
          lockDuration,
          errorCode: partition.errorCode,
          request: JSON.parse(JSON.stringify(exchange.fields)),
          acquiredRecords: partition.acquiredRecords,
          records: records.map((message, index) => ({
            identity: shareRecordIdentity({
              ...message,
              topic: cohort.topic,
              partition: cohort.partition,
            }),
            deliveryCount: counts[index],
          })),
        }
        trace.rows.push(row)
        trace.record(row)
      }
      return cohort
    }
    this.restore = () => {
      ShareFetchLoop.prototype.adopt = original
    }
  }

  /** @param {string} identity Full identity. @param {number} count Original delivery count. @param {string} label Member. @returns {string} Original acquisition id. */
  find(identity, count, label) {
    return this.rows.findLast(
      (row) =>
        row.label === label &&
        row.records.some((record) => record.identity === identity && record.deliveryCount === count)
    )?.id
  }
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
        if (![76, 78, 79].includes(header.apiKey)) continue
        const fact = {
          ...header,
          label,
          socket: ordinal,
          bytes,
          requestHex: bytes.toString('hex'),
          headerBytes: header.byteLength,
          at: performance.now(),
        }
        const reader =
          header.apiKey === 76
            ? heartbeatWire
            : header.apiKey === 78
              ? shareFetchWire
              : shareAckWire
        const request = {
          ...header,
          label,
          socket: ordinal,
          ...reader.readRequest(bytes.subarray(header.byteLength)),
        }
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
        const decoder =
          fact.apiKey === 76
            ? heartbeatResponse
            : fact.apiKey === 78
              ? fetchAcknowledgementResponse
              : acknowledgeResponse
        fact.receivedAt = performance.now()
        fact.responseHex = bytes.toString('hex')
        fact.decoding = Promise.resolve()
          .then(() => {
            if (bytes[8] !== 0) throw new Error('Unexpected response header tags')
            return decoder.decode(bytes.subarray(9))
          })
          .then(({ errorCode, responses = [], ...fields }) => {
            fact.response = {
              errorCode,
              ...fields,
              at: fact.receivedAt,
              topics: responses.map(({ topicId, partitions }) => ({
                topicId,
                partitions: partitions.map(
                  ({
                    partitionIndex,
                    errorCode,
                    acknowledgeErrorCode,
                    currentLeader,
                    acquiredRecords,
                  }) => ({
                    partitionIndex,
                    errorCode,
                    acknowledgeErrorCode,
                    currentLeader,
                    acquiredRecords,
                  })
                ),
              })),
            }
          })
          .then(() =>
            this.record({
              ...fact.request,
              at: fact.at,
              response: fact.response,
              requestHex: fact.requestHex,
              responseHex: fact.responseHex,
            })
          )
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
      if (
        request.apiKey === 76 ||
        request.groupId !== group ||
        request.isRenewAck ||
        response?.errorCode !== 0
      )
        continue
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

/** Observes original handler authority and retains every scheduled renewal failure. */
class RenewalObserver {
  /** @param {Function} [record] Receives failures and validated stale-call refusals. */
  constructor(record = () => {}) {
    this.record = record
    this.calls = new WeakMap()
    this.failures = []
    this.pending = new Set()
    this.stops = new Set()
  }

  /** @returns {void} Installs a call-through observer before handlers start. */
  install() {
    const original = ShareHandlerCall.prototype.messagePayload
    const calls = this.calls
    ShareHandlerCall.prototype.messagePayload = function (...args) {
      const payload = original.apply(this, args)
      calls.set(payload, this)
      return payload
    }
    this.restore = () => {
      ShareHandlerCall.prototype.messagePayload = original
    }
  }

  /**
   * @param {object} payload Original invocation payload.
   * @param {string} identity Held identity, retained with any failure.
   * @returns {Function} Stops the timer and joins its observed renewals.
   */
  start(payload, identity) {
    const call = this.calls.get(payload)
    if (!call) throw new Error('Missing original handler call for renewal')
    const timer = setInterval(() => {
      let revoked = false
      try {
        revoked = !call.open || !call.authority.current()
        const renewal = payload.renew()
        const observed = renewal.catch((error) => this.reject(error, false, revoked, identity))
        this.pending.add(observed)
        observed.then(() => this.pending.delete(observed))
      } catch (error) {
        this.reject(error, true, revoked, identity)
        clearInterval(timer)
      }
    }, RENEW_MS)
    const stop = async () => {
      clearInterval(timer)
      this.stops.delete(stop)
      await this.join()
    }
    this.stops.add(stop)
    return stop
  }

  /**
   * @param {*} error Original thrown value or rejection.
   * @param {boolean} synchronous Whether the renewal call threw before returning.
   * @param {boolean} revoked Whether original authority was already revoked before the call.
   * @param {string} identity Original held identity.
   * @returns {void} Only the exact synchronous post-revocation refusal is tolerated.
   */
  reject(error, synchronous, revoked, identity) {
    const tolerated =
      synchronous &&
      revoked &&
      error instanceof KafkaJSNonRetriableError &&
      error.name === 'KafkaJSNonRetriableError' &&
      error.message === 'Handler call is no longer current'
    if (!tolerated) this.failures.push({ error, identity })
    this.record({ identity, synchronous, revoked, tolerated, error: describeError(error) })
  }

  /** @returns {Promise<void>} Joins all renewals and propagates retained failures. */
  async join() {
    await Promise.all([...this.pending])
    if (this.failures.length) throw this.failures[0].error
  }

  /** @returns {Promise<void>} Stops every schedule and restores the original method. */
  async uninstall() {
    try {
      await Promise.all([...this.stops].map((stop) => stop()))
      await this.join()
    } finally {
      this.restore?.()
    }
  }
}

/**
 * The child member's whole program. It writes each invocation's start to a synchronous journal
 * before it can block or be killed, reports over IPC only after that, and keeps each held
 * invocation's continuation bound to the invocation itself.
 */
const CHILD_SOURCE = `
const { readRequestHeader } = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/fakeSocket')
const { describeError } = require(process.env.SHARE_LIVE_ROOT + '/src/producer/__tests__/fixtures/liveProbe')
const fetchAcknowledgementResponse = require(process.env.SHARE_LIVE_ROOT + '/src/protocol/requests/shareFetch/v2/response')
const acknowledgeResponse = require(process.env.SHARE_LIVE_ROOT + '/src/protocol/requests/shareAcknowledge/v2/response')
const defaultSocketFactory = require(process.env.SHARE_LIVE_ROOT + '/src/network/socketFactory')
const shareFetchWire = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/wire/shareFetchV2')
const shareAckWire = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/wire/shareAcknowledgeV2')
const heartbeatWire = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/wire/shareGroupHeartbeatV1')
const heartbeatResponse = require(process.env.SHARE_LIVE_ROOT + '/src/protocol/requests/shareGroupHeartbeat/v1/response')
const { ShareFetchLoop } = require(process.env.SHARE_LIVE_ROOT + '/src/consumer/share/shareFetchLoop')
const AcknowledgementTrace = ${AcknowledgementTrace.toString()}
const AcquisitionTrace = ${AcquisitionTrace.toString()}
const { Kafka, logLevel } = require(process.env.SHARE_LIVE_ROOT + '/index')
const { KafkaJSNonRetriableError } = require(process.env.SHARE_LIVE_ROOT + '/src/errors')
const { ShareHandlerCall } = require(process.env.SHARE_LIVE_ROOT + '/src/consumer/share/shareHandlerCall')
const RENEW_MS = ${RENEW_MS}
const RenewalObserver = ${RenewalObserver.toString()}
const { ShareHandlerLog, shareRecordIdentity } = require(process.env.SHARE_LIVE_ROOT + '/testHelpers/shareFixtures')
const config = JSON.parse(process.env.SHARE_LIVE_CHILD)
const journal = new ShareHandlerLog(config.journal, config.clientId)
let sequence = 0
const report = (message) => process.send({ ...message, sequence: ++sequence, wallAt: Date.now() })
const acquisitions = new AcquisitionTrace(() => 'C', (row) => report({ event: 'acquisition', row }))
acquisitions.install()
const renewals = new RenewalObserver((fact) => report({ event: 'renewal', ...fact }))
renewals.install()
let hold = null
let current = null
let frozen = null
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
const handle = async (payload) => {
  const { topic, partition, message } = payload
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
  report({ event: 'start', invocation, identity, deliveryCount: message.deliveryCount,
    acquisition: acquisitions.find(identity, message.deliveryCount, 'C') })
  if (hold) {
    const mine = hold
    hold = null
    const stopRenewing = mine.renew ? renewals.start(payload, identity) : null
    try {
      await new Promise((resolve) => {
        current = { invocation, release: resolve }
        report({ event: 'held', invocation, identity, deliveryCount: message.deliveryCount })
      })
    } finally {
      if (stopRenewing) await stopRenewing()
      current = null
    }
  } else if (!frozen) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  if (frozen) await frozen.promise
  journal.finish(invocation)
  report({ event: 'finish', invocation })
}
const stop = async () => {
  try {
    if (frozen) frozen.release()
    if (current) current.release()
    await client.disconnect({ timeout: 3000 })
    await renewals.uninstall()
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
  else if (message.cmd === 'freeze' && !frozen) {
    let release
    const promise = new Promise((resolve) => { release = resolve })
    frozen = { promise, release }
    report({ event: 'frozen' })
  } else if (message.cmd === 'stop') stop()
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
   * @param {RenewalObserver} options.renewals Original-call renewal observer.
   * @param {AcquisitionTrace} options.acquisitions Original successful broker acquisitions.
   */
  constructor({ kafka, label, groupId, workMs = 5, renewals, acquisitions }) {
    this.label = label
    this.groupId = groupId
    this.workMs = workMs
    this.renewals = renewals
    this.acquisitions = acquisitions
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
        this.client.on(events[name], ({ payload }) =>
          list.push({ ...payload, wallAt: Date.now(), at: performance.now() })
        )
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
      at: performance.now(),
      wallAt: Date.now(),
    }
    entry.acquisition = this.acquisitions.find(entry.identity, entry.deliveryCount, this.label)
    this.entries.push(entry)
    this.inflightEntries.add(entry)
    try {
      const { hold } = this
      if (hold) {
        this.hold = null
        this.held = entry
        const stopRenewing = this.renewals.start(payload, entry.identity)
        try {
          await hold.promise
        } finally {
          await stopRenewing()
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
    this.acquisitions = []
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
    const fact = {
      ...message,
      receivedWallAt: Date.now(),
      at: performance.now(),
      clock: 'parent-monotonic',
    }
    this.messages.push(fact)
    if (message.event === 'ready') {
      if (message.nonce !== this.nonce || message.pid !== this.child.pid) {
        this.failure = new Error('The child did not identify itself with its nonce and pid')
      }
    } else if (message.event === 'acquisition') {
      this.acquisitions.push({ ...message.row, receivedAt: fact.at })
    } else if (message.event === 'start') {
      this.starts.push(fact)
      this.unfinished.set(message.invocation, message.identity)
    } else if (message.event === 'finish') {
      this.unfinished.delete(message.invocation)
    } else if (message.event === 'assignment') {
      this.assignments.push(fact)
    } else if (message.event === 'wire') {
      this.wireAcknowledgements.push({ ...message.row, receivedAt: fact.at })
    } else if (message.event === 'acknowledge') {
      this.acks.push(fact)
    } else if (message.event === 'renewal' && !message.tolerated) {
      this.failure = new Error(`The child renewal failed: ${JSON.stringify(message)}`)
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
    this.killedWallAt = Date.now()
    this.killedAt = performance.now()
    expect(this.child.kill('SIGKILL')).toBe(true)
    this.deadline = this.killedAt + CHILD_RETURN_MS
    return bounded(scope, remainingRecovery(this.deadline), () => this.closed)
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
    this.renewals = new RenewalObserver((fact) => this.row('renewal', fact))
    this.acquisitions = new AcquisitionTrace(
      (memberId) =>
        Object.values(this.members).find((member) =>
          member.joins?.some((join) => join.memberId === memberId)
        )?.label,
      (row) => this.row('acquisition', row)
    )
    this.recovery = {}
    this.prefixes = []
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
      acquisitions: this.acquisitions.rows,
      childAcquisitions: this.members.C?.acquisitions ?? [],
      recovery: this.recovery,
      prefixes: this.prefixes,
      childMessages: this.members.C?.messages ?? [],
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
    outer.add('release acquisitions', () => this.acquisitions.restore?.())
    outer.add('release resource gauge', () => this.gauge.uninstall())
    outer.add('release renewal observer', () => this.renewals.uninstall())
    outer.add('disconnect admin', () => this.admin.disconnect())
    let pre
    let base
    let after
    try {
      await outer.run(
        async () => {
          this.gauge.install()
          this.renewals.install()
          this.acquisitions.install()
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
      await this.renewals.join()
      this.assertHealthy()
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
      resetShareGroupConfigs(admin, group, GROUP_KEYS, { signal })
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
    const version = process.env.KAFKA_VERSION
    const names = [
      'share.record.lock.duration.ms',
      'share.session.timeout.ms',
      'share.heartbeat.interval.ms',
    ]
    if (version?.startsWith('4.4.0')) names.push('share.assignment.interval.ms')
    const read = await bounded(scope, STEP_MS, () =>
      admin.describeConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.GROUP,
            name: group,
            configNames: names,
          },
        ],
      })
    )
    expect(read.resources).toHaveLength(1)
    expect(read.resources[0].errorCode).toBe(0)
    for (const name of names.filter((name) => name !== 'share.assignment.interval.ms')) {
      const matches = read.resources[0].configEntries.filter(
        ({ configName }) => configName === name
      )
      expect(matches).toHaveLength(1)
      expect(matches[0].configValue).not.toBeNull()
    }
    const effective = Object.fromEntries(
      read.resources[0].configEntries.map(({ configName, configValue }) => [
        configName,
        configValue,
      ])
    )
    let assignment
    let brokerRead
    if (version?.startsWith('4.4.0')) {
      const groupEntries = read.resources[0].configEntries.filter(
        ({ configName }) => configName === 'share.assignment.interval.ms'
      )
      expect(groupEntries.length).toBeLessThanOrEqual(1)
      const groupValue = effective['share.assignment.interval.ms']
      if (groupValue !== null && groupValue !== undefined) {
        assignment = { source: 'group', value: Number(groupValue), readback: [groupValue] }
      } else {
        const { brokers } = await bounded(scope, STEP_MS, () => admin.describeCluster())
        brokerRead = await bounded(scope, STEP_MS, () =>
          admin.describeConfigs({
            resources: brokers.map(({ nodeId }) => ({
              type: CONFIG_RESOURCE_TYPES.BROKER,
              name: String(nodeId),
              configNames: ['group.share.assignment.interval.ms'],
            })),
          })
        )
        expect(brokerRead.resources).toHaveLength(brokers.length)
        const values = brokerRead.resources.map((resource) => {
          expect(resource.errorCode).toBe(0)
          const entries = resource.configEntries.filter(
            ({ configName }) => configName === 'group.share.assignment.interval.ms'
          )
          expect(entries).toHaveLength(1)
          expect(entries[0].configValue).not.toBeNull()
          return entries[0].configValue
        })
        assignment = { source: 'broker', value: Number(values[0]), readback: values }
      }
    }
    this.config = { ...configs, ...effective }
    this.timerFacts = { version, configs: this.config, assignment }
    expect(childAllowance(this.timerFacts)).toBe(CHILD_RETURN_MS)
    this.row('config-readback', {
      configs: this.config,
      timerFacts: this.timerFacts,
      groupReadback: read,
      brokerReadback: brokerRead,
    })
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
    const member = new Member({
      kafka,
      label,
      groupId: this.group,
      workMs,
      renewals: this.renewals,
      acquisitions: this.acquisitions,
    })
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
        this.prefixes.push({ end, expected: [...this.expected], raw, broker: position.rows })
        this.row('prefix-complete', {
          end,
          expected: this.expected,
          raw,
          broker: position.rows,
          ...this.progress(),
        })
      },
      { label: `Every partition settling through ${end}` }
    )
  }

  /**
   * @returns {void}
   * @throws {Error} a crash of an in-process member or any failure of the child
   */
  assertHealthy() {
    expect(this.renewals.failures).toEqual([])
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
   * @param {number} [deadline] Original absolute recovery deadline, when fault-bound.
   * @returns {Promise<object>} per member: broker member id, epoch and normalized assignment
   * @throws {Error} sources that never agree
   */
  async converge(scope, labels, stage, deadline) {
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
      deadline === undefined ? STAGE_MS : remainingRecovery(deadline),
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
          last.description = await bounded(
            scope,
            deadline === undefined ? STEP_MS : remainingRecovery(deadline),
            () => this.admin.describeShareGroups([this.group])
          )
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
    const originalHeld = { ...b.held }
    this.assertReplayAcquisition(originalHeld)
    expect(b.inflightEntries.has(b.held)).toBe(true)
    const childInvocation = c.of('held').at(-1).invocation
    const leaveCalledWallAt = Date.now()
    const leaveCalledAt = performance.now()
    c.command({ cmd: 'release' })
    const gateReleasedAt = performance.now()
    await bounded(scope, CLEAN_LEAVE_MS, () => b.client.disconnect({ timeout: CLEAN_LEAVE_MS }))
    const leftAt = performance.now()
    const leftWallAt = Date.now()
    this.recovery.leave = {
      kind: 'leave',
      clock: 'parent-monotonic',
      leaveCalledAt,
      leftAt,
      boundary: leftAt,
      releaseAt: leaveCalledAt,
      deadline: leftAt + RELEASE_WITHIN_MS,
      gateReleasedAt,
      leaveCalledWallAt,
      leftWallAt,
      originalHeld,
      heldAtLeft: b.inflight(),
      childInvocation,
    }
    expect(this.recovery.leave.heldAtLeft).toContain(originalHeld.identity)
    expect(b.inflightEntries.has(b.held)).toBe(true)
    this.row('acknowledgement', {
      name: 'cleanLeaveChronology',
      leaveCalledAt,
      leftAt,
      leaveCalledWallAt,
      leftWallAt,
      gateReleasedAt,
      originalHeld,
      heldAtLeft: this.recovery.leave.heldAtLeft,
      wire: this.sockets.acknowledgements.snapshot().filter(({ label }) => label === 'B'),
      acknowledgements: b.acks
        .filter(({ at }) => at >= leaveCalledAt - 100)
        .map(({ topic, partition, type, firstOffset, lastOffset, error, at }) => ({
          topic,
          partition,
          type,
          firstOffset,
          lastOffset,
          at,
          error: describeError(error),
        })),
    })
    await this.sockets.acknowledgements.join()
    const leaveFrames = this.sockets.acknowledgements
      .snapshot()
      .filter(({ label, at }) => label === 'B' && at >= leaveCalledAt)
    const finals = leaveFrames.filter(
      ({ apiKey, shareSessionEpoch }) => apiKey === 79 && shareSessionEpoch === -1
    )
    expect(finals.length).toBeGreaterThan(0)
    for (const frame of finals) {
      expect(frame.response?.errorCode).toBe(0)
      expect(frame.at).toBeLessThanOrEqual(leaveCalledAt + CLEAN_LEAVE_MS)
    }
    const leaves = leaveFrames.filter(
      ({ apiKey, memberEpoch }) => apiKey === 76 && memberEpoch === -1
    )
    expect(leaves).toHaveLength(1)
    expect(leaves[0].response?.errorCode).toBe(0)
    for (const frame of finals) expect(frame.at).toBeLessThan(leaves[0].at)
    for (const frame of leaveFrames)
      expect(frame.at).toBeLessThanOrEqual(leaveCalledAt + CLEAN_LEAVE_MS)
    expect(b.inflightEntries.has(b.held)).toBe(true)
    assertHeldLeaveOutcome(originalHeld, b.acks)
    this.recovery.leave.wire = leaveFrames
    this.recovery.leave.releaseAt = Math.min(...finals.map(({ at }) => at))
    bGate.open()
    this.recovery.leave.bGateReleasedAt = performance.now()
    await this.awaitReleased(scope, this.frozen.cleanHandoverHeld, this.recovery.leave)
    await this.converge(scope, ['A', 'C'], 'B left', this.recovery.leave.deadline)
    await c.waitFor(
      scope,
      () => c.of('finish').some(({ invocation }) => invocation === childInvocation),
      'The child finishing its phase2 invocation',
      remainingRecovery(this.recovery.leave.deadline)
    )
    remainingRecovery(this.recovery.leave.deadline)
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
    await this.converge(scope, ['A'], 'C dead', fault.deadline)
    await this.awaitReturned(scope, a, fault)
    await this.settled(scope, 4 * PER_PARTITION)

    await this.produce(scope, 4)
    await this.settled(scope, 5 * PER_PARTITION)
    await this.produce(scope, 5)
    await this.settled(scope, STAGES * PER_PARTITION)
  }

  /**
   * Requires release-linked replay within the original leave-completion window.
   * @param {AbortSignal} scope Case scope.
   * @param {string[]} identities Frozen original held set.
   * @param {object} fault Original leave chronology.
   * @returns {Promise<void>} Once every original replay has entered a surviving handler.
   */
  async awaitReleased(scope, identities, fault) {
    const delivered = () => [...this.members.A.entries, ...this.members.C.starts]
    const back = (identity) =>
      delivered().find(
        (entry) =>
          entry.identity === identity && entry.deliveryCount >= 2 && entry.at > fault.releaseAt
      )
    await pollUntil(
      scope,
      remainingRecovery(fault.deadline),
      () => {
        this.assertHealthy()
        return identities.every((identity) => back(identity))
      },
      { delayMs: 25, label: 'The records B held being delivered again' }
    )
    fault.replays = identities.map((identity) => {
      const replay = back(identity)
      const facts = {
        ...fault,
        replayAt: replay.at,
        count: replay.deliveryCount,
        acquisition: replay.acquisition,
        originalAcquisition: fault.originalHeld.acquisition,
      }
      const latencyMs = assertRecoveryTiming(facts)
      this.assertReplayAcquisition(replay)
      return { identity, ...facts, latencyMs }
    })
    this.row('held-set', { name: 'cleanLeaveRelease', identities, ...fault })
  }

  /** @param {object} replay Original handler entry. @returns {void} Requires its own matching broker acquisition. */
  assertReplayAcquisition(replay) {
    const all = [...this.acquisitions.rows, ...this.members.C.acquisitions]
    const matching = all.filter(({ id }) => id === replay.acquisition)
    expect(matching).toHaveLength(1)
    expect(matching[0]).toMatchObject({ groupId: this.group, topicId: this.topicId, errorCode: 0 })
    expect(matching[0].records).toContainEqual({
      identity: replay.identity,
      deliveryCount: replay.deliveryCount,
    })
  }

  /**
   * Blocks every child handler's completion before snapshotting its durable journal once it
   * agrees with the child's reports. Kills that exact process once, then reads the journal again:
   * what a dead child held is what its journal says, and it must include what was frozen.
   * @param {AbortSignal} scope
   * @param {Member} a the survivor
   * @param {ChildMember} c the child
   * @returns {Promise<{killedAt: number, held: string[]}>} the kill time and the identities the
   *   journal says the child held when it died
   * @throws {Error} a journal that never agrees with the child's reports, or a child that did not
   *   die by SIGKILL
   */
  async killChild(scope, a, c) {
    c.command({ cmd: 'freeze' })
    await c.waitFor(scope, () => c.of('frozen').length === 1, 'The child freezing its handler work')
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
    const outcome = await c.kill(scope)
    const killedAt = c.killedAt
    expect(outcome.signal).toBe('SIGKILL')
    const final = ShareHandlerLog.read(c.journal)
    for (const identity of frozen.held) expect(final.held).toContain(identity)
    this.frozen.killedHeld = [...frozen.held]
    this.recovery.child = {
      kind: 'child',
      clock: 'parent-monotonic',
      boundary: killedAt,
      killedAt,
      releaseAt: killedAt,
      killedWallAt: c.killedWallAt,
      deadline: c.deadline,
      held: [...frozen.held],
      pid: c.child.pid,
      nonce: this.nonce,
      ready,
      frozenRows: frozen.rows,
      finalRows: final.rows,
      finalHeld: final.held,
      closedAt: performance.now(),
      exit: outcome,
      drained: { ...c.output },
    }
    assertKilledProvenance(this.recovery.child)
    this.row('held-set', {
      name: 'killedHeld',
      frozenBeforeKill: frozen.held,
      identities: final.held,
      otherHeldAtRecordedHandover: this.frozen.otherAtKill,
      journalRows: final.rows.length,
    })
    this.row('fault', { pid: c.child.pid, nonce: this.nonce, signal: outcome.signal, killedAt })
    return this.recovery.child
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
  async awaitReturned(scope, a, fault) {
    const { killedAt, held } = fault
    const back = (identity) =>
      a.entries.find((entry) => entry.identity === identity && entry.at > killedAt)
    await pollUntil(
      scope,
      remainingRecovery(fault.deadline),
      () => {
        this.assertHealthy()
        return held.every((identity) => back(identity))
      },
      { delayMs: 25, label: 'The identities the dead child held returning to A' }
    )
    fault.replays = held.map((identity) => {
      const replay = back(identity)
      const original = this.members.C.starts.findLast(
        (entry) => entry.identity === identity && entry.at < killedAt
      )
      expect(original).toBeDefined()
      this.assertReplayAcquisition(original)
      const facts = {
        kind: 'child',
        boundary: killedAt,
        releaseAt: killedAt,
        replayAt: replay.at,
        count: replay.deliveryCount,
        clock: 'parent-monotonic',
        acquisition: replay.acquisition,
        originalAcquisition: original.acquisition,
      }
      const latencyMs = assertRecoveryTiming(facts)
      this.assertReplayAcquisition(replay)
      return { identity, ...facts, latencyMs }
    })
    remainingRecovery(fault.deadline)
    this.row('held-set', { name: 'childReplay', ...fault })
  }

  /** @returns {void} Checks every frozen required replay's own successful terminal receipt. */
  checkHeldReplaySettlements() {
    const acquisitions = [...this.acquisitions.rows, ...this.members.C.acquisitions]
    for (const fault of Object.values(this.recovery)) {
      fault.settlements = fault.replays.map((facts) => {
        const member = [this.members.A, this.members.C].find((owner) =>
          (owner.entries ?? owner.starts).some((entry) => entry.acquisition === facts.acquisition)
        )
        const replay = (member.entries ?? member.starts).find(
          (entry) => entry.identity === facts.identity && entry.acquisition === facts.acquisition
        )
        const acquisition = acquisitions.find(({ id }) => id === replay.acquisition)
        const [, partition, offset] = JSON.parse(replay.identity)
        const outcomes = member.acks
          .filter(
            (row) =>
              row.topic === this.topic &&
              row.partition === partition &&
              row.type === 'accept' &&
              row.at >= replay.at &&
              BigInt(row.firstOffset) <= BigInt(offset) &&
              BigInt(row.lastOffset) >= BigInt(offset)
          )
          .map(({ error, ...row }) => ({ ...row, error: describeError(error) }))
        const wire =
          member instanceof ChildMember
            ? member.wireAcknowledgements
            : this.sockets.acknowledgements.snapshot()
        const frames = wire.filter(
          (frame) =>
            frame.memberId === acquisition.memberId &&
            !frame.isRenewAck &&
            frame.at >= acquisition.adoptedAt &&
            (frame.topics ?? []).some(
              (row) =>
                row.topicId === this.topicId &&
                row.partitions.some(
                  (row) =>
                    row.partitionIndex === partition &&
                    row.acknowledgementBatches.some(
                      (batch) =>
                        BigInt(batch.firstOffset) <= BigInt(offset) &&
                        BigInt(batch.lastOffset) >= BigInt(offset)
                    )
                )
            )
        )
        const evidence = {
          groupId: this.group,
          topicId: this.topicId,
          replay,
          acquisition,
          outcomes,
          frames,
        }
        assertHeldReplaySettlement(JSON.parse(JSON.stringify(evidence)))
        return evidence
      })
    }
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
    this.checkHeldReplaySettlements()
    assertCompletedPrefixes(JSON.parse(JSON.stringify(this.prefixes)))
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
        timers: this.timerFacts,
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

describe('membership acquisition observation', () => {
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
    observer = new AcquisitionTrace(() => 'A')
  })

  afterEach(() => {
    observer.restore?.()
    jest.restoreAllMocks()
    node.session.retire()
    run.manager.shutdown()
    run.poll.stop()
    run.unrouted()
  })

  it('copies parallel counts before dispatch attaches them to the original records', async () => {
    observer.install()
    expect(partition.messages.every((message) => !Object.hasOwn(message, 'deliveryCount'))).toBe(
      true
    )
    const cohort = node.loop.adopt(exchange, topicId, partition, LOCK_MS)
    expect(partition.messages.every((message) => !Object.hasOwn(message, 'deliveryCount'))).toBe(
      true
    )
    expect(observer.rows).toHaveLength(1)
    const [acquisition] = observer.rows
    expect(acquisition.records).toEqual([
      { identity: '["records",0,"0",null,null]', deliveryCount: 1 },
      { identity: '["records",0,"1",null,null]', deliveryCount: 32767 },
    ])
    expect(observer.find('["records",0,"0",null,null]', 1, 'A')).toBe(acquisition.id)
    expect(observer.find('["records",0,"1",null,null]', 32767, 'A')).toBe(acquisition.id)
    await node.loop.dispatchCohorts([cohort])
    expect(handled).toEqual([1, 32767])
    cohort.walk.deliver.deliveryCounts.fill(2)
    partition.messages.forEach((message) => (message.deliveryCount = 3))
    expect(JSON.parse(JSON.stringify(acquisition)).records).toEqual([
      { identity: '["records",0,"0",null,null]', deliveryCount: 1 },
      { identity: '["records",0,"1",null,null]', deliveryCount: 32767 },
    ])
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
    observer.install()
    expect(() => node.loop.adopt(exchange, topicId, partition, LOCK_MS)).toThrow(
      'Acquisition delivery counts'
    )
    expect(observer.rows).toEqual([])
  })
})

describe('membership renewal observation', () => {
  afterEach(() => jest.useRealTimers())

  it.each([
    ['synchronous stale refusal after revocation', true, true, 'stale', true],
    ['synchronous stale refusal before revocation', false, true, 'stale', false],
    ['asynchronous stale refusal after revocation', true, false, 'stale', false],
    ['asynchronous stale refusal before revocation', false, false, 'stale', false],
    ['synchronous TypeError after revocation', true, true, 'type', false],
    ['synchronous TypeError before revocation', false, true, 'type', false],
    ['asynchronous TypeError after revocation', true, false, 'type', false],
    ['asynchronous TypeError before revocation', false, false, 'type', false],
    ['another nonretriable refusal after revocation', true, true, 'other', false],
    ['a same-message untyped refusal after revocation', true, true, 'untyped', false],
  ])('checks %s', async (_name, revoked, synchronous, kind, tolerated) => {
    jest.useFakeTimers()
    const facts = []
    const observer = new RenewalObserver((fact) => facts.push(fact))
    observer.install()
    const stale = 'Handler call is no longer current'
    const error =
      kind === 'type'
        ? new TypeError('Unexpected renewal failure')
        : kind === 'untyped'
          ? new Error(stale)
          : new KafkaJSNonRetriableError(kind === 'stale' ? stale : 'Another refusal')
    try {
      const call = new ShareHandlerCall({
        records: [{ offset: '0' }],
        ledger: {},
        authority: { current: () => !revoked },
      })
      const payload = call.messagePayload('records', 0)
      payload.renew = () => {
        if (synchronous) throw error
        return Promise.reject(error)
      }
      const stop = observer.start(payload, 'held-identity')
      jest.advanceTimersByTime(RENEW_MS)
      if (tolerated) await expect(stop()).resolves.toBeUndefined()
      else await expect(stop()).rejects.toBe(error)
      expect(facts).toEqual([
        {
          identity: 'held-identity',
          synchronous,
          revoked,
          tolerated,
          error: describeError(error),
        },
      ])
      expect(observer.failures).toEqual(tolerated ? [] : [{ error, identity: 'held-identity' }])
    } finally {
      if (observer.failures.length) await expect(observer.uninstall()).rejects.toBe(error)
      else await observer.uninstall()
    }
  })

  it('retains a child renewal failure for the parent health check', () => {
    const child = new ChildMember({ label: 'C' })
    child.receive({
      event: 'renewal',
      synchronous: false,
      revoked: true,
      tolerated: false,
      error: { name: 'TypeError', message: 'Unexpected renewal failure' },
    })
    expect(child.failure.message).toContain('Unexpected renewal failure')
    expect(child.of('renewal')).toHaveLength(1)
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

describe('scripted membership clean-leave outcomes', () => {
  const held = { identity: '["records",0,"200",null,null]' }
  const lost = {
    topic: 'records',
    partition: 0,
    firstOffset: '200',
    lastOffset: '200',
    type: 'release',
    error: {
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'membership-lost',
      retriable: false,
    },
  }
  const accepted = { ...lost, type: 'accept', error: null }

  it('keeps same-offset Accepts on other partitions or topics separate from the held loss', () => {
    assertHeldLeaveOutcome(held, [
      lost,
      { ...accepted, partition: 1 },
      { ...accepted, topic: 'other' },
    ])
  })

  it('rejects an Accept covering the original held topic and partition', () => {
    expect(() => assertHeldLeaveOutcome(held, [accepted])).toThrow()
    expect(() =>
      assertHeldLeaveOutcome(held, [{ ...accepted, firstOffset: '199', lastOffset: '201' }])
    ).toThrow()
    expect(() => assertHeldLeaveOutcome(held, [lost, accepted])).toThrow()
  })

  it('requires one failed membership release for the original held identity', () => {
    for (const outcomes of [
      [],
      [lost, lost],
      [{ ...lost, partition: 1 }],
      [{ ...lost, topic: 'other' }],
      [{ ...lost, firstOffset: '201', lastOffset: '201' }],
      [{ ...lost, error: null }],
      [{ ...lost, error: { ...lost.error, reason: 'expired' } }],
    ]) {
      expect(() => assertHeldLeaveOutcome(held, outcomes)).toThrow()
    }
  })
})

describe('scripted membership absolute fault clocks', () => {
  /** @returns {object} Fixed readback facts for both concurrent timers. */
  const timers = () => ({
    version: '4.4.0',
    configs: {
      'share.record.lock.duration.ms': '3000',
      'share.session.timeout.ms': '6000',
      'share.heartbeat.interval.ms': '1000',
    },
    assignment: { source: 'broker', value: 1000, readback: ['1000'] },
  })
  /** @param {string} kind Fault kind. @param {number} replayAt Parent handler-entry timestamp. @returns {object} Labelled original observations. */
  const timing = (kind, replayAt) => ({
    source: 'scripted',
    kind,
    boundary: 10000,
    releaseAt: kind === 'leave' ? 9500 : 10000,
    replayAt,
    count: 2,
    clock: 'parent-monotonic',
    acquisition: 'A:2',
    originalAcquisition: kind === 'leave' ? 'B:1' : 'C:1',
  })

  it.each([
    [19000, true],
    [19001, false],
    [22000, false],
    [18000, true],
  ])('checks child replay at %i against the original K', (at, accepted) => {
    const input = JSON.parse(JSON.stringify(timing('child', at)))
    const check = () => assertRecoveryTiming(input)
    if (accepted) expect(check()).toBe(at - 10000)
    else expect(check).toThrow()
  })

  it.each([
    [12000, true],
    [12001, false],
    [9900, true],
    [9400, false],
  ])('checks release-linked replay at %i against original Lft', (at, accepted) => {
    const input = JSON.parse(JSON.stringify(timing('leave', at)))
    const check = () => assertRecoveryTiming(input)
    if (accepted) expect(check()).toBe(at - 10000)
    else expect(check).toThrow()
  })

  it('uses concurrent timers and requires original effective assignment readback', () => {
    expect(childAllowance(JSON.parse(JSON.stringify(timers())))).toBe(9000)
    expect(childAllowance({ ...timers(), version: '4.3.1', assignment: undefined })).toBe(9000)
    for (const mutate of [
      (row) => {
        delete row.configs['share.record.lock.duration.ms']
      },
      (row) => {
        row.configs['share.session.timeout.ms'] = '9000'
      },
      (row) => {
        row.configs['share.heartbeat.interval.ms'] = '2000'
      },
      (row) => {
        delete row.assignment
      },
      (row) => {
        row.assignment.value = 1001
        row.assignment.readback = ['1001']
      },
      (row) => {
        row.assignment.readback = ['1000', '2000']
      },
      (row) => {
        row.assignment.readback = []
      },
    ]) {
      const input = JSON.parse(JSON.stringify(timers()))
      mutate(input)
      expect(() => childAllowance(input)).toThrow()
    }
  })

  it('keeps the original deadline when gate, convergence, exit or polling move later', () => {
    const clock = { now: 18000 }
    const input = JSON.parse(JSON.stringify(timing('child', clock.now)))
    expect(remainingRecovery(19000, clock.now)).toBe(1000)
    for (const later of [18000, 19001, 22000]) {
      clock.now = later
      input.exitAt = later
      input.adminConvergedAt = later
      input.pollInvokedAt = later
      expect(assertRecoveryTiming(input)).toBe(8000)
    }
    expect(() => remainingRecovery(19000, clock.now)).toThrow()
    expect(() => remainingRecovery(19000, 19000)).toThrow()
    const leave = JSON.parse(JSON.stringify(timing('leave', 12001)))
    leave.gateReleasedAt = 12000
    leave.adminConvergedAt = 13000
    expect(() => assertRecoveryTiming(leave)).toThrow()
    expect(leave.boundary).toBe(10000)
  })

  it.each([
    [
      'missing boundary',
      (row) => {
        delete row.boundary
      },
    ],
    [
      'missing original release',
      (row) => {
        delete row.releaseAt
      },
    ],
    [
      'cross-process clock',
      (row) => {
        row.clock = 'child-monotonic'
      },
    ],
    [
      'non-distinct acquisition',
      (row) => {
        row.acquisition = row.originalAcquisition
      },
    ],
    [
      'count1 replay',
      (row) => {
        row.count = 1
      },
    ],
  ])('rejects %s', (_label, mutate) => {
    const input = JSON.parse(JSON.stringify(timing('child', 18000)))
    mutate(input)
    expect(() => assertRecoveryTiming(input)).toThrow()
  })

  it('requires the retained child and frozen fsynced held identities in the final journal', () => {
    const input = {
      source: 'scripted',
      clock: 'parent-monotonic',
      killedAt: 10000,
      boundary: 10000,
      deadline: 19000,
      pid: 12,
      nonce: 'original',
      ready: { pid: 12, nonce: 'original', at: 100 },
      exit: { signal: 'SIGKILL' },
      held: ['held'],
      finalHeld: ['held'],
      frozenRows: [{ event: 'start', invocation: 1, identity: 'held' }],
      finalRows: [{ event: 'start', invocation: 1, identity: 'held' }],
    }
    assertKilledProvenance(JSON.parse(JSON.stringify(input)))
    for (const mutate of [
      (row) => {
        row.pid = 13
      },
      (row) => {
        row.nonce = 'replacement'
      },
      (row) => {
        row.frozenRows = []
      },
      (row) => {
        row.finalRows = []
      },
      (row) => {
        row.finalHeld = []
      },
      (row) => {
        row.killedAt = undefined
      },
      (row) => {
        row.boundary = 18000
      },
    ]) {
      const changed = JSON.parse(JSON.stringify(input))
      mutate(changed)
      expect(() => assertKilledProvenance(changed)).toThrow()
    }
  })
})

describe('scripted membership held replay receipts', () => {
  /** @returns {object} Complete labelled original acquisition and settlement facts. */
  const receipt = () => {
    const identity = JSON.stringify(['scripted-topic', 0, '0', '61', '62'])
    return {
      source: 'scripted',
      groupId: 'group',
      topicId: 'uuid',
      replay: { identity, deliveryCount: 2, acquisition: 'A:2', at: 100 },
      acquisition: {
        id: 'A:2',
        groupId: 'group',
        topicId: 'uuid',
        partition: 0,
        errorCode: 0,
        memberId: 'member',
        adoptedAt: 90,
        records: [{ identity, deliveryCount: 2 }],
      },
      outcomes: [
        {
          topic: 'scripted-topic',
          partition: 0,
          firstOffset: '0',
          lastOffset: '0',
          type: 'accept',
          error: null,
          at: 150,
        },
      ],
      frames: [
        {
          apiKey: 79,
          groupId: 'group',
          memberId: 'member',
          isRenewAck: false,
          at: 110,
          topics: [
            {
              topicId: 'uuid',
              partitions: [
                {
                  partitionIndex: 0,
                  acknowledgementBatches: [
                    { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
                  ],
                },
              ],
            },
          ],
          response: {
            errorCode: 0,
            topics: [{ topicId: 'uuid', partitions: [{ partitionIndex: 0, errorCode: 0 }] }],
          },
        },
      ],
    }
  }

  it('requires the held replay itself to settle successfully', () => {
    assertHeldReplaySettlement(JSON.parse(JSON.stringify(receipt())))
    for (const mutate of [
      (row) => {
        row.outcomes = []
      },
      (row) => {
        row.outcomes[0].error = { reason: 'rejected-by-broker' }
      },
      (row) => {
        row.outcomes.push(row.outcomes[0])
      },
      (row) => {
        row.frames = []
      },
      (row) => {
        row.frames[0].memberId = 'foreign'
      },
      (row) => {
        row.frames[0].response.topics[0].partitions[0].errorCode = 121
      },
      (row) => {
        row.frames[0].at = 89
      },
    ]) {
      const input = JSON.parse(JSON.stringify(receipt()))
      mutate(input)
      expect(() => assertHeldReplaySettlement(input)).toThrow()
    }
  })

  it('rejects the omitted500 checkpoint even if final lag is zero', () => {
    const prefixes = Array.from({ length: 6 }, (_, index) => {
      const end = 100 * (index + 1)
      const raw = Array.from({ length: end * PARTITIONS }, (_, row) => String(row))
      return {
        end,
        raw,
        expected: [...raw],
        broker: Array.from({ length: PARTITIONS }, (_, partition) => ({
          partition,
          errorCode: 0,
          lag: '0',
          agree: true,
          logEnd: String(end),
          expectedEnd: String(end),
        })),
      }
    })
    assertCompletedPrefixes(JSON.parse(JSON.stringify(prefixes)))
    expect(() =>
      assertCompletedPrefixes(JSON.parse(JSON.stringify(prefixes.filter(({ end }) => end !== 500))))
    ).toThrow()
  })
})
