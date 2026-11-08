const net = require('net')
const { execFile } = require('child_process')
const { deflateRawSync, inflateRawSync } = require('zlib')
const { createHash } = require('crypto')
const { readFileSync, statSync, truncateSync } = require('fs')
const { readFile } = require('fs/promises')
const { isDeepStrictEqual, promisify } = require('util')
const Decoder = require('../../../protocol/decoder')
const Encoder = require('../../../protocol/encoder')
const ProducerStateManager = require('../../../producer/state')
const PartitionSequencer = require('../../../producer/state/partitionSequencer')
const ProducerIdAndEpoch = require('../../../producer/state/producerIdAndEpoch')
const { EventEmitter } = require('events')
const { AsyncLocalStorage } = require('async_hooks')
const Broker = require('../../../broker')
const RequestQueue = require('../../../network/requestQueue')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareRun } = require('../shareRun')
const { KafkaJSShareAcknowledgementError } = require('../../../errors')
const fetchWire = require('../../../../testHelpers/wire/shareFetchV2')
const ackWire = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const { Kafka, logLevel, ConfigResourceTypes } = require('../../../../index')
const {
  createTopic,
  createConnectionPool,
  evidencePath,
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
  setShareGroupConfigs,
  resetShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
} = require('../../../../testHelpers/shareFixtures')
const { BrokerBouncer, NODES, waitForFullIsr } = require('../../../../testHelpers/leaderElection')
const { readLog } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const {
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  terminalSharePosition,
  acknowledgementResults,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  CASE_JOIN_MS,
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

// Gated: it needs the three-node share-enabled base cluster (docker-compose.yml, broker share
// session 45000 ms and heartbeat 5000 ms) and runs through the share-base lane or the guarded live
// entry with SHARE_BOUNCE_E2E=1.
const enabled = process.env.SHARE_BOUNCE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-bounce'
const PROFILE = 'tv2'
const TITLE = 'preserves share records across clean and hard rolling restarts'
const FILE_MS = 540000
const BODY_MS = 500000
const HOOK_MS = 10000
const CLEANUP_MS = 18000
const FINAL_MS = 10000
const BUDGET_MS = FILE_MS - HOOK_MS - CASE_JOIN_MS
const PARTITIONS = 3
const RECORDS = 30000
const BURST_SIZE = 100
const BURSTS = RECORDS / BURST_SIZE
const BURST_INTERVAL_MS = 400
const PRODUCER_MS = 120000
const LATE_BURST_MS = 1000
const WARMUP_APPENDS = 2000
const MAX_OUTAGE_MS = 45000
const MAX_HELD = 300
const BOUNCE_COUNT = 6
const MAX_REPLAY = BOUNCE_COUNT * MAX_HELD
const MEMBERSHIP_QUIET_MS = 3000
/**
 * One wall-clock timestamp taken when the run starts. Brokers expire idempotent producer state by
 * record timestamp (producer.id.expiration.ms, 24 h by default), so a fixed old timestamp would let
 * a restarted leader drop the producer and accept a duplicate retry.
 */
const RUN_TIMESTAMP = String(Date.now())
const SERVICES = ['kafka1', 'kafka2', 'kafka3']
const SHARE_STATE_TOPIC = '__share_group_state'
const OFFSETS_TOPIC = '__consumer_offsets'
const SHARE_APIS = new Set([76, 78, 79])
const WIRE_BYTES = 96 * 1024 * 1024
const PRODUCER_EVIDENCE_BYTES = 64 * 1024 * 1024
const ARCHIVE_BYTES = 10 * 1024 * 1024
const ARCHIVE_ROW_BYTES = 256 * 1024
const RAW_PRODUCE_BYTES = 2 * 1024 * 1024
const RAW_PRODUCE_PER_PARTITION = 32
const PRODUCER_CONFIGS = [
  'producer.id.expiration.ms',
  'producer.id.expiration.check.interval.ms',
  'transactional.id.expiration.ms',
  'log.producer.snapshot.interval.ms',
  'unclean.leader.election.enable',
  'min.insync.replicas',
]
const SESSION_TIMEOUT = { name: 'group.share.session.timeout.ms', value: '45000' }
const HEARTBEAT_INTERVAL = { name: 'group.share.heartbeat.interval.ms', value: '5000' }
const GROUP_CONFIG = {
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': 30000,
}
const CLIENT_CONFIG = {
  acknowledgementMode: 'implicit',
  acquireMode: 'record_limit',
  maxRecords: 100,
  maxWaitTimeInMs: 250,
}
const RETRY = { retries: 40, initialRetryTime: 200, maxRetryTime: 2000 }

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
 * Wraps the plaintext socket factory of every client of one case. It counts sockets and unanswered
 * requests from the bytes actually written and read, and keeps the share frames each socket sent.
 * Bytes, order, return values and callbacks of every write are untouched.
 */
class ShareWireTap {
  /**
   * @param {Function} [connect] opens one socket; replaced by a fake in the offline checks
   */
  constructor(connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect)) {
    this.connect = connect
    this.sockets = []
    this.frames = []
    this.allFrames = []
    this.context = new AsyncLocalStorage()
    this.socketEvents = []
    this.order = 0
    this.byteCount = 0
    this.problems = []
    this.rawProduce = new Map()
    this.recentProduce = new Map()
    this.rawProduceBytes = 0
    this.pinnedProduce = new Set()
    this.fetchedStamps = new Map()
  }

  /** @returns {object} A case-monotonic timestamp and a total observation order. */
  stamp() {
    return { at: performance.now(), order: this.order++ }
  }

  /**
   * @param {string} label the client the sockets belong to
   * @returns {Function} a Kafka `socketFactory`
   */
  factory(label) {
    return (options) => {
      const socket = this.connect(options)
      socket.setKeepAlive?.(true, 60000)
      return this.wrap(label, socket, options)
    }
  }

  /**
   * @param {string} label the owning client
   * @param {object} socket the real socket
   * @param {object} [options] destination passed to the original socket factory
   * @returns {object} the same socket, observed
   */
  wrap(label, socket, options = {}) {
    const entry = {
      label,
      socket,
      opened: this.stamp(),
      endpoint: `${options.host}:${options.port}`,
      writer: new FrameSplitter(),
      reader: new FrameSplitter(),
      outstanding: new Map(),
    }
    this.sockets.push(entry)
    for (const event of ['end', 'close', 'error'])
      socket.on(event, (error) => {
        const fact = {
          id: this.socketEvents.length,
          socket: this.sockets.indexOf(entry),
          event,
          ...this.stamp(),
          label: entry.label,
          endpoint: entry.endpoint,
          outstanding: [...entry.outstanding.values()].map(({ id }) => id),
          error: error instanceof Error ? describeError(error) : null,
        }
        if (this.socketEvents.length < 100000) this.socketEvents.push(fact)
        else this.problem('Socket event archive exceeded 100000 events')
        for (const frame of entry.outstanding.values()) {
          frame.lostAt ??= fact.at
          frame.lossOrder ??= fact.order
          frame.lossEvent ??= fact.id
        }
        this.onInterruption?.('socket', fact)
      })
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      const bytes = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
      const frames = entry.writer.push(bytes)
      const written = this.stamp()
      const result = write(data, ...rest)
      frames.forEach((frame) => this.record(entry, frame, written))
      return result
    }
    socket.on('data', (chunk) => {
      entry.reader.push(chunk).forEach((bytes) => {
        const request = entry.outstanding.get(bytes.readInt32BE(4))
        if (!request) return
        this.response(request, bytes)
        const stamp = this.stamp()
        request.responseAt = stamp.at
        request.responseOrder = stamp.order
        entry.outstanding.delete(request.correlationId)
      })
    })
    return socket
  }

  /**
   * Retains the complete written frame and its socket-scoped request identity before any answer.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @param {object} [written] Original socket-write start.
   * @returns {void}
   */
  record(entry, frame, written = this.stamp()) {
    if (this.allFrames.length >= 100000) {
      this.problem('Wire archive exhausted; original attempts are missing')
      return
    }
    const { apiKey, apiVersion, correlationId, clientId, byteLength } = readRequestHeader(frame)
    const fact = {
      id: this.allFrames.length,
      label: entry.label,
      socket: this.sockets.indexOf(entry),
      endpoint: entry.endpoint,
      nodeId:
        Object.values(NODES).find(({ host, port }) => `${host}:${port}` === entry.endpoint)
          ?.nodeId ?? null,
      socketOpened: entry.opened,
      writeStarted: written,
      apiKey,
      apiVersion,
      correlationId,
      clientId,
      ...this.stamp(),
    }
    const context = this.context.getStore()
    if (context && context.apiKey === apiKey) fact.attempt = context.id ?? null
    if (apiKey === 0 || apiKey === 22) {
      try {
        if (apiKey === 0) {
          fact.batches = wireBatchStamps(fact, frame).filter(
            ({ topic, topicId }) => topic === this.topic || (topicId && topicId === this.topicId)
          )
        } else fact.identityRequest = initProducerFact(fact, frame, false)
      } catch (error) {
        this.problem(`Producer diagnostic frame ${fact.id}: ${error.message}`)
      }
    }
    if (this.reserve(frame.length)) fact.bytes = frame.toString('base64')
    if ([78, 79].includes(apiKey) && apiVersion === 2) {
      const reader = apiKey === 78 ? fetchWire : ackWire
      try {
        fact.request = reader.readRequest(frame.subarray(byteLength))
      } catch (error) {
        fact.requestError = describeError(error)
      }
    }
    this.allFrames.push(fact)
    if (fact.batches?.length) this.retainProduce(fact, frame)
    entry.outstanding.set(correlationId, fact)
    if (context && context.apiKey === apiKey) context.frames.push(fact)
    if (SHARE_APIS.has(apiKey)) this.frames.push(fact)
  }

  /** @param {object} frame Original request. @param {Buffer} bytes Complete response. @returns {void} */
  response(frame, bytes) {
    if (frame.apiKey === 0 || frame.apiKey === 22) {
      try {
        if (frame.apiKey === 0) frame.partitionResults = produceResponseFacts(frame, bytes)
        else frame.identityResponse = initProducerFact(frame, bytes, true)
      } catch (error) {
        this.problem(`Producer diagnostic response ${frame.id}: ${error.message}`)
      }
    }
    if (this.reserve(bytes.length)) frame.responseBytes = bytes.toString('base64')
    if (frame.apiKey !== 78 || frame.apiVersion !== 2) return
    try {
      frame.batches = wireBatchStamps(frame, bytes)
      for (const batch of frame.batches) {
        if (this.topicId && batch.topicId !== this.topicId) continue
        const key = batchStampKey(batch)
        const prior = this.fetchedStamps.get(key)
        if (prior && prior.baseOffset !== batch.baseOffset) this.pinProduce(batch)
        else if (!prior) this.fetchedStamps.set(key, batch)
      }
    } catch (error) {
      this.problem(`Fetch diagnostic response ${frame.id}: ${error.message}`)
    }
  }

  /**
   * Keeps recent original payloads without base64 encoding; eviction never removes compact facts.
   * @param {object} fact Compact Produce fact.
   * @param {Buffer} bytes Complete original request.
   * @returns {void}
   */
  retainProduce(fact, bytes) {
    if (!fact.batches.length) return
    this.rawProduce.set(fact.id, bytes)
    this.rawProduceBytes += bytes.length
    for (const partition of new Set(fact.batches.map(({ partition }) => partition))) {
      if (!this.recentProduce.has(partition)) this.recentProduce.set(partition, [])
      this.recentProduce.get(partition).push(fact.id)
      while (this.recentProduce.get(partition).length > RAW_PRODUCE_PER_PARTITION) {
        const id = this.recentProduce.get(partition).find((id) => !this.pinnedProduce.has(id))
        if (id === undefined) break
        this.evictProduce(id)
      }
    }
    while (this.rawProduceBytes > RAW_PRODUCE_BYTES) {
      const id = [...this.rawProduce.keys()].find((id) => !this.pinnedProduce.has(id))
      if (id === undefined) break
      this.evictProduce(id)
    }
  }

  /** @param {number} id Evicted raw payload; compact attempt facts remain. @returns {void} */
  evictProduce(id) {
    this.rawProduceBytes -= this.rawProduce.get(id).length
    this.rawProduce.delete(id)
    for (const [partition, ids] of this.recentProduce)
      this.recentProduce.set(
        partition,
        ids.filter((candidate) => candidate !== id)
      )
  }

  /** @param {object} batch Original fetched batch at two offsets. @returns {void} */
  pinProduce(batch) {
    const key = batchStampKey(batch)
    for (const id of this.rawProduce.keys())
      if (this.allFrames[id]?.batches?.some((candidate) => batchStampKey(candidate) === key))
        this.pinnedProduce.add(id)
  }

  /** @param {string} problem Missing evidence, recorded once. @returns {void} */
  problem(problem) {
    if (!this.problems.includes(problem)) this.problems.push(problem)
  }

  /** @param {number} bytes Original wire bytes. @returns {boolean} Whether they fit losslessly. */
  reserve(bytes) {
    if (this.byteCount + bytes > WIRE_BYTES) {
      this.problem('Wire byte archive exceeded its bound')
      return false
    }
    this.byteCount += bytes
    return true
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
    return this.liveOutstanding().length
  }

  /** @returns {object[]} the share requests on live sockets that have no response yet */
  inFlight() {
    return this.liveOutstanding().filter(({ apiKey }) => SHARE_APIS.has(apiKey))
  }

  /** @returns {object[]} every unanswered request on a socket that is still open */
  liveOutstanding() {
    return this.sockets
      .filter(({ socket }) => !socket.destroyed)
      .flatMap(({ outstanding }) => [...outstanding.values()])
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
 * @param {object} batch Original acknowledgement batch.
 * @param {string} offset Exact offset.
 * @returns {boolean} Whether this batch writes Accept for that offset.
 */
const accepts = (batch, offset) => {
  const first = BigInt(batch.firstOffset)
  const index = BigInt(offset) - first
  if (index < 0n || BigInt(offset) > BigInt(batch.lastOffset)) return false
  const types = batch.acknowledgeTypes
  return (types.length === 1 ? types[0] : types[Number(index)]) === 1
}

/**
 * @param {object} exchange Original exchange observation.
 * @param {object} entry Handler identity and its original acquisition.
 * @returns {object|null} The sole matching written partition, never an unwritten decision.
 */
const writtenAccept = (exchange, entry) => {
  if (!exchange || exchange.frames.length !== 1) return null
  const frame = exchange.frames[0]
  const request = frame.request
  if (
    !request ||
    ![78, 79].includes(frame.apiKey) ||
    frame.apiVersion !== 2 ||
    frame.socket !== entry.acquisition.source.frames[0].socket ||
    frame.endpoint !== entry.acquisition.source.endpoint ||
    request.groupId !== entry.groupId ||
    request.memberId !== entry.memberId ||
    request.isRenewAck ||
    exchange.open !== entry.acquisition.open ||
    exchange.pool !== entry.acquisition.pool ||
    exchange.generation !== entry.acquisition.generation
  )
    return null
  const topics = request.topics.filter(({ topicId }) => topicId === entry.topicId)
  if (topics.length !== 1) return null
  const partitions = topics[0].partitions.filter(
    ({ partitionIndex }) => partitionIndex === entry.partition
  )
  if (partitions.length !== 1) return null
  return partitions[0].acknowledgementBatches.filter((batch) => accepts(batch, entry.offset))
    .length === 1
    ? partitions[0]
    : null
}

/**
 * Records original authority and response identities without changing dispatch or settlement.
 * All wrappers call through, preserve object identity, and are removed after client cleanup.
 */
class BounceEvidence {
  /** @param {ShareBounceCase} owner Case whose workload is observed. */
  constructor(owner) {
    this.owner = owner
    this.labels = new Map()
    this.exchanges = []
    this.acquisitions = []
    this.outcomes = []
    this.outcomesByCohort = new Map()
    this.exchangesByCohort = new Map()
    this.retirements = []
    this.commands = []
    this.controls = []
    this.closes = []
    this.restorations = []
    this.epochs = []
    this.brokers = new Set()
    this.epochAuthorities = new Map()
    this.epochSamples = new Map()
    this.epochProbes = new Set()
    this.epochSignal = null
    this.brokerStates = []
    this.brokerStateKeys = new Map()
    this.cohorts = new Map()
    this.originalExchanges = new Map()
    this.restores = []
    this.handlers = new AsyncLocalStorage()
    if (owner.tap) owner.tap.onInterruption = (kind, event) => this.interrupt(kind, event)
  }

  /** @param {*} value Original object or symbol. @returns {number|null} Stable local label. */
  label(value) {
    if (value === null || value === undefined) return null
    if (!this.labels.has(value)) this.labels.set(value, this.labels.size)
    return this.labels.get(value)
  }

  /**
   * @param {object} prototype Original prototype.
   * @param {string} method Method name.
   * @param {Function} observe Call-through observer, supplied the receiver and original method.
   * @returns {void}
   */
  wrap(prototype, method, observe) {
    const original = prototype[method]
    prototype[method] = function observed(...args) {
      return observe(this, original, args)
    }
    this.restores.push(() => {
      prototype[method] = original
    })
  }

  /** @returns {void} Installs read-only observers before the workload connects. */
  install() {
    const owns = (loop) => loop.groupId === this.owner.group
    this.wrap(ShareRun.prototype, 'closeTraffic', async (run, original, [episode]) => {
      if (run.options.groupId !== this.owner.group) return original.call(run, episode)
      const fact = {
        id: this.closes.length,
        kind: 'close-traffic',
        started: this.owner.tap.stamp(),
        memberId: run.membership.memberId,
        memberEpoch: run.membership.memberEpoch,
      }
      this.closes.push(fact)
      try {
        return await original.call(run, episode)
      } finally {
        Object.assign(fact, {
          ended: this.owner.tap.stamp(),
          leaveWritten: Boolean(episode.leaveWritten),
          leaveReported: Boolean(episode.leaveReported),
          revoked: episode.revoked,
          finalDeadline: episode.finalDeadline,
        })
      }
    })
    this.wrap(ShareRun.prototype, 'leaveSkipped', (run, original, [episode, reason]) => {
      if (run.options.groupId === this.owner.group)
        this.closes.push(
          immutable({
            id: this.closes.length,
            kind: 'leave-skipped',
            ...this.owner.tap.stamp(),
            reason,
            leaveWritten: Boolean(episode.leaveWritten),
            memberId: run.membership.memberId,
          })
        )
      return original.call(run, episode, reason)
    })
    this.wrap(Broker.prototype, 'shareGroupHeartbeat', async (broker, original, args) => {
      const [request] = args
      if (request.groupId !== this.owner.group || request.memberEpoch >= 0)
        return original.apply(broker, args)
      const fact = {
        id: this.closes.length,
        kind: 'leave',
        apiKey: 76,
        request: immutable(request),
        nodeId: broker.nodeId,
        frames: [],
        started: this.owner.tap.stamp(),
      }
      this.closes.push(fact)
      return this.owner.tap.context.run(fact, async () => {
        try {
          const response = await original.apply(broker, args)
          fact.response = immutable(response)
          return response
        } catch (error) {
          fact.error = describeError(error)
          throw error
        } finally {
          fact.ended = this.owner.tap.stamp()
        }
      })
    })
    this.wrap(Broker.prototype, 'connect', async (broker, original, args) => {
      const result = await original.apply(broker, args)
      if (broker.connectionPool.clientId === `member-${this.owner.nonce}`) {
        this.brokers.add(broker)
        this.sampleEpoch(broker)
      }
      return result
    })
    this.wrap(RequestQueue.prototype, 'push', (queue, original, [request]) => {
      const context = this.owner.tap.context.getStore()
      if (!context) return original.call(queue, request)
      return original.call(queue, {
        ...request,
        sendRequest: () => this.owner.tap.context.run(context, () => request.sendRequest()),
      })
    })
    this.wrap(ShareFetchLoop.prototype, 'send', (loop, original, [exchange]) => {
      if (!owns(loop)) return original.call(loop, exchange)
      this.brokers.add(loop.broker)
      const binding = exchange.binding
      const authority = `${this.label(binding?.pool)}:${binding?.generation}`
      if (this.epochAuthorities.get(loop.broker) !== authority) {
        this.epochAuthorities.set(loop.broker, authority)
        this.sampleEpoch(loop.broker)
      }
      const fact = {
        id: this.label(exchange),
        prepared: this.owner.tap.stamp(),
        apiKey: exchange.apiKey,
        open: this.label(exchange.open),
        pool: this.label(binding?.pool),
        generation: binding?.generation,
        nodeId: loop.broker.nodeId,
        endpoint: loop.broker.brokerAddress,
        groupId: loop.groupId,
        memberId: loop.memberId,
        request: exchange.fields,
        model: [...loop.session.model.values()].map((row) => ({ ...row })),
        frames: [],
        tickets: exchange.prepared.map(({ row, ticket }) => ({
          cohort: this.label(row),
          ledger: this.label(row.ledger),
          ticket: this.label(ticket),
          batches: ticket.acknowledgementBatches,
          isRenewAck: ticket.isRenewAck,
        })),
      }
      this.originalExchanges.set(exchange, fact)
      this.exchanges.push(fact)
      for (const { cohort } of fact.tickets) {
        if (!this.exchangesByCohort.has(cohort)) this.exchangesByCohort.set(cohort, [])
        this.exchangesByCohort.get(cohort).push(fact)
      }
      return this.owner.tap.context.run(fact, () => original.call(loop, exchange))
    })
    this.wrap(ShareFetchLoop.prototype, 'complete', (loop, original, [exchange, response]) => {
      const fact = this.originalExchanges.get(exchange)
      if (!fact) return original.call(loop, exchange, response)
      fact.response = response
      fact.responseIdentity = this.label(response)
      if (
        fact.apiKey === 78 &&
        response.errorCode === 0 &&
        response.responses.some(({ partitions }) =>
          partitions.some(
            ({ errorCode, acquiredRecords }) => errorCode === 0 && acquiredRecords.length
          )
        )
      )
        this.sampleEpoch(loop.broker, { fresh: true })
      return original.call(loop, exchange, response)
    })
    this.wrap(ShareFetchLoop.prototype, 'failExchange', (loop, original, [exchange, error]) => {
      const fact = this.originalExchanges.get(exchange)
      if (fact) {
        const stamp = this.owner.tap.stamp()
        fact.failedAt = stamp.at
        fact.failedOrder = stamp.order
        fact.error = describeError(error)
        fact.cause = this.label(error)
        this.interrupt('failure', fact)
      }
      return original.call(loop, exchange, error)
    })
    this.wrap(ShareFetchLoop.prototype, 'adopt', (loop, original, args) => {
      const row = original.apply(loop, args)
      if (!owns(loop)) return row
      const [exchange, topicId, partition] = args
      const source = this.originalExchanges.get(exchange)
      const adoption = this.owner.tap.stamp()
      const fact = {
        id: this.label(row),
        token: this.label(row.token),
        ledger: this.label(row.ledger),
        topicId,
        partition: row.partition,
        topic: row.topic,
        exchange: source?.id,
        responseIdentity: source?.responseIdentity,
        open: source?.open,
        pool: source?.pool,
        generation: source?.generation,
        adoptedAt: adoption.at,
        adoptedOrder: adoption.order,
        topError: source?.response?.errorCode,
        fetchError: partition.errorCode,
        acknowledgeError: partition.acknowledgeErrorCode,
        acquiredRecords: partition.acquiredRecords.map((range) => ({ ...range })),
        source,
      }
      this.cohorts.set(row, fact)
      this.acquisitions.push(fact)
      return row
    })
    this.wrap(ShareRun.prototype, 'invoke', (run, original, args) => {
      const cohort = this.cohorts.get(args[1])
      return cohort
        ? this.handlers.run(cohort, () => original.apply(run, args))
        : original.apply(run, args)
    })
    this.wrap(ShareFetchLoop.prototype, 'emit', (loop, original, [results, row]) => {
      if (owns(loop))
        for (const result of results) {
          const outcome = {
            ...result,
            cohort: this.label(row),
            ...this.owner.tap.stamp(),
            typed:
              result.error instanceof KafkaJSShareAcknowledgementError &&
              result.error.topic === row.topic &&
              result.error.partition === row.partition,
            cause: this.label(result.error?.cause),
            error: describeError(result.error),
          }
          this.outcomes.push(outcome)
          if (!this.outcomesByCohort.has(outcome.cohort))
            this.outcomesByCohort.set(outcome.cohort, [])
          this.outcomesByCohort.get(outcome.cohort).push(outcome)
        }
      return original.call(loop, results, row)
    })
    for (const method of ['onSessionReset', 'retireKey']) {
      this.wrap(ShareFetchLoop.prototype, method, (loop, original, args) => {
        if (owns(loop)) {
          const loss = {
            id: this.retirements.length,
            ...this.owner.tap.stamp(),
            nodeId: loop.broker.nodeId,
            groupId: loop.groupId,
            memberId: loop.memberId,
            open: this.label(method === 'onSessionReset' ? args[0] : loop.session.token),
            key: method === 'retireKey' ? args[0] : null,
            reason: args[1],
            cause: this.label(args[2]),
            error: describeError(args[2]),
          }
          this.retirements.push(loss)
          this.interrupt('retirement', loss)
        }
        return original.apply(loop, args)
      })
    }
    this.wrap(Broker.prototype, 'metadata', (broker, original, args) => {
      const fact = {
        apiKey: 3,
        frames: [],
        nodeId: broker.nodeId,
        pool: this.label(broker.connectionPool),
        endpoint: broker.brokerAddress,
        startedAt: performance.now(),
      }
      return this.owner.tap.context.run(fact, async () => {
        const response = await original.apply(broker, args)
        fact.endedAt = performance.now()
        fact.response = immutable(response)
        if (response.topicMetadata?.some(({ topic }) => topic === this.owner.topic))
          this.epochs.push(fact)
        return response
      })
    })
  }

  /**
   * Captures original pending work before socket, exchange or ledger loss processing continues.
   * @param {string} kind Primitive observation type.
   * @param {object} event Original observation, retained in its independent journal.
   * @returns {void}
   */
  interrupt(kind, event) {
    const { entries, snapshots } = this.owner
    const cut = interruptionEvent(kind, event)
    const bounce = snapshots.findLast(
      (episode) => precedes(episode, cut) && cut.at <= (episode.restoredAt ?? Infinity)
    )
    if (!bounce) return
    const scopes = new Map()
    for (const scope of interruptedScopes(entries, this, kind, event, bounce)) {
      if (scope.nodeId === bounce.nodeId) scopes.set(JSON.stringify(scope), scope)
    }
    if (!scopes.size) return
    const history = unresolvedAt(entries, this, cut)
    for (const scope of scopes.values()) {
      if (bounce.interruptions.some((prior) => samePartition(prior.scope, scope))) continue
      const obligations = history.filter((entry) => sameAuthority(authority(entry), scope))
      const carried = carriedAt(snapshots, bounce, history)
      bounce.interruptions.push(
        immutable({
          ...cut,
          kind,
          event: event.id,
          scope,
          obligations: obligations.map((entry) =>
            obligation(entry, snapshots, bounce.index, this, cut, true)
          ),
          carried,
          lateEntries: obligations.filter((entry) => precedes(bounce, entry)).map(({ id }) => id),
          history: history.map(({ id }) => id),
        })
      )
      this.owner.captureProblems.push(
        ...historyAccounting(entries, snapshots, this, cut).problems,
        ...admissionAccounting(snapshots).problems
      )
    }
  }

  /** @returns {void} Restores originals after all workload and verifier calls have joined. */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.handlers.disable()
    this.owner.tap.context.disable()
    delete this.owner.tap.onInterruption
  }

  /**
   * Starts independent source-node sampling, coalescing another observation after a pending sample.
   * Acquisition observers request a fresh upper bound without delaying the original dispatch.
   * @param {object} broker Original workload broker.
   * @param {{fresh?:boolean}} [options] Starts an acquisition observation alongside pending sampling.
   * @returns {void}
   */
  sampleEpoch(broker, { fresh = false } = {}) {
    const signal = this.epochSignal
    if (!signal || signal.aborted || this.owner.driveFinished) return
    const pending = this.epochSamples.get(broker)
    if (pending && !fresh) {
      pending.again = true
      return
    }
    if (!broker.connectionPool.isConnected()) return
    const sample = { again: false }
    if (!pending) this.epochSamples.set(broker, sample)
    sample.task = (async () => {
      do {
        sample.again = false
        await bounded(signal, Infinity, (scope) =>
          (async () => {
            const response = await broker.metadata([this.owner.topic], {
              guard: () => (scope.aborted ? scope.reason : null),
            })
            const topic = response.topicMetadata?.find(({ topic }) => topic === this.owner.topic)
            const key = JSON.stringify(topic)
            if (this.brokerStateKeys.get(broker) !== key) {
              this.brokerStateKeys.set(broker, key)
              await this.captureBrokerState(broker, scope)
            }
          })()
        ).catch((error) => {
          if (signal.aborted) throw error
        })
      } while (
        sample.again &&
        !this.owner.driveFinished &&
        !signal.aborted &&
        broker.connectionPool.isConnected()
      )
    })().finally(() => {
      if (this.epochSamples.get(broker) === sample) this.epochSamples.delete(broker)
      this.epochProbes.delete(sample.task)
    })
    this.epochProbes.add(sample.task)
    sample.task.catch(() => {})
  }

  /**
   * Captures exposed producer timestamps and ends when a node observes changed leadership/ISR.
   * @param {object} broker Existing source broker.
   * @param {AbortSignal} signal Original sampling scope.
   * @returns {Promise<void>} Both original observations, including item errors.
   */
  async captureBrokerState(broker, signal) {
    for (const [method, apiKey, options] of [
      ['describeProducers', 61, { topics: [{ topic: this.owner.topic, partitions: [0, 1, 2] }] }],
      [
        'listOffsets',
        2,
        {
          replicaId: -1,
          isolationLevel: 0,
          topics: [
            {
              topic: this.owner.topic,
              partitions: [0, 1, 2].map((partition) => ({ partition, timestamp: -1 })),
            },
          ],
        },
      ],
    ]) {
      signal.throwIfAborted()
      if (this.brokerStates.length >= 2000) {
        this.owner.tap.problem('Broker-state archive exceeded 2000 observations')
        return
      }
      const fact = {
        apiKey,
        method,
        nodeId: broker.nodeId,
        endpoint: broker.brokerAddress,
        frames: [],
        started: this.owner.tap.stamp(),
      }
      this.brokerStates.push(fact)
      try {
        const response = await this.owner.tap.context.run(fact, () => broker[method](options))
        fact.response = this.owner.producerEvidence
          ? (this.owner.producerEvidence.retain([], { response })?.response ?? null)
          : immutable(response)
        if (fact.response === null) fact.captureError = 'Producer evidence byte bound exhausted'
      } catch (error) {
        fact.error = describeError(error, 8)
      }
      fact.ended = this.owner.tap.stamp()
    }
  }

  /**
   * Samples the existing workload brokers' own metadata, without installing it in the client cache.
   * @param {AbortSignal} signal Existing drive scope.
   * @returns {Promise<void>} Once drive finishes; every sample stays within that scope.
   */
  async sampleEpochs(signal) {
    this.epochSignal = signal
    try {
      while (!this.owner.driveFinished) {
        signal.throwIfAborted()
        for (const broker of this.brokers) this.sampleEpoch(broker)
        await delay(signal, 100)
      }
    } finally {
      this.epochSignal = null
      await Promise.all([...this.epochProbes])
    }
  }

  /**
   * @param {object} entry Actual handler start.
   * @returns {object[]} Actual terminal outcomes for this cohort and offset.
   */
  results(entry) {
    return (this.outcomesByCohort.get(entry.acquisition.id) ?? []).filter(
      (outcome) =>
        BigInt(outcome.firstOffset) <= BigInt(entry.offset) &&
        BigInt(outcome.lastOffset) >= BigInt(entry.offset)
    )
  }

  /**
   * @param {object} entry Handler start.
   * @param {object} outcome Actual emitted outcome.
   * @returns {object[]} Original written Accept exchanges for this outcome's ticket/cohort.
   */
  acceptsFor(entry, outcome) {
    return (this.exchangesByCohort.get(entry.acquisition.id) ?? []).filter(
      (exchange) =>
        precedes(entry, exchange.prepared ?? exchange.frames[0] ?? { at: -Infinity }) &&
        exchange.tickets.some(
          (ticket) =>
            ticket.cohort === entry.acquisition.id &&
            ticket.ledger === entry.acquisition.ledger &&
            ticket.ticket !== undefined &&
            !ticket.isRenewAck &&
            ticket.batches.some((batch) => accepts(batch, entry.offset))
        ) &&
        exchange.frames[0]?.at <= outcome.at &&
        writtenAccept(exchange, entry)
    )
  }

  /**
   * @param {object} entry Handler start.
   * @returns {object[]} Actual null-error Accepts with matching original request/response.
   */
  successfulReceipts(entry) {
    return this.results(entry).filter(
      (outcome) =>
        outcome.type === 'accept' &&
        outcome.error === null &&
        this.acceptsFor(entry, outcome).some((exchange) => {
          const response = exchange.response
          if (
            response?.errorCode !== 0 ||
            !exchange.frames[0].responseBytes ||
            exchange.frames[0].responseAt > outcome.at
          )
            return false
          const topics = response.responses.filter(({ topicId }) => topicId === entry.topicId)
          const rows =
            topics.length === 1
              ? topics[0].partitions.filter(
                  ({ partitionIndex }) => partitionIndex === entry.partition
                )
              : []
          return (
            rows.length === 1 &&
            (exchange.apiKey === 78 ? rows[0].acknowledgeErrorCode : rows[0].errorCode) === 0
          )
        })
    )
  }

  /** @param {object} entry Handler. @param {object} [cut] Boundary. @returns {boolean} Valid earlier receipt. */
  successful(entry, cut = { at: Infinity }) {
    return (this.auditIndex?.receipts.get(entry.id) ?? this.successfulReceipts(entry)).some(
      (outcome) => precedes(outcome, cut)
    )
  }
}

/**
 * @param {object} entry Handler start with original acquisition.
 * @returns {boolean} Whether its response, range, authority and chronology prove an acquisition.
 */
const validAcquisition = (entry) => {
  const a = entry.acquisition
  const source = a?.source
  const frame = source?.frames[0]
  const ranges =
    a?.acquiredRecords?.filter(
      (range) =>
        BigInt(range.firstOffset) <= BigInt(entry.offset) &&
        BigInt(range.lastOffset) >= BigInt(entry.offset)
    ) ?? []
  if (
    !/^(0|[1-9][0-9]*)$/.test(entry.offset) ||
    !entry.topicId ||
    !Number.isInteger(entry.partition) ||
    entry.keyBytes === undefined ||
    entry.valueBytes === undefined
  )
    return false
  const identity = shareRecordIdentity({
    topic: entry.topic,
    partition: entry.partition,
    offset: entry.offset,
    key: entry.keyBytes === null ? null : Buffer.from(entry.keyBytes, 'hex'),
    value: entry.valueBytes === null ? null : Buffer.from(entry.valueBytes, 'hex'),
  })
  const topics =
    source?.response?.responses.filter(({ topicId }) => topicId === entry.topicId) ?? []
  const partitions =
    topics.length === 1
      ? topics[0].partitions.filter(({ partitionIndex }) => partitionIndex === entry.partition)
      : []
  return (
    partitions.length === 1 &&
    source.response.errorCode === 0 &&
    partitions[0].errorCode === 0 &&
    JSON.stringify(partitions[0].acquiredRecords) === JSON.stringify(a.acquiredRecords) &&
    identity === entry.identity &&
    source?.frames.length === 1 &&
    frame.apiKey === 78 &&
    frame.apiVersion === 2 &&
    frame.responseBytes &&
    a.responseIdentity !== undefined &&
    a.topError === 0 &&
    a.fetchError === 0 &&
    frame.request?.groupId === entry.groupId &&
    frame.request?.memberId === entry.memberId &&
    frame.endpoint === source.endpoint &&
    Number.isInteger(frame.nodeId) &&
    frame.nodeId === source.nodeId &&
    source.groupId === entry.groupId &&
    source.memberId === entry.memberId &&
    source.open !== null &&
    source.pool !== null &&
    Number.isInteger(source.generation) &&
    frame.at <= frame.responseAt &&
    frame.responseAt <= a.adoptedAt &&
    a.adoptedAt <= entry.at &&
    a.open === source.open &&
    a.pool === source.pool &&
    a.generation === source.generation &&
    a.exchange === source.id &&
    a.topic === entry.topic &&
    a.topicId === entry.topicId &&
    a.partition === entry.partition &&
    ranges.length === 1 &&
    ranges[0].deliveryCount === entry.deliveryCount
  )
}

/** @param {object} value Evidence projection. @returns {object} A detached, immutable snapshot. */
const immutable = (value) => {
  const copy = JSON.parse(JSON.stringify(value))
  const freeze = (item) => {
    if (item && typeof item === 'object') {
      Object.values(item).forEach(freeze)
      Object.freeze(item)
    }
    return item
  }
  return freeze(copy)
}

/**
 * @param {object} first Earlier observation.
 * @param {object} second Later observation.
 * @returns {boolean} Strict observed order; equal clocks without journal order prove nothing.
 */
const precedes = (first, second) =>
  first.at < second.at ||
  (first.at === second.at &&
    Number.isInteger(first.order) &&
    Number.isInteger(second.order) &&
    first.order < second.order)

/** @param {object} entry Original handler. @returns {object} Its complete original authority. */
const authority = (entry) => ({
  groupId: entry.groupId,
  memberId: entry.memberId,
  nodeId: entry.acquisition?.source?.nodeId,
  endpoint: entry.acquisition?.source?.endpoint,
  socket: entry.acquisition?.source?.frames[0]?.socket,
  pool: entry.acquisition?.pool,
  generation: entry.acquisition?.generation,
  open: entry.acquisition?.open,
  topicId: entry.topicId,
  partition: entry.partition,
})

/** @param {object} a Authority. @param {object} b Authority. @returns {boolean} Exact scope match. */
const sameAuthority = (a, b) => JSON.stringify(a) === JSON.stringify(b)

/** @param {object} a Authority. @param {object} b Authority. @returns {boolean} Same group partition. */
const samePartition = (a, b) =>
  a.groupId === b.groupId && a.topicId === b.topicId && a.partition === b.partition

/**
 * @param {string} kind Primitive observation type.
 * @param {object} event Original event.
 * @returns {object} The event's original clock and order.
 */
const interruptionEvent = (kind, event) =>
  kind === 'failure'
    ? { at: event.failedAt, order: event.failedOrder }
    : { at: event.at, order: event.order }

/**
 * @param {string} kind Primitive observation type.
 * @param {object} event Original event.
 * @param {object} scope Original workload authority.
 * @returns {boolean} Whether the event interrupts that authority, rather than a replacement.
 */
const interrupts = (kind, event, scope) => {
  if (kind === 'socket') return event.socket === scope.socket
  if (
    event.nodeId !== scope.nodeId ||
    event.groupId !== scope.groupId ||
    event.memberId !== scope.memberId ||
    event.open !== scope.open ||
    event.cause === null ||
    event.cause === undefined
  )
    return false
  if (kind === 'retirement')
    return (
      (!event.key || event.key === `${scope.topicId}:${scope.partition}`) &&
      ['connection', 'leader-moved', 'session-lost'].includes(event.reason)
    )
  return (
    kind === 'failure' &&
    event.pool === scope.pool &&
    event.generation === scope.generation &&
    event.endpoint === scope.endpoint &&
    (!event.frames.length || event.frames[0].socket === scope.socket) &&
    ['KafkaJSConnectionError', 'KafkaJSConnectionClosedError', 'ShareSessionStaleError'].includes(
      event.error?.name
    )
  )
}

/** @param {Map} index Join index. @param {*} key Key. @param {*} value Member. @returns {void} */
const indexMember = (index, key, value) => {
  if (!index.has(key)) index.set(key, [])
  index.get(key).push(value)
}

/** @param {Iterable} values Source membership. @param {Function} project Row projection. @returns {Generator} Projected rows with bounded steps. */
const mappedAuditWork = function* (values, project) {
  const rows = []
  let visited = 0
  for (const value of values) {
    if (visited++ % 100 === 0) yield
    rows.push(project(value))
  }
  return rows
}

/** @param {object[]} values Rows. @param {Function} compare Ordering. @returns {Generator} Sorted rows with a yield per 100 comparisons. */
const sortedAuditWork = function* (values, compare) {
  let source = [...values]
  let target = Array(source.length)
  let comparisons = 0
  for (let width = 1; width < source.length; width *= 2) {
    for (let base = 0; base < source.length; base += 2 * width) {
      let left = base
      let right = Math.min(base + width, source.length)
      const middle = right
      const end = Math.min(base + 2 * width, source.length)
      for (let at = base; at < end; at++) {
        if (comparisons++ % 100 === 0) yield
        target[at] =
          left < middle && (right >= end || compare(source[left], source[right]) <= 0)
            ? source[left++]
            : source[right++]
      }
    }
    ;[source, target] = [target, source]
  }
  return source
}

/**
 * Seals reference membership before yielding, then copies rows and builds joins cooperatively.
 * Previously each primitive scanned/sorted all handlers and each obligation scanned all exchanges.
 * Index/sweep cost is O((H+A+J+X) log(H+A+J+X) + K*L log L), plus scoped joins;
 * previously reconstruction cost O(J*(H+A)*log(H+A) + J*H*(J+X)). K contains only
 * the six commands and first qualifying partition cuts; L is their retained membership.
 * Numeric proof uses suffix queries instead of upper-by-lower/sample rescans; archival
 * exclusion uses O((R+U) log(R+U)) offset sweeps instead of O(U*X). No live budget changes.
 * @param {object[]} entries Handler journal.
 * @param {BounceEvidence} original Live evidence.
 * @param {object[]} bounces Original restoration bounds.
 * @returns {Generator} Detached journal membership and per-audit indices, never shared across polls.
 */
const indexedEvidenceWork = function* (entries, original, bounces) {
  const evidence = Object.create(original)
  const journals = Object.fromEntries(
    ['exchanges', 'acquisitions', 'epochs', 'outcomes', 'retirements', 'commands', 'controls'].map(
      (key) => [key, [...original[key]]]
    )
  )
  const originalEntries = [...entries]
  const socketEvents = [...original.owner.tap.socketEvents]
  const cohortExchanges = new Map(original.exchangesByCohort)
  const cohortOutcomes = new Map(original.outcomesByCohort)
  const originalExchanges = new Set(journals.exchanges)
  const originalOutcomes = new Set(journals.outcomes)
  const frames = new Map()
  const exchanges = new Map()
  const acquisitions = new Map()
  /** @param {object} frame Original frame. @returns {object} Sealed mutable frame shell. */
  const frameCopy = (frame) => {
    if (!frames.has(frame)) frames.set(frame, { ...frame })
    return frames.get(frame)
  }
  /** @param {object} exchange Original exchange. @returns {object} Sealed frame membership and response state. */
  const exchangeCopy = (exchange) => {
    if (!exchange) return exchange
    if (!exchanges.has(exchange))
      exchanges.set(exchange, {
        ...exchange,
        frames: (exchange.frames ?? []).map(frameCopy),
        tickets: [...(exchange.tickets ?? [])],
      })
    return exchanges.get(exchange)
  }
  /** @param {object} acquisition Original acquisition. @returns {object} Sealed adoption and source state. */
  const acquisitionCopy = (acquisition) => {
    if (!acquisition) return acquisition
    if (!acquisitions.has(acquisition))
      acquisitions.set(acquisition, { ...acquisition, source: exchangeCopy(acquisition.source) })
    return acquisitions.get(acquisition)
  }
  evidence.exchanges = yield* mappedAuditWork(journals.exchanges, exchangeCopy)
  evidence.acquisitions = yield* mappedAuditWork(journals.acquisitions, acquisitionCopy)
  evidence.epochs = yield* mappedAuditWork(journals.epochs, (sample) => ({
    ...sample,
    frames: sample.frames.map(frameCopy),
  }))
  for (const key of ['outcomes', 'retirements', 'commands', 'controls'])
    evidence[key] = journals[key]
  evidence.owner = {
    ...original.owner,
    tap: { socketEvents },
  }
  evidence.exchangesByCohort = new Map()
  evidence.outcomesByCohort = new Map()
  let visited = 0
  for (const [cohort, outcomes] of cohortOutcomes) {
    if (visited++ % 100 === 0) yield
    evidence.outcomesByCohort.set(
      cohort,
      outcomes.filter((row) => originalOutcomes.has(row))
    )
  }
  entries = yield* mappedAuditWork(originalEntries, (entry) => ({
    ...entry,
    acquisition: acquisitionCopy(entry.acquisition),
  }))
  evidence.auditEntries = entries
  evidence.auditIndex = {
    scopes: new Map(),
    opens: new Map(),
    losses: new Map(),
    history: new Map(),
    deliveries: new Map(),
    receipts: new Map(),
    boundaries: new Map(),
  }
  for (const exchange of evidence.exchanges) {
    if (visited++ % 100 === 0) yield
    const cohorts = new Set(exchange.tickets.map(({ cohort }) => cohort))
    for (const cohort of cohorts) indexMember(evidence.exchangesByCohort, cohort, exchange)
  }
  // Preserve cohort membership even for scripted empty-ticket acquisitions.
  for (const [cohort, exchanges] of cohortExchanges)
    for (const exchange of exchanges) {
      if (visited++ % 100 === 0) yield
      if (!originalExchanges.has(exchange)) continue
      const copied = exchangeCopy(exchange)
      if (!(evidence.exchangesByCohort.get(cohort) ?? []).includes(copied))
        indexMember(evidence.exchangesByCohort, cohort, copied)
    }
  const originals = yield* mappedAuditWork(evidence.acquisitions, (acquisition) => ({
    groupId: acquisition.source?.groupId,
    memberId: acquisition.source?.memberId,
    topicId: acquisition.topicId,
    partition: acquisition.partition,
    acquisition,
  }))
  const seenAcquisitions = new Set()
  for (const { acquisition } of originals) {
    if (visited++ % 100 === 0) yield
    seenAcquisitions.add(acquisition)
  }
  for (const entry of entries) {
    if (visited++ % 100 === 0) yield
    if (!seenAcquisitions.has(entry.acquisition)) {
      seenAcquisitions.add(entry.acquisition)
      originals.push(entry)
    }
  }
  const authorities = new Map()
  for (const entry of originals) {
    if (visited++ % 100 === 0) yield
    if (!entry.acquisition?.source?.frames[0]) continue
    const scope = authority(entry)
    const key = JSON.stringify(scope)
    const prior = authorities.get(key)
    if (!prior || entry.acquisition.adoptedAt < prior.acquisition.adoptedAt)
      authorities.set(key, entry)
  }
  for (const entry of authorities.values()) {
    const scope = authority(entry)
    indexMember(evidence.auditIndex.scopes, scope.socket, entry)
    indexMember(evidence.auditIndex.opens, `${scope.nodeId}:${scope.open}`, entry)
  }
  const journal = interruptionJournal(evidence)
  for (const item of journal) {
    if (visited++ % 100 === 0) yield
    const key =
      item.kind === 'socket'
        ? `socket:${item.event.socket}`
        : `open:${item.event.nodeId}:${item.event.open}`
    indexMember(evidence.auditIndex.losses, key, item)
  }
  const cutKeys = new Set()
  const episodes = []
  const primitives = yield* sortedAuditWork(
    [...journal, ...evidence.commands.map((event) => ({ kind: 'command', event }))],
    (a, b) => {
      const first = interruptionEvent(a.kind, a.event)
      const second = interruptionEvent(b.kind, b.event)
      return first.at - second.at || first.order - second.order
    }
  )
  for (const { kind, event } of primitives) {
    if (visited++ % 100 === 0) yield
    const cut = interruptionEvent(kind, event)
    if (kind === 'command') {
      episodes.push({
        ...event,
        interruptions: [],
        restoredAt: bounces.find(({ index }) => index === event.index)?.restoredAt,
      })
      cutKeys.add(`${cut.at}:${cut.order}`)
      continue
    }
    const bounce = episodes.findLast(
      (episode) => precedes(episode, cut) && cut.at <= (episode.restoredAt ?? Infinity)
    )
    if (!bounce) continue
    for (const scope of interruptedScopes(entries, evidence, kind, event, bounce)) {
      if (
        scope.nodeId !== bounce.nodeId ||
        bounce.interruptions.some((prior) => samePartition(prior.scope, scope))
      )
        continue
      bounce.interruptions.push({ scope })
      cutKeys.add(`${cut.at}:${cut.order}`)
    }
  }
  const timeline = []
  for (const entry of entries) {
    if (visited++ % 100 === 0) yield
    indexMember(evidence.auditIndex.deliveries, entry.identity, entry)
    timeline.push({ kind: 'handler', event: entry })
    const outcomes = entry.acquisition ? evidence.successfulReceipts(entry) : []
    evidence.auditIndex.receipts.set(entry.id, outcomes)
    for (const event of outcomes) timeline.push({ kind: 'success', event, entry })
  }
  for (const { kind, event } of [
    ...journal,
    ...evidence.commands.map((event) => ({ kind: 'command', event })),
  ])
    if (cutKeys.has(`${interruptionEvent(kind, event).at}:${interruptionEvent(kind, event).order}`))
      timeline.push({ kind: 'cut', event: interruptionEvent(kind, event) })
  const entryPosition = new Map(entries.map((entry, position) => [entry, position]))
  const orderedTimeline = yield* sortedAuditWork(timeline, (a, b) => a.event.at - b.event.at)
  const active = new Map()
  let position = 0
  while (position < orderedTimeline.length) {
    const at = orderedTimeline[position].event.at
    const successes = []
    const cuts = []
    while (position < orderedTimeline.length && orderedTimeline[position].event.at === at) {
      if (visited++ % 100 === 0) yield
      const item = orderedTimeline[position++]
      if (item.kind === 'handler') indexMember(active, item.event.identity, item.event)
      else if (item.kind === 'success') successes.push(item)
      else cuts.push(item.event)
    }
    // Equal clocks are a partial order. Apply receipts to each cut only with strict evidence.
    for (const cut of cuts) {
      const key = `${cut.at}:${cut.order}`
      if (evidence.auditIndex.history.has(key)) continue
      const lastSuccess = new Map()
      for (const { entry, event } of successes) {
        if (visited++ % 100 === 0) yield
        if (!precedes(entry, cut) || !precedes(event, cut)) continue
        const prior = lastSuccess.get(entry.identity)
        if (!prior || entryPosition.get(prior) < entryPosition.get(entry))
          lastSuccess.set(entry.identity, entry)
      }
      const history = []
      for (const [identity, group] of active) {
        const success = lastSuccess.get(identity)
        for (const entry of group) {
          if (visited++ % 100 === 0) yield
          if (precedes(entry, cut) && (!success || precedes(success, entry))) history.push(entry)
        }
      }
      evidence.auditIndex.history.set(
        key,
        yield* sortedAuditWork(history, (a, b) => entryPosition.get(a) - entryPosition.get(b))
      )
    }
    // Every later clock strictly follows these receipts, even when their sequence is unavailable.
    for (const { entry } of successes) {
      if (visited++ % 100 === 0) yield
      const remaining = (active.get(entry.identity) ?? []).filter((later) => precedes(entry, later))
      if (remaining.length) active.set(entry.identity, remaining)
      else active.delete(entry.identity)
    }
  }
  yield* epochSampleIndexWork(evidence.epochs)
  return evidence
}

/** @param {object} scope Authority. @param {object} evidence Indexed evidence. @returns {object[]} Scoped primitive losses. */
const scopedLosses = (scope, evidence) =>
  evidence.auditIndex
    ? [
        ...(evidence.auditIndex.losses.get(`socket:${scope.socket}`) ?? []),
        ...(evidence.auditIndex.losses.get(`open:${scope.nodeId}:${scope.open}`) ?? []),
      ]
    : interruptionJournal(evidence)

/**
 * Finds the original authority still live immediately before a primitive loss.
 * @param {object[]} entries Original handler journal.
 * @param {BounceEvidence} evidence Original acquisitions and retirements.
 * @param {string} kind Primitive observation type.
 * @param {object} event Original primitive event.
 * @param {object} [bounce] Excludes partitions whose first interruption is already frozen.
 * @returns {object[]} Affected authorities, newest open first for each partition.
 */
const interruptedScopes = (entries, evidence, kind, event, bounce) => {
  const cut = interruptionEvent(kind, event)
  const candidates = evidence.auditIndex
    ? kind === 'socket'
      ? (evidence.auditIndex.scopes.get(event.socket) ?? [])
      : (evidence.auditIndex.opens.get(`${event.nodeId}:${event.open}`) ?? [])
    : [
        ...entries,
        ...evidence.acquisitions.map((acquisition) => ({
          groupId: acquisition.source.groupId,
          memberId: acquisition.source.memberId,
          topicId: acquisition.topicId,
          partition: acquisition.partition,
          acquisition,
        })),
      ]
  const originals = candidates
    .filter(
      (entry) =>
        entry.acquisition?.source?.frames[0] &&
        precedes({ at: entry.acquisition.adoptedAt, order: entry.acquisition.adoptedOrder }, cut) &&
        !bounce?.interruptions.some(({ scope }) => samePartition(scope, entry))
    )
    .sort((a, b) => b.acquisition.adoptedAt - a.acquisition.adoptedAt)
  const scopes = new Map()
  for (const entry of originals) {
    const scope = authority(entry)
    if (
      !interrupts(kind, event, scope) ||
      scopedLosses(scope, evidence).some(
        ({ kind: lossKind, event: loss }) =>
          lossKind !== 'failure' && interrupts(lossKind, loss, scope) && precedes(loss, cut)
      )
    )
      continue
    scopes.set(JSON.stringify(scope), scope)
  }
  return [...scopes.values()]
}

/**
 * @param {object[]} entries Complete original handler journal, including retired ledgers.
 * @param {BounceEvidence} evidence Original receipts.
 * @param {object} cut Observation boundary.
 * @returns {object[]} All handler-started attempts still unresolved immediately before the cut.
 */
const unresolvedAt = (entries, evidence, cut) => {
  const cached = evidence.auditIndex?.history.get(`${cut.at}:${cut.order}`)
  if (cached) return cached
  const started = entries.filter((entry) => precedes(entry, cut))
  const lastSuccess = new Map()
  for (const entry of started)
    if (entry.acquisition && evidence.successful(entry, cut)) lastSuccess.set(entry.identity, entry)
  return started.filter((entry) => {
    const success = lastSuccess.get(entry.identity)
    return !success || precedes(success, entry)
  })
}

/** @param {object} bounce Frozen episode. @returns {Map} Its combined identity and attempt projections. */
const effectiveObligations = (bounce) => {
  const identities = new Map()
  const snapshots = [
    { obligations: [], carried: bounce.carried ?? [] },
    ...(bounce.interruptions ?? []),
  ]
  for (const cut of snapshots)
    for (const held of [...cut.obligations, ...cut.carried]) {
      if (!identities.has(held.identity)) identities.set(held.identity, [])
      if (!identities.get(held.identity).some(({ entry }) => entry === held.entry))
        identities.get(held.identity).push(held)
    }
  return identities
}

/**
 * @param {object} entry Actual original handler.
 * @param {object[]} bounces Earlier freezes.
 * @param {number} index Episode recording this observation.
 * @param {BounceEvidence} evidence Original ticket journal.
 * @param {object} cut Observation boundary.
 * @param {boolean} [admit] Whether this observation is a qualifying interruption.
 * @returns {object} Original attempt and tickets, retaining the first interruption attribution.
 */
const obligation = (entry, bounces, index, evidence, cut, admit = false) => ({
  identity: entry.identity,
  entry: entry.id,
  cohort: entry.acquisition?.id,
  ledger: entry.acquisition?.ledger,
  acquisition: entry.acquisition?.exchange,
  authority: authority(entry),
  tickets: (
    evidence.exchangesByCohort.get(entry.acquisition?.id) ??
    (evidence.auditIndex
      ? []
      : evidence.exchanges.filter((exchange) =>
          exchange.tickets.some(({ cohort }) => cohort === entry.acquisition?.id)
        ))
  ).flatMap((exchange) =>
    precedes(exchange.prepared ?? exchange.frames[0] ?? { at: Infinity }, cut)
      ? exchange.tickets.filter(({ cohort }) => cohort === entry.acquisition?.id)
      : []
  ),
  originatingEpisode:
    bounces.find(
      (episode) =>
        episode.index < index && newObligations(episode).some((held) => held.entry === entry.id)
    )?.index ?? (admit ? index : null),
})

/**
 * @param {object[]} bounces Original episodes.
 * @param {object} bounce Current episode.
 * @param {object[]} history Unresolved attempts at this cut.
 * @returns {object[]} Earlier obligations still unresolved, with their original charge unchanged.
 */
const carriedAt = (bounces, bounce, history) => {
  const ids = new Set(history.map(({ id }) => id))
  const carried = new Map()
  for (const prior of bounces.filter(({ index }) => index < bounce.index))
    for (const held of [...effectiveObligations(prior).values()].flat())
      if (ids.has(held.entry) && !carried.has(held.entry)) carried.set(held.entry, held)
  return [...carried.values()]
}

/**
 * @param {BounceEvidence} evidence Primitive journals.
 * @returns {object[]} Every socket loss, original exchange failure and authority retirement.
 */
const interruptionJournal = (evidence) => [
  ...(evidence.owner.tap?.socketEvents ?? []).map((event) => ({ kind: 'socket', event })),
  ...evidence.exchanges
    .filter((event) => event.failedAt !== undefined)
    .map((event) => ({ kind: 'failure', event })),
  ...evidence.retirements.map((event) => ({ kind: 'retirement', event })),
]

/**
 * @param {object} entry Original handler.
 * @param {object} bounce Designated episode.
 * @param {BounceEvidence} evidence Primitive observations.
 * @param {number} [after] New request write, if auditing replay.
 * @returns {object|null} Original immutable interruption with the independently matched retirement.
 */
const matchingInterruption = (entry, bounce, evidence, after = Infinity) => {
  const scope = authority(entry)
  if (scope.nodeId !== bounce.nodeId || !effectiveObligations(bounce).has(entry.identity))
    return null
  for (const cut of bounce.interruptions ?? []) {
    if (
      !sameAuthority(cut.scope, scope) ||
      !precedes(entry, cut) ||
      cut.at >= after ||
      !cut.obligations.some(
        (held) => held.entry === entry.id && held.originatingEpisode === bounce.index
      )
    )
      continue
    const loss = evidence.retirements.find(
      (event) =>
        interrupts('retirement', event, scope) &&
        !precedes(event, cut) &&
        event.at <= bounce.restoredAt + MEMBERSHIP_QUIET_MS &&
        event.at < after
    )
    if (loss) return { cut, loss }
  }
  return null
}

/**
 * @param {object} bounce Original interruption snapshots.
 * @returns {object[]} Immutable new attempts; copies of earlier admissions are excluded.
 */
const newObligations = (bounce) => [
  ...new Map(
    (bounce.interruptions ?? [])
      .flatMap(({ obligations }) => obligations)
      .filter(({ originatingEpisode }) => originatingEpisode === bounce.index)
      .map((held) => [held.entry, held])
  ).values(),
]

/**
 * @param {object[]} bounces Independently reconstructed or actively captured admissions.
 * @returns {object} Separate new, carried and eligible identity budgets with attempt references.
 */
const admissionAccounting = (bounces) => {
  const problems = []
  const admitted = new Map()
  const union = new Set()
  const effectiveUnion = new Set()
  const episodes = bounces.map((bounce, position) => {
    const newly = newObligations(bounce)
    const newIdentities = new Set(newly.map(({ identity }) => identity))
    const carry = [
      ...new Map(
        [bounce, ...(bounce.interruptions ?? [])]
          .flatMap(({ carried }) => carried ?? [])
          .map((held) => [held.entry, held])
      ).values(),
    ]
    const carriedIdentities = new Set(carry.map(({ identity }) => identity))
    const effective = effectiveObligations(bounce)
    for (const identity of effective.keys()) effectiveUnion.add(identity)
    if (bounce.index !== position || position >= BOUNCE_COUNT)
      problems.push('Fault admission requires one of the six original episodes')
    for (const held of carry) {
      const original = admitted.get(held.entry)
      if (!original || JSON.stringify(original) !== JSON.stringify(held))
        problems.push(`bounce ${bounce.index} has unattributed carry`)
    }
    for (const held of newly) {
      if (admitted.has(held.entry))
        problems.push(`bounce ${bounce.index} admits an original attempt twice`)
      admitted.set(held.entry, held)
      union.add(held.identity)
    }
    if (newIdentities.size > MAX_HELD)
      problems.push(`bounce ${bounce.index} new uncertainty exceeds its allowance`)
    if (
      carriedIdentities.size > MAX_HELD * position ||
      effective.size > newIdentities.size + carriedIdentities.size ||
      effective.size > MAX_HELD * (position + 1)
    )
      problems.push(`bounce ${bounce.index} combined freeze exceeds its allowance`)
    return {
      index: bounce.index,
      newCount: newIdentities.size,
      newIdentities: [...newIdentities],
      admissions: newly,
      carriedCount: carriedIdentities.size,
      carried: carry,
      frozenCount: effective.size,
      frozenIdentities: [...effective.keys()],
    }
  })
  if (
    episodes.reduce((sum, { newCount }) => sum + newCount, 0) > MAX_REPLAY ||
    union.size > MAX_REPLAY ||
    JSON.stringify([...union].sort()) !== JSON.stringify([...effectiveUnion].sort())
  )
    problems.push('Combined interruption union exceeds its allowance')
  return { episodes, eligibleIdentities: [...union], problems }
}

/**
 * Decomposes complete unresolved history without treating retired unknowns as live pending work.
 * @param {object[]} entries Original handler journal, including retired attempts.
 * @param {object[]} bounces Admissions captured before or at this observation.
 * @param {BounceEvidence} evidence Primitive loss and outcome journals.
 * @param {object} cut Original observation boundary.
 * @returns {object} Live/history/admitted counts and unattributed attempts with original loss evidence.
 */
const historyAccounting = (entries, bounces, evidence, cut) => {
  const history = unresolvedAt(entries, evidence, cut)
  const pending = history.filter(
    (entry) =>
      !scopedLosses(authority(entry), evidence).some(
        ({ kind, event }) =>
          precedes(interruptionEvent(kind, event), cut) && interrupts(kind, event, authority(entry))
      ) &&
      !evidence.results(entry).some((outcome) => outcome.type !== 'renew' && precedes(outcome, cut))
  )
  const pendingIds = new Set(pending.map(({ id }) => id))
  const admissions = new Map(
    bounces.flatMap((bounce) =>
      (bounce.interruptions ?? [])
        .filter((snapshot) => !precedes(cut, snapshot))
        .flatMap(({ obligations }) => obligations)
        .filter(({ originatingEpisode }) => originatingEpisode === bounce.index)
        .map((held) => [held.entry, held])
    )
  )
  const retained = history.filter(({ id }) => admissions.has(id))
  const liveIdentities = new Set(pending.map(({ identity }) => identity))
  const retainedIdentities = new Set(retained.map(({ identity }) => identity))
  const historyIdentities = new Set(history.map(({ identity }) => identity))
  const problems = []
  const unattributed = history
    .filter(({ id }) => !pendingIds.has(id) && !admissions.has(id))
    .map((entry) => ({
      entry: entry.id,
      identity: entry.identity,
      cohort: entry.acquisition?.id,
      authority: authority(entry),
      handler: { at: entry.at, order: entry.order },
      admissionSnapshots: bounces.flatMap((bounce) =>
        (bounce.interruptions ?? []).flatMap((snapshot) =>
          snapshot.obligations
            .filter((held) => held.entry === entry.id)
            .map((held) => ({
              bounce: bounce.index,
              at: snapshot.at,
              order: snapshot.order,
              kind: snapshot.kind,
              event: snapshot.event,
              obligation: held,
            }))
        )
      ),
      interruptions: scopedLosses(authority(entry), evidence)
        .filter(
          ({ kind, event }) =>
            precedes(interruptionEvent(kind, event), cut) &&
            interrupts(kind, event, authority(entry))
        )
        .map(({ kind, event }) => ({
          kind,
          event: event.id,
          ...interruptionEvent(kind, event),
          reason: event.reason,
          cause: event.cause,
          error: event.error,
        })),
      retirements: evidence.retirements.filter(
        (event) =>
          precedes(event, cut) &&
          event.nodeId === entry.acquisition?.source?.nodeId &&
          event.groupId === entry.groupId &&
          event.memberId === entry.memberId &&
          event.open === entry.acquisition?.open &&
          (!event.key || event.key === `${entry.topicId}:${entry.partition}`)
      ),
      outcomes: evidence.results(entry).filter((outcome) => precedes(outcome, cut)),
    }))
  if (unattributed.length) problems.push('Unresolved retired history has no original admission')
  if (liveIdentities.size > MAX_HELD) problems.push('Live pending work exceeds its allowance')
  if (historyIdentities.size > MAX_HELD + retainedIdentities.size)
    problems.push('Unresolved history exceeds its live plus admitted allowance')
  return {
    at: cut.at,
    order: cut.order,
    liveCount: liveIdentities.size,
    liveEntries: pending.map(({ id }) => id),
    retainedCount: retainedIdentities.size,
    retained: retained.map(({ id }) => admissions.get(id)),
    historyCount: historyIdentities.size,
    history: history.map(({ id }) => id),
    unattributed,
    problems,
  }
}

/**
 * Rebuilds admissions and ownership solely from command, loss, handler and receipt observations.
 * Supplied freezes and originatingEpisode fields cannot create or remove a qualifying scope.
 * @param {object[]} entries Original handler journal.
 * @param {object[]} bounces Archived episode restoration boundaries.
 * @param {BounceEvidence} evidence Primitive observations.
 * @returns {Generator} Steps returning reconstructed command and first-interruption snapshots.
 */
const reconstructFreezes = function* (entries, bounces, evidence) {
  const reconstructed = []
  const journal = yield* sortedAuditWork(
    [
      ...interruptionJournal(evidence),
      ...evidence.commands.map((event) => ({ kind: 'command', event })),
    ],
    (a, b) => {
      const first = interruptionEvent(a.kind, a.event)
      const second = interruptionEvent(b.kind, b.event)
      return first.at - second.at || first.order - second.order
    }
  )
  let primitiveCount = 0
  for (const { kind, event } of journal) {
    if (primitiveCount++ % 100 === 0) yield
    const cut = interruptionEvent(kind, event)
    if (kind === 'command') {
      const history = unresolvedAt(entries, evidence, cut)
      const bounce = {
        ...event,
        held: new Set(history.map(({ identity }) => identity)),
        obligations: history.map((entry) =>
          obligation(entry, reconstructed, event.index, evidence, cut)
        ),
        carried: carriedAt(reconstructed, event, history),
        interruptions: [],
        restoredAt: bounces.find(({ index }) => index === event.index)?.restoredAt,
      }
      reconstructed.push(bounce)
      continue
    }
    const bounce = reconstructed.findLast(
      (episode) => precedes(episode, cut) && cut.at <= (episode.restoredAt ?? Infinity)
    )
    if (!bounce) continue
    for (const scope of interruptedScopes(entries, evidence, kind, event, bounce)) {
      if (
        scope.nodeId !== bounce.nodeId ||
        bounce.interruptions.some((prior) => samePartition(prior.scope, scope))
      )
        continue
      const history = unresolvedAt(entries, evidence, cut)
      const scoped = history.filter((entry) => sameAuthority(authority(entry), scope))
      bounce.interruptions.push({
        ...cut,
        kind,
        event: event.id,
        scope,
        obligations: scoped.map((entry) =>
          obligation(entry, reconstructed, bounce.index, evidence, cut, true)
        ),
        carried: carriedAt(reconstructed, bounce, history),
        lateEntries: scoped.filter((entry) => precedes(bounce, entry)).map(({ id }) => id),
        history: history.map(({ id }) => id),
      })
    }
  }
  return reconstructed
}

/**
 * Recomputes both freezes and budgets; final settlement cannot rewrite membership or ownership.
 * @param {object[]} entries All original handlers.
 * @param {object[]} bounces Archived command and interruption snapshots.
 * @param {BounceEvidence} evidence Primitive observations and actual receipts.
 * @returns {Generator} Steps returning separate budgets, reconstructed cuts and disagreements.
 */
const freezeAccountingWork = function* (entries, bounces, evidence) {
  if (!evidence.auditIndex) evidence = yield* indexedEvidenceWork(entries, evidence, bounces)
  entries = evidence.auditEntries
  const reconstructed = yield* reconstructFreezes(entries, bounces, evidence)
  const accounting = admissionAccounting(reconstructed)
  const problems = [...accounting.problems]
  const observations = []
  if (reconstructed.length !== bounces.length) problems.push('Command journal length mismatch')
  for (const [position, bounce] of reconstructed.entries()) {
    yield
    const supplied = bounces[position]
    if (
      !supplied ||
      ['index', 'at', 'order', 'nodeId', 'kind', 'service'].some(
        (key) => bounce[key] !== supplied[key]
      )
    ) {
      problems.push(`bounce ${bounce.index} command observation changed`)
      continue
    }
    if (
      JSON.stringify(bounce.obligations) !== JSON.stringify(supplied.obligations) ||
      JSON.stringify([...bounce.held].sort()) !== JSON.stringify([...supplied.held].sort()) ||
      JSON.stringify(bounce.carried) !== JSON.stringify(supplied.carried)
    )
      problems.push(`bounce ${bounce.index} command snapshot disagrees with the journal`)
    if (JSON.stringify(bounce.interruptions) !== JSON.stringify(supplied.interruptions))
      problems.push(`bounce ${bounce.index} interruption snapshot disagrees with the journal`)
    const early = historyAccounting(entries, reconstructed, evidence, bounce)
    observations.push({ bounce: bounce.index, phase: 'command', ...early })
    problems.push(...early.problems)
    if (early.historyCount > MAX_HELD * (position + 1))
      problems.push(`bounce ${bounce.index} command history exceeds its allowance`)
    for (const cut of bounce.interruptions) {
      const relevant = scopedLosses(cut.scope, evidence).filter(
        ({ kind, event }) =>
          interrupts(kind, event, cut.scope) && precedes(bounce, interruptionEvent(kind, event))
      )
      if (
        relevant.some(({ kind, event }) => {
          const stamp = interruptionEvent(kind, event)
          return !(kind === cut.kind && event.id === cut.event) && !precedes(cut, stamp)
        })
      )
        problems.push(`bounce ${bounce.index} lacks its first original interruption`)
      const history = historyAccounting(entries, reconstructed, evidence, cut)
      observations.push({
        bounce: bounce.index,
        phase: 'interruption',
        scope: cut.scope,
        ...history,
      })
      problems.push(...history.problems)
    }
  }
  return { ...accounting, observations, problems }
}

/**
 * Drains a read-only audit synchronously for deterministic scripted controls.
 * @param {Generator} work Audit steps.
 * @returns {*} Final audit result.
 */
const finishAudit = (work) => {
  let step
  do {
    step = work.next()
  } while (!step.done)
  return step.value
}

/**
 * Lets live heartbeats and shutdown traffic run between read-only audit steps.
 * @param {Generator} work The same audit used by scripted controls.
 * @param {AbortSignal} signal Existing body scope, never extended.
 * @returns {Promise<object>} Final audit result.
 */
const cooperativeAudit = async (work, signal) => {
  try {
    for (;;) {
      signal.throwIfAborted()
      const step = work.next()
      if (step.done) return step.value
      await new Promise((resolve) => setImmediate(resolve))
    }
  } finally {
    work.return()
  }
}

/**
 * @param {object[]} entries Original handlers.
 * @param {object[]} bounces Original episodes.
 * @param {BounceEvidence} evidence Original journals.
 * @returns {object} Complete frozen accounting.
 */
const freezeAccounting = (entries, bounces, evidence) =>
  finishAudit(freezeAccountingWork(entries, bounces, evidence))

/**
 * @param {object[]} entries All original handlers.
 * @param {object[]} bounces Archived freezes.
 * @param {BounceEvidence} evidence Independent primitive journals.
 * @returns {string[]} Snapshot disagreement and budget failures.
 */
const freezeProblems = (entries, bounces, evidence) =>
  freezeAccounting(entries, bounces, evidence).problems

const epochSampleCache = new WeakMap()

/** @param {object[]} samples Original Metadata membership. @returns {Generator} Cooperatively built source/partition index. */
const epochSampleIndexWork = function* (samples) {
  let cached = epochSampleCache.get(samples)
  if (!cached || cached.length !== samples.length) {
    cached = { length: samples.length, rows: new Map() }
    let visited = 0
    for (const sample of samples) {
      if (visited++ % 100 === 0) yield
      if (sample.frames.length !== 1) continue
      const frame = sample.frames[0]
      if (frame.apiKey !== 3 || frame.apiVersion < 7) continue
      for (const topic of sample.response.topicMetadata) {
        if (
          sample.response.topicMetadata.filter(({ topicId }) => topicId === topic.topicId)
            .length !== 1
        )
          continue
        for (const row of topic.partitionMetadata) {
          if (
            topic.partitionMetadata.filter(({ partitionId }) => partitionId === row.partitionId)
              .length !== 1
          )
            continue
          if (
            row.partitionErrorCode !== 0 ||
            !Number.isInteger(row.leader) ||
            row.leader < 0 ||
            !Number.isInteger(row.leaderEpoch) ||
            row.leaderEpoch < 0
          )
            continue
          indexMember(
            cached.rows,
            `${topic.topicId}:${row.partitionId}:${sample.nodeId}:${sample.pool}`,
            { sample, frame, row }
          )
        }
      }
    }
    for (const [key, rows] of cached.rows)
      cached.rows.set(key, yield* sortedAuditWork(rows, (a, b) => a.frame.at - b.frame.at))
    epochSampleCache.set(samples, cached)
  }
  return cached
}

/**
 * @param {object} entry Original handler acquisition.
 * @param {object[]} samples Original Metadata responses and their wire frames.
 * @param {boolean} [requireBytes] Whether only byte-backed samples can supply a witness.
 * @returns {object[]} Successful metadata observations from this acquisition's source node/pool.
 */
const sourceEpochSamples = (entry, samples, requireBytes = true) => {
  let cached = epochSampleCache.get(samples)
  if (!cached || cached.length !== samples.length)
    cached = finishAudit(epochSampleIndexWork(samples))
  const rows = entry.acquisition?.source
    ? (cached.rows.get(
        `${entry.topicId}:${entry.partition}:${entry.acquisition.source.nodeId}:${entry.acquisition.pool}`
      ) ?? [])
    : []
  return requireBytes ? rows.filter(({ frame }) => frame.responseBytes) : rows
}

/**
 * @param {object} entry Original handler acquisition.
 * @param {object[]} samples Original Metadata responses and their wire frames.
 * @returns {object[]} Source-node local observations identifying that node as leader.
 */
const ownEpochSamples = (entry, samples) =>
  sourceEpochSamples(entry, samples).filter(({ sample, row }) => row.leader === sample.nodeId)

/** @param {object[]} observations Transition rows. @returns {object[]} Epoch index with latest-write suffixes. */
const transitionIndex = (observations) => {
  const rows = [...observations].sort((a, b) => a.row.leaderEpoch - b.row.leaderEpoch)
  let latest = null
  for (let index = rows.length - 1; index >= 0; index--) {
    if (!latest || rows[index].frame.at > latest.frame.at) latest = rows[index]
    rows[index] = { ...rows[index], latest }
  }
  return rows
}

/** @param {object[]} rows Indexed transitions. @param {object} before Upper sample. @returns {object|null} Later higher-epoch transition. */
const transitionAfter = (rows, before) => {
  let low = 0
  let high = rows.length
  while (low < high) {
    const mid = (low + high) >>> 1
    if (rows[mid].row.leaderEpoch <= before.row.leaderEpoch) low = mid + 1
    else high = mid
  }
  const candidate = rows[low]?.latest
  return candidate && before.frame.responseAt < candidate.frame.at ? candidate : null
}

/**
 * Proves disjoint serving-epoch bounds using source-node local epoch observations and down barriers.
 * A source must itself be the observed leader: a remote cache, batch epoch or CurrentLeader default
 * supplies no bound. Same-node epochs never decrease; samples bracket the original wire intervals.
 * A post-command request and a completed down action bracket the old leader's service before a
 * different leader's later lower bound. Other late samples still need the original transition trace.
 * @param {object} old Previous handler acquisition.
 * @param {object} next New handler acquisition.
 * @param {object} bounce Designated outage.
 * @param {object[]} samples Original Metadata responses and their wire frames.
 * @param {object[]} exchanges Original exchanges, including leadership-error answers.
 * @returns {object|null} Source references and proven epoch bounds, or no proof.
 */
const epochAdvance = (old, next, bounce, samples, exchanges) => {
  if (!Number.isFinite(bounce.downAt) || bounce.downAt <= bounce.at) return null
  const upperSamples = ownEpochSamples(old, samples).filter(
    ({ frame }) =>
      old.acquisition.source.frames[0].responseAt < frame.at && frame.responseAt < bounce.downAt
  )
  const lowerSamples = ownEpochSamples(next, samples).filter(
    ({ frame }) =>
      bounce.downAt < frame.at && frame.responseAt < next.acquisition.source.frames[0].at
  )
  const after = lowerSamples.reduce(
    (best, sample) => (!best || sample.row.leaderEpoch > best.row.leaderEpoch ? sample : best),
    null
  )
  if (!after) return null
  const transitions = sourceEpochSamples(old, samples).filter(
    ({ frame, row }) =>
      frame.endpoint === old.acquisition.source.endpoint &&
      bounce.at < frame.at &&
      frame.at < frame.responseAt &&
      frame.responseAt < bounce.downAt &&
      frame.responseAt < after.frame.at &&
      row.leader !== bounce.nodeId &&
      row.leaderEpoch <= after.row.leaderEpoch &&
      (row.leader === after.row.leader || row.leaderEpoch < after.row.leaderEpoch)
  )
  const errors = exchanges.flatMap((exchange) => {
    const frame = exchange.frames[0]
    const topics =
      exchange.response?.responses.filter(({ topicId }) => topicId === old.topicId) ?? []
    const rows =
      topics.length === 1
        ? topics[0].partitions.filter(({ partitionIndex }) => partitionIndex === old.partition)
        : []
    return exchange.frames.length === 1 &&
      frame.apiKey === 78 &&
      frame.apiVersion === 2 &&
      frame.responseBytes &&
      exchange.response?.errorCode === 0 &&
      exchange.groupId === old.groupId &&
      exchange.memberId === old.memberId &&
      frame.request?.groupId === old.groupId &&
      frame.request?.memberId === old.memberId &&
      frame.at < frame.responseAt &&
      exchange.nodeId === bounce.nodeId &&
      exchange.open === old.acquisition.open &&
      exchange.pool === old.acquisition.pool &&
      exchange.generation === old.acquisition.generation &&
      frame.socket === old.acquisition.source.frames[0].socket &&
      frame.endpoint === old.acquisition.source.endpoint &&
      bounce.at < frame.at &&
      frame.responseAt < bounce.downAt &&
      frame.responseAt < after.frame.at &&
      rows.length === 1 &&
      [6, 74].includes(rows[0].errorCode) &&
      rows[0].currentLeader?.leaderId === after.row.leader &&
      Number.isInteger(rows[0].currentLeader?.leaderEpoch) &&
      rows[0].currentLeader.leaderEpoch <= after.row.leaderEpoch
      ? [{ frame, row: { leaderEpoch: rows[0].currentLeader.leaderEpoch } }]
      : []
  })
  const metadataIndex = transitionIndex(transitions)
  const errorIndex = transitionIndex(errors)
  for (const before of upperSamples) {
    if (before.row.leader !== bounce.nodeId || after.row.leaderEpoch <= before.row.leaderEpoch)
      continue
    const transition = transitionAfter(errorIndex, before)
    const metadataTransition = transitionAfter(metadataIndex, before)
    const downBarrier =
      ['stop', 'kill'].includes(bounce.kind) &&
      bounce.at < old.acquisition.source.frames[0].at &&
      before.row.leader !== after.row.leader
    if (before.frame.responseAt < bounce.at || transition || metadataTransition || downBarrier)
      return {
        upper: before.row.leaderEpoch,
        lower: after.row.leaderEpoch,
        before: before.frame.id,
        after: after.frame.id,
        bounce: bounce.index,
        transition: transition?.frame.id,
        metadataTransition: metadataTransition?.frame.id,
        downBarrier: downBarrier ? bounce.downAt : undefined,
      }
  }
  return null
}

/**
 * Rejects conflicting leaders at one retained partition epoch, independent of sampling brackets.
 * @param {object} entry Partition identity.
 * @param {object[]} samples Original Metadata facts.
 * @returns {boolean} Whether the retained observations agree.
 */
const consistentEpochs = (entry, samples) => {
  sourceEpochSamples(entry, samples)
  const cached = epochSampleCache.get(samples)
  const key = `${entry.topicId}:${entry.partition}`
  if (cached?.consistency?.has(key)) return cached.consistency.get(key)
  const leaders = new Map()
  let consistent = true
  for (const sample of samples) {
    const frame = sample.frames[0]
    if (sample.frames.length !== 1 || frame.apiKey !== 3 || !frame.responseBytes) continue
    for (const topic of sample.response.topicMetadata) {
      if (topic.topicId !== entry.topicId) continue
      for (const row of topic.partitionMetadata) {
        if (
          row.partitionId !== entry.partition ||
          row.partitionErrorCode !== 0 ||
          !Number.isInteger(row.leaderEpoch) ||
          row.leaderEpoch < 0 ||
          !Number.isInteger(row.leader) ||
          row.leader < 0
        )
          continue
        if (leaders.has(row.leaderEpoch) && leaders.get(row.leaderEpoch) !== row.leader)
          consistent = false
        leaders.set(row.leaderEpoch, row.leader)
      }
    }
  }
  if (cached) {
    cached.consistency ??= new Map()
    cached.consistency.set(key, consistent)
  }
  return consistent
}

/** @param {*} result Runner result. @returns {boolean} Whether actual command output is available. */
const commandResult = (result) =>
  result !== null &&
  typeof result === 'object' &&
  typeof result.stdout === 'string' &&
  typeof result.stderr === 'string'

/** @param {*} count Original broker count. @returns {boolean} Valid positive short. */
const validCount = (count) => Number.isInteger(count) && count >= 1 && count <= 32767

/**
 * Reconstructs one incarnation boundary from original controls and successful partition service.
 * Process names identify control intervals, never an inferred Kafka leader epoch or OS PID.
 * @param {object} old Previous handler.
 * @param {object} next Adjacent handler of the same identity.
 * @param {object} bounce Original episode.
 * @param {BounceEvidence} evidence Primitive journals.
 * @returns {{witness:object|null, failed:string[]}} One complete alternative or its missing components.
 */
const changedIncarnation = (old, next, bounce, evidence) => {
  const cache = evidence.auditIndex?.boundaries
  const key = `${old.acquisition.id}:${next.acquisition.id}:${bounce.index}:${old.topicId}:${old.partition}`
  if (cache?.has(key)) return cache.get(key)
  const result = incarnationWitness(old, next, bounce, evidence)
  cache?.set(key, result)
  return result
}

/** @param {object} old Previous handler. @param {object} next Adjacent handler. @param {object} bounce Episode. @param {object} evidence Journals. @returns {object} Complete witness or failed components. */
const incarnationWitness = (old, next, bounce, evidence) => {
  if (!consistentEpochs(old, evidence.epochs))
    return { witness: null, failed: ['partition-observations: conflicting leaders at one epoch'] }
  const failed = []
  const before = old.acquisition.source.frames[0]
  const after = next.acquisition.source.frames[0]
  const interruption = matchingInterruption(old, bounce, evidence, after.at)
  const controls = evidence.controls ?? []
  const down = controls.find(
    (control) => control.episode === bounce.index && control.kind !== 'start'
  )
  const command = evidence.commands.find(({ index }) => index === bounce.index)
  const mapping = evidence.owner.lifecycle?.[bounce.service]
  const targeted =
    down &&
    command &&
    ['stop', 'kill'].includes(down.kind) &&
    down.kind === bounce.kind &&
    down.service === bounce.service &&
    down.nodeId === bounce.nodeId &&
    down.invoked &&
    down.settled &&
    down.invoked.at === command.at &&
    down.invoked.order === command.order &&
    down.semantics === 'awaited-compose-service-process' &&
    mapping &&
    isDeepStrictEqual(mapping, down.mapping) &&
    down.mapping?.containerId &&
    ['', 'no'].includes(down.mapping.restartPolicy) &&
    down.mapping.service === down.service &&
    down.mapping.nodeId === down.nodeId &&
    down.mapping.endpoint === before.endpoint &&
    old.acquisition.source.nodeId === down.nodeId &&
    down.status === 'fulfilled' &&
    down.exitCode === 0 &&
    commandResult(down.result) &&
    precedes(down.invoked, down.settled)
  const ordered =
    interruption &&
    precedes({ at: before.responseAt, order: before.responseOrder }, after) &&
    precedes(interruption.cut, after)
  if (!targeted) failed.push('control: missing successful stop/kill of the original serving node')
  if (!ordered)
    failed.push('chronology: old response and original interruption must precede new write')
  if (targeted && ordered && next.acquisition.source.nodeId !== down.nodeId)
    return {
      witness: {
        kind: 'different-serving-nodes',
        down: down.id,
        oldFrame: before.id,
        newFrame: after.id,
        cut: { kind: interruption.cut.kind, event: interruption.cut.event },
      },
      failed: [],
    }
  failed.push('different-serving-nodes: no ordered successful service at another node')
  const start = controls.find(
    (control) => control.kind === 'start' && control.episode === bounce.index
  )
  const priorDown = controls.findLast(
    (control) =>
      control.service === bounce.service && control.kind !== 'start' && control.id < down?.id
  )
  const priorStart =
    priorDown &&
    controls.find(
      (control) =>
        control.kind === 'start' &&
        control.replaces === priorDown.id &&
        control.status === 'fulfilled' &&
        control.exitCode === 0 &&
        commandResult(control.result) &&
        isDeepStrictEqual(control.mapping, mapping)
    )
  const originalProcess =
    !priorDown ||
    (priorDown.status === 'fulfilled' &&
      priorDown.settled &&
      priorStart?.settled &&
      before.socketOpened &&
      precedes(priorDown.settled, before.socketOpened) &&
      precedes(priorStart.settled, down.invoked))
  const replacement =
    targeted &&
    originalProcess &&
    start &&
    start.invoked &&
    start.settled &&
    start.replaces === down.id &&
    start.semantics === down.semantics &&
    start.status === 'fulfilled' &&
    start.exitCode === 0 &&
    commandResult(start.result) &&
    isDeepStrictEqual(start.mapping, down.mapping) &&
    precedes(down.settled, start.invoked) &&
    precedes(start.invoked, start.settled) &&
    precedes(start.settled, after) &&
    Number.isInteger(down.mapping.process?.pid) &&
    down.mapping.process.pid > 0 &&
    typeof down.mapping.process.startedAt === 'string' &&
    down.mapping.process.startedAt.length > 0 &&
    before.socketOpened &&
    down.mapping.observed &&
    precedes(down.mapping.observed, before.socketOpened) &&
    precedes(before.socketOpened, down.settled) &&
    before.responseAt < down.settled.at &&
    after.socketOpened &&
    precedes(down.settled, after.socketOpened) &&
    precedes(after.socketOpened, after) &&
    before.socket !== after.socket &&
    after.endpoint === start.mapping.endpoint &&
    next.acquisition.source.nodeId === start.nodeId &&
    start.service === down.service &&
    start.nodeId === down.nodeId &&
    !controls.some(
      (control) =>
        control.id !== down.id &&
        control.id !== start.id &&
        control.service === down.service &&
        precedes(down.invoked, control.invoked) &&
        precedes(control.invoked, after)
    )
  if (ordered && replacement)
    return {
      witness: {
        kind: 'serving-process-restarted',
        down: down.id,
        start: start.id,
        oldFrame: before.id,
        newFrame: after.id,
        oldSocket: before.socket,
        newSocket: after.socket,
        oldProcessStart: priorStart?.id ?? null,
        cut: { kind: interruption.cut.kind, event: interruption.cut.event },
      },
      failed: [],
    }
  failed.push(
    'lifecycle: missing termination/replacement mapping or original/replacement socket order'
  )
  const numeric = epochAdvance(old, next, bounce, evidence.epochs, evidence.exchanges)
  if (numeric) return { witness: { kind: 'partition-transition', ...numeric }, failed: [] }
  failed.push('partition-transition: missing disjoint serving-epoch bounds or causal transition')
  return { witness: null, failed }
}

/**
 * @param {object} entry Failing replay.
 * @param {object[]} entries Complete original handler journal.
 * @param {object} bounce Causally matched command and interruption observations.
 * @param {object} evidence Original epoch and exchange journals.
 * @param {string[]} failed Missing witness components.
 * @returns {string} Identity, every delivery interval and the nearest usable or overlapping epochs.
 */
const countProgressionProblem = (entry, entries, bounce, evidence, failed) => {
  const deliveries = (
    evidence.auditIndex?.deliveries.get(entry.identity) ??
    entries.filter(({ identity }) => identity === entry.identity)
  ).map((delivery) => {
    const acquisition = delivery.acquisition ?? {}
    const source = acquisition.source ?? {}
    const frame = source.frames?.[0] ?? {}
    const samples = ownEpochSamples(delivery, evidence.epochs).sort(
      (a, b) => a.frame.at - b.frame.at
    )
    const fact = (observation) =>
      observation
        ? {
            frame: observation.frame.id,
            apiVersion: observation.frame.apiVersion,
            nodeId: observation.sample.nodeId,
            pool: observation.sample.pool,
            socket: observation.frame.socket,
            correlationId: observation.frame.correlationId,
            requestAt: observation.frame.at,
            responseAt: observation.frame.responseAt,
            leaderId: observation.row.leader,
            leaderEpoch: observation.row.leaderEpoch,
          }
        : null
    const overlapping = samples.filter(
      ({ frame: sample }) => sample.at <= frame.responseAt && sample.responseAt >= frame.at
    )
    return {
      entry: delivery.id,
      handlerAt: delivery.at,
      deliveryCount: delivery.deliveryCount,
      acquisition: acquisition.id,
      acquiredRecords: acquisition.acquiredRecords,
      exchange: source.id,
      frame: frame.id,
      apiVersion: frame.apiVersion,
      correlationId: frame.correlationId,
      nodeId: source.nodeId,
      endpoint: source.endpoint,
      socket: frame.socket,
      pool: acquisition.pool,
      generation: acquisition.generation,
      open: acquisition.open,
      groupId: delivery.groupId,
      memberId: delivery.memberId,
      sessionEpoch: frame.request?.shareSessionEpoch,
      requestAt: frame.at,
      responseAt: frame.responseAt,
      adoptedAt: acquisition.adoptedAt,
      epochs: {
        latestBeforeRequest: fact(
          samples.findLast(({ frame: sample }) => sample.responseAt < frame.at)
        ),
        earliestAfterResponse: fact(
          samples.find(({ frame: sample }) => sample.at > frame.responseAt)
        ),
        overlapping: [...new Set([overlapping[0], overlapping.at(-1)])].filter(Boolean).map(fact),
        transitionsBeforeDown: sourceEpochSamples(delivery, evidence.epochs)
          .filter(
            ({ frame: sample, row }) =>
              row.leader !== source.nodeId &&
              frame.responseAt < sample.at &&
              bounce.at < sample.at &&
              sample.responseAt < bounce.downAt
          )
          .slice(0, 2)
          .map(fact),
      },
    }
  })
  return `${entry.identity} has no proved delivery-count progression: ${JSON.stringify({
    failedWitnessComponents: failed,
    topicId: entry.topicId,
    partition: entry.partition,
    bounce: {
      index: bounce.index,
      nodeId: bounce.nodeId,
      kind: bounce.kind,
      at: bounce.at,
      downAt: bounce.downAt,
      interruptions: bounce.interruptions.map(({ at, kind, event, scope }) => ({
        at,
        kind,
        event,
        scope,
      })),
    },
    deliveries,
  })}`
}

/**
 * Checks event charges against original admissions, independently of when recovery arrived.
 * @param {object[]} extras Actual extra handler events with their preceding handler references.
 * @param {object[]} episodes Independently reconstructed new pools.
 * @returns {object} Event counts and ownership, duplication and budget violations.
 */
const replayBudgets = (extras, episodes) => {
  const problems = []
  const owners = new Map(
    episodes.flatMap(({ admissions }) => admissions.map((held) => [held.entry, held]))
  )
  const charged = new Set()
  for (const extra of extras) {
    const original = owners.get(extra.originalEntry)
    if (
      !original ||
      original.originatingEpisode !== extra.bounce ||
      original.identity !== extra.identity
    )
      problems.push(`${extra.identity} has a changed original replay charge`)
    if (charged.has(extra.entry)) problems.push(`${extra.identity} charged an entry twice`)
    charged.add(extra.entry)
  }
  const perBounce = episodes.map(({ index }) => {
    const count = extras.filter(({ bounce }) => bounce === index).length
    if (count > MAX_HELD) problems.push(`bounce ${index} took ${count} extra entries`)
    return count
  })
  if (extras.length > MAX_REPLAY) problems.push(`${extras.length} extra entries in total`)
  return { perBounce, problems }
}

/**
 * @param {object[]} unknown Provisional or terminal unknown identities and supporting attempts.
 * @param {object[]} episodes Independently reconstructed new pools.
 * @returns {object} Unknown counts without renewing the allowance of carried candidates.
 */
const unknownBudgets = (unknown, episodes) => {
  const problems = []
  const owners = new Map(
    episodes.flatMap(({ admissions }) => admissions.map((held) => [held.entry, held]))
  )
  const identities = new Set()
  for (const candidate of unknown) {
    const original = owners.get(candidate.entry)
    if (
      !original ||
      original.originatingEpisode !== candidate.bounce ||
      original.identity !== candidate.identity ||
      identities.has(candidate.identity)
    )
      problems.push(`${candidate.identity} has a changed or duplicate unknown charge`)
    identities.add(candidate.identity)
  }
  const perBounce = episodes.map(
    ({ index }) => unknown.filter(({ bounce }) => bounce === index).length
  )
  if (perBounce.some((count) => count > MAX_HELD) || unknown.length > MAX_REPLAY)
    problems.push('Unknown settlement exceeds the frozen fault allowance')
  return { perBounce, problems }
}

/**
 * Recomputes once-only dispatch and independently charged replay from the original journals.
 * @param {object[]} entries All handler starts in order.
 * @param {object[]} bounces Immutable held sets, obligations and down/restore observations.
 * @param {Set<string>} corpus Immutable append identities.
 * @param {object} evidence Original epoch samples and authority retirements.
 * @returns {Generator} Steps returning charged extras, counters and all violations.
 */
const auditReplayWork = function* (entries, bounces, corpus, evidence) {
  entries = [...entries]
  if (!evidence.auditIndex) epochSampleCache.delete(evidence.epochs)
  if (!evidence.auditIndex) evidence = yield* indexedEvidenceWork(entries, evidence, bounces)
  entries = evidence.auditEntries
  const previous = new Map()
  const dispatched = new Set()
  const uncertainty = yield* freezeAccountingWork(entries, bounces, evidence)
  const problems = [...uncertainty.problems]
  const extras = []
  for (const [index, entry] of entries.entries()) {
    if (index % 100 === 0) yield
    const old = previous.get(entry.identity)
    const key = `${entry.acquisition?.source?.frames[0]?.id}:${entry.topicId}:${entry.partition}:${entry.offset}`
    if (!corpus.has(entry.identity)) problems.push(`${entry.identity} is outside the corpus`)
    if (!validAcquisition(entry))
      problems.push(`${entry.identity} lacks an adopted wire acquisition`)
    if (!validCount(entry.deliveryCount))
      problems.push(`${entry.identity} has an invalid delivery count`)
    const duplicate = dispatched.has(key)
    if (duplicate) problems.push(`${entry.identity} dispatched one acquisition twice`)
    dispatched.add(key)
    if (old) {
      const eligible =
        corpus.has(entry.identity) &&
        validAcquisition(old) &&
        validAcquisition(entry) &&
        !duplicate &&
        samePartition(old, entry) &&
        old.memberId === entry.memberId
      const bounce = bounces.find((episode) =>
        matchingInterruption(old, episode, evidence, entry.acquisition?.source?.frames[0]?.at)
      )
      const boundary =
        eligible && bounce
          ? entry.deliveryCount > old.deliveryCount
            ? { witness: null, failed: [] }
            : changedIncarnation(old, entry, bounce, evidence)
          : {
              witness: null,
              failed: ['eligibility: missing distinct successful original acquisitions'],
            }
      if (!bounce) problems.push(`${entry.identity} has no original fault obligation`)
      else if (
        !eligible ||
        !validCount(old.deliveryCount) ||
        !validCount(entry.deliveryCount) ||
        !(entry.deliveryCount > old.deliveryCount || boundary.witness)
      )
        problems.push(countProgressionProblem(entry, entries, bounce, evidence, boundary.failed))
      else if (!dispatched.has(`charged:${entry.id}`)) {
        dispatched.add(`charged:${entry.id}`)
        extras.push({
          identity: entry.identity,
          entry: entry.id,
          originalEntry: old.id,
          bounce: bounce.index,
          deliveryCount: entry.deliveryCount,
          at: entry.at,
          incarnationWitness: boundary.witness,
          epochWitness:
            boundary.witness?.kind === 'partition-transition' ? boundary.witness : undefined,
        })
      } else problems.push(`${entry.identity} charged an entry twice`)
    }
    previous.set(entry.identity, entry)
  }
  const budget = replayBudgets(extras, uncertainty.episodes)
  problems.push(...budget.problems)
  return { extras, perBounce: budget.perBounce, uncertainty, problems }
}

/**
 * @param {object[]} entries Original handlers.
 * @param {object[]} bounces Original episodes.
 * @param {Set<string>} corpus Immutable corpus.
 * @param {BounceEvidence} evidence Primitive journals.
 * @returns {object} The synchronous replay audit for scripted controls and closed workloads.
 */
const auditReplay = (entries, bounces, corpus, evidence) =>
  finishAudit(auditReplayWork(entries, bounces, corpus, evidence))

/**
 * Classifies successful and fault-unknown candidates without promoting any client outcome.
 * @param {object[]} entries Complete handler journal.
 * @param {Set<string>} corpus Exact immutable append corpus.
 * @param {object[]} bounces Frozen original obligations.
 * @param {BounceEvidence} evidence Original requests, results and authority losses.
 * @param {object} [frozen] Precomputed freeze accounting over the same sealed journals.
 * @returns {Generator<void, object, unknown>} Audit steps completing with disjoint successful/candidate sets and missing identities.
 */
const settlementCandidatesWork = function* (entries, corpus, bounces, evidence, frozen) {
  entries = [...entries]
  if (!evidence.auditIndex) epochSampleCache.delete(evidence.epochs)
  if (!evidence.auditIndex) evidence = yield* indexedEvidenceWork(entries, evidence, bounces)
  entries = evidence.auditEntries
  const uncertainty = frozen ?? (yield* freezeAccountingWork(entries, bounces, evidence))
  const problems = [...uncertainty.problems]
  const successful = new Set()
  const last = new Map()
  for (const [index, entry] of entries.entries()) {
    if (index % 100 === 0) yield
    last.set(entry.identity, entry)
    if (
      entry.acquisition &&
      evidence.results(entry).filter(({ type }) => type !== 'renew').length > 1
    )
      problems.push(`${entry.identity} has duplicate terminal outcomes for one acquisition`)
  }
  let checked = 0
  for (const entry of last.values()) {
    if (checked++ % 100 === 0) yield
    if (corpus.has(entry.identity) && validAcquisition(entry) && evidence.successful(entry))
      successful.add(entry.identity)
  }
  const unknown = []
  const missing = []
  const unresolved = []
  let visited = 0
  for (const identity of corpus) {
    if (visited++ % 100 === 0) yield
    if (successful.has(identity)) continue
    const entry = last.get(identity)
    let candidate = null
    if (entry && validAcquisition(entry))
      for (const outcome of evidence.results(entry)) {
        if (
          outcome.type !== 'accept' ||
          !outcome.typed ||
          outcome.error?.name !== 'KafkaJSShareAcknowledgementError' ||
          outcome.error?.reason !== 'session-lost' ||
          outcome.at < entry.at
        )
          continue
        for (const exchange of evidence.acceptsFor(entry, outcome)) {
          const frame = exchange.frames[0]
          if (
            frame.responseBytes ||
            exchange.cause !== outcome.cause ||
            !exchange.error ||
            exchange.failedAt > outcome.at ||
            ![
              'KafkaJSConnectionError',
              'KafkaJSConnectionClosedError',
              'ShareSessionStaleError',
            ].includes(exchange.error.name)
          )
            continue
          const bounce = bounces.find((episode) => {
            const match = matchingInterruption(entry, episode, evidence)
            if (!match) return false
            const { cut, loss } = match
            return (
              precedes(entry, frame) &&
              precedes(frame, cut) &&
              precedes(cut, outcome) &&
              frame.at <= exchange.failedAt &&
              cut.at <= exchange.failedAt &&
              exchange.failedAt <= outcome.at &&
              exchange.nodeId === episode.nodeId &&
              exchange.cause === loss.cause &&
              (cut.kind !== 'failure' || cut.event === exchange.id) &&
              (cut.kind !== 'socket' ||
                (frame.lostAt === cut.at &&
                  evidence.owner.tap.socketEvents.some(
                    (event) => event.id === cut.event && event.outstanding.includes(frame.id)
                  )))
            )
          })
          if (bounce) {
            const { kind, event, at, order, scope } = matchingInterruption(
              entry,
              bounce,
              evidence
            ).cut
            candidate = {
              identity,
              entry: entry.id,
              bounce: bounce.index,
              exchange: exchange.id,
              interruption: { kind, event, at, order, scope },
              cohort: entry.acquisition.id,
              ledger: entry.acquisition.ledger,
              request: frame.id,
              error: outcome.error,
              outcomeAt: outcome.at,
            }
          }
        }
      }
    if (candidate) unknown.push(candidate)
    else {
      missing.push(identity)
      unresolved.push({
        identity,
        entry: entry?.id,
        handlerAt: entry?.at,
        frozenEpisodes: bounces
          .filter((episode) => episode.obligations.some((held) => held.entry === entry?.id))
          .map(({ index, nodeId, at, downAt }) => ({ index, nodeId, at, downAt })),
        interruptionEpisodes: bounces.flatMap((episode) =>
          (episode.interruptions ?? [])
            .filter((cut) => cut.obligations.some((held) => held.entry === entry?.id))
            .map(({ at, order, scope, kind, event }) => ({
              bounce: episode.index,
              at,
              order,
              scope,
              kind,
              event,
            }))
        ),
        outcomes: entry?.acquisition ? evidence.results(entry) : [],
        accepts: entry?.acquisition
          ? evidence.results(entry).flatMap((outcome) =>
              evidence.acceptsFor(entry, outcome).map((exchange) => ({
                exchange: exchange.id,
                nodeId: exchange.nodeId,
                writtenAt: exchange.frames[0].at,
                lostAt: exchange.frames[0].lostAt,
                failedAt: exchange.failedAt,
              }))
            )
          : [],
      })
    }
  }
  const budget = unknownBudgets(unknown, uncertainty.episodes)
  problems.push(...budget.problems)
  return {
    successful: [...successful].sort(),
    unknown,
    missing,
    unresolved,
    perBounce: budget.perBounce,
    uncertainty,
    problems,
  }
}

/** @param {...*} args Original journals. @returns {object} Synchronous scripted settlement. */
const settlementCandidates = (...args) => finishAudit(settlementCandidatesWork(...args))

/**
 * Uses all observed acquisitions plus every potentially lost fetch as an acquisition upper bound.
 * A lost response can have acquired any identity it requested. No archive-producing decision,
 * unobserved consumer, retention or offset mutation is allowed in this isolated arm.
 * @param {object[]} entries Complete handler entries.
 * @param {object[]} exchanges Every original exchange, including lost responses.
 * @param {number[]} limits Actual readback from every broker.
 * @returns {Generator<void, string[], unknown>} Audit steps completing with unproven archival exclusions.
 */
const archivalProblemsWork = function* (entries, exchanges, limits) {
  const problems = []
  if (
    limits.length !== SERVICES.length ||
    limits.some((limit) => !Number.isInteger(limit) || limit < 1)
  )
    return ['Missing delivery-limit configuration']
  const limit = Math.min(...limits)
  const partitions = new Map()
  for (const entry of new Map(entries.map((entry) => [entry.identity, entry])).values()) {
    const key = `${entry.topicId}:${entry.partition}`
    if (!partitions.has(key)) partitions.set(key, { entries: [], events: [], lost: 0 })
    partitions.get(key).entries.push(entry)
  }
  let visited = 0
  for (const [occurrence, exchange] of exchanges.entries()) {
    if (visited++ % 100 === 0) yield
    for (const frame of exchange.frames) {
      if (
        frame.request?.topics.some(({ partitions }) =>
          partitions.some(({ acknowledgementBatches }) =>
            acknowledgementBatches.some(({ acknowledgeTypes }) =>
              acknowledgeTypes.some((type) => type === 2 || type === 3)
            )
          )
        )
      )
        problems.push(`Exchange ${exchange.id} wrote an archive/release decision`)
    }
    if (exchange.apiKey !== 78 || !exchange.frames.length) continue
    const requested = new Set(
      (exchange.model ?? []).map(({ topicId, partition }) => `${topicId}:${partition}`)
    )
    for (const topic of exchange.frames[0].request?.topics ?? [])
      for (const { partitionIndex } of topic.partitions)
        requested.add(`${topic.topicId}:${partitionIndex}`)
    if (!exchange.response) {
      for (const key of requested) if (partitions.has(key)) partitions.get(key).lost++
      continue
    }
    for (const topic of exchange.response.responses)
      for (const partition of topic.partitions) {
        const key = `${topic.topicId}:${partition.partitionIndex}`
        const state = requested.has(key) && partitions.get(key)
        if (!state) continue
        for (const range of partition.acquiredRecords ?? []) {
          if (visited++ % 100 === 0) yield
          const bad =
            !Number.isInteger(range.deliveryCount) ||
            range.deliveryCount < 1 ||
            range.deliveryCount >= limit
          state.events.push(
            { at: BigInt(range.firstOffset), occurrence, delta: 1, bad },
            { at: BigInt(range.lastOffset) + 1n, occurrence, delta: -1, bad }
          )
        }
      }
  }
  // One offset sweep replaces the former U-by-all-exchanges Cartesian join.
  for (const state of partitions.values()) {
    const compare = (a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : 0)
    const events = yield* sortedAuditWork(state.events, compare)
    const identities = yield* sortedAuditWork(
      state.entries.map((entry) => ({ at: BigInt(entry.offset), entry })),
      compare
    )
    const active = new Map()
    let bad = 0
    let position = 0
    for (const { at, entry } of identities) {
      while (position < events.length && events[position].at <= at) {
        if (visited++ % 100 === 0) yield
        const event = events[position++]
        const count = (active.get(event.occurrence) ?? 0) + event.delta
        if (count) active.set(event.occurrence, count)
        else active.delete(event.occurrence)
        if (event.bad) bad += event.delta
      }
      if (active.size + state.lost >= limit || bad)
        problems.push(`${entry.identity} may have exhausted its delivery limit`)
    }
  }
  return problems
}

/**
 * Recomputes final acceptance from original identity, receipt, terminal and verifier streams.
 * @param {object} proof Independent corpus, original journals and raw terminal observations.
 * @returns {Generator<void, object, unknown>} Audit steps completing with settlement classes and violated obligations; candidates alone never pass.
 */
const auditTerminalWork = function* ({
  corpus,
  raw,
  entries,
  bounces,
  evidence,
  limits,
  target,
  before,
  after,
  quiet,
}) {
  entries = [...entries]
  if (!evidence.auditIndex) evidence = yield* indexedEvidenceWork(entries, evidence, bounces)
  entries = evidence.auditEntries
  const identities = new Set(corpus)
  const problems = []
  if (
    identities.size !== corpus.length ||
    JSON.stringify(multisetProblems(tally(corpus), tally(raw))) !==
      JSON.stringify({ missing: [], extra: [], count: [] })
  )
    problems.push('Raw corpus mismatch')
  const handled = new Set(entries.map(({ identity }) => identity))
  if (corpus.some((identity) => !handled.has(identity))) problems.push('Missing handler identity')
  const replay = yield* auditReplayWork(entries, bounces, identities, evidence)
  problems.push(...replay.problems)
  const settlement = yield* settlementCandidatesWork(
    entries,
    identities,
    bounces,
    evidence,
    replay.uncertainty
  )
  problems.push(
    ...settlement.problems,
    ...settlement.missing.map((identity) => `Unsettled ${identity}`)
  )
  const unknown = new Set(settlement.unknown.map(({ identity }) => identity))
  problems.push(
    ...(yield* archivalProblemsWork(
      entries.filter(({ identity }) => unknown.has(identity)),
      evidence.exchanges,
      limits
    ))
  )
  for (const observation of [before, after]) {
    if (!observation || !terminalSharePosition(target, observation).settled)
      problems.push('Incomplete terminal proof')
    else if (observation.topicId !== entries[0]?.topicId || observation.groupId !== target.groupId)
      problems.push('Terminal identity mismatch')
  }
  if (
    !quiet ||
    !Number.isFinite(quiet.activeAt) ||
    quiet.endedAt - quiet.activeAt < 5000 ||
    quiet.delivered.length ||
    !quiet.assignment?.some(
      ({ topic, partitions }) => topic === target.topic && partitions.length
    ) ||
    !quiet.events.some(({ name }) => name === 'GROUP_JOIN') ||
    !quiet.events.some(
      ({ name, at, payload }) =>
        name === 'REQUEST' &&
        payload.apiName === 'ShareFetch' &&
        at >= quiet.endedAt - 5000 &&
        at <= quiet.endedAt
    )
  )
    problems.push('Inactive or nonquiet verifier')
  return { settlement, replay, problems }
}

/** @param {object} proof Original terminal proof. @returns {object} Scripted terminal audit. */
const auditTerminal = (proof) => finishAudit(auditTerminalWork(proof))

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
 * @param {Error|null} error Original public or internal error.
 * @param {number} [depth] Maximum causal depth retained.
 * @returns {object|null} Structured error, without truncating its message.
 */
const producerError = (error, depth = 8) =>
  error == null
    ? null
    : {
        ...describeError(error, 0),
        message: String(error.message),
        retryCount: error.retryCount ?? null,
        retryTime: error.retryTime ?? null,
        recordErrors: error.recordErrors ?? null,
        currentLeader: error.currentLeader ?? null,
        nodeEndpoints: error.nodeEndpoints ?? null,
        cause: depth > 0 ? producerError(error.cause, depth - 1) : { status: 'causal-depth-limit' },
      }

/** @param {Buffer|string|null} bytes Original bytes. @returns {string|null} Lossless base64. */
const originalBytes = (bytes) => (bytes == null ? null : Buffer.from(bytes).toString('base64'))

/** @param {object} message Original record. @returns {object} Ordered, lossless input fields. */
const originalMessage = ({ partition, key, value, timestamp, headers }) => ({
  partition,
  key: originalBytes(key),
  value: originalBytes(value),
  timestamp: timestamp == null ? null : String(timestamp),
  headers:
    headers === undefined
      ? null
      : Object.entries(headers).flatMap(([key, values]) =>
          (Array.isArray(values) ? values : [values]).map((value) => ({
            key: originalBytes(key),
            value: originalBytes(value),
          }))
        ),
})

/**
 * Reads magic-2 batch stamps directly from original record-set bytes, without decoding records.
 * @param {Buffer} records Complete Records field.
 * @returns {object[]} Batch boundaries, exact stamps and body hashes; bytes remain in the frame.
 * @throws {Error} An incomplete or unsupported batch.
 */
const originalBatchStamps = (records) => {
  const batches = []
  for (let position = 0; position < records.length;) {
    if (records.length - position < 61) throw new Error('Incomplete diagnostic batch header')
    const size = records.readInt32BE(position + 8) + 12
    if (size < 61 || position + size > records.length || records[position + 16] !== 2)
      throw new Error('Incomplete or non-magic-2 diagnostic batch')
    const batch = records.subarray(position, position + size)
    batches.push({
      baseOffset: batch.readBigInt64BE(0).toString(),
      leaderEpoch: batch.readInt32BE(12),
      producerId: batch.readBigInt64BE(43).toString(),
      producerEpoch: batch.readInt16BE(51),
      firstSequence: batch.readInt32BE(53),
      count: batch.readInt32BE(57),
      attributes: batch.readInt16BE(21),
      firstTimestamp: batch.readBigInt64BE(27).toString(),
      maxTimestamp: batch.readBigInt64BE(35).toString(),
      payloadHash: createHash('sha256').update(batch.subarray(21)).digest('hex'),
      lastOffsetDelta: batch.readInt32BE(23),
      bodyHash: createHash('sha256').update(batch.subarray(61)).digest('hex'),
      recordsPosition: position,
      byteLength: size,
    })
    position += size
  }
  return batches
}

/**
 * Projects original Produce requests and ShareFetch responses for diagnostic joins only.
 * @param {object} frame Archived complete wire exchange.
 * @returns {object[]} Topic/partition batch stamps, or an empty array when no response exists.
 * @throws {Error} Incomplete or unsupported evidence, never a substituted stamp.
 */
const wireBatchStamps = (frame, raw) => {
  const batches = []
  if (frame.apiKey === 0) {
    const bytes = raw ?? Buffer.from(frame.bytes, 'base64')
    const { byteLength } = readRequestHeader(bytes)
    const reader = new Decoder(bytes.subarray(byteLength))
    const flexible = frame.apiVersion >= 9
    frame.transactionalId = flexible ? reader.readUVarIntString() : reader.readString()
    frame.acks = reader.readInt16()
    frame.timeout = reader.readInt32()
    const array = (read) =>
      flexible ? reader.readUVarIntArray(() => read()) : reader.readArray(() => read())
    array(() => {
      const topic =
        frame.apiVersion >= 13 ? null : flexible ? reader.readUVarIntString() : reader.readString()
      const topicId = frame.apiVersion >= 13 ? reader.readUuid() : null
      array(() => {
        const partition = reader.readInt32()
        const records = flexible ? reader.readUVarIntBytes() : reader.readBytes()
        for (const batch of originalBatchStamps(records ?? Buffer.alloc(0)))
          batches.push({ topic, topicId, partition, ...batch })
        if (flexible) reader.readTaggedFields()
      })
      if (flexible) reader.readTaggedFields()
    })
    if (flexible) reader.readTaggedFields()
  } else if (frame.apiKey === 78 && frame.apiVersion === 2 && (raw || frame.responseBytes)) {
    const reader = new Decoder((raw ?? Buffer.from(frame.responseBytes, 'base64')).subarray(8))
    reader.readTaggedFields()
    reader.readInt32()
    const topError = reader.readInt16()
    reader.readUVarIntString()
    reader.readInt32()
    reader.readUVarIntArray(() => {
      const topicId = reader.readUuid()
      reader.readUVarIntArray(() => {
        const partition = reader.readInt32()
        const partitionError = reader.readInt16()
        reader.readUVarIntString()
        reader.readInt16()
        reader.readUVarIntString()
        reader.readInt32()
        reader.readInt32()
        reader.readTaggedFields()
        const records = reader.readUVarIntBytes()
        for (const batch of originalBatchStamps(records ?? Buffer.alloc(0)))
          batches.push({ topicId, partition, topError, partitionError, ...batch })
        reader.readUVarIntArray(() => {
          reader.readInt64()
          reader.readInt64()
          reader.readInt16()
          reader.readTaggedFields()
        })
        reader.readTaggedFields()
      })
      reader.readTaggedFields()
    })
  }
  return batches.map((batch) => ({ frame: frame.id, ...batch }))
}

/** @param {object} batch Original batch. @returns {string} Offset-independent batch identity. */
const batchStampKey = (batch) =>
  [
    batch.partition,
    batch.producerId,
    batch.producerEpoch,
    batch.firstSequence,
    batch.count,
    batch.bodyHash,
    batch.payloadHash,
  ].join(':')

/**
 * Reads partition offsets/statuses without invoking the product's result mapping.
 * @param {object} frame Original Produce request fact.
 * @param {Buffer} bytes Original response, including length and correlation.
 * @returns {object[]} Compact original partition results, including errors and leader hints.
 */
const produceResponseFacts = (frame, bytes) => {
  const reader = new Decoder(bytes.subarray(8))
  const flexible = frame.apiVersion >= 9
  if (flexible) reader.readTaggedFields()
  const array = (read) =>
    flexible ? reader.readUVarIntArray(() => read()) : reader.readArray(() => read())
  const results = []
  array(() => {
    const topic =
      frame.apiVersion >= 13 ? null : flexible ? reader.readUVarIntString() : reader.readString()
    const topicId = frame.apiVersion >= 13 ? reader.readUuid() : null
    array(() => {
      const fact = {
        topic,
        topicId,
        partition: reader.readInt32(),
        errorCode: reader.readInt16(),
        baseOffset: reader.readInt64().toString(),
        logAppendTime: reader.readInt64().toString(),
        logStartOffset: frame.apiVersion >= 5 ? reader.readInt64().toString() : null,
      }
      if (frame.apiVersion >= 8) {
        fact.recordErrors = array(() => {
          const error = {
            batchIndex: reader.readInt32(),
            message: flexible ? reader.readUVarIntString() : reader.readString(),
          }
          if (flexible) reader.readTaggedFields()
          return error
        })
        fact.errorMessage = flexible ? reader.readUVarIntString() : reader.readString()
      }
      if (flexible) {
        const hint = reader.readTaggedFields().get(0)
        if (hint) {
          const leader = new Decoder(hint)
          fact.currentLeader = { leaderId: leader.readInt32(), leaderEpoch: leader.readInt32() }
        }
      }
      results.push(fact)
    })
    if (flexible) reader.readTaggedFields()
  })
  frame.throttleTime = reader.readInt32()
  if (flexible) reader.readTaggedFields()
  return results
}

/**
 * @param {object} frame InitProducerId version/header facts.
 * @param {Buffer} bytes Complete original frame.
 * @param {boolean} response Whether this is the answer.
 * @returns {object} Original identity fields, with no record decoding or base64 copy.
 */
const initProducerFact = (frame, bytes, response) => {
  const reader = new Decoder(bytes.subarray(response ? 8 : readRequestHeader(bytes).byteLength))
  const flexible = frame.apiVersion >= 2
  if (response && flexible) reader.readTaggedFields()
  const fact = response
    ? { throttleTime: reader.readInt32(), errorCode: reader.readInt16() }
    : {
        transactionalId: flexible ? reader.readUVarIntString() : reader.readString(),
        transactionTimeout: reader.readInt32(),
      }
  if (response || frame.apiVersion >= 3) {
    fact.producerId = reader.readInt64().toString()
    fact.producerEpoch = reader.readInt16()
  }
  if (flexible) reader.readTaggedFields()
  return fact
}

/**
 * Copies only a bounded amount of evidence; never traverses or stringifies a large journal.
 * @param {*} value Original facts.
 * @param {number} limit Conservative upper bound on encoded bytes, including JSON escaping.
 * @returns {{value:*, bytes:number, truncated:boolean}} Detached projection and explicit omission flag.
 */
const boundedEvidence = (value, limit) => {
  let bytes = 0
  let truncated = false
  const copy = (item, depth = 0) => {
    if (bytes + 64 > limit || depth > 24) {
      truncated = true
      return undefined
    }
    if (item == null || typeof item !== 'object') {
      const cost = typeof item === 'string' ? item.length * 6 + 2 : 32
      if (bytes + cost > limit) {
        truncated = true
        return undefined
      }
      bytes += cost
      return typeof item === 'bigint' ? String(item) : item
    }
    bytes += 2
    const result = Array.isArray(item) ? [] : {}
    for (const key of Array.isArray(item) ? item.keys() : Object.keys(item)) {
      const cost = Array.isArray(item) ? 1 : key.length * 6 + 4
      if (bytes + cost + 64 > limit) {
        truncated = true
        break
      }
      bytes += cost
      const child = copy(item[key], depth + 1)
      if (child !== undefined) result[key] = child
      if (truncated) break
    }
    return result
  }
  return { value: copy(value), bytes, truncated }
}

/**
 * Compresses complete JSON losslessly; chunks split encoded bytes, never nested membership.
 * Manifest counts and the digest let readers reject missing, reordered or changed chunks.
 * @param {string} event Original row kind.
 * @param {string} id Stable object ID.
 * @param {object} fields Complete original facts.
 * @returns {object[]} Lossless rows below the existing row bound.
 */
const evidenceRows = (event, id, fields) => {
  const json = JSON.stringify(fields ?? {})
  if (Buffer.byteLength(json) < 20000) return [{ ...fields, event, evidenceId: id }]
  const encoded = deflateRawSync(Buffer.from(json)).toString('base64')
  const chunks = []
  for (let offset = 0; offset < encoded.length; offset += 16000)
    chunks.push(encoded.slice(offset, offset + 16000))
  return [
    {
      event,
      evidenceId: id,
      encoding: 'deflate-raw-base64-chunks',
      chunkCount: chunks.length,
      characterCount: json.length,
      encodedCharacters: encoded.length,
      byteCount: Buffer.byteLength(json),
      sha256: createHash('sha256').update(json).digest('hex'),
      membership: Object.fromEntries(
        Object.entries(fields)
          .filter(([, value]) => Array.isArray(value))
          .map(([key, value]) => [key, value.length])
      ),
    },
    ...chunks.map((data, index) => ({ event: 'evidence-chunk', evidenceId: id, index, data })),
  ]
}

/**
 * Reconstructs a chunked object, rejecting missing, reordered or damaged members.
 * @param {object[]} rows Manifest followed by original chunks.
 * @returns {object} Complete original object.
 * @throws {Error} The specific incomplete evidence component.
 */
const readEvidenceRows = (rows) => {
  const [manifest, ...chunks] = rows
  if (!manifest?.encoding) return manifest
  if (chunks.length !== manifest.chunkCount) throw new Error('chunks: missing member')
  if (chunks.some((row, index) => row.evidenceId !== manifest.evidenceId || row.index !== index))
    throw new Error('chunks: order or identity mismatch')
  const encoded = chunks.map(({ data }) => data).join('')
  if (encoded.length !== manifest.encodedCharacters) throw new Error('chunks: encoded length')
  const json = inflateRawSync(Buffer.from(encoded, 'base64')).toString()
  if (
    Buffer.byteLength(json) !== manifest.byteCount ||
    json.length !== manifest.characterCount ||
    createHash('sha256').update(json).digest('hex') !== manifest.sha256
  )
    throw new Error('chunks: content mismatch')
  const fields = JSON.parse(json)
  for (const [key, count] of Object.entries(manifest.membership))
    if (!Array.isArray(fields[key]) || fields[key].length !== count)
      throw new Error('chunks: membership mismatch')
  return fields
}

/** @param {object[]} messages Original batch membership. @returns {string} Length-delimited input hash. */
const membershipHash = (messages) => {
  const hash = createHash('sha256')
  const field = (value) => {
    const bytes = value == null ? null : Buffer.from(value)
    hash.update(`${bytes?.length ?? -1}:`)
    if (bytes) hash.update(bytes)
  }
  for (const message of messages) {
    field(message.key)
    field(message.value)
    field(String(message.timestamp))
    for (const [key, values] of Object.entries(message.headers ?? {})) {
      for (const value of Array.isArray(values) ? values : [values]) {
        field(key)
        field(value)
      }
    }
    field(null)
  }
  return hash.digest('hex')
}

/** Test-local, call-through producer observations; no batch or producer state is changed. */
class BounceProducerEvidence {
  /** @param {ShareBounceCase} owner Original workload. */
  constructor(owner) {
    this.owner = owner
    this.managers = new Map()
    this.batchIds = new WeakMap()
    this.retryCounts = new Map()
    this.batches = []
    this.transitions = []
    this.attempts = []
    this.bytes = 0
    this.problems = []
    this.causes = new AsyncLocalStorage()
  }

  /** @param {Array} stream Destination. @param {object} fact Original observation. @returns {object|null} Stored snapshot. */
  retain(stream, fact) {
    const snapshot = boundedEvidence(fact, 128 * 1024)
    if (snapshot.truncated || this.bytes + snapshot.bytes > PRODUCER_EVIDENCE_BYTES) {
      if (!this.problems.length) this.problems.push('Producer evidence exceeded its byte bound')
      return null
    }
    this.bytes += snapshot.bytes
    const copy = snapshot.value
    stream.push(copy)
    return copy
  }

  /** @param {object} batch Original batch. @returns {number|null} Stable label and original membership. */
  batchId(batch) {
    if (!this.batchIds.has(batch)) {
      const fact = this.retain(this.batches, {
        id: this.batches.length,
        productId: batch.id,
        topic: batch.topic,
        topicId: this.owner.topicId,
        partition: batch.partition,
        count: batch.messages?.length ?? 0,
        membershipHash: membershipHash(batch.messages ?? []),
        sends: this.sendIds(batch.messages ?? []),
        ...this.owner.tap.stamp(),
      })
      if (!fact) return null
      this.batchIds.set(batch, fact.id)
    }
    return this.batchIds.get(batch)
  }

  /** @param {object} batch Original batch. @returns {object} Stamp and prior transmission facts. */
  batch(batch) {
    const delivery = batch.settlers?.[batch.settlers.length - 1]
    const id = this.batchId(batch)
    this.retryCounts.set(id, Math.max(this.retryCounts.get(id) ?? 0, batch.attempt ?? 0))
    return {
      id,
      topic: batch.topic,
      partition: batch.partition,
      producerId: batch.producerId == null ? null : String(batch.producerId),
      producerEpoch: batch.producerEpoch,
      firstSequence: batch.firstSequence,
      count: batch.recordCount ?? batch.messages?.length,
      attempt: batch.attempt,
      sequenceReset: batch.sequenceReset,
      acks: batch.acks,
      timeout: batch.timeout,
      compression: batch.compression,
      createdAt: batch.createdAt,
      incarnation: batch.incarnation,
      child: this.owner.evidence.label(batch.child),
      membershipHash: this.batches[id]?.membershipHash ?? null,
      everWritten: delivery?.everWritten ?? null,
      dispatched: delivery?.dispatched ?? null,
      settled: delivery?.settled ?? null,
      routingFailures: delivery?.routingFailures ?? null,
      backoffUntil: batch.backoffUntil,
    }
  }

  /** @param {object[]} messages Batch membership. @returns {Array<number|null>} Original parent sends. */
  sendIds(messages) {
    return [
      ...new Set(
        messages.map(
          ({ key }) =>
            this.owner.sendKeys.get(createHash('sha256').update(key).digest('hex')) ?? null
        )
      ),
    ]
  }

  /** @param {object} manager Original manager. @returns {object} Current identity and partition queues. */
  state(manager) {
    return {
      producerId: String(manager.producerId),
      producerEpoch: manager.producerEpoch,
      partitions: Array.from({ length: PARTITIONS }, (_, partition) => {
        const entry = manager.sequencer.entry(this.owner.topic, partition)
        return {
          partition,
          ...(entry && {
            ...entry,
            queued: entry.queued.map((batch) => this.batchId(batch)),
            inFlight: entry.inFlight.map((batch) => this.batch(batch)),
          }),
        }
      }),
    }
  }

  /** @returns {void} Installs observations using the existing shared restore stack. */
  install() {
    const wrap = (...args) => this.owner.evidence.wrap(...args)
    wrap(ProducerStateManager.prototype, 'enqueue', (manager, original, args) => {
      if (args[0].some(({ topic }) => topic === this.owner.topic))
        this.managers.set(manager.sequencer, manager)
      return original.apply(manager, args)
    })
    for (const method of [
      'enqueue',
      'drain',
      'complete',
      'fail',
      'requeue',
      'verdict',
      'rebase',
      'restoreUnsent',
      'restartPartition',
      'markStaleExcept',
      'markAllStale',
      'resetPartition',
      'resetAll',
      'markUnresolved',
      'resolveUnresolved',
      'abortQueued',
    ]) {
      wrap(PartitionSequencer.prototype, method, (sequencer, original, args) => {
        const manager = this.managers.get(sequencer)
        if (!manager) return original.apply(sequencer, args)
        const before = this.state(manager)
        const started = this.owner.tap.stamp()
        const result = original.apply(sequencer, args)
        const after = this.state(manager)
        if (!isDeepStrictEqual(before, after) || method === 'verdict')
          this.retain(this.transitions, {
            method,
            started,
            ...this.owner.tap.stamp(),
            before,
            after,
            trigger: this.causes.getStore() ?? null,
            batch: args[0]?.topic ? this.batch(args[0]) : null,
            verdict: method === 'verdict' ? result : null,
            failure: method === 'verdict' ? args[1] : null,
            route: method === 'drain' ? { nodeId: args[0], leaders: args[1] } : null,
          })
        return result
      })
    }
    for (const method of [
      'fail',
      'resolveSequences',
      'initialize',
      'ensureProducerId',
      'reinitialize',
      'adopt',
    ]) {
      wrap(ProducerStateManager.prototype, method, (manager, original, args) => {
        if (!this.managers.has(manager.sequencer)) return original.apply(manager, args)
        const before = this.state(manager)
        const trigger = {
          method,
          batch: args[0]?.topic ? this.batch(args[0]) : null,
          error: method === 'fail' ? producerError(args[1]) : null,
          response: args[2] ?? null,
          ...this.owner.tap.stamp(),
        }
        const observe = (result, error) => {
          const after = this.state(manager)
          if (method === 'fail' || !isDeepStrictEqual(before, after) || error)
            this.retain(this.transitions, {
              ...trigger,
              ended: this.owner.tap.stamp(),
              before,
              after,
              verdict: typeof result === 'string' ? result : null,
              error: error ? producerError(error) : trigger.error,
            })
        }
        const result = this.causes.run(trigger, () => original.apply(manager, args))
        if (result?.then)
          result.then(
            (value) => observe(value),
            (error) => observe(null, error)
          )
        else observe(result)
        return result
      })
    }
    wrap(ProducerIdAndEpoch.prototype, 'bumpEpoch', (identity, original, args) => {
      const trigger = this.causes.getStore()
      if (!trigger) return original.apply(identity, args)
      const before = {
        producerId: String(identity.producerId),
        producerEpoch: identity.producerEpoch,
      }
      const started = this.owner.tap.stamp()
      const result = original.apply(identity, args)
      this.retain(this.transitions, {
        method: 'identity.bumpEpoch',
        trigger,
        started,
        ...this.owner.tap.stamp(),
        before,
        after: { producerId: String(identity.producerId), producerEpoch: identity.producerEpoch },
      })
      return result
    })
    wrap(Broker.prototype, 'produce', (broker, original, [options]) => {
      if (broker.connectionPool.clientId !== `producer-${this.owner.nonce}`)
        return original.call(broker, options)
      const partitions = options.topicData.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, firstSequence, messages }) => ({
          topic,
          topicId: options.topicIds?.get(topic) ?? this.owner.topicId,
          partition,
          firstSequence,
          count: messages.length,
          membershipHash: membershipHash(messages),
          sends: this.sendIds(messages),
        }))
      )
      const fact = {
        id: this.attempts.length,
        apiKey: 0,
        frames: [],
        nodeId: broker.nodeId,
        endpoint: broker.brokerAddress,
        pool: this.owner.evidence.label(broker.connectionPool),
        started: this.owner.tap.stamp(),
        producerId: String(options.producerId),
        producerEpoch: options.producerEpoch,
        acks: options.acks,
        timeout: options.timeout,
        compression: options.compression,
        partitions,
        batchStates: [...this.managers.values()].map((manager) => this.state(manager)),
        metadataSources: this.owner.evidence.epochs
          .slice(-Object.keys(NODES).length)
          .map((source) => ({
            nodeId: source.nodeId,
            frames: source.frames.map(({ id }) => id),
          })),
      }
      if (!this.retain([], { ...fact, frames: [] })) return original.call(broker, options)
      this.attempts.push(fact)
      const result = this.owner.tap.context.run(fact, () => original.call(broker, options))
      result.then(
        (results) => {
          fact.settlement = this.retain([], {
            results: results?.map((result) => ({ ...result, error: producerError(result.error) })),
            ...this.owner.tap.stamp(),
          })
        },
        (error) => {
          fact.settlement = this.retain([], {
            error: producerError(error),
            ...this.owner.tap.stamp(),
          })
        }
      )
      return result
    })
  }

  /** @returns {object[]} Original send links derived from immutable record membership, never offset admission. */
  links() {
    const links = this.batches.map((batch) => ({
      batch: batch.id,
      sends: batch.sends,
      attempts: [],
      retryCount: this.retryCounts.get(batch.id) ?? 0,
    }))
    for (const attempt of this.attempts) {
      const batches = new Set(
        attempt.batchStates.flatMap(({ partitions }) =>
          partitions.flatMap(({ inFlight }) =>
            (inFlight ?? [])
              .filter((batch) =>
                attempt.partitions.some(
                  (partition) =>
                    partition.topic === batch.topic &&
                    partition.partition === batch.partition &&
                    partition.firstSequence === batch.firstSequence &&
                    attempt.producerId === batch.producerId &&
                    attempt.producerEpoch === batch.producerEpoch
                )
              )
              .map(({ id }) => id)
          )
        )
      )
      for (const batch of batches) if (links[batch]) links[batch].attempts.push(attempt.id)
    }
    return links
  }
}

/**
 * One live run of the bounce scenario: a share member reads three partitions of a replication
 * factor three, min-ISR two topic while a producer appends 30000 records, and every broker is taken
 * down in turn, cleanly and then hard. The log, the broker position and the handler entries are
 * checked as three independent oracles.
 */
class ShareBounceCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.topic = shareTopicName('bounce')
    this.group = `e2e-p9e-bounce-group-${this.nonce}`
    this.stack = new CleanupStack()
    this.tap = new ShareWireTap()
    this.tap.topic = this.topic
    this.evidence = new BounceEvidence(this)
    this.producerEvidence = new BounceProducerEvidence(this)
    this.gauge = new ResourceGauge()
    this.probe = this.gauge.probe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      activeResources: true,
    })
    this.bouncer = new BrokerBouncer({ now: () => performance.now() })
    this.requests = []
    this.expected = []
    this.sends = []
    this.sendKeys = new Map()
    this.pendingSends = []
    this.appended = 0
    this.lateness = 0
    this.entries = []
    this.seen = new Set()
    this.confirmed = new Set()
    this.ackProblems = []
    this.joins = []
    this.changes = []
    this.resets = []
    this.crashes = []
    this.handlerErrors = []
    this.captureProblems = []
    this.deliveryLimits = []
    this.snapshots = []
    this.outages = []
    this.baselineMembership = null
    this.readiness = null
    this.shareState = null
    this.baseline = null
    this.beforeAdmin = null
    this.groupConfig = GROUP_CONFIG
    this.clientConfig = CLIENT_CONFIG
  }

  /**
   * @param {string} label the client label, also the start of its clientId
   * @returns {Kafka} a client whose sockets go through the tap and whose requests retry through an
   *   outage
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.factory(label),
      retry: RETRY,
    })
  }

  /**
   * Appends one evidence row.
   * @param {string} event the row kind
   * @param {object} [fields] the measured facts
   * @param {boolean} [required] Whether omission must fail completion.
   * @returns {boolean} Whether the complete object was archived.
   */
  row(event, fields = {}, required = true) {
    const file = evidencePath(EVIDENCE)
    const storage = this.archiveStorage ?? {
      size: () => statSync(file).size,
      write: (row) => writeEvidence(EVIDENCE, row),
      truncate: (size) => truncateSync(file, size),
    }
    let before = 0
    try {
      before = storage.size()
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
    const final = ['case-failed', 'case-complete'].includes(event)
    const envelope = {
      case: TITLE,
      profile: PROFILE,
      scenario: this.nonce,
      groups: { bounce: this.group },
      budgets: {
        fileMs: FILE_MS,
        caseMs: BUDGET_MS,
        bodyMs: BODY_MS,
        cleanupMs: CLEANUP_MS,
        finalMs: FINAL_MS,
      },
      wireVersions: wireVersionsOf(this.requests),
    }
    const id = `${this.nonce}:${this.archiveSequence ?? 0}`
    this.archiveSequence = (this.archiveSequence ?? 0) + 1
    const rows = evidenceRows(event, id, fields)
    const ceiling = ARCHIVE_BYTES - (final ? 0 : 2 * ARCHIVE_ROW_BYTES)
    const bytes = rows.reduce(
      (sum, fact) => sum + Buffer.byteLength(JSON.stringify({ ...envelope, ...fact })) + 4096,
      0
    )
    if (this.archiveFull || before + bytes > ceiling) {
      if (!required) return false
      this.archiveFull = true
      this.archiveIncomplete = true
      const problem = 'Required evidence does not fit the archive byte bound'
      if (!this.captureProblems.includes(problem)) this.captureProblems.push(problem)
      if (!this.archiveLimitRecorded || final) {
        storage.write({
          ...envelope,
          event: final ? 'case-failed' : 'evidence-incomplete',
          originalEvent: event,
          evidenceId: id,
          requiredBytes: bytes,
          archiveLimit: ARCHIVE_BYTES,
          error: problem,
        })
        this.archiveLimitRecorded = true
        if (storage.size() > ARCHIVE_BYTES) storage.truncate(before)
      }
      return false
    }
    for (const fact of rows) storage.write({ ...envelope, ...fact })
    if (storage.size() > ceiling) {
      storage.truncate(before)
      this.archiveFull = true
      this.archiveIncomplete = true
      this.captureProblems.push('Evidence envelope exceeded its reserved byte bound')
      return false
    }
    if (
      event === 'case-complete' &&
      performance.now() >= Math.min(this.deadline, this.finalDeadline ?? Infinity)
    ) {
      storage.truncate(before)
      throw new Error('deadline: finalization overrun')
    }
    return true
  }

  /** @returns {object} Progress and public outcomes before cleanup can change them. */
  progress() {
    return {
      appended: this.appended,
      handlerEntries: this.entries.length,
      distinctHandled: this.seen.size,
      confirmedOffsets: this.confirmed.size,
      sendFailures: this.sends.filter(({ error }) => error).length,
      crashes: this.crashes.map((error) => describeError(error)),
      handlerErrors: this.handlerErrors,
      resets: this.resets,
      acknowledgementProblems: this.ackProblems,
      outages: this.outages,
      settlement: this.settlement,
      missingHandlerIdentities: this.expected.filter((identity) => !this.seen.has(identity)),
      captureProblems: this.captureProblems,
      bounces: this.snapshots.map(({ held, ...rest }) => ({ ...rest, held: held.size })),
    }
  }

  /**
   * @param {number} limit the phase's own allowance, ms
   * @param {number} [reserve] what later phases still need, ms
   * @returns {number} the allowance, cut to what the selection has left
   * @throws {Error} when no phase time remains after reserving later work
   */
  window(limit, reserve = CLEANUP_MS + FINAL_MS) {
    const remaining = this.deadline - performance.now() - reserve
    if (remaining <= 0) throw new Error('The selection deadline has no phase time left')
    return Math.min(limit, remaining)
  }

  /**
   * Runs the whole case: setup, the concurrent produce and bounce, settlement, the three oracles
   * and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @param {number} deadline Absolute selection deadline, including the join reserve.
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope, deadline) {
    this.deadline = deadline - CASE_JOIN_MS
    this.gauge.install()
    this.evidence.install()
    this.producerEvidence.install()
    try {
      let primary
      const bodyDeadline = performance.now() + this.window(BODY_MS)
      try {
        this.beforeAdmin = await captureShareCleanupBaseline(this.probe, { signal: scope })
        this.baseline = this.beforeAdmin
        const remaining = bodyDeadline - performance.now()
        if (remaining <= 0) throw new Error('The body deadline passed during baseline capture')
        await bounded(scope, remaining, async (body) => {
          await this.setup(body)
          await bounded(body, this.window(330000, 100000), (inner) => this.drive(inner), {
            label: 'The produce and bounce',
          })
          await bounded(body, this.window(60000, 70000), (inner) => this.drain(inner), {
            label: 'The settlement',
          })
          await this.observe(body)
          await this.finish(body)
          if (performance.now() > bodyDeadline) throw new Error('The body deadline passed')
        })
      } catch (error) {
        primary = error
        this.appendAnomalies = this.diagnoseAppends()
        if (this.appendAnomalies.length && !primary.message.includes('Log/idempotency anomaly'))
          primary.message += `; Log/idempotency anomaly: ${this.appendFailure()}`
        await this.captureFailureLog(scope, bodyDeadline)
      }
      const cleanupErrors = await this.stack.closeAll({
        budgetMs: Math.max(0, Math.min(CLEANUP_MS, this.deadline - performance.now() - FINAL_MS)),
      })
      this.cleanupProof = { completed: true, tainted: cleanupErrors.length > 0 }
      if (primary) {
        primary.cleanupErrors = cleanupErrors
        throw primary
      }
      if (cleanupErrors.length) throw new AggregateError(cleanupErrors, 'Cleanup failed')
      const finalWindow = this.window(FINAL_MS, 0)
      this.finalDeadline = performance.now() + finalWindow
      await bounded(scope, finalWindow, async (inner) => {
        await cooperativeAudit(this.archiveWork(), inner)
        await this.complete(inner)
      })
    } catch (error) {
      try {
        await cooperativeAudit(this.archiveWork(true), scope)
      } catch (archiveError) {
        this.archiveIncomplete = true
        this.captureProblems.push(`Failure archive unavailable: ${archiveError.message}`)
      }
      this.row('case-failed', {
        logAnomaly: this.appendAnomalies?.length ? this.appendFailure() : null,
        error: describeError(error),
        ...this.progress(),
      })
      throw error
    } finally {
      this.evidence.restore()
      this.producerEvidence.causes.disable()
      this.gauge.uninstall()
    }
  }

  /**
   * Checks the cluster shape, creates the replication factor three topic, applies and reads back
   * the group config, and starts a producer and the member, waiting until the member and the admin
   * API agree on its membership.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} when the cluster does not match the scenario or the member never joins
   */
  async setup(scope) {
    this.assertOwnership()
    await bounded(scope, 30000, async (signal) => {
      await this.bouncer.assertAllUp()
      await this.captureLifecycle(signal)
    })
    this.admin = this.client('admin').admin()
    const removeAdmin = this.admin.on(this.admin.events.REQUEST, ({ payload }) =>
      this.requests.push(payload)
    )
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('remove admin listeners', removeAdmin)
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    this.stack.add('restore every broker', (signal) => this.restoreBrokers(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 60000, () =>
      createTopic({
        topic: this.topic,
        partitions: PARTITIONS,
        replicas: 3,
        config: [
          { name: 'min.insync.replicas', value: '2' },
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 20000, (inner) => this.readReadiness(inner))
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, this.group, this.groupConfig)
    )
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({
      idempotent: true,
      maxInFlightRequests: 5,
      retry: RETRY,
    })
    this.stack.add(
      'remove producer listeners',
      this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    )
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
    await this.startMember(scope)
    await this.awaitBaseline(scope)
  }

  /**
   * Refuses a half-set controller ownership environment before any broker is touched.
   * @returns {void}
   * @throws {Error} when only one of the guard and its manifest is named
   */
  assertOwnership() {
    const { KAFKAJS_CLUSTER_GUARD: guard, KAFKAJS_CLUSTER_MANIFEST: manifest } = process.env
    if (Boolean(guard) !== Boolean(manifest)) {
      throw new Error('The cluster guard and its manifest must be named together')
    }
    this.guarded = Boolean(guard)
  }

  /**
   * Observes the actual container mapping before the workload; controls keep their existing schedule.
   * Only stopped/started containers with no automatic restart policy can prove replacement by result.
   * @param {AbortSignal} signal Existing setup scope.
   * @returns {Promise<void>} Detached lifecycle mapping, or an explicit unavailable observation.
   */
  async captureLifecycle(signal) {
    try {
      const manifest = process.env.KAFKAJS_CLUSTER_MANIFEST
        ? JSON.parse(readFileSync(process.env.KAFKAJS_CLUSTER_MANIFEST, 'utf8'))
        : null
      const { stdout } = await promisify(execFile)('docker', ['inspect', ...SERVICES], { signal })
      const items = JSON.parse(stdout)
      this.lifecycle = Object.fromEntries(
        SERVICES.map((service) => {
          const item = items.find(({ Name }) => Name === `/${service}`)
          const node = NODES[service]
          const labels = item?.Config?.Labels ?? {}
          const environment = item?.Config?.Env ?? []
          const policy = item?.HostConfig?.RestartPolicy?.Name
          const endpoint = `${node.host}:${node.port}`
          if (
            !item?.State?.Running ||
            item.State.Paused ||
            labels['com.docker.compose.service'] !== service ||
            !environment.includes(`KAFKA_NODE_ID=${node.nodeId}`) ||
            !environment.some(
              (entry) =>
                entry.startsWith('KAFKA_ADVERTISED_LISTENERS=') && entry.includes(`://${endpoint}`)
            ) ||
            !['', 'no'].includes(policy) ||
            (manifest && !manifest.ids.includes(item.Id))
          )
            throw new Error(`Lifecycle mapping unavailable for ${service}`)
          return [
            service,
            immutable({
              service,
              nodeId: node.nodeId,
              endpoint,
              containerId: item.Id,
              project: labels['com.docker.compose.project'],
              composeFile: labels['com.docker.compose.project.config_files'],
              restartPolicy: policy,
              process: {
                pid: item.State.Pid,
                startedAt: item.State.StartedAt,
                restartCount: item.RestartCount,
              },
              observed: this.tap.stamp(),
            }),
          ]
        })
      )
    } catch (error) {
      this.lifecycleUnavailable = describeError(error)
    }
  }

  /**
   * Reads the topic and broker settings back and refuses a cluster that is not the scenario's: three
   * partitions of three replicas in sync, min-ISR two, and the base share session of 45000 ms and
   * heartbeat of 5000 ms on every node.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   * @throws {Error} naming the readiness facts when any differs
   */
  async readReadiness(scope) {
    const { resources } = await this.admin.describeConfigs({
      includeSynonyms: false,
      resources: [
        {
          type: ConfigResourceTypes.TOPIC,
          name: this.topic,
          configNames: ['min.insync.replicas', 'retention.ms', 'cleanup.policy'],
        },
        ...Object.values(NODES).map(({ nodeId }) => ({
          type: ConfigResourceTypes.BROKER,
          name: String(nodeId),
          configNames: [
            SESSION_TIMEOUT.name,
            HEARTBEAT_INTERVAL.name,
            'group.share.delivery.count.limit',
            ...PRODUCER_CONFIGS,
          ],
        })),
      ],
    })
    this.brokerConfigs = immutable({
      resources,
      ...this.tap.stamp(),
      producerState: Object.values(NODES).map(({ nodeId }) => ({
        nodeId,
        configs: PRODUCER_CONFIGS.map((name) => {
          const entry = resources
            .find(
              ({ resourceType, resourceName }) =>
                resourceType === ConfigResourceTypes.BROKER && resourceName === String(nodeId)
            )
            ?.configEntries.find(({ configName }) => configName === name)
          return (
            entry ?? {
              configName: name,
              status: 'unverified',
              reason: 'Not returned by DescribeConfigs',
            }
          )
        }),
      })),
    })
    const valueOf = (type, name, config) =>
      resources
        .find(({ resourceType, resourceName }) => resourceType === type && resourceName === name)
        ?.configEntries.find(({ configName }) => configName === config)?.configValue
    scope.throwIfAborted()
    const { topics } = await this.admin.fetchTopicMetadata({ topics: [this.topic] })
    const partitions = topics[0].partitions
    this.topicId = topics[0].topicId
    this.tap.topicId = this.topicId
    this.deliveryLimits = Object.values(NODES).map(({ nodeId }) =>
      Number(
        valueOf(ConfigResourceTypes.BROKER, String(nodeId), 'group.share.delivery.count.limit')
      )
    )
    this.readiness = {
      nodes: Object.keys(NODES).length,
      deliveryLimits: this.deliveryLimits,
      maxRecords: CLIENT_CONFIG.maxRecords,
      minIsr: Number(valueOf(ConfigResourceTypes.TOPIC, this.topic, 'min.insync.replicas')),
      retention: valueOf(ConfigResourceTypes.TOPIC, this.topic, 'retention.ms'),
      cleanupPolicy: valueOf(ConfigResourceTypes.TOPIC, this.topic, 'cleanup.policy'),
      partitions: partitions.length,
      replicas: partitions.map(({ replicas }) => replicas.length),
      isr: partitions.map(({ isr }) => isr.length),
      distinctReplicas: partitions.map(({ replicas }) => new Set(replicas).size),
      sessionMs: Object.values(NODES).map(({ nodeId }) =>
        valueOf(ConfigResourceTypes.BROKER, String(nodeId), SESSION_TIMEOUT.name)
      ),
      heartbeatMs: Object.values(NODES).map(({ nodeId }) =>
        valueOf(ConfigResourceTypes.BROKER, String(nodeId), HEARTBEAT_INTERVAL.name)
      ),
      guarded: this.guarded,
    }
    this.row('readiness', this.readiness)
    const r = this.readiness
    if (
      this.deliveryLimits.some((limit) => !Number.isInteger(limit) || limit < 1) ||
      r.minIsr !== 2 ||
      r.retention !== '-1' ||
      r.cleanupPolicy !== 'delete' ||
      r.partitions !== PARTITIONS ||
      r.replicas.some((count) => count !== 3) ||
      r.isr.some((count) => count !== 3) ||
      r.distinctReplicas.some((count) => count !== 3) ||
      r.sessionMs.some((value) => value !== SESSION_TIMEOUT.value) ||
      r.heartbeatMs.some((value) => value !== HEARTBEAT_INTERVAL.value)
    ) {
      throw new Error(`The cluster does not match the scenario: ${JSON.stringify(r)}`)
    }
  }

  /**
   * Starts the share member: every listener first, then connect, subscribe and run.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   */
  async startMember(scope) {
    const consumer = this.client('member').shareConsumer({ groupId: this.group, ...CLIENT_CONFIG })
    this.consumer = consumer
    const { events } = consumer
    const at = () => performance.now()
    const removers = [
      consumer.on(events.GROUP_JOIN, ({ payload }) =>
        this.joins.push({ at: at(), memberId: payload.memberId })
      ),
      consumer.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        this.assignment = payload.assignment
        this.changes.push({
          at: at(),
          added: payload.added,
          removed: payload.removed,
          assignment: payload.assignment,
        })
      }),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => this.onAcknowledge(payload)),
      consumer.on(events.SESSION_RESET, ({ payload }) =>
        this.resets.push({ at: at(), nodeId: payload.nodeId, reason: payload.reason })
      ),
      consumer.on(events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
      consumer.on(events.HANDLER_ERROR, ({ payload }) =>
        this.handlerErrors.push({ at: at(), topic: payload.topic, partition: payload.partition })
      ),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
    ]
    this.stack.add('remove member listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect member', () => consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const identity = shareRecordIdentity({ ...message, topic, partition })
          this.seen.add(identity)
          const acquisition = this.evidence.handlers.getStore()
          if (!acquisition) this.captureProblems.push(`Handler ${identity} has no original cohort`)
          this.entries.push({
            id: this.entries.length,
            identity,
            key: `${partition}:${message.offset}`,
            topic,
            topicId: acquisition?.topicId,
            partition,
            offset: message.offset,
            keyBytes: message.key?.toString('hex') ?? null,
            valueBytes: message.value?.toString('hex') ?? null,
            groupId: this.group,
            memberId: acquisition?.source?.memberId,
            acquisition,
            deliveryCount: message.deliveryCount,
            acquiredRangeIndex: acquisition?.acquiredRecords.findIndex(
              (range) =>
                BigInt(range.firstOffset) <= BigInt(message.offset) &&
                BigInt(range.lastOffset) >= BigInt(message.offset)
            ),
            ...this.tap.stamp(),
          })
        },
      })
    )
  }

  /**
   * Keeps what a public acknowledgement outcome says: a null-error accept confirms its offsets; every
   * other outcome, and a range too wide for a plain-record case, is kept as a problem for the
   * evidence. It never throws, because a listener's failure would be contained and lost.
   * @param {object} payload the ACKNOWLEDGE event payload
   * @returns {void}
   */
  onAcknowledge(payload) {
    const { partition, firstOffset, lastOffset, type, error } = payload
    const last = BigInt(lastOffset)
    if (type !== 'accept' || error !== null || last - BigInt(firstOffset) > 10000n) {
      this.ackProblems.push(...acknowledgementResults([payload]))
      return
    }
    for (let offset = BigInt(firstOffset); offset <= last; offset++) {
      this.confirmed.add(`${partition}:${offset}`)
    }
  }

  /**
   * Waits until the member holds every partition and the admin API reports exactly that one member
   * with the same assignment; that membership is the baseline the bounces must leave unchanged.
   * @param {AbortSignal} scope the setup scope
   * @returns {Promise<void>}
   */
  async awaitBaseline(scope) {
    await pollTolerating(
      scope,
      30000,
      async (poll) => {
        const [join] = this.joins
        if (!join || !this.assignment) return false
        const [member, ...others] = await this.describeMembers(poll)
        if (!member || others.length) return false
        const assignment = normalizeAssignment(this.assignment)
        const agreed =
          member.memberId === join.memberId && normalizeAssignment(member.assignment) === assignment
        if (agreed) this.baselineMembership = { memberId: member.memberId, assignment }
        return agreed
      },
      { label: 'The baseline membership' }
    )
    const covered = new Set(this.assignment.flatMap(({ partitions }) => partitions))
    expect([...covered].sort()).toEqual([0, 1, 2])
    this.row('assignment', { ...this.baselineMembership, joins: this.joins.length })
  }

  /**
   * @param {AbortSignal} scope the caller's scope
   * @returns {Promise<object[]>} the group's members as the admin API describes them
   */
  async describeMembers(scope) {
    const { groups } = await bounded(scope, 8000, () =>
      this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
    )
    return groups[0].members
  }

  /**
   * Runs the producer and the two rolling bounces together. Both settle before this returns; a
   * failure of either aborts the other, and the first failure is the one thrown.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the producer or the bounce
   */
  async drive(scope) {
    const own = new AbortController()
    const inner = AbortSignal.any([scope, own.signal])
    const failures = []
    const guard = (task) =>
      task.catch((error) => {
        failures.push(error)
        own.abort(error)
      })
    this.bouncer.run = this.watchDown(this.bouncer.run)
    const step = this.bouncer.step.bind(this.bouncer)
    this.bouncer.step = async (...args) => {
      try {
        const result = await step(...args)
        const frozen = this.snapshots[this.snapshots.length - 1]
        frozen.restoredAt = result.tRestored
        this.evidence.restorations.push(
          immutable({ episode: frozen.index, result, settled: this.tap.stamp() })
        )
        return result
      } catch (error) {
        this.evidence.restorations.push(
          immutable({
            episode: this.snapshots.at(-1)?.index,
            error: describeError(error),
            settled: this.tap.stamp(),
          })
        )
        throw error
      }
    }
    const driving = Promise.all([guard(this.produce(inner)), guard(this.bounce(inner))]).finally(
      () => {
        this.driveFinished = true
      }
    )
    await Promise.all([driving, guard(this.evidence.sampleEpochs(inner))])
    if (failures.length) throw failures[0]
    const failed = this.sends.filter(({ error }) => error)
    if (failed.length) {
      throw new Error(`${failed.length} sends were not acknowledged: ${JSON.stringify(failed[0])}`)
    }
    this.row('produced', {
      records: this.expected.length,
      bursts: this.sends.length,
      produceStart: this.produceStart,
      produceEnd: this.produceEnd,
      maxLatenessMs: this.lateness,
    })
  }

  /**
   * Wraps the bouncer's command runner so the held set and the in-flight requests are frozen in the
   * same turn that a node is told to go down, with nothing awaited between the two.
   * @param {Function} run the bouncer's own runner, which stays the one that reaches the cluster
   * @returns {Function} the runner with the freeze in front
   */
  watchDown(run) {
    return async (kind, service) => {
      const down = kind === 'stop' || kind === 'kill'
      if (down) this.freeze(kind, service)
      const frozen = this.snapshots[this.snapshots.length - 1]
      if (
        down &&
        (frozen.at < this.produceStart || performance.now() >= this.produceStart + PRODUCER_MS)
      )
        throw new Error('A down action would start outside the producer interval')
      const controls = this.evidence.controls
      const prior = controls.findLast(
        (control) => control.service === service && control.kind !== 'start'
      )
      const control = {
        id: controls.length,
        episode: frozen?.index,
        kind,
        service,
        nodeId: NODES[service]?.nodeId,
        mapping: this.lifecycle?.[service] ?? null,
        lifecycleUnavailable: this.lifecycleUnavailable,
        invoked: down ? { at: frozen.at, order: frozen.order } : this.tap.stamp(),
        ...(kind === 'start' && { replaces: prior?.id }),
        semantics: 'unavailable',
        status: 'pending',
      }
      controls.push(control)
      try {
        const result = await run(kind, service)
        Object.assign(control, {
          settled: this.tap.stamp(),
          status: 'fulfilled',
          exitCode: commandResult(result) ? 0 : null,
          semantics: commandResult(result) ? 'awaited-compose-service-process' : 'unavailable',
          result: result === undefined ? null : immutable(result),
        })
        if (down) frozen.downAt = control.settled.at
        return result
      } catch (error) {
        Object.assign(control, {
          settled: this.tap.stamp(),
          status: 'rejected',
          exitCode: error.code ?? null,
          error: describeError(error, 8),
          stdout: error.stdout ?? null,
          stderr: error.stderr ?? null,
        })
        throw error
      }
    }
  }

  /**
   * Freezes what is unresolved right now: the handler starts whose acknowledgement is not yet
   * confirmed, and the share requests still waiting for an answer.
   * @param {string} kind `stop` or `kill`
   * @param {string} service the compose service about to go down
   * @returns {void}
   */
  freeze(kind, service) {
    const corpus = new Set(this.expected)
    const cut = this.tap.stamp()
    const unresolved = unresolvedAt(this.entries, this.evidence, cut)
    const held = new Set(unresolved.map(({ identity }) => identity))
    const index = this.snapshots.length
    this.evidence.commands.push(
      immutable({ index, kind, service, nodeId: NODES[service].nodeId, ...cut })
    )
    this.snapshots.push({
      index,
      service,
      nodeId: NODES[service].nodeId,
      kind,
      ...cut,
      held,
      obligations: immutable(
        unresolved.map((entry) => obligation(entry, this.snapshots, index, this.evidence, cut))
      ),
      handlerEntries: this.entries.length,
      confirmedOffsets: this.confirmed.size,
      inFlight: immutable(this.tap.inFlight().map((frame) => ({ ...frame }))),
      interruptions: [],
      carried: immutable(carriedAt(this.snapshots, { index }, unresolved)),
    })
    const accounting = historyAccounting(this.entries, this.snapshots, this.evidence, cut)
    const problems = [...accounting.problems, ...admissionAccounting(this.snapshots).problems]
    if (held.size > MAX_HELD * (index + 1))
      problems.push(`Command history has ${held.size} identities`)
    if (problems.length) {
      this.row('freeze-failed', {
        index,
        kind,
        service,
        accounting,
        problems,
        crashes: this.crashes.map((error) => describeError(error)),
      })
      throw new Error(problems.join('; '))
    }
    if (unresolved.some(({ identity }) => !corpus.has(identity))) {
      this.row('unresolved-before-producer-receipt', { identities: [...held] })
    }
  }

  /**
   * Appends the corpus in 300 bursts of 100 records, one burst every 400 ms, never waiting for a
   * send before the next burst is due. A late burst fails the run rather than change the duration.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} when a burst starts more than a second late or a send exceeds the deadline
   */
  async produce(scope) {
    const start = performance.now()
    const deadline = start + PRODUCER_MS
    this.produceStart = start
    this.produceEnd = deadline
    await bounded(
      scope,
      deadline - performance.now(),
      async (phase) => {
        for (let burst = 0; burst < BURSTS; burst++) {
          const scheduledAt = start + burst * BURST_INTERVAL_MS
          const wait = scheduledAt - performance.now()
          if (wait > 0) await delay(phase, wait)
          phase.throwIfAborted()
          const now = performance.now()
          const late = Math.max(0, now - scheduledAt)
          if (late > LATE_BURST_MS) {
            throw new Error(`Burst ${burst} started ${Math.round(late)} ms late`)
          }
          if (now >= deadline) throw new Error('The producer deadline passed before a burst')
          this.lateness = Math.max(this.lateness, late)
          this.sendBurst(burst)
        }
        await Promise.all(this.pendingSends)
        if (
          performance.now() > deadline ||
          this.sends.some(({ settledAt }) => settledAt === null || settledAt > deadline)
        ) {
          throw new Error('A send did not settle within the producer interval')
        }
      },
      { label: 'The producer appends' }
    )
    while (performance.now() < deadline) await delay(scope, deadline - performance.now())
    scope.throwIfAborted()
  }

  /**
   * @param {number} index the record's position in the corpus
   * @returns {{ partition: number, key: Buffer, value: Buffer, timestamp: string }} the record
   */
  message(index) {
    return {
      partition: index % PARTITIONS,
      key: Buffer.from(`bounce:${String(index).padStart(5, '0')}`),
      value: Buffer.from(`bounce:value:${index}`),
      timestamp: RUN_TIMESTAMP,
    }
  }

  /**
   * Sends one burst and keeps its outcome. Both settlements are observed at once, so a rejection
   * cannot go unhandled while the schedule waits for the next burst.
   * @param {number} burst the burst's number
   * @returns {void}
   */
  sendBurst(burst) {
    const messages = Array.from({ length: BURST_SIZE }, (_, n) =>
      this.message(burst * BURST_SIZE + n)
    )
    const send = {
      id: this.sends.length,
      burst,
      startedAt: performance.now(),
      started: this.tap.stamp(),
      settledAt: null,
      outcome: 'pending',
      error: null,
      original: immutable({
        topic: this.topic,
        topicId: this.topicId,
        options: {
          acks: -1,
          timeout: null,
          compression: null,
          idempotent: true,
          maxInFlightRequests: 5,
          retry: RETRY,
        },
        omittedOptions: ['timeout', 'compression'],
        messages: messages.map(originalMessage),
      }),
      results: null,
      receiptIdentities: [],
    }
    this.sends.push(send)
    for (const { key } of messages)
      this.sendKeys.set(createHash('sha256').update(key).digest('hex'), send.id)
    this.pendingSends.push(
      this.producer.send({ topic: this.topic, acks: -1, messages }).then(
        (results) => {
          send.settlement = this.tap.stamp()
          send.settledAt = send.settlement.at
          send.outcome = 'success'
          send.results = immutable(results)
          send.error = this.recordAppends(messages, results, send)
        },
        (error) => {
          send.settlement = this.tap.stamp()
          send.settledAt = send.settlement.at
          send.outcome = error.outcome === 'unknown' ? 'unknown' : 'failure'
          send.error = immutable(producerError(error))
        }
      )
    )
  }

  /**
   * Turns the broker's acknowledgement into the expected identities: each partition's records take
   * consecutive offsets from the acknowledged base offset.
   * @param {object[]} messages the burst
   * @param {object[]} results the send's per-partition outcomes
   * @param {object} [send] Original public send ledger.
   * @returns {object|null} what was not acknowledged, or null when every record was
   */
  recordAppends(messages, results, send) {
    const next = new Map()
    for (const { partition, errorCode, baseOffset } of results) {
      if (errorCode === 0) next.set(partition, BigInt(baseOffset))
    }
    const missing = new Set()
    for (const { partition, key, value } of messages) {
      const offset = next.get(partition)
      if (offset === undefined) {
        missing.add(partition)
        continue
      }
      next.set(partition, offset + 1n)
      const identity = shareRecordIdentity({
        topic: this.topic,
        partition,
        offset: String(offset),
        key,
        value,
      })
      this.expected.push(identity)
      send?.receiptIdentities.push({
        identity,
        partition,
        offset: String(offset),
        key: originalBytes(key),
        value: originalBytes(value),
      })
      this.appended++
    }
    return missing.size ? { name: 'NotAcknowledged', partitions: [...missing] } : null
  }

  /**
   * Waits for the warm-up, checks that the share state topic survives every single removal, then
   * rolls every broker cleanly and hard, each step restored and back in full ISR before the next.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   */
  async bounce(scope) {
    await pollUntil(
      scope,
      60000,
      () => this.appended >= WARMUP_APPENDS && this.entries.length > 0,
      { label: 'The first appends and a delivery' }
    )
    await this.captureInternalTopics(scope)
    const topics = [this.topic, this.shareState.topic]
    for (const hard of [false, true]) {
      const outages = await this.bouncer.rollingBounce({
        hard,
        between: 0,
        admin: this.admin,
        topics,
        services: SERVICES,
        signal: scope,
      })
      this.outages.push(...outages)
      for (let index = 0; index < this.outages.length; index++) {
        this.snapshots[index].restoredAt = this.outages[index].tRestored
      }
      await bounded(scope, 90000, () => waitForFullIsr(this.admin, topics), {
        label: 'The full ISR',
      })
      await this.awaitMembership(scope, hard ? 'after the hard bounce' : 'after the clean bounce')
    }
  }

  /**
   * Captures the share state and offsets topics' real names, replicas, in-sync sets and min-ISR, and
   * refuses a cluster where removing any one node would leave a partition below its min-ISR.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} when the share state topic is missing or cannot survive a planned removal
   */
  async captureInternalTopics(scope) {
    const names = (await bounded(scope, 10000, () => this.admin.listTopics())).filter(
      (name) => name === SHARE_STATE_TOPIC
    )
    if (names.length !== 1) throw new Error(`No ${SHARE_STATE_TOPIC} topic after the member joined`)
    const topics = [...names, OFFSETS_TOPIC]
    const { topics: described } = await bounded(scope, 10000, () =>
      this.admin.fetchTopicMetadata({ topics })
    )
    const { resources } = await bounded(scope, 10000, () =>
      this.admin.describeConfigs({
        includeSynonyms: false,
        resources: topics.map((name) => ({
          type: ConfigResourceTypes.TOPIC,
          name,
          configNames: ['min.insync.replicas'],
        })),
      })
    )
    const internal = described.map(({ name, partitions }) => ({
      topic: name,
      minIsr: Number(
        resources
          .find(({ resourceName }) => resourceName === name)
          ?.configEntries.find(({ configName }) => configName === 'min.insync.replicas')
          ?.configValue
      ),
      partitions: partitions.map(({ partitionId, leader, replicas, isr }) => ({
        partition: partitionId,
        leader,
        replicas,
        isr,
      })),
    }))
    const short = internal.flatMap(({ topic, minIsr, partitions }) =>
      Object.entries(NODES).flatMap(([service, { nodeId }]) =>
        partitions
          .filter(({ isr }) => !(isr.filter((id) => id !== nodeId).length >= minIsr))
          .map(({ partition }) => `${topic}-${partition} without ${service}`)
      )
    )
    this.shareState = internal.find(({ topic }) => topic === SHARE_STATE_TOPIC)
    this.row('internal-topics', { topics: internal, voters: 3, maxDown: 1, short })
    if (short.length) throw new Error(`An internal partition cannot survive a removal: ${short}`)
  }

  /**
   * Waits until the admin API reports exactly the baseline member and assignment again.
   * @param {AbortSignal} scope the phase scope
   * @param {string} stage what has just finished, for the failure message
   * @returns {Promise<void>}
   */
  async awaitMembership(scope, stage) {
    const { memberId, assignment } = this.baselineMembership
    await pollTolerating(
      scope,
      30000,
      async (poll) => {
        const members = await this.describeMembers(poll)
        return (
          members.length === 1 &&
          members[0].memberId === memberId &&
          normalizeAssignment(members[0].assignment) === assignment
        )
      },
      { label: `The baseline membership ${stage}` }
    )
  }

  /**
   * Admits complete delivery with real successful receipts or evidenced unknown candidates.
   * Candidate admission never substitutes for the later independent terminal proof.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   */
  async drain(scope) {
    let previous
    try {
      await pollUntil(
        scope,
        60000,
        async () => {
          this.settlement = await cooperativeAudit(
            settlementCandidatesWork(
              this.entries,
              new Set(this.expected),
              this.snapshots,
              this.evidence
            ),
            scope
          )
          const counts = {
            handled: this.seen.size,
            successful: this.settlement.successful.length,
            unknownCandidates: this.settlement.unknown.length,
            missing: this.settlement.missing.length,
            problems: this.settlement.problems,
          }
          const current = JSON.stringify(counts)
          if (current !== previous) this.row('settlement-progress', counts)
          previous = current
          return (
            this.expected.length === RECORDS &&
            this.expected.every((identity) => this.seen.has(identity)) &&
            this.settlement.missing.length === 0 &&
            this.settlement.problems.length === 0
          )
        },
        { label: 'Every identity settling', delayMs: 500 }
      )
    } catch (error) {
      this.row('unresolved-settlement', { identities: this.settlement?.unresolved ?? [] })
      throw error
    }
  }

  /**
   * Checks the six outages, the membership and replay allowances, and the three independent
   * oracles: raw log, handler entries and broker offsets.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    await this.checkBounces(scope)
    const log = await bounded(scope, 40000, (signal) => this.readRawLog(signal))
    this.log = log
    const raw = []
    for (const [partition, { endOffset, records }] of log) {
      expect(endOffset).toBe(String(RECORDS / PARTITIONS))
      expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
      expect(records.map(({ offset }) => String(offset))).toEqual(
        Array.from({ length: RECORDS / PARTITIONS }, (_, n) => String(n))
      )
      for (const record of records) {
        raw.push(
          shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(record.offset),
            key: record.key,
            value: record.value,
          })
        )
      }
    }
    this.rawIdentities = raw
    expect(multisetProblems(tally(this.expected), tally(raw))).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    expect(this.expected).toHaveLength(RECORDS)

    this.handled = tally(this.entries.map(({ identity }) => identity))
    this.expectedHandled = tally(this.expected)
    for (const { identity } of this.replay.extras) {
      this.expectedHandled.set(identity, this.expectedHandled.get(identity) + 1)
    }
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    this.broker = await this.awaitBrokerEnd(scope)
  }

  /**
   * Checks the facts of the six outages against the plan's limits: each outage short of the session,
   * each node taken down while the producer ran, no crash and no handler failure, at least one
   * authentic session reset from a lost connection or a moved leader, no membership change, and
   * the frozen held sets and charged replay entries within their allowances.
   * @param {AbortSignal} scope Existing body scope.
   * @returns {Promise<void>} After the cooperative replay audit.
   * @throws {Error} the first way the evidence falls short
   */
  async checkBounces(scope) {
    expect(this.outages).toHaveLength(2 * SERVICES.length)
    expect(this.snapshots).toHaveLength(this.outages.length)
    expect(this.crashes.map((error) => error.message)).toEqual([])
    expect(this.handlerErrors).toEqual([])
    expect(this.resets.some(({ reason }) => ['connection', 'leader-moved'].includes(reason))).toBe(
      true
    )
    this.bounceFacts = this.outages.map((outage, index) => {
      const frozen = this.snapshots[index]
      const until = outage.tRestored + MEMBERSHIP_QUIET_MS
      const inside = ({ at }) => at >= frozen.at && at <= until
      return {
        ...outage,
        index,
        frozenAt: frozen.at,
        fullIsr: true,
        joins: this.joins.filter(inside).length,
        assignmentChanges: this.changes.filter(inside).length,
      }
    })
    const producerDeadline = this.produceStart + PRODUCER_MS
    expect(this.produceEnd).toBe(producerDeadline)
    for (const fact of this.bounceFacts) {
      expect(fact.outageMs).toBeLessThan(MAX_OUTAGE_MS)
      expect(fact.frozenAt).toBeGreaterThanOrEqual(this.produceStart)
      expect(fact.frozenAt).toBeLessThan(producerDeadline)
      expect({ index: fact.index, joins: fact.joins }).toEqual({ index: fact.index, joins: 0 })
      expect({ index: fact.index, changes: fact.assignmentChanges }).toEqual({
        index: fact.index,
        changes: 0,
      })
    }
    expect(this.joins).toHaveLength(1)
    for (const { index, held } of this.snapshots)
      expect(held.size).toBeLessThanOrEqual(MAX_HELD * (index + 1))
    expect([
      ...this.captureProblems,
      ...this.tap.problems,
      ...this.producerEvidence.problems,
    ]).toEqual([])
    this.appendAnomalies = this.diagnoseAppends()
    if (this.appendAnomalies.length) {
      throw new Error(`Log/idempotency anomaly: ${this.appendFailure()}`)
    }
    this.replay = await cooperativeAudit(
      auditReplayWork(this.entries, this.snapshots, new Set(this.expected), this.evidence),
      scope
    )
    expect(this.replay.problems).toEqual([])
  }

  /** @returns {object[]} Fixed floors and independent corpus ends for every partition. */
  terminalPartitions() {
    return Array.from({ length: PARTITIONS }, (_, partition) => ({
      partition,
      floor: '0',
      end: String(RECORDS / PARTITIONS),
    }))
  }

  /**
   * @param {AbortSignal} scope Case scope.
   * @returns {Promise<object[]>} Actual terminal rows, retaining both original listing forms.
   */
  async awaitBrokerEnd(scope) {
    this.position = await awaitTerminalSharePosition(
      this.admin,
      {
        groupId: this.group,
        topic: this.topic,
        partitions: this.terminalPartitions(),
      },
      {
        signal: scope,
        timeoutMs: this.window(30000, 40000),
        record: (fields) => {
          this.beforeTerminal = fields.last
          this.row('broker-position', fields)
        },
      }
    )
    return this.position.rows
  }

  /**
   * Observes verifier activity through a separate wrapper around the frozen public consumer.
   * @param {object} consumer Original consumer, whose properties are never replaced.
   * @returns {object} Forwarding interface with tracked, call-through disposal.
   */
  trackVerifier(consumer) {
    const removers = ['GROUP_JOIN', 'ASSIGNMENT_CHANGED', 'REQUEST'].map((name) =>
      consumer.on(consumer.events[name], ({ payload }) => {
        const at = performance.now()
        this.quiet.events.push({ name, at, payload })
        if (
          !this.quiet.activeAt &&
          name === 'REQUEST' &&
          payload.apiName === 'ShareFetch' &&
          consumer.assignment().some(({ partitions }) => partitions.length)
        )
          this.quiet.activeAt = at
      })
    )
    return {
      ...consumer,
      disconnect: (...args) => {
        this.quiet.endedAt = performance.now()
        this.quiet.assignment = consumer.assignment()
        return consumer.disconnect(...args).finally(() => removers.forEach((remove) => remove()))
      },
    }
  }

  /**
   * Closes the workload, actively verifies the same group, then rechecks terminality and ends.
   * @param {AbortSignal} scope Case scope.
   * @returns {Promise<void>} Only after unknown candidates pass every independent proof.
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    this.quiet = { events: [], delivered: [], startedAt: performance.now() }
    const client = this.client('verifier')
    const kafka = { shareConsumer: (config) => this.trackVerifier(client.shareConsumer(config)) }
    this.verifier = await assertQuietTerminalGroup({
      kafka,
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions: this.terminalPartitions(),
      signal: scope,
      timeoutMs: 30000,
      record: (fields) => {
        if (fields.last) this.afterTerminal = fields.last
        if (fields.delivered) this.quiet.delivered = fields.delivered
        this.row('quiet-verifier', fields)
      },
    })
    const terminal = await cooperativeAudit(
      auditTerminalWork({
        corpus: this.expected,
        raw: this.rawIdentities,
        entries: this.entries,
        bounces: this.snapshots,
        evidence: this.evidence,
        limits: this.deliveryLimits,
        target: { groupId: this.group, topic: this.topic, partitions: this.terminalPartitions() },
        before: this.beforeTerminal,
        after: this.afterTerminal,
        quiet: this.quiet,
      }),
      scope
    )
    this.settlement = terminal.settlement
    expect(terminal.problems).toEqual([])
    expect(this.settlement.missing).toEqual([])
    expect(this.settlement.successful.length + this.settlement.unknown.length).toBe(RECORDS)
    expect(this.settlement.successful.length).toBeGreaterThanOrEqual(RECORDS - MAX_REPLAY)
    expect(this.crashes.map((error) => error.message)).toEqual([])
    expect([...this.tap.problems, ...this.producerEvidence.problems]).toEqual([])
    this.terminalVerified = true
  }

  /**
   * Joins outside-corpus physical identities to original sends, all receipt offsets and wire batches.
   * These links explain anomalies and never add an identity to the corpus.
   * @returns {object[]} Complete diagnostic joins for the observed outside-corpus identities.
   */
  diagnoseAppends() {
    const corpus = new Set(this.expected)
    const outside = [
      ...new Map(
        this.entries
          .filter(({ identity }) => !corpus.has(identity))
          .map((entry) => [entry.identity, entry])
      ).values(),
    ]
    if (!outside.length) return []
    const batches = []
    for (const frame of this.tap.allFrames) {
      if (![0, 78].includes(frame.apiKey)) continue
      try {
        const raw = this.tap.rawProduce.get(frame.id)
        batches.push(
          ...((frame.apiKey === 0 ? raw || frame.bytes : frame.responseBytes)
            ? wireBatchStamps(frame, raw)
            : (frame.batches ?? []))
        )
      } catch (error) {
        this.tap.problem(`Batch diagnostic frame ${frame.id}: ${error.message}`)
      }
    }
    const frames = new Map(this.tap.allFrames.map((frame) => [frame.id, frame]))
    const stampKey = ({
      topicId,
      partition,
      producerId,
      producerEpoch,
      firstSequence,
      count,
      bodyHash,
      payloadHash,
    }) =>
      JSON.stringify([
        topicId ?? this.topicId,
        partition,
        producerId,
        producerEpoch,
        firstSequence,
        count,
        bodyHash,
        payloadHash,
      ])
    let diagnosticBytes = 0
    const joins = []
    for (const entry of outside) {
      const key = entry.keyBytes == null ? null : originalBytes(Buffer.from(entry.keyBytes, 'hex'))
      const value =
        entry.valueBytes == null ? null : originalBytes(Buffer.from(entry.valueBytes, 'hex'))
      const sends = this.sends.filter((send) =>
        send.original.messages.some(
          (message) =>
            message.partition === entry.partition && message.key === key && message.value === value
        )
      )
      const received = batches.filter(
        (batch) =>
          frames.get(batch.frame).apiKey === 78 &&
          batch.topicId === entry.topicId &&
          batch.partition === entry.partition &&
          batch.topError === 0 &&
          batch.partitionError === 0 &&
          BigInt(batch.baseOffset) <= BigInt(entry.offset) &&
          BigInt(batch.baseOffset) + BigInt(batch.lastOffsetDelta) >= BigInt(entry.offset)
      )
      const stamps = new Set(received.map(stampKey))
      const matching = batches.filter(
        (batch) =>
          (batch.topic == null || batch.topic === this.topic) && stamps.has(stampKey(batch))
      )
      const join = {
        identity: entry.identity,
        offset: entry.offset,
        partition: entry.partition,
        sends: sends.map(({ id }) => id),
        receiptIdentities: sends.flatMap((send) =>
          send.receiptIdentities.filter(
            (receipt) =>
              receipt.partition === entry.partition &&
              receipt.key === key &&
              receipt.value === value
          )
        ),
        batches: received,
        copies: matching
          .filter((batch) => frames.get(batch.frame).apiKey === 78)
          .map((batch) => ({
            offset: String(
              BigInt(batch.baseOffset) + BigInt(entry.offset) - BigInt(received[0].baseOffset)
            ),
            ...batch,
          })),
        produceAttempts: matching
          .filter((batch) => frames.get(batch.frame).apiKey === 0)
          .map((batch) => {
            const frame = frames.get(batch.frame)
            return {
              ...batch,
              socket: frame.socket,
              correlationId: frame.correlationId,
              nodeId: frame.nodeId,
              endpoint: frame.endpoint,
              writtenAt: frame.at,
              responseAt: frame.responseAt ?? null,
              lostAt: frame.lostAt ?? null,
              attempt:
                this.producerEvidence.attempts.find(({ frames }) =>
                  frames.some(({ id }) => id === frame.id)
                )?.id ?? null,
            }
          }),
        evidenceStatus: received.length ? 'original-wire-batches' : 'missing-wire-batch',
      }
      join.problems = this.appendJoinProblems(join, frames, sends)
      const snapshot = boundedEvidence(join, ARCHIVE_ROW_BYTES)
      diagnosticBytes += snapshot.bytes
      if (snapshot.truncated || diagnosticBytes > 1024 * 1024) {
        this.tap.problem(
          'Append diagnostic joins exceeded 1 MiB; remaining identities are unverified'
        )
        if (!joins.length)
          joins.push({
            ...join,
            batches: received.slice(0, 1),
            copies: [...new Map(join.copies.map((copy) => [copy.offset, copy])).values()].slice(
              0,
              8
            ),
            produceAttempts: join.produceAttempts.slice(0, 32),
            receiptIdentities: join.receiptIdentities.slice(0, 8),
            truncated: true,
          })
        break
      }
      joins.push(snapshot.value)
    }
    return joins
  }

  /**
   * Checks the diagnostic Produce/receipt join without changing physical corpus admission.
   * @param {object} join Outside-corpus identity and wire matches.
   * @param {Map} frames Original wire exchanges.
   * @param {object[]} sends Original public sends selected by record bytes.
   * @returns {string[]} Explicit missing or contradictory producer-evidence components.
   */
  appendJoinProblems(join, frames, sends) {
    const problems = []
    if (!join.batches.length) problems.push('fetch-batch: missing original UUID/partition/offset')
    if (!join.produceAttempts.length)
      problems.push('produce-batch: missing matching original stamp/body')
    for (const batch of join.produceAttempts) {
      const frame = frames.get(batch.frame)
      const attempt = this.producerEvidence.attempts.find(({ id }) => id === batch.attempt)
      const partition = attempt?.partitions.find(
        (part) => part.partition === batch.partition && part.firstSequence === batch.firstSequence
      )
      if (
        !attempt ||
        !partition ||
        attempt.producerId !== batch.producerId ||
        attempt.producerEpoch !== batch.producerEpoch ||
        partition.count !== batch.count ||
        partition.topicId !== this.topicId ||
        !partition.sends.some((id) => sends.some((send) => send.id === id))
      )
        problems.push('producer-state: stamp or parent-send mismatch')
      if (
        !attempt ||
        attempt.nodeId !== frame.nodeId ||
        attempt.endpoint !== frame.endpoint ||
        !attempt.frames.some(
          (original) =>
            original.id === frame.id &&
            original.socket === frame.socket &&
            original.correlationId === frame.correlationId
        )
      )
        problems.push('produce-authority: socket/correlation/endpoint mismatch')
      if (!frame.responseBytes) {
        const loss = this.tap.socketEvents.find((event) => event.id === frame.lossEvent)
        if (
          !loss ||
          loss.socket !== frame.socket ||
          !loss.outstanding.includes(frame.id) ||
          loss.at !== frame.lostAt ||
          loss.order !== frame.lossOrder
        )
          problems.push('produce-loss: missing original outstanding/loss event')
      }
    }
    for (const send of sends) {
      if (send.original.topic !== this.topic || send.original.topicId !== this.topicId) {
        problems.push('send-identity: topic UUID mismatch')
        continue
      }
      const partitions = new Map()
      for (const message of send.original.messages)
        indexMember(partitions, message.partition, message)
      for (const [partition, messages] of partitions) {
        const results = (send.results ?? []).filter(
          (result) => result.partition === partition && result.errorCode === 0
        )
        const result = results[0]
        const acknowledged = join.produceAttempts.some((batch) => {
          const frame = frames.get(batch.frame)
          if (!frame.responseBytes) return false
          const bytes = Buffer.from(frame.responseBytes, 'base64')
          if (bytes.readInt32BE(4) !== frame.correlationId) return false
          return produceResponseFacts(frame, bytes).some(
            (response) =>
              response.partition === partition &&
              response.errorCode === 0 &&
              (response.topicId === this.topicId || response.topic === this.topic) &&
              response.baseOffset === result?.baseOffset
          )
        })
        if (results.length !== 1 || !acknowledged) {
          problems.push('public-receipt: no matching successful original Produce result')
          continue
        }
        const expected = messages.map((message, index) => ({
          identity: shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(BigInt(result.baseOffset) + BigInt(index)),
            key: message.key == null ? null : Buffer.from(message.key, 'base64'),
            value: message.value == null ? null : Buffer.from(message.value, 'base64'),
          }),
          partition,
          offset: String(BigInt(result.baseOffset) + BigInt(index)),
          key: message.key,
          value: message.value,
        }))
        if (
          !isDeepStrictEqual(
            expected,
            send.receiptIdentities.filter((receipt) => receipt.partition === partition)
          )
        )
          problems.push('receipt-mapping: public result/physical identity mismatch')
      }
    }
    return [...new Set(problems)]
  }

  /** @returns {string} Both physical offsets, the original stamp and matching attempt/frame IDs. */
  appendFailure() {
    const anomaly = this.appendAnomalies[0]
    const batch = anomaly.batches[0]
    const stamp = batch
      ? {
          producerId: batch.producerId,
          producerEpoch: batch.producerEpoch,
          baseSequence: batch.firstSequence,
          count: batch.count,
        }
      : { status: 'missing' }
    const offsets = [...new Set([anomaly.offset, ...anomaly.copies.map(({ offset }) => offset)])]
    return `partition ${anomaly.partition} offsets ${offsets.join('/')} batch ${JSON.stringify(stamp)} Produce attempts ${JSON.stringify(anomaly.produceAttempts.map(({ attempt, frame }) => ({ attempt, frame })))}`
  }

  /**
   * Opens independent raw-log readers whose original Fetch/ListOffsets frames also enter the tap.
   * @param {AbortSignal} signal Existing observation scope.
   * @returns {Promise<Map>} Independent raw log, without a consumer group or retry helper.
   */
  async readRawLog(signal) {
    const brokers = []
    const disconnect = () => {
      for (const broker of brokers) broker.disconnect().catch(() => {})
    }
    signal.addEventListener('abort', disconnect, { once: true })
    try {
      return await readLog({
        brokers: plainTextBrokers(),
        topic: this.topic,
        open: async ({ host, port }) => {
          signal.throwIfAborted()
          const broker = new Broker({
            connectionPool: createConnectionPool({
              host,
              port,
              connectionTimeout: 3000,
              requestTimeout: 30000,
              socketFactory: this.tap.factory('raw-log'),
            }),
            logger: newLogger(),
          })
          brokers.push(broker)
          try {
            await broker.connect()
            signal.throwIfAborted()
            return broker
          } catch (error) {
            await broker.disconnect()
            throw error
          }
        },
      })
    } finally {
      signal.removeEventListener('abort', disconnect)
      await Promise.all(brokers.map((broker) => broker.disconnect()))
    }
  }

  /**
   * Preserves the primary failure while attempting raw-log capture before owned topic deletion.
   * @param {AbortSignal} scope Original case scope.
   * @param {number} bodyDeadline Original body deadline, never extended for diagnosis.
   * @returns {Promise<void>} A snapshot or an explicit reason it could not be captured.
   */
  async captureFailureLog(scope, bodyDeadline) {
    this.failureLog = { started: this.tap.stamp(), status: 'unverified' }
    try {
      if (!this.readiness || liveRun.tainted || scope.aborted)
        throw new Error(
          'Raw-log diagnosis unavailable: no ready fixture, expired scope or tainted run'
        )
      const remaining = Math.min(40000, bodyDeadline - performance.now(), this.window(40000))
      if (remaining <= 0)
        throw new Error('No original body allowance remains for raw-log diagnosis')
      const log = await bounded(scope, remaining, async (signal) => {
        const log = this.log ?? (await this.readRawLog(signal))
        for (const broker of this.evidence.brokers) {
          if (broker.connectionPool.isConnected())
            await this.evidence.captureBrokerState(broker, signal)
        }
        return log
      })
      const involved = new Set(
        (this.appendAnomalies ?? []).flatMap((anomaly) =>
          [anomaly, ...anomaly.copies, ...anomaly.receiptIdentities].map(
            ({ partition, offset }) => `${partition}:${offset}`
          )
        )
      )
      for (const entry of this.entries) if (!this.confirmed.has(entry.key)) involved.add(entry.key)
      const partitions = [...log].map(([partition, { endOffset, records }]) => ({
        partition,
        endOffset,
        recordCount: records.length,
        records: records
          .filter(({ offset }) => involved.has(`${partition}:${offset}`))
          .map((record) => ({
            offset: String(record.offset),
            ...originalMessage(record),
            batchContext: record.batchContext,
          })),
      }))
      this.failureLog.partitions = partitions
      this.failureLog.status = 'captured'
    } catch (error) {
      this.failureLog.error = describeError(error, 8)
    }
    this.failureLog.ended = this.tap.stamp()
  }

  /**
   * Writes compact producer facts on success; failures retain only involved identity/batch streams.
   * Large streams are emitted one bounded row at a time, never as one acquisition/fetch journal.
   * @param {boolean} [failed] Whether the original case failed, including after terminal proof.
   * @returns {void}
   */
  archive(failed = false) {
    return finishAudit(this.archiveWork(failed))
  }

  /** @param {boolean} [failed] Original case status. @returns {Generator} Closure construction within the existing final scope. */
  *archiveWork(failed = false) {
    if (this.archived) return
    this.archived = true
    const rows = []
    const emit = (event, fields) => rows.push({ event, fields })
    this.appendAnomalies ??= failed ? this.diagnoseAppends() : []
    emit('acquisition-journal', {
      evidenceSchema: 3,
      producerSchema: 2,
      status: failed ? 'failed' : 'terminal-verified',
      projection: failed ? 'involved-identities-and-batches' : 'compact-summary',
      topic: this.topic,
      topicId: this.topicId,
      counts: {
        corpus: this.expected.length,
        entries: this.entries.length,
        acquisitions: this.evidence.acquisitions.length,
        exchanges: this.evidence.exchanges.length,
        outcomes: this.evidence.outcomes.length,
        successfulAccept: this.settlement?.successful.length ?? 0,
        replayEvents: this.replay?.extras.length ?? 0,
        replayPerEpisode: this.replay?.perBounce ?? [],
        provisionalUnknown: this.settlement?.unknown.length ?? 0,
        terminalUnknown: this.terminalVerified ? (this.settlement?.unknown.length ?? 0) : 0,
      },
      deliveryLimits: this.deliveryLimits,
      position: this.beforeTerminal,
      postVerifierPosition: this.afterTerminal,
      verifier: this.verifier,
      replayProblems: this.replay?.problems ?? [],
      settlementProblems: this.settlement?.problems ?? [],
      captureBounds: {
        archiveBytes: ARCHIVE_BYTES,
        rowBytes: ARCHIVE_ROW_BYTES,
        wireBytes: WIRE_BYTES,
        wireUsed: this.tap.byteCount,
        producerBytes: PRODUCER_EVIDENCE_BYTES,
        producerUsed: this.producerEvidence.bytes,
        rawProduceBytes: RAW_PRODUCE_BYTES,
        rawProduceUsed: this.tap.rawProduceBytes,
        rawProducePerPartition: RAW_PRODUCE_PER_PARTITION,
      },
      captureProblems: [
        ...this.captureProblems,
        ...this.tap.problems,
        ...this.producerEvidence.problems,
      ],
      brokerMechanism: {
        status: 'unverified',
        unavailable: [
          'image digest/build',
          'on-disk producer snapshots',
          'log truncation/recovery history',
          'continuous high-watermark/ELR/election history',
        ],
        reason:
          'Client APIs expose sampled metadata, ends and DescribeProducers; broker artifacts are required for the remaining facts',
      },
    })
    for (const candidate of this.settlement?.unknown ?? [])
      emit('unknown-settlement', {
        ...candidate,
        terminalVerified: Boolean(this.terminalVerified),
      })
    for (const extra of this.replay?.extras ?? []) emit('replay-entry', extra)
    emit('quiet-observation', this.quiet)
    const identities = new Set(this.appendAnomalies.map(({ identity }) => identity))
    if (failed) {
      const counts = tally(this.entries.map(({ identity }) => identity))
      for (const [identity, count] of counts) if (count > 1) identities.add(identity)
      for (const entry of this.entries)
        if (!this.confirmed.has(entry.key)) identities.add(entry.identity)
      for (const { identity } of this.settlement?.unknown ?? []) identities.add(identity)
      for (const anomaly of this.appendAnomalies) {
        emit('append-anomaly', anomaly)
        for (const { identity } of anomaly.receiptIdentities) identities.add(identity)
      }
    }
    const sendIds = new Set(this.appendAnomalies.flatMap(({ sends }) => sends))
    if (failed) {
      for (const send of this.sends)
        if (
          send.error ||
          send.outcome !== 'success' ||
          send.receiptIdentities.some(({ identity }) => identities.has(identity))
        )
          sendIds.add(send.id)
    }
    const links = this.producerEvidence.links()
    const batchIds = new Set(
      links
        .filter(({ sends }) => !failed || sends.some((id) => sendIds.has(id)))
        .map(({ batch }) => batch)
    )
    const attemptIds = new Set(
      links.filter(({ batch }) => batchIds.has(batch)).flatMap(({ attempts }) => attempts)
    )
    const frameIds = new Set(
      this.appendAnomalies.flatMap(({ batches, copies, produceAttempts }) =>
        [...batches, ...copies, ...produceAttempts].map(({ frame }) => frame)
      )
    )
    for (const state of this.evidence.brokerStates)
      for (const { id } of state.frames) frameIds.add(id)
    for (const attempt of this.producerEvidence.attempts)
      if (!failed || attemptIds.has(attempt.id))
        for (const { id } of attempt.frames) frameIds.add(id)

    for (const bounce of this.snapshots)
      for (const held of [
        ...bounce.obligations,
        ...bounce.carried,
        ...bounce.interruptions.flatMap((cut) => [...cut.obligations, ...cut.carried]),
      ])
        identities.add(held.identity)
    const historyEntries = new Set(
      this.snapshots.flatMap((bounce) => [
        ...bounce.obligations.map(({ entry }) => entry),
        ...bounce.carried.map(({ entry }) => entry),
        ...bounce.interruptions.flatMap((cut) => [
          ...cut.history,
          ...cut.lateEntries,
          ...cut.obligations.map(({ entry }) => entry),
          ...cut.carried.map(({ entry }) => entry),
        ]),
      ])
    )
    for (const entry of this.entries)
      if (historyEntries.has(entry.id)) identities.add(entry.identity)
    for (const extra of this.replay?.extras ?? []) identities.add(extra.identity)
    for (const candidate of this.settlement?.unknown ?? []) identities.add(candidate.identity)
    yield* this.archiveFailureIdentities(identities, frameIds, emit)
    if (failed) emit('failure-log', this.failureLog)
    const bulkFrames = []
    let planned = 0
    for (const frame of this.tap.allFrames) {
      if (planned++ % 100 === 0) yield
      const producerFrame = [0, 22].includes(frame.apiKey)
      if (failed && !producerFrame && !frameIds.has(frame.id)) continue
      const { bytes, responseBytes, ...fact } = frame
      const raw = this.tap.rawProduce.get(frame.id)
      if (frameIds.has(frame.id) && !(bytes || raw)) {
        this.archiveIncomplete = true
        this.captureProblems.push(
          `Required original request bytes unavailable for frame ${frame.id}`
        )
      }
      const original = {
        ...fact,
        bytes,
        responseBytes,
        ...(frame.apiKey === 0 &&
          frameIds.has(frame.id) && {
            bytes: bytes ?? raw?.toString('base64'),
            rawStatus: bytes || raw ? 'retained-original' : 'unavailable',
          }),
      }
      if (frameIds.has(frame.id)) emit('wire-exchange', original)
      else bulkFrames.push(original)
    }
    emit('wire-history', { frames: bulkFrames })
    for (const send of this.sends) {
      const { receiptIdentities, ...original } = send
      if (failed && !sendIds.has(send.id)) {
        const { original: input, ...settlement } = original
        emit('original-send', {
          ...settlement,
          outcome: send.outcome === 'pending' ? 'unknown' : send.outcome,
          inputRecordCount: input.messages.length,
          receiptCount: receiptIdentities.length,
          projection: 'uninvolved-send-outcome',
        })
        continue
      }
      emit('original-send', {
        ...original,
        outcome: send.outcome === 'pending' ? 'unknown' : send.outcome,
        receiptCount: receiptIdentities.length,
        receiptIdentities,
      })
    }
    for (const batch of this.producerEvidence.batches)
      if (batchIds.has(batch.id)) emit('producer-batch', { ...batch, link: links[batch.id] })
    for (const { frames, batchStates, ...attempt } of this.producerEvidence.attempts)
      emit('produce-attempt', {
        ...attempt,
        batchStates: attemptIds.has(attempt.id) ? batchStates : undefined,
        frames: frames.map(({ id }) => id),
      })
    for (const transition of this.producerEvidence.transitions) {
      const states = [transition.before, transition.after].filter(Boolean)
      const involved =
        (transition.batch && batchIds.has(transition.batch.id)) ||
        states.some(({ partitions }) =>
          partitions?.some(
            ({ queued, inFlight }) =>
              (queued ?? []).some((id) => batchIds.has(id)) ||
              (inFlight ?? []).some(({ id }) => batchIds.has(id))
          )
        )
      if (!failed || involved || transition.method === 'identity.bumpEpoch')
        emit('producer-transition', transition)
    }
    if (!failed) {
      emit('handler-history', {
        entries: yield* mappedAuditWork(this.entries, ({ acquisition, ...entry }) => ({
          ...entry,
          acquisition: acquisition?.id,
        })),
      })
      emit('acquisition-history', {
        acquisitions: yield* mappedAuditWork(
          this.evidence.acquisitions,
          ({ source, ...acquisition }) => acquisition
        ),
      })
      emit('exchange-history', {
        exchanges: yield* mappedAuditWork(
          this.evidence.exchanges,
          ({ frames, response, ...exchange }) => ({
            ...exchange,
            frames: frames.map(({ id }) => id),
          })
        ),
      })
    }
    emit('broker-configs', { configs: this.brokerConfigs })
    for (const { frames, ...state } of this.evidence.brokerStates)
      emit('producer-broker-state', { ...state, frames: frames.map(({ id }) => id) })
    const first = new Set([
      'bounce-restoration',
      'lifecycle-mapping',
      'bounce-control',
      'bounce-command',
      'frozen-bounce',
      'socket-event',
      'retirement',
      'group-deletion',
      'member-close',
    ])
    const required = new Set([
      'handler-entry',
      'acquisition',
      'share-exchange',
      'acknowledgement-outcome',
      'epoch-source',
      'unknown-settlement',
      'replay-entry',
      'append-anomaly',
      'failure-log',
    ])
    const priority = ({ event, fields }) => {
      if (first.has(event)) return 0
      if (event === 'wire-exchange') return frameIds.has(fields.id) ? 1 : 4
      if (event === 'wire-history')
        return fields.frames.some(({ id }) => frameIds.has(id)) ? 1 : failed ? 4 : 3
      if (event === 'socket-history') return failed ? 4 : 3
      if (required.has(event)) return 2
      if (event === 'original-send') return sendIds.has(fields.id) ? 2 : failed ? 4 : 3
      if (event === 'producer-batch') return batchIds.has(fields.id) ? 2 : 4
      if (event === 'produce-attempt') return attemptIds.has(fields.id) ? 2 : 4
      return 3
    }
    // Use the existing lossless history schema to avoid thousands of duplicate row envelopes.
    const histories = new Map([
      ['handler-entry', ['handler-history', 'entries']],
      ['acquisition', ['acquisition-history', 'acquisitions']],
      ['share-exchange', ['exchange-history', 'exchanges']],
      ['wire-exchange', ['wire-history', 'frames']],
    ])
    const counts = new Map()
    for (const row of rows) {
      if (planned++ % 100 === 0) yield
      indexMember(counts, row.event, row)
    }
    const packed = []
    for (const [event, members] of counts) {
      const history = histories.get(event)
      if (!history || members.length < 100) {
        packed.push(...members)
        continue
      }
      for (let first = 0; first < members.length; first += 100) {
        yield
        packed.push({
          event: history[0],
          fields: { [history[1]]: members.slice(first, first + 100).map(({ fields }) => fields) },
        })
      }
    }
    const orderedRows = yield* sortedAuditWork(packed, (a, b) => priority(a) - priority(b))
    const accounting = yield* freezeAccountingWork(this.entries, this.snapshots, this.evidence)
    const problems = yield* this.archiveProblemsWork(orderedRows, frameIds, accounting)
    this.archiveClosure = { frames: [...frameIds], problems }
    if (problems.length) {
      this.archiveIncomplete = true
      this.captureProblems.push(...problems)
    }
    const manifest = []
    for (const [index, fact] of orderedRows.entries()) {
      if (index % 100 === 0) yield
      manifest.push({
        index,
        event: fact.event,
        id: fact.fields?.id,
        fields: Object.keys(fact.fields ?? {}),
        sha256: createHash('sha256')
          .update(JSON.stringify(fact.fields ?? {}))
          .digest('hex'),
        required: priority(fact) < 4,
        evidenceId: `${this.nonce}:${(this.archiveSequence ?? 0) + 1 + index + (priority(fact) >= 4 ? 1 : 0)}`,
      })
    }
    const closure = {
      evidenceSchema: 3,
      identities: [...identities],
      frames: [...frameIds].sort((a, b) => a - b),
      rows: manifest,
      omitted: {
        epochSources:
          this.evidence.epochs.length - rows.filter(({ event }) => event === 'epoch-source').length,
      },
    }
    this.archiveClosure.sha256 = createHash('sha256').update(JSON.stringify(closure)).digest('hex')
    this.row('witness-closure', closure)
    const requiredRows = orderedRows.filter((fact) => priority(fact) < 4)
    const optionalRows = orderedRows.filter((fact) => priority(fact) >= 4)
    for (const fact of requiredRows) {
      yield
      this.row(fact.event, fact.fields)
    }
    this.row('witness-closure-end', {
      complete: !this.archiveIncomplete,
      problems,
      rows: requiredRows.length,
      optionalRows: optionalRows.length,
    })
    for (const fact of optionalRows) {
      yield
      this.row(fact.event, fact.fields, false)
    }
  }

  /**
   * Validates references against emitted originals, including genuinely unanswered requests.
   * @param {object[]} rows Planned lossless archive rows.
   * @param {Set<number>} frameIds Complete required frame closure.
   * @param {object} accounting Independently reconstructed interruption membership.
   * @returns {Generator} Missing acceptance-bearing evidence components, with bounded traversal steps.
   */
  *archiveProblemsWork(rows, frameIds, accounting) {
    const problems = []
    const byEvent = new Map()
    let visited = 0
    for (const { event, fields } of rows) {
      if (visited++ % 100 === 0) yield
      indexMember(byEvent, event, fields)
    }
    /** @param {string} plain Individual row event. @param {string} bulk History event. @param {string} key History membership. @returns {Generator} Indexed originals. */
    const originals = function* (plain, bulk, key) {
      const index = new Map()
      for (const group of [
        byEvent.get(plain) ?? [],
        ...(byEvent.get(bulk) ?? []).map((row) => row[key]),
      ])
        for (const row of group) {
          if (visited++ % 100 === 0) yield
          index.set(row.id, row)
        }
      return index
    }
    const frames = yield* originals('wire-exchange', 'wire-history', 'frames')
    const referenced = [
      ...(byEvent.get('share-exchange') ?? []),
      ...(byEvent.get('epoch-source') ?? []),
      ...(byEvent.get('producer-broker-state') ?? []),
      ...(byEvent.get('member-close') ?? []),
      ...(byEvent.get('exchange-history') ?? []).flatMap(({ exchanges }) => exchanges),
    ]
    for (const row of referenced) {
      if (visited++ % 100 === 0) yield
      for (const id of row.frames ?? []) frameIds.add(id)
    }
    problems.push(...this.archiveLifecycleProblems(byEvent))
    const controls = new Map(
      (byEvent.get('bounce-control') ?? []).map((control) => [control.id, control])
    )
    for (const extra of byEvent.get('replay-entry') ?? []) {
      if (visited++ % 100 === 0) yield
      const witness = extra.incarnationWitness
      if (!witness) continue
      for (const key of [
        'oldFrame',
        'newFrame',
        'before',
        'after',
        'transition',
        'metadataTransition',
      ])
        if (witness[key] !== undefined) frameIds.add(witness[key])
      for (const key of ['down', 'start'])
        if (witness[key] !== undefined) {
          const control = controls.get(witness[key])
          if (!control || control.status !== 'fulfilled' || !commandResult(control.result))
            problems.push(`lifecycle-result: missing successful ${key} ${witness[key]}`)
        }
    }
    const completed = new Set()
    for (const source of [
      ...this.evidence.exchanges,
      ...this.evidence.epochs,
      ...this.evidence.brokerStates,
    ]) {
      if (visited++ % 100 === 0) yield
      if (source.response) for (const { id } of source.frames) completed.add(id)
    }
    for (const id of frameIds) {
      if (visited++ % 100 === 0) yield
      const frame = frames.get(id)
      if (!frame) problems.push(`frame: missing required original ${id}`)
      else {
        if (!frame.bytes) problems.push(`request-bytes: missing required original ${id}`)
        if ((completed.has(id) || frame.responseAt !== undefined) && !frame.responseBytes)
          problems.push(`response-bytes: missing completed original ${id}`)
      }
    }
    const entries = yield* originals('handler-entry', 'handler-history', 'entries')
    const acquisitions = yield* originals('acquisition', 'acquisition-history', 'acquisitions')
    const exchanges = yield* originals('share-exchange', 'exchange-history', 'exchanges')
    for (const entry of entries.values()) {
      if (visited++ % 100 === 0) yield
      const acquisition = acquisitions.get(entry.acquisition)
      if (!acquisition || !exchanges.has(acquisition.exchange))
        problems.push(`acquisition-reference: missing original for ${entry.id}`)
    }
    for (const bounce of byEvent.get('frozen-bounce') ?? []) {
      for (const held of [...bounce.obligations, ...bounce.carried]) {
        if (visited++ % 100 === 0) yield
        if (!entries.has(held.entry))
          problems.push(`interruption-member: missing original ${held.entry}`)
      }
      for (const cut of bounce.interruptions) {
        for (const id of [
          ...cut.history,
          ...cut.lateEntries,
          ...cut.obligations.map(({ entry }) => entry),
          ...cut.carried.map(({ entry }) => entry),
        ]) {
          if (visited++ % 100 === 0) yield
          if (!entries.has(id)) problems.push(`interruption-member: missing original ${id}`)
        }
        const event =
          cut.kind === 'failure'
            ? exchanges.get(cut.event)
            : (byEvent.get(cut.kind === 'socket' ? 'socket-event' : 'retirement') ?? []).find(
                ({ id }) => id === cut.event
              )
        if (!event) problems.push(`interruption-reference: missing ${cut.kind} ${cut.event}`)
      }
    }
    // Reconstruct membership independently: deleting an obligation must not shrink the required set.
    for (const problem of accounting.problems) problems.push(`interruption-membership: ${problem}`)
    return problems
  }

  /**
   * Requires original command, process and restoration evidence regardless of replay selection.
   * @param {Map<string, object[]>} byEvent Decoded archive components.
   * @returns {string[]} Missing lifecycle results, references, availability or ordering.
   */
  archiveLifecycleProblems(byEvent) {
    const problems = []
    const commands = byEvent.get('bounce-command') ?? []
    const bounces = byEvent.get('frozen-bounce') ?? []
    const controls = byEvent.get('bounce-control') ?? []
    const restorations = byEvent.get('bounce-restoration') ?? []
    const mappings = byEvent.get('lifecycle-mapping') ?? []
    const episodes = new Set([
      ...this.snapshots.map(({ index }) => index),
      ...this.evidence.commands.map(({ index }) => index),
      ...this.evidence.controls.map(({ episode }) => episode),
    ])
    for (const index of episodes) {
      const commandRows = commands.filter((command) => command.index === index)
      const bounceRows = bounces.filter((bounce) => bounce.index === index)
      const command = commandRows[0]
      const bounce = bounceRows[0]
      if (commandRows.length !== 1 || !['stop', 'kill'].includes(command?.kind)) {
        problems.push(`lifecycle-reference: missing original command ${index}`)
        continue
      }
      if (
        bounceRows.length !== 1 ||
        ['index', 'kind', 'service', 'nodeId', 'at', 'order'].some(
          (key) => command[key] !== bounce[key]
        )
      )
        problems.push(`lifecycle-reference: missing original episode ${index}`)
      const downs = controls.filter(
        (control) => control.episode === index && control.kind !== 'start'
      )
      const starts = controls.filter(
        (control) => control.episode === index && control.kind === 'start'
      )
      const down = downs[0]
      const start = starts[0]
      for (const [name, rows] of [
        ['down', downs],
        ['start', starts],
      ]) {
        const control = rows[0]
        if (rows.length !== 1) {
          problems.push(`lifecycle-reference: missing original ${name} ${index}`)
          continue
        }
        if (
          control.status !== 'fulfilled' ||
          control.exitCode !== 0 ||
          !commandResult(control.result)
        )
          problems.push(`lifecycle-result: missing successful ${name} ${control.id}`)
        if (
          control.kind !== (name === 'down' ? command.kind : 'start') ||
          control.service !== command.service ||
          control.nodeId !== command.nodeId ||
          (name === 'start' && control.replaces !== down?.id)
        )
          problems.push(`lifecycle-reference: invalid original ${name} ${index}`)
        const mapping = mappings.length === 1 && mappings[0].nodes?.[command.service]
        if (
          control.semantics !== 'awaited-compose-service-process' ||
          !mapping ||
          !isDeepStrictEqual(mapping, control.mapping) ||
          mapping.service !== command.service ||
          mapping.nodeId !== command.nodeId ||
          !mapping.endpoint ||
          !mapping.containerId ||
          !['', 'no'].includes(mapping.restartPolicy) ||
          !Number.isInteger(mapping.process?.pid) ||
          mapping.process.pid <= 0 ||
          typeof mapping.process.startedAt !== 'string' ||
          !mapping.process.startedAt ||
          !mapping.observed ||
          !control.invoked ||
          !precedes(mapping.observed, control.invoked)
        )
          problems.push(`lifecycle-availability: missing original ${name} ${index}`)
        if (!control.invoked || !control.settled || !precedes(control.invoked, control.settled))
          problems.push(`lifecycle-order: invalid original ${name} ${index}`)
      }
      if (
        !down?.invoked ||
        down.invoked.at !== command.at ||
        down.invoked.order !== command.order ||
        !down.settled ||
        !start?.invoked ||
        !precedes(down.settled, start.invoked) ||
        bounce?.downAt !== down.settled.at
      )
        problems.push(`lifecycle-order: invalid original episode ${index}`)
      const restored = restorations.filter(({ episode }) => episode === index)
      const restoration = restored[0]
      const result = restoration?.result
      if (
        restored.length !== 1 ||
        restoration.error ||
        result?.service !== command.service ||
        result?.kind !== command.kind ||
        !Number.isFinite(result?.tDown) ||
        !Number.isFinite(result?.tRestored) ||
        result.tDown < (down?.settled?.at ?? Infinity) ||
        result.tRestored < (start?.settled?.at ?? Infinity) ||
        result.outageMs !== result.tRestored - result.tDown ||
        bounce?.restoredAt !== result.tRestored ||
        !(restoration?.settled?.at >= result.tRestored)
      )
        problems.push(`lifecycle-restoration: missing successful original ${index}`)
    }
    return problems
  }

  /** @param {AbortSignal} scope Original final scope. @returns {Promise<void>} Validates persisted evidence once at completion. */
  async requireArchive(scope) {
    scope.throwIfAborted()
    if (!this.archived || !this.archiveClosure || this.archiveIncomplete)
      throw new Error(
        `Required witness closure is incomplete: ${(this.archiveClosure?.problems ?? ['closure: unavailable']).join('; ')}`
      )
    const rows = this.archiveStorage?.read
      ? this.archiveStorage.read()
      : await readFile(evidencePath(EVIDENCE), 'utf8')
    await cooperativeAudit(this.requireArchiveWork(rows), scope)
  }

  /** @param {object[]|string} stored Persisted rows or JSONL. @returns {Generator<void, void, unknown>} Decoded closure, membership and content checks. */
  *requireArchiveWork(stored) {
    const rows = []
    const groups = new Map()
    let visited = 0
    for (const original of typeof stored === 'string' ? stored.split('\n') : stored) {
      if (visited++ % 100 === 0) yield
      if (!original) continue
      const row = typeof original === 'string' ? JSON.parse(original) : original
      rows.push(row)
      if (row.evidenceId) indexMember(groups, row.evidenceId, row)
    }
    const closureRow = rows.findLast(({ event }) => event === 'witness-closure')
    const end = rows.findLast(({ event }) => event === 'witness-closure-end')
    if (!closureRow || !end?.complete)
      throw new Error('archive-read: missing complete closure/footer')
    const decodedClosure = readEvidenceRows(groups.get(closureRow.evidenceId))
    const closure = Object.fromEntries(
      ['evidenceSchema', 'identities', 'frames', 'rows', 'omitted'].map((key) => [
        key,
        decodedClosure[key],
      ])
    )
    if (
      createHash('sha256').update(JSON.stringify(closure)).digest('hex') !==
      this.archiveClosure.sha256
    )
      throw new Error('archive-read: closure manifest changed')
    const decoded = []
    const changed = []
    for (const required of closure.rows) {
      yield
      if (!required.required) continue
      const members = groups.get(required.evidenceId)
      if (!members || members[0].event !== required.event)
        throw new Error(`archive-read: missing required ${required.event} ${required.evidenceId}`)
      const original = readEvidenceRows(members)
      const fields = Object.fromEntries(required.fields.map((key) => [key, original[key]]))
      decoded.push({ event: required.event, fields })
      if (createHash('sha256').update(JSON.stringify(fields)).digest('hex') !== required.sha256)
        changed.push(`archive-content: changed required ${required.event} ${required.evidenceId}`)
    }
    const bounces = decoded
      .filter(({ event }) => event === 'frozen-bounce')
      .map(({ fields }) => ({ ...fields, held: new Set(fields.held) }))
    const accounting = yield* freezeAccountingWork(this.entries, bounces, this.evidence)
    const problems = yield* this.archiveProblemsWork(decoded, new Set(closure.frames), accounting)
    if (problems.length || changed.length)
      throw new Error(`archive-read: ${[...problems, ...changed].join('; ')}`)
  }

  /**
   * Retains original acquisition/receipt provenance only for involved identities, with raw frame links.
   * @param {Set<string>} identities Failure identities and their matching public receipts.
   * @param {Set<number>} frameIds Relevant frames, extended with acquisition/receipt/epoch sources.
   * @param {Function} emit Collects rows for closure ordering.
   * @returns {Generator<void, void, unknown>} Steps emitting the original reference closure.
   */
  *archiveFailureIdentities(identities, frameIds, emit) {
    yield* epochSampleIndexWork(this.evidence.epochs)
    let visited = 0
    const cohorts = new Set()
    const exchanges = new Set()
    for (const { acquisition, ...entry } of this.entries) {
      if (visited++ % 100 === 0) yield
      if (!identities.has(entry.identity)) continue
      cohorts.add(acquisition?.id)
      exchanges.add(acquisition?.exchange)
      for (const frame of acquisition?.source?.frames ?? []) frameIds.add(frame.id)
      emit('handler-entry', { ...entry, acquisition: acquisition?.id })
    }
    for (const { source, ...acquisition } of this.evidence.acquisitions)
      if (cohorts.has(acquisition.id)) {
        emit('acquisition', acquisition)
        for (const frame of source.frames) frameIds.add(frame.id)
      }
    for (const bounce of this.snapshots)
      for (const cut of bounce.interruptions) if (cut.kind === 'failure') exchanges.add(cut.event)
    for (const extra of this.replay?.extras ?? []) {
      const witness = extra.incarnationWitness
      if (witness?.kind !== 'partition-transition') continue
      for (const key of ['before', 'after', 'transition', 'metadataTransition']) {
        if (witness[key] === undefined) continue
        frameIds.add(witness[key])
        for (const exchange of this.evidence.exchanges)
          if (exchange.frames.some(({ id }) => id === witness[key])) exchanges.add(exchange.id)
      }
    }
    for (const { frames, response, ...exchange } of this.evidence.exchanges) {
      if (visited++ % 100 === 0) yield
      if (
        !exchange.tickets.some(({ cohort }) => cohorts.has(cohort)) &&
        !exchanges.has(exchange.id)
      )
        continue
      for (const { id } of frames) frameIds.add(id)
      emit('share-exchange', {
        ...exchange,
        frames: frames.map(({ id }) => id),
        response: response && {
          errorCode: response.errorCode,
          errorMessage: response.errorMessage,
          responses: response.responses.map(({ topicId, partitions }) => ({
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
        },
      })
    }
    for (const outcome of this.evidence.outcomes)
      if (cohorts.has(outcome.cohort)) emit('acknowledgement-outcome', outcome)
    const chosen = new Set()
    for (const extra of this.replay?.extras ?? []) {
      const witness = extra.incarnationWitness
      if (witness?.kind === 'partition-transition')
        for (const key of ['before', 'after', 'transition', 'metadataTransition'])
          if (witness[key] !== undefined) chosen.add(witness[key])
    }
    const involvedSources = new Map(
      this.entries
        .filter(({ identity }) => identities.has(identity))
        .map((entry) => [
          JSON.stringify([
            entry.acquisition?.source?.frames[0]?.id,
            entry.topicId,
            entry.partition,
          ]),
          entry,
        ])
    )
    for (const entry of involvedSources.values()) {
      if (visited++ % 100 === 0) yield
      const frame = entry.acquisition?.source?.frames[0]
      if (!frame) continue
      const samples = sourceEpochSamples(entry, this.evidence.epochs, false)
      const before = samples.findLast(({ frame: sample }) => sample.responseAt < frame.at)
      const after = samples.find(({ frame: sample }) => sample.at > frame.responseAt)
      if (before) chosen.add(before.frame.id)
      if (after) chosen.add(after.frame.id)
      for (const sample of samples)
        if (sample.frame.at <= frame.responseAt && sample.frame.responseAt >= frame.at)
          chosen.add(sample.frame.id)
      for (const sample of samples)
        if (
          sample.row.leader !== entry.acquisition.source.nodeId &&
          this.snapshots.some(
            (bounce) =>
              bounce.nodeId === entry.acquisition.source.nodeId &&
              frame.responseAt < sample.frame.at &&
              bounce.at < sample.frame.at &&
              sample.frame.responseAt < bounce.restoredAt
          )
        )
          chosen.add(sample.frame.id)
    }
    for (const { frames, ...source } of this.evidence.epochs) {
      if (visited++ % 100 === 0) yield
      if (!frames.some(({ id }) => chosen.has(id))) continue
      for (const { id } of frames) frameIds.add(id)
      emit('epoch-source', { ...source, frames: frames.map(({ id }) => id) })
    }
    const sockets = new Set(
      this.entries
        .filter(({ identity }) => identities.has(identity))
        .map((entry) => entry.acquisition?.source?.frames[0]?.socket)
    )
    for (const frame of this.tap.allFrames) if (frameIds.has(frame.id)) sockets.add(frame.socket)
    const omittedEvents = []
    for (const event of this.tap.socketEvents) {
      if (visited++ % 100 === 0) yield
      if (!sockets.has(event.socket)) {
        omittedEvents.push(event)
        continue
      }
      for (const id of event.outstanding) frameIds.add(id)
      const { event: socketEvent, ...original } = event
      emit('socket-event', { ...original, socketEvent })
    }
    emit('socket-history', { events: omittedEvents })
    for (const retirement of this.evidence.retirements) emit('retirement', retirement)
    emit('lifecycle-mapping', { nodes: this.lifecycle, unavailable: this.lifecycleUnavailable })
    for (const { frames, ...close } of this.evidence.closes) {
      for (const frame of frames ?? []) frameIds.add(frame.id)
      emit('member-close', { ...close, frames: frames?.map(({ id }) => id) })
    }
    for (const restoration of this.evidence.restorations) emit('bounce-restoration', restoration)
    for (const observation of this.groupDeletion ?? []) emit('group-deletion', observation)
    for (const control of this.evidence.controls) emit('bounce-control', control)
    for (const command of this.evidence.commands) emit('bounce-command', command)
    for (const { held, inFlight, ...bounce } of this.snapshots) {
      for (const frame of inFlight) frameIds.add(frame.id)
      emit('frozen-bounce', { ...bounce, held: [...held], inFlight: inFlight.map(({ id }) => id) })
    }
    for (const frame of this.tap.allFrames)
      if (
        [15, 42, 66, 76].includes(frame.apiKey) ||
        (frame.label === 'admin' && [3, 32].includes(frame.apiKey))
      )
        frameIds.add(frame.id)
  }

  /**
   * Waits until every broker answers again and no bounce step is left unrestored.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async restoreBrokers(signal) {
    await pollUntil(
      signal,
      30000,
      async () =>
        this.bouncer.assertAllUp().then(
          () => true,
          () => false
        ),
      { label: 'Every broker answering again' }
    )
    if (this.readiness) await bounded(signal, 10000, () => waitForFullIsr(this.admin, [this.topic]))
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
        this.after = await assertShareCleanup(
          this.admin,
          [this.group],
          [this.topic],
          this.baseline,
          this.probe,
          { signal, disconnectAdmin: true }
        )
      }
    } finally {
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
      { label: 'The topic disappearing' }
    )
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the group is still listed after deletion
   */
  async deleteGroup(signal) {
    this.groupDeletion = []
    await pollUntil(
      signal,
      10000,
      async (scope) => {
        const observation = { id: this.groupDeletion.length, started: this.tap.stamp() }
        this.groupDeletion.push(observation)
        try {
          observation.result = await this.admin.deleteGroups([this.group])
          return true
        } catch (error) {
          observation.error = describeError(error, 8)
          observation.groups = error.groups?.map(({ groupId, errorCode, errorMessage, error }) => ({
            groupId,
            errorCode,
            errorMessage,
            error: describeError(error),
          }))
          const code = error.groups?.[0]?.errorCode ?? error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code !== 68) throw error
          try {
            observation.membership = await bounded(scope, 6000, () =>
              this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
            )
          } catch (failure) {
            observation.membershipError = describeError(failure)
          }
          return false
        } finally {
          observation.ended = this.tap.stamp()
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
    await this.requireArchive(scope)
    if (!this.cleanupProof?.completed) throw new Error('cleanup: missing completed evidence')
    if (this.cleanupProof.tainted !== false)
      throw new Error('taint: missing or failed cleanup evidence')
    if (performance.now() >= Math.min(this.deadline, this.finalDeadline ?? Infinity))
      throw new Error('deadline: selection deadline passed')
    expect(this.terminalVerified).toBe(true)
    expect([...this.tap.problems, ...this.producerEvidence.problems]).toEqual([])
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    const final = await bounded(scope, 10000, () => this.probe.snapshot())
    if (!this.row('cleanup', { snapshot: final, ...this.cleanupProof }))
      throw new Error('cleanup: evidence omitted')
    const { extras, perBounce } = this.replay
    scope.throwIfAborted()
    if (performance.now() >= Math.min(this.deadline, this.finalDeadline ?? Infinity))
      throw new Error('deadline: selection deadline passed')
    if (
      !this.row('case-complete', {
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
        terminalPosition: this.beforeTerminal,
        postVerifierPosition: this.afterTerminal,
        quietVerifier: { ...this.quiet, ...this.verifier },
        acknowledgementReceipts: this.evidence.outcomes,
        settlement: this.settlement,
        successfulIdentities: this.settlement.successful.length,
        unknownIdentities: this.settlement.unknown.length,
        resources: {
          before: this.resourceSummary(this.baseline),
          after: this.resourceSummary(this.after),
        },
        cleanup: {
          groupsAbsent: true,
          topicsAbsent: true,
          completedAt: Number(process.hrtime.bigint() / 1000000n),
        },
        readiness: { ...this.readiness, internal: this.shareState },
        produceStart: this.produceStart,
        produceEnd: this.produceEnd,
        crashes: [],
        resets: this.resets,
        bounces: this.bounceFacts.map((fact, index) => ({
          ...fact,
          held: [...this.snapshots[index].held].sort(),
          inFlight: this.snapshots[index].inFlight.map(({ id }) => id),
          duplicates: extras.filter(({ bounce }) => bounce === index),
          extraEntries: perBounce[index],
        })),
        shareFrames: this.tap.frames.length,
        wireVersions: wireVersionsOf(this.requests),
        config: { client: this.clientConfig, group: this.groupConfig },
      })
    )
      throw new Error('completion: evidence omitted')
  }
}

describe('share bounce wire tap', () => {
  const frame = (apiKey, correlationId) => {
    const header = Buffer.alloc(11)
    header.writeInt16BE(apiKey, 0)
    header.writeInt16BE(2, 2)
    header.writeInt32BE(correlationId, 4)
    header.writeInt16BE(1, 8)
    header.write('t', 10)
    const body = Buffer.concat([header, Buffer.from([0])])
    const bytes = Buffer.alloc(4 + body.length)
    bytes.writeInt32BE(body.length, 0)
    body.copy(bytes, 4)
    return bytes
  }
  const response = (correlationId) => {
    const bytes = Buffer.alloc(8)
    bytes.writeInt32BE(4, 0)
    bytes.writeInt32BE(correlationId, 4)
    return bytes
  }
  const open = () => {
    const written = []
    const socket = Object.assign(new EventEmitter(), {
      destroyed: false,
      write: (data) => {
        written.push(Buffer.from(data))
        return true
      },
    })
    const tap = new ShareWireTap(() => socket)
    return { tap, socket, written, wrapped: tap.factory('A')({}) }
  }

  it('splits fragmented and coalesced writes into whole frames and forwards every byte', () => {
    const splitter = new FrameSplitter()
    const first = frame(78, 7)
    const second = frame(79, 8)
    const stream = Buffer.concat([first, second])
    const seen = [
      stream.subarray(0, 3),
      stream.subarray(3, first.length + 2),
      stream.subarray(first.length + 2),
    ].flatMap((chunk) => splitter.push(chunk))
    expect(seen).toEqual([first, second])
  })

  it('keeps reused correlations separate on different sockets and later requests', () => {
    const sockets = [new EventEmitter(), new EventEmitter()]
    for (const socket of sockets) Object.assign(socket, { destroyed: false, write: () => true })
    let next = 0
    const tap = new ShareWireTap(() => sockets[next++])
    const first = tap.factory('first')({})
    const second = tap.factory('second')({})
    first.write(frame(76, 7))
    second.write(frame(76, 7))
    first.emit('data', response(7))
    expect(tap.pending()).toBe(1)
    expect(tap.allFrames[1].responseBytes).toBeUndefined()
    second.emit('data', response(7))
    first.write(frame(76, 7))
    expect(tap.allFrames.map(({ id }) => id)).toEqual([0, 1, 2])
    expect(tap.allFrames[2].responseBytes).toBeUndefined()
  })

  it('counts a request as pending until its answer, and only on an open socket', () => {
    const { tap, socket, written, wrapped } = open()
    const stream = Buffer.concat([frame(78, 7), frame(0, 8)])
    wrapped.write(stream.subarray(0, 5))
    wrapped.write(stream.subarray(5))
    expect(Buffer.concat(written)).toEqual(stream)
    expect(tap.pending()).toBe(2)
    expect(tap.inFlight().map(({ apiKey, correlationId }) => [apiKey, correlationId])).toEqual([
      [78, 7],
    ])
    expect(tap.frames.map(({ apiKey }) => apiKey)).toEqual([78])
    socket.emit('data', response(7))
    expect(tap.pending()).toBe(1)
    socket.destroyed = true
    expect(tap.pending()).toBe(0)
    expect(tap.socketCounts()).toEqual({ created: 1, destroyed: 1 })
  })
})

/**
 * Drives the same live capture from independently scripted primitive journals.
 * Call before the sensitivity mutation; final predicates never reconstruct missing snapshots.
 * @param {object} fixture Scripted handlers, receipts, command boundaries and primitive losses.
 * @returns {void}
 */
const captureFreezes = (fixture) => {
  const { evidence, entries, bounces } = fixture
  evidence.owner.entries = entries
  evidence.owner.snapshots = bounces
  evidence.commands = []
  for (const bounce of bounces) {
    bounce.interruptions = []
    bounce.carried = []
  }
  const journal = [
    ...interruptionJournal(evidence),
    ...bounces.map((event) => ({ kind: 'command', event })),
  ].sort((a, b) => {
    const first = interruptionEvent(a.kind, a.event)
    const second = interruptionEvent(b.kind, b.event)
    return first.at - second.at || first.order - second.order
  })
  for (const { kind, event } of journal) {
    if (kind !== 'command') {
      evidence.interrupt(kind, event)
      continue
    }
    const pending = unresolvedAt(entries, evidence, event)
    evidence.commands.push(
      immutable({
        index: event.index,
        at: event.at,
        order: event.order,
        nodeId: event.nodeId,
        kind: event.kind,
        service: event.service,
      })
    )
    event.carried = immutable(carriedAt(bounces, event, pending))
    event.held = new Set(pending.map(({ identity }) => identity))
    event.obligations = immutable(
      pending.map((entry) => obligation(entry, bounces, event.index, evidence, event))
    )
  }
}

/**
 * Adds an original correlated successful Accept, independently of any frozen membership.
 * @param {BounceEvidence} evidence Original request and receipt journals.
 * @param {object} entry Handler receiving the receipt.
 * @param {number} at Complete write time.
 * @returns {void}
 */
const successfulAcceptFixture = (evidence, entry, at) => {
  const batch = { firstOffset: entry.offset, lastOffset: entry.offset, acknowledgeTypes: [1] }
  const source = entry.acquisition.source
  const exchange = {
    id: 10000000 + entry.id,
    apiKey: 79,
    open: entry.acquisition.open,
    pool: entry.acquisition.pool,
    generation: entry.acquisition.generation,
    frames: [
      {
        id: 10000000 + entry.id,
        apiKey: 79,
        apiVersion: 2,
        at,
        responseAt: at + 1,
        socket: source.frames[0].socket,
        endpoint: source.endpoint,
        responseBytes: 'original-success',
        request: {
          groupId: entry.groupId,
          memberId: entry.memberId,
          topics: [
            {
              topicId: entry.topicId,
              partitions: [{ partitionIndex: entry.partition, acknowledgementBatches: [batch] }],
            },
          ],
        },
      },
    ],
    response: {
      errorCode: 0,
      responses: [
        { topicId: entry.topicId, partitions: [{ partitionIndex: entry.partition, errorCode: 0 }] },
      ],
    },
    tickets: [
      {
        cohort: entry.acquisition.id,
        ledger: entry.acquisition.ledger,
        ticket: 10000000 + entry.id,
        isRenewAck: false,
        batches: [batch],
      },
    ],
  }
  const outcome = {
    cohort: entry.acquisition.id,
    type: 'accept',
    firstOffset: entry.offset,
    lastOffset: entry.offset,
    error: null,
    at: at + 1,
  }
  evidence.exchanges.push(exchange)
  evidence.exchangesByCohort.set(entry.acquisition.id, [exchange])
  evidence.outcomes.push(outcome)
  evidence.outcomesByCohort.set(entry.acquisition.id, [outcome])
}

/**
 * Supplies independent primitive wire, authority and epoch observations for oracle sensitivity.
 * @param {{count?:number, sameLeader?:boolean, offset?:string}} [options] Count and leader identity of the replay.
 * @returns {object} Two acquisitions, one frozen episode and their original evidence streams.
 */
const replayFixture = ({ count = 1, sameLeader = false, offset = '0' } = {}) => {
  const topic = 'bounce-control'
  const topicId = '11111111-2222-3333-4444-555555555555'
  const groupId = 'bounce-control-group'
  const memberId = 'member'
  const makeEntry = (id, nodeId, at, deliveryCount) => {
    const range = { firstOffset: offset, lastOffset: offset, deliveryCount }
    const frame = {
      id,
      nodeId,
      apiKey: 78,
      apiVersion: 2,
      correlationId: 7,
      socket: id,
      endpoint: `node-${nodeId}`,
      at,
      responseAt: at + 1,
      responseBytes: 'original-response',
      request: {
        groupId,
        memberId,
        shareSessionEpoch: id,
        isRenewAck: false,
        topics: [{ topicId, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
      },
    }
    const source = {
      id,
      apiKey: 78,
      frames: [frame],
      open: id,
      pool: id,
      generation: id,
      groupId,
      memberId,
      nodeId,
      endpoint: frame.endpoint,
      model: [{ topicId, partition: 0 }],
      tickets: [],
      response: {
        errorCode: 0,
        responses: [
          {
            topicId,
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                acknowledgeErrorCode: 0,
                acquiredRecords: [range],
              },
            ],
          },
        ],
      },
    }
    const acquisition = {
      id,
      token: id,
      ledger: id,
      responseIdentity: id,
      exchange: source.id,
      source,
      open: id,
      pool: id,
      generation: id,
      adoptedAt: at + 2,
      topic,
      topicId,
      partition: 0,
      acquiredRecords: [range],
      topError: 0,
      fetchError: 0,
      acknowledgeError: 0,
    }
    const entry = {
      id,
      topic,
      topicId,
      groupId,
      memberId,
      partition: 0,
      offset,
      keyBytes: '6b',
      valueBytes: '76',
      deliveryCount,
      at: at + 3,
      acquisition,
    }
    entry.identity = shareRecordIdentity({
      topic,
      partition: 0,
      offset,
      key: Buffer.from('k'),
      value: Buffer.from('v'),
    })
    return entry
  }
  const old = makeEntry(0, 1, 1, 1)
  const next = makeEntry(1, sameLeader ? 1 : 2, 20, count)
  const evidence = new BounceEvidence({ tap: { socketEvents: [] }, captureProblems: [] })
  evidence.exchanges.push(old.acquisition.source, next.acquisition.source)
  evidence.acquisitions.push(old.acquisition, next.acquisition)
  evidence.retirements.push({
    id: 0,
    at: 8,
    nodeId: 1,
    groupId,
    memberId,
    open: 0,
    reason: 'connection',
    cause: 99,
  })
  const epoch = (id, nodeId, leaderEpoch, at) => ({
    nodeId,
    pool: id === 10 ? old.acquisition.pool : next.acquisition.pool,
    startedAt: at,
    endedAt: at + 1,
    frames: [
      {
        id,
        apiKey: 3,
        apiVersion: 12,
        socket: nodeId,
        correlationId: 7,
        at,
        responseAt: at + 1,
        responseBytes: 'original-metadata',
      },
    ],
    response: {
      topicMetadata: [
        {
          topicId,
          topic,
          partitionMetadata: [
            { partitionId: 0, partitionErrorCode: 0, leader: nodeId, leaderEpoch },
          ],
        },
      ],
    },
  })
  evidence.epochs.push(epoch(10, 1, 1, 5), epoch(11, next.acquisition.source.nodeId, 2, 12))
  const bounce = {
    index: 0,
    at: 7,
    downAt: 9,
    restoredAt: 18,
    nodeId: 1,
    held: new Set([old.identity]),
    obligations: [{ identity: old.identity, entry: old.id }],
    inFlight: [],
  }
  const fixture = {
    old,
    next,
    entries: [old, next],
    evidence,
    bounces: [bounce],
    corpus: [old.identity],
    target: { groupId, topic, partitions: [{ partition: 0, floor: '0', end: '1' }] },
  }
  captureFreezes(fixture)
  return fixture
}

/**
 * Scripts one real ledger outcome for a written or unwritten terminal decision.
 * Detaches recorded batches from the immutable ledger ticket before fixture remapping.
 * @param {object} fixture Independent replay fixture.
 * @param {{written?:boolean, applied?:boolean, type?:string, reason?:string, writtenAt?:number, lostAt?:number}} [options] Fault script.
 * @returns {object} Complete independent terminal observations, with the actual outcome journal.
 */
const lostAcceptFixture = (
  fixture,
  {
    written = true,
    applied = true,
    type = 'accept',
    reason = 'session-lost',
    writtenAt = 6,
    lostAt = 8,
  } = {}
) => {
  const { ShareAckLedger } = require('../shareAckLedger')
  const { ShareRecordWalk } = require('../shareRecordWalk')
  const entry = fixture.old
  const token = Symbol('original-control-acquisition')
  const ledger = new ShareAckLedger({
    topic: entry.topic,
    topicId: entry.topicId,
    partition: 0,
    token,
    sentAt: 1,
    lockDuration: 30000,
    walk: ShareRecordWalk.walk({
      acquiredRecords: entry.acquisition.acquiredRecords,
      messages: [{ offset: entry.offset, key: Buffer.from('k'), value: Buffer.from('v') }],
    }),
  })
  if (type === 'reject') ledger.decide(entry.offset, 'reject')
  if (type !== 'renew') ledger.complete([entry.offset], type === 'release' ? 'release' : 'accept')
  const { ticket } = ledger.prepare({
    token,
    ...(type === 'renew' ? { renewOffsets: [entry.offset] } : {}),
  })
  const cause = new Error('designated connection closed')
  cause.name = 'KafkaJSConnectionClosedError'
  const results = ledger.invalidate(reason, null, cause)
  expect(ledger.invalidate(reason, null, cause)).toEqual([])
  const outcomes = results.map((outcome) => ({
    ...outcome,
    cohort: entry.acquisition.id,
    at: lostAt + 0.3,
    typed: outcome.error instanceof KafkaJSShareAcknowledgementError,
    cause: 99 + entry.id,
    error: describeError(outcome.error),
  }))
  fixture.evidence.outcomes.push(...outcomes)
  fixture.evidence.outcomesByCohort.set(entry.acquisition.id, outcomes)
  const request = {
    ...entry.acquisition.source.frames[0].request,
    topics: [
      {
        topicId: entry.topicId,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: ticket.acknowledgementBatches.map((batch) => ({
              ...batch,
              acknowledgeTypes: [...batch.acknowledgeTypes],
            })),
          },
        ],
      },
    ],
    isRenewAck: ticket.isRenewAck,
  }
  const exchange = {
    id: 100 + entry.id,
    prepared: { at: (entry.at + writtenAt) / 2 },
    apiKey: 79,
    nodeId: entry.acquisition.source.nodeId,
    groupId: entry.groupId,
    memberId: entry.memberId,
    endpoint: entry.acquisition.source.endpoint,
    open: entry.acquisition.open,
    pool: entry.acquisition.pool,
    generation: entry.acquisition.generation,
    model: [],
    frames: written
      ? [
          {
            id: 100 + entry.id,
            apiKey: 79,
            apiVersion: 2,
            request,
            at: writtenAt,
            lostAt,
            socket: entry.acquisition.source.frames[0].socket,
            endpoint: entry.acquisition.source.endpoint,
          },
        ]
      : [],
    failedAt: lostAt + 0.1,
    cause: 99 + entry.id,
    error: describeError(cause),
    tickets: [
      {
        cohort: entry.acquisition.id,
        ticket: 100 + entry.id,
        ledger: entry.acquisition.ledger,
        batches: ticket.acknowledgementBatches.map((batch) => ({
          ...batch,
          acknowledgeTypes: [...batch.acknowledgeTypes],
        })),
        isRenewAck: ticket.isRenewAck,
      },
    ],
  }
  fixture.evidence.exchanges.push(exchange)
  fixture.evidence.exchangesByCohort.set(entry.acquisition.id, [exchange])
  fixture.evidence.owner.tap.socketEvents.push({
    id: fixture.evidence.owner.tap.socketEvents.length,
    socket: entry.acquisition.source.frames[0].socket,
    at: lostAt,
    event: 'end',
    outstanding: exchange.frames.map(({ id }) => id),
  })
  const retirement = fixture.evidence.retirements.find(
    ({ open }) => open === entry.acquisition.open
  )
  retirement.at = lostAt + 0.2
  retirement.cause = 99 + entry.id
  captureFreezes(fixture)
  const listing = [
    {
      topic: entry.topic,
      topicId: entry.topicId,
      partitions: [
        { partition: 0, startOffset: '0', lag: applied ? '0' : '1', errorCode: 0, leaderEpoch: 2 },
      ],
    },
  ]
  const observation = {
    groupId: entry.groupId,
    topicId: entry.topicId,
    named: listing,
    all: JSON.parse(JSON.stringify(listing)),
    ends: [{ partition: 0, high: '1' }],
  }
  return {
    corpus: fixture.corpus,
    raw: [...fixture.corpus],
    entries: [entry],
    evidence: fixture.evidence,
    bounces: fixture.bounces,
    limits: [5, 5, 5],
    target: fixture.target,
    before: observation,
    after: JSON.parse(JSON.stringify(observation)),
    quiet: {
      activeAt: 10,
      endedAt: 5011,
      delivered: [],
      assignment: [{ topic: entry.topic, partitions: [0] }],
      events: [
        { name: 'GROUP_JOIN', at: 9, payload: {} },
        { name: 'REQUEST', at: 5010, payload: { apiName: 'ShareFetch' } },
      ],
    },
  }
}

/**
 * Builds independent corpus, acquisition and outcome streams at the replay/unknown boundaries.
 * Each original socket loss retains all requests outstanding on that socket.
 * @param {number} count Distinct identities to exercise.
 * @param {number|number[]} perEpisode New identities in each sequential episode.
 * @param {boolean} unknown Whether to script lost Accepts instead of higher-count replays.
 * @returns {object} Combined evidence without deriving expected multiplicities from dispatches.
 */
const budgetFixture = (count, perEpisode, unknown) => {
  const combined = {
    entries: [],
    corpus: [],
    bounces: [],
    evidence: new BounceEvidence({ tap: { socketEvents: [] }, captureProblems: [] }),
  }
  for (let index = 0; index < count; index++) {
    const f = replayFixture({ count: 2, offset: String(index) })
    const proof = unknown ? lostAcceptFixture(f) : null
    const episode = Array.isArray(perEpisode)
      ? perEpisode.findIndex(
          (_count, position) =>
            index < perEpisode.slice(0, position + 1).reduce((sum, size) => sum + size, 0)
        )
      : Math.floor(index / perEpisode)
    const shift = episode * 100
    for (const entry of f.entries) {
      entry.id += 2 * index
      entry.offset = String(index)
      entry.at += shift
      entry.acquisition.id = entry.id
      entry.acquisition.responseIdentity += 1000 * index
      entry.acquisition.exchange += 1000 * index
      entry.acquisition.token += 2 * index
      entry.acquisition.ledger += 2 * index
      entry.acquisition.open += 2 * episode
      entry.acquisition.pool += 2 * episode
      entry.acquisition.generation += 2 * episode
      entry.acquisition.adoptedAt += shift
      entry.acquisition.acquiredRecords[0].firstOffset = String(index)
      entry.acquisition.acquiredRecords[0].lastOffset = String(index)
      entry.identity = shareRecordIdentity({
        topic: entry.topic,
        partition: 0,
        offset: entry.offset,
        key: Buffer.from('k'),
        value: Buffer.from('v'),
      })
    }
    for (const exchange of f.evidence.exchanges) {
      exchange.id += 1000 * index
      exchange.open += 2 * episode
      exchange.pool += 2 * episode
      exchange.generation += 2 * episode
      if (exchange.failedAt !== undefined) exchange.failedAt += shift
      if (exchange.prepared) exchange.prepared.at += shift
      for (const ticket of exchange.tickets) {
        ticket.cohort += 2 * index
        ticket.ledger += 2 * index
      }
      for (const frame of exchange.frames) {
        frame.id += 1000 * index
        frame.socket += 2 * episode
        frame.at += shift
        if (frame.responseAt !== undefined) frame.responseAt += shift
        if (frame.lostAt !== undefined) frame.lostAt += shift
      }
    }
    for (const outcome of f.evidence.outcomes) {
      outcome.cohort += 2 * index
      outcome.at += shift
      outcome.firstOffset = String(index)
      outcome.lastOffset = String(index)
    }
    const frozen = f.bounces[0]
    frozen.at += shift
    frozen.downAt += shift
    frozen.restoredAt += shift
    frozen.index = episode
    frozen.held = new Set([f.old.identity])
    frozen.obligations = [{ entry: f.old.id, identity: f.old.identity }]
    if (unknown) frozen.inFlight = f.evidence.exchanges.at(-1).frames.map((frame) => ({ ...frame }))
    for (const retirement of f.evidence.retirements) {
      retirement.id = index
      retirement.order = index
      retirement.open += 2 * episode
      retirement.at += shift
    }
    for (const event of f.evidence.owner.tap.socketEvents) {
      event.id = index
      event.order = index
      event.socket += 2 * episode
      event.at += shift
      event.outstanding = event.outstanding.map((id) => id + 1000 * index)
      const loss = combined.evidence.owner.tap.socketEvents.find(
        ({ socket, at }) => socket === event.socket && at === event.at
      )
      if (loss) loss.outstanding.push(...event.outstanding)
      else combined.evidence.owner.tap.socketEvents.push(event)
    }
    if (!combined.bounces[episode])
      combined.bounces[episode] = { ...frozen, held: new Set(), obligations: [], inFlight: [] }
    const episodeProof = combined.bounces[episode]
    episodeProof.held.add(f.old.identity)
    episodeProof.obligations.push(...frozen.obligations)
    episodeProof.inFlight.push(...frozen.inFlight)
    combined.corpus.push(f.old.identity)
    combined.entries.push(...(proof ? proof.entries : f.entries))
    combined.evidence.exchanges.push(...f.evidence.exchanges)
    combined.evidence.acquisitions.push(...f.evidence.acquisitions)
    combined.evidence.outcomes.push(...f.evidence.outcomes)
    combined.evidence.retirements.push(...f.evidence.retirements)
    for (const outcome of f.evidence.outcomes)
      combined.evidence.outcomesByCohort.set(outcome.cohort, [outcome])
    for (const exchange of f.evidence.exchanges)
      for (const ticket of exchange.tickets)
        combined.evidence.exchangesByCohort.set(ticket.cohort, [exchange])
  }
  if (!unknown)
    for (const entry of combined.entries.filter(({ id }) => id % 2 === 1))
      successfulAcceptFixture(combined.evidence, entry, entry.at + 1)
  captureFreezes(combined)
  return combined
}

/**
 * Scripts six unknown pools and the remaining real Accepts against a fixed three-partition corpus.
 * @returns {object} Full terminal proof with 1800 unknown and 28200 successful identities.
 */
const terminalBudgetFixture = () => {
  const fixture = budgetFixture(MAX_REPLAY, MAX_HELD, true)
  const { evidence } = fixture
  evidence.acquisitions = fixture.entries.map(({ acquisition }) => acquisition)
  evidence.exchanges = evidence.exchanges.filter(
    (exchange) =>
      evidence.acquisitions.some(({ source }) => source === exchange) || exchange.tickets.length
  )
  for (const [index, entry] of fixture.entries.entries()) {
    const partition = Math.floor(index / MAX_HELD) % PARTITIONS
    const offset = String(
      Math.floor(index / (MAX_HELD * PARTITIONS)) * MAX_HELD + (index % MAX_HELD)
    )
    entry.partition = partition
    entry.offset = offset
    entry.acquisition.partition = partition
    const source = entry.acquisition.source
    source.model[0].partition = partition
    source.response.responses[0].partitions[0].partitionIndex = partition
    source.frames[0].request.topics[0].partitions[0].partitionIndex = partition
    entry.acquisition.acquiredRecords[0].firstOffset = offset
    entry.acquisition.acquiredRecords[0].lastOffset = offset
    for (const exchange of evidence.exchangesByCohort.get(entry.acquisition.id)) {
      exchange.frames[0].request.topics[0].partitions[0].partitionIndex = partition
      for (const ticket of exchange.tickets)
        for (const batch of ticket.batches) batch.firstOffset = batch.lastOffset = offset
      for (const batch of exchange.frames[0].request.topics[0].partitions[0].acknowledgementBatches)
        batch.firstOffset = batch.lastOffset = offset
    }
    for (const outcome of evidence.outcomesByCohort.get(entry.acquisition.id))
      outcome.firstOffset = outcome.lastOffset = offset
    entry.identity = shareRecordIdentity({
      topic: entry.topic,
      partition,
      offset,
      key: Buffer.from('k'),
      value: Buffer.from('v'),
    })
  }
  const batchesPerPartition = (RECORDS - MAX_REPLAY) / PARTITIONS / BURST_SIZE
  for (let batch = 0; batch < (RECORDS - MAX_REPLAY) / BURST_SIZE; batch++) {
    const partition = Math.floor(batch / batchesPerPartition)
    const first = MAX_REPLAY / PARTITIONS + (batch % batchesPerPartition) * BURST_SIZE
    const last = first + BURST_SIZE - 1
    const entry = replayFixture({ offset: String(first) }).old
    const acquisition = entry.acquisition
    const source = acquisition.source
    const frame = source.frames[0]
    const id = 100000000 + batch
    const at = 600 + batch * 6
    Object.assign(source, { id, open: id, pool: id, generation: id })
    Object.assign(frame, { id, socket: id, at, responseAt: at + 1 })
    Object.assign(acquisition, {
      id,
      token: id,
      ledger: id,
      exchange: id,
      responseIdentity: id,
      open: id,
      pool: id,
      generation: id,
      partition,
      adoptedAt: at + 2,
    })
    source.model[0].partition = partition
    source.response.responses[0].partitions[0].partitionIndex = partition
    frame.request.topics[0].partitions[0].partitionIndex = partition
    acquisition.acquiredRecords[0].lastOffset = String(last)
    Object.assign(entry, { id: 2 * MAX_REPLAY + batch * BURST_SIZE, partition, at: at + 3 })
    evidence.acquisitions.push(acquisition)
    evidence.exchanges.push(source)
    for (let offset = first; offset <= last; offset++) {
      const handler = { ...entry, id: entry.id + offset - first, offset: String(offset) }
      handler.identity = shareRecordIdentity({
        topic: entry.topic,
        partition,
        offset: handler.offset,
        key: Buffer.from('k'),
        value: Buffer.from('v'),
      })
      fixture.entries.push(handler)
    }
    successfulAcceptFixture(evidence, entry, at + 4)
    const receipt = evidence.exchangesByCohort.get(acquisition.id)[0]
    receipt.frames[0].request.topics[0].partitions[0].acknowledgementBatches[0].lastOffset =
      String(last)
    receipt.tickets[0].batches[0].lastOffset = String(last)
    evidence.results(entry)[0].lastOffset = String(last)
  }
  captureFreezes(fixture)
  const { topic, topicId, groupId } = fixture.entries[0]
  const corpus = Array.from({ length: RECORDS }, (_value, index) =>
    shareRecordIdentity({
      topic,
      partition: Math.floor(index / 10000),
      offset: String(index % 10000),
      key: Buffer.from('k'),
      value: Buffer.from('v'),
    })
  )
  const partitions = Array.from({ length: PARTITIONS }, (_value, partition) => ({
    partition,
    floor: '0',
    end: '10000',
  }))
  const rows = [
    {
      topic,
      topicId,
      partitions: partitions.map(({ partition }) => ({
        partition,
        startOffset: '0',
        lag: '0',
        errorCode: 0,
        leaderEpoch: 2,
      })),
    },
  ]
  const position = {
    groupId,
    topicId,
    named: rows,
    all: JSON.parse(JSON.stringify(rows)),
    ends: partitions.map(({ partition }) => ({ partition, high: '10000' })),
  }
  return {
    ...fixture,
    corpus,
    raw: [...corpus],
    target: { groupId, topic, partitions },
    limits: [5, 5, 5],
    before: position,
    after: JSON.parse(JSON.stringify(position)),
    quiet: {
      activeAt: 3000,
      endedAt: 8001,
      delivered: [],
      assignment: [{ topic, partitions: [0, 1, 2] }],
      events: [
        { name: 'GROUP_JOIN', at: 2999, payload: {} },
        { name: 'REQUEST', at: 8000, payload: { apiName: 'ShareFetch' } },
      ],
    },
  }
}

describe('share bounce acquisition and settlement sensitivity', () => {
  const audit = ({ entries, bounces, corpus, evidence }) =>
    auditReplay(entries, bounces, new Set(corpus), evidence)

  /** @param {object} result Replay result. @returns {string[]} Explicit failed witness components. */
  const failedComponents = (result) =>
    result.problems.flatMap((problem) => {
      const marker = 'has no proved delivery-count progression: '
      const offset = problem.indexOf(marker)
      return offset < 0
        ? []
        : JSON.parse(problem.slice(offset + marker.length)).failedWitnessComponents
    })

  /** @returns {object} Same-node stop/restart with late entry and no old numeric upper bound. */
  const restartFixture = () => {
    const fixture = replayFixture({ sameLeader: true })
    const { old, next, bounces, evidence } = fixture
    Object.assign(bounces[0], { at: 3, kind: 'stop', service: 'kafka1' })
    Object.assign(old.acquisition.source.frames[0], {
      at: 3.1,
      responseAt: 3.2,
      socketOpened: { at: 0.5 },
    })
    old.acquisition.adoptedAt = 3.3
    next.acquisition.source.frames[0].socketOpened = { at: 12 }
    evidence.epochs[0].frames[0].at = 3.1
    evidence.epochs[0].frames[0].responseAt = 3.15
    const away = JSON.parse(JSON.stringify(evidence.epochs[0]))
    Object.assign(away.frames[0], { id: 50, at: 5, responseAt: 6 })
    Object.assign(away.response.topicMetadata[0].partitionMetadata[0], {
      leader: 2,
      leaderEpoch: 2,
    })
    evidence.epochs.push(away)
    evidence.epochs[1].response.topicMetadata[0].partitionMetadata[0].leaderEpoch = 3
    captureFreezes(fixture)
    const mapping = {
      service: 'kafka1',
      nodeId: 1,
      endpoint: 'node-1',
      containerId: 'owned-container',
      restartPolicy: 'no',
      observed: { at: 0.1 },
      process: { pid: 100, startedAt: 'original' },
    }
    evidence.owner.lifecycle = { kafka1: immutable(mapping) }
    evidence.controls = [
      {
        id: 0,
        episode: 0,
        kind: 'stop',
        service: 'kafka1',
        nodeId: 1,
        mapping,
        semantics: 'awaited-compose-service-process',
        invoked: { at: 3 },
        settled: { at: 9 },
        status: 'fulfilled',
        exitCode: 0,
        result: { stdout: '', stderr: 'Stopped' },
      },
      {
        id: 1,
        episode: 0,
        kind: 'start',
        service: 'kafka1',
        nodeId: 1,
        mapping,
        semantics: 'awaited-compose-service-process',
        invoked: { at: 10 },
        settled: { at: 11 },
        status: 'fulfilled',
        exitCode: 0,
        result: { stdout: '', stderr: 'Started' },
        replaces: 0,
      },
    ]
    return fixture
  }

  it('admits a late count-one same-node return using its actual stop/start results', () => {
    const fixture = restartFixture()
    const result = audit(fixture)
    expect(fixture.bounces[0].held.size).toBe(0)
    expect(fixture.bounces[0].interruptions[0].lateEntries).toContain(fixture.old.id)
    expect(result.problems).toEqual([])
    expect(result.extras[0].incarnationWitness).toMatchObject({
      kind: 'serving-process-restarted',
      down: 0,
      start: 1,
    })
    expect(result.extras[0].epochWitness).toBeUndefined()
  })

  it('admits different-node service before graceful stop returns without old Metadata', () => {
    const fixture = restartFixture()
    fixture.evidence.epochs = []
    fixture.evidence.controls[0].settled.at = 25
    fixture.evidence.controls[1].invoked.at = 26
    fixture.evidence.controls[1].settled.at = 27
    fixture.bounces[0].restoredAt = 28
    const source = fixture.next.acquisition.source
    source.nodeId = 2
    source.endpoint = 'node-2'
    source.frames[0].endpoint = 'node-2'
    source.frames[0].nodeId = 2
    expect(audit(fixture).problems).toEqual([])
    expect(audit(fixture).extras[0].incarnationWitness.kind).toBe('different-serving-nodes')
    source.response.responses[0].partitions[0].errorCode = 6
    expect(audit(fixture).problems).toContain(
      `${fixture.next.identity} lacks an adopted wire acquisition`
    )
    expect(failedComponents(audit(fixture))).toContain(
      'eligibility: missing distinct successful original acquisitions'
    )
  })

  it.each([
    ['failed stop', (f) => (f.evidence.controls[0].status = 'rejected')],
    ['nonzero exit', (f) => (f.evidence.controls[0].exitCode = 1)],
    ['unfinished stop', (f) => delete f.evidence.controls[0].settled],
    ['invocation only', (f) => delete f.evidence.controls[0].result],
    ['pause', (f) => (f.evidence.controls[0].kind = 'pause')],
    ['no start', (f) => f.evidence.controls.pop()],
    ['failed start', (f) => (f.evidence.controls[1].status = 'rejected')],
    ['wrong replacement', (f) => (f.evidence.controls[1].replaces = 99)],
    [
      'wrong container',
      (f) =>
        (f.evidence.controls[1].mapping = {
          ...f.evidence.controls[1].mapping,
          containerId: 'foreign',
        }),
    ],
    ['wrong node', (f) => (f.evidence.controls[0].nodeId = 3)],
    ['source-node mismatch', (f) => (f.next.acquisition.source.frames[0].nodeId = 3)],
    [
      'missing process identity',
      (f) => {
        delete f.evidence.controls[0].mapping.process
        f.evidence.owner.lifecycle.kafka1 = immutable(f.evidence.controls[0].mapping)
      },
    ],
    [
      'wrong endpoint',
      (f) => {
        f.next.acquisition.source.endpoint = 'other'
        f.next.acquisition.source.frames[0].endpoint = 'other'
      },
    ],
    [
      'automatic restart',
      (f) => {
        f.evidence.controls[0].mapping.restartPolicy = 'always'
        f.evidence.owner.lifecycle.kafka1 = immutable(f.evidence.controls[0].mapping)
      },
    ],
    ['old socket survives', (f) => (f.next.acquisition.source.frames[0].socketOpened.at = 2)],
    ['write before restoration', (f) => (f.evidence.controls[1].settled.at = 22)],
    [
      'delayed old response',
      (f) => {
        f.old.acquisition.source.frames[0].responseAt = 13
        f.old.acquisition.adoptedAt = 13.1
        f.old.at = 13.2
        f.evidence.retirements[0].at = 14
      },
    ],
    [
      'restarted original socket',
      (f) => {
        Object.assign(f.old.acquisition.source.frames[0], {
          socketOpened: { at: 12 },
          at: 12.1,
          responseAt: 12.2,
        })
        f.old.acquisition.adoptedAt = 12.3
        f.old.at = 12.4
        f.evidence.retirements[0].at = 14
      },
    ],
    [
      'contradictory start',
      (f) =>
        f.evidence.controls.push({
          ...f.evidence.controls[1],
          id: 2,
          invoked: { at: 14 },
          settled: { at: 15 },
        }),
    ],
  ])('rejects lifecycle evidence with %s', (name, mutate) => {
    const fixture = restartFixture()
    expect(audit(fixture).problems).toEqual([])
    mutate(fixture)
    captureFreezes(fixture)
    const component = [
      'failed stop',
      'nonzero exit',
      'unfinished stop',
      'invocation only',
      'pause',
      'wrong node',
      'automatic restart',
    ].includes(name)
      ? 'control: missing successful stop/kill of the original serving node'
      : name === 'source-node mismatch'
        ? 'eligibility: missing distinct successful original acquisitions'
        : 'lifecycle: missing termination/replacement mapping or original/replacement socket order'
    if (name !== 'source-node mismatch') expect(fixture.entries.every(validAcquisition)).toBe(true)
    expect(failedComponents(audit(fixture))).toContain(component)
  })

  it('rejects a corrupt lifecycle mapping independently of missing process identity', () => {
    const fixture = restartFixture()
    fixture.evidence.controls[0].mapping = {
      ...fixture.evidence.controls[0].mapping,
      containerId: 'foreign',
    }
    expect(fixture.evidence.controls[0].mapping.process).toEqual({
      pid: 100,
      startedAt: 'original',
    })
    expect(failedComponents(audit(fixture))).toContain(
      'control: missing successful stop/kill of the original serving node'
    )
  })

  it('rejects reversed acquisition chronology while both acquisitions remain valid', () => {
    const fixture = restartFixture()
    Object.assign(fixture.old.acquisition.source.frames[0], { at: 19, responseAt: 20.5 })
    fixture.old.acquisition.adoptedAt = 21
    fixture.old.at = 21.5
    fixture.evidence.retirements[0].at = 22
    fixture.bounces[0].restoredAt = 24
    captureFreezes(fixture)
    expect(fixture.entries.every(validAcquisition)).toBe(true)
    expect(
      changedIncarnation(fixture.old, fixture.next, fixture.bounces[0], fixture.evidence).failed
    ).toContain('chronology: old response and original interruption must precede new write')
    expect(audit(fixture).problems).toContain(
      `${fixture.next.identity} has no original fault obligation`
    )
  })

  it('admits a socket opened during graceful stop in the terminated process interval', () => {
    const fixture = restartFixture()
    fixture.old.acquisition.source.frames[0].socketOpened.at = 3.05
    expect(audit(fixture).problems).toEqual([])
    expect(audit(fixture).extras[0].incarnationWitness.kind).toBe('serving-process-restarted')
  })

  it.each([false, true])(
    'rejects contradictory retained partition observations for causal witness %s',
    (differentNode) => {
      const fixture = restartFixture()
      if (differentNode) {
        Object.assign(fixture.next.acquisition.source, { nodeId: 2, endpoint: 'node-2' })
        Object.assign(fixture.next.acquisition.source.frames[0], { nodeId: 2, endpoint: 'node-2' })
        fixture.evidence.epochs = [fixture.evidence.epochs[0], fixture.evidence.epochs[2]]
      }
      expect(audit(fixture).problems).toEqual([])
      const conflict = JSON.parse(JSON.stringify(fixture.evidence.epochs.at(-1)))
      conflict.frames[0].id = 99
      conflict.response.topicMetadata[0].partitionMetadata[0].leader = 3
      fixture.evidence.epochs.push(conflict)
      expect(failedComponents(audit(fixture))).toEqual([
        'partition-observations: conflicting leaders at one epoch',
      ])
    }
  )

  it.each([1, 2])('requires a boundary for same-process count %s to one', (count) => {
    const fixture = restartFixture()
    fixture.old.deliveryCount = count
    fixture.old.acquisition.acquiredRecords[0].deliveryCount = count
    expect(audit(fixture).problems).toEqual([])
    fixture.evidence.controls = []
    expect(failedComponents(audit(fixture))).toContain(
      'control: missing successful stop/kill of the original serving node'
    )
  })

  it.each([undefined, 0, -1, 1.5, 32768])(
    'rejects invalid original or replacement count %s across restart',
    (count) => {
      for (const which of ['old', 'next']) {
        const fixture = restartFixture()
        fixture[which].deliveryCount = count
        fixture[which].acquisition.acquiredRecords[0].deliveryCount = count
        expect(audit(fixture).problems).toContain(
          `${fixture[which].identity} has an invalid delivery count`
        )
      }
    }
  )

  it('cannot reuse a restart boundary for a second equal-count replacement acquisition', () => {
    const fixture = restartFixture()
    expect(audit(fixture).problems).toEqual([])
    const third = JSON.parse(JSON.stringify(fixture.next))
    third.id = 2
    third.at = 33
    Object.assign(third.acquisition, { id: 2, exchange: 2, adoptedAt: 32 })
    Object.assign(third.acquisition.source, { id: 2 })
    Object.assign(third.acquisition.source.frames[0], {
      id: 2,
      socket: 2,
      socketOpened: { at: 28 },
      at: 30,
      responseAt: 31,
    })
    // Give the replacement predecessor its own valid original interruption; only the old barrier is reused.
    fixture.entries = [fixture.next, third]
    fixture.evidence.acquisitions = fixture.entries.map(({ acquisition }) => acquisition)
    fixture.evidence.exchanges = fixture.entries.map(({ acquisition }) => acquisition.source)
    fixture.evidence.retirements = [
      { ...fixture.evidence.retirements[0], open: fixture.next.acquisition.open, at: 24 },
    ]
    fixture.bounces[0].restoredAt = 35
    captureFreezes(fixture)
    expect(fixture.entries.every(validAcquisition)).toBe(true)
    expect(
      matchingInterruption(fixture.next, fixture.bounces[0], fixture.evidence, 30)
    ).not.toBeNull()
    const failed = failedComponents(audit(fixture))
    expect(failed).toContain(
      'lifecycle: missing termination/replacement mapping or original/replacement socket order'
    )
    expect(failed).not.toContain(
      'chronology: old response and original interruption must precede new write'
    )
    expect(failed).not.toContain(
      'control: missing successful stop/kill of the original serving node'
    )
    expect(failed).not.toContain('eligibility: missing distinct successful original acquisitions')
  })

  /** @param {Encoder|Buffer} body Packet body. @returns {Buffer} Length-prefixed original wire packet. */
  const packet = (body) =>
    new Encoder()
      .writeInt32(body.buffer?.length ?? body.length)
      .writeBuffer(Buffer.isBuffer(body) ? body : body.buffer).buffer

  /** @returns {Promise<object>} Original Produce attempts, real public receipt mapping and two physical copies. */
  const appendFixture = async () => {
    const fixture = restartFixture()
    const instance = new ShareBounceCase()
    const { RecordBatch } = require('../../../protocol/recordBatch/v0')
    const Record = require('../../../protocol/recordBatch/record/v0')
    const message = {
      partition: 0,
      key: Buffer.from('k'),
      value: Buffer.from('v'),
      timestamp: RUN_TIMESTAMP,
      headers: { source: Buffer.from('original') },
    }
    /** @param {object} [changes] One mutation. @returns {Promise<Buffer>} Complete checksummed batch. */
    const batch = async (changes = {}) =>
      (
        await RecordBatch({
          firstOffset: '0',
          firstTimestamp: Number(RUN_TIMESTAMP),
          maxTimestamp: Number(RUN_TIMESTAMP),
          producerId: '0',
          producerEpoch: changes.producerEpoch ?? 0,
          firstSequence: 0,
          records: [Record({ ...message, ...changes })],
        })
      ).buffer
    const records = await batch()
    Object.assign(instance, {
      topic: fixture.old.topic,
      topicId: fixture.old.topicId,
      entries: fixture.entries,
    })
    const send = {
      id: 0,
      outcome: 'success',
      original: immutable({
        topic: instance.topic,
        topicId: instance.topicId,
        messages: [originalMessage(message)],
      }),
      results: [{ partition: 0, errorCode: 0, baseOffset: '0' }],
      receiptIdentities: [],
    }
    instance.sends.push(send)
    expect(instance.recordAppends([message], send.results, send)).toBeNull()
    /** @param {Buffer} bytes Original batch. @param {number} correlation Socket-scoped correlation. @returns {Buffer} Produce request. */
    const request = (bytes, correlation) =>
      packet(
        new Encoder()
          .writeInt16(0)
          .writeInt16(8)
          .writeInt32(correlation)
          .writeString('producer-control')
          .writeString(null)
          .writeInt16(-1)
          .writeInt32(30000)
          .writeArray([
            new Encoder()
              .writeString(instance.topic)
              .writeArray([new Encoder().writeInt32(0).writeBytes(bytes)]),
          ])
      )
    const response = (correlation) =>
      packet(
        new Encoder()
          .writeInt32(correlation)
          .writeArray([
            new Encoder()
              .writeString(instance.topic)
              .writeArray([
                new Encoder()
                  .writeInt32(0)
                  .writeInt16(0)
                  .writeInt64('0')
                  .writeInt64('-1')
                  .writeInt64('0')
                  .writeArray([])
                  .writeString(null),
              ]),
          ])
          .writeInt32(0)
      )
    const produce = [100, 101].map((id, index) => ({
      id,
      apiKey: 0,
      apiVersion: 8,
      socket: 100 + index,
      correlationId: id,
      nodeId: 1,
      endpoint: 'node-1',
      at: index + 1,
      bytes: request(records, id).toString('base64'),
      ...(index
        ? { responseBytes: response(id).toString('base64'), responseAt: 4 }
        : { lostAt: 2, lossOrder: 2, lossEvent: 0 }),
    }))
    instance.tap.socketEvents.push({ id: 0, socket: 100, at: 2, order: 2, outstanding: [100] })
    for (const [id, frame] of produce.entries())
      instance.producerEvidence.attempts.push({
        id,
        frames: [immutable(frame)],
        nodeId: 1,
        endpoint: 'node-1',
        producerId: '0',
        producerEpoch: 0,
        partitions: [
          {
            topic: instance.topic,
            topicId: instance.topicId,
            partition: 0,
            firstSequence: 0,
            count: 1,
            sends: [0],
          },
        ],
      })
    const extra = JSON.parse(JSON.stringify(fixture.next))
    extra.id = 2
    extra.offset = '1'
    extra.identity = shareRecordIdentity({
      topic: instance.topic,
      partition: 0,
      offset: '1',
      key: message.key,
      value: message.value,
    })
    const range = { firstOffset: '1', lastOffset: '1', deliveryCount: 1 }
    Object.assign(extra.acquisition, {
      id: 2,
      exchange: 2,
      adoptedAt: 32,
      acquiredRecords: [range],
    })
    extra.acquisition.source.id = 2
    extra.acquisition.source.response.responses[0].partitions[0].acquiredRecords = [range]
    Object.assign(extra.acquisition.source.frames[0], { id: 2, at: 30, responseAt: 31 })
    extra.at = 33
    fixture.entries.push(extra)
    fixture.evidence.exchanges.push(extra.acquisition.source)
    fixture.evidence.acquisitions.push(extra.acquisition)
    for (const entry of fixture.entries) {
      const bytes = Buffer.from(records)
      bytes.writeBigInt64BE(BigInt(entry.offset), 0)
      entry.acquisition.source.frames[0].responseBytes = packet(
        new Encoder()
          .writeInt32(7)
          .writeTaggedFields([])
          .writeBuffer(
            fetchWire.buildResponse({
              errorCode: 0,
              responses: [
                {
                  topicId: instance.topicId,
                  partitions: [
                    {
                      partitionIndex: 0,
                      errorCode: 0,
                      errorMessage: null,
                      acknowledgeErrorCode: 0,
                      acknowledgeErrorMessage: null,
                      currentLeader: { leaderId: 0, leaderEpoch: 0 },
                      records: bytes.toString('hex'),
                      acquiredRecords: entry.acquisition.acquiredRecords,
                    },
                  ],
                },
              ],
              nodeEndpoints: [],
            })
          )
      ).toString('base64')
    }
    successfulAcceptFixture(fixture.evidence, fixture.next, 40)
    successfulAcceptFixture(fixture.evidence, extra, 41)
    instance.tap.allFrames = [
      ...produce,
      ...fixture.entries.map((entry) => entry.acquisition.source.frames[0]),
    ]
    return { fixture, instance, extra, produce, request, batch }
  }

  it('rejects a same-stamp new physical offset despite successful settlement and restart proof', async () => {
    const { fixture, instance, extra } = await appendFixture()
    const [join] = instance.diagnoseAppends()
    expect(join.problems).toEqual([])
    expect(join.produceAttempts).toHaveLength(2)
    expect(join.copies.map(({ offset }) => offset).sort()).toEqual(['0', '0', '1'])
    expect(join.receiptIdentities.map(({ offset }) => offset)).toEqual(['0'])
    expect(fixture.evidence.successful(extra)).toBe(true)
    expect(validAcquisition(extra)).toBe(true)
    expect(audit(fixture).problems).toContain(`${extra.identity} is outside the corpus`)
  })

  it.each([
    [
      'producer stamp',
      async ({ produce, request, batch }) => {
        const bytes = await batch({ producerEpoch: 1 })
        for (const frame of produce)
          frame.bytes = request(bytes, frame.correlationId).toString('base64')
      },
      'produce-batch: missing matching original stamp/body',
    ],
    [
      'batch body',
      async ({ produce, request, batch }) => {
        const bytes = await batch({ value: Buffer.from('changed') })
        for (const frame of produce)
          frame.bytes = request(bytes, frame.correlationId).toString('base64')
      },
      'produce-batch: missing matching original stamp/body',
    ],
    [
      'batch header',
      async ({ produce, request, batch }) => {
        const bytes = await batch({ headers: { source: Buffer.from('changed') } })
        for (const frame of produce)
          frame.bytes = request(bytes, frame.correlationId).toString('base64')
      },
      'produce-batch: missing matching original stamp/body',
    ],
    [
      'topic UUID',
      ({ instance }) => {
        instance.sends[0].original = { ...instance.sends[0].original, topicId: 'foreign' }
      },
      'send-identity: topic UUID mismatch',
    ],
    [
      'socket join',
      ({ produce }) => {
        produce[1].socket = 999
      },
      'produce-authority: socket/correlation/endpoint mismatch',
    ],
    [
      'correlation join',
      ({ produce }) => {
        produce[1].correlationId = 999
      },
      'produce-authority: socket/correlation/endpoint mismatch',
    ],
    [
      'Produce loss',
      ({ instance }) => {
        instance.tap.socketEvents = []
      },
      'produce-loss: missing original outstanding/loss event',
    ],
    [
      'public result',
      ({ instance }) => {
        instance.sends[0].results[0].baseOffset = '9'
      },
      'public-receipt: no matching successful original Produce result',
    ],
    [
      'receipt mapping',
      ({ instance }) => {
        instance.sends[0].receiptIdentities[0].offset = '9'
      },
      'receipt-mapping: public result/physical identity mismatch',
    ],
  ])(
    'diagnoses mutated %s through original Produce and receipt joins',
    async (_name, mutate, component) => {
      const context = await appendFixture()
      expect(context.instance.diagnoseAppends()[0].problems).toEqual([])
      await mutate(context)
      expect(context.instance.diagnoseAppends()[0].problems).toContain(component)
      expect(audit(context.fixture).problems).toContain(
        `${context.extra.identity} is outside the corpus`
      )
    }
  )

  it('reconstructs complete chunk membership and rejects deleted or reordered chunks', () => {
    const fields = {
      interruptions: Array.from({ length: 300 }, (_, entry) => ({
        obligations: [{ entry, authority: { socket: entry } }],
      })),
      bytes: Array.from({ length: 12000 }, (_, index) =>
        createHash('sha256').update(String(index)).digest('hex')
      ).join(''),
    }
    const rows = evidenceRows('frozen-bounce', 'stable-object', fields)
    expect(rows[0].chunkCount).toBeGreaterThan(2)
    expect(readEvidenceRows(rows)).toEqual(fields)
    expect(rows.every((row) => Buffer.byteLength(JSON.stringify(row)) < ARCHIVE_ROW_BYTES)).toBe(
      true
    )
    expect(() => readEvidenceRows([rows[0], ...rows.slice(2)])).toThrow('chunks: missing member')
    expect(() => readEvidenceRows([rows[0], rows[2], rows[1], ...rows.slice(3)])).toThrow(
      'chunks: order or identity mismatch'
    )
  })

  it('rejects a fulfilled undefined no-op through the actual watchDown wrapper', async () => {
    const fixture = restartFixture()
    const instance = new ShareBounceCase()
    instance.produceStart = performance.now() - 10
    instance.freeze = () => instance.snapshots.push({ index: 0, ...instance.tap.stamp() })
    instance.lifecycle = fixture.evidence.owner.lifecycle
    const wrapped = instance.watchDown(async () => undefined)
    await wrapped('stop', 'kafka1')
    await wrapped('start', 'kafka1')
    for (const [index, control] of instance.evidence.controls.entries()) {
      fixture.evidence.controls[index].result = control.result
      fixture.evidence.controls[index].semantics = control.semantics
      fixture.evidence.controls[index].exitCode = control.exitCode
    }
    expect(instance.evidence.controls[0].status).toBe('fulfilled')
    expect(instance.evidence.controls[0].result).toBeNull()
    expect(failedComponents(audit(fixture))).toContain(
      'control: missing successful stop/kill of the original serving node'
    )
  })

  /** @param {string} [mode] Replay history. @returns {object} Actual writer with a detached in-memory file boundary. */
  const archiveFixture = (mode = 'reset') => {
    const fixture = restartFixture()
    if (mode === 'no-replay') fixture.entries = [fixture.old]
    if (mode === 'growth') {
      fixture.next.deliveryCount = 2
      fixture.next.acquisition.acquiredRecords[0].deliveryCount = 2
    }
    successfulAcceptFixture(fixture.evidence, mode === 'no-replay' ? fixture.old : fixture.next, 24)
    captureFreezes(fixture)
    fixture.evidence.restorations.push({
      episode: 0,
      result: { service: 'kafka1', kind: 'stop', tDown: 9, tRestored: 18, outageMs: 9 },
      settled: { at: 18.1 },
    })
    const instance = new ShareBounceCase()
    Object.assign(instance, {
      entries: fixture.entries,
      snapshots: fixture.bounces,
      expected: fixture.corpus,
      topic: fixture.old.topic,
      topicId: fixture.old.topicId,
      lifecycle: fixture.evidence.owner.lifecycle,
      replay: audit(fixture),
    })
    instance.evidence = fixture.evidence
    instance.evidence.owner = instance
    instance.tap.socketEvents = []
    instance.tap.allFrames = [
      ...new Map(
        [...fixture.evidence.exchanges, ...fixture.evidence.epochs]
          .flatMap(({ frames }) => frames)
          .map((frame) => [frame.id, frame])
      ).values(),
    ]
    for (const frame of instance.tap.allFrames) frame.bytes = 'original-request'
    const rows = []
    let bytes = 0
    instance.archiveStorage = {
      read: () => rows,
      size: () => bytes,
      write: (row) => {
        rows.push(JSON.parse(JSON.stringify(row)))
        bytes += Buffer.byteLength(JSON.stringify(row)) + 1
      },
      truncate: (size) => {
        while (bytes > size && rows.length)
          bytes -= Buffer.byteLength(JSON.stringify(rows.pop())) + 1
        bytes = size
      },
    }
    return { fixture, instance, rows }
  }

  /** @param {object} instance Archive fixture. @returns {void} Complete post-oracle and cleanup facts. */
  const readyCompletion = (instance) => {
    const snapshot = {
      sockets: { open: 0 },
      activeResources: {},
      listeners: {},
      queueDepth: 0,
      ledgerSize: 0,
    }
    Object.assign(instance, {
      cleanupProof: { completed: true, tainted: false },
      deadline: performance.now() + FINAL_MS,
      terminalVerified: true,
      bounceFacts: [],
      beforeAdmin: snapshot,
      baseline: snapshot,
      after: snapshot,
      probe: { snapshot: async () => snapshot },
      rawIdentities: [...instance.expected],
      expectedHandled: tally(instance.entries.map(({ identity }) => identity)),
      handled: tally(instance.entries.map(({ identity }) => identity)),
      settlement: { successful: [...instance.expected], unknown: [] },
    })
  }

  it('emits completion only with intact closure, cleanup, taint and deadline evidence', async () => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    await instance.complete(NEVER)
    expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
  })

  it('stops completion when the final scope aborts between persisted validation steps', async () => {
    const baseline = archiveFixture()
    readyCompletion(baseline.instance)
    baseline.instance.archive()
    await baseline.instance.complete(NEVER)
    expect(baseline.rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    const controller = new AbortController()
    const read = instance.requireArchiveWork.bind(instance)
    let finished = false
    instance.requireArchiveWork = function* (...args) {
      const work = read(...args)
      try {
        for (;;) {
          const step = work.next()
          if (step.done) {
            finished = true
            return step.value
          }
          setImmediate(() => controller.abort(new Error('archive-control: interrupted validation')))
          yield
        }
      } finally {
        work.return()
      }
    }
    await expect(instance.complete(controller.signal)).rejects.toThrow(
      'archive-control: interrupted validation'
    )
    expect(finished).toBe(false)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it('checks original bytes inside losslessly packed required wire histories', async () => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    for (let id = 1000; id < 1100; id++) {
      const frame = { id, bytes: 'original-request', responseBytes: 'original-response' }
      instance.tap.allFrames.push(frame)
      instance.evidence.brokerStates.push({ frames: [frame], response: {} })
    }
    instance.archive()
    expect(instance.archiveClosure.problems).toEqual([])
    await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
    const manifest = rows.find(({ event }) => event === 'wire-history')
    const members = rows.filter(({ evidenceId }) => evidenceId === manifest.evidenceId)
    const { frames } = readEvidenceRows(members)
    expect(frames).toHaveLength(100)
    const id = frames[0].id
    delete frames[0].bytes
    rows.splice(
      rows.indexOf(manifest),
      members.length,
      ...evidenceRows('wire-history', manifest.evidenceId, { frames })
    )
    await expect(instance.complete(NEVER)).rejects.toThrow(
      `request-bytes: missing required original ${id}`
    )
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it('rejects an original deadline overrun during the completion write and rolls the pass row back', async () => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    const write = instance.archiveStorage.write
    instance.archiveStorage.write = (row) => {
      write(row)
      if (row.event === 'case-complete') instance.deadline = performance.now() - 1
    }
    await expect(instance.complete(NEVER)).rejects.toThrow('deadline: finalization overrun')
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it('validates the intact reference closure through archive and the completion gate', async () => {
    const { instance, rows } = archiveFixture()
    instance.archive()
    expect(instance.archiveClosure.problems).toEqual([])
    await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
    expect(rows.find(({ event }) => event === 'witness-closure-end').complete).toBe(true)
  })

  it.each([
    [
      'required frame',
      ({ instance, fixture }) => {
        instance.tap.allFrames = instance.tap.allFrames.filter(
          ({ id }) => id !== fixture.old.acquisition.source.frames[0].id
        )
      },
      'frame: missing required original 0',
    ],
    [
      'completed response bytes',
      ({ fixture }) => {
        delete fixture.old.acquisition.source.frames[0].responseBytes
      },
      'response-bytes: missing completed original 0',
    ],
    [
      'completed receipt response bytes',
      ({ fixture }) => {
        delete fixture.evidence.exchanges.at(-1).frames[0].responseBytes
      },
      'response-bytes: missing completed original 10000001',
    ],
    [
      'completed Metadata response bytes',
      ({ fixture }) => {
        delete fixture.evidence.epochs[0].frames[0].responseBytes
      },
      'response-bytes: missing completed original 10',
    ],
    [
      'lifecycle result',
      ({ fixture }) => {
        delete fixture.evidence.controls[0].result
      },
      'lifecycle-result: missing successful down 0',
    ],
    [
      'referenced interruption member',
      ({ instance }) => {
        instance.entries = instance.entries.filter(({ id }) => id !== 0)
      },
      'interruption-member: missing original 0',
    ],
    [
      'interruption membership',
      ({ fixture }) => {
        const cut = fixture.bounces[0].interruptions[0]
        fixture.bounces[0].interruptions[0] = { ...cut, obligations: [] }
      },
      'interruption-membership: bounce 0 interruption snapshot disagrees with the journal',
    ],
  ])(
    'blocks completion after deletion of %s from archive input',
    async (_name, mutate, component) => {
      const baseline = archiveFixture()
      baseline.instance.archive()
      expect(baseline.instance.archiveClosure.problems).toEqual([])
      await expect(baseline.instance.requireArchive(NEVER)).resolves.toBeUndefined()
      const context = archiveFixture()
      mutate(context)
      context.instance.archive()
      expect(context.instance.archiveClosure.problems).toContain(component)
      await expect(context.instance.complete(NEVER)).rejects.toThrow(
        'Required witness closure is incomplete'
      )
      expect(context.rows.find(({ event }) => event === 'witness-closure-end').complete).toBe(false)
      expect(context.rows.some(({ event }) => event === 'case-complete')).toBe(false)
    }
  )

  it.each([
    [
      'request bytes',
      'wire-exchange',
      (row) => delete row.bytes,
      'request-bytes: missing required original 0',
    ],
    [
      'response bytes',
      'wire-exchange',
      (row) => delete row.responseBytes,
      'response-bytes: missing completed original 0',
    ],
    [
      'down result',
      'bounce-control',
      (row) => delete row.result,
      'lifecycle-result: missing successful down 0',
    ],
    [
      'interruption membership',
      'frozen-bounce',
      (row) => {
        row.interruptions[0].obligations = []
      },
      'interruption-membership: bounce 0 interruption snapshot disagrees with the journal',
    ],
    [
      'required control reference',
      'bounce-control',
      (row) => {
        row.episode = 99
      },
      'lifecycle-reference: missing original down 0',
    ],
  ])(
    'rejects persisted plain-row deletion of %s through completion',
    async (_name, event, mutate, component) => {
      const { instance, rows } = archiveFixture()
      readyCompletion(instance)
      instance.archive()
      expect(instance.archiveClosure.problems).toEqual([])
      await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
      const row = rows.find((row) => row.event === event)
      expect(row.encoding).toBeUndefined()
      mutate(row)
      await expect(instance.complete(NEVER)).rejects.toThrow(component)
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    }
  )

  it('binds required plain-row contents to the sealed closure manifest', async () => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
    rows.find(({ event }) => event === 'wire-exchange').bytes = 'substituted-request'
    await expect(instance.complete(NEVER)).rejects.toThrow(
      'archive-content: changed required wire-exchange'
    )
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it('rejects a persisted closure manifest that drops a required component', async () => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
    const manifest = rows.find(({ event }) => event === 'witness-closure')
    const closure = readEvidenceRows(
      rows.filter(({ evidenceId }) => evidenceId === manifest.evidenceId)
    )
    const fields = Object.fromEntries(
      ['evidenceSchema', 'identities', 'frames', 'rows', 'omitted'].map((key) => [
        key,
        closure[key],
      ])
    )
    fields.rows = fields.rows.filter(({ event }) => event !== 'bounce-control')
    const first = rows.indexOf(manifest)
    const count = rows.filter(({ evidenceId }) => evidenceId === manifest.evidenceId).length
    rows.splice(first, count, ...evidenceRows('witness-closure', manifest.evidenceId, fields))
    await expect(instance.complete(NEVER)).rejects.toThrow('archive-read: closure manifest changed')
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it.each(['no-replay', 'growth'])(
    'requires every original lifecycle component in %s histories',
    async (mode) => {
      const mutations = [
        [
          'down result',
          ({ fixture }) => delete fixture.evidence.controls[0].result,
          'lifecycle-result: missing successful down 0',
        ],
        [
          'start result',
          ({ fixture }) => delete fixture.evidence.controls[1].result,
          'lifecycle-result: missing successful start 1',
        ],
        [
          'down reference',
          ({ fixture }) => fixture.evidence.controls.shift(),
          'lifecycle-reference: missing original down 0',
        ],
        [
          'start reference',
          ({ fixture }) => fixture.evidence.controls.pop(),
          'lifecycle-reference: missing original start 0',
        ],
        [
          'command reference',
          ({ fixture }) => {
            fixture.evidence.commands = []
          },
          'lifecycle-reference: missing original command 0',
        ],
        [
          'restoration reference',
          ({ fixture }) => {
            fixture.evidence.restorations = []
          },
          'lifecycle-restoration: missing successful original 0',
        ],
        [
          'lifecycle availability',
          ({ instance }) => delete instance.lifecycle,
          'lifecycle-availability: missing original down 0',
        ],
      ]
      for (const [, mutate, component] of mutations) {
        const baseline = archiveFixture(mode)
        readyCompletion(baseline.instance)
        expect(baseline.instance.replay.problems).toEqual([])
        if (mode === 'no-replay') expect(baseline.instance.replay.extras).toHaveLength(0)
        else expect(baseline.instance.replay.extras[0].incarnationWitness).toBeNull()
        baseline.instance.archive()
        expect(baseline.instance.archiveClosure.problems).toEqual([])
        await baseline.instance.complete(NEVER)
        expect(baseline.rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
        const context = archiveFixture(mode)
        readyCompletion(context.instance)
        mutate(context)
        context.instance.archive()
        expect(context.instance.archiveClosure.problems).toContain(component)
        await expect(context.instance.complete(NEVER)).rejects.toThrow(component)
        expect(context.rows.some(({ event }) => event === 'case-complete')).toBe(false)
      }
    }
  )

  it.each(['no-replay', 'growth'])(
    'revalidates persisted unreferenced down/start results and references in %s histories',
    async (mode) => {
      for (const kind of ['stop', 'start']) {
        for (const field of ['result', 'episode']) {
          const { instance, rows } = archiveFixture(mode)
          readyCompletion(instance)
          instance.archive()
          expect(instance.archiveClosure.problems).toEqual([])
          await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
          const control = rows.find((row) => row.event === 'bounce-control' && row.kind === kind)
          expect(control.encoding).toBeUndefined()
          const name = kind === 'stop' ? 'down' : 'start'
          const component =
            field === 'result'
              ? `lifecycle-result: missing successful ${name} ${control.id}`
              : `lifecycle-reference: missing original ${name} 0`
          delete control[field]
          await expect(instance.complete(NEVER)).rejects.toThrow(component)
          expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
        }
      }
    }
  )

  it.each(['delete', 'reorder'])(
    'rejects persisted chunk %s through the completion reader',
    async (mutation) => {
      const { instance, rows } = archiveFixture()
      readyCompletion(instance)
      instance.snapshots[0].retainedDetail = Array.from({ length: 12000 }, (_, index) =>
        createHash('sha256').update(String(index)).digest('hex')
      ).join('')
      instance.archive()
      await expect(instance.requireArchive(NEVER)).resolves.toBeUndefined()
      const manifest = rows.find(({ event }) => event === 'frozen-bounce')
      expect(manifest.chunkCount).toBeGreaterThan(2)
      const first = rows.findIndex(
        ({ event, evidenceId }) => event === 'evidence-chunk' && evidenceId === manifest.evidenceId
      )
      if (mutation === 'delete') rows.splice(first, 1)
      else [rows[first], rows[first + 1]] = [rows[first + 1], rows[first]]
      await expect(instance.complete(NEVER)).rejects.toThrow(
        mutation === 'delete' ? 'chunks: missing member' : 'chunks: order or identity mismatch'
      )
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    }
  )

  it('rejects required closure above 10 MiB through the archive writer', async () => {
    const { instance, rows } = archiveFixture()
    const original = instance.archiveFailureIdentities.bind(instance)
    instance.archiveFailureIdentities = function* (...args) {
      yield* original(...args)
      const emit = args[2]
      // Separate incompressible, bounded rows exercise aggregate accounting, not row truncation.
      for (let index = 0; index < 700; index++)
        emit('handler-entry', {
          id: `budget-${index}`,
          acquisition: 0,
          bytes: Array.from({ length: 400 }, (_, part) =>
            createHash('sha256').update(`${index}:${part}`).digest('hex')
          ).join(''),
        })
    }
    instance.archive()
    expect(instance.archiveClosure.problems).toEqual([])
    expect(instance.captureProblems).toContain(
      'Required evidence does not fit the archive byte bound'
    )
    await expect(instance.complete(NEVER)).rejects.toThrow('Required witness closure is incomplete')
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it.each([
    [
      'cleanup omission',
      (instance) => delete instance.cleanupProof,
      'cleanup: missing completed evidence',
    ],
    [
      'taint omission',
      (instance) => delete instance.cleanupProof.tainted,
      'taint: missing or failed cleanup evidence',
    ],
    [
      'tainted cleanup',
      (instance) => {
        instance.cleanupProof.tainted = true
      },
      'taint: missing or failed cleanup evidence',
    ],
    [
      'original deadline overrun',
      (instance) => {
        instance.deadline = performance.now() - 1
      },
      'deadline: selection deadline passed',
    ],
  ])('blocks completion for %s', async (_name, mutate, component) => {
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    instance.archive()
    mutate(instance)
    await expect(instance.complete(NEVER)).rejects.toThrow(component)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  it('archives actual down/start results without awaiting anything between freeze and down', async () => {
    const instance = new ShareBounceCase()
    instance.produceStart = performance.now() - 10
    const calls = []
    instance.freeze = (kind, service) => {
      calls.push('freeze')
      instance.snapshots.push({ index: 0, kind, service, ...instance.tap.stamp() })
    }
    const result = { stdout: 'actual stdout', stderr: 'actual stderr' }
    const wrapped = instance.watchDown((kind) => {
      calls.push(kind)
      return Promise.resolve(result)
    })
    const pending = wrapped('stop', 'kafka1')
    expect(calls).toEqual(['freeze', 'stop'])
    expect(instance.evidence.controls[0].status).toBe('pending')
    expect(await pending).toBe(result)
    expect(await wrapped('start', 'kafka1')).toBe(result)
    expect(
      instance.evidence.controls.map(({ kind, status, result: observed }) => ({
        kind,
        status,
        observed,
      }))
    ).toEqual([
      { kind: 'stop', status: 'fulfilled', observed: result },
      { kind: 'start', status: 'fulfilled', observed: result },
    ])
    expect(instance.evidence.controls[1].replaces).toBe(0)
  })

  it('runs the same read-only audit while allowing live heartbeat turns', async () => {
    const fixture = restartFixture()
    let turns = 0
    const heartbeat = new Promise((resolve) =>
      setImmediate(() => {
        turns++
        resolve()
      })
    )
    const result = await cooperativeAudit(
      auditReplayWork(fixture.entries, fixture.bounces, new Set(fixture.corpus), fixture.evidence),
      NEVER
    )
    expect(turns).toBeGreaterThan(0)
    expect(result).toEqual(audit(fixture))
    await heartbeat
  })

  it('audits 30000 records with Metadata and count resets through drain, terminal, archive and completion work', async () => {
    const proof = terminalBudgetFixture()
    const replay = replayFixture({ sameLeader: true })
    replay.next.acquisition.acquiredRecords[0].lastOffset = '99'
    for (let offset = 0; offset < 100; offset++) {
      const original = proof.entries[offset]
      proof.entries.push({
        ...replay.next,
        id: 2 * offset + 1,
        offset: original.offset,
        identity: original.identity,
      })
    }
    proof.evidence.acquisitions.push(replay.next.acquisition)
    proof.evidence.exchanges.push(replay.next.acquisition.source)
    successfulAcceptFixture(proof.evidence, replay.next, 24)
    const receipt = proof.evidence.exchanges.at(-1)
    receipt.tickets[0].batches[0].lastOffset = '99'
    proof.evidence.outcomes.at(-1).lastOffset = '99'
    for (let index = 0; index < 2000; index++) {
      const sample = JSON.parse(JSON.stringify(replay.evidence.epochs[index % 2]))
      sample.frames[0].id = 200000000 + index
      proof.evidence.epochs.push(sample)
    }
    const mapping = restartFixture().evidence.owner.lifecycle.kafka1
    proof.evidence.owner.lifecycle = { kafka1: mapping }
    for (const bounce of proof.bounces) {
      Object.assign(bounce, { kind: 'stop', service: 'kafka1' })
      const shift = bounce.index * 100
      const down = 2 * bounce.index
      proof.evidence.controls.push(
        {
          id: down,
          episode: bounce.index,
          kind: 'stop',
          service: 'kafka1',
          nodeId: 1,
          mapping,
          semantics: 'awaited-compose-service-process',
          status: 'fulfilled',
          exitCode: 0,
          result: { stdout: '', stderr: 'Stopped' },
          invoked: { at: bounce.at },
          settled: { at: 9 + shift },
        },
        {
          id: down + 1,
          episode: bounce.index,
          kind: 'start',
          service: 'kafka1',
          nodeId: 1,
          mapping,
          semantics: 'awaited-compose-service-process',
          status: 'fulfilled',
          exitCode: 0,
          result: { stdout: '', stderr: 'Started' },
          replaces: down,
          invoked: { at: 10 + shift },
          settled: { at: 11 + shift },
        }
      )
      proof.evidence.restorations.push({
        episode: bounce.index,
        result: {
          service: 'kafka1',
          kind: 'stop',
          tDown: bounce.downAt,
          tRestored: bounce.restoredAt,
          outageMs: bounce.restoredAt - bounce.downAt,
        },
        settled: { at: bounce.restoredAt + 0.1 },
      })
    }
    captureFreezes(proof)
    let turns = 0
    let maxStepMs = 0
    /** @param {Generator} work Actual audit. @returns {Generator} Identical steps with runtime observations. */
    const measured = function* (work) {
      for (;;) {
        const started = performance.now()
        const step = work.next()
        maxStepMs = Math.max(maxStepMs, performance.now() - started)
        if (step.done) return step.value
        turns++
        yield
      }
    }
    const started = performance.now()
    const candidates = await cooperativeAudit(
      measured(
        settlementCandidatesWork(
          proof.entries,
          new Set(proof.corpus),
          proof.bounces,
          proof.evidence
        )
      ),
      NEVER
    )
    const terminal = await cooperativeAudit(measured(auditTerminalWork(proof)), NEVER)
    expect(proof.corpus).toHaveLength(RECORDS)
    expect(proof.evidence.epochs).toHaveLength(2000)
    expect(candidates.problems).toEqual([])
    expect(terminal.problems).toEqual([])
    expect(terminal.replay.extras).toHaveLength(100)
    expect(
      terminal.replay.extras.every(
        ({ incarnationWitness }) => incarnationWitness.kind === 'partition-transition'
      )
    ).toBe(true)
    expect(terminal.settlement.successful).toEqual(candidates.successful)
    const { instance, rows } = archiveFixture()
    readyCompletion(instance)
    const socketEvents = proof.evidence.owner.tap.socketEvents
    Object.assign(instance, {
      entries: proof.entries,
      snapshots: proof.bounces,
      expected: proof.corpus,
      evidence: proof.evidence,
      replay: terminal.replay,
      settlement: terminal.settlement,
      lifecycle: proof.evidence.owner.lifecycle,
      deadline: started + FINAL_MS,
      rawIdentities: proof.raw,
      handled: tally(proof.entries.map(({ identity }) => identity)),
      expectedHandled: tally([
        ...proof.corpus,
        ...terminal.replay.extras.map(({ identity }) => identity),
      ]),
      beforeTerminal: proof.before,
      afterTerminal: proof.after,
      quiet: proof.quiet,
    })
    instance.evidence.owner = instance
    instance.tap.socketEvents = socketEvents
    instance.tap.allFrames = [
      ...new Map(
        [...proof.evidence.exchanges, ...proof.evidence.epochs]
          .flatMap(({ frames }) => frames)
          .map((frame) => [frame.id, frame])
      ).values(),
    ]
    for (const frame of instance.tap.allFrames) frame.bytes = 'original-request'
    let validationRuns = 0
    let validationTurns = 0
    const read = instance.requireArchiveWork.bind(instance)
    instance.requireArchiveWork = function* (...args) {
      validationRuns++
      const before = turns
      const result = yield* measured(read(...args))
      validationTurns += turns - before
      return result
    }
    await cooperativeAudit(measured(instance.archiveWork()), NEVER)
    expect(instance.archiveClosure.problems).toEqual([])
    expect(instance.captureProblems).toEqual([])
    expect(instance.archiveIncomplete).not.toBe(true)
    await instance.complete(NEVER)
    expect(validationRuns).toBe(1)
    expect(validationTurns).toBeGreaterThan(RECORDS / 100)
    expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    expect(rows.some(({ event }) => event === 'witness-closure-end')).toBe(true)
    expect(turns).toBeGreaterThan(RECORDS / 100)
    expect(maxStepMs).toBeLessThan(1000)
    expect(performance.now() - started).toBeLessThan(FINAL_MS)
  })

  it.each([
    ['receipt before cut', 9, 10, false],
    ['receipt after cut', 11, 10, true],
    ['same sequence', 10, 10, true],
    ['missing receipt sequence', undefined, 10, true],
    ['missing cut sequence', 9, undefined, true],
    ['both sequences missing', undefined, undefined, true],
  ])(
    'preserves strict equal-clock membership for %s',
    (_name, receiptOrder, cutOrder, unresolved) => {
      const fixture = restartFixture()
      successfulAcceptFixture(fixture.evidence, fixture.old, 7)
      const receipt = fixture.evidence.results(fixture.old)[0]
      receipt.order = receiptOrder
      const cut = fixture.evidence.retirements[0]
      cut.order = cutOrder
      expect(receipt.at).toBe(cut.at)
      expect(precedes(receipt, cut)).toBe(!unresolved)
      captureFreezes(fixture)
      const expected = unresolved ? [fixture.old.id] : []
      expect(unresolvedAt(fixture.entries, fixture.evidence, cut).map(({ id }) => id)).toEqual(
        expected
      )
      const indexed = finishAudit(
        indexedEvidenceWork(fixture.entries, fixture.evidence, fixture.bounces)
      )
      expect(unresolvedAt(indexed.auditEntries, indexed, cut).map(({ id }) => id)).toEqual(expected)
      const original = finishAudit(
        reconstructFreezes(fixture.entries, fixture.bounces, fixture.evidence)
      )
      const reconstructed = finishAudit(
        reconstructFreezes(indexed.auditEntries, fixture.bounces, indexed)
      )
      expect(reconstructed).toEqual(original)
      expect(fixture.bounces[0].interruptions[0].history).toEqual(expected)
      expect(freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence).problems).toEqual(
        []
      )
      if (unresolved) {
        const cut = fixture.bounces[0].interruptions[0]
        fixture.bounces[0].interruptions[0] = { ...cut, obligations: [] }
        expect(
          freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence).problems
        ).toContain('bounce 0 interruption snapshot disagrees with the journal')
      }
    }
  )

  it('seals audit journal membership before yielding to concurrent live additions', async () => {
    const fixture = restartFixture()
    const expected = audit(fixture)
    const work = auditReplayWork(
      fixture.entries,
      fixture.bounces,
      new Set(fixture.corpus),
      fixture.evidence
    )
    expect(work.next().done).toBe(false)
    const lengths = {
      entries: fixture.entries.length,
      exchanges: fixture.evidence.exchanges.length,
    }
    fixture.entries.push({ ...fixture.next, id: 99 })
    fixture.evidence.exchanges.push({ ...fixture.next.acquisition.source, id: 99 })
    expect(await cooperativeAudit(work, NEVER)).toEqual(expected)
    expect(fixture.entries).toHaveLength(lengths.entries + 1)
    expect(fixture.evidence.exchanges).toHaveLength(lengths.exchanges + 1)
  })

  it('permits 1800 terminal unknowns only beside 28200 real Accepts and complete terminal proof', () => {
    const proof = terminalBudgetFixture()
    const result = auditTerminal(proof)
    expect(result.problems).toEqual([])
    expect(result.settlement.successful).toHaveLength(28200)
    expect(result.settlement.unknown).toHaveLength(1800)
    expect(result.settlement.perBounce).toEqual(Array(6).fill(300))
    expect(
      proof.evidence.outcomes.filter(({ error }) => error?.reason === 'session-lost')
    ).toHaveLength(1800)
    proof.after = null
    expect(auditTerminal(proof).problems).toContain('Incomplete terminal proof')
  })

  it('samples sibling brokers independently and follows an overlapping sample with a fresh one', async () => {
    let complete
    const first = {
      connectionPool: { isConnected: () => true },
      metadata: jest
        .fn()
        .mockImplementationOnce(() => new Promise((resolve) => (complete = resolve)))
        .mockResolvedValue({}),
    }
    const next = {
      connectionPool: { isConnected: () => true },
      metadata: jest.fn().mockResolvedValue({}),
    }
    const evidence = new BounceEvidence({ topic: 'sampling-control' })
    evidence.epochSignal = NEVER
    try {
      evidence.sampleEpoch(first)
      evidence.sampleEpoch(next)
      expect(first.metadata).toHaveBeenCalledTimes(1)
      expect(next.metadata).toHaveBeenCalledTimes(1)
      evidence.sampleEpoch(first)
      expect(first.metadata).toHaveBeenCalledTimes(1)
      const pending = evidence.epochSamples.get(first).task
      complete({})
      await pending
      expect(first.metadata).toHaveBeenCalledTimes(2)
    } finally {
      evidence.owner.driveFinished = true
      complete?.({})
      await Promise.all([...evidence.epochSamples.values()].map(({ task }) => task))
    }
  })

  it('starts an acquisition epoch sample without waiting for an overlapping periodic sample', async () => {
    let complete
    const broker = {
      connectionPool: { isConnected: () => true },
      metadata: jest
        .fn()
        .mockImplementationOnce(() => new Promise((resolve) => (complete = resolve)))
        .mockResolvedValue({}),
    }
    const evidence = new BounceEvidence({ topic: 'sampling-control' })
    evidence.epochSignal = NEVER
    try {
      evidence.sampleEpoch(broker)
      evidence.sampleEpoch(broker, { fresh: true })
      expect(broker.metadata).toHaveBeenCalledTimes(2)
    } finally {
      evidence.owner.driveFinished = true
      complete?.({})
      await Promise.all([...evidence.epochProbes])
    }
  })

  it('retains original metadata epochs when the routing cache later mutates its response', async () => {
    const fixture = replayFixture()
    const response = fixture.evidence.epochs[0].response
    const original = Broker.prototype.metadata
    Broker.prototype.metadata = jest.fn().mockResolvedValue(response)
    const evidence = new BounceEvidence({ topic: fixture.old.topic, tap: new ShareWireTap() })
    try {
      evidence.install()
      const broker = { nodeId: 1, connectionPool: {}, brokerAddress: 'node-1' }
      const result = await Broker.prototype.metadata.call(broker)
      expect(result).toBe(response)
      result.topicMetadata[0].partitionMetadata[0].leaderEpoch = 99
      expect(evidence.epochs[0].response.topicMetadata[0].partitionMetadata[0].leaderEpoch).toBe(1)
    } finally {
      evidence.restore()
      Broker.prototype.metadata = original
    }
  })

  it('starts sampling a connected member broker before its first share exchange', async () => {
    const original = Broker.prototype.connect
    Broker.prototype.connect = jest.fn().mockResolvedValue('connected')
    const evidence = new BounceEvidence({
      nonce: 'sampling-control',
      topic: 'sampling-control',
      tap: new ShareWireTap(),
    })
    const broker = {
      connectionPool: { clientId: 'member-sampling-control', isConnected: () => true },
      metadata: jest.fn().mockResolvedValue({}),
    }
    evidence.epochSignal = NEVER
    try {
      evidence.install()
      await expect(Broker.prototype.connect.call(broker)).resolves.toBe('connected')
      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(evidence.brokers.has(broker)).toBe(true)
    } finally {
      evidence.owner.driveFinished = true
      await Promise.all([...evidence.epochProbes])
      evidence.restore()
      Broker.prototype.connect = original
    }
  })

  it('retains both delivery intervals and overlapping epoch facts in a count failure', () => {
    const fixture = replayFixture()
    fixture.evidence.epochs[1].frames[0].responseAt = 21
    const problem = audit(fixture).problems.find((value) =>
      value.startsWith(`${fixture.old.identity} has no proved delivery-count progression: `)
    )
    const facts = JSON.parse(problem.slice(problem.indexOf(': ') + 2))
    expect(facts.deliveries.map(({ entry, deliveryCount }) => [entry, deliveryCount])).toEqual([
      [0, 1],
      [1, 1],
    ])
    expect(facts.deliveries[1]).toMatchObject({
      nodeId: 2,
      sessionEpoch: 1,
      requestAt: 20,
      responseAt: 21,
      epochs: { overlapping: [{ frame: 11, leaderEpoch: 2 }] },
    })
    expect(facts.bounce).toMatchObject({ index: 0, at: 7, downAt: 9 })
  })

  /** @returns {object} Late original acquisition and a source-local away-and-back transition. */
  const metadataTransitionFixture = () => {
    const fixture = replayFixture({ sameLeader: true })
    fixture.bounces[0].at = 3
    fixture.old.acquisition.source.frames[0].at = 3.1
    fixture.old.acquisition.source.frames[0].responseAt = 3.2
    fixture.old.acquisition.adoptedAt = 3.3
    fixture.evidence.epochs[1].response.topicMetadata[0].partitionMetadata[0].leaderEpoch = 3
    const transition = JSON.parse(JSON.stringify(fixture.evidence.epochs[0]))
    Object.assign(transition.frames[0], {
      id: 50,
      endpoint: fixture.old.acquisition.source.endpoint,
      at: 6.2,
      responseAt: 6.4,
    })
    Object.assign(transition.response.topicMetadata[0].partitionMetadata[0], {
      leader: 2,
      leaderEpoch: 2,
    })
    fixture.evidence.epochs.push(transition)
    captureFreezes(fixture)
    return { ...fixture, transition }
  }

  it('corroborates a late old epoch with its source node metadata transition before down', () => {
    const fixture = metadataTransitionFixture()
    expect(audit(fixture).problems).toEqual([])
    expect(audit(fixture).extras[0].epochWitness).toMatchObject({
      upper: 1,
      lower: 3,
      metadataTransition: 50,
    })
  })

  it.each([
    ['no old upper bound', (f) => f.evidence.epochs.shift()],
    ['overlapping new lower bound', (f) => (f.evidence.epochs[1].frames[0].responseAt = 21)],
    ['another source node', (f) => (f.transition.nodeId = 3)],
    ['another pool', (f) => (f.transition.pool = 99)],
    ['another endpoint', (f) => (f.transition.frames[0].endpoint = 'node-3')],
    [
      'another partition',
      (f) => (f.transition.response.topicMetadata[0].partitionMetadata[0].partitionId = 1),
    ],
    ['unanswered sample', (f) => delete f.transition.frames[0].responseBytes],
    ['after down', (f) => (f.transition.frames[0].responseAt = 10)],
    ['before the upper bound', (f) => (f.transition.frames[0].at = 5.5)],
    [
      'unchanged leader',
      (f) => (f.transition.response.topicMetadata[0].partitionMetadata[0].leader = 1),
    ],
    [
      'unchanged epoch',
      (f) => (f.transition.response.topicMetadata[0].partitionMetadata[0].leaderEpoch = 1),
    ],
    [
      'different leaders at one epoch',
      (f) => (f.evidence.epochs[1].response.topicMetadata[0].partitionMetadata[0].leaderEpoch = 2),
    ],
  ])('rejects a metadata transition with %s', (_name, mutate) => {
    const fixture = metadataTransitionFixture()
    mutate(fixture)
    expect(audit(fixture).problems).toContainEqual(
      expect.stringContaining(`${fixture.old.identity} has no proved delivery-count progression`)
    )
  })

  /** @returns {object} Distinct acquisitions bracketed by source epochs and a completed kill. */
  const downBarrierFixture = () => {
    const fixture = replayFixture()
    Object.assign(fixture.bounces[0], { at: 0.5, kind: 'kill', service: 'kafka1' })
    captureFreezes(fixture)
    return fixture
  }

  it('proves late equal-count replay across a completed down action using disjoint epoch bounds', () => {
    const fixture = downBarrierFixture()
    expect(fixture.bounces[0].held.size).toBe(0)
    expect(audit(fixture).problems).toEqual([])
    expect(audit(fixture).extras[0].epochWitness).toMatchObject({
      upper: 1,
      lower: 2,
      before: 10,
      after: 11,
      downBarrier: 9,
    })
  })

  it.each([
    ['missing upper bound', (f) => f.evidence.epochs.shift()],
    ['missing lower bound', (f) => f.evidence.epochs.pop()],
    ['unobserved down action', (f) => delete f.bounces[0].downAt],
    ['unobserved command kind', (f) => delete f.bounces[0].kind],
    ['request before command start', (f) => (f.bounces[0].at = 1.5)],
    ['upper bound overlaps old response', (f) => (f.evidence.epochs[0].frames[0].at = 1.5)],
    ['lower bound overlaps new request', (f) => (f.evidence.epochs[1].frames[0].responseAt = 21)],
  ])('cannot use a down barrier with %s', (_name, mutate) => {
    const fixture = downBarrierFixture()
    mutate(fixture)
    expect(
      epochAdvance(
        fixture.old,
        fixture.next,
        fixture.bounces[0],
        fixture.evidence.epochs,
        fixture.evidence.exchanges
      )
    ).toBeNull()
  })

  it('discards old-open and duplicate completions before creating another cohort', () => {
    const { ConsumerOperation } = require('../../groupProtocol/interface')
    const { ShareSession } = require('../shareSession')
    const operation = new ConsumerOperation()
    const session = new ShareSession({
      operation,
      onReset: (...args) => loop.onSessionReset(...args),
    })
    const loop = new ShareFetchLoop({
      session,
      groupId: 'g',
      memberId: 'm',
      broker: { nodeId: 1 },
      now: () => 1000,
      cluster: { topicName: () => 'topic' },
    })
    const request = () => {
      const exchange = loop.newExchange({ apiKey: 78, kind: 'fetch' }, session.token)
      exchange.sentAt = 1000
      exchange.descriptor = { token: exchange.open, apiKey: 78, epoch: session.epoch, topics: [] }
      loop.exchanges.add(exchange)
      return exchange
    }
    const response = {
      errorCode: 0,
      acquisitionLockTimeoutMs: 30000,
      responses: [
        {
          topicId: '11111111-2222-3333-4444-555555555555',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              acknowledgeErrorCode: 0,
              acquiredRecords: [{ firstOffset: '0', lastOffset: '0', deliveryCount: 1 }],
              messages: [{ offset: '0' }],
            },
          ],
        },
      ],
    }
    try {
      const first = request()
      loop.complete(first, response)
      expect([...loop.cohorts.values()].flat()).toHaveLength(1)
      expect(() => loop.complete(first, response)).toThrow()
      expect([...loop.cohorts.values()].flat()).toHaveLength(1)
      const stale = request()
      session.reset('connection', new Error('scripted down action'))
      expect(() => loop.complete(stale, response)).toThrow()
      expect(loop.cohorts.size).toBe(0)
    } finally {
      operation.retire(new Error('control finished'))
    }
  })

  it.each([false, true])(
    'allows distinct equal-count acquisitions with disjoint serving epochs (same leader %s)',
    (sameLeader) => {
      expect(audit(replayFixture({ sameLeader })).problems).toEqual([])
    }
  )

  it.each([1, 2])(
    'rejects a second dispatch of one wire acquisition even with count %s or another cohort',
    (count) => {
      const fixture = replayFixture()
      fixture.next.acquisition = {
        ...fixture.old.acquisition,
        id: 99,
        token: 99,
        responseIdentity: 99,
      }
      fixture.next.deliveryCount = count
      expect(audit(fixture).problems).toContain(
        `${fixture.old.identity} dispatched one acquisition twice`
      )
    }
  )

  it.each([
    [
      'unchanged epoch',
      (f) => {
        f.evidence.epochs[1].response.topicMetadata[0].partitionMetadata[0].leaderEpoch = 1
      },
    ],
    [
      'another node bounced',
      (f) => {
        f.bounces[0].nodeId = 3
      },
    ],
    [
      'only generation changed',
      (f) => {
        f.evidence.epochs = []
      },
    ],
    [
      'coordinator change',
      (f) => {
        f.evidence.retirements[0].reason = 'membership'
      },
    ],
    [
      'missing epoch',
      (f) => {
        delete f.evidence.epochs[1].response.topicMetadata[0].partitionMetadata[0].leaderEpoch
      },
    ],
    [
      'remote metadata cache',
      (f) => {
        f.evidence.epochs[1].nodeId = 3
      },
    ],
    [
      'default CurrentLeader',
      (f) => {
        f.evidence.epochs = []
        f.next.acquisition.currentLeader = { leaderId: -1, leaderEpoch: -1 }
      },
    ],
    [
      'record batch epoch',
      (f) => {
        f.evidence.epochs = []
        f.next.acquisition.batchEpoch = 2
      },
    ],
    [
      'delayed old response',
      (f) => {
        f.old.acquisition.source.frames[0].responseAt = 15
      },
    ],
    [
      'overlapping new request',
      (f) => {
        f.next.acquisition.source.frames[0].at = 11
      },
    ],
    [
      'no frozen handler',
      (f) => {
        f.bounces[0].held.clear()
      },
    ],
    [
      'no original obligation',
      (f) => {
        f.bounces[0].obligations = []
      },
    ],
    [
      'no corpus identity',
      (f) => {
        f.corpus = []
      },
    ],
  ])('rejects equal-count replay with %s', (name, mutate) => {
    const fixture = replayFixture({ sameLeader: true })
    expect(audit(fixture).problems).toEqual([])
    mutate(fixture)
    const result = audit(fixture)
    if (name === 'another node bounced' || name === 'coordinator change')
      expect(result.problems).toContain(`${fixture.old.identity} has no original fault obligation`)
    else if (name === 'delayed old response')
      expect(result.problems).toContain(`${fixture.old.identity} lacks an adopted wire acquisition`)
    else if (name === 'no frozen handler' || name === 'no original obligation')
      expect(result.problems).toContain('bounce 0 command snapshot disagrees with the journal')
    else if (name === 'no corpus identity')
      expect(result.problems).toContain(`${fixture.old.identity} is outside the corpus`)
    else
      expect(failedComponents(result)).toContain(
        'partition-transition: missing disjoint serving-epoch bounds or causal transition'
      )
  })

  it.each([0, -1, 1.5, NaN, 32768])('rejects invalid count %s', (count) => {
    expect(audit(replayFixture({ count })).problems).toContain(
      `${replayFixture().old.identity} has an invalid delivery count`
    )
  })

  it('permits a valid reset floor across a complete partition transition', () => {
    const fixture = replayFixture()
    fixture.old.deliveryCount = 2
    fixture.old.acquisition.acquiredRecords[0].deliveryCount = 2
    expect(audit(fixture).problems).toEqual([])
  })

  it('retains the original episode across later held sets and rejects a duplicate charge', () => {
    const fixture = replayFixture({ count: 2 })
    fixture.bounces.push({ ...fixture.bounces[0], index: 1, at: 19, downAt: 20, restoredAt: 30 })
    captureFreezes(fixture)
    expect(audit(fixture).perBounce).toEqual([1, 0])
    fixture.entries.push(fixture.next)
    expect(audit(fixture).problems).toContain(
      `${fixture.next.identity} dispatched one acquisition twice`
    )
  })

  it.each([false, true])(
    'enforces replay and unknown ceilings together with unresolved carry (%s)',
    (unknown) => {
      const boundary = budgetFixture(1800, 300, unknown)
      const result = unknown
        ? settlementCandidates(
            boundary.entries,
            new Set(boundary.corpus),
            boundary.bounces,
            boundary.evidence
          )
        : audit(boundary)
      expect(result.problems).toEqual([])
      expect(result.perBounce).toEqual([300, 300, 300, 300, 300, 300])
      const overEpisode = budgetFixture(301, 301, unknown)
      const episodeResult = unknown
        ? settlementCandidates(
            overEpisode.entries,
            new Set(overEpisode.corpus),
            overEpisode.bounces,
            overEpisode.evidence
          )
        : audit(overEpisode)
      expect(episodeResult.perBounce).toEqual([301])
      expect(episodeResult.problems).toContain(
        unknown
          ? 'Unknown settlement exceeds the frozen fault allowance'
          : 'bounce 0 took 301 extra entries'
      )
      const overTotal = budgetFixture(1801, 300, unknown)
      const totalResult = unknown
        ? settlementCandidates(
            overTotal.entries,
            new Set(overTotal.corpus),
            overTotal.bounces,
            overTotal.evidence
          )
        : audit(overTotal)
      expect(unknown ? totalResult.unknown.length : totalResult.extras.length).toBe(1801)
      expect(totalResult.problems).toContain(
        unknown
          ? 'Unknown settlement exceeds the frozen fault allowance'
          : '1801 extra entries in total'
      )
    }
  )

  it('admits an applied Accept with a lost answer only after independent terminal proof', () => {
    const proof = lostAcceptFixture(replayFixture())
    const candidate = settlementCandidates(
      proof.entries,
      new Set(proof.corpus),
      proof.bounces,
      proof.evidence
    )
    expect(candidate.successful).toEqual([])
    expect(candidate.unknown.map(({ identity }) => identity)).toEqual(proof.corpus)
    expect(candidate.successful.length === proof.corpus.length).toBe(false)
    expect(auditTerminal(proof).problems).toEqual([])
    expect(proof.evidence.outcomes[0].error.reason).toBe('session-lost')
    expect(proof.evidence.outcomes.filter(({ error }) => error === null)).toEqual([])
  })

  it('tracks a frozen verifier without replacing any public consumer method', async () => {
    const instance = new ShareBounceCase()
    const events = new EventEmitter()
    const assignment = [{ topic: 'control', partitions: [0] }]
    let disconnects = 0
    const consumer = Object.freeze({
      events: { GROUP_JOIN: 'join', ASSIGNMENT_CHANGED: 'assignment', REQUEST: 'request' },
      on: (name, listener) => {
        events.on(name, listener)
        return () => events.off(name, listener)
      },
      assignment: () => assignment,
      disconnect: async () => {
        disconnects++
      },
    })
    instance.quiet = { events: [] }
    const wrapper = instance.trackVerifier(consumer)
    events.emit('request', { payload: { apiName: 'ShareFetch' } })
    expect(instance.quiet.activeAt).toBeGreaterThan(0)
    await wrapper.disconnect({ timeout: 2000 })
    expect(disconnects).toBe(1)
    expect(instance.quiet.assignment).toEqual(assignment)
    expect(instance.quiet.endedAt).toBeGreaterThanOrEqual(instance.quiet.activeAt)
    expect(events.eventNames()).toEqual([])
    expect(Object.isFrozen(consumer)).toBe(true)
    expect(wrapper).not.toBe(consumer)
  })

  it('admits a late written Accept only through its original interruption snapshot', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    const bounce = fixture.bounces[0]
    bounce.at = 3
    const acquisition = fixture.old.acquisition
    acquisition.source.frames[0].at = 3.1
    acquisition.source.frames[0].responseAt = 3.2
    acquisition.adoptedAt = 3.3
    captureFreezes(fixture)
    expect(bounce.held.size).toBe(0)
    expect(bounce.obligations).toEqual([])
    expect(bounce.interruptions).toHaveLength(1)
    expect(bounce.interruptions[0]).toMatchObject({ at: 8, kind: 'socket', history: [0] })
    expect([...effectiveObligations(bounce).keys()]).toEqual(proof.corpus)
    const result = auditTerminal(proof)
    expect(result.problems).toEqual([])
    expect(result.settlement.successful).toEqual([])
    expect(result.settlement.unknown.map(({ identity }) => identity)).toEqual(proof.corpus)
    expect(proof.evidence.outcomes.every(({ error }) => error !== null)).toBe(true)
    bounce.interruptions = []
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })

  it.each([false, true])(
    'retains late held work for replay without admitting an unwritten Accept (%s)',
    (written) => {
      const fixture = replayFixture({ count: 2 })
      const proof = lostAcceptFixture(fixture, { written })
      fixture.bounces[0].at = 3
      if (written) fixture.evidence.exchanges.at(-1).frames[0].at = 8.05
      captureFreezes(fixture)
      expect([...effectiveObligations(fixture.bounces[0]).keys()]).toEqual(proof.corpus)
      expect(audit(fixture).problems).toEqual([])
      expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
    }
  )

  it('does not move the first socket cut to the failure, reset or command return', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    fixture.bounces[0].downAt = 18
    fixture.evidence.exchanges.at(-1).failedAt = 10
    fixture.evidence.retirements[0].at = 11
    fixture.evidence.outcomes[0].at = 11.1
    captureFreezes(fixture)
    expect(fixture.bounces[0].interruptions[0].at).toBe(8)
    expect(auditTerminal(proof).problems).toEqual([])
    fixture.evidence.exchanges.at(-1).frames[0].at = 9
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
    fixture.old.at = 8.5
    expect(audit(fixture).problems.length).toBeGreaterThan(0)
  })

  it('rejects a late extension when primitive event ordering is ambiguous', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    expect(auditTerminal(proof).problems).toEqual([])
    const frame = fixture.evidence.exchanges.at(-1).frames[0]
    frame.at = fixture.bounces[0].interruptions[0].at
    expect(precedes(frame, fixture.bounces[0].interruptions[0])).toBe(false)
    expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toEqual([])
    expect(auditTerminal(proof).problems).toEqual([`Unsettled ${proof.corpus[0]}`])
  })

  it.each([8.4, 9, 17])('keeps late eligibility independent of command return at %s', (downAt) => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    fixture.bounces[0].downAt = downAt
    expect(auditTerminal(proof).problems).toEqual([])
    fixture.evidence.owner.tap.socketEvents = []
    fixture.evidence.exchanges.at(-1).failedAt = undefined
    fixture.evidence.retirements = []
    expect(auditTerminal(proof).problems).toContain(`Unsettled ${proof.corpus[0]}`)
  })

  it.each([
    'socket',
    'nodeId',
    'groupId',
    'memberId',
    'topicId',
    'partition',
    'pool',
    'generation',
    'open',
  ])('rejects a late interruption with a changed %s authority', (field) => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    const cut = fixture.bounces[0].interruptions[0]
    const changed = typeof cut.scope[field] === 'number' ? cut.scope[field] + 1 : 'foreign'
    fixture.bounces[0].interruptions[0] = { ...cut, scope: { ...cut.scope, [field]: changed } }
    expect(auditTerminal(proof).problems).toContain(
      'bounce 0 interruption snapshot disagrees with the journal'
    )
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })

  it.each(['cohort', 'ticket', 'cause', 'correlation'])(
    'rejects late Accept evidence with a changed %s link',
    (field) => {
      const fixture = replayFixture({ count: 2 })
      const proof = lostAcceptFixture(fixture)
      fixture.bounces[0].at = 3
      captureFreezes(fixture)
      expect(auditTerminal(proof).problems).toEqual([])
      const exchange = fixture.evidence.exchanges.at(-1)
      if (field === 'cause') exchange.cause++
      else if (field === 'correlation') exchange.frames[0].socket++
      else exchange.tickets[0][field]++
      if (field === 'ticket') {
        const problem = 'bounce 0 interruption snapshot disagrees with the journal'
        expect(exchange.tickets[0].ticket).toBeDefined()
        expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toEqual([
          problem,
        ])
        expect(auditTerminal(proof).problems).toEqual([problem, problem])
      } else {
        captureFreezes(fixture)
        expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toEqual([])
        expect(auditTerminal(proof).problems).toEqual([`Unsettled ${proof.corpus[0]}`])
      }
    }
  )

  it('rejects late Accept evidence with a missing ticket link', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    expect(auditTerminal(proof).problems).toEqual([])
    delete fixture.evidence.exchanges.at(-1).tickets[0].ticket
    captureFreezes(fixture)
    expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toEqual([])
    expect(auditTerminal(proof).problems).toEqual([`Unsettled ${proof.corpus[0]}`])
  })

  it('uses an original exchange failure when no earlier relevant socket loss was observed', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    fixture.evidence.owner.tap.socketEvents = []
    delete fixture.evidence.exchanges.at(-1).frames[0].lostAt
    captureFreezes(fixture)
    expect(fixture.bounces[0].interruptions[0]).toMatchObject({
      kind: 'failure',
      event: 100,
      at: 8.1,
    })
    expect(auditTerminal(proof).problems).toEqual([])
    fixture.evidence.retirements[0].cause++
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })

  it('does not reconstruct a missing pending attempt from final classifications', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    const cut = fixture.bounces[0].interruptions[0]
    fixture.bounces[0].interruptions[0] = { ...cut, obligations: [], history: [] }
    proof.settled = true
    proof.unknown = [...proof.corpus]
    expect(auditTerminal(proof).problems).toContain(
      'bounce 0 interruption snapshot disagrees with the journal'
    )
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })

  it('preserves 201 early identities while freezing only the 167 still unresolved at interruption', () => {
    const fixture = budgetFixture(301, 301, false)
    fixture.entries = fixture.entries.filter(({ id }) => id % 2 === 0)
    fixture.evidence.acquisitions = fixture.entries.map(({ acquisition }) => acquisition)
    fixture.bounces[0].at = 5
    for (const entry of fixture.entries.slice(201)) entry.at = 7.2
    for (const entry of fixture.entries.slice(0, 134))
      successfulAcceptFixture(fixture.evidence, entry, 6)
    captureFreezes(fixture)
    const bounce = fixture.bounces[0]
    expect(bounce.held.size).toBe(201)
    expect(effectiveObligations(bounce).size).toBe(167)
    expect(bounce.interruptions[0].history).toHaveLength(167)
    expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toEqual([])
    const receipt = fixture.evidence.outcomesByCohort.get(fixture.entries[0].acquisition.id)[0]
    receipt.at = 8.1
    expect(
      freezeProblems(fixture.entries, fixture.bounces, fixture.evidence).length
    ).toBeGreaterThan(0)
    receipt.at = 7
    bounce.at = 4.5
    expect(
      freezeProblems(fixture.entries, fixture.bounces, fixture.evidence).length
    ).toBeGreaterThan(0)
  })

  it('uses one 300-identity ceiling across independent interruption scopes', () => {
    for (const count of [300, 301]) {
      const fixture = budgetFixture(count, count, false)
      for (const entry of fixture.entries.filter(({ id }) => id % 2 === 0)) {
        if (entry.id % 4) continue
        entry.partition = 1
        entry.acquisition.partition = 1
        entry.acquisition.source.response.responses[0].partitions[0].partitionIndex = 1
        entry.acquisition.source.frames[0].request.topics[0].partitions[0].partitionIndex = 1
        entry.acquisition.source.model[0].partition = 1
        entry.acquisition.open = 2
        entry.acquisition.source.open = 2
        entry.acquisition.source.frames[0].socket = 2
        entry.identity = shareRecordIdentity({
          topic: entry.topic,
          partition: 1,
          offset: entry.offset,
          key: Buffer.from('k'),
          value: Buffer.from('v'),
        })
      }
      fixture.evidence.retirements.push({ ...fixture.evidence.retirements[0], id: count, open: 2 })
      captureFreezes(fixture)
      expect(fixture.bounces[0].interruptions).toHaveLength(2)
      expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence).length > 0).toBe(
        count === 301
      )
    }
  })

  it('retains the archived-size 133 carry plus two live groups of 100 without inventing receipts', () => {
    const fixture = budgetFixture(333, [33, 100, 200], true)
    for (const entry of fixture.entries.slice(-100)) {
      const related = fixture.evidence.exchanges.filter(
        (exchange) =>
          exchange === entry.acquisition.source ||
          exchange.tickets.some(({ cohort }) => cohort === entry.acquisition.id)
      )
      entry.acquisition.open = 99
      for (const exchange of related) {
        exchange.nodeId = 3
        exchange.open = 99
        exchange.endpoint = 'node-3'
        for (const frame of exchange.frames) {
          frame.socket = 99
          frame.endpoint = 'node-3'
          frame.nodeId = 3
        }
      }
    }
    captureFreezes(fixture)
    const accounting = freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence)
    expect(accounting.problems).toEqual([])
    expect(
      accounting.observations.find(({ bounce, phase }) => bounce === 2 && phase === 'command')
    ).toMatchObject({ historyCount: 333, liveCount: 200, retainedCount: 133 })
    const carry = fixture.bounces[2].carried
    expect(carry.filter(({ originatingEpisode }) => originatingEpisode === 0)).toHaveLength(33)
    expect(carry.filter(({ originatingEpisode }) => originatingEpisode === 1)).toHaveLength(100)
    expect(fixture.evidence.outcomes.filter(({ error }) => error === null)).toEqual([])
  })

  it.each([300, 301])('separates live and new limits from 133 carried identities (%s)', (count) => {
    const fixture = budgetFixture(133 + count, [33, 100, count], true)
    const accounting = freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence)
    expect(fixture.bounces[2].held.size).toBe(133 + count)
    expect(accounting.episodes[2]).toMatchObject({
      newCount: count,
      carriedCount: 133,
      frozenCount: 133 + count,
    })
    expect(accounting.problems.includes('Live pending work exceeds its allowance')).toBe(
      count > 300
    )
    expect(accounting.problems.includes('bounce 2 new uncertainty exceeds its allowance')).toBe(
      count > 300
    )
    if (count === 300) expect(accounting.problems).toEqual([])
  })

  it('keeps all six disjoint admissions while bounding carry at each command and cut', () => {
    const fixture = budgetFixture(1800, 300, true)
    const accounting = freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence)
    expect(accounting.problems).toEqual([])
    expect(accounting.eligibleIdentities).toHaveLength(1800)
    expect(accounting.episodes.map(({ newCount }) => newCount)).toEqual(Array(6).fill(300))
    for (const [index, bounce] of fixture.bounces.entries()) {
      expect(bounce.held.size).toBe(300 * (index + 1))
      expect(effectiveObligations(bounce).size).toBe(300 * (index + 1))
      expect(accounting.episodes[index].carriedCount).toBe(300 * index)
    }
    const last = fixture.bounces[5]
    fixture.bounces.push({ ...last, index: 6, at: 607, interruptions: [] })
    fixture.evidence.commands.push({ ...fixture.evidence.commands[5], index: 6, at: 607 })
    expect(freezeProblems(fixture.entries, fixture.bounces, fixture.evidence)).toContain(
      'Fault admission requires one of the six original episodes'
    )
  })

  it('accounts for 2100 raw unresolved identities without enlarging the 1800 eligible union', () => {
    const fixture = budgetFixture(2100, 300, true)
    fixture.bounces = fixture.bounces.slice(0, 6)
    fixture.evidence.commands = fixture.evidence.commands.slice(0, 6)
    const cut = { at: 607 }
    const history = historyAccounting(fixture.entries, fixture.bounces, fixture.evidence, cut)
    expect(history).toMatchObject({
      historyCount: 2100,
      liveCount: 300,
      retainedCount: 1800,
      problems: [],
    })
    const accounting = freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence)
    expect(accounting.problems).toEqual([])
    expect(accounting.eligibleIdentities).toHaveLength(1800)
    expect(effectiveObligations(fixture.bounces[5]).size).toBe(1800)
    const ordinary = fixture.entries[1800]
    expect(
      unknownBudgets(
        [{ identity: ordinary.identity, entry: ordinary.id, bounce: 5 }],
        accounting.episodes
      ).problems.length
    ).toBeGreaterThan(0)
    expect(
      replayBudgets(
        [{ identity: ordinary.identity, originalEntry: ordinary.id, entry: 90000, bounce: 5 }],
        accounting.episodes
      ).problems.length
    ).toBeGreaterThan(0)
    fixture.evidence.retirements.push({
      ...fixture.evidence.retirements.at(-1),
      id: 90000,
      at: 606,
    })
    expect(
      historyAccounting(fixture.entries, fixture.bounces, fixture.evidence, cut).problems
    ).toContain('Unresolved retired history has no original admission')
  })

  it('rejects 301 ordinary live identities even beside the complete admitted history', () => {
    const fixture = budgetFixture(2101, [300, 300, 300, 300, 300, 300, 301], true)
    fixture.bounces = fixture.bounces.slice(0, 6)
    const history = historyAccounting(fixture.entries, fixture.bounces, fixture.evidence, {
      at: 607,
    })
    expect(history.problems).toContain('Live pending work exceeds its allowance')
  })

  it('does not refresh an admission or unknown charge through repeated loss callbacks and later freezes', () => {
    const fixture = budgetFixture(6, 1, true)
    const original = JSON.stringify(fixture.bounces[0].interruptions)
    const loss = fixture.evidence.owner.tap.socketEvents[0]
    fixture.evidence.owner.tap.socketEvents.push({ ...loss, id: 90000, at: loss.at + 1 })
    captureFreezes(fixture)
    const result = settlementCandidates(
      fixture.entries,
      new Set(fixture.corpus),
      fixture.bounces,
      fixture.evidence
    )
    expect(result.problems).toEqual([])
    expect(result.perBounce).toEqual([1, 1, 1, 1, 1, 1])
    expect(JSON.stringify(fixture.bounces[0].interruptions)).toBe(original)
    expect(result.uncertainty.episodes.map(({ newCount }) => newCount)).toEqual([1, 1, 1, 1, 1, 1])
    const candidate = result.unknown[0]
    expect(
      unknownBudgets([{ ...candidate, bounce: 3 }], result.uncertainty.episodes).problems.length
    ).toBeGreaterThan(0)
    expect(
      unknownBudgets([candidate, candidate], result.uncertainty.episodes).problems.length
    ).toBeGreaterThan(0)
  })

  it('charges a delayed recovery to its original episode and never refunds frozen admissions', () => {
    const fixture = budgetFixture(4, 1, false)
    const recovery = fixture.entries.find(({ id }) => id === 1)
    const shift = 300
    recovery.at += shift
    recovery.acquisition.adoptedAt += shift
    recovery.acquisition.source.frames[0].at += shift
    recovery.acquisition.source.frames[0].responseAt += shift
    for (const exchange of fixture.evidence.exchangesByCohort.get(recovery.acquisition.id)) {
      exchange.frames[0].at += shift
      exchange.frames[0].responseAt += shift
    }
    for (const outcome of fixture.evidence.results(recovery)) outcome.at += shift
    fixture.entries.sort((a, b) => a.at - b.at)
    captureFreezes(fixture)
    const result = audit(fixture)
    expect(result.problems).toEqual([])
    expect(result.perBounce).toEqual([1, 1, 1, 1])
    const extra = result.extras.find(({ entry }) => entry === recovery.id)
    expect(extra.bounce).toBe(0)
    expect(result.uncertainty.episodes[0].newCount).toBe(1)
    expect(
      replayBudgets([{ ...extra, bounce: 3 }], result.uncertainty.episodes).problems.length
    ).toBeGreaterThan(0)
    expect(
      replayBudgets([extra, extra], result.uncertainty.episodes).problems.length
    ).toBeGreaterThan(0)
    const before = JSON.stringify(fixture.bounces[0].interruptions)
    expect(fixture.bounces[3].carried.some(({ entry }) => entry === 0)).toBe(true)
    expect(
      historyAccounting(fixture.entries, fixture.bounces, fixture.evidence, { at: 330 }).retained
    ).not.toEqual(expect.arrayContaining([expect.objectContaining({ entry: 0 })]))
    expect(JSON.stringify(fixture.bounces[0].interruptions)).toBe(before)
    expect(fixture.evidence.results(recovery)[0].error).toBeNull()
  })

  it('admits a distinct later interrupted attempt even when its identity already occurs in carry', () => {
    const fixture = budgetFixture(2, 1, true)
    const [old, later] = fixture.entries
    later.offset = old.offset
    later.identity = old.identity
    later.deliveryCount = 2
    later.acquisition.acquiredRecords[0].deliveryCount = 2
    for (const range of later.acquisition.acquiredRecords) {
      range.firstOffset = old.offset
      range.lastOffset = old.offset
    }
    for (const exchange of fixture.evidence.exchanges) {
      if (exchange.open !== later.acquisition.open) continue
      for (const ticket of exchange.tickets)
        for (const batch of ticket.batches) batch.firstOffset = batch.lastOffset = old.offset
      for (const frame of exchange.frames)
        for (const topic of frame.request.topics)
          for (const partition of topic.partitions)
            for (const batch of partition.acknowledgementBatches)
              batch.firstOffset = batch.lastOffset = old.offset
    }
    for (const outcome of fixture.evidence.outcomesByCohort.get(later.acquisition.id))
      outcome.firstOffset = outcome.lastOffset = old.offset
    fixture.corpus = [old.identity]
    captureFreezes(fixture)
    const result = audit(fixture)
    expect(result.problems).toEqual([])
    expect(result.uncertainty.episodes.map(({ newCount }) => newCount)).toEqual([1, 1])
    expect(result.uncertainty.eligibleIdentities).toEqual([old.identity])
    expect(effectiveObligations(fixture.bounces[1]).get(old.identity)).toHaveLength(2)
    const candidates = settlementCandidates(
      fixture.entries,
      new Set(fixture.corpus),
      fixture.bounces,
      fixture.evidence
    )
    expect(candidates.problems).toEqual([])
    expect(candidates.perBounce).toEqual([0, 1])
    expect(candidates.unknown[0].entry).toBe(later.id)
  })

  it.each([
    [
      'new attempt labelled carry',
      (bounce) => {
        const cut = bounce.interruptions[0]
        const held = { ...cut.obligations[0], originatingEpisode: 0 }
        bounce.interruptions[0] = { ...cut, obligations: [], carried: [...cut.carried, held] }
      },
    ],
    [
      'forged original owner',
      (bounce) => {
        const cut = bounce.interruptions[0]
        bounce.interruptions[0] = {
          ...cut,
          carried: cut.carried.map((held) => ({ ...held, originatingEpisode: 1 })),
        }
      },
    ],
    [
      'borrowed socket',
      (bounce) => {
        const cut = bounce.interruptions[0]
        bounce.interruptions[0] = {
          ...cut,
          carried: cut.carried.map((held) => ({
            ...held,
            authority: { ...held.authority, socket: 999 },
          })),
        }
      },
    ],
    [
      'omitted carry',
      (bounce) => {
        const cut = bounce.interruptions[0]
        bounce.interruptions[0] = { ...cut, carried: cut.carried.slice(1) }
      },
    ],
    [
      'omitted entire interruption',
      (bounce) => {
        bounce.interruptions = []
      },
    ],
  ])('reconstructs provenance despite %s with unchanged primitive journals', (_name, mutate) => {
    const fixture = budgetFixture(3, 1, true)
    const primitive = JSON.stringify(fixture.evidence.retirements)
    mutate(fixture.bounces[2])
    expect(
      freezeProblems(fixture.entries, fixture.bounces, fixture.evidence).length
    ).toBeGreaterThan(0)
    expect(JSON.stringify(fixture.evidence.retirements)).toBe(primitive)
  })

  it.each([false, true])(
    'keeps 300/1800 event and unknown budgets separate from carry (%s)',
    (unknown) => {
      const fixture = budgetFixture(1800, 300, unknown)
      const result = unknown
        ? settlementCandidates(
            fixture.entries,
            new Set(fixture.corpus),
            fixture.bounces,
            fixture.evidence
          )
        : audit(fixture)
      const charges = unknown ? result.unknown : result.extras
      const budget = unknown ? unknownBudgets : replayBudgets
      const episodes = result.uncertainty.episodes
      expect(budget(charges, episodes).problems).toEqual([])
      const originOverflow = [...charges.slice(0, 300), { ...charges[0], entry: 90000 }]
      expect(budget(originOverflow, episodes).perBounce[0]).toBe(301)
      expect(budget(originOverflow, episodes).problems.length).toBeGreaterThan(0)
      const totalOverflow = [...charges, { ...charges[0], entry: 90000 }]
      expect(budget(totalOverflow, episodes).problems.length).toBeGreaterThan(0)
      const moved = originOverflow.map((charge, index) =>
        index === 300 ? { ...charge, bounce: 3 } : charge
      )
      expect(budget(moved, episodes).problems.length).toBeGreaterThan(0)
    }
  )

  it.each([300, 301])(
    'checks the historical new pool across staggered scopes with carry (%s)',
    (count) => {
      const fixture = budgetFixture(133 + count, [33, 100, count], true)
      const current = fixture.entries.slice(133)
      const later = current.slice(150)
      const originalOpen = later[0].acquisition.open
      for (const entry of later) {
        entry.at = 210
        entry.partition = 1
        entry.acquisition.partition = 1
        entry.acquisition.open = 99
        entry.acquisition.adoptedAt = 209.4
        const source = entry.acquisition.source
        source.open = 99
        source.model[0].partition = 1
        source.response.responses[0].partitions[0].partitionIndex = 1
        source.frames[0].at = 209
        source.frames[0].responseAt = 209.2
        source.frames[0].socket = 99
        source.frames[0].request.topics[0].partitions[0].partitionIndex = 1
        const exchange = fixture.evidence.exchangesByCohort.get(entry.acquisition.id)[0]
        exchange.open = 99
        exchange.prepared.at = 210.2
        exchange.failedAt = 211.1
        exchange.frames[0].socket = 99
        exchange.frames[0].at = 210.5
        exchange.frames[0].lostAt = 211
        exchange.frames[0].request.topics[0].partitions[0].partitionIndex = 1
        fixture.evidence.results(entry)[0].at = 211.4
        entry.identity = shareRecordIdentity({
          topic: entry.topic,
          partition: 1,
          offset: entry.offset,
          key: Buffer.from('k'),
          value: Buffer.from('v'),
        })
      }
      const movedRequests = new Set(
        later.map(
          (entry) => fixture.evidence.exchangesByCohort.get(entry.acquisition.id)[0].frames[0].id
        )
      )
      for (const event of fixture.evidence.owner.tap.socketEvents)
        event.outstanding = event.outstanding.filter((id) => !movedRequests.has(id))
      fixture.evidence.owner.tap.socketEvents.push({
        id: 90000,
        socket: 99,
        at: 211,
        event: 'end',
        outstanding: later.map(
          (entry) => fixture.evidence.exchangesByCohort.get(entry.acquisition.id)[0].frames[0].id
        ),
      })
      fixture.evidence.retirements.push({
        ...fixture.evidence.retirements.find(({ open }) => open === originalOpen),
        id: 90000,
        open: 99,
        at: 211.2,
      })
      fixture.bounces[2].downAt = 212
      captureFreezes(fixture)
      const result = freezeAccounting(fixture.entries, fixture.bounces, fixture.evidence)
      expect(result.observations.every(({ liveCount }) => liveCount <= 300)).toBe(true)
      expect(result.episodes[2].newCount).toBe(count)
      expect(result.episodes[2].carriedCount).toBe(133)
      expect(result.problems.includes('bounce 2 new uncertainty exceeds its allowance')).toBe(
        count > 300
      )
      if (count === 300) expect(result.problems).toEqual([])
    }
  )

  it('matches every outstanding Accept to the first shared socket loss', () => {
    const fixture = budgetFixture(2, 2, true)
    /** @returns {object} Candidates recomputed from the original loss and request evidence. */
    const classify = () =>
      settlementCandidates(
        fixture.entries,
        new Set(fixture.corpus),
        fixture.bounces,
        fixture.evidence
      )
    expect(classify().problems).toEqual([])
    expect(classify().unknown.map(({ identity }) => identity)).toEqual(fixture.corpus)
    expect(fixture.evidence.owner.tap.socketEvents).toHaveLength(1)
    const loss = fixture.evidence.owner.tap.socketEvents[0]
    expect(loss.outstanding).toHaveLength(2)
    loss.outstanding.pop()
    expect(classify().missing).toEqual([fixture.corpus[1]])
  })

  it('retains carried retired unknowns without granting a new pool or episode charge', () => {
    const fixture = budgetFixture(301, 300, true)
    const result = settlementCandidates(
      fixture.entries,
      new Set(fixture.corpus),
      fixture.bounces,
      fixture.evidence
    )
    expect(result.perBounce).toEqual([300, 1])
    expect(result.problems).toEqual([])
    expect(fixture.bounces[1].held.size).toBe(301)
    expect(effectiveObligations(fixture.bounces[1]).size).toBe(301)
    expect(fixture.bounces[1].interruptions[0].carried).toHaveLength(300)
    expect(
      fixture.bounces[1].interruptions[0].carried.every(
        ({ originatingEpisode }) => originatingEpisode === 0
      )
    ).toBe(true)
    const cut = fixture.bounces[1].interruptions[0]
    fixture.bounces[1].interruptions[0] = { ...cut, carried: [] }
    expect(
      freezeProblems(fixture.entries, fixture.bounces, fixture.evidence).length
    ).toBeGreaterThan(0)
  })

  it('permits late higher-count replay but requires an independent epoch bracket for equal counts', () => {
    const fixture = replayFixture({ count: 2 })
    fixture.bounces[0].at = 3
    fixture.old.acquisition.source.frames[0].at = 3.1
    fixture.old.acquisition.source.frames[0].responseAt = 3.2
    fixture.old.acquisition.adoptedAt = 3.3
    captureFreezes(fixture)
    expect(audit(fixture).problems).toEqual([])
    fixture.next.deliveryCount = 1
    fixture.next.acquisition.acquiredRecords[0].deliveryCount = 1
    expect(audit(fixture).problems).toContainEqual(
      expect.stringContaining(`${fixture.old.identity} has no proved delivery-count progression`)
    )
    const source = fixture.old.acquisition.source
    const transition = {
      ...source,
      id: 50,
      tickets: [],
      frames: [{ ...source.frames[0], id: 50, at: 6.2, responseAt: 6.4 }],
      response: {
        errorCode: 0,
        responses: [
          {
            topicId: fixture.old.topicId,
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 6,
                acknowledgeErrorCode: 0,
                acquiredRecords: [],
                currentLeader: { leaderId: 2, leaderEpoch: 2 },
              },
            ],
          },
        ],
      },
    }
    fixture.evidence.exchanges.push(transition)
    expect(audit(fixture).problems).toEqual([])
    expect(audit(fixture).extras[0].epochWitness.transition).toBe(50)
    transition.response.responses[0].partitions[0].currentLeader.leaderEpoch = 1
    expect(audit(fixture).problems).toContainEqual(
      expect.stringContaining(`${fixture.old.identity} has no proved delivery-count progression`)
    )
  })

  it.each([
    ['unwritten', { written: false }],
    ['Release', { type: 'release' }],
    ['Reject', { type: 'reject' }],
  ])('cannot turn a late %s decision into terminal success', (_name, options) => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture, options)
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    proof.oraclePassed = true
    proof.settled = true
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
    expect(proof.evidence.outcomes.filter(({ error }) => error === null)).toEqual([])
  })

  it('does not reopen a partition for a replacement generation after its first interruption', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    fixture.bounces[0].at = 3
    fixture.bounces[0].downAt = 15
    const replacement = fixture.next
    replacement.acquisition.source.nodeId = 1
    replacement.acquisition.source.endpoint = 'node-1'
    replacement.acquisition.source.frames[0].endpoint = 'node-1'
    replacement.acquisition.source.frames[0].nodeId = 1
    replacement.acquisition.source.frames[0].at = 8.4
    replacement.acquisition.source.frames[0].responseAt = 8.5
    replacement.acquisition.adoptedAt = 8.6
    replacement.at = 8.7
    fixture.evidence.retirements.push({
      ...fixture.evidence.retirements[0],
      id: 1,
      open: 1,
      at: 9.2,
    })
    const original = fixture.old
    fixture.old = replacement
    lostAcceptFixture(fixture, { writtenAt: 8.8, lostAt: 9 })
    fixture.old = original
    expect(fixture.bounces[0].interruptions).toHaveLength(1)
    expect(fixture.bounces[0].interruptions[0].history).toEqual([0])
    proof.entries.push(replacement)
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
    expect(freezeProblems(proof.entries, proof.bounces, proof.evidence)).toEqual([])
  })

  it('retains the write chronology when an Accept is written during the down command', () => {
    const proof = lostAcceptFixture(replayFixture())
    proof.bounces[0].inFlight = []
    proof.evidence.exchanges.at(-1).frames[0].at = 7.5
    expect(auditTerminal(proof).problems).toEqual([])
  })

  it('does not charge a post-freeze handler to a later unrelated bounce', () => {
    const proof = lostAcceptFixture(replayFixture())
    const original = proof.bounces[0]
    proof.bounces.push({
      ...original,
      index: 1,
      nodeId: 2,
      at: 19,
      downAt: 20,
      restoredAt: 30,
    })
    original.at = 3
    original.held = new Set()
    original.obligations = []
    original.interruptions = []
    const settlement = auditTerminal(proof).settlement
    expect(settlement.unknown).toEqual([])
    expect(settlement.missing).toEqual(proof.corpus)
    expect(settlement.unresolved[0]).toMatchObject({
      entry: 0,
      handlerAt: 4,
      frozenEpisodes: [{ index: 1, nodeId: 2, at: 19, downAt: 20 }],
      accepts: [{ exchange: 100, nodeId: 1, writtenAt: 6, lostAt: 8, failedAt: 8.1 }],
    })
  })

  it('requires recovery when the lost Accept was not applied and retains its old unknown history', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture, { applied: false })
    fixture.bounces[0].at = 3
    captureFreezes(fixture)
    expect(fixture.bounces[0].held.size).toBe(0)
    expect(auditTerminal(proof).problems).toContain('Incomplete terminal proof')
    proof.entries.push(fixture.next)
    expect(
      settlementCandidates(proof.entries, new Set(proof.corpus), proof.bounces, proof.evidence)
        .missing
    ).toEqual(proof.corpus)
    const request = {
      ...fixture.next.acquisition.source.frames[0].request,
      topics: [
        {
          topicId: fixture.next.topicId,
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
    }
    const response = {
      errorCode: 0,
      responses: [
        { topicId: fixture.next.topicId, partitions: [{ partitionIndex: 0, errorCode: 0 }] },
      ],
    }
    const exchange = {
      id: 101,
      apiKey: 79,
      open: 1,
      pool: 1,
      generation: 1,
      frames: [
        {
          request,
          at: 24,
          responseAt: 25,
          apiKey: 79,
          apiVersion: 2,
          socket: 1,
          endpoint: 'node-2',
          responseBytes: 'original-success',
        },
      ],
      response,
      tickets: [
        {
          cohort: 1,
          ledger: 1,
          ticket: 101,
          isRenewAck: false,
          batches: request.topics[0].partitions[0].acknowledgementBatches,
        },
      ],
    }
    proof.evidence.exchanges.push(exchange)
    proof.evidence.exchangesByCohort.set(1, [exchange])
    proof.evidence.outcomes.push({
      cohort: 1,
      type: 'accept',
      firstOffset: '0',
      lastOffset: '0',
      error: null,
      at: 25,
    })
    proof.evidence.outcomesByCohort.set(1, [proof.evidence.outcomes[1]])
    const settled = settlementCandidates(
      proof.entries,
      new Set(proof.corpus),
      proof.bounces,
      proof.evidence
    )
    expect(settled.successful).toEqual(proof.corpus)
    expect(settled.unknown).toEqual([])
    expect(proof.evidence.outcomes[0].error.reason).toBe('session-lost')
  })

  it.each([
    ['unwritten Accept', { written: false }],
    ['pre-write refusal', { written: false, reason: 'not-sent' }],
    ['Release', { type: 'release' }],
    ['Reject', { type: 'reject' }],
    ['Renew', { type: 'renew' }],
  ])('cannot settle %s as a remote unknown Accept', (_name, options) => {
    const proof = lostAcceptFixture(replayFixture(), options)
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })

  it.each([
    [
      'missing corpus identity',
      (p) => {
        p.raw = []
      },
    ],
    [
      'never handled identity',
      (p) => {
        p.entries = []
      },
    ],
    [
      'changed value bytes',
      (p) => {
        p.entries[0].valueBytes = '00'
      },
    ],
    [
      'foreign group receipt',
      (p) => {
        p.evidence.exchanges.at(-1).frames[0].request.groupId = 'foreign'
      },
    ],
    [
      'foreign topic UUID',
      (p) => {
        p.before.named[0].topicId = '22222222-2222-3333-4444-555555555555'
      },
    ],
    [
      'duplicate raw offset',
      (p) => {
        p.raw.push(p.raw[0])
      },
    ],
    [
      'duplicate range masking a hole',
      (p) => {
        p.evidence.exchanges
          .at(-1)
          .frames[0].request.topics[0].partitions[0].acknowledgementBatches.push({
            firstOffset: '0',
            lastOffset: '0',
            acknowledgeTypes: [1],
          })
      },
    ],
    [
      'delivery exhaustion',
      (p) => {
        p.limits = [1, 1, 1]
      },
    ],
    [
      'final count at the limit',
      (p) => {
        p.entries[0].deliveryCount = 5
        p.entries[0].acquisition.acquiredRecords[0].deliveryCount = 5
      },
    ],
    [
      'unknown delivery limit',
      (p) => {
        p.limits = []
      },
    ],
    [
      'positive lag',
      (p) => {
        p.before.named[0].partitions[0].lag = '1'
      },
    ],
    [
      'null lag',
      (p) => {
        p.before.named[0].partitions[0].lag = null
      },
    ],
    [
      'missing lag',
      (p) => {
        delete p.before.named[0].partitions[0].lag
      },
    ],
    [
      'negative SPSO',
      (p) => {
        p.before.named[0].partitions[0].startOffset = '-1'
      },
    ],
    [
      'SPSO beyond end',
      (p) => {
        p.before.named[0].partitions[0].startOffset = '2'
      },
    ],
    [
      'wrong independent end',
      (p) => {
        p.before.ends[0].high = '2'
      },
    ],
    [
      'listing disagreement',
      (p) => {
        p.before.all[0].partitions[0].leaderEpoch++
      },
    ],
    [
      'missing partition',
      (p) => {
        p.before.all[0].partitions = []
      },
    ],
    [
      'partition error',
      (p) => {
        p.before.named[0].partitions[0].errorCode = 6
      },
    ],
    [
      'silent verifier',
      (p) => {
        p.quiet.events = []
      },
    ],
    [
      'unassigned verifier',
      (p) => {
        p.quiet.assignment = []
      },
    ],
    [
      'short verifier',
      (p) => {
        p.quiet.endedAt = 100
      },
    ],
    [
      'verifier delivery',
      (p) => {
        p.quiet.delivered = [...p.corpus]
      },
    ],
    [
      'missing post-verifier proof',
      (p) => {
        p.after = null
      },
    ],
    [
      'untyped unknown',
      (p) => {
        p.evidence.outcomes[0].typed = false
      },
    ],
    [
      'known rejection',
      (p) => {
        p.evidence.outcomes[0].error.reason = 'rejected-by-broker'
      },
    ],
    [
      'membership loss',
      (p) => {
        p.evidence.outcomes[0].error.reason = 'membership-lost'
      },
    ],
    [
      'unrelated cause',
      (p) => {
        p.evidence.outcomes[0].cause = 98
      },
    ],
    [
      'uninterrupted request',
      (p) => {
        p.bounces[0].inFlight = []
        p.evidence.exchanges.at(-1).frames[0].lostAt = undefined
      },
    ],
    [
      'already received answer',
      (p) => {
        p.evidence.exchanges.at(-1).frames[0].responseBytes = 'known-answer'
      },
    ],
    [
      'forgotten retired obligation',
      (p) => {
        p.bounces[0].obligations = []
      },
    ],
    [
      'accumulated unresolved history',
      (p) => {
        p.bounces[0].held = new Set(Array.from({ length: 301 }, (_, i) => String(i)))
      },
    ],
  ])('fails finalization with %s despite a forged passing summary', (name, mutate) => {
    const proof = lostAcceptFixture(replayFixture())
    proof.oraclePassed = true
    proof.settled = true
    mutate(proof)
    const component =
      {
        'missing corpus identity': 'Raw corpus mismatch',
        'never handled identity': 'Missing handler identity',
        'changed value bytes': `${proof.corpus[0]} lacks an adopted wire acquisition`,
        'duplicate raw offset': 'Raw corpus mismatch',
        'delivery exhaustion': `${proof.corpus[0]} may have exhausted its delivery limit`,
        'final count at the limit': `${proof.corpus[0]} may have exhausted its delivery limit`,
        'unknown delivery limit': 'Missing delivery-limit configuration',
        'silent verifier': 'Inactive or nonquiet verifier',
        'unassigned verifier': 'Inactive or nonquiet verifier',
        'short verifier': 'Inactive or nonquiet verifier',
        'verifier delivery': 'Inactive or nonquiet verifier',
        'foreign group receipt': `Unsettled ${proof.corpus[0]}`,
        'duplicate range masking a hole': `Unsettled ${proof.corpus[0]}`,
        'untyped unknown': `Unsettled ${proof.corpus[0]}`,
        'known rejection': `Unsettled ${proof.corpus[0]}`,
        'membership loss': `Unsettled ${proof.corpus[0]}`,
        'unrelated cause': `Unsettled ${proof.corpus[0]}`,
        'uninterrupted request': `Unsettled ${proof.corpus[0]}`,
        'already received answer': `Unsettled ${proof.corpus[0]}`,
        'forgotten retired obligation': 'bounce 0 command snapshot disagrees with the journal',
        'accumulated unresolved history': 'bounce 0 command snapshot disagrees with the journal',
      }[name] ?? 'Incomplete terminal proof'
    expect(auditTerminal(proof).problems).toContain(component)
  })

  it('rejects duplicate terminal outcomes without converting an unknown into success', () => {
    const proof = lostAcceptFixture(replayFixture())
    const original = proof.evidence.outcomes[0]
    proof.evidence.outcomesByCohort.get(0).push({ ...original })
    expect(auditTerminal(proof).problems).toContain(
      `${proof.corpus[0]} has duplicate terminal outcomes for one acquisition`
    )
  })

  it('cannot emit completion from candidate classifications alone', async () => {
    const instance = new ShareBounceCase()
    instance.settlement = { successful: [], unknown: [{ identity: 'candidate' }], missing: [] }
    await expect(instance.complete(NEVER)).rejects.toThrow()
  })

  it('does not let an old unknown discharge a later handler acquisition', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture)
    proof.entries.push(fixture.next)
    expect(auditTerminal(proof).settlement.missing).toEqual(proof.corpus)
  })
})

enabled('share bounce on a live group', () => {
  let deadline
  beforeAll(() => {
    deadline = performance.now() + FILE_MS
    return bounded(NEVER, HOOK_MS - CASE_JOIN_MS, () => {
      liveRun.assertClean()
    })
  }, HOOK_MS)

  it(
    TITLE,
    liveCase(BUDGET_MS, (scope) => new ShareBounceCase().run(scope, deadline)),
    caseTimeout(BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
