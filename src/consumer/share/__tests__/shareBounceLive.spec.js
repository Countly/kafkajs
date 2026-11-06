const net = require('net')
const { EventEmitter } = require('events')
const { AsyncLocalStorage } = require('async_hooks')
const Broker = require('../../../broker')
const RequestQueue = require('../../../network/requestQueue')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareRun } = require('../shareRun')
const { KafkaJSShareAcknowledgementError } = require('../../../errors')
const fetchWire = require('../../../../testHelpers/wire/shareFetchV2')
const ackWire = require('../../../../testHelpers/wire/shareAcknowledgeV2')
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
const BODY_MS = 500000
const HOOK_MS = 10000
const CLEANUP_MS = 45000
const BUDGET_MS = BODY_MS + CLEANUP_MS
const PARTITIONS = 3
const RECORDS = 30000
const BURST_SIZE = 100
const BURSTS = RECORDS / BURST_SIZE
const BURST_INTERVAL_MS = 400
const LATE_BURST_MS = 1000
const WARMUP_APPENDS = 2000
const MAX_OUTAGE_MS = 45000
const MAX_HELD = 300
const MAX_REPLAY = 6 * MAX_HELD
const MEMBERSHIP_QUIET_MS = 3000
const OLD_TIMESTAMP = '1700000000000'
const SERVICES = ['kafka1', 'kafka2', 'kafka3']
const SHARE_STATE_TOPIC = '__share_group_state'
const OFFSETS_TOPIC = '__consumer_offsets'
const SHARE_APIS = new Set([76, 78, 79])
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
      endpoint: `${options.host}:${options.port}`,
      writer: new FrameSplitter(),
      reader: new FrameSplitter(),
      outstanding: new Map(),
    }
    this.sockets.push(entry)
    for (const event of ['end', 'close', 'error'])
      socket.on(event, (error) => {
        const at = performance.now()
        this.socketEvents.push({
          socket: this.sockets.indexOf(entry),
          event,
          at,
          error: error instanceof Error ? describeError(error) : null,
        })
        for (const frame of entry.outstanding.values()) frame.lostAt ??= at
      })
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      const bytes = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
      const frames = entry.writer.push(bytes)
      const result = write(data, ...rest)
      frames.forEach((frame) => this.record(entry, frame))
      return result
    }
    socket.on('data', (chunk) => {
      entry.reader.push(chunk).forEach((bytes) => {
        const request = entry.outstanding.get(bytes.readInt32BE(4))
        if (!request) return
        request.responseBytes = bytes.toString('base64')
        request.responseAt = performance.now()
        entry.outstanding.delete(request.correlationId)
      })
    })
    return socket
  }

  /**
   * Retains the complete written frame and its socket-scoped request identity before any answer.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @returns {void}
   */
  record(entry, frame) {
    const { apiKey, apiVersion, correlationId, clientId, byteLength } = readRequestHeader(frame)
    const fact = {
      id: this.allFrames.length,
      label: entry.label,
      socket: this.sockets.indexOf(entry),
      endpoint: entry.endpoint,
      apiKey,
      apiVersion,
      correlationId,
      clientId,
      at: performance.now(),
      bytes: frame.toString('base64'),
    }
    if ([78, 79].includes(apiKey) && apiVersion === 2) {
      const reader = apiKey === 78 ? fetchWire : ackWire
      try {
        fact.request = reader.readRequest(frame.subarray(byteLength))
      } catch (error) {
        fact.requestError = describeError(error)
      }
    }
    this.allFrames.push(fact)
    entry.outstanding.set(correlationId, fact)
    const context = this.context.getStore()
    if (context && context.apiKey === apiKey) context.frames.push(fact)
    if (SHARE_APIS.has(apiKey)) this.frames.push(fact)
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
    this.epochs = []
    this.brokers = new Set()
    this.cohorts = new Map()
    this.originalExchanges = new Map()
    this.restores = []
    this.handlers = new AsyncLocalStorage()
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
      const fact = {
        id: this.label(exchange),
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
      return original.call(loop, exchange, response)
    })
    this.wrap(ShareFetchLoop.prototype, 'failExchange', (loop, original, [exchange, error]) => {
      const fact = this.originalExchanges.get(exchange)
      if (fact) {
        fact.failedAt = performance.now()
        fact.error = describeError(error)
        fact.cause = this.label(error)
      }
      return original.call(loop, exchange, error)
    })
    this.wrap(ShareFetchLoop.prototype, 'adopt', (loop, original, args) => {
      const row = original.apply(loop, args)
      if (!owns(loop)) return row
      const [exchange, topicId, partition] = args
      const source = this.originalExchanges.get(exchange)
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
        adoptedAt: performance.now(),
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
            at: performance.now(),
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
        if (owns(loop))
          this.retirements.push({
            at: performance.now(),
            nodeId: loop.broker.nodeId,
            open: this.label(method === 'onSessionReset' ? args[0] : loop.session.token),
            key: method === 'retireKey' ? args[0] : null,
            reason: args[1],
            cause: this.label(args[2]),
            error: describeError(args[2]),
          })
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
        fact.response = response
        if (response.topicMetadata?.some(({ topic }) => topic === this.owner.topic))
          this.epochs.push(fact)
        return response
      })
    })
  }

  /** @returns {void} Restores originals after all workload and verifier calls have joined. */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.handlers.disable()
    this.owner.tap.context.disable()
  }

  /**
   * Samples the existing workload brokers' own metadata, without installing it in the client cache.
   * @param {AbortSignal} signal Existing drive scope.
   * @returns {Promise<void>} Once drive finishes; every sample stays within that scope.
   */
  async sampleEpochs(signal) {
    while (!this.owner.driveFinished) {
      for (const broker of [...this.brokers]) {
        signal.throwIfAborted()
        if (!broker.connectionPool.isConnected()) continue
        await bounded(signal, Infinity, () => broker.metadata([this.owner.topic])).catch(
          (error) => {
            if (signal.aborted) throw error
          }
        )
      }
      if (!this.owner.driveFinished) await delay(signal, 100)
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
        exchange.tickets.some(
          (ticket) =>
            ticket.cohort === entry.acquisition.id &&
            !ticket.isRenewAck &&
            ticket.batches.some((batch) => accepts(batch, entry.offset))
        ) &&
        exchange.frames[0]?.at <= outcome.at &&
        writtenAccept(exchange, entry)
    )
  }

  /**
   * @param {object} entry Handler start.
   * @returns {boolean} Whether an actual null-error Accept has matching original request/response.
   */
  successful(entry) {
    const success = this.results(entry).some(
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
    return success
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
    source.groupId === entry.groupId &&
    source.memberId === entry.memberId &&
    source.open !== null &&
    source.pool !== null &&
    Number.isInteger(source.generation) &&
    frame.at <= frame.responseAt &&
    frame.responseAt <= a.adoptedAt &&
    a.adoptedAt <= entry.at &&
    a.topic === entry.topic &&
    a.topicId === entry.topicId &&
    a.partition === entry.partition &&
    ranges.length === 1 &&
    ranges[0].deliveryCount === entry.deliveryCount
  )
}

/**
 * @param {object} entry Original handler acquisition.
 * @param {object} bounce Frozen episode.
 * @param {object[]} retirements Original open/partition authority losses.
 * @param {number|false} [after=false] Requires the new request to follow the authority loss.
 * @returns {boolean} Whether this bounce retired the original authority before reacquisition.
 */
const faultRetired = (entry, bounce, retirements, after = false) =>
  retirements.some(
    (loss) =>
      loss.nodeId === bounce.nodeId &&
      loss.open === entry.acquisition.open &&
      loss.at >= bounce.at &&
      loss.at <= bounce.restoredAt + MEMBERSHIP_QUIET_MS &&
      (!loss.key || loss.key === `${entry.topicId}:${entry.partition}`) &&
      ['connection', 'leader-moved', 'session-lost'].includes(loss.reason) &&
      (!after || loss.at < after)
  )

/**
 * Proves disjoint serving-epoch bounds using source-node local epoch observations and down barriers.
 * A source must itself be the observed leader: a remote cache, batch epoch or CurrentLeader default
 * supplies no bound. Same-node epochs never decrease; samples bracket the original wire intervals.
 * @param {object} old Previous handler acquisition.
 * @param {object} next New handler acquisition.
 * @param {object} bounce Designated outage.
 * @param {object[]} samples Original Metadata responses and their wire frames.
 * @returns {object|null} Source references and proven epoch bounds, or no proof.
 */
const epochAdvance = (old, next, bounce, samples) => {
  const ownSamples = (entry) =>
    samples.flatMap((sample) => {
      const topics = sample.response.topicMetadata.filter(
        ({ topicId }) => topicId === entry.topicId
      )
      if (topics.length !== 1 || sample.frames.length !== 1) return []
      const frame = sample.frames[0]
      if (frame.apiKey !== 3 || frame.apiVersion < 7 || !frame.responseBytes) return []
      const rows = topics[0].partitionMetadata.filter(
        ({ partitionId }) => partitionId === entry.partition
      )
      if (rows.length !== 1) return []
      const row = rows[0]
      return row.partitionErrorCode === 0 &&
        row.leader === sample.nodeId &&
        sample.nodeId === entry.acquisition.source.nodeId &&
        sample.pool === entry.acquisition.pool &&
        Number.isInteger(row.leaderEpoch) &&
        row.leaderEpoch >= 0
        ? [{ sample, frame, row }]
        : []
    })
  for (const before of ownSamples(old))
    for (const after of ownSamples(next)) {
      if (
        old.acquisition.source.frames[0].responseAt < before.frame.at &&
        before.frame.responseAt < bounce.at &&
        bounce.downAt < after.frame.at &&
        after.frame.responseAt < next.acquisition.source.frames[0].at &&
        before.row.leader === bounce.nodeId &&
        after.row.leaderEpoch > before.row.leaderEpoch
      )
        return {
          upper: before.row.leaderEpoch,
          lower: after.row.leaderEpoch,
          before: before.frame.id,
          after: after.frame.id,
          bounce: bounce.index,
        }
    }
  return null
}

/**
 * Recomputes once-only dispatch and independently charged replay from the original journals.
 * @param {object[]} entries All handler starts in order.
 * @param {object[]} bounces Immutable held sets, obligations and down/restore observations.
 * @param {Set<string>} corpus Immutable append identities.
 * @param {object} evidence Original epoch samples and authority retirements.
 * @returns {object} Charged extras, counters and all violations.
 */
const auditReplay = (entries, bounces, corpus, evidence) => {
  const previous = new Map()
  const dispatched = new Set()
  const problems = []
  const extras = []
  for (const entry of entries) {
    const old = previous.get(entry.identity)
    const key = `${entry.acquisition?.source?.frames[0]?.id}:${entry.topicId}:${entry.partition}:${entry.offset}`
    if (!corpus.has(entry.identity)) problems.push(`${entry.identity} is outside the corpus`)
    if (!validAcquisition(entry))
      problems.push(`${entry.identity} lacks an adopted wire acquisition`)
    if (
      !Number.isInteger(entry.deliveryCount) ||
      entry.deliveryCount < 1 ||
      entry.deliveryCount > 32767
    )
      problems.push(`${entry.identity} has an invalid delivery count`)
    if (dispatched.has(key)) problems.push(`${entry.identity} dispatched one acquisition twice`)
    dispatched.add(key)
    if (old) {
      const bounce = bounces.find(
        (episode) =>
          episode.held.has(entry.identity) &&
          episode.obligations.some((held) => held.entry === old.id) &&
          faultRetired(old, episode, evidence.retirements, entry.acquisition?.source?.frames[0]?.at)
      )
      const witness =
        bounce &&
        validAcquisition(old) &&
        validAcquisition(entry) &&
        epochAdvance(old, entry, bounce, evidence.epochs)
      if (!bounce) problems.push(`${entry.identity} has no original fault obligation`)
      else if (
        entry.deliveryCount < old.deliveryCount ||
        (entry.deliveryCount === old.deliveryCount && !witness)
      )
        problems.push(`${entry.identity} has no proved delivery-count progression`)
      else if (!dispatched.has(`charged:${entry.id}`)) {
        dispatched.add(`charged:${entry.id}`)
        extras.push({
          identity: entry.identity,
          entry: entry.id,
          bounce: bounce.index,
          deliveryCount: entry.deliveryCount,
          at: entry.at,
          epochWitness: witness,
        })
      } else problems.push(`${entry.identity} charged an entry twice`)
    }
    previous.set(entry.identity, entry)
  }
  const perBounce = bounces.map(({ index, held }) => {
    if (held.size > MAX_HELD)
      problems.push(`bounce ${index} held ${held.size} unresolved identities`)
    const count = extras.filter(({ bounce }) => bounce === index).length
    if (count > MAX_HELD) problems.push(`bounce ${index} took ${count} extra entries`)
    return count
  })
  if (extras.length > MAX_REPLAY) problems.push(`${extras.length} extra entries in total`)
  return { extras, perBounce, problems }
}

/**
 * Classifies successful and fault-unknown candidates without promoting any client outcome.
 * @param {object[]} entries Complete handler journal.
 * @param {Set<string>} corpus Exact immutable append corpus.
 * @param {object[]} bounces Frozen original obligations.
 * @param {BounceEvidence} evidence Original requests, results and authority losses.
 * @returns {object} Disjoint successful/candidate identity sets and missing identities.
 */
const settlementCandidates = (entries, corpus, bounces, evidence) => {
  const problems = []
  const successful = new Set()
  const last = new Map()
  for (const entry of entries) {
    last.set(entry.identity, entry)
    if (
      entry.acquisition &&
      evidence.results(entry).filter(({ type }) => type !== 'renew').length > 1
    )
      problems.push(`${entry.identity} has duplicate terminal outcomes for one acquisition`)
    if (corpus.has(entry.identity) && validAcquisition(entry) && evidence.successful(entry))
      successful.add(entry.identity)
  }
  const unknown = []
  const missing = []
  for (const identity of corpus) {
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
          const bounce = bounces.find(
            (episode) =>
              episode.held.has(identity) &&
              episode.obligations.some((held) => held.entry === entry.id) &&
              (episode.inFlight.some(({ id }) => id === frame.id) ||
                (frame.at >= episode.at && frame.at <= episode.downAt)) &&
              exchange.nodeId === episode.nodeId &&
              frame.lostAt >= episode.at &&
              frame.lostAt <= exchange.failedAt &&
              evidence.retirements.some(
                (loss) => loss.open === entry.acquisition.open && loss.cause === outcome.cause
              ) &&
              exchange.failedAt >= episode.at &&
              exchange.failedAt <= episode.restoredAt + MEMBERSHIP_QUIET_MS &&
              faultRetired(entry, episode, evidence.retirements)
          )
          if (bounce)
            candidate = {
              identity,
              entry: entry.id,
              bounce: bounce.index,
              exchange: exchange.id,
              error: outcome.error,
              outcomeAt: outcome.at,
            }
        }
      }
    if (candidate) unknown.push(candidate)
    else missing.push(identity)
  }
  const perBounce = bounces.map(
    ({ index }) => unknown.filter(({ bounce }) => bounce === index).length
  )
  const heldUnion = new Set(bounces.flatMap(({ held }) => [...held]))
  if (
    perBounce.some((count) => count > MAX_HELD) ||
    unknown.length > MAX_REPLAY ||
    heldUnion.size > MAX_REPLAY
  )
    problems.push('Unknown settlement exceeds the frozen fault allowance')
  if (bounces.some(({ held }) => held.size > MAX_HELD))
    problems.push('Unresolved held history exceeds its allowance')
  return { successful: [...successful].sort(), unknown, missing, perBounce, problems }
}

/**
 * Uses all observed acquisitions plus every potentially lost fetch as an acquisition upper bound.
 * A lost response can have acquired any identity it requested. No archive-producing decision,
 * unobserved consumer, retention or offset mutation is allowed in this isolated arm.
 * @param {object[]} entries Complete handler entries.
 * @param {object[]} exchanges Every original exchange, including lost responses.
 * @param {number[]} limits Actual readback from every broker.
 * @returns {string[]} Unproven archival exclusions.
 */
const archivalProblems = (entries, exchanges, limits) => {
  const problems = []
  if (
    limits.length !== SERVICES.length ||
    limits.some((limit) => !Number.isInteger(limit) || limit < 1)
  )
    return ['Missing delivery-limit configuration']
  const limit = Math.min(...limits)
  for (const entry of new Map(entries.map((entry) => [entry.identity, entry])).values()) {
    const possible = exchanges.filter(
      (exchange) =>
        exchange.apiKey === 78 &&
        exchange.frames.length &&
        (exchange.model.some(
          ({ topicId, partition }) => topicId === entry.topicId && partition === entry.partition
        ) ||
          exchange.frames[0].request?.topics.some(
            ({ topicId, partitions }) =>
              topicId === entry.topicId &&
              partitions.some(({ partitionIndex }) => partitionIndex === entry.partition)
          )) &&
        (!exchange.response ||
          exchange.response.responses.some(
            ({ topicId, partitions }) =>
              topicId === entry.topicId &&
              partitions.some(
                ({ partitionIndex, acquiredRecords }) =>
                  partitionIndex === entry.partition &&
                  acquiredRecords?.some(
                    (range) =>
                      BigInt(range.firstOffset) <= BigInt(entry.offset) &&
                      BigInt(range.lastOffset) >= BigInt(entry.offset)
                  )
              )
          ))
    )
    const counts = possible.flatMap(
      ({ response }) =>
        response?.responses.flatMap(({ topicId, partitions }) =>
          topicId === entry.topicId
            ? partitions.flatMap(({ partitionIndex, acquiredRecords }) =>
                partitionIndex === entry.partition
                  ? (acquiredRecords ?? [])
                      .filter(
                        (range) =>
                          BigInt(range.firstOffset) <= BigInt(entry.offset) &&
                          BigInt(range.lastOffset) >= BigInt(entry.offset)
                      )
                      .map(({ deliveryCount }) => deliveryCount)
                  : []
              )
            : []
        ) ?? []
    )
    const bound = Math.max(possible.length, ...counts)
    if (bound >= limit || counts.some((count) => !Number.isInteger(count) || count < 1))
      problems.push(`${entry.identity} may have exhausted its delivery limit`)
  }
  for (const exchange of exchanges)
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
  return problems
}

/**
 * Recomputes final acceptance from original identity, receipt, terminal and verifier streams.
 * @param {object} proof Independent corpus, original journals and raw terminal observations.
 * @returns {object} Settlement classes and every violated obligation; candidates alone never pass.
 */
const auditTerminal = ({
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
}) => {
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
  const replay = auditReplay(entries, bounces, identities, evidence)
  problems.push(...replay.problems)
  const settlement = settlementCandidates(entries, identities, bounces, evidence)
  problems.push(
    ...settlement.problems,
    ...settlement.missing.map((identity) => `Unsettled ${identity}`)
  )
  const unknown = new Set(settlement.unknown.map(({ identity }) => identity))
  problems.push(
    ...archivalProblems(
      entries.filter(({ identity }) => unknown.has(identity)),
      evidence.exchanges,
      limits
    )
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
    this.evidence = new BounceEvidence(this)
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      queueDepth: () => this.tap.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.bouncer = new BrokerBouncer({ now: () => performance.now() })
    this.openHandlers = 0
    this.requests = []
    this.expected = []
    this.sends = []
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
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: TITLE,
      profile: PROFILE,
      scenario: this.nonce,
      groups: { bounce: this.group },
      budgets: { caseMs: BUDGET_MS, cleanupMs: CLEANUP_MS },
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
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
   * @returns {number} the allowance, cut to what the case budget has left
   */
  window(limit, reserve = CLEANUP_MS) {
    return Math.max(1000, Math.min(limit, this.deadline - performance.now() - reserve))
  }

  /**
   * Runs the whole case: setup, the concurrent produce and bounce, settlement, the three oracles
   * and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    this.deadline = performance.now() + BUDGET_MS
    this.evidence.install()
    this.stack.add('restore test observers', () => this.evidence.restore())
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await bounded(scope, this.window(330000, 100000), (inner) => this.drive(inner), {
            label: 'The produce and bounce',
          })
          await bounded(scope, this.window(60000, 70000), (inner) => this.drain(inner), {
            label: 'The settlement',
          })
          await this.observe(scope)
          await this.finish(scope)
        },
        { budgetMs: CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.archive()
      this.row('case-failed', { error: describeError(error), ...this.progress() })
      throw error
    }
  }

  /**
   * Checks the cluster shape, creates the replication factor three topic, applies and reads back
   * the group config, measures the baseline, and starts a producer and the member, waiting until the
   * member and the admin API agree on its membership.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} when the cluster does not match the scenario or the member never joins
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    this.assertOwnership()
    await bounded(scope, 30000, () => this.bouncer.assertAllUp())
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
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
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: this.groupConfig, client: this.clientConfig })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({
      idempotent: true,
      maxInFlightRequests: 5,
      retry: RETRY,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
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
          ],
        })),
      ],
    })
    const valueOf = (type, name, config) =>
      resources
        .find(({ resourceType, resourceName }) => resourceType === type && resourceName === name)
        ?.configEntries.find(({ configName }) => configName === config)?.configValue
    scope.throwIfAborted()
    const { topics } = await this.admin.fetchTopicMetadata({ topics: [this.topic] })
    const partitions = topics[0].partitions
    this.topicId = topics[0].topicId
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
          this.openHandlers++
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
            at: performance.now(),
          })
          this.openHandlers--
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
      if (kind !== 'stop' && kind !== 'kill') return run(kind, service)
      this.freeze(kind, service)
      const frozen = this.snapshots[this.snapshots.length - 1]
      const result = await run(kind, service)
      frozen.downAt = performance.now()
      return result
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
    const successful = new Set(
      this.entries
        .filter((entry) => this.evidence.successful(entry))
        .map(({ identity }) => identity)
    )
    const unresolved = this.entries.filter(({ identity }) => !successful.has(identity))
    const held = new Set(unresolved.map(({ identity }) => identity))
    const obligations = unresolved.map((entry) =>
      Object.freeze({
        identity: entry.identity,
        entry: entry.id,
        cohort: entry.acquisition?.id,
        nodeId: entry.acquisition?.source?.nodeId,
        tickets: this.evidence.exchanges.flatMap(({ tickets }) =>
          tickets.filter(({ cohort }) => cohort === entry.acquisition?.id)
        ),
        originatingEpisode:
          this.snapshots.find(({ held: prior }) => prior.has(entry.identity))?.index ??
          this.snapshots.length,
      })
    )
    this.snapshots.push({
      index: this.snapshots.length,
      service,
      nodeId: NODES[service].nodeId,
      kind,
      at: performance.now(),
      held,
      obligations: Object.freeze(obligations),
      handlerEntries: this.entries.length,
      confirmedOffsets: this.confirmed.size,
      inFlight: this.tap.inFlight().map((frame) => ({ ...frame })),
    })
    if (held.size > MAX_HELD)
      throw new Error(`Frozen unresolved history has ${held.size} identities`)
    if (unresolved.some(({ identity }) => !corpus.has(identity))) {
      this.row('unresolved-before-producer-receipt', { identities: [...held] })
    }
  }

  /**
   * Appends the corpus in 300 bursts of 100 records, one burst every 400 ms, never waiting for a
   * send before the next burst is due. A late burst fails the run rather than change the duration.
   * @param {AbortSignal} scope the phase scope
   * @returns {Promise<void>}
   * @throws {Error} when a burst starts more than a second late
   */
  async produce(scope) {
    const start = performance.now()
    this.produceStart = start
    for (let burst = 0; burst < BURSTS; burst++) {
      const wait = start + burst * BURST_INTERVAL_MS - performance.now()
      if (wait > 0) await delay(scope, wait)
      scope.throwIfAborted()
      const late = Math.max(0, -wait)
      if (late > LATE_BURST_MS) {
        throw new Error(`Burst ${burst} started ${Math.round(late)} ms late`)
      }
      this.lateness = Math.max(this.lateness, late)
      this.sendBurst(burst)
    }
    await Promise.all(this.pendingSends)
    this.produceEnd = performance.now()
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
      timestamp: OLD_TIMESTAMP,
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
    const send = { burst, startedAt: performance.now(), settledAt: null, error: null }
    this.sends.push(send)
    this.pendingSends.push(
      this.producer.send({ topic: this.topic, acks: -1, messages }).then(
        (results) => {
          send.settledAt = performance.now()
          send.error = this.recordAppends(messages, results)
        },
        (error) => {
          send.settledAt = performance.now()
          send.error = describeError(error)
        }
      )
    )
  }

  /**
   * Turns the broker's acknowledgement into the expected identities: each partition's records take
   * consecutive offsets from the acknowledged base offset.
   * @param {object[]} messages the burst
   * @param {object[]} results the send's per-partition outcomes
   * @returns {object|null} what was not acknowledged, or null when every record was
   */
  recordAppends(messages, results) {
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
      this.expected.push(
        shareRecordIdentity({ topic: this.topic, partition, offset: String(offset), key, value })
      )
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
    await pollUntil(
      scope,
      60000,
      () => {
        this.settlement = settlementCandidates(
          this.entries,
          new Set(this.expected),
          this.snapshots,
          this.evidence
        )
        return (
          this.expected.length === RECORDS &&
          this.expected.every((identity) => this.seen.has(identity)) &&
          this.settlement.missing.length === 0 &&
          this.settlement.problems.length === 0
        )
      },
      { label: 'Every identity settling', delayMs: 500 }
    )
  }

  /**
   * Checks the six outages, the membership and replay allowances, and the three independent
   * oracles: raw log, handler entries and broker offsets.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    this.checkBounces()
    const log = await bounded(scope, 40000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
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
   * @returns {void}
   * @throws {Error} the first way the evidence falls short
   */
  checkBounces() {
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
    for (const fact of this.bounceFacts) {
      expect(fact.outageMs).toBeLessThan(MAX_OUTAGE_MS)
      expect(fact.frozenAt).toBeLessThanOrEqual(this.produceEnd)
      expect({ index: fact.index, joins: fact.joins }).toEqual({ index: fact.index, joins: 0 })
      expect({ index: fact.index, changes: fact.assignmentChanges }).toEqual({
        index: fact.index,
        changes: 0,
      })
    }
    expect(this.joins).toHaveLength(1)
    for (const { held } of this.snapshots) expect(held.size).toBeLessThanOrEqual(MAX_HELD)
    expect(this.captureProblems).toEqual([])
    this.replay = auditReplay(this.entries, this.snapshots, new Set(this.expected), this.evidence)
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
   * Closes the workload, actively verifies the same group, then rechecks terminality and ends.
   * @param {AbortSignal} scope Case scope.
   * @returns {Promise<void>} Only after unknown candidates pass every independent proof.
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    this.quiet = { events: [], delivered: [], startedAt: performance.now() }
    const kafka = this.client('verifier')
    const create = kafka.shareConsumer.bind(kafka)
    kafka.shareConsumer = (config) => {
      const consumer = create(config)
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
      const disconnect = consumer.disconnect.bind(consumer)
      consumer.disconnect = (...args) => {
        this.quiet.endedAt = performance.now()
        this.quiet.assignment = consumer.assignment()
        return disconnect(...args).finally(() => removers.forEach((remove) => remove()))
      }
      return consumer
    }
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
    this.settlement = settlementCandidates(
      this.entries,
      new Set(this.expected),
      this.snapshots,
      this.evidence
    )
    expect(this.settlement.missing).toEqual([])
    expect(this.settlement.problems).toEqual([])
    expect(
      auditTerminal({
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
      }).problems
    ).toEqual([])
    expect(this.settlement.successful.length + this.settlement.unknown.length).toBe(RECORDS)
    expect(this.settlement.successful.length).toBeGreaterThanOrEqual(RECORDS - MAX_REPLAY)
    expect(this.crashes.map((error) => error.message)).toEqual([])
    this.terminalVerified = true
    this.archive()
  }

  /** @returns {void} Archives original streams on success and failure, without success summaries. */
  archive() {
    this.row('acquisition-journal', {
      topic: this.topic,
      topicId: this.topicId,
      corpus: this.expected,
      entries: this.entries.map(({ acquisition, ...entry }) => ({
        ...entry,
        acquisition: acquisition?.id,
      })),
      acquisitions: this.evidence.acquisitions.map(({ source, ...acquisition }) => acquisition),
      exchanges: this.evidence.exchanges.map(({ frames, ...exchange }) => ({
        ...exchange,
        frames: frames.map(({ id }) => id),
      })),
      outcomes: this.evidence.outcomes,
      retirements: this.evidence.retirements,
      epochSources: this.evidence.epochs.map(({ frames, ...source }) => ({
        ...source,
        frames: frames.map(({ id }) => id),
      })),
      socketEvents: this.tap.socketEvents,
      wire: this.tap.allFrames.filter(({ apiKey }) => [3, 76, 78, 79].includes(apiKey)),
      frozen: this.snapshots.map(({ held, ...rest }) => ({ ...rest, held: [...held] })),
      deliveryLimits: this.deliveryLimits,
      position: this.beforeTerminal,
      verifier: this.verifier,
      quiet: this.quiet,
      postVerifierPosition: this.afterTerminal,
      raw: this.rawIdentities,
      settlement: this.settlement,
    })
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
        await pollUntil(
          signal,
          3000,
          () =>
            this.tap.socketCounts().created - this.tap.socketCounts().destroyed <=
            this.baseline.sockets.open,
          { delayMs: 50, label: 'The sockets closing' }
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
    if (resources[0].errorCode !== 0) {
      throw new Error(`Config reset refused: ${resources[0].errorCode}`)
    }
    const read = await bounded(signal, 8000, () =>
      this.admin.describeConfigs({
        resources: [{ type: ConfigResourceTypes.GROUP, name: this.group, configNames: names }],
      })
    )
    const left = read.resources[0].configEntries.filter(
      ({ configSource }) => configSource === ConfigSource.GROUP_CONFIG
    )
    if (left.length > 0) {
      throw new Error(`Group config overrides remain: ${left.map((e) => e.configName)}`)
    }
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
    expect(this.terminalVerified).toBe(true)
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    const final = await bounded(scope, 10000, () => this.probe.snapshot())
    this.row('cleanup', { snapshot: final })
    const { extras, perBounce } = this.replay
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
        inFlight: this.snapshots[index].inFlight,
        duplicates: extras.filter(({ bounce }) => bounce === index),
        extraEntries: perBounce[index],
      })),
      shareFrames: this.tap.frames.length,
      wireVersions: wireVersionsOf(this.requests),
      config: { client: this.clientConfig, group: this.groupConfig },
    })
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
  const evidence = new BounceEvidence({})
  evidence.exchanges.push(old.acquisition.source, next.acquisition.source)
  evidence.retirements.push({ at: 8, nodeId: 1, open: 0, reason: 'connection', cause: 99 })
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
  return {
    old,
    next,
    entries: [old, next],
    evidence,
    bounces: [bounce],
    corpus: [old.identity],
    target: { groupId, topic, partitions: [{ partition: 0, floor: '0', end: '1' }] },
  }
}

/**
 * Scripts one real ledger outcome for a written or unwritten terminal decision.
 * @param {object} fixture Independent replay fixture.
 * @param {{written?:boolean, applied?:boolean, type?:string, reason?:string}} [options] Fault script.
 * @returns {object} Complete independent terminal observations, with the actual outcome journal.
 */
const lostAcceptFixture = (
  fixture,
  { written = true, applied = true, type = 'accept', reason = 'session-lost' } = {}
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
  fixture.evidence.outcomes.push(
    ...results.map((outcome) => ({
      ...outcome,
      cohort: 0,
      at: 8,
      typed: outcome.error instanceof KafkaJSShareAcknowledgementError,
      cause: 99,
      error: describeError(outcome.error),
    }))
  )
  fixture.evidence.outcomesByCohort.set(0, [...fixture.evidence.outcomes])
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
    id: 100,
    apiKey: 79,
    nodeId: 1,
    open: 0,
    pool: 0,
    generation: 0,
    model: [],
    frames: written
      ? [
          {
            id: 100,
            apiKey: 79,
            apiVersion: 2,
            request,
            at: 6,
            lostAt: 8,
            socket: entry.acquisition.source.frames[0].socket,
            endpoint: entry.acquisition.source.endpoint,
          },
        ]
      : [],
    failedAt: 8,
    cause: 99,
    error: describeError(cause),
    tickets: [
      {
        cohort: 0,
        ticket: 100,
        ledger: 0,
        batches: ticket.acknowledgementBatches,
        isRenewAck: ticket.isRenewAck,
      },
    ],
  }
  fixture.evidence.exchanges.push(exchange)
  fixture.evidence.exchangesByCohort.set(0, [exchange])
  fixture.bounces[0].inFlight = exchange.frames.map((frame) => ({ ...frame }))
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
 * @param {number} count Distinct identities to exercise.
 * @param {number} perEpisode Frozen identities in each sequential episode.
 * @param {boolean} unknown Whether to script lost Accepts instead of higher-count replays.
 * @returns {object} Combined evidence without deriving expected multiplicities from dispatches.
 */
const budgetFixture = (count, perEpisode, unknown) => {
  const combined = { entries: [], corpus: [], bounces: [], evidence: new BounceEvidence({}) }
  for (let index = 0; index < count; index++) {
    const f = replayFixture({ count: 2, offset: String(index) })
    const proof = unknown ? lostAcceptFixture(f) : null
    const episode = Math.floor(index / perEpisode)
    const shift = episode * 100
    for (const entry of f.entries) {
      entry.id += 2 * index
      entry.offset = String(index)
      entry.at += shift
      entry.acquisition.id = entry.id
      entry.acquisition.responseIdentity += 1000 * index
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
      if (exchange.failedAt !== undefined) exchange.failedAt += shift
      for (const ticket of exchange.tickets) {
        ticket.cohort += 2 * index
      }
      for (const frame of exchange.frames) {
        frame.id += 1000 * index
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
    for (const retirement of f.evidence.retirements) retirement.at += shift
    if (!combined.bounces[episode])
      combined.bounces[episode] = { ...frozen, held: new Set(), obligations: [], inFlight: [] }
    const episodeProof = combined.bounces[episode]
    episodeProof.held.add(f.old.identity)
    episodeProof.obligations.push(...frozen.obligations)
    episodeProof.inFlight.push(...frozen.inFlight)
    combined.corpus.push(f.old.identity)
    combined.entries.push(...(proof ? proof.entries : f.entries))
    combined.evidence.exchanges.push(...f.evidence.exchanges)
    combined.evidence.outcomes.push(...f.evidence.outcomes)
    combined.evidence.retirements.push(...f.evidence.retirements)
    for (const outcome of f.evidence.outcomes)
      combined.evidence.outcomesByCohort.set(outcome.cohort, [outcome])
    for (const exchange of f.evidence.exchanges)
      for (const ticket of exchange.tickets)
        combined.evidence.exchangesByCohort.set(ticket.cohort, [exchange])
  }
  return combined
}

describe('share bounce acquisition and settlement sensitivity', () => {
  const audit = ({ entries, bounces, corpus, evidence }) =>
    auditReplay(entries, bounces, new Set(corpus), evidence)

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
  ])('rejects equal-count replay with %s', (_name, mutate) => {
    const fixture = replayFixture()
    mutate(fixture)
    expect(audit(fixture).problems.length).toBeGreaterThan(0)
  })

  it.each([0, -1, 1.5, NaN, 32768])('rejects invalid count %s', (count) => {
    expect(audit(replayFixture({ count })).problems.length).toBeGreaterThan(0)
  })

  it('rejects lower counts even across a proved epoch advance', () => {
    const fixture = replayFixture()
    fixture.old.deliveryCount = 2
    fixture.old.acquisition.acquiredRecords[0].deliveryCount = 2
    expect(audit(fixture).problems).toContain(
      `${fixture.old.identity} has no proved delivery-count progression`
    )
  })

  it('retains the original episode across later held sets and rejects a duplicate charge', () => {
    const fixture = replayFixture({ count: 2 })
    fixture.bounces.push({ ...fixture.bounces[0], index: 1, at: 19 })
    expect(audit(fixture).perBounce).toEqual([1, 0])
    fixture.entries.push(fixture.next)
    expect(audit(fixture).problems.length).toBeGreaterThan(0)
  })

  it.each([false, true])('keeps the 300/1800 boundaries for unknown settlement %s', (unknown) => {
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
    expect(episodeResult.problems.length).toBeGreaterThan(0)
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
    expect(totalResult.problems.length).toBeGreaterThan(0)
  })

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

  it('retains the write chronology when an Accept is written during the down command', () => {
    const proof = lostAcceptFixture(replayFixture())
    proof.bounces[0].inFlight = []
    proof.evidence.exchanges.at(-1).frames[0].at = 7.5
    expect(auditTerminal(proof).problems).toEqual([])
  })

  it('requires recovery when the lost Accept was not applied and retains its old unknown history', () => {
    const fixture = replayFixture({ count: 2 })
    const proof = lostAcceptFixture(fixture, { applied: false })
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
  ])('fails finalization with %s despite a forged passing summary', (_name, mutate) => {
    const proof = lostAcceptFixture(replayFixture())
    proof.oraclePassed = true
    proof.settled = true
    mutate(proof)
    expect(auditTerminal(proof).problems.length).toBeGreaterThan(0)
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
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  it(
    TITLE,
    liveCase(BUDGET_MS, (scope) => new ShareBounceCase().run(scope)),
    caseTimeout(BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
