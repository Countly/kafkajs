/**
 * Share-consumer throughput probe. It consumes a prepared, fixed corpus through the public client
 * factory and reports delivered records per second, ShareFetch frames per 1000 records,
 * ShareAcknowledge frames, acknowledgement latency and retained state. Its only protocol
 * observation is the header of each written request and each complete response, read through the
 * `socketFactory`. It never reaches into the share runner, session or ledger.
 *
 * Usage:
 *   node --expose-gc scripts/benchmarks/shareThroughput.js --messages 200000 --size 100
 *     --partitions 3 --acquire-mode record_limit|batch_optimized --max-records 500 --control 0|1
 *     [--arm share|ordinary]
 *   node scripts/benchmarks/shareThroughput.js --prepare|--cleanup <the same six flag/value pairs>
 *
 * Every flag takes an explicit value. `--control 1` runs the broker-free controls and prints them.
 * `--max-records 1` selects the separate 3000-record frame control, measured over 1000 records: a
 * positive movement control, never a baseline. `--arm ordinary` reads the same topic with a fresh
 * ordinary group as the same-tree reference.
 *
 * A measured run, `--prepare` and `--cleanup` need a reviewed live guard
 * (`KAFKAJS_CLUSTER_MANIFEST`) and `P9_SHARE_CORPUS`, the corpus manifest path (created by
 * `--prepare`, which requires it absent). A measured run also needs `P9_SHARE_TRACE`, a fresh path
 * for its exclusive, hashed trace. It prints one JSON record. Exit code 0 is a valid observation,
 * 2 an inconclusive one (no measurement window could be formed), 1 any failure.
 *
 * The measured window covers complete, fully acknowledged acquisitions only. The first full
 * acquisition of every node is discarded and each measured acquisition needs a full successor on
 * its node. The window starts at the first measured fetch write and ends at the last matching
 * acknowledgement event, whose settling fetch answer must have arrived no later. The tail and
 * close are outside it. Timings are only meaningful on the quiet benchmark CI lane.
 */
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { createHash, randomUUID } = require('node:crypto')
const { performance } = require('node:perf_hooks')
const { Kafka, logLevel } = require('../../index')
const sleep = require('../../src/utils/sleep')
const defaultSocketFactory = require('../../src/network/socketFactory')
const { plainTextBrokers } = require('../../testHelpers')
const {
  resetShareGroupConfigs,
  setShareGroupConfigs,
  shareRecordIdentity,
  waitForShareFixtureAbsence,
} = require('../../testHelpers/shareFixtures')

/** Fixed configuration, declared before any measurement. */
const LIMITS = Object.freeze({
  messages: 200000,
  controlMessages: 3000,
  size: 100,
  partitions: 3,
  maxRecords: 500,
  wait: 500,
  measured: 180000,
  controlMeasured: 1000,
  frameFloor: 900,
  timeout: 540000,
  cleanup: 30000,
  lock: 30000,
  heap: 1048576,
  aa: 0.03,
  floor: 0.97,
  observations: 8,
})

const SHARE_FETCH = 78
const SHARE_ACKNOWLEDGE = 79
const MAX_FRAME_BYTES = 1073741824
const ARMS = ['batch_optimized', 'ordinary', 'record_limit']
const FLAGS = ['messages', 'size', 'partitions', 'acquire-mode', 'max-records', 'control']
const TOPIC = /^share-bench-[a-f0-9-]+$/
const GROUP_CONFIG = Object.freeze({
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': LIMITS.lock,
})

/**
 * Parses explicit flag/value pairs. Unknown, duplicate, missing and noninteger values fail.
 * @param {string[]} argv CLI arguments without a leading `--prepare` or `--cleanup`.
 * @returns {{messages:number,size:number,partitions:number,acquireMode:string,maxRecords:number,control:number,arm:string}}
 *   Validated fixed configuration. Parsing never starts a client.
 * @throws {Error} For an invalid, repeated or missing argument or a value outside the fixed corpus.
 */
const parseArgs = (argv) => {
  const values = {}
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i].startsWith('--') ? argv[i].slice(2) : undefined
    assert(
      [...FLAGS, 'arm'].includes(name) && argv[i + 1] !== undefined && !Object.hasOwn(values, name),
      `Invalid argument: ${argv[i]}`
    )
    values[name] = argv[i + 1]
  }
  for (const name of FLAGS) assert(Object.hasOwn(values, name), `Missing --${name}`)
  const integer = (name) => {
    assert(/^(0|[1-9][0-9]*)$/.test(values[name]), `--${name} must be an integer`)
    return Number(values[name])
  }
  const config = {
    messages: integer('messages'),
    size: integer('size'),
    partitions: integer('partitions'),
    acquireMode: values['acquire-mode'],
    maxRecords: integer('max-records'),
    control: integer('control'),
    arm: values.arm ?? 'share',
  }
  assert([1, LIMITS.maxRecords].includes(config.maxRecords), 'Invalid record limit')
  assert(
    config.messages === (config.maxRecords === 1 ? LIMITS.controlMessages : LIMITS.messages) &&
      config.size === LIMITS.size &&
      config.partitions === LIMITS.partitions,
    'The corpus configuration is fixed'
  )
  assert(['record_limit', 'batch_optimized'].includes(config.acquireMode), 'Invalid acquire mode')
  assert(['share', 'ordinary'].includes(config.arm), 'Invalid arm')
  assert([0, 1].includes(config.control), 'Invalid control')
  assert(
    config.maxRecords !== 1 || (config.acquireMode === 'record_limit' && config.arm === 'share'),
    'The frame control is a record_limit share run'
  )
  return config
}

/**
 * Splits a leading fixture action from the configuration pairs.
 * @param {string[]} argv Full CLI arguments.
 * @returns {{phase:'run'|'prepare'|'cleanup',config:object}} The action and its configuration.
 * @throws {Error} From `parseArgs`.
 */
const parseCommand = (argv) => {
  const fixture = ['--prepare', '--cleanup'].includes(argv[0])
  return {
    phase: fixture ? argv[0].slice(2) : 'run',
    config: parseArgs(fixture ? argv.slice(1) : argv),
  }
}

/**
 * Creates a stable full identity from topic, partition, decimal offset, key and value bytes.
 * Delivery count is an observation, never part of the identity.
 * @param {{topic:string,partition:number,message:{offset:string,key:Buffer|null,value:Buffer|null}}} row A delivered or produced record.
 * @returns {string} Collision-safe identity.
 * @throws {Error} For a malformed topic, partition or offset.
 */
const identity = ({ topic, partition, message }) =>
  shareRecordIdentity({
    topic,
    partition,
    offset: message.offset,
    key: message.key,
    value: message.value,
  })

/**
 * Rebuilds the flat per-record stream from retained handler calls.
 * @param {{callback:number,topic:string,partition:number,messages:object[]}[]} calls Handler calls.
 * @returns {{topic:string,partition:number,message:object,callback:number}[]} One row per record.
 */
const observedStream = (calls) =>
  calls.flatMap(({ callback, topic, partition, messages }) =>
    messages.map((message) => ({ topic, partition, message, callback }))
  )

/**
 * Fails unless two identity lists hold the same members. The two lists must already be free of
 * repeats in the expected side, so equal lengths and equal members mean a one-to-one match.
 * @param {string[]} actual Identities under test.
 * @param {string[]} expected Producer-derived identities.
 * @param {string} label Failure prefix.
 * @returns {void}
 * @throws {Error} With counts only, never a diff of the corpus.
 */
const assertSameIdentities = (actual, expected, label) => {
  const wanted = new Set(expected)
  const seen = new Set(actual)
  const missing = expected.filter((id) => !seen.has(id)).length
  const extra = actual.filter((id) => !wanted.has(id)).length
  assert(
    actual.length === expected.length && missing === 0 && extra === 0,
    `${label}: ${missing} missing, ${extra} unexpected, ${actual.length} of ${expected.length}`
  )
}

/**
 * Validates delivery against producer-derived identities. Every count comes from its own traversal,
 * and no claimed figure is its own oracle.
 * @param {object} facts Independent corpus, retained calls and observation stream.
 * @param {string[]} facts.expected Producer-acknowledged identities.
 * @param {{topic:string,partition:number,messages:object[]}[]} facts.callbacks Handler calls.
 * @param {{topic:string,partition:number,message:object,callback:number}[]} facts.observed Per-record stream.
 * @param {number} facts.claimedMessages Message sum the handler reported.
 * @param {number} facts.claimedBatches Handler entry count the handler reported.
 * @returns {{batches:number,messages:number}} Checked counts.
 * @throws {Error} For a missing, extra, duplicate, fabricated or miscounted delivery.
 */
const verifyDelivery = ({ expected, callbacks, observed, claimedMessages, claimedBatches }) => {
  assert(
    expected.length > 0 && new Set(expected).size === expected.length,
    'Invalid expected corpus'
  )
  const ids = callbacks.flatMap(({ topic, partition, messages }) =>
    messages.map((message) => identity({ topic, partition, message }))
  )
  const observedIds = observed.map(identity)
  const messageSum = callbacks.reduce((sum, call) => sum + call.messages.length, 0)
  assert.equal(claimedBatches, callbacks.length, 'Callback count differs')
  assert.equal(
    new Set(observed.map((row) => row.callback)).size,
    callbacks.length,
    'Observed batch count differs'
  )
  assert.equal(claimedMessages, messageSum, 'Fabricated message sum')
  assert.equal(observedIds.length, messageSum, 'Independent message sum differs')
  assert.equal(new Set(ids).size, ids.length, 'Duplicate callback identity')
  assertSameIdentities(ids, expected, 'Callback corpus differs')
  assertSameIdentities(observedIds, expected, 'Observed corpus differs')
  return { batches: callbacks.length, messages: messageSum }
}

/**
 * Nearest-rank quantile, with no averaging and no tolerance for missing samples.
 * @param {number[]} values Finite samples, at least one.
 * @param {number} fraction Quantile in (0, 1].
 * @returns {number} The observed quantile.
 * @throws {Error} For no samples or a non-finite sample.
 */
const quantile = (values, fraction) => {
  assert(values.length > 0 && values.every(Number.isFinite), 'Missing finite samples')
  return [...values].sort((a, b) => a - b)[Math.ceil(values.length * fraction) - 1]
}

/**
 * Reads one resource counter, refusing absent, negative and fractional readings.
 * @param {object} snapshot A resource snapshot.
 * @param {string} key Counter name.
 * @returns {number} The counter.
 * @throws {Error} When the source did not report a nonnegative integer.
 */
const reading = (snapshot, key) => {
  const value = snapshot?.[key]
  assert(Number.isSafeInteger(value) && value >= 0, `Missing resource measurement: ${key}`)
  return value
}

/**
 * Checks the actual delivered-entry and compact-range counters while the client is still connected.
 * @param {object} snapshot Reading taken after the last acknowledgement.
 * @param {{share:boolean}} options Whether the arm owns share state; ordinary arms own none.
 * @returns {void}
 * @throws {Error} When a counter is missing or an entry or range is retained at idle.
 */
const assertIdle = (snapshot, { share }) => {
  if (!share) return
  assert.equal(reading(snapshot, 'shareRecords'), 0, 'Ledger is not empty at idle')
  assert.equal(reading(snapshot, 'shareRanges'), 0, 'Compact ranges remain at idle')
}

/**
 * Checks that the client released everything after disconnect and bounds its retained heap.
 * @param {{heapUsed:number}} before Reading before the client existed.
 * @param {object} after Reading after disconnect, with observer storage already released.
 * @param {{share:boolean}} options Whether the arm owns share state.
 * @returns {number} Retained heap in bytes, which may be negative.
 * @throws {Error} For a missing counter, a retained session, entry, range or request, or a heap
 *   increase above the ceiling.
 */
const assertReleased = (before, after, { share }) => {
  if (share) {
    assert.equal(reading(after, 'shareSessions'), 0, 'Retained share session')
    assert.equal(reading(after, 'shareRecords'), 0, 'Retained delivered entry')
    assert.equal(reading(after, 'shareRanges'), 0, 'Retained compact range')
  }
  assert.equal(reading(after, 'queueDepth'), 0, 'Retained request')
  assert(
    Number.isFinite(before?.heapUsed) && Number.isFinite(after?.heapUsed),
    'Missing heap measurement'
  )
  const heapDelta = after.heapUsed - before.heapUsed
  assert(heapDelta <= LIMITS.heap, 'Retained heap ceiling')
  return heapDelta
}

/**
 * Parses Kafka frame headers across arbitrary chunks without retaining payload bytes.
 */
class HeaderStream {
  /**
   * @param {number} headerSize Header bytes including the length prefix: 12 for a request, 8 for a response.
   * @param {(header:Buffer, startedAt:number) => void} begin Receives the header and the first-byte time once it is complete.
   * @param {(header:Buffer, at:number) => void} [end] Receives the same header after the whole frame passed.
   */
  constructor(headerSize, begin, end = () => {}) {
    this.headerSize = headerSize
    this.begin = begin
    this.end = end
    this.prefix = Buffer.alloc(headerSize)
    this.used = 0
    this.left = null
    this.startedAt = null
  }

  /**
   * Observes bytes in order; payload bytes are skipped, never copied.
   * @param {Buffer} chunk Written or received bytes.
   * @param {number} now Monotonic milliseconds for this chunk.
   * @returns {void}
   * @throws {Error} For an impossible frame size, or what `begin` throws; the stream is then unusable.
   */
  push(chunk, now) {
    let at = 0
    while (at < chunk.length) {
      if (this.left === null) {
        if (this.used === 0) this.startedAt = now
        const take = Math.min(this.headerSize - this.used, chunk.length - at)
        chunk.copy(this.prefix, this.used, at, at + take)
        this.used += take
        at += take
        if (this.used < this.headerSize) continue
        const size = this.prefix.readInt32BE(0)
        assert(size >= this.headerSize - 4 && size <= MAX_FRAME_BYTES, 'Invalid Kafka frame size')
        this.left = size + 4 - this.headerSize
        this.begin(this.prefix, this.startedAt)
      }
      const take = Math.min(this.left, chunk.length - at)
      this.left -= take
      at += take
      if (this.left === 0) {
        this.end(this.prefix, now)
        this.used = 0
        this.left = null
      }
    }
  }
}

/**
 * One written request, matched to its answer by socket and correlation id.
 * @typedef {object} FrameRow
 * @property {number} socketId Observer-assigned socket number.
 * @property {string} endpoint Advertised `host:port` the socket dialed.
 * @property {number} correlationId Request correlation id.
 * @property {number} apiKey Kafka API key.
 * @property {number} apiVersion Kafka API version.
 * @property {number} writeAt Monotonic ms of the first header byte written.
 * @property {number|null} responseAt Monotonic ms the answer completed, or null while unanswered.
 */

/**
 * Normalizes a socket write argument to bytes.
 * @param {Buffer|Uint8Array|string} chunk Written data.
 * @param {*} encoding The encoding argument of `write`, ignored unless a string.
 * @returns {Buffer} The bytes, shared when already a Buffer.
 */
const asBuffer = (chunk, encoding) =>
  Buffer.isBuffer(chunk)
    ? chunk
    : Buffer.from(chunk, typeof encoding === 'string' ? encoding : undefined)

/**
 * Public `socketFactory` observer. Every request belongs to one socket and one correlation id, and
 * a response is matched on both, never on the correlation id alone.
 */
class Frames {
  /**
   * @param {Function} factory The socket factory to wrap.
   * @param {() => number} [now] Monotonic millisecond clock.
   */
  constructor(factory, now = () => performance.now()) {
    this.factory = factory
    this.now = now
    /** @type {FrameRow[]} */
    this.rows = []
    this.failure = null
    this.nextSocket = 0
    this.pending = new Map()
  }

  /**
   * Builds a socket and installs the header observers before Kafka sees any data. The socket keeps
   * its write return value and callbacks. An observer fault is recorded, never thrown into Kafka.
   * @param {{host:string,port:number}} args The unmodified socket factory arguments.
   * @returns {object} The original socket.
   */
  socket(args) {
    const socket = this.factory(args)
    const socketId = ++this.nextSocket
    const endpoint = `${args.host}:${args.port}`
    const guard = (work) => {
      try {
        work()
      } catch (error) {
        this.failure ||= error
      }
    }
    const outgoing = new HeaderStream(12, (header, at) => {
      const key = `${socketId}:${header.readInt32BE(8)}`
      assert(!this.pending.has(key), 'Correlation reused before answer')
      const row = {
        socketId,
        endpoint,
        correlationId: header.readInt32BE(8),
        apiKey: header.readInt16BE(4),
        apiVersion: header.readInt16BE(6),
        writeAt: at,
        responseAt: null,
      }
      this.rows.push(row)
      this.pending.set(key, row)
    })
    const incoming = new HeaderStream(
      8,
      () => {},
      (header, at) => {
        const key = `${socketId}:${header.readInt32BE(4)}`
        const row = this.pending.get(key)
        assert(row, 'Response without written request')
        row.responseAt = at
        this.pending.delete(key)
      }
    )
    const write = socket.write
    socket.write = (chunk, ...rest) => {
      guard(() => outgoing.push(asBuffer(chunk, rest[0]), this.now()))
      return write.call(socket, chunk, ...rest)
    }
    socket.prependListener('data', (chunk) => guard(() => incoming.push(chunk, this.now())))
    socket.once('close', () => {
      for (const [key, row] of this.pending) if (row.socketId === socketId) this.pending.delete(key)
    })
    return socket
  }

  /**
   * Finds the answered ShareFetch that owns a handler call. With no prefetch and one request per
   * node, the latest completely answered ShareFetch on the leader's endpoint is the owner.
   * @param {string} endpoint Metadata-derived advertised address of the partition leader.
   * @param {number} at Handler-entry time.
   * @returns {FrameRow} The original request observation.
   * @throws {Error} For an earlier observer fault or no answered ShareFetch on that endpoint.
   */
  acquisition(endpoint, at) {
    if (this.failure) throw this.failure
    for (let i = this.rows.length - 1; i >= 0; i--) {
      const row = this.rows[i]
      if (
        row.endpoint === endpoint &&
        row.apiKey === SHARE_FETCH &&
        row.responseAt !== null &&
        row.responseAt <= at
      )
        return row
    }
    throw new Error('No answered acquisition for handler')
  }
}

/**
 * One handler call with its retained records.
 * @typedef {object} AcquisitionCall
 * @property {number} callback Entry number.
 * @property {string} topic Topic.
 * @property {number} partition Partition.
 * @property {object[]} messages The batch's own message objects.
 * @property {number} enteredAt Monotonic handler-entry time.
 * @property {number|null} returnedAt Monotonic handler-return time.
 * @property {number} lock Acquisition lock the broker applied, in ms.
 * @property {AcquisitionGroup} group The fetch that delivered it.
 */

/**
 * Every call and record of one original answered fetch.
 * @typedef {object} AcquisitionGroup
 * @property {FrameRow} frame The ShareFetch that delivered them.
 * @property {AcquisitionCall[]} calls Handler calls.
 * @property {{call:AcquisitionCall,message:object,ackAt:number|null}[]} entries One per delivered record.
 * @property {number} settled Records with a terminal accepted result.
 * @property {number|null} settledAt Time of the latest accepted result.
 */

/** Stores original handler calls and the terminal acknowledgement time of every delivered offset. */
class AcquisitionLog {
  /**
   * @param {Frames} frames Written-header observations.
   * @param {Map<number,string>} leaders Partition to leader endpoint, from public Admin metadata.
   */
  constructor(frames, leaders) {
    this.frames = frames
    this.leaders = leaders
    /** @type {AcquisitionCall[]} */
    this.calls = []
    /** @type {Map<FrameRow,AcquisitionGroup>} */
    this.groups = new Map()
    this.byPartition = new Map()
    this.events = []
    this.delivered = 0
    this.acknowledged = 0
    this.failure = null
  }

  /**
   * Captures one public batch before the handler returns. Identities are derived later, so the
   * handler path only keeps references and one offset entry per record.
   * @param {{topic:string,partition:number,messages:object[],acquisitionLockTimeoutMs:number}} batch The actual public batch.
   * @param {number} enteredAt Monotonic handler-entry time.
   * @returns {AcquisitionCall} The call; complete it with `returned`.
   * @throws {Error} For an unowned acquisition, a batch spanning partitions or a repeated offset.
   */
  enter(batch, enteredAt) {
    const frame = this.frames.acquisition(this.leaders.get(batch.partition), enteredAt)
    let group = this.groups.get(frame)
    if (!group) {
      group = { frame, calls: [], entries: [], settled: 0, settledAt: null }
      this.groups.set(frame, group)
    }
    assert(
      group.calls.every((call) => call.partition === batch.partition),
      'Acquisition spans partitions'
    )
    const call = {
      callback: this.calls.length,
      topic: batch.topic,
      partition: batch.partition,
      messages: batch.messages,
      enteredAt,
      returnedAt: null,
      lock: batch.acquisitionLockTimeoutMs,
      group,
    }
    const key = JSON.stringify([batch.topic, batch.partition])
    if (!this.byPartition.has(key)) this.byPartition.set(key, new Map())
    const offsets = this.byPartition.get(key)
    for (const message of batch.messages) {
      assert(!offsets.has(message.offset), 'Duplicate delivered offset')
      const entry = { call, message, ackAt: null }
      offsets.set(message.offset, entry)
      group.entries.push(entry)
    }
    this.calls.push(call)
    group.calls.push(call)
    this.delivered += batch.messages.length
    return call
  }

  /**
   * Records handler completion, independent of acknowledgement receipt.
   * @param {AcquisitionCall} call The original handler call.
   * @param {number} at Monotonic handler-return time.
   * @returns {void}
   */
  returned(call, at) {
    call.returnedAt = at
  }

  /**
   * Matches one compact public result to the delivered offsets of its partition. The range is
   * bounded by the delivered population first, so an absent wide range is never expanded, and
   * offsets stay decimal strings and BigInt, never Number.
   * @param {{topic:string,partition:number,firstOffset:string,lastOffset:string,type:string,error:*}} event Public acknowledge payload.
   * @param {number} at Monotonic event receipt time.
   * @returns {void}
   * @throws {Error} For an error, a non-accept result, an unobserved range, or a repeated or premature result.
   */
  acknowledge(event, at) {
    assert(event.error === null && event.type === 'accept', 'Unexpected acknowledgement result')
    const first = BigInt(event.firstOffset)
    const last = BigInt(event.lastOffset)
    assert(first <= last, 'Reversed acknowledgement result')
    const offsets = this.byPartition.get(JSON.stringify([event.topic, event.partition]))
    assert(offsets && last - first + 1n <= BigInt(offsets.size), 'Unobserved acknowledgement range')
    for (let offset = first; offset <= last; offset++) {
      const entry = offsets.get(String(offset))
      assert(
        entry && entry.ackAt === null && entry.call.returnedAt !== null,
        'Repeated or premature acknowledgement'
      )
      entry.ackAt = at
      entry.call.group.settled += 1
      entry.call.group.settledAt = at
      this.acknowledged += 1
    }
    this.events.push({
      topic: event.topic,
      partition: event.partition,
      firstOffset: event.firstOffset,
      lastOffset: event.lastOffset,
      type: event.type,
      at,
    })
  }

  /**
   * Releases every retained call, entry and event so retained-heap reads see product state only.
   * @returns {void}
   */
  clear() {
    this.calls = []
    this.groups.clear()
    this.byPartition.clear()
    this.events = []
  }
}

/**
 * Forms the complete-acquisition interval and gates it. Each node needs an untimed first full
 * acquisition, and every measured acquisition needs a full successor on its node.
 * @param {object} input Observations and the fixed target.
 * @param {{groups:Map<*,AcquisitionGroup>,failure:Error|null}} input.log The acquisition log.
 * @param {{rows:FrameRow[],failure:Error|null}} input.frames The written-frame observer.
 * @param {number} input.maxRecords Records per full acquisition.
 * @param {number} [input.target] Records to measure across the three nodes.
 * @param {boolean} [input.positive] Frame movement control: demands at least 900 fetches per 1000
 *   records instead of the upper ceiling.
 * @returns {object} Valid metrics with the measured identities, or `{status:'inconclusive',reason}`.
 * @throws {Error} For an earlier observer fault or a violated correctness, frame, latency or lock gate.
 */
const measureWindow = ({ log, frames, maxRecords, target = LIMITS.measured, positive = false }) => {
  if (frames.failure) throw frames.failure
  if (log.failure) throw log.failure
  const byNode = new Map()
  for (const group of log.groups.values()) {
    const node = group.frame.endpoint
    if (!byNode.has(node)) byNode.set(node, [])
    byNode.get(node).push(group)
  }
  const full = (group) => group.entries.length === maxRecords && group.settled === maxRecords
  const chosen = []
  for (const groups of byNode.values()) {
    groups.sort((a, b) => a.frame.writeAt - b.frame.writeAt)
    if (groups.length < 3)
      return { status: 'inconclusive', reason: 'Too few acquisitions on a node' }
    if (!full(groups[0])) return { status: 'inconclusive', reason: 'No initial full acquisition' }
    for (let i = 1; i + 1 < groups.length && full(groups[i]) && full(groups[i + 1]); i++)
      chosen.push(groups[i])
  }
  chosen.sort((a, b) => a.frame.writeAt - b.frame.writeAt)
  const wanted = target / maxRecords
  const measured = chosen.slice(0, wanted)
  if (byNode.size !== LIMITS.partitions || !Number.isInteger(wanted) || measured.length !== wanted)
    return { status: 'inconclusive', reason: 'Cannot form declared full-acquisition window' }
  const start = Math.min(...measured.map((group) => group.frame.writeAt))
  const end = Math.max(...measured.map((group) => group.settledAt))
  assert(end > start, 'Invalid measurement interval')
  const identities = measured.flatMap((group) =>
    group.entries.map(({ call, message }) =>
      identity({ topic: call.topic, partition: call.partition, message })
    )
  )
  assert.equal(new Set(identities).size, target, 'Duplicate measured identity')
  const latencies = []
  for (const group of measured)
    for (const { call, ackAt } of group.entries) {
      assert(
        Number.isFinite(ackAt) && Number.isFinite(call.returnedAt),
        'Unsettled measured record'
      )
      assert(ackAt >= call.returnedAt, 'Acknowledgement precedes return')
      assert(call.returnedAt - group.frame.writeAt < call.lock / 2, 'Handler exceeded half lock')
      latencies.push(ackAt - call.returnedAt)
    }
  const inside = frames.rows.filter((row) => row.writeAt >= start && row.writeAt <= end)
  const fetch = inside.filter((row) => row.apiKey === SHARE_FETCH).length
  const ack = inside.filter((row) => row.apiKey === SHARE_ACKNOWLEDGE).length
  const fetchPer1000 = (fetch * 1000) / target
  assert(fetch > 0, 'No ShareFetch observed')
  if (positive) assert(fetchPer1000 >= LIMITS.frameFloor, 'Frame positive control did not move')
  else assert(fetchPer1000 <= Math.ceil(1000 / maxRecords) + 2, 'ShareFetch frame ceiling')
  assert.equal(ack, 0, 'Steady ShareAcknowledge frame')
  const p99 = quantile(latencies, 0.99)
  assert(p99 <= LIMITS.wait + 100, 'Acknowledgement latency ceiling')
  const settling = measured.every((group) =>
    frames.rows.some(
      (row) =>
        row.endpoint === group.frame.endpoint &&
        row.apiKey === SHARE_FETCH &&
        row.writeAt > group.frame.writeAt &&
        row.responseAt !== null &&
        row.responseAt <= group.settledAt
    )
  )
  if (!settling) return { status: 'inconclusive', reason: 'No piggyback-settling fetch answer' }
  return {
    status: 'valid',
    start,
    end,
    records: target,
    callbacks: measured.reduce((sum, group) => sum + group.calls.length, 0),
    independentMessageSum: target,
    identities,
    fetch,
    ack,
    fetchPer1000,
    p99,
    latencySamples: latencies.length,
    recordsPerSecond: (target * 1000) / (end - start),
  }
}

/**
 * Evaluates one complete session: eight observations per arm for each of two identical-tree
 * repetitions. The first observation of a repetition is discarded and the other seven give medians.
 * The session never writes a baseline.
 * @param {{key?:string,arms:Record<string,{a:object[],aprime:object[]}>}} session Observations per arm.
 * @param {{key:string,arms:Record<string,{recordsPerSecond:number}>}|null} [baseline] The accepted
 *   baseline of the same comparability key, or null for a first session.
 * @returns {{status:'accepted',arms:object}|{status:'void',arm:string,metric:string,delta:number}}
 *   Accepted medians, or a void session when identical trees differ by more than three percent.
 * @throws {Error} For a missing or unaccepted observation, a heap ceiling breach, a missing arm or
 *   baseline arm, a different comparability key or a throughput regression.
 */
const evaluateSession = (session, baseline = null) => {
  assert.deepEqual(Object.keys(session.arms).sort(), ARMS, 'Expected every arm')
  if (baseline)
    assert(
      typeof baseline.key === 'string' && baseline.key === session.key,
      'Baseline comparability key differs'
    )
  const results = {}
  for (const [arm, repeats] of Object.entries(session.arms)) {
    const medians = [repeats.a, repeats.aprime].map((observations) => {
      assert.equal(
        observations.length,
        LIMITS.observations,
        'Expected one discard and seven observations'
      )
      assert(
        observations.every((row) => row.status === 'valid' && row.controls === true),
        'Unaccepted observation'
      )
      assert(
        observations.every((row) => row.heapDelta <= LIMITS.heap),
        'Retained heap ceiling'
      )
      const series = observations.slice(1)
      return Object.fromEntries(
        ['recordsPerSecond', 'p99', 'fetchPer1000', 'heapDelta'].map((metric) => [
          metric,
          quantile(
            series.map((row) => row[metric]),
            0.5
          ),
        ])
      )
    })
    for (const metric of ['recordsPerSecond', 'p99', 'fetchPer1000']) {
      const a = medians[0][metric]
      const b = medians[1][metric]
      const delta = a === b ? 0 : a === 0 ? Infinity : Math.abs(a - b) / Math.abs(a)
      if (delta > LIMITS.aa) return { status: 'void', arm, metric, delta }
    }
    if (baseline) {
      const reference = baseline.arms[arm]?.recordsPerSecond
      assert(Number.isFinite(reference), `Missing baseline: ${arm}`)
      assert(
        medians[0].recordsPerSecond >= reference * LIMITS.floor,
        `Throughput regression: ${arm}`
      )
    }
    results[arm] = medians[0]
  }
  return { status: 'accepted', arms: results }
}

/**
 * Runs the broker-free arithmetic, corruption, A/A and missing-window controls. Each intended
 * corruption must fail with its own diagnostic, not merely fail.
 * @returns {{status:string,arithmetic:{batches:number,messages:number},rejected:string[],aaVoid:boolean,missingWindow:string}}
 *   Named results, with no measured timing and no baseline.
 * @throws {Error} If a clean fixture fails or an intended corruption is accepted or fails otherwise.
 */
const controls = () => {
  const messages = Array.from({ length: 1000 }, (_, i) => ({
    offset: String(i),
    key: Buffer.from(String(i)),
    value: Buffer.from('x'),
  }))
  const call = (slice) => ({ topic: 'control', partition: 0, messages: slice })
  const callbacks = [call(messages.slice(0, 500)), call(messages.slice(500))]
  const observed = observedStream(callbacks.map((entry, callback) => ({ ...entry, callback })))
  const expected = messages.map((message) => identity({ topic: 'control', partition: 0, message }))
  const clean = { expected, callbacks, observed, claimedMessages: 1000, claimedBatches: 2 }
  assert.deepEqual(verifyDelivery(clean), { batches: 2, messages: 1000 })
  const mutants = {
    drop: [
      { ...clean, callbacks: callbacks.slice(0, 1), claimedBatches: 1, claimedMessages: 500 },
      /Observed batch count differs/,
    ],
    omit: [{ ...clean, observed: observed.slice(1) }, /Independent message sum differs/],
    duplicate: [
      {
        ...clean,
        callbacks: [...callbacks, callbacks[0]],
        observed: [...observed, ...observed.slice(0, 500).map((row) => ({ ...row, callback: 2 }))],
        claimedBatches: 3,
        claimedMessages: 1500,
      },
      /Duplicate callback identity/,
    ],
    fabricatedSum: [{ ...clean, claimedMessages: 999 }, /Fabricated message sum/],
  }
  for (const [facts, diagnostic] of Object.values(mutants))
    assert.throws(() => verifyDelivery(facts), diagnostic)
  const sample = {
    status: 'valid',
    controls: true,
    recordsPerSecond: 100,
    p99: 10,
    fetchPer1000: 2,
    heapDelta: 0,
  }
  const arm = () => ({
    a: Array.from({ length: LIMITS.observations }, () => ({ ...sample })),
    aprime: Array.from({ length: LIMITS.observations }, () => ({ ...sample })),
  })
  const session = { arms: { record_limit: arm(), batch_optimized: arm(), ordinary: arm() } }
  assert.equal(evaluateSession(session).status, 'accepted')
  for (const row of session.arms.record_limit.aprime) row.recordsPerSecond = 104
  assert.equal(evaluateSession(session).status, 'void')
  const empty = measureWindow({
    frames: { rows: [], failure: null },
    log: { groups: new Map(), failure: null },
    maxRecords: LIMITS.maxRecords,
  })
  assert.equal(empty.status, 'inconclusive')
  return {
    status: 'controls-passed',
    arithmetic: { batches: 2, messages: 1000 },
    rejected: Object.keys(mutants),
    aaVoid: true,
    missingWindow: empty.status,
  }
}

/**
 * Bounds lazy work by an original monotonic deadline. A noncancellable client operation is not
 * cancelled: the guard contains a run that expires here.
 * @param {() => *} work Lazy operation, never a pre-created promise.
 * @param {number} due Original `performance.now()` deadline.
 * @returns {Promise<*>} The operation result.
 * @throws {Error} For an expired deadline or the operation's own failure.
 */
const bounded = async (work, due) => {
  const left = due - performance.now()
  assert(left > 0, 'Benchmark deadline exceeded')
  let timer
  try {
    return await Promise.race([
      Promise.resolve().then(work),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Benchmark deadline exceeded; run tainted')),
          left
        )
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Polls a lazy predicate until it holds, under one original deadline.
 * @param {() => boolean|Promise<boolean>} predicate Lazy observation.
 * @param {number} due Original `performance.now()` deadline.
 * @param {number} [interval] Milliseconds between polls.
 * @returns {Promise<void>} Resolves when the predicate holds.
 * @throws {Error} For the predicate's failure or an expired deadline.
 */
const until = async (predicate, due, interval = 100) => {
  while (!(await bounded(predicate, due))) await sleep(interval)
}

/**
 * Appends JSON lines to an exclusive trace file and hashes exactly what it writes, so a large trace
 * is never held in memory.
 */
class TraceWriter {
  /**
   * @param {string} file Fresh absolute path.
   * @throws {Error} When the file exists or cannot be created.
   */
  constructor(file) {
    this.fd = fs.openSync(file, 'wx', 0o600)
    this.hash = createHash('sha256')
  }

  /**
   * @param {object} row One JSON-serializable fact.
   * @returns {void}
   * @throws {Error} On a filesystem failure.
   */
  write(row) {
    const line = `${JSON.stringify(row)}\n`
    this.hash.update(line)
    fs.writeSync(this.fd, line)
  }

  /**
   * Makes the trace durable and closes it.
   * @returns {string} SHA-256 of the whole file, in hex.
   * @throws {Error} On a filesystem failure.
   */
  close() {
    fs.fsyncSync(this.fd)
    fs.closeSync(this.fd)
    return this.hash.digest('hex')
  }
}

/**
 * Reads the partition leaders through public Admin metadata.
 * @param {object} admin A connected public admin client.
 * @param {string} topic Corpus topic.
 * @param {number} due Original deadline.
 * @returns {Promise<Map<number,string>>} Partition to advertised leader `host:port`.
 * @throws {Error} Unless the three partitions have three distinct leaders with known endpoints.
 */
const readLeaders = async (admin, topic, due) => {
  const metadata = await bounded(() => admin.fetchTopicMetadata({ topics: [topic] }), due)
  const cluster = await bounded(() => admin.describeCluster(), due)
  const nodes = new Map(cluster.brokers.map((b) => [b.nodeId, `${b.host}:${b.port}`]))
  const partitions = metadata.topics.find((t) => t.name === topic)?.partitions ?? []
  assert(
    partitions.length === LIMITS.partitions &&
      new Set(partitions.map((p) => p.leader)).size === LIMITS.partitions,
    'The fixture requires one leader partition per node'
  )
  return new Map(
    partitions.map((p) => {
      const endpoint = nodes.get(p.leader)
      assert(typeof endpoint === 'string', 'Missing advertised endpoint')
      return [p.partitionId, endpoint]
    })
  )
}

/**
 * Produces the fixed corpus once, in explicit-partition batches of the acquisition size, so every
 * record batch holds exactly one full acquisition. Values are exactly `LIMITS.size` bytes.
 * @param {object} producer A connected idempotent public producer.
 * @param {string} topic Fresh owned topic.
 * @param {{messages:number}} config Validated configuration.
 * @returns {Promise<string[]>} Producer-acknowledged full identities, derived from base offsets.
 * @throws {Error} When an acknowledgement lacks a base offset, or what `send` throws.
 */
const produceCorpus = async (producer, topic, { messages: total }) => {
  const expected = []
  for (let partition = 0; partition < LIMITS.partitions; partition++) {
    const messages = []
    for (let i = partition; i < total; i += LIMITS.partitions)
      messages.push({
        partition,
        key: Buffer.from(`k${String(i).padStart(6, '0')}`),
        value: Buffer.from(String(i).padStart(LIMITS.size, '0')),
      })
    for (let start = 0; start < messages.length; start += LIMITS.maxRecords) {
      const chunk = messages.slice(start, start + LIMITS.maxRecords)
      const answer = await producer.send({ topic, acks: -1, messages: chunk })
      const meta = answer.find((r) => r.partition === partition)
      assert(meta && typeof meta.baseOffset === 'string', 'Missing producer offset')
      chunk.forEach((message, i) =>
        expected.push(
          identity({
            topic,
            partition,
            message: { ...message, offset: String(BigInt(meta.baseOffset) + BigInt(i)) },
          })
        )
      )
    }
  }
  return expected
}

/**
 * Measures records delivered once. Share arms and the ordinary reference read the same prepared
 * topic through the public factory with a fresh group, so one instance is one observation.
 */
class ShareThroughput {
  /**
   * @param {object} config Validated CLI configuration.
   * @param {object} dependencies Collaborators.
   * @param {object} dependencies.kafka Public client whose sockets pass through `frames`.
   * @param {Frames} dependencies.frames The written-header observer wired into that client.
   * @param {{track:(client:object)=>void,snapshot:()=>Promise<object>}} dependencies.resources
   *   Real retained-state source; it only observes and never alters acquisition or acknowledgement.
   * @param {string} dependencies.trace Fresh path of the exclusive trace file.
   */
  constructor(config, { kafka, frames, resources, trace }) {
    this.config = config
    this.kafka = kafka
    this.frames = frames
    this.resources = resources
    this.trace = trace
    this.share = config.arm === 'share'
    this.groupId = `bench-${randomUUID()}`
    this.log = null
    this.consumer = null
    this.removers = []
    this.fetchEvents = []
    this.calls = []
    this.gaps = []
    this.entries = 0
    this.messages = 0
    this.lastEntryAt = null
    this.firstReturnAt = null
    this.lastReturnAt = null
    this.afterFirst = 0
    this.failure = null
    this.closed = false
    this.used = false
    this.handler = async ({ batch }) => {
      try {
        this.handle(batch)
      } catch (error) {
        this.fail(error)
        throw error
      }
    }
  }

  /**
   * Runs the observation: admission, consumption, checks, trace, release and cleanup. Cleanup
   * failures invalidate the observation and keep the primary failure.
   * @param {{topic:string,expected:string[],leaders:Map<number,string>}} corpus Prepared corpus,
   *   loaded before the baseline reading so it is never counted as retained.
   * @returns {Promise<object>} Metrics, post-GC resource readings and the trace hash.
   * @throws {Error} For a missing source, a runtime or cleanup failure, or a broken gate.
   */
  async run(corpus) {
    assert(typeof globalThis.gc === 'function', 'Use --expose-gc')
    assert(!this.used, 'A driver runs one observation')
    this.used = true
    const cleanupDeadline = performance.now() + LIMITS.timeout
    this.log = new AcquisitionLog(this.frames, corpus.leaders)
    let result
    let primary = null
    try {
      result = await this.observe(corpus, cleanupDeadline - LIMITS.cleanup, cleanupDeadline)
    } catch (error) {
      primary = error
    }
    const failures = await this.cleanup(cleanupDeadline)
    if (failures.length)
      throw new AggregateError(
        [...(primary ? [primary] : []), ...failures],
        'Benchmark cleanup failed'
      )
    if (primary) throw primary
    return result
  }

  /**
   * @param {{topic:string,expected:string[],leaders:Map<number,string>}} corpus Prepared corpus.
   * @param {number} deadline Deadline for the work that precedes cleanup.
   * @param {number} cleanupDeadline Deadline including the reserved cleanup time.
   * @returns {Promise<object>} The observation.
   * @throws {Error} For any failed step or gate.
   */
  async observe({ topic, expected, leaders }, deadline, cleanupDeadline) {
    await this.admit(topic, leaders, deadline)
    const before = await this.resources.snapshot()
    await this.consume(topic, expected.length, deadline)
    const idle = await this.resources.snapshot()
    assertIdle(idle, { share: this.share })
    const { delivered, metrics } = this.evaluate(expected)
    const traceHash = this.archive(metrics, delivered)
    await this.disconnect(cleanupDeadline)
    const after = await this.resources.snapshot()
    const heapDelta = assertReleased(before, after, { share: this.share })
    return {
      ...metrics,
      heapDelta,
      before,
      idle,
      after,
      delivered,
      controls: true,
      groupId: this.groupId,
      arm: this.share ? this.config.acquireMode : 'ordinary',
      trace: this.trace,
      traceHash,
    }
  }

  /**
   * Confirms the leader layout is unchanged and sets the share group configuration, with a
   * read-back, before any member can join. The admin is closed before the baseline reading.
   * @param {string} topic Corpus topic.
   * @param {Map<number,string>} leaders Leaders recorded when the corpus was prepared.
   * @param {number} deadline Original deadline.
   * @returns {Promise<void>}
   * @throws {Error} For a changed leader layout, a refused or unread configuration, or expiry.
   */
  async admit(topic, leaders, deadline) {
    const admin = this.kafka.admin()
    await bounded(() => admin.connect(), deadline)
    try {
      const current = await readLeaders(admin, topic, deadline)
      assert.deepEqual([...current].sort(), [...leaders].sort(), 'The leader layout changed')
      if (this.share)
        await bounded(() => setShareGroupConfigs(admin, this.groupId, GROUP_CONFIG), deadline)
    } finally {
      await bounded(() => admin.disconnect(), deadline)
    }
  }

  /**
   * Creates the consumer, subscribes, runs, and waits for every record, and for every share record
   * to have a terminal accepted result.
   * @param {string} topic Corpus topic.
   * @param {number} count Records in the corpus.
   * @param {number} deadline Original deadline.
   * @returns {Promise<void>}
   * @throws {Error} For an observer, crash or unexpected-event failure, or expiry.
   */
  async consume(topic, count, deadline) {
    const { kafka, config } = this
    this.consumer = this.share
      ? kafka.shareConsumer({
          groupId: this.groupId,
          acquireMode: config.acquireMode,
          acknowledgementMode: 'implicit',
          maxRecords: config.maxRecords,
          maxWaitTimeInMs: LIMITS.wait,
        })
      : kafka.consumer({ groupId: this.groupId, maxWaitTimeInMs: LIMITS.wait })
    if (this.share) this.resources.track(this.consumer)
    this.listen()
    await bounded(() => this.consumer.connect(), deadline)
    await bounded(
      () =>
        this.consumer.subscribe(this.share ? { topics: [topic] } : { topic, fromBeginning: true }),
      deadline
    )
    await bounded(
      () =>
        this.consumer.run(
          this.share ? { eachBatch: this.handler } : { autoCommit: false, eachBatch: this.handler }
        ),
      deadline
    )
    await until(() => this.drained(count), deadline, 50)
  }

  /**
   * Registers the event listeners. Any crash, reset, handler error or lock expiry invalidates the
   * observation; the public fetch and acknowledge events feed the independent checks.
   * @returns {void}
   */
  listen() {
    const { consumer } = this
    const on = (name, listener) => this.removers.push(consumer.on(consumer.events[name], listener))
    on('CRASH', ({ payload }) => this.fail(payload.error))
    if (!this.share) return
    on('FETCH', ({ payload }) => this.fetchEvents.push({ ...payload }))
    on('ACKNOWLEDGE', ({ payload }) => {
      try {
        this.log.acknowledge(payload, performance.now())
      } catch (error) {
        this.log.failure ||= error
      }
    })
    for (const name of ['SESSION_RESET', 'HANDLER_ERROR', 'LOCK_EXPIRED'])
      on(name, () => this.fail(new Error(`Unexpected ${name}`)))
  }

  /**
   * Handles one public batch with the least possible work: counters, references and timestamps.
   * @param {{topic:string,partition:number,messages:object[],acquisitionLockTimeoutMs:number}} batch Public batch.
   * @returns {void}
   * @throws {Error} From the acquisition log.
   */
  handle(batch) {
    const enteredAt = performance.now()
    this.entries += 1
    this.messages += batch.messages.length
    if (this.share) {
      this.log.returned(this.log.enter(batch, enteredAt), performance.now())
      return
    }
    if (this.lastEntryAt !== null) this.gaps.push(enteredAt - this.lastEntryAt)
    this.lastEntryAt = enteredAt
    const returnedAt = performance.now()
    this.calls.push({
      callback: this.calls.length,
      topic: batch.topic,
      partition: batch.partition,
      messages: batch.messages,
      enteredAt,
      returnedAt,
    })
    if (this.firstReturnAt === null) this.firstReturnAt = returnedAt
    else this.afterFirst += batch.messages.length
    this.lastReturnAt = returnedAt
  }

  /**
   * Keeps the first failure.
   * @param {Error} error Any failure.
   * @returns {void}
   */
  fail(error) {
    this.failure ||= error
  }

  /**
   * @returns {void}
   * @throws {Error} The first recorded failure, if any.
   */
  check() {
    const error = this.failure || this.log.failure || this.frames.failure
    if (error) throw error
  }

  /**
   * @param {number} count Records in the corpus.
   * @returns {boolean} Whether every record arrived and, for share, every one has a terminal result.
   * @throws {Error} The first recorded failure, if any.
   */
  drained(count) {
    this.check()
    return this.share
      ? this.log.delivered >= count && this.log.acknowledged === this.log.delivered
      : this.messages >= count
  }

  /**
   * Validates delivery against the producer corpus and the public fetch total, then measures.
   * @param {string[]} expected Producer-acknowledged identities.
   * @returns {{delivered:{batches:number,messages:number},metrics:object}} Checked counts and metrics.
   * @throws {Error} For any correctness, frame, latency, lock or interval gate.
   */
  evaluate(expected) {
    this.check()
    const calls = this.share ? this.log.calls : this.calls
    const delivered = verifyDelivery({
      expected,
      callbacks: calls,
      observed: observedStream(calls),
      claimedMessages: this.messages,
      claimedBatches: this.entries,
    })
    if (!this.share) {
      assert(this.lastReturnAt > this.firstReturnAt, 'No ordinary delivery interval')
      return {
        delivered,
        metrics: {
          status: 'valid',
          recordsPerSecond: (this.afterFirst * 1000) / (this.lastReturnAt - this.firstReturnAt),
          p99: quantile(this.gaps, 0.99),
          fetchPer1000: 0,
          gapSamples: this.gaps.length,
        },
      }
    }
    assert.equal(
      this.fetchEvents.reduce((sum, event) => sum + event.numberOfRecords, 0),
      this.messages,
      'Public fetch count differs from handler message sum'
    )
    assert(
      calls.every((call) => call.lock === LIMITS.lock),
      'Unexpected acquisition lock duration'
    )
    const frame = this.config.maxRecords === 1
    return {
      delivered,
      metrics: measureWindow({
        log: this.log,
        frames: this.frames,
        maxRecords: this.config.maxRecords,
        positive: frame,
        target: frame ? LIMITS.controlMeasured : LIMITS.measured,
      }),
    }
  }

  /**
   * Writes the exclusive trace, then releases all variable observer storage so the later heap
   * reading sees product state only. The measured identities leave `metrics`.
   * @param {object} metrics Window or ordinary metrics; `identities` is removed.
   * @param {{batches:number,messages:number}} delivered Checked counts.
   * @returns {string} SHA-256 of the trace file.
   * @throws {Error} When the trace exists or cannot be written.
   */
  archive(metrics, delivered) {
    const calls = this.share ? this.log.calls : this.calls
    const trace = new TraceWriter(this.trace)
    let traceHash
    try {
      trace.write({
        kind: 'summary',
        config: this.config,
        delivered,
        metrics: { ...metrics, identities: undefined },
        observer: {
          frames: this.frames.rows.length,
          callbacks: this.entries,
          records: this.messages,
          acknowledgements: this.log.events.length,
          fetchEvents: this.fetchEvents.length,
        },
      })
      for (const row of this.frames.rows) trace.write({ kind: 'frame', ...row })
      for (const call of calls)
        trace.write({
          kind: 'callback',
          callback: call.callback,
          topic: call.topic,
          partition: call.partition,
          enteredAt: call.enteredAt,
          returnedAt: call.returnedAt,
          identities: call.messages.map((message) =>
            identity({ topic: call.topic, partition: call.partition, message })
          ),
        })
      for (const event of this.log.events) trace.write({ kind: 'acknowledge', ...event })
      for (const event of this.fetchEvents) trace.write({ kind: 'fetch', ...event })
      if (metrics.identities) trace.write({ kind: 'measured', identities: metrics.identities })
    } finally {
      traceHash = trace.close()
    }
    delete metrics.identities
    this.log.clear()
    this.calls = []
    this.gaps = []
    this.fetchEvents = []
    this.frames.rows.length = 0
    return traceHash
  }

  /**
   * Removes the listeners and disconnects the consumer.
   * @param {number} due Original deadline.
   * @returns {Promise<void>}
   * @throws {Error} When the disconnect fails or expires.
   */
  async disconnect(due) {
    for (const remove of this.removers.splice(0)) remove()
    if (this.closed || !this.consumer) return
    await bounded(() => this.consumer.disconnect({ timeout: 10000 }), due)
    this.closed = true
  }

  /**
   * Disconnects what is left and removes the owned group and its configuration overrides. Every
   * step runs even when an earlier one failed.
   * @param {number} due Original deadline including the reserved cleanup time.
   * @returns {Promise<Error[]>} Failures; empty when cleanup completed.
   */
  async cleanup(due) {
    const failures = []
    for (const step of [() => this.disconnect(due), () => this.removeGroup(due)]) {
      try {
        await step()
      } catch (error) {
        failures.push(error)
      }
    }
    return failures
  }

  /**
   * Deletes the owned group, waiting while it is still non-empty, proves it is gone, and resets the
   * share configuration with one delete and a bounded read-back.
   * @param {number} due Original deadline.
   * @returns {Promise<void>}
   * @throws {Error} For a refused deletion, a remaining group or override, or expiry.
   */
  async removeGroup(due) {
    const admin = this.kafka.admin()
    await bounded(() => admin.connect(), due)
    try {
      await until(async () => {
        try {
          await admin.deleteGroups([this.groupId])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      }, due)
      await until(
        async () =>
          !(await admin.listGroups()).groups.some(({ groupId }) => groupId === this.groupId),
        due
      )
      if (this.share)
        await bounded(
          () => resetShareGroupConfigs(admin, this.groupId, Object.keys(GROUP_CONFIG)),
          due
        )
    } finally {
      await bounded(() => admin.disconnect(), due)
    }
  }
}

/**
 * Creates or removes the shared corpus through public APIs inside the guarded CI owner. The topic
 * outlives all arms and repetitions, and only this fixture deletes it.
 */
class CorpusFixture {
  /**
   * @param {object} config Validated fixed corpus configuration.
   * @throws {Error} Without the reviewed guard or the corpus manifest path.
   */
  constructor(config) {
    assert(
      process.env.KAFKAJS_CLUSTER_MANIFEST && process.env.P9_SHARE_CORPUS,
      'Corpus actions require the reviewed guard'
    )
    this.config = config
    this.file = path.resolve(process.env.P9_SHARE_CORPUS)
    this.deadline = performance.now() + LIMITS.timeout - 20000
    const kafka = new Kafka({
      brokers: plainTextBrokers(),
      clientId: `bench-fixture-${randomUUID()}`,
      logLevel: logLevel.NOTHING,
    })
    this.admin = kafka.admin()
    this.producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
  }

  /**
   * Runs one fixture action and closes its clients. A failed preparation removes its own topic.
   * @param {'prepare'|'cleanup'} phase The action.
   * @returns {Promise<void>} Prints one `fixture-complete` record on success.
   * @throws {Error} For a fixture, metadata, production, cleanup or deadline failure.
   */
  async run(phase) {
    const topic =
      phase === 'cleanup'
        ? JSON.parse(fs.readFileSync(this.file, 'utf8')).topic
        : `share-bench-${randomUUID()}`
    assert(TOPIC.test(topic), 'Unowned corpus topic')
    let primary = null
    try {
      await bounded(() => this.admin.connect(), this.deadline)
      if (phase === 'cleanup') await this.removeTopic(topic, this.deadline)
      else await this.prepare(topic)
      process.stdout.write(
        `${JSON.stringify({ status: 'fixture-complete', phase, topic, corpus: this.file })}\n`
      )
    } catch (error) {
      primary = error
    }
    const failures = await this.close(primary && phase === 'prepare' ? topic : null)
    if (failures.length)
      throw new AggregateError(
        [...(primary ? [primary] : []), ...failures],
        'Corpus cleanup failed'
      )
    if (primary) throw primary
  }

  /**
   * Creates the topic, checks one leader partition per node, produces the corpus once and writes
   * the manifest exclusively.
   * @param {string} topic Fresh owned topic.
   * @returns {Promise<void>}
   * @throws {Error} For an existing manifest or any failed step.
   */
  async prepare(topic) {
    assert(!fs.existsSync(this.file), 'Corpus manifest already exists')
    const created = await bounded(
      () =>
        this.admin.createTopics({
          waitForLeaders: true,
          topics: [
            {
              topic,
              numPartitions: LIMITS.partitions,
              replicationFactor: 3,
              configEntries: [
                { name: 'cleanup.policy', value: 'delete' },
                { name: 'retention.ms', value: '-1' },
              ],
            },
          ],
        }),
      this.deadline
    )
    assert(created, 'The corpus topic already existed')
    const leaders = await readLeaders(this.admin, topic, this.deadline)
    await bounded(() => this.producer.connect(), this.deadline)
    const expected = await bounded(
      () => produceCorpus(this.producer, topic, this.config),
      this.deadline
    )
    fs.writeFileSync(
      this.file,
      JSON.stringify({
        schema: 1,
        topic,
        brokers: plainTextBrokers(),
        leaders: [...leaders],
        expected,
        messages: this.config.messages,
        size: this.config.size,
        partitions: this.config.partitions,
      }),
      { flag: 'wx' }
    )
  }

  /**
   * Deletes the topic and waits until every broker no longer lists it.
   * @param {string} topic Owned topic.
   * @param {number} due Original deadline.
   * @returns {Promise<void>}
   * @throws {Error} For a refused deletion or a topic still listed at the deadline.
   */
  async removeTopic(topic, due) {
    await bounded(() => this.admin.deleteTopics({ topics: [topic], timeout: 8000 }), due).catch(
      (error) => {
        if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
      }
    )
    await bounded(
      () =>
        waitForShareFixtureAbsence(this.admin, [], [topic], {
          signal: AbortSignal.timeout(Math.max(1, due - performance.now())),
          unrelated: [],
        }),
      due
    )
  }

  /**
   * Closes both clients, first removing a half-prepared topic.
   * @param {string|null} topic Topic to remove, or null.
   * @returns {Promise<Error[]>} Failures; empty when every close succeeded.
   */
  async close(topic) {
    const due = performance.now() + 15000
    const failures = []
    const attempt = async (work) => {
      try {
        await bounded(work, due)
      } catch (error) {
        failures.push(error)
      }
    }
    if (topic) await attempt(() => this.removeTopic(topic, due))
    await attempt(() => this.producer.disconnect({ timeout: 5000 }))
    await attempt(() => this.admin.disconnect())
    return failures
  }
}

/**
 * Loads and validates the prepared corpus manifest.
 * @param {string} file Manifest path.
 * @param {{messages:number,size:number,partitions:number}} config Validated configuration.
 * @returns {{topic:string,brokers:string[],expected:string[],leaders:Map<number,string>}} Corpus.
 * @throws {Error} For a manifest that does not match the fixed corpus.
 */
const readCorpus = (file, config) => {
  const corpus = JSON.parse(fs.readFileSync(file, 'utf8'))
  assert(
    corpus.schema === 1 &&
      TOPIC.test(corpus.topic) &&
      Array.isArray(corpus.brokers) &&
      corpus.leaders.length === LIMITS.partitions &&
      corpus.expected.length === config.messages &&
      corpus.messages === config.messages &&
      corpus.size === config.size &&
      corpus.partitions === config.partitions,
    'Wrong fixed corpus manifest'
  )
  return { ...corpus, leaders: new Map(corpus.leaders) }
}

/**
 * Runs the untimed controls, a fixture action, or one guarded observation, and prints one JSON
 * record. Session scheduling and aggregation belong to the benchmark CI lane.
 * @param {string[]} argv Explicit arguments.
 * @returns {Promise<void>}
 * @throws {Error} For a missing guard, corpus, trace or resource source, or any failed control or gate.
 */
const main = async (argv) => {
  const { phase, config } = parseCommand(argv)
  const checks = controls()
  if (phase !== 'run') {
    await new CorpusFixture(config).run(phase)
    return
  }
  if (config.control === 1) {
    process.stdout.write(`${JSON.stringify(checks)}\n`)
    return
  }
  const {
    KAFKAJS_CLUSTER_MANIFEST: guard,
    P9_SHARE_CORPUS: manifest,
    P9_SHARE_TRACE: trace,
  } = process.env
  assert(guard && manifest, 'Measured runs require the reviewed guard and corpus manifest')
  assert(trace && !fs.existsSync(trace), 'A fresh trace path is required')
  const corpus = readCorpus(path.resolve(manifest), config)
  const { instrumentShare } = require('../soak/soak')
  assert(typeof instrumentShare === 'function', 'The share resource observer is not available')
  const frames = new Frames(defaultSocketFactory())
  const observed = instrumentShare({
    brokers: corpus.brokers,
    clientId: `share-bench-${randomUUID()}`,
    logLevel: logLevel.NOTHING,
    socketFactory: (args) => frames.socket(args),
  })
  const driver = new ShareThroughput(config, {
    kafka: observed.kafka,
    frames,
    resources: observed.shareResources,
    trace: path.resolve(trace),
  })
  const result = await driver.run(corpus)
  process.stdout.write(`${JSON.stringify({ ...result, config, checks, schema: 1 })}\n`)
  if (result.status !== 'valid') process.exitCode = 2
}

if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })

module.exports = {
  LIMITS,
  parseArgs,
  parseCommand,
  identity,
  verifyDelivery,
  quantile,
  assertIdle,
  assertReleased,
  HeaderStream,
  Frames,
  AcquisitionLog,
  measureWindow,
  evaluateSession,
  controls,
  produceCorpus,
  ShareThroughput,
  CorpusFixture,
  main,
}
